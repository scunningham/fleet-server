// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"context"
	"time"
	"reflect"

	"github.com/rs/zerolog/log"
)

func (b *Bulker) MUpdate(ctx context.Context, ops []MultiOp, opts ...Opt) error {
	_, err := b.multiWaitBulkAction(ctx, ActionUpdate, ops)
	return err
}

func (b *Bulker) multiWaitBulkAction(ctx context.Context, action Action, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	opt := b.parseOpts(opts...)

	// Serialize requests
	nops := make([]*bulkT, 0, len(ops))
	for _, op := range ops {

		blk := b.NewBlk(action, opt)

		// Prealloc buffer
		const kSlop = 64
		blk.buf.Grow(len(op.Body) + kSlop)

		if err := b.writeBulkMeta(&blk.buf, action, op.Index, op.Id); err != nil {
			return nil, err
		}

		if err := b.writeBulkBody(&blk.buf, op.Body); err != nil {
			return nil, err
		}

		nops = append(nops, blk)
	}

	// Dispatch and wait for response
	resps, err := b.multiDispatch(ctx, nops)
	if err != nil {
		return nil, err
	}

	var lastErr error
	items := make([]BulkIndexerResponseItem, len(resps))
	for i, r := range resps {
		b.FreeBlk(nops[i])

		if r.err != nil {
			log.Info().Err(r.err).Msg("Fail muliDispatch")
			lastErr = r.err
		}

		items[i] = *r.data.(*BulkIndexerResponseItem)
	}

	return items, lastErr
}

func (b *Bulker) multiDispatch(ctx context.Context, blks []*bulkT) ([]respT, error) {
	start := time.Now()

	var err error

	cases := make([]reflect.SelectCase, len(blks) + 1)

	cases[0] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
        Chan: reflect.ValueOf(ctx.Done()),
	}

	for i, blk := range blks {

		// Dispatch to bulk Run loop
		select {
		case b.ch <- blk:
		case <-ctx.Done():
			log.Error().
				Err(ctx.Err()).
				Str("mod", kModBulk).
				Dur("rtt", time.Since(start)).
				Msg("multiDispatch abort queue")
			return nil, ctx.Err()
		}

		cases[i+1] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
            Chan: reflect.ValueOf(blk.ch),
		}
	}

	// Wait for response
	responses := make([]respT, len(blks))

	cnt := 0

LOOP:
	for cnt < len(blks) {
		i, v, ok := reflect.Select(cases) 
		if !ok {
			panic("channel should never close")
		}

		// Check if ctx.Done() fired
		if i == 0 {
			err = ctx.Err()
			responses = nil
			log.Error().
				Err(err).
				Str("mod", kModBulk).
				Dur("rtt", time.Since(start)).
				Msg("multiDispatch abort response")
			break LOOP
		}

		responses[i-1] = v.Interface().(respT)
		cnt += 1
	}

	log.Trace().Err(err).
		Int("nOps", len(blks)).
		Str("mod", kModBulk).
		Dur("rtt", time.Since(start)).
		Msg("multiDispatch done")

	return responses, err
}
