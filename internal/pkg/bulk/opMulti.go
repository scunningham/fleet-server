// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"context"
	"errors"
)

func (b *Bulker) MCreate(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	return b.multiWaitBulkOp(ctx, ActionCreate, ops)
}

func (b *Bulker) MIndex(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	return b.multiWaitBulkOp(ctx, ActionIndex, ops)
}

func (b *Bulker) MUpdate(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	return b.multiWaitBulkOp(ctx, ActionUpdate, ops)
}

func (b *Bulker) MDelete(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	return b.multiWaitBulkOp(ctx, ActionDelete, ops)
}

func (b *Bulker) multiWaitBulkOp(ctx context.Context, action actionT, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	if len(ops) == 0 {
		return nil, nil
	}

	const kMaxBulk = (1 << 32) - 1

	if len(ops) > kMaxBulk {
		return nil, errors.New("Too many bulk ops")
	}

	opt := b.parseOpts(opts...)

	ch := make(chan respT, len(ops))

	actionStr := action.Str()

	// O(n) Determine how much space we need
	var byteCnt int
	for _, op := range ops {
		byteCnt += b.calcBulkSz(actionStr, op.Index, op.Id, op.Body)
	}

	// Create one bulk buffer to serialize each piece.
	// This decreases pressure on the heap.
	var bulkBuf Buf
	bulkBuf.Grow(byteCnt)

	// Serialize requests
	bulks := make([]bulkT, len(ops))
	for i := range ops {

		bufIdx := bulkBuf.Len()

		op := &ops[i]

		if err := b.writeBulkMeta(&bulkBuf, actionStr, op.Index, op.Id, opt.RetryOnConflict); err != nil {
			return nil, err
		}

		if err := b.writeBulkBody(&bulkBuf, op.Body); err != nil {
			return nil, err
		}

		bulk := &bulks[i]
		bulk.ch = ch
		bulk.idx = int32(i)
		bulk.action = action
		bulk.buf.buf = bulkBuf.buf[bufIdx:]
		if opt.Refresh {
			bulk.flags.Set(flagRefresh)
		}
	}

	// Dispatch requests
	if err := b.multiDispatch(ctx, bulks); err != nil {
		return nil, err
	}

	// Wait for response and populate return slice
	var lastErr error
	items := make([]BulkIndexerResponseItem, len(ops))

	for i := 0; i < len(ops); i++ {
		select {
		case r := <-ch:
			if r.err != nil {
				lastErr = r.err
			}
			if r.data != nil {
				items[r.idx] = *r.data.(*BulkIndexerResponseItem)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	return items, lastErr
}

func (b *Bulker) multiDispatch(ctx context.Context, blks []bulkT) error {

	// Dispatch to bulk Run loop; Iterate by reference.
	for i := range blks {
		select {
		case b.ch <- &blks[i]:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}
