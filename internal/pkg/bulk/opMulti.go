// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"context"
	//"time"

	"github.com/rs/zerolog/log"
)

func (b *Bulker) MUpdate(ctx context.Context, ops []MultiOp, opts ...Opt) error {
	_, err := b.multiWaitBulkAction(ctx, ActionUpdate, ops)
	return err
}

func (b *Bulker) multiWaitBulkAction(ctx context.Context, action Action, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error) {
	if len(ops) == 0 {
		return nil, nil
	}
	
	opt := b.parseOpts(opts...)

	ch := make(chan respT, len(ops))


	actionStr := action.Str()

	// O(n) Determine how much space we need
	var cnt int
	for _, op := range ops {
		cnt += b.calcBulkSz(actionStr, op.Index, op.Id, op.Body)
	}

	// Create one bulk buffer to serialize each piece. 
	// This decreases the number of heap objects necessary.
	var bulkBuf Buf
	bulkBuf.Grow(cnt)

	// Serialize requests
	nops := make([]bulkT, len(ops))
	for i := range ops {

		bufIdx := bulkBuf.Len()

		op := &ops[i]

		if err := b.writeBulkMeta(&bulkBuf, actionStr, op.Index, op.Id); err != nil {
			return nil, err
		}

		if err := b.writeBulkBody(&bulkBuf, op.Body); err != nil {
			return nil, err
		}

		nop := &nops[i]
		nop.ch = ch
		nop.idx = i
		nop.opts = opt
		nop.action = action
		nop.buf.buf = bulkBuf.buf[bufIdx:]
	}

/*
	// Dispatch and wait for response
	resps, err := b.multiDispatch(ctx, nops)
	if err != nil {
		return nil, err
	}
*/

	for i := range nops {
		select {
		case b.ch <- &nops[i]:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}


	var lastErr error
	items := make([]BulkIndexerResponseItem, len(ops))


	// Wait for responses
	var i int

	for i < len(items) {
		select {
		case r := <-ch:
			if r.err != nil {
				log.Info().Err(r.err).Msg("Fail muliDispatch")
				lastErr = r.err
			}

			// fix up index here, this is not right
			if r.data != nil { 
				items[i] = *r.data.(*BulkIndexerResponseItem)
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		i += 1
	}



	return items, lastErr
}

func (b *Bulker) multiDispatch(ctx context.Context, blks []bulkT) ([]respT, error) {
	//start := time.Now()

	var err error

	// Iterate by reference
	for i := range blks {
		// Dispatch to bulk Run loop
		select {
		case b.ch <- &blks[i]:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	ch := blks[0].ch


		// Wait for response
	responses := make([]respT, 0, len(blks))

LOOP:
	for len(responses) < len(blks) {
		select {
		case resp := <-ch:
			responses = append(responses, resp)
		case <-ctx.Done():
			err = ctx.Err()
			responses = nil
			break LOOP
		}
	}

	return responses, err

	/*

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
	*/
}



/*
before blkPool

cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
BenchmarkUpdate8-16       	  163479	      6986 ns/op	    2802 B/op	      15 allocs/op
BenchmarkUpdate64-16      	  178108	      6985 ns/op	    2802 B/op	      15 allocs/op
BenchmarkUpdate512-16     	  172149	      7008 ns/op	    2802 B/op	      15 allocs/op
BenchmarkUpdate2K-16      	     883	   1328043 ns/op	  673004 B/op	    2061 allocs/op
BenchmarkUpdate8K-16      	     205	   5588052 ns/op	 2703251 B/op	    8285 allocs/op
BenchmarkUpdate32K-16     	      52	  22085310 ns/op	10978855 B/op	   34042 allocs/op
BenchmarkUpdate128K-16    	      12	  88842438 ns/op	46890715 B/op	  152925 allocs/op

*/

/* after blk pool
BenchmarkUpdate8-16       	  183309	      6372 ns/op	    3566 B/op	       7 allocs/op
BenchmarkUpdate64-16      	  176274	      6434 ns/op	    3735 B/op	       7 allocs/op
BenchmarkUpdate512-16     	  181476	      6363 ns/op	    3723 B/op	       7 allocs/op
BenchmarkUpdate2K-16      	     838	   1232986 ns/op	  897387 B/op	      42 allocs/op
BenchmarkUpdate8K-16      	     241	   4707331 ns/op	 3354837 B/op	     386 allocs/op
BenchmarkUpdate32K-16     	      57	  19851519 ns/op	14042188 B/op	    5194 allocs/op
BenchmarkUpdate128K-16    	      12	  89346973 ns/op	62825971 B/op	   76522 allocs/op
*/



