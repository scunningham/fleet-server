// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/rs/zerolog/log"
)

func (b *Bulker) Read(ctx context.Context, index, id string, opts ...Opt) ([]byte, error) {
	opt := b.parseOpts(opts...)

	blk := b.NewBlk(ActionRead, opt)

	// Serialize request
	const kSlop = 64
	blk.buf.Grow(kSlop)

	if err := b.writeMget(&blk.buf, index, id); err != nil {
		return nil, err
	}

	// Process response
	resp := b.dispatch(ctx, blk)
	if resp.err != nil {
		return nil, resp.err
	}
	b.FreeBlk(blk)

	// Interpret response, looking for generated id
	r := resp.data.(*MgetResponseItem)
	return r.Source, nil
}

func (b *Bulker) flushRead(ctx context.Context, queue *bulkT, szPending int) error {
	start := time.Now()

	buf := bytes.NewBufferString(rPrefix)
	buf.Grow(szPending + len(rSuffix))

	// Each item a JSON array element followed by comma
	queueCnt := 0
	for n := queue; n != nil; n = n.next {
		buf.Write(n.buf.Bytes())
		queueCnt += 1
	}

	// Need to strip the last element and append the suffix
	payload := buf.Bytes()
	payload = append(payload[:len(payload)-1], []byte(rSuffix)...)

	// Do actual bulk request; and send response on chan
	req := esapi.MgetRequest{
		Body: bytes.NewReader(payload),
	}
	res, err := req.Do(ctx, b.es)

	if err != nil {
		return err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	if res.IsError() {
		return fmt.Errorf("flush: %s", res.String()) // TODO: Wrap error
	}

	var blk MgetResponse
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&blk); err != nil {
		return fmt.Errorf("flush: error parsing response body: %s", err) // TODO: Wrap error
	}

	log.Trace().
		Err(err).
		Str("mod", kModBulk).
		Dur("rtt", time.Since(start)).
		Int("sz", len(blk.Items)).
		Msg("flushRead")

	if len(blk.Items) != queueCnt {
		return fmt.Errorf("Mget queue length mismatch")
	}

	n := queue
	for _, item := range blk.Items {
		next := n.next // 'n' is invalid immediately on channel send
		citem := item
		n.ch <- respT{
			err:  item.deriveError(),
			idx:  n.idx,
			data: &citem,
		}
		n = next
	}

	return nil
}
