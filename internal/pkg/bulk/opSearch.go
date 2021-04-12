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

	"github.com/elastic/fleet-server/v7/internal/pkg/es"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/rs/zerolog/log"
)

func (b *Bulker) Search(ctx context.Context, index []string, body []byte, opts ...Opt) (*es.ResultT, error) {
	opt := b.parseOpts(opts...)

	blk := b.NewBlk(ActionSearch, opt)

	// Serialize request
	const kSlop = 64
	blk.buf.Grow(len(body) + kSlop)

	if err := b.writeMsearchMeta(blk.buf, index); err != nil {
		return nil, err
	}

	if err := b.writeMsearchBody(blk.buf, body); err != nil {
		return nil, err
	}

	// Process response
	resp := b.dispatch(ctx, blk)
	if resp.err != nil {
		return nil, resp.err
	}
	b.FreeBlk(blk)

	// Interpret response
	r := resp.data.(*MsearchResponseItem)
	return &es.ResultT{HitsT: r.Hits, Aggregations: r.Aggregations}, nil
}

func (b *Bulker) writeMsearchMeta(buf *bytes.Buffer, indices []string) error {
	if err := b.validateIndices(indices); err != nil {
		return err
	}

	switch len(indices) {
	case 0:
		buf.WriteString("{ }\n")
	case 1:
		buf.WriteString(`{"index": "`)
		buf.WriteString(indices[0])
		buf.WriteString("\"}\n")
	default:
		buf.WriteString(`{"index": `)
		if d, err := json.Marshal(indices); err != nil {
			return err
		} else {
			buf.Write(d)
		}
		buf.WriteString("}\n")
	}

	return nil
}

func (b *Bulker) writeMsearchBody(buf *bytes.Buffer, body []byte) error {
	buf.Write(body)
	buf.WriteRune('\n')

	return b.validateBody(body)
}

func (b *Bulker) flushSearch(ctx context.Context, queue *bulkT, szPending int) error {
	start := time.Now()

	buf := bytes.Buffer{}
	buf.Grow(szPending)

	queueCnt := 0
	for n := queue; n != nil; n = n.next {
		buf.Write(n.buf.Bytes())
		b.blkPool.Put(n.buf)
		n.buf = nil

		queueCnt += 1
	}

	// Do actual bulk request; and send response on chan
	req := esapi.MsearchRequest{
		Body: bytes.NewReader(buf.Bytes()),
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

	var blk MsearchResponse
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&blk); err != nil {
		return fmt.Errorf("flush: error parsing response body: %s", err) // TODO: Wrap error
	}

	log.Trace().
		Err(err).
		Str("mod", kModBulk).
		Dur("rtt", time.Since(start)).
		Int("took", blk.Took).
		Int("sz", len(blk.Responses)).
		Msg("flushSearch")

	if len(blk.Responses) != queueCnt {
		return fmt.Errorf("Bulk queue length mismatch")
	}

	n := queue
	for _, response := range blk.Responses {
		next := n.next // 'n' is invalid immediately on channel send

		cResponse := response
		n.ch <- respT{
			err:  response.deriveError(),
			data: &cResponse,
		}
		n = next
	}

	return nil
}
