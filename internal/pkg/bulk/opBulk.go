// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v8/esapi"	
	"github.com/rs/zerolog/log"
)

func (b *Bulker) Create(ctx context.Context, index, id string, body []byte, opts ...Opt) (string, error) {
	item, err := b.waitBulkAction(ctx, ActionCreate, index, id, body, opts...)
	if err != nil {
		return "", err
	}

	return item.DocumentID, nil
}

func (b *Bulker) Index(ctx context.Context, index, id string, body []byte, opts ...Opt) (string, error) {
	item, err := b.waitBulkAction(ctx, ActionIndex, index, id, body, opts...)
	if err != nil {
		return "", err
	}
	return item.DocumentID, nil
}

func (b *Bulker) Update(ctx context.Context, index, id string, body []byte, opts ...Opt) error {
	_, err := b.waitBulkAction(ctx, ActionUpdate, index, id, body, opts...)
	return err
}

func (b *Bulker) waitBulkAction(ctx context.Context, action Action, index, id string, body []byte, opts ...Opt) (*BulkIndexerResponseItem, error) {
	opt := b.parseOpts(opts...)

	blk := b.NewBlk(action, opt)

	// Serialize request
	const kSlop = 64
	blk.buf.Grow(len(body) + kSlop)

	if err := b.writeBulkMeta(blk.buf, action, index, id); err != nil {
		return nil, err
	}

	if err := b.writeBulkBody(blk.buf, body); err != nil {
		return nil, err
	}

	// Dispatch and wait for response
	resp := b.dispatch(ctx, blk)
	if resp.err != nil {
		return nil, resp.err
	}
	b.FreeBlk(blk)

	r := resp.data.(*BulkIndexerResponseItem)
	return r, nil
}

func (b *Bulker) writeMget(buf *bytes.Buffer, index, id string) error {
	if err := b.validateMeta(index, id); err != nil {
		return err
	}

	buf.WriteString(`{"_index":"`)
	buf.WriteString(index)
	buf.WriteString(`","_id":"`)
	buf.WriteString(id)
	buf.WriteString(`"},`)
	return nil
}

func (b *Bulker) writeBulkMeta(buf *bytes.Buffer, action Action, index, id string) error {
	if err := b.validateMeta(index, id); err != nil {
		return err
	}

	buf.WriteString(`{"`)
	buf.WriteString(action.Str())
	buf.WriteString(`":{`)
	if id != "" {
		buf.WriteString(`"_id":"`)
		buf.WriteString(id)
		buf.WriteString(`",`)
	}

	buf.WriteString(`"_index":"`)
	buf.WriteString(index)
	buf.WriteString("\"}}\n")
	return nil
}

func (b *Bulker) writeBulkBody(buf *bytes.Buffer, body []byte) error {
	if body == nil {
		return nil
	}
	buf.Write(body)
	buf.WriteRune('\n')

	return b.validateBody(body)
}

func (b *Bulker) flushBulk(ctx context.Context, queue *bulkT, szPending int) error {

	buf := bytes.Buffer{}
	buf.Grow(szPending)

	doRefresh := "false"

	queueCnt := 0
	for n := queue; n != nil; n = n.next {
		buf.Write(n.buf.Bytes())
		b.blkPool.Put(n.buf)
		n.buf = nil
		
		if n.opts.Refresh {
			doRefresh = "true"
		}
		queueCnt += 1
	}

	// Do actual bulk request; and send response on chan
	req := esapi.BulkRequest{
		Body:    bytes.NewReader(buf.Bytes()),
		Refresh: doRefresh,
	}
	res, err := req.Do(ctx, b.es)

	if err != nil {
		log.Error().Err(err).Str("mod", kModBulk).Msg("Fail req.Do")
		return err
	}

	if res.Body != nil {
		defer res.Body.Close()
	}

	if res.IsError() {
		log.Error().Str("mod", kModBulk).Str("err", res.String()).Msg("Fail result")
		return fmt.Errorf("flush: %s", res.String()) // TODO: Wrap error
	}

	var blk BulkIndexerResponse
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&blk); err != nil {
		log.Error().Err(err).Str("mod", kModBulk).Msg("Decode error")
		return fmt.Errorf("flush: error parsing response body: %s", err) // TODO: Wrap error
	}

	log.Trace().
		Err(err).
		Bool("refresh", doRefresh == "true").
		Str("mod", kModBulk).
		Int("took", blk.Took).
		Bool("hasErrors", blk.HasErrors).
		Int("sz", len(blk.Items)).
		Msg("flushBulk")

	if len(blk.Items) != queueCnt {
		return fmt.Errorf("Bulk queue length mismatch")
	}

	n := queue
	for _, blkItem := range blk.Items {
		next := n.next // 'n' is invalid immediately on channel send

		for _, item := range blkItem {

			select {
			case n.ch <- respT{
				err:  item.deriveError(),
				data: &item,
			}:
			default:
				panic("Should not happen")
			}

			break
		}
		n = next
	}

	return nil
}
