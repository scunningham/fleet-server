// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package checkin

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/bulk"
	"github.com/elastic/fleet-server/v7/internal/pkg/dl"
	"github.com/elastic/fleet-server/v7/internal/pkg/sqn"

	"github.com/rs/zerolog/log"
)

type Fields = bulk.UpdateFields

const defaultFlushInterval = 10 * time.Second

type optionsT struct {
	flushInterval time.Duration
}

type Opt func(*optionsT)

func WithFlushInterval(d time.Duration) Opt {
	return func(opt *optionsT) {
		opt.flushInterval = d
	}
}

type PendingData struct {
	ts    string
	meta  []byte
	seqNo sqn.SeqNo
}

type BulkCheckin struct {
	opts    optionsT
	bulker  bulk.Bulk
	mut     sync.Mutex
	pending map[string]PendingData

	ts   string
	unix int64
}

func NewBulkCheckin(bulker bulk.Bulk, opts ...Opt) *BulkCheckin {
	parsedOpts := parseOpts(opts...)

	return &BulkCheckin{
		opts:    parsedOpts,
		bulker:  bulker,
		pending: make(map[string]PendingData),
	}
}

func parseOpts(opts ...Opt) optionsT {

	outOpts := optionsT{
		flushInterval: defaultFlushInterval,
	}

	for _, f := range opts {
		f(&outOpts)
	}

	return outOpts
}

// Generate and cache timestamp on seconds change.
// Avoid thousands of formats of an identical string.
func (bc *BulkCheckin) timestamp() string {

	// WARNING: Expects mutex locked.
	now := time.Now()
	if now.Unix() != bc.unix {
		bc.unix = now.Unix()
		bc.ts = now.UTC().Format(time.RFC3339)
	}

	return bc.ts
}

// WARNING: BulkCheckin will take ownership of fields,
// so do not use after passing in.
func (bc *BulkCheckin) CheckIn(id string, meta []byte, seqno sqn.SeqNo) error {

	bc.mut.Lock()

	bc.pending[id] = PendingData{
		ts:    bc.timestamp(),
		meta:  meta,
		seqNo: seqno,
	}

	bc.mut.Unlock()
	return nil
}

func (bc *BulkCheckin) Run(ctx context.Context) error {

	tick := time.NewTicker(bc.opts.flushInterval)

	var err error
LOOP:
	for {
		select {
		case <-tick.C:
			if err = bc.flush(ctx); err != nil {
				log.Error().Err(err).Msg("Eat bulk checkin error; Keep on truckin'")
				err = nil
			}

		case <-ctx.Done():
			err = ctx.Err()
			break LOOP
		}
	}

	return err
}

func (bc *BulkCheckin) flush(ctx context.Context) error {
	start := time.Now()

	bc.mut.Lock()
	pending := bc.pending
	bc.pending = make(map[string]PendingData, len(pending))
	bc.mut.Unlock()

	if len(pending) == 0 {
		return nil
	}

	updates := make([]bulk.MultiOp, 0, len(pending))

	simpleCache := make(map[string][]byte)

	nowTimestamp := start.UTC().Format(time.RFC3339)

	var err error
	var needRefresh bool
	for id, pendingData := range pending {

		// In the simple case, there are no fields and no seqNo.
		// When that is true, we can reuse an already generated
		// JSON body containing just the timestamp updates.
		var body []byte
		if pendingData.meta == nil && !pendingData.seqNo.IsSet() {
			var ok bool
			body, ok = simpleCache[pendingData.ts]
			if !ok {
				fields := Fields{
					dl.FieldLastCheckin: pendingData.ts,
					dl.FieldUpdatedAt:   nowTimestamp,
				}
				if body, err = fields.Marshal(); err != nil {
					return err
				}
				simpleCache[pendingData.ts] = body
			}
		} else {

			fields := Fields{
				dl.FieldLastCheckin: pendingData.ts, // Set the checkin timestamp
				dl.FieldUpdatedAt:   nowTimestamp,   // Set "updated_at" to the current timestamp
			}

			// Update local metadata if provided
			if pendingData.meta != nil {
				fields[dl.FieldLocalMetadata] = json.RawMessage(pendingData.meta)
			}

			// If seqNo changed, set the field appropriately
			if pendingData.seqNo.IsSet() {
				fields[dl.FieldActionSeqNo] = pendingData.seqNo

				// Only refresh if seqNo changed; dropping metadata not important.
				needRefresh = true
			}

			if body, err = fields.Marshal(); err != nil {
				return err
			}

			log.Info().RawJSON("before", pendingData.meta).RawJSON("body", body).Msg("WTF")
		}

		updates = append(updates, bulk.MultiOp{
			Id:    id,
			Body:  body,
			Index: dl.FleetAgents,
		})
	}

	var opts []bulk.Opt
	if needRefresh {
		opts = append(opts, bulk.WithRefresh())
	}

	_, err = bc.bulker.MUpdate(ctx, updates, opts...)

	log.Trace().
		Err(err).
		Dur("rtt", time.Since(start)).
		Int("cnt", len(updates)).
		Bool("refresh", needRefresh).
		Msg("Flush updates")

	return err
}
