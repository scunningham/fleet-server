// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/config"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/semaphore"
)

type MultiOp struct {
	Id    string
	Index string
	Body  []byte
}

type Bulk interface {

	// Synchronous operations run in the bulk engine
	Create(ctx context.Context, index, id string, body []byte, opts ...Opt) (string, error)
	Index(ctx context.Context, index, id string, body []byte, opts ...Opt) (string, error)
	Update(ctx context.Context, index, id string, body []byte, opts ...Opt) error
	Read(ctx context.Context, index, id string, opts ...Opt) ([]byte, error)
	Search(ctx context.Context, index []string, body []byte, opts ...Opt) (*es.ResultT, error)

	// Multi Operation API's run in the bulk engine
	MUpdate(ctx context.Context, ops []MultiOp, opts ...Opt) ([]BulkIndexerResponseItem, error)

	// Accessor used to talk to elastic search direcly bypassing bulk engine
	Client() *elasticsearch.Client
}

type Action string

func (a Action) Str() string { return string(a) }

const (
	ActionCreate Action = "create"
	ActionDelete        = "delete"
	ActionIndex         = "index"
	ActionUpdate        = "update"
	ActionRead          = "read"
	ActionSearch        = "search"
)

const kModBulk = "bulk"

type Bulker struct {
	es *elasticsearch.Client
	ch chan *bulkT

	blkPool sync.Pool
}

const (
	rPrefix = "{\"docs\": ["
	rSuffix = "]}"

	defaultFlushInterval     = time.Second * 5
	defaultFlushThresholdCnt = 32768
	defaultFlushThresholdSz  = 1024 * 1024 * 10
	defaultMaxPending        = 32
)

func InitES(ctx context.Context, cfg *config.Config, opts ...BulkOpt) (*elasticsearch.Client, Bulk, error) {

	es, err := es.NewClient(ctx, cfg, false)
	if err != nil {
		return nil, nil, err
	}

	opts = append(opts,
		WithFlushInterval(cfg.Output.Elasticsearch.BulkFlushInterval),
		WithFlushThresholdCount(cfg.Output.Elasticsearch.BulkFlushThresholdCount),
		WithFlushThresholdSize(cfg.Output.Elasticsearch.BulkFlushThresholdSize),
		WithMaxPending(cfg.Output.Elasticsearch.BulkFlushMaxPending),
	)

	blk := NewBulker(es)
	go func() {
		err := blk.Run(ctx, opts...)
		log.Info().Err(err).Msg("Bulker exit")
	}()

	return es, blk, nil
}

func NewBulker(es *elasticsearch.Client) *Bulker {

	return &Bulker{
		es:      es,
		ch:      make(chan *bulkT, 32),
		blkPool: sync.Pool{New: func() interface{} { return &bulkT{ch: make(chan respT, 1)} }},
	}
}

func (b *Bulker) Client() *elasticsearch.Client {
	return b.es
}

func (b *Bulker) parseBulkOpts(opts ...BulkOpt) bulkOptT {
	bopt := bulkOptT{
		flushInterval:     defaultFlushInterval,
		flushThresholdCnt: defaultFlushThresholdCnt,
		flushThresholdSz:  defaultFlushThresholdSz,
		maxPending:        defaultMaxPending,
	}

	for _, f := range opts {
		f(&bopt)
	}

	return bopt
}

// Stop timer, but don't stall on channel.
// API doesn't not seem to work as specified.
func stopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}

type queueT struct {
	action  Action
	head    *bulkT
	tail    *bulkT
	pending int
}

const (
	kQueueBulk = iota
	kQueueRead
	kQueueSearch
	kQueueRefresh
	kNumQueues
)

func (b *Bulker) Run(ctx context.Context, opts ...BulkOpt) error {
	var err error

	bopts := b.parseBulkOpts(opts...)

	log.Info().Interface("opts", &bopts).Msg("Run bulker with options")

	// Create timer in stopped state
	timer := time.NewTimer(bopts.flushInterval)
	stopTimer(timer)
	defer timer.Stop()

	w := semaphore.NewWeighted(int64(bopts.maxPending))

	queues := make([]*queueT, 0, kNumQueues)
	for i := 0; i < kNumQueues; i++ {
		var action Action
		switch i {
		case kQueueRead:
			action = ActionRead
		case kQueueSearch:
			action = ActionSearch
		case kQueueBulk, kQueueRefresh:
			// Empty action is correct
		default:
			// Bad programmer
			panic("Unknown bulk queue")
		}

		queues = append(queues, &queueT{
			action: action,
		})
	}

	var itemCnt int
	var byteCnt int

	doFlush := func() error {

		for _, q := range queues {
			if q.pending > 0 {
				if err := b.flushQueue(ctx, w, q.head, q.pending, q.action); err != nil {
					return err
				}

				q.pending = 0
				q.head = nil
				q.tail = nil
			}
		}

		// Reset threshold counters
		itemCnt = 0
		byteCnt = 0

		stopTimer(timer)
		return nil
	}

LOOP:
	for err == nil {

		select {

		case blk := <-b.ch:

			queueIdx := kQueueBulk

			switch blk.action {
			case ActionRead:
				queueIdx = kQueueRead
			case ActionSearch:
				queueIdx = kQueueSearch
			default:
				if blk.opts.Refresh {
					queueIdx = kQueueRefresh
				}
			}

			blk.next = nil // safety check

			q := queues[queueIdx]
			oldTail := q.tail
			q.tail = blk

			if oldTail != nil {
				oldTail.next = blk
			}

			if q.head == nil {
				q.head = blk
			}

			q.pending += blk.buf.Len()

			// Update threshold counters
			itemCnt += 1
			byteCnt += blk.buf.Len()

			// Start timer on first queued item
			if itemCnt == 1 {
				timer.Reset(bopts.flushInterval)
			}

			// Threshold test, short circuit timer on pending count
			if itemCnt >= bopts.flushThresholdCnt || byteCnt >= bopts.flushThresholdSz {
				log.Trace().
					Str("mod", kModBulk).
					Int("itemCnt", itemCnt).
					Int("byteCnt", byteCnt).
					Msg("Flush on threshold")

				err = doFlush()
			}

		case <-timer.C:
			log.Trace().
				Str("mod", kModBulk).
				Int("itemCnt", itemCnt).
				Int("byteCnt", byteCnt).
				Msg("Flush on timer")
			err = doFlush()

		case <-ctx.Done():
			err = ctx.Err()
			break LOOP

		}

	}

	return err
}

func (b *Bulker) flushQueue(ctx context.Context, w *semaphore.Weighted, queue *bulkT, szPending int, action Action) error {
	start := time.Now()
	log.Trace().
		Str("mod", kModBulk).
		Int("szPending", szPending).
		Str("action", action.Str()).
		Msg("flushQueue Wait")

	if err := w.Acquire(ctx, 1); err != nil {
		return err
	}

	log.Trace().
		Str("mod", kModBulk).
		Dur("tdiff", time.Since(start)).
		Int("szPending", szPending).
		Str("action", action.Str()).
		Msg("flushQueue Acquired")

	go func() {
		start := time.Now()

		defer w.Release(1)

		var err error
		switch action {
		case ActionRead:
			err = b.flushRead(ctx, queue, szPending)
		case ActionSearch:
			err = b.flushSearch(ctx, queue, szPending)
		default:
			err = b.flushBulk(ctx, queue, szPending)
		}

		if err != nil {
			failQueue(queue, err)
		}

		log.Trace().
			Err(err).
			Str("mod", kModBulk).
			Int("szPending", szPending).
			Str("action", action.Str()).
			Dur("rtt", time.Since(start)).
			Msg("flushQueue Done")

	}()

	return nil
}

func failQueue(queue *bulkT, err error) {
	for n := queue; n != nil; {
		next := n.next // 'n' is invalid immediately on channel send
		n.ch <- respT{
			err: err,
		}
		n = next
	}
}

func (b *Bulker) parseOpts(opts ...Opt) optionsT {
	var opt optionsT
	for _, o := range opts {
		o(&opt)
	}
	return opt
}

func (b *Bulker) NewBlk(action Action, opts optionsT) *bulkT {
	blk := b.blkPool.Get().(*bulkT)
	blk.action = action
	blk.opts = opts
	return blk
}

func (b *Bulker) FreeBlk(blk *bulkT) {
	blk.reset()
	b.blkPool.Put(blk)
}

<<<<<<< HEAD
func (b *Bulker) waitBulkAction(ctx context.Context, action Action, index, id string, body []byte, opts ...Opt) (*BulkIndexerResponseItem, error) {
	opt := b.parseOpts(opts...)

	blk := b.GetBlk(action, opt)

	// Serialize request
	const kSlop = 64
	blk.data.Grow(len(body) + kSlop)

	if err := b.writeBulkMeta(&blk.data, action, index, id, opt); err != nil {
		return nil, err
	}

	if err := b.writeBulkBody(&blk.data, body); err != nil {
		return nil, err
	}

	// Dispatch and wait for response
	resp := b.dispatch(ctx, blk)
	if resp.err != nil {
		return nil, resp.err
	}
	b.PutBlk(blk)

	r := resp.data.(*BulkIndexerResponseItem)
	return r, nil
}

func (b *Bulker) Read(ctx context.Context, index, id string, opts ...Opt) ([]byte, error) {
	opt := b.parseOpts(opts...)

	blk := b.GetBlk(ActionRead, opt)

	// Serialize request
	const kSlop = 64
	blk.data.Grow(kSlop)

	if err := b.writeMget(&blk.data, index, id); err != nil {
		return nil, err
	}

	// Process response
	resp := b.dispatch(ctx, blk)
	if resp.err != nil {
		return nil, resp.err
	}
	b.PutBlk(blk)

	// Interpret response, looking for generated id
	r := resp.data.(*MgetResponseItem)
	return r.Source, nil
}

func (b *Bulker) Search(ctx context.Context, index []string, body []byte, opts ...Opt) (*es.ResultT, error) {
	opt := b.parseOpts(opts...)

	blk := b.GetBlk(ActionSearch, opt)

	// Serialize request
	const kSlop = 64
	blk.data.Grow(len(body) + kSlop)

	if err := b.writeMsearchMeta(&blk.data, index); err != nil {
		return nil, err
	}

	if err := b.writeMsearchBody(&blk.data, body); err != nil {
		return nil, err
	}

	// Process response
	resp := b.dispatch(ctx, blk)
	if resp.err != nil {
		return nil, resp.err
	}
	b.PutBlk(blk)

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

func (b *Bulker) validateIndex(index string) error {
	// TODO: index
	return nil
}

func (b *Bulker) validateIndices(indices []string) error {
	for _, i := range indices {
		if err := b.validateIndex(i); err != nil {
			return err
		}
	}
	return nil
}

func (b *Bulker) validateMeta(index, id string) error {
	// TODO: validate id and index; not quotes anyhow
	return nil
}

// TODO: Fail on non-escaped line feeds
func (b *Bulker) validateBody(body []byte) error {
	if !json.Valid(body) {
		return es.ErrInvalidBody
	}

	return nil
}

func (b *Bulker) dispatch(ctx context.Context, blk *bulkT) respT {
	start := time.Now()

	// Dispatch to bulk Run loop
	select {
	case b.ch <- blk:
	case <-ctx.Done():
		log.Error().
			Err(ctx.Err()).
			Str("mod", kModBulk).
			Str("action", blk.action.Str()).
			Bool("refresh", blk.opts.Refresh).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch abort queue")
		return respT{err: ctx.Err()}
	}

	// Wait for response
	select {
	case resp := <-blk.ch:
		log.Trace().
			Str("mod", kModBulk).
			Str("action", blk.action.Str()).
			Bool("refresh", blk.opts.Refresh).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch OK")

		return resp
	case <-ctx.Done():
		log.Error().
			Err(ctx.Err()).
			Str("mod", kModBulk).
			Str("action", blk.action.Str()).
			Bool("refresh", blk.opts.Refresh).
			Dur("rtt", time.Since(start)).
			Msg("Dispatch abort response")
	}

	return respT{err: ctx.Err()}
}

type UpdateFields map[string]interface{}

func (u UpdateFields) Marshal() ([]byte, error) {
	doc := struct {
		Doc map[string]interface{} `json:"doc"`
	}{
		u,
	}

	return json.Marshal(doc)
}
