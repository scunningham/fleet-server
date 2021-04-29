// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

// +build integration

package bulk

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/elastic/fleet-server/v7/internal/pkg/es"

	"github.com/google/go-cmp/cmp"
	"github.com/rs/zerolog/log"
)

func TestBulkCreate(t *testing.T) {
	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, t, testPolicy)

	sample := NewRandomSample()

	// Create
	id, err := bulker.Create(ctx, index, "", sample.marshal(t))
	if err != nil {
		t.Fatal(err)
	}

	// Read
	var dst testT
	dst.read(t, bulker, ctx, index, id)
	diff := cmp.Diff(sample, dst)
	if diff != "" {
		t.Fatal(diff)
	}
}

func TestBulkIndex(t *testing.T) {
	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, t, testPolicy)

	sample := NewRandomSample()

	// Index
	id, err := bulker.Index(ctx, index, "", sample.marshal(t))
	if err != nil {
		t.Fatal(err)
	}

	// Read
	var dst testT
	dst.read(t, bulker, ctx, index, id)
	diff := cmp.Diff(sample, dst)
	if diff != "" {
		t.Fatal(diff)
	}
}

func TestBulkUpdate(t *testing.T) {
	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, t, testPolicy)

	sample := NewRandomSample()

	// Create
	id, err := bulker.Create(ctx, index, "", sample.marshal(t))
	if err != nil {
		t.Fatal(err)
	}

	// Update
	nVal := "funkycoldmedina"
	fields := UpdateFields{"kwval": nVal}
	data, err := fields.Marshal()
	if err != nil {
		t.Fatal(err)
	}

	err = bulker.Update(ctx, index, id, data, WithRefresh())
	if err != nil {
		t.Fatal(err)
	}

	// Read again, validate update
	var dst2 testT
	dst2.read(t, bulker, ctx, index, id)

	sample.KWVal = nVal
	diff := cmp.Diff(sample, dst2)
	if diff != "" {
		t.Fatal(diff)
	}
}

func TestBulkSearch(t *testing.T) {
	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, t, testPolicy)

	sample := NewRandomSample()

	// Create
	_, err := bulker.Create(ctx, index, "", sample.marshal(t), WithRefresh())
	if err != nil {
		t.Fatal(err)
	}

	// Search
	dsl := fmt.Sprintf(`{"query": { "term": {"kwval": "%s"}}}`, sample.KWVal)

	res, err := bulker.Search(ctx, index, []byte(dsl))

	if err != nil {
		t.Fatal(err)
	}

	if res == nil {
		t.Fatal(nil)
	}

	if len(res.Hits) != 1 {
		t.Fatal(fmt.Sprintf("hit mismatch: %d", len(res.Hits)))
	}

	var dst3 testT
	if err = json.Unmarshal(res.Hits[0].Source, &dst3); err != nil {
		t.Fatal(err)
	}

	diff := cmp.Diff(sample, dst3)
	if diff != "" {
		t.Fatal(diff)
	}
}

func TestBulkDelete(t *testing.T) {
	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, t, testPolicy)

	sample := NewRandomSample()

	// Create
	id, err := bulker.Create(ctx, index, "", sample.marshal(t))
	if err != nil {
		t.Fatal(err)
	}

	// Delete
	err = bulker.Delete(ctx, index, id)
	if err != nil {
		t.Fatal(err)
	}

	data, err := bulker.Read(ctx, index, id)
	if err != es.ErrElasticNotFound || data != nil {
		t.Fatal(err)
	}
}

// This runs a series of CRUD operations through elastic.
// Not a particularly useful benchmark, but gives some idea of memory overhead.

func benchmarkCreate(n int, b *testing.B) {
	b.ReportAllocs()
	defer (QuietLogger())()

	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, b, testPolicy, WithFlushThresholdCount(n))

	var wait sync.WaitGroup
	wait.Add(n)
	for i := 0; i < n; i++ {

		go func() {
			defer wait.Done()

			sample := NewRandomSample()
			sampleData := sample.marshal(b)

			for j := 0; j < b.N; j++ {

				// Create
				_, err := bulker.Create(ctx, index, "", sampleData)
				if err != nil {
					b.Fatal(err)
				}
			}
		}()
	}

	wait.Wait()
}

func BenchmarkCreate(b *testing.B) {

	benchmarks := []int{1, 64, 8192, 16384, 32768, 65536}

	for _, n := range benchmarks {

		bindFunc := func(n int) func(b *testing.B) {
			return func(b *testing.B) {
				benchmarkCreate(n, b)
			}
		}
		b.Run(strconv.Itoa(n), bindFunc(n))
	}
}

// This runs a series of CRUD operations through elastic.
// Not a particularly useful benchmark, but gives some idea of memory overhead.

func benchmarkCRUD(n int, b *testing.B) {
	b.ReportAllocs()
	defer (QuietLogger())()

	ctx, cn := context.WithCancel(context.Background())
	defer cn()

	index, bulker := SetupIndexWithBulk(ctx, b, testPolicy, WithFlushThresholdCount(n))

	fieldUpdate := UpdateFields{"kwval": "funkycoldmedina"}
	fieldData, err := fieldUpdate.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	var wait sync.WaitGroup
	wait.Add(n)
	for i := 0; i < n; i++ {

		go func() {
			defer wait.Done()

			sample := NewRandomSample()
			sampleData := sample.marshal(b)

			for j := 0; j < b.N; j++ {

				// Create
				id, err := bulker.Create(ctx, index, "", sampleData)
				if err != nil {
					b.Fatal(err)
				}

				// Read
				_, err = bulker.Read(ctx, index, id)
				if err != nil {
					b.Fatal(err)
				}

				// Update
				err = bulker.Update(ctx, index, id, fieldData)
				if err != nil {
					b.Fatal(err)
				}

				// Delete
				err = bulker.Delete(ctx, index, id)
				if err != nil {
					log.Info().Str("index", index).Str("id", id).Msg("dlete fail")
					b.Fatal(err)
				}
			}
		}()
	}

	wait.Wait()
}

func BenchmarkCRUD(b *testing.B) {

	benchmarks := []int{1, 64, 8192, 16384, 32768, 65536}

	for _, n := range benchmarks {

		bindFunc := func(n int) func(b *testing.B) {
			return func(b *testing.B) {
				benchmarkCRUD(n, b)
			}
		}
		b.Run(strconv.Itoa(n), bindFunc(n))
	}
}
