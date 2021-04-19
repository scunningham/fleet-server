// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
)

const payload = `{"_id" : "1", "_index" : "test"}`

// Test throughput of creating multiOps
func benchmarkUpdateN(n int, b *testing.B) {

	b.ReportAllocs()

	l := zerolog.GlobalLevel()
	defer zerolog.SetGlobalLevel(l)

	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	// Allocate, but don't run.  Stub the client.
	bulker := NewBulker(nil)
	defer close(bulker.ch)

	go func() {
		for v := range bulker.ch {
			v.ch <- respT{nil, v.idx, nil}
		}
	}()

	body := []byte(payload)

	// Create the ops

	ops := make([]MultiOp, 0, n)
	for i := 0; i < n; i++ {
		ops = append(ops, MultiOp{
			Id:    "abba",
			Index: "bogus",
			Body:  body,
		})
	}

	ctx := context.Background()

	for i := 0; i < b.N; i++ {
		bulker.MUpdate(ctx, ops)
	}
}

func BenchmarkUpdate8(b *testing.B)    { benchmarkUpdateN(8, b) }
func BenchmarkUpdate64(b *testing.B)   { benchmarkUpdateN(8, b) }
func BenchmarkUpdate512(b *testing.B)  { benchmarkUpdateN(8, b) }
func BenchmarkUpdate2K(b *testing.B)   { benchmarkUpdateN(2048, b) }
func BenchmarkUpdate8K(b *testing.B)   { benchmarkUpdateN(8192, b) }
func BenchmarkUpdate32K(b *testing.B)  { benchmarkUpdateN(32768, b) }
func BenchmarkUpdate128K(b *testing.B) { benchmarkUpdateN(131072, b) }
