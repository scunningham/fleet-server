// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"context"
	"strconv"
	"testing"
)

const payload = `{"_id" : "1", "_index" : "test"}`

// Test throughput of creating multiOps
func BenchmarkMultiUpdateMock(b *testing.B) {
	defer (QuietLogger())()

	// Allocate, but don't run.  Stub the client.
	bulker := NewBulker(nil)
	defer close(bulker.ch)

	go func() {
		for v := range bulker.ch {
			v.ch <- respT{nil, v.idx, nil}
		}
	}()

	body := []byte(payload)

	benchmarks := []int{1, 8, 64, 4096, 32768, 131072}

	// Create the samples outside the loop to avoid accounting
	max := 0
	for _, v := range benchmarks {
		if max < v {
			max = v
		}
	}

	// Create the ops
	ops := make([]MultiOp, 0, max)
	for i := 0; i < max; i++ {
		ops = append(ops, MultiOp{
			Id:    "abba",
			Index: "bogus",
			Body:  body,
		})
	}

	for _, n := range benchmarks {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			ctx := context.Background()
			for i := 0; i < b.N; i++ {
				bulker.MUpdate(ctx, ops[:n])
			}
		})
	}

}
