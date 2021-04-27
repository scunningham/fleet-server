// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package checkin

import (
	"context"
	"testing"

	tst "github.com/elastic/fleet-server/v7/internal/pkg/testing"
	"github.com/rs/xid"
	"github.com/rs/zerolog"
)

func benchmarkBulk(n int, flush bool, b *testing.B) {
	b.ReportAllocs()

	l := zerolog.GlobalLevel()
	defer zerolog.SetGlobalLevel(l)

	zerolog.SetGlobalLevel(zerolog.ErrorLevel)

	var mockBulk tst.MockBulk

	bc := NewBulkCheckin(mockBulk)

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		id := xid.New().String()
		ids = append(ids, id)
	}

	for i := 0; i < b.N; i++ {

		for _, id := range ids {
			err := bc.CheckIn(id, nil, nil)
			if err != nil {
				b.Fatal(err)
			}
		}

		if flush {
			err := bc.flush(context.Background())
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkBulk_1(b *testing.B)      { benchmarkBulk(1, false, b) }
func BenchmarkBulk_64(b *testing.B)     { benchmarkBulk(64, false, b) }
func BenchmarkBulk_8192(b *testing.B)   { benchmarkBulk(8192, false, b) }
func BenchmarkBulk_37268(b *testing.B)  { benchmarkBulk(37268, false, b) }
func BenchmarkBulk_131072(b *testing.B) { benchmarkBulk(131072, false, b) }
func BenchmarkBulk_262144(b *testing.B) { benchmarkBulk(262144, false, b) }

func BenchmarkBulkFlush_1(b *testing.B)      { benchmarkBulk(1, true, b) }
func BenchmarkBulkFlush_64(b *testing.B)     { benchmarkBulk(64, true, b) }
func BenchmarkBulkFlush_8192(b *testing.B)   { benchmarkBulk(8192, true, b) }
func BenchmarkBulkFlush_37268(b *testing.B)  { benchmarkBulk(37268, true, b) }
func BenchmarkBulkFlush_131072(b *testing.B) { benchmarkBulk(131072, true, b) }
func BenchmarkBulkFlush_262144(b *testing.B) { benchmarkBulk(262144, true, b) }
