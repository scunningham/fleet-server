package bulk

import (
	"testing"
	"context"
	"strconv"

	"github.com/rs/zerolog"
)

const payload = `{"_id" : "1", "_index" : "test"}`


func benchmarkUpdateN(n int, b *testing.B) {

	b.ReportAllocs()

	l := zerolog.GlobalLevel()
	defer zerolog.SetGlobalLevel(l)

	zerolog.SetGlobalLevel(zerolog.InfoLevel)


	bulker := NewBulker(nil)
	defer close(bulker.ch)

	go func() {
		for v := range bulker.ch {
			bulker.bufPool.Put(v.buf)
			v.buf = nil
			v.ch <- respT{nil, nil}
		}
	}()

	// Create the ops

	var ops []MultiOp
	for i := 0; i < n; i++ {
		ops = append(ops, MultiOp{
			Id: strconv.Itoa(i),
			Index: "bogus",
			Body: []byte(payload),
		})
	}


	ctx := context.Background()

    for i := 0; i < b.N; i++ {
        bulker.MUpdate(ctx, ops)
    }
}

func BenchmarkUpdate8(b *testing.B) { benchmarkUpdateN(8, b) }
func BenchmarkUpdate64(b *testing.B) { benchmarkUpdateN(8, b) }
func BenchmarkUpdate512(b *testing.B) { benchmarkUpdateN(8, b) }
func BenchmarkUpdate2K(b *testing.B) { benchmarkUpdateN(2048, b) }
func BenchmarkUpdate8K(b *testing.B) { benchmarkUpdateN(8192, b) }
func BenchmarkUpdate32K(b *testing.B) { benchmarkUpdateN(32768, b) }
func BenchmarkUpdate128K(b *testing.B) { benchmarkUpdateN(131072, b) }






/*
pkg: github.com/elastic/fleet-server/v7/internal/pkg/bulk
cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
BenchmarkUpdate8-16       	   82266	     15171 ns/op	    8075 B/op	     101 allocs/op
BenchmarkUpdate64-16      	   76138	     15084 ns/op	    8075 B/op	     101 allocs/op
BenchmarkUpdate512-16     	   84952	     15172 ns/op	    8075 B/op	     101 allocs/op
BenchmarkUpdate2K-16      	       2	 724808654 ns/op	427279684 B/op	 4208655 allocs/op
BenchmarkUpdate8K-16      	       1	12975826377 ns/op	6449473032 B/op	67190884 allocs/op
BenchmarkUpdate32K-16     	       1	234330489923 ns/op	99886941032 B/op	1074070135 allocs/op
panic: reflect.Select: too many cases (max 65536)
*/