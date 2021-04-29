// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TODO:
// WithREfresh() options
// Delete not found?

type stubTransport struct {
	cb func(*http.Request) (*http.Response, error)
}

func (s *stubTransport) Perform(req *http.Request) (*http.Response, error) {
	return s.cb(req)
}

type mockBulkTransport struct {
	b *testing.B
}

// Parse request as ndjson, mock responses
func (m *mockBulkTransport) Perform(req *http.Request) (*http.Response, error) {
	log.Info().Msg("perform")
	type shardT struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Failed     int `json:"failed"`
	}

	type innerT struct {
		Index       string `json:"_index"`
		Type        string `json:"_type"`
		Id          string `json:"_id"`
		Version     int    `json:"_version"`
		Result      string `json:"result"`
		Shards      shardT `json:"_shards"`
		Status      int    `json:"status"`
		SeqNo       int    `json:"_seq_no"`
		PrimaryTerm int    `json:"_primary_term"`
	}

	type frameT struct {
		Index  *innerT `json:"index,omitempty"`
		Delete *innerT `json:"delete,omitempty"`
		Create *innerT `json:"create,omitempty"`
		Update *innerT `json:"update,omitempty"`
	}

	type emptyT struct {
	}

	var body bytes.Buffer

	// Write framing
	body.WriteString(`{"items": [`)

	cnt := 0

	skip := false
	decoder := json.NewDecoder(req.Body)
	for decoder.More() {
		if skip {
			skip = false
			var e emptyT
			if err := decoder.Decode(&e); err != nil {
				return nil, err
			}
		} else {
			var frame frameT
			if err := decoder.Decode(&frame); err != nil {
				return nil, err
			}

			// Which op
			var op *innerT
			switch {
			case frame.Index != nil:
				op = frame.Index
				skip = true
			case frame.Delete != nil:
				op = frame.Delete
			case frame.Create != nil:
				op = frame.Create
				skip = true
			case frame.Update != nil:
				op = frame.Update
				skip = true
			default:
				return nil, errors.New("Unknown op")
			}

			type shardT struct {
				Total      int `json:"total"`
				Successful int `json:"successful"`
				Failed     int `json:"failed"`
			}

			op.Type = "_doc"

			//  mock the following
			op.Version = 1
			op.Shards.Total = 1
			op.Shards.Successful = 1
			op.Status = 200
			op.PrimaryTerm = 1

			data, err := json.Marshal(frame)

			if err != nil {
				return nil, err
			}

			body.Write(data)
			body.WriteByte(',')

			cnt += 1
		}
	}

	if cnt > 0 {
		body.Truncate(body.Len() - 1)
	}

	// Write trailer
	body.WriteString(`], "took": 1, "errors": false}`)

	//ioutil.WriteFile("/tmp/dat1", body.Bytes(), 0644)

	resp := &http.Response{
		Request:    req,
		StatusCode: 200,
		Status:     "200 OK",
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Body:       ioutil.NopCloser(&body),
	}

	return resp, nil
}

// API should exit quickly if cancelled.
// Note: In the real world, the transaction may already be in flight,
// cancelling a call does not mean the transaction did not occur.
func TestCancelCtx(t *testing.T) {

	// create a bulker, but don't bother running it
	bulker := NewBulker(nil)

	tests := []struct {
		name string
		test func(t *testing.T, ctx context.Context)
	}{
		{
			"create",
			func(t *testing.T, ctx context.Context) {
				id, err := bulker.Create(ctx, "testidx", "", []byte(`{"hey":"now"}`))

				if id != "" {
					t.Error("Expected empty id on context cancel:", id)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"read",
			func(t *testing.T, ctx context.Context) {
				data, err := bulker.Read(ctx, "testidx", "11")

				if data != nil {
					t.Error("Expected empty data on context cancel:", data)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"update",
			func(t *testing.T, ctx context.Context) {
				err := bulker.Update(ctx, "testidx", "11", []byte(`{"now":"hey"}`))

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"delete",
			func(t *testing.T, ctx context.Context) {
				err := bulker.Delete(ctx, "testidx", "11")

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"index",
			func(t *testing.T, ctx context.Context) {
				id, err := bulker.Index(ctx, "testidx", "", []byte(`{"hey":"now"}`))

				if id != "" {
					t.Error("Expected empty id on context cancel:", id)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"search",
			func(t *testing.T, ctx context.Context) {
				res, err := bulker.Search(ctx, "testidx", []byte(`{"hey":"now"}`))

				if res != nil {
					t.Error("Expected empty result on context cancel:", res)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"mcreate",
			func(t *testing.T, ctx context.Context) {
				res, err := bulker.MCreate(ctx, []MultiOp{{Index: "testidx", Body: []byte(`{"hey":"now"}`)}})

				if res != nil {
					t.Error("Expected empty result on context cancel:", res)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"mindex",
			func(t *testing.T, ctx context.Context) {
				res, err := bulker.MIndex(ctx, []MultiOp{{Index: "testidx", Body: []byte(`{"hey":"now"}`)}})

				if res != nil {
					t.Error("Expected empty result on context cancel:", res)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"mupdate",
			func(t *testing.T, ctx context.Context) {
				res, err := bulker.MUpdate(ctx, []MultiOp{{Index: "testidx", Id: "umm", Body: []byte(`{"hey":"now"}`)}})

				if res != nil {
					t.Error("Expected empty result on context cancel:", res)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
		{
			"mdelete",
			func(t *testing.T, ctx context.Context) {
				res, err := bulker.MDelete(ctx, []MultiOp{{Index: "testidx", Id: "myid"}})

				if res != nil {
					t.Error("Expected empty result on context cancel:", res)
				}

				if err != context.Canceled {
					t.Error("Expected context cancel err: ", err)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			ctx, cancelF := context.WithCancel(context.Background())

			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()

				test.test(t, ctx)
			}()

			time.Sleep(time.Millisecond)
			cancelF()

			wg.Wait()
		})
	}
}

func benchmarkMockBulk(n int, b *testing.B) {
	b.ReportAllocs()
	defer (QuietLogger())()

	mock := &mockBulkTransport{}

	ctx, cancelF := context.WithCancel(context.Background())
	defer cancelF()

	bulker := NewBulker(mock)

	var waitBulker sync.WaitGroup
	waitBulker.Add(1)
	go func() {
		defer waitBulker.Done()
		if err := bulker.Run(ctx, WithFlushThresholdCount(n)); err != context.Canceled {
			b.Fatal(err)
		}
	}()

	fieldUpdate := UpdateFields{"kwval": "funkycoldmedina"}
	fieldData, err := fieldUpdate.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	index := "fakeIndex"

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

				// Index
				_, err = bulker.Index(ctx, index, id, sampleData)
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
					log.Info().Str("index", index).Str("id", id).Msg("delete fail")
					b.Fatal(err)
				}
			}
		}()
	}

	wait.Wait()
	cancelF()
	waitBulker.Wait()
}

func BenchmarkMockBulk(b *testing.B) {

	benchmarks := []int{8, 64, 512, 4096, 32768}

	for _, n := range benchmarks {

		bindFunc := func(n int) func(b *testing.B) {
			return func(b *testing.B) {
				benchmarkMockBulk(n, b)
			}
		}
		b.Run(strconv.Itoa(n), bindFunc(n))
	}
}
