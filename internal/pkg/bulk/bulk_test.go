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
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
)

// TODO:
// specify id
// specify illegal id
// no body
// bad body
// bad index
// WithREfresh() options
// cancel ctx works

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

/*
This should be an integration test
// Test that specified ID is passed through on request
func TestBulkSpecifyId(t *testing.T) {

	stub := &stubTransport{func(req *http.Request) (*http.Response, error) {
		return nil, nil
	}}

	bulker := NewBulker(stub)

	id, err :=
}
*/

// API should exit quickly if cancelled.
// Note: In the real world, the transaction may already be in flight,
// cancelling a call does not mean the transaction did not occur.
func TestCancelCtx(t *testing.T) {

	stubCtx, stubCancel := context.WithCancel(context.Background())
	defer stubCancel()

	stub := &stubTransport{func(req *http.Request) (*http.Response, error) {
		// Stall on transport; shouldn't affect coming out of the routine.
		<-stubCtx.Done() // Do not return until test exits
		return nil, nil
	}}

	ctx, cancelF := context.WithCancel(context.Background())

	id := "notempty"
	var err error

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		bulker := NewBulker(stub)
		bulker.Run(ctx)
		id, err = bulker.Create(ctx, "testidx", "", []byte(`{"hey":"now"}`))
	}()

	time.Sleep(time.Millisecond)
	cancelF()

	wg.Wait()

	if id != "" {
		t.Error("Expected empty id on context cancel:", id)
	}

	if err != context.Canceled {
		t.Error("Expected context cancel err: ", err)
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

func BenchmarkMockBulk_64(b *testing.B) { benchmarkMockBulk(64, b) }

//func BenchmarkMockBulk_32768(b *testing.B)    { benchmarkMockBulk(32768, b) }
