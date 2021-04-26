// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import (
	"encoding/json"
	"errors"
	"github.com/elastic/fleet-server/v7/internal/pkg/es"
)

type BulkIndexerResponse struct {
	Took      int                       `json:"took"`
	HasErrors bool                      `json:"errors"`
	Items     []BulkIndexerResponseItem `json:"items,omitempty"`
}

// Comment out fields we don't use; no point decoding.
type BulkIndexerResponseItem struct {
	//	Index      string `json:"_index"`
	DocumentID string `json:"_id"`
	//	Version    int64  `json:"_version"`
	//	Result     string `json:"result"`
	Status int `json:"status"`
	//	SeqNo      int64  `json:"_seq_no"`
	//	PrimTerm   int64  `json:"_primary_term"`

	//	Shards struct {
	//		Total      int `json:"total"`
	//		Successful int `json:"successful"`
	//		Failed     int `json:"failed"`
	//	} `json:"_shards"`

	Error es.ErrorT `json:"error,omitempty"`
}

// Elastic returns shape: map[action]BulkIndexerResponseItem.
// Avoid allocating map; very expensive memory alloc for this
// case when there is only one legal key.
func (bi *BulkIndexerResponseItem) UnmarshalJSON(data []byte) error {
	type innerT struct {
		DocumentID string    `json:"_id"`
		Status     int       `json:"status"`
		Error      es.ErrorT `json:"error,omitempty"`
	}

	var w struct {
		Index  *innerT `json:"index"`
		Delete *innerT `json:"delete"`
		Create *innerT `json:"create"`
		Update *innerT `json:"update"`
	}

	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}

	var v *innerT
	switch {
	case w.Index != nil:
		v = w.Index
	case w.Delete != nil:
		v = w.Delete
	case w.Create != nil:
		v = w.Create
	case w.Update != nil:
		v = w.Update
	default:
		return errors.New("unknown bulk action")
	}

	*bi = BulkIndexerResponseItem{
		DocumentID: v.DocumentID,
		Status:     v.Status,
		Error:      v.Error,
	}
	return nil
}

type MgetResponse struct {
	Items []MgetResponseItem `json:"docs"`
}

// Comment out fields we don't use; no point decoding.
type MgetResponseItem struct {
	//	Index      string          `json:"_index"`
	//	Type       string          `json:"_type"`
	//	DocumentID string          `json:"_id"`
	//	Version    int64           `json:"_version"`
	//	SeqNo      int64           `json:"_seq_no"`
	//	PrimTerm   int64           `json:"_primary_term"`
	Found bool `json:"found"`
	//	Routing    string          `json:"_routing"`
	Source json.RawMessage `json:"_source"`
	//	Fields     json.RawMessage `json:"_fields"`
}

func (i *MgetResponseItem) deriveError() error {
	if !i.Found {
		return es.ErrElasticNotFound
	}
	return nil
}

type MsearchResponseItem struct {
	Status   int    `json:"status"`
	Took     uint64 `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Total      uint64 `json:"total"`
		Successful uint64 `json:"successful"`
		Skipped    uint64 `json:"skipped"`
		Failed     uint64 `json:"failed"`
	} `json:"_shards"`
	Hits         es.HitsT                  `json:"hits"`
	Aggregations map[string]es.Aggregation `json:"aggregations,omitempty"`

	Error es.ErrorT `json:"error,omitempty"`
}

type MsearchResponse struct {
	Responses []MsearchResponseItem `json:"responses"`
	Took      int                   `json:"took"`
}

func (b *BulkIndexerResponseItem) deriveError() error {
	return es.TranslateError(b.Status, b.Error)
}

func (b *MsearchResponseItem) deriveError() error {
	return es.TranslateError(b.Status, b.Error)
}
