// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

// +build integration

package bulk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/elastic/fleet-server/v7/internal/pkg/es"

	"github.com/Pallinder/go-randomdata"
	"github.com/google/go-cmp/cmp"
)

const testPolicy = `{
	"properties": {
		"intval": {
			"type": "integer"
		},
		"objval": {
			"type": "object"
		},
		"boolval": {
			"type": "boolean"
		},
		"kwval": {
			"type": "keyword"
		},
		"binaryval": {
			"type": "binary"
		},
		"dateval": {
			"type": "date"
		}		
	}
}`

type subT struct {
	SubString string `json:"substring"`
}

type testT struct {
	IntVal    int    `json:"intval"`
	ObjVal    subT   `json:"objval"`
	BoolVal   bool   `json:"boolval"`
	KWVal     string `json:"kwval"`
	BinaryVal string `json:"binaryval"`
	DateVal   string `json:"dateval"`
}

func NewRandomSample() testT {

	return testT{
		IntVal:    int(rand.Int31()),
		ObjVal:    subT{SubString: randomdata.SillyName()},
		BoolVal:   (rand.Intn(1) == 1),
		KWVal:     randomdata.SillyName(),
		BinaryVal: base64.StdEncoding.EncodeToString([]byte(randomdata.SillyName())),
		DateVal:   time.Now().Format(time.RFC3339),
	}
}

func (ts testT) marshal(t *testing.T) []byte {
	data, err := json.Marshal(&ts)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func (ts *testT) read(t *testing.T, bulker Bulk, ctx context.Context, index, id string) {
	data, err := bulker.Read(ctx, index, id)
	if err != nil {
		t.Fatal(err)
	}

	err = json.Unmarshal(data, ts)
	if err != nil {
		t.Fatal(err)
	}
}

// break into multiple tests. not one big crud
// no index
// specify id
// specify illegal id
// no boyd
// bad body
// bad index
// WithREfresh() options
// cancel ctx works

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

	err = bulker.Update(ctx, index, id, data)
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
