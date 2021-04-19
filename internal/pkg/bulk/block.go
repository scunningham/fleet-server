// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

import ()

type bulkT struct {
	idx    int
	action Action
	ch     chan respT
	buf    Buf
	opts   optionsT
	next   *bulkT
}

func (blk *bulkT) reset() {
	blk.action = ""
	blk.buf.Reset()
	blk.opts = optionsT{}
	blk.next = nil
}

func newBlk() interface{} {
	return &bulkT{
		ch: make(chan respT, 1),
	}
}

type respT struct {
	err  error
	idx  int
	data interface{}
}
