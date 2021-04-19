// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package bulk

// bulkT is generally allocated in the bulk engines's 'blkPool'
// However, the multiOp API's will allocate directly in large blocks.

type bulkT struct {
	idx    int        // idx of originating requeset, used in mulitOp
	action Action     // requested actions
	ch     chan respT // response channel, caller is waiting synchronously
	buf    Buf        // json payload to be sent to elastic
	opts   optionsT   // various options
	next   *bulkT     // pointer to next bulkT, used for fast internal queueing
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
