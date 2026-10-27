// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2026, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package diff

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/vbauerster/mpb/v8"
)

// initialHashTimeout limits one hash query of the first pass over the
// blocks.
const initialHashTimeout = 60 * time.Second

// blockHashers hash the blocks of a list on every node of a table-diff. Each
// node has its own workers, and they share one cursor over the list, so
// every node reads the list at its own pace. The first error stops the list,
// so all workers end.
type blockHashers struct {
	t     *TableDiffTask
	ctx   context.Context
	list  *slicer.BlockList
	bar   *mpb.Bar
	onErr func()
	wg    sync.WaitGroup

	mu      sync.Mutex
	results map[int]RangeResults
}

// newBlockHashers returns hashers with no workers. onErr is called after a
// hash fails, so the producer of the list can stop.
func newBlockHashers(ctx context.Context, t *TableDiffTask, list *slicer.BlockList, bar *mpb.Bar, onErr func()) *blockHashers {
	return &blockHashers{
		t:       t,
		ctx:     ctx,
		list:    list,
		bar:     bar,
		onErr:   onErr,
		results: make(map[int]RangeResults),
	}
}

// addNode starts workers hash workers for node. They use the pool of the
// node.
func (h *blockHashers) addNode(node string, workers int) {
	cur := h.list.NewCursor()
	for i := 0; i < workers; i++ {
		h.wg.Add(1)
		go h.work(node, cur)
	}
}

func (h *blockHashers) work(node string, cur *slicer.Cursor) {
	defer h.wg.Done()
	for {
		idx, b, ok := cur.Next()
		if !ok {
			return
		}
		r := h.t.rangeFromBlock(b)
		qctx, cancel := context.WithTimeout(h.ctx, initialHashTimeout)
		hash, err := h.t.hashRange(qctx, node, r)
		cancel()
		if err != nil {
			h.t.recordError(fmt.Errorf("initial hash failed for node %s range %v-%v: %w", node, r.Start, r.End, err))
			h.list.Stop()
			h.onErr()
			return
		}
		h.mu.Lock()
		if h.results[idx] == nil {
			h.results[idx] = make(RangeResults)
		}
		h.results[idx][node] = HashResult{hash: hash}
		h.mu.Unlock()
		h.bar.Increment()
	}
}

// wait waits for all workers and returns the hashes, keyed by the index of
// the block in the list and then by node. The caller must close or stop the
// list first.
func (h *blockHashers) wait() map[int]RangeResults {
	h.wg.Wait()
	return h.results
}
