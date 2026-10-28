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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/vbauerster/mpb/v8"
)

// initialHashTimeout limits one hash query of the first pass over the
// blocks. A worker already holds its connection when the query starts, so
// the limit counts only the query.
const initialHashTimeout = 60 * time.Second

// blockHashers hash the blocks of a list on every node of a table-diff.
// Every worker has its own connection and keeps it until the list ends. The
// workers of a node share one cursor over the list, so every node reads the
// list at its own pace. The first error stops the list and cancels the
// queries that still run, so all workers end at once.
type blockHashers struct {
	t      *TableDiffTask
	ctx    context.Context
	cancel context.CancelFunc
	list   *slicer.BlockList
	bar    *mpb.Bar
	onErr  func()
	wg     sync.WaitGroup

	mu      sync.Mutex
	cursors map[string]*slicer.Cursor
	results map[int]RangeResults
}

// newBlockHashers returns hashers with no workers. onErr is called after a
// hash fails, so the producer of the list can stop.
func newBlockHashers(ctx context.Context, t *TableDiffTask, list *slicer.BlockList, bar *mpb.Bar, onErr func()) *blockHashers {
	ctx, cancel := context.WithCancel(ctx)
	return &blockHashers{
		t:       t,
		ctx:     ctx,
		cancel:  cancel,
		list:    list,
		bar:     bar,
		onErr:   onErr,
		cursors: make(map[string]*slicer.Cursor),
		results: make(map[int]RangeResults),
	}
}

// addConn starts one hash worker for node on conn. The worker gives conn
// back to its pool when it ends. addConn can be called while other workers
// run, but not after wait.
func (h *blockHashers) addConn(node string, conn *pgxpool.Conn) {
	h.mu.Lock()
	cur, ok := h.cursors[node]
	if !ok {
		cur = h.list.NewCursor()
		h.cursors[node] = cur
	}
	h.mu.Unlock()

	h.wg.Add(1)
	go h.work(node, conn, cur)
}

func (h *blockHashers) work(node string, conn *pgxpool.Conn, cur *slicer.Cursor) {
	defer h.wg.Done()
	defer conn.Release()
	for {
		idx, b, ok := cur.Next()
		if !ok {
			return
		}
		r := h.t.rangeFromBlock(b)
		qctx, cancel := context.WithTimeout(h.ctx, initialHashTimeout)
		hash, err := h.t.hashRangeOn(qctx, conn, node, r)
		cancel()
		if err != nil {
			// Only the first error is kept, so the errors of the queries
			// that the cancel below ends do not hide it.
			h.t.recordError(fmt.Errorf("initial hash failed for node %s range %v-%v: %w", node, r.Start, r.End, err))
			h.list.Stop()
			h.cancel()
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
	h.cancel()
	return h.results
}
