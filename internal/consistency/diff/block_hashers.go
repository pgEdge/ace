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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/vbauerster/mpb/v8"
)

// blockHashers hash the blocks of a list on every node of a table-diff.
// Every worker has its own connection and keeps it until the list ends, so
// the block timeout of a hash query counts only the query. The workers of a
// node share one cursor over the list, so every node reads the list at its
// own pace. The first error stops the list and cancels the queries that
// still run, so all workers end at once.
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
	// rowsByNode sums the rows that the hash queries of each node counted.
	rowsByNode map[string]int64
}

// newBlockHashers returns hashers with no workers. onErr is called after a
// hash fails, so the producer of the list can stop.
func newBlockHashers(ctx context.Context, t *TableDiffTask, list *slicer.BlockList, bar *mpb.Bar, onErr func()) *blockHashers {
	ctx, cancel := context.WithCancel(ctx)
	return &blockHashers{
		t:          t,
		ctx:        ctx,
		cancel:     cancel,
		list:       list,
		bar:        bar,
		onErr:      onErr,
		cursors:    make(map[string]*slicer.Cursor),
		results:    make(map[int]RangeResults),
		rowsByNode: make(map[string]int64),
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
		var (
			hash string
			rows int64
		)
		err := h.t.withBlockLimit(h.ctx, func(ctx context.Context) error {
			var err error
			hash, rows, err = h.t.hashRangeOn(ctx, conn, node, r)
			return err
		})
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
		h.rowsByNode[node] += rows
		h.mu.Unlock()
		h.bar.Increment()
	}
}

// rows returns the number of rows that the hash queries of node counted.
// Call it after wait.
func (h *blockHashers) rows(node string) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rowsByNode[node]
}

// wait waits for all workers and returns the hashes, keyed by the index of
// the block in the list and then by node. The caller must close or stop the
// list first.
func (h *blockHashers) wait() map[int]RangeResults {
	h.wg.Wait()
	h.cancel()
	return h.results
}
