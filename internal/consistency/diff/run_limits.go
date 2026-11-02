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
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
)

// defaultBlockTimeout is the block timeout, in seconds, when neither the
// task nor ace.yaml sets one.
const defaultBlockTimeout = 60

// blockTimeoutError means that one query on one block ran longer than the
// block timeout.
type blockTimeoutError struct {
	Limit time.Duration
}

func (e *blockTimeoutError) Error() string {
	return fmt.Sprintf("block timeout of %s reached", e.Limit)
}

// runTimeoutError means that a table-diff run ran longer than the run
// timeout.
type runTimeoutError struct {
	Limit time.Duration
}

func (e *runTimeoutError) Error() string {
	return fmt.Sprintf("run timeout of %s reached", e.Limit)
}

func isBlockTimeout(err error) bool {
	var bt *blockTimeoutError
	return errors.As(err, &bt)
}

// stopCause returns why ctx is done, or nil if ctx is nil or still running.
func stopCause(ctx context.Context) error {
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	return context.Cause(ctx)
}

// maxLimitSeconds is the largest time limit, in seconds, that fits in a
// time.Duration.
const maxLimitSeconds = int64(math.MaxInt64 / int64(time.Second))

// timeLimit returns the limit in seconds as a duration. set is the value
// from the task, or nil if the task does not set one; then fromConfig is
// used.
func timeLimit(name string, set *int, fromConfig int) (time.Duration, error) {
	sec := fromConfig
	if set != nil {
		sec = *set
	}
	if sec < 0 {
		return 0, fmt.Errorf("%s must be >= 0, got %d", name, sec)
	}
	if int64(sec) > maxLimitSeconds {
		return 0, fmt.Errorf("%s must be <= %d, got %d", name, maxLimitSeconds, sec)
	}
	return time.Duration(sec) * time.Second, nil
}

// timeLimits returns the block timeout and the run timeout of the task. A
// value set on the task wins over ace.yaml. Zero means no limit.
func (t *TableDiffTask) timeLimits() (block, run time.Duration, err error) {
	cfgBlock, cfgRun := defaultBlockTimeout, 0
	if cfg := config.Get(); cfg != nil {
		if cfg.TableDiff.BlockTimeout != nil {
			cfgBlock = *cfg.TableDiff.BlockTimeout
		}
		cfgRun = cfg.TableDiff.RunTimeout
	}
	if block, err = timeLimit("block_timeout", t.BlockTimeout, cfgBlock); err != nil {
		return 0, 0, err
	}
	if run, err = timeLimit("run_timeout", t.RunTimeout, cfgRun); err != nil {
		return 0, 0, err
	}
	return block, run, nil
}

// newRunContext returns the context of one table-diff run and the function
// that stops it with a cause. All workers of the run use this context, so
// the first error, the run timeout or the end of the parent context stops
// all of them. A zero limit means no run timeout.
func newRunContext(parent context.Context, limit time.Duration) (context.Context, context.CancelCauseFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, stop := context.WithCancelCause(parent)
	if limit <= 0 {
		return ctx, stop
	}
	timed, cancelTimer := context.WithTimeoutCause(ctx, limit, &runTimeoutError{Limit: limit})
	return timed, func(cause error) {
		stop(cause)
		cancelTimer()
	}
}

// blockContext returns the context of one operation on a block: ctx with
// the block timeout, if there is one.
func (t *TableDiffTask) blockContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if t.blockTimeout > 0 {
		return context.WithTimeoutCause(ctx, t.blockTimeout, &blockTimeoutError{Limit: t.blockTimeout})
	}
	return context.WithCancel(ctx)
}

// blockError returns the blockTimeoutError when err comes from the end of
// the block timeout of opCtx, and err otherwise. ctx is the context that
// opCtx was made from.
func blockError(ctx, opCtx context.Context, err error) error {
	if err != nil && ctx.Err() == nil && opCtx.Err() != nil {
		return context.Cause(opCtx)
	}
	return err
}

// withBlockLimit runs fn as one operation on a block under the block
// timeout. The caller already holds the connection for the operation, so
// only the operation counts. If the operation runs longer than the block
// timeout, withBlockLimit returns a blockTimeoutError.
func (t *TableDiffTask) withBlockLimit(ctx context.Context, fn func(ctx context.Context) error) error {
	opCtx, cancel := t.blockContext(ctx)
	defer cancel()
	return blockError(ctx, opCtx, fn(opCtx))
}

// withBlockConn runs fn as one operation on a block of node. It gets a
// connection from the pool of the node first and starts the block timeout
// only then, so the wait for a free connection does not count. If the
// operation runs longer than the block timeout, withBlockConn returns a
// blockTimeoutError.
func (t *TableDiffTask) withBlockConn(ctx context.Context, node string, fn func(ctx context.Context, conn *pgxpool.Conn) error) error {
	pool, ok := t.Pools[node]
	if !ok {
		return fmt.Errorf("no pool for node %s", node)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()

	return t.withBlockLimit(ctx, func(ctx context.Context) error {
		return fn(ctx, conn)
	})
}

// blockQuerier runs each query as one operation on a block of the node of
// pool. It gets a connection from the pool first and starts the block
// timeout only then, so the wait for a free connection does not count. The
// connection goes back to the pool when the rows are closed or all read.
type blockQuerier struct {
	t    *TableDiffTask
	pool *pgxpool.Pool
}

// Query gets a connection from the pool, starts the block timeout and runs
// sql on the connection. The rows hold the connection until they are closed.
func (q *blockQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	conn, err := q.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	opCtx, cancel := q.t.blockContext(ctx)
	rows, err := conn.Query(opCtx, sql, args...) // nosemgrep
	if err != nil {
		err = blockError(ctx, opCtx, err)
		cancel()
		conn.Release()
		return nil, err
	}
	return &blockRows{Rows: rows, ctx: ctx, opCtx: opCtx, cancel: cancel, release: conn.Release}, nil
}

// blockRows are the rows of a blockQuerier query. They hold the connection
// and the block timeout of the query until they are closed. The error of the
// query is taken before the block timeout is cancelled, so that the cancel
// does not hide it.
type blockRows struct {
	pgx.Rows
	ctx     context.Context
	opCtx   context.Context
	cancel  context.CancelFunc
	release func()
	closed  bool
	err     error
}

// Next moves to the next row. After the last row it closes the rows, so that
// the connection goes back to the pool at once.
func (r *blockRows) Next() bool {
	if r.closed {
		return false
	}
	if r.Rows.Next() {
		return true
	}
	r.Close()
	return false
}

// Close ends the query, keeps its error, stops the block timeout and gives
// the connection back. A second call does nothing.
func (r *blockRows) Close() {
	if r.closed {
		return
	}
	r.closed = true
	r.Rows.Close()
	r.err = blockError(r.ctx, r.opCtx, r.Rows.Err())
	r.cancel()
	r.release()
}

// Err returns the error of the query. A query that ran longer than the block
// timeout gives a blockTimeoutError.
func (r *blockRows) Err() error {
	if r.closed {
		return r.err
	}
	return blockError(r.ctx, r.opCtx, r.Rows.Err())
}
