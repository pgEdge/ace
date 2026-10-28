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
	"time"

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

	var (
		opCtx  context.Context
		cancel context.CancelFunc
	)
	if t.blockTimeout > 0 {
		opCtx, cancel = context.WithTimeoutCause(ctx, t.blockTimeout, &blockTimeoutError{Limit: t.blockTimeout})
	} else {
		opCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	err = fn(opCtx, conn)
	if err != nil && ctx.Err() == nil && opCtx.Err() != nil {
		return context.Cause(opCtx)
	}
	return err
}

// queryBlockRow runs a query that returns one row as one operation on a
// block of node, and scans the row into dest.
func (t *TableDiffTask) queryBlockRow(ctx context.Context, node, sql string, args []any, dest ...any) error {
	return t.withBlockConn(ctx, node, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, sql, args...).Scan(dest...) // nosemgrep
	})
}
