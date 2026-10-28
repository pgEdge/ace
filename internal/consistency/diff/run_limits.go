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
)

// stopCause returns why ctx is done, or nil if ctx is nil or still running.
func stopCause(ctx context.Context) error {
	if ctx == nil || ctx.Err() == nil {
		return nil
	}
	return context.Cause(ctx)
}

// newRunContext returns the context of one table-diff run and the function
// that stops it with a cause. All workers of the run use this context, so
// the first error, or the end of the parent context, stops all of them.
func newRunContext(parent context.Context) (context.Context, context.CancelCauseFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithCancelCause(parent)
}
