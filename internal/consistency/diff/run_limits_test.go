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
	"testing"
)

func TestStopCause(t *testing.T) {
	if err := stopCause(nil); err != nil {
		t.Fatalf("stopCause(nil) = %v, want nil", err)
	}
	if err := stopCause(context.Background()); err != nil {
		t.Fatalf("stopCause of a running context = %v, want nil", err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("stop")
	cancel(want)
	if err := stopCause(ctx); err != want {
		t.Fatalf("stopCause = %v, want %v", err, want)
	}
}

// The first recorded error must stop the run with itself as the cause, so
// the other workers stop and the run reports this error.
func TestRecordErrorStopsRun(t *testing.T) {
	task := NewTableDiffTask()
	ctx, stop := newRunContext(nil)
	defer stop(nil)
	task.stopRun = stop

	first := errors.New("first")
	task.recordError(first)
	task.recordError(errors.New("second"))

	if ctx.Err() == nil {
		t.Fatal("run context is not done after an error")
	}
	if cause := context.Cause(ctx); cause != first {
		t.Fatalf("cause = %v, want %v", cause, first)
	}
	if err := task.getFirstError(); err != first {
		t.Fatalf("first error = %v, want %v", err, first)
	}
}

// A parent that is done stops the run with the cause of the parent.
func TestRunContextKeepsParentCause(t *testing.T) {
	parent, cancel := context.WithCancelCause(context.Background())
	ctx, stop := newRunContext(parent)
	defer stop(nil)

	want := errors.New("signal")
	cancel(want)
	if cause := stopCause(ctx); cause != want {
		t.Fatalf("cause = %v, want %v", cause, want)
	}
}
