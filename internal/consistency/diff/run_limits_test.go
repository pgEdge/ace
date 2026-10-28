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
	"io"
	"testing"

	"github.com/vbauerster/mpb/v8"
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

// When the run stops while the diff workers wait for a free slot, they end
// without an error. The run must still fail with the cause of the stop, so
// that an incomplete diff is not reported as a complete one.
func TestStoppedRecursiveDiffsFailTheRun(t *testing.T) {
	task := NewTableDiffTask()
	ctx, stop := newRunContext(nil)
	task.stopRun = stop

	if err := task.runError(ctx); err != nil {
		t.Fatalf("runError of a running context = %v, want nil", err)
	}

	cause := errors.New("interrupted by signal")
	stop(cause)

	// No free slot, so a worker can only see the stop.
	task.diffSem = make(chan struct{}, 1)
	task.diffSem <- struct{}{}

	p := mpb.New(mpb.WithOutput(io.Discard))
	tasks := []RecursiveDiffTask{
		{Node1Name: "n1", Node2Name: "n2"},
		{Node1Name: "n1", Node2Name: "n2"},
	}
	bar := p.AddBar(int64(len(tasks)))
	task.runRecursiveDiffs(ctx, tasks, bar)
	p.Wait()

	if err := task.getFirstError(); err != nil {
		t.Fatalf("first error = %v, want none", err)
	}
	if err := task.runError(ctx); err != cause {
		t.Fatalf("runError = %v, want %v", err, cause)
	}
}

// When a signal stops schema-diff --ddl-only, the queries of the nodes fail
// and these nodes are skipped. The command must fail with the cause of the
// stop and not compare only a part of the nodes.
func TestSchemaObjectDiffStopped(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	want := errors.New("interrupted by signal")
	cancel(want)

	task := &SchemaDiffCmd{Ctx: ctx, SchemaName: "public"}
	if err := task.schemaObjectDiff(); !errors.Is(err, want) {
		t.Fatalf("schemaObjectDiff = %v, want an error that wraps %v", err, want)
	}
}
