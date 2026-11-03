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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pgedge/ace/pkg/config"
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
	ctx, stop := newRunContext(nil, 0)
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
	ctx, stop := newRunContext(parent, 0)
	defer stop(nil)

	want := errors.New("signal")
	cancel(want)
	if cause := stopCause(ctx); cause != want {
		t.Fatalf("cause = %v, want %v", cause, want)
	}
}

// The run timeout stops the run with a runTimeoutError as the cause.
func TestRunContextTimeout(t *testing.T) {
	ctx, stop := newRunContext(context.Background(), 10*time.Millisecond)
	defer stop(nil)

	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("run context is not done after the run timeout")
	}
	var rt *runTimeoutError
	if !errors.As(stopCause(ctx), &rt) {
		t.Fatalf("cause = %v, want a runTimeoutError", stopCause(ctx))
	}
	if !strings.Contains(rt.Error(), "run timeout") {
		t.Fatalf("message %q does not name the run timeout", rt.Error())
	}
}

// An error recorded before the run timeout is the cause, not the timeout.
func TestRunContextErrorBeforeTimeout(t *testing.T) {
	ctx, stop := newRunContext(context.Background(), time.Hour)
	want := errors.New("first")
	stop(want)
	if cause := stopCause(ctx); cause != want {
		t.Fatalf("cause = %v, want %v", cause, want)
	}
}

func TestIsBlockTimeout(t *testing.T) {
	err := &blockTimeoutError{Limit: time.Second}
	if !isBlockTimeout(err) {
		t.Fatal("isBlockTimeout is false for a blockTimeoutError")
	}
	if !isBlockTimeout(errors.Join(errors.New("hash failed"), err)) {
		t.Fatal("isBlockTimeout is false for a wrapped blockTimeoutError")
	}
	if isBlockTimeout(context.DeadlineExceeded) {
		t.Fatal("isBlockTimeout is true for another error")
	}
}

func intPtr(v int) *int { return &v }

// TestTimeLimits checks that a limit set on the task wins over ace.yaml,
// that 0 means no limit, and that a negative value is an error.
func TestTimeLimits(t *testing.T) {
	prev := config.Get()
	defer config.Set(prev)

	cases := []struct {
		name       string
		cfg        config.DiffConfig
		task       *TableDiffTask
		block, run time.Duration
		wantErr    bool
	}{
		{name: "defaults", cfg: config.DiffConfig{}, task: &TableDiffTask{},
			block: 60 * time.Second, run: 0},
		{name: "from config", cfg: config.DiffConfig{BlockTimeout: intPtr(5), RunTimeout: 30}, task: &TableDiffTask{},
			block: 5 * time.Second, run: 30 * time.Second},
		{name: "zero in config means no limit", cfg: config.DiffConfig{BlockTimeout: intPtr(0)}, task: &TableDiffTask{},
			block: 0, run: 0},
		{name: "task wins", cfg: config.DiffConfig{BlockTimeout: intPtr(5), RunTimeout: 30},
			task:  &TableDiffTask{BlockTimeout: intPtr(0), RunTimeout: intPtr(7)},
			block: 0, run: 7 * time.Second},
		{name: "negative block", cfg: config.DiffConfig{}, task: &TableDiffTask{BlockTimeout: intPtr(-1)}, wantErr: true},
		{name: "negative run in config", cfg: config.DiffConfig{RunTimeout: -2}, task: &TableDiffTask{}, wantErr: true},
		{name: "negative block in config", cfg: config.DiffConfig{BlockTimeout: intPtr(-1)}, task: &TableDiffTask{}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			config.Set(&config.Config{TableDiff: tc.cfg})
			block, run, err := tc.task.timeLimits()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if block != tc.block || run != tc.run {
				t.Fatalf("got block %v run %v, want block %v run %v", block, run, tc.block, tc.run)
			}
		})
	}
}

// When the run stops while the diff workers wait for a free slot, they end
// without an error. The run must still fail with the cause of the stop, so
// that an incomplete diff is not reported as a complete one.
func TestStoppedRecursiveDiffsFailTheRun(t *testing.T) {
	task := NewTableDiffTask()
	ctx, stop := newRunContext(nil, 0)
	task.stopRun = stop

	if err := task.runError(ctx); err != nil {
		t.Fatalf("runError of a running context = %v, want nil", err)
	}

	limit := &runTimeoutError{Limit: time.Second}
	stop(limit)

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
	if err := task.runError(ctx); err != limit {
		t.Fatalf("runError = %v, want %v", err, limit)
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

// A limit that does not fit in a time.Duration is an error and does not
// wrap around to a wrong value. The largest one that fits is accepted.
func TestTimeLimitTooLarge(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("an int cannot hold a limit that does not fit in a time.Duration")
	}
	largest := maxLimitSeconds
	ok := int(largest)
	d, err := timeLimit("run_timeout", &ok, 0)
	if err != nil {
		t.Fatalf("timeLimit(%d) = %v, want no error", ok, err)
	}
	if d != time.Duration(largest)*time.Second {
		t.Fatalf("timeLimit(%d) = %v", ok, d)
	}

	tooLarge := int(largest + 1)
	if _, err := timeLimit("run_timeout", &tooLarge, 0); err == nil {
		t.Fatalf("timeLimit(%d) = no error, want an error", tooLarge)
	}
	if _, err := timeLimit("block_timeout", nil, tooLarge); err == nil {
		t.Fatalf("timeLimit from config (%d) = no error, want an error", tooLarge)
	}
}
