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
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/pgedge/ace/pkg/config"
)

// TestIsQueryTimeout checks which errors count as a time limit on one
// block: the block timeout and statement_timeout, not the run timeout.
func TestIsQueryTimeout(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{fmt.Errorf("hash: %w", &blockTimeoutError{Limit: time.Minute}), true},
		{fmt.Errorf("hash: %w", &pgconn.PgError{Code: "57014"}), true},
		{fmt.Errorf("hash: %w", &pgconn.PgError{Code: "42P01"}), false},
		{&runTimeoutError{Limit: time.Hour}, false},
		{context.DeadlineExceeded, false},
		{context.Canceled, false},
		{errors.New("boom"), false},
	}
	for _, c := range cases {
		if got := isQueryTimeout(c.err); got != c.want {
			t.Errorf("isQueryTimeout(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

// TestWithSlicerAdvice checks that only a block timeout after a cut by a
// sample gets the advice to use the cutter.
func TestWithSlicerAdvice(t *testing.T) {
	timeout := fmt.Errorf("initial hash failed: %w", &blockTimeoutError{Limit: time.Minute})
	other := errors.New("permission denied")

	task := &TableDiffTask{}
	if got := task.withSlicerAdvice(timeout); got != timeout {
		t.Errorf("no choice: got %v", got)
	}

	task.slicerChoice = &slicer.Choice{Requested: slicer.MethodAuto, Method: slicer.MethodCutter}
	if got := task.withSlicerAdvice(timeout); got != timeout {
		t.Errorf("cutter: got %v", got)
	}

	task.slicerChoice = &slicer.Choice{Requested: slicer.MethodAuto, Method: slicer.MethodSample}
	if got := task.withSlicerAdvice(other); got != other {
		t.Errorf("sample, other error: got %v", got)
	}
	if got := task.withSlicerAdvice(nil); got != nil {
		t.Errorf("sample, no error: got %v", got)
	}
	got := task.withSlicerAdvice(timeout)
	if !isBlockTimeout(got) {
		t.Errorf("the advice must keep the cause: %v", got)
	}
	if !strings.Contains(got.Error(), "--slicer=cutter") {
		t.Errorf("no advice in %q", got)
	}
	run := &runTimeoutError{Limit: time.Hour}
	if got := task.withSlicerAdvice(run); got != run {
		t.Errorf("sample, run timeout: got %v", got)
	}
	var read error = &sampleReadError{err: fmt.Errorf("sample: %w", &blockTimeoutError{Limit: time.Minute})}
	if got := task.withSlicerAdvice(read); got != read {
		t.Errorf("sample, timeout of the sample query: got %v", got)
	}
}

// TestHashIdleShare checks the share of idle time of the hash workers and
// the limits for a slow cut.
func TestHashIdleShare(t *testing.T) {
	if _, ok := hashIdleShare(5*time.Second, 5*time.Second, 5*time.Second); ok {
		t.Error("a short cut must not be reported")
	}
	if share, ok := hashIdleShare(30*time.Second, 10*time.Second, 20*time.Second); !ok || share != 0.75 {
		t.Errorf("got %v, %v; want 0.75, true", share, ok)
	}
	if share, ok := hashIdleShare(10*time.Second, 30*time.Second, 20*time.Second); ok || share != 0.25 {
		t.Errorf("got %v, %v; want 0.25, false", share, ok)
	}
	if share, ok := hashIdleShare(0, 0, 20*time.Second); ok || share != 0 {
		t.Errorf("no worker time must give 0, false; got %v, %v", share, ok)
	}
}

// TestHashersCutTime checks that only the time from the first block to the
// end of the cut counts.
func TestHashersCutTime(t *testing.T) {
	h := &blockHashers{}
	t0 := time.Unix(1000, 0)
	if d := h.cutTime(t0, t0.Add(time.Second)); d != 0 {
		t.Errorf("before the first block: got %v", d)
	}
	h.firstBlockAt.Store(t0.UnixNano())
	if d := h.cutTime(t0.Add(-5*time.Second), t0.Add(3*time.Second)); d != 3*time.Second {
		t.Errorf("wait from before the first block: got %v", d)
	}
	h.cutEndAt.Store(t0.Add(10 * time.Second).UnixNano())
	if d := h.cutTime(t0.Add(8*time.Second), t0.Add(20*time.Second)); d != 2*time.Second {
		t.Errorf("wait past the end of the cut: got %v", d)
	}
	if d := h.cutTime(t0.Add(12*time.Second), t0.Add(20*time.Second)); d != 0 {
		t.Errorf("wait after the end of the cut: got %v", d)
	}
}

// TestHashersIdleShare checks that the cut is reported as slow only when the
// workers of every node waited for blocks most of the time.
func TestHashersIdleShare(t *testing.T) {
	newHashers := func(times map[string]*workerTimes) *blockHashers {
		h := &blockHashers{cutTimes: times}
		t0 := time.Unix(1000, 0)
		h.firstBlockAt.Store(t0.UnixNano())
		h.cutEndAt.Store(t0.Add(20 * time.Second).UnixNano())
		return h
	}
	h := newHashers(map[string]*workerTimes{
		"n1": {idle: 30 * time.Second, busy: 10 * time.Second},
		"n2": {idle: 12 * time.Second, busy: 8 * time.Second},
	})
	if share, ok := h.idleShare(); !ok || share != 0.6 {
		t.Errorf("got %v, %v; want 0.6, true", share, ok)
	}
	h = newHashers(map[string]*workerTimes{
		"n1": {idle: 30 * time.Second, busy: 10 * time.Second},
		"n2": {idle: 2 * time.Second, busy: 18 * time.Second},
	})
	if share, ok := h.idleShare(); ok || share != 0.1 {
		t.Errorf("one busy node: got %v, %v; want 0.1, false", share, ok)
	}
	if _, ok := newHashers(map[string]*workerTimes{}).idleShare(); ok {
		t.Error("no workers must not be reported")
	}
	if _, ok := (&blockHashers{}).idleShare(); ok {
		t.Error("no cut must not be reported")
	}
}

// TestSlicerSummary checks the slicer part of the diff summary, also for a
// partitioned table, which has no catalog values of its own.
func TestSlicerSummary(t *testing.T) {
	if slicerSummary(nil) != nil {
		t.Error("nil choice must give nil")
	}
	c := 0.02
	sum := slicerSummary(&slicer.Choice{
		Requested: slicer.MethodAuto,
		Method:    slicer.MethodSample,
		Reason:    "r",
		Signals:   &slicer.Signals{Pages: 100, AllVisiblePages: 40, Correlation: &c},
	})
	if sum.Method != slicer.MethodSample || sum.KeyCorrelation == nil || *sum.KeyCorrelation != 0.02 ||
		sum.NotAllVisible == nil || *sum.NotAllVisible != 0.6 {
		t.Errorf("unexpected summary %+v", sum)
	}
	part := slicerSummary(&slicer.Choice{Method: slicer.MethodCutter, Signals: &slicer.Signals{Partitioned: true}})
	if part.NotAllVisible != nil || part.KeyCorrelation != nil {
		t.Errorf("a partitioned table has no signals of its own: %+v", part)
	}
}

// TestConfiguredSlicer checks that a wrong table_diff.slicer in the
// configuration file fails the check of repset-diff and schema-diff when
// the command does not set the way to cut.
func TestConfiguredSlicer(t *testing.T) {
	if m, err := configuredSlicer("", nil); err != nil || m != slicer.MethodAuto {
		t.Errorf("no config: got %q, %v", m, err)
	}
	cfg := &config.Config{}
	cfg.TableDiff.Slicer = "Cutter"
	if m, err := configuredSlicer(" ", cfg); err != nil || m != slicer.MethodCutter {
		t.Errorf("configured cutter: got %q, %v", m, err)
	}
	cfg.TableDiff.Slicer = "bogus"
	if m, err := configuredSlicer("sample", cfg); err != nil || m != slicer.MethodSample {
		t.Errorf("the command wins over the config: got %q, %v", m, err)
	}

	prev := config.Get()
	config.Set(cfg)
	t.Cleanup(func() { config.Set(prev) })
	rd := &RepsetDiffCmd{ClusterName: "c", RepsetName: "r"}
	if err := rd.Validate(); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Errorf("repset-diff: got %v, want the error about the configured slicer", err)
	}
	sd := &SchemaDiffCmd{ClusterName: "c", SchemaName: "s", Nodes: "n1,n2"}
	if err := sd.Validate(); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Errorf("schema-diff: got %v, want the error about the configured slicer", err)
	}
}

// TestCloneKeepsSlicer checks that a scheduled run uses the way to cut that
// the user gave.
func TestCloneKeepsSlicer(t *testing.T) {
	ctx := context.Background()
	td := NewTableDiffTask()
	td.Slicer = slicer.MethodSample
	if got := td.CloneForSchedule(ctx).Slicer; got != slicer.MethodSample {
		t.Errorf("table-diff clone: Slicer = %q", got)
	}
	sd := &SchemaDiffCmd{Slicer: slicer.MethodCutter}
	if got := sd.CloneForSchedule(ctx).Slicer; got != slicer.MethodCutter {
		t.Errorf("schema-diff clone: Slicer = %q", got)
	}
	rd := &RepsetDiffCmd{Slicer: slicer.MethodCutter}
	if got := rd.CloneForSchedule(ctx).Slicer; got != slicer.MethodCutter {
		t.Errorf("repset-diff clone: Slicer = %q", got)
	}
}
