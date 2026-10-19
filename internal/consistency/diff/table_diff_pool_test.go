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
	"runtime"
	"testing"

	"github.com/pgedge/ace/internal/consistency/slicer"
)

// The pool of the anchor node must have room for the hash workers and for
// the cutting workers of the slicer, or the cut waits for hash queries to
// end and does not run beside them.
func TestMaxPoolSizeHasRoomForTheCut(t *testing.T) {
	// One hash worker per CPU, so the number of hash workers is NumCPU.
	task := &TableDiffTask{ConcurrencyFactor: 1}
	hash := task.hashWorkers()
	if hash != runtime.NumCPU() {
		t.Fatalf("hashWorkers = %d, want %d", hash, runtime.NumCPU())
	}
	want := hash + slicer.DefaultWorkers(hash)
	if want < 4 {
		want = 4
	}
	if got := task.maxPoolSize(); got != want {
		t.Errorf("maxPoolSize = %d, want %d (%d hash workers + %d cutting workers)",
			got, want, hash, slicer.DefaultWorkers(hash))
	}

	// MaxConnections still caps the pool.
	task.MaxConnections = 2
	if got := task.maxPoolSize(); got != 2 {
		t.Errorf("maxPoolSize with MaxConnections=2 = %d, want 2", got)
	}

	// A very small concurrency factor still gives one hash worker and a
	// pool of at least 4 connections.
	task = &TableDiffTask{ConcurrencyFactor: 0.0001}
	if got := task.hashWorkers(); got != 1 {
		t.Errorf("hashWorkers with a tiny factor = %d, want 1", got)
	}
	if got := task.maxPoolSize(); got != 4 {
		t.Errorf("maxPoolSize with a tiny factor = %d, want 4", got)
	}
}
