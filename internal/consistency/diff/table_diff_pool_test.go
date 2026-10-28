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

	"github.com/pgedge/ace/pkg/config"
)

// useConfig makes c the active configuration until the end of the test.
func useConfig(t *testing.T, c *config.Config) {
	t.Helper()
	old := config.Get()
	t.Cleanup(func() { config.Set(old) })
	config.Set(c)
}

// The pool of a node must have room for every comparison task, so that no
// task waits for a connection.
func TestMaxPoolSizeHasRoomForComparison(t *testing.T) {
	// One comparison task per CPU.
	task := &TableDiffTask{ConcurrencyFactor: 1}
	tasks := task.compareWorkers()
	if tasks != runtime.NumCPU() {
		t.Fatalf("compareWorkers = %d, want %d", tasks, runtime.NumCPU())
	}
	if got, want := task.maxPoolSize(), max(tasks, 4); got != want {
		t.Errorf("maxPoolSize = %d, want %d", got, want)
	}

	// A very small concurrency factor still gives one comparison task and a
	// pool of at least 4 connections.
	task = &TableDiffTask{ConcurrencyFactor: 0.0001}
	if got := task.compareWorkers(); got != 1 {
		t.Errorf("compareWorkers with a tiny factor = %d, want 1", got)
	}
	if got := task.maxPoolSize(); got != 4 {
		t.Errorf("maxPoolSize with a tiny factor = %d, want 4", got)
	}
}

// The number of connections comes from MaxConnections, then from the
// configuration file, then from the built-in default. A number that the user
// sets is also the size of the pool; with the built-in default the pool has
// room for every comparison task.
func TestNodeLimits(t *testing.T) {
	nodes := []string{"n1", "n2"}
	useConfig(t, &config.Config{MaxConnections: config.ConnectionLimits{
		Default: 3,
		Nodes:   map[string]int{"demo.n1": 64},
	}})

	task := &TableDiffTask{ConcurrencyFactor: 16}
	task.ClusterName = "demo"
	limits := task.nodeLimits(nodes)
	if l := limits["n1"]; l.want != 64 || !l.set {
		t.Errorf("limit of n1 = %+v, want 64 from the file", *l)
	}
	if l := limits["n2"]; l.want != 3 || !l.set {
		t.Errorf("limit of n2 = %+v, want the default 3 from the file", *l)
	}
	if got := limits["n1"].poolOpts(task.maxPoolSize()); got.PoolSize != 64 || !got.KeepOpen {
		t.Errorf("pool of n1 = %+v, want a pool of 64 that keeps its connections", got)
	}
	if got := limits["n2"].poolOpts(task.maxPoolSize()).PoolSize; got != 3 {
		t.Errorf("pool of n2 = %d, want 3: a limit from the file is strict", got)
	}

	// The limits are read once: a later change of the file does not change
	// them.
	useConfig(t, &config.Config{})
	if l := limits["n2"]; l.want != 3 || !l.set {
		t.Errorf("limit of n2 after a reload = %+v, want it unchanged", *l)
	}

	task.MaxConnections = 5
	limits = task.nodeLimits(nodes)
	for _, node := range nodes {
		if l := limits[node]; l.want != 5 || !l.set {
			t.Errorf("limit of %s with MaxConnections=5 = %+v, want 5", node, *l)
		}
		if got := limits[node].poolOpts(task.maxPoolSize()).PoolSize; got != 5 {
			t.Errorf("pool of %s with MaxConnections=5 = %d, want 5", node, got)
		}
	}

	task.MaxConnections = 0
	limits = task.nodeLimits(nodes)
	if l := limits["n1"]; l.want != config.DefaultMaxConnections || l.set {
		t.Errorf("limit of n1 with an empty file = %+v, want the built-in default", *l)
	}
	if got, want := limits["n1"].poolOpts(task.maxPoolSize()).PoolSize, max(config.DefaultMaxConnections, task.maxPoolSize()); got != want {
		t.Errorf("pool of n1 with an empty file = %d, want %d", got, want)
	}
}

// The comparison phase runs no more tasks at the same time than a node has
// open connections, when the pool of that node must not open more: when the
// user sets the limit, or when the server gave fewer connections than the
// limit.
func TestCompareTasks(t *testing.T) {
	task := &TableDiffTask{ConcurrencyFactor: 64}
	all := task.compareWorkers()
	if all < 8 {
		t.Fatalf("compareWorkers = %d, want at least 8 for this test", all)
	}
	def := config.DefaultMaxConnections

	tests := []struct {
		name   string
		limits map[string]*nodeLimit
		want   int
	}{
		{"built-in default, all connections open", map[string]*nodeLimit{
			"n1": {want: def, opened: def}, "n2": {want: def, opened: def}}, all},
		{"built-in default, n2 gave 2", map[string]*nodeLimit{
			"n1": {want: def, opened: def}, "n2": {want: def, opened: 2}}, 2},
		{"limits set by the user", map[string]*nodeLimit{
			"n1": {want: 6, set: true, opened: 6}, "n2": {want: 3, set: true, opened: 3}}, 3},
		{"one connection", map[string]*nodeLimit{
			"n1": {want: 1, set: true, opened: 1}, "n2": {want: 1, set: true, opened: 1}}, 1},
	}
	for _, tt := range tests {
		if got := task.compareTasks(tt.limits); got != tt.want {
			t.Errorf("%s: %d tasks, want %d", tt.name, got, tt.want)
		}
	}
}
