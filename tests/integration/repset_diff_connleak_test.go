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

package integration

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConnLeak_100Tables_ConnectionExhaustion reproduces a user issue
// where a scheduled repset-diff with 100 tables and max_connections=10 resulted
// in 79 observed connections instead of the expected cap of 10.
//
// Root cause at v1.8.0:
//   - No singleton mode on the gocron scheduler, so overlapping runs stacked
//     connection pools. If a single repset-diff of 100 tables exceeded the
//     run_frequency (10m), the scheduler launched another run in parallel.
//   - Discovery pools (used to list repset tables) did not respect the
//     max_connections cap.
//
// This test creates 100 tables with 1000 rows each, then:
//
//  1. Runs a SINGLE repset-diff and asserts the connection cap holds.
//  2. Runs 4 CONCURRENT repset-diffs (simulating overlapping scheduled runs
//     without singleton mode) and shows that connections blow past the cap.
func TestConnLeak_100Tables_ConnectionExhaustion(t *testing.T) {
	const (
		numTables         = 100
		rowsPerTable      = 10000
		repsetName        = "default"
		maxConnections    = 10
		concurrencyFactor = 4.0 // With 4 CPUs → poolSize=16, capped to maxConnections=10
		blockSize         = 100 // Small blocks to create more work per table
		pollInterval      = 50 * time.Millisecond
	)

	ctx := context.Background()

	// ── Phase 0: Create 100 tables with data on both nodes ──────────────

	t.Log("Creating 100 tables with 1000 rows each on both nodes...")
	tableNames := make([]string, numTables)
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("connleak_%03d", i)
		qualifiedName := fmt.Sprintf("%s.%s", testSchema, tableName)
		tableNames[i] = qualifiedName

		createSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id   INT PRIMARY KEY,
				val  TEXT
			)`, qualifiedName)

		pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}
		nodeNames := []string{serviceN1, serviceN2}

		for j, pool := range pools {
			_, err := pool.Exec(ctx, createSQL)
			require.NoError(t, err, "create table %s on %s", tableName, nodeNames[j])

			_, err = pool.Exec(ctx, fmt.Sprintf(
				`INSERT INTO %s (id, val)
				 SELECT g, 'row_' || g FROM generate_series(1, %d) g
				 ON CONFLICT DO NOTHING`, qualifiedName, rowsPerTable))
			require.NoError(t, err, "insert rows into %s on %s", tableName, nodeNames[j])
		}

		// Diverge every 10th table to make diffs interesting.
		if i%10 == 0 {
			_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
				`UPDATE %s SET val = 'diverged_n1' WHERE id <= 10`, qualifiedName))
			require.NoError(t, err, "diverge %s on n1", tableName)
		}

		// Add to repset on both nodes.
		for j, pool := range pools {
			_, err := pool.Exec(ctx, fmt.Sprintf(
				`SELECT spock.repset_add_table('%s', '%s')`, repsetName, qualifiedName))
			require.NoError(t, err, "add %s to repset on %s", tableName, nodeNames[j])
		}
	}

	t.Cleanup(func() {
		for _, qn := range tableNames {
			for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
				pool.Exec(ctx, fmt.Sprintf(
					`SELECT spock.repset_remove_table('%s', '%s')`, repsetName, qn))
				pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qn))
			}
		}
	})

	t.Logf("Setup complete: %d tables × %d rows on 2 nodes", numTables, rowsPerTable)

	monitorPools := map[string]*pgxpool.Pool{
		serviceN1: pgCluster.Node1Pool,
		serviceN2: pgCluster.Node2Pool,
	}

	// Helper: measure baseline ACE connections (from the test pools themselves).
	baseline := make(map[string]int)
	for name, pool := range monitorPools {
		var aceCount int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = 'ACE' AND pid <> pg_backend_pid()
		`).Scan(&aceCount)
		require.NoError(t, err)
		baseline[name] = aceCount
	}
	t.Logf("Baseline ACE connections: n1=%d, n2=%d", baseline[serviceN1], baseline[serviceN2])

	// ── Phase 1: Single repset-diff run ─────────────────────────────────
	// This should respect the max_connections cap.

	t.Log("=== Phase 1: Single repset-diff run ===")
	snapshots, stopMonitor := monitorConnections(ctx, monitorPools, pollInterval)

	task := newTestRepsetDiffTask(repsetName)
	task.ConcurrencyFactor = concurrencyFactor
	task.MaxConnections = maxConnections
	task.BlockSize = blockSize
	task.Quiet = true
	task.SkipDBUpdate = true

	err := diff.RepsetDiff(task)
	require.NoError(t, err, "single repset-diff should succeed")

	stopMonitor()
	time.Sleep(300 * time.Millisecond) // let pg_stat_activity settle

	singlePeakPerNode := make(map[string]int)
	for _, snap := range *snapshots {
		above := snap.ACEConns - baseline[snap.Node]
		if above > singlePeakPerNode[snap.Node] {
			singlePeakPerNode[snap.Node] = above
		}
	}

	for node, peak := range singlePeakPerNode {
		t.Logf("Phase 1 — Node %s: peak ACE connections above baseline = %d", node, peak)
	}

	// A single run must respect the cap.
	for node, peak := range singlePeakPerNode {
		assert.LessOrEqual(t, peak, maxConnections,
			"Phase 1 — node %s peak ACE connections (%d) exceeds max_connections (%d)",
			node, peak, maxConnections)
	}

	// Verify no leaks after single run.
	for name, pool := range monitorPools {
		var aceCount int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = 'ACE' AND pid <> pg_backend_pid()
		`).Scan(&aceCount)
		require.NoError(t, err)
		leaked := aceCount - baseline[name]
		t.Logf("Phase 1 — Node %s: post-diff ACE conns=%d baseline=%d leaked=%d",
			name, aceCount, baseline[name], leaked)
		assert.LessOrEqual(t, leaked, 1,
			"Phase 1 — node %s should not leak connections (leaked %d)", name, leaked)
	}

	// ── Phase 2: Simulate overlapping runs (v1.8.0 without singleton) ──
	// At v1.8.0 the scheduler had no WithSingletonMode, so if a repset-diff
	// of 100 tables took >10m, the next tick launched another run in parallel.
	// With 4 overlapping runs × 2 nodes × max_connections connections each,
	// we expect peak connections ≈ 4 × max_connections per node.

	const concurrentRuns = 4
	t.Logf("=== Phase 2: %d concurrent repset-diff runs (simulating v1.8.0) ===", concurrentRuns)

	snapshots2, stopMonitor2 := monitorConnections(ctx, monitorPools, pollInterval)

	var wg sync.WaitGroup
	var failures atomic.Int32

	for i := 0; i < concurrentRuns; i++ {
		wg.Add(1)
		go func(runID int) {
			defer wg.Done()
			rTask := newTestRepsetDiffTask(repsetName)
			rTask.ConcurrencyFactor = concurrencyFactor
			rTask.MaxConnections = maxConnections
			rTask.BlockSize = blockSize
			rTask.Quiet = true
			rTask.SkipDBUpdate = true
			if err := diff.RepsetDiff(rTask); err != nil {
				t.Logf("concurrent run %d failed: %v", runID, err)
				failures.Add(1)
			}
		}(i)
	}
	wg.Wait()

	stopMonitor2()
	time.Sleep(300 * time.Millisecond)

	concurrentPeakPerNode := make(map[string]int)
	for _, snap := range *snapshots2 {
		above := snap.ACEConns - baseline[snap.Node]
		if above > concurrentPeakPerNode[snap.Node] {
			concurrentPeakPerNode[snap.Node] = above
		}
	}
	// Sum the per-node peaks as a conservative total peak estimate.
	var totalPeakACE int
	for _, peak := range concurrentPeakPerNode {
		totalPeakACE += peak
	}

	t.Logf("Phase 2 — Concurrent peak connections (total across all nodes):")
	for node, peak := range concurrentPeakPerNode {
		t.Logf("  Node %s: peak ACE connections above baseline = %d", node, peak)
	}
	t.Logf("  Total peak across nodes = %d", totalPeakACE)

	// This is the user's bug: with overlapping runs, connections blow
	// past the per-run max_connections cap. The combined peak should exceed
	// what a single run would use.
	if totalPeakACE > maxConnections {
		t.Logf("REPRODUCED: %d concurrent runs caused peak of %d total ACE connections "+
			"(max_connections=%d). Without singleton mode this is expected.",
			concurrentRuns, totalPeakACE, maxConnections)
	}

	// Per-node peak with N concurrent runs can reach up to N × maxConnections.
	for node, peak := range concurrentPeakPerNode {
		t.Logf("Phase 2 — Node %s: peak=%d, theoretical_max=%d (runs=%d × max_conn=%d)",
			node, peak, concurrentRuns*maxConnections, concurrentRuns, maxConnections)
	}

	// Verify no leaks after concurrent runs.
	for name, pool := range monitorPools {
		var aceCount int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = 'ACE' AND pid <> pg_backend_pid()
		`).Scan(&aceCount)
		require.NoError(t, err)
		leaked := aceCount - baseline[name]
		t.Logf("Phase 2 — Node %s: post-diff ACE conns=%d baseline=%d leaked=%d",
			name, aceCount, baseline[name], leaked)
		assert.LessOrEqual(t, leaked, 1,
			"Phase 2 — node %s should not leak connections after all runs complete (leaked %d)",
			name, leaked)
	}

	t.Logf("Failures in concurrent runs: %d/%d", failures.Load(), concurrentRuns)
}

// TestRepsetDiff_RunChecksLeakAccumulation proves that at v1.8.0, connection
// pools leaked from failed RunChecks calls accumulate across sequential
// scheduled runs. Each table that fails RunChecks leaks 1 connection per node
// because the pool is never closed on error paths. The leaked pools have
// background goroutines that prevent garbage collection, so the connections
// persist for the lifetime of the process.
//
// This test creates a repset with a mix of valid and invalid tables (dropped
// from one node), then runs multiple sequential repset-diffs and watches the
// connection count grow.
func TestConnLeak_RunChecksLeakAccumulation(t *testing.T) {
	const (
		numGoodTables  = 10
		numBadTables   = 5
		rowsPerTable   = 100
		repsetName     = "default"
		maxConnections = 10
		numRuns        = 5
	)

	ctx := context.Background()

	// ── Setup: create good tables on both nodes, bad tables only on n1 ──

	var allTableNames []string

	for i := 0; i < numGoodTables+numBadTables; i++ {
		tableName := fmt.Sprintf("leaktest_%03d", i)
		qualifiedName := fmt.Sprintf("%s.%s", testSchema, tableName)
		allTableNames = append(allTableNames, qualifiedName)

		createSQL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id   INT PRIMARY KEY,
				val  TEXT
			)`, qualifiedName)

		pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}
		nodeNames := []string{serviceN1, serviceN2}

		for j, pool := range pools {
			_, err := pool.Exec(ctx, createSQL)
			require.NoError(t, err, "create table %s on %s", tableName, nodeNames[j])

			_, err = pool.Exec(ctx, fmt.Sprintf(
				`INSERT INTO %s (id, val)
				 SELECT g, 'row_' || g FROM generate_series(1, %d) g
				 ON CONFLICT DO NOTHING`, qualifiedName, rowsPerTable))
			require.NoError(t, err, "insert rows into %s on %s", tableName, nodeNames[j])
		}

		for j, pool := range pools {
			_, err := pool.Exec(ctx, fmt.Sprintf(
				`SELECT spock.repset_add_table('%s', '%s')`, repsetName, qualifiedName))
			require.NoError(t, err, "add %s to repset on %s", tableName, nodeNames[j])
		}
	}

	// Now break the bad tables: drop them from n2 so RunChecks fails with
	// "table not found" when it queries columns on that node.
	badTableNames := allTableNames[numGoodTables:]
	for _, qn := range badTableNames {
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf(
			`SELECT spock.repset_remove_table('%s', '%s')`, repsetName, qn))
		require.NoError(t, err, "remove %s from repset on n2", qn)

		_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf(
			`DROP TABLE IF EXISTS %s CASCADE`, qn))
		require.NoError(t, err, "drop %s on n2", qn)
	}

	t.Cleanup(func() {
		for _, qn := range allTableNames {
			for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
				pool.Exec(ctx, fmt.Sprintf(
					`SELECT spock.repset_remove_table('%s', '%s')`, repsetName, qn))
				pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qn))
			}
		}
	})

	t.Logf("Setup: %d good tables (both nodes) + %d bad tables (n1 only, dropped from n2)",
		numGoodTables, numBadTables)

	monitorPools := map[string]*pgxpool.Pool{
		serviceN1: pgCluster.Node1Pool,
		serviceN2: pgCluster.Node2Pool,
	}

	aceConns := func() map[string]int {
		counts := make(map[string]int)
		for name, pool := range monitorPools {
			var c int
			err := pool.QueryRow(ctx, `
				SELECT count(*) FROM pg_stat_activity
				WHERE application_name = 'ACE' AND pid <> pg_backend_pid()
			`).Scan(&c)
			require.NoError(t, err)
			counts[name] = c
		}
		return counts
	}

	baseline := aceConns()
	t.Logf("Baseline: n1=%d, n2=%d", baseline[serviceN1], baseline[serviceN2])

	// ── Run sequential repset-diffs and watch for accumulating leaks ──

	for run := 1; run <= numRuns; run++ {
		task := newTestRepsetDiffTask(repsetName)
		task.MaxConnections = maxConnections
		task.BlockSize = 1000
		task.Quiet = true
		task.SkipDBUpdate = true

		_ = diff.RepsetDiff(task) // errors expected from bad tables

		// Give pg_stat_activity a moment to settle.
		time.Sleep(200 * time.Millisecond)

		counts := aceConns()
		leakedN1 := counts[serviceN1] - baseline[serviceN1]
		leakedN2 := counts[serviceN2] - baseline[serviceN2]
		t.Logf("After run %d: n1 ACE conns=%d (leaked=%d), n2 ACE conns=%d (leaked=%d)",
			run, counts[serviceN1], leakedN1, counts[serviceN2], leakedN2)
	}

	// Check final state.
	final := aceConns()
	totalLeaked := (final[serviceN1] - baseline[serviceN1]) + (final[serviceN2] - baseline[serviceN2])
	t.Logf("Final leaked connections after %d runs: %d", numRuns, totalLeaked)

	// With the closure fix applied, there should be zero leaked connections.
	// Without the fix (v1.8.0), each run leaks ~numBadTables connections on
	// the node where the table is missing, accumulating across runs.
	for name, pool := range monitorPools {
		var c int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = 'ACE' AND pid <> pg_backend_pid()
		`).Scan(&c)
		require.NoError(t, err)
		leaked := c - baseline[name]
		assert.Equal(t, 0, leaked,
			"node %s should have 0 leaked connections after fix (got %d)", name, leaked)
	}
}
