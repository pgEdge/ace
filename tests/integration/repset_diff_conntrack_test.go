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
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connSnapshot holds a point-in-time reading of ACE connections on a node.
type connSnapshot struct {
	Time      time.Time
	Node      string
	ACEConns  int
	TotalConn int
}

// monitorConnections polls pg_stat_activity on the given pools at the specified
// interval, recording the number of connections where application_name = 'ACE'.
// It stops when the returned cancel function is called, and returns all
// collected snapshots.
func monitorConnections(
	ctx context.Context,
	pools map[string]*pgxpool.Pool,
	interval time.Duration,
) (snapshots *[]connSnapshot, stop func()) {

	var mu sync.Mutex
	var snaps []connSnapshot
	snapshots = &snaps

	monCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		sample := func() {
			for name, pool := range pools {
				var aceCount, totalCount int
				err := pool.QueryRow(monCtx, `
					SELECT
						count(*) FILTER (WHERE application_name = 'ACE') AS ace_conns,
						count(*) AS total_conns
					FROM pg_stat_activity
					WHERE pid <> pg_backend_pid()
				`).Scan(&aceCount, &totalCount)
				if err != nil {
					continue // context cancelled or transient error
				}
				mu.Lock()
				snaps = append(snaps, connSnapshot{
					Time:      time.Now(),
					Node:      name,
					ACEConns:  aceCount,
					TotalConn: totalCount,
				})
				mu.Unlock()
			}
		}

		// Take an initial sample immediately.
		sample()
		for {
			select {
			case <-monCtx.Done():
				// Take a final sample after the diff has finished.
				sample()
				return
			case <-ticker.C:
				sample()
			}
		}
	}()

	stop = func() {
		cancel()
		wg.Wait()
		// Update the caller's pointer since append may have reallocated.
		*snapshots = snaps
	}
	return snapshots, stop
}

// aceConnCount returns the number of ACE connections to the node of pool.
func aceConnCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE application_name = 'ACE' AND pid <> pg_backend_pid()`).Scan(&n))
	return n
}

// waitForACEConns waits until every node of pools has no more ACE
// connections than want gives for it, and fails the test after 10 seconds.
func waitForACEConns(t *testing.T, ctx context.Context, pools map[string]*pgxpool.Pool, want map[string]int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		extra := make(map[string]int)
		for name, pool := range pools {
			if n := aceConnCount(t, ctx, pool); n > want[name] {
				extra[name] = n - want[name]
			}
		}
		if len(extra) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ACE connections still open above the expected number: %v", extra)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestRepsetDiff_MaxConnectionsCap verifies that MaxConnections limits the
// number of database connections per node for the whole task, even when the
// concurrency factor would allow more. On a 4-vCPU GitHub Actions runner with
// concurrency factor 4.0, the comparison phase could run 16 tasks, but with
// MaxConnections = 2 it runs at most 2 at the same time, on the 2
// connections that cutting and hashing used. Every other table differs
// between the nodes, so the comparison phase runs.
func TestRepsetDiff_MaxConnectionsCap(t *testing.T) {
	const (
		numTables         = 5
		rowsPerTable      = 50000
		repsetName        = "default"
		concurrencyFactor = 4.0
		maxConnections    = 2
		pollInterval      = 50 * time.Millisecond
	)

	ctx := context.Background()

	tableNames := make([]string, numTables)
	for i := 0; i < numTables; i++ {
		tableName := fmt.Sprintf("maxconn_%03d", i)
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

		// Diverge every other table before adding to repset.
		if i%2 == 0 {
			_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
				`UPDATE %s SET val = 'diverged_n1' WHERE id <= 50`, qualifiedName))
			require.NoError(t, err, "diverge %s on n1", tableName)
		}

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

	monitorPools := map[string]*pgxpool.Pool{
		serviceN1: pgCluster.Node1Pool,
		serviceN2: pgCluster.Node2Pool,
	}

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

	snapshots, stopMonitor := monitorConnections(ctx, monitorPools, pollInterval)

	task := newTestRepsetDiffTask(repsetName)
	task.ConcurrencyFactor = concurrencyFactor
	task.MaxConnections = maxConnections
	task.BlockSize = 1000
	task.Quiet = true

	err := diff.RepsetDiff(task)
	require.NoError(t, err, "repset-diff should succeed")

	stopMonitor()
	time.Sleep(500 * time.Millisecond)

	t.Logf("Collected %d connection snapshots", len(*snapshots))

	peakPerNode := make(map[string]int)
	for _, snap := range *snapshots {
		above := snap.ACEConns - baseline[snap.Node]
		if above > peakPerNode[snap.Node] {
			peakPerNode[snap.Node] = above
		}
	}

	for node, peak := range peakPerNode {
		t.Logf("Node %s: peak ACE connections above baseline = %d", node, peak)
	}

	// With MaxConnections=2, peak should never exceed 2 per node, regardless
	// of concurrency factor (16 comparison tasks on 4 CPUs), in every phase
	// of the task. Allow no margin.
	for node, peak := range peakPerNode {
		assert.LessOrEqual(t, peak, maxConnections,
			"node %s peak ACE connections (%d) exceeds max_connections (%d) — "+
				"MaxConnections cap is not being applied",
			node, peak, maxConnections)
	}

	// Verify no leaks.
	for name, pool := range monitorPools {
		var aceCount int
		err := pool.QueryRow(ctx, `
			SELECT count(*) FROM pg_stat_activity
			WHERE application_name = 'ACE' AND pid <> pg_backend_pid()
		`).Scan(&aceCount)
		require.NoError(t, err)
		leaked := aceCount - baseline[name]
		t.Logf("Node %s: post-diff ACE connections = %d (baseline=%d, leaked=%d)",
			name, aceCount, baseline[name], leaked)
		assert.LessOrEqual(t, leaked, 1,
			"node %s should not leak connections (leaked %d)", name, leaked)
	}
}

// TestTableDiff_ConnectionsPerNode checks that table-diff opens exactly the
// number of connections that the max_connections section of the
// configuration file gives for each node, and no more, while it cuts and
// hashes a table. The tables are equal, so there is no comparison phase.
func TestTableDiff_ConnectionsPerNode(t *testing.T) {
	const (
		tableName = "conn_per_node"
		rows      = 200000
		n1Conns   = 2
		n2Conns   = 3
	)
	ctx := context.Background()
	qualifiedName := fmt.Sprintf("%s.%s", testSchema, tableName)
	pools := map[string]*pgxpool.Pool{serviceN1: pgCluster.Node1Pool, serviceN2: pgCluster.Node2Pool}

	for name, pool := range pools {
		_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, val text)`, qualifiedName))
		require.NoError(t, err, "create table on %s", name)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s SELECT g, 'row_' || g FROM generate_series(1, %d) g ON CONFLICT DO NOTHING`, qualifiedName, rows))
		require.NoError(t, err, "insert rows on %s", name)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qualifiedName))
		}
	})

	old := config.Get()
	cfg := *old
	cfg.MaxConnections = config.ConnectionLimits{
		Default: n2Conns,
		Nodes:   map[string]int{pgCluster.ClusterName + "." + serviceN1: n1Conns},
	}
	config.Set(&cfg)
	t.Cleanup(func() { config.Set(old) })

	baseline := make(map[string]int)
	for name, pool := range pools {
		baseline[name] = aceConnCount(t, ctx, pool)
	}

	task := newTestTableDiffTask(t, qualifiedName, []string{serviceN1, serviceN2})
	task.ConcurrencyFactor = 4
	task.QuietMode = true
	task.SkipDBUpdate = true
	require.NoError(t, task.RunChecks(false))
	// The checks use their own short connections; count only the
	// connections of the diff.
	waitForACEConns(t, ctx, pools, baseline)

	snapshots, stop := monitorConnections(ctx, pools, 10*time.Millisecond)
	err := task.ExecuteTask()
	stop()
	require.NoError(t, err)

	peak := make(map[string]int)
	for _, s := range *snapshots {
		peak[s.Node] = max(peak[s.Node], s.ACEConns-baseline[s.Node])
	}
	assert.Equal(t, n1Conns, peak[serviceN1], "peak ACE connections on %s", serviceN1)
	assert.Equal(t, n2Conns, peak[serviceN2], "peak ACE connections on %s", serviceN2)
	for name := range pools {
		t.Logf("node %s: peak ACE connections above baseline = %d", name, peak[name])
	}
}

// TestTableDiff_OneConnectionPerNode runs table-diff with one connection to
// each node. On the anchor the only connection first cuts the table and then
// hashes it, and the comparison phase also runs on that one connection. The
// tables differ, so every phase of the task runs.
func TestTableDiff_OneConnectionPerNode(t *testing.T) {
	const (
		tableName = "conn_one_per_node"
		rows      = 20000
		diverged  = 30
	)
	ctx := context.Background()
	qualifiedName := fmt.Sprintf("%s.%s", testSchema, tableName)
	pools := map[string]*pgxpool.Pool{serviceN1: pgCluster.Node1Pool, serviceN2: pgCluster.Node2Pool}

	for name, pool := range pools {
		_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int PRIMARY KEY, val text)`, qualifiedName))
		require.NoError(t, err, "create table on %s", name)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s SELECT g, 'row_' || g FROM generate_series(1, %d) g ON CONFLICT DO NOTHING`, qualifiedName, rows))
		require.NoError(t, err, "insert rows on %s", name)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qualifiedName))
		}
	})
	// The table is in no replication set, so the change stays on n1. The
	// changed rows are spread over the table, so several blocks differ.
	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`UPDATE %s SET val = 'diverged_n1' WHERE id %% %d = 0`, qualifiedName, rows/diverged))
	require.NoError(t, err, "diverge the table on %s", serviceN1)

	task := newTestTableDiffTask(t, qualifiedName, []string{serviceN1, serviceN2})
	task.ConcurrencyFactor = 4
	task.MaxConnections = 1
	task.BlockSize = 1000
	task.OverrideBlockSize = true
	task.QuietMode = true
	task.SkipDBUpdate = true
	baseline := make(map[string]int)
	for name, pool := range pools {
		baseline[name] = aceConnCount(t, ctx, pool)
	}
	require.NoError(t, task.RunChecks(false))
	// The checks use their own short connections, and a backend leaves
	// pg_stat_activity a little after the client closes it. Wait until they
	// are gone, so the count below sees only the connections of the diff.
	waitForACEConns(t, ctx, pools, baseline)

	snapshots, stop := monitorConnections(ctx, pools, 10*time.Millisecond)
	err = task.ExecuteTask()
	stop()
	require.NoError(t, err)

	pairKey := serviceN1 + "/" + serviceN2
	assert.Equal(t, diverged, task.DiffResult.Summary.DiffRowsCount[pairKey],
		"table-diff with one connection per node must find every changed row")

	peak := make(map[string]int)
	for _, s := range *snapshots {
		peak[s.Node] = max(peak[s.Node], s.ACEConns-baseline[s.Node])
	}
	for name := range pools {
		t.Logf("node %s: peak ACE connections above baseline = %d", name, peak[name])
		assert.Equal(t, 1, peak[name], "peak ACE connections on %s", name)
	}
}
