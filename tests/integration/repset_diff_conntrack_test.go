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

// TestRepsetDiff_MaxConnectionsCap verifies that setting MaxConnections limits
// the number of database connections per node, even when the concurrency factor
// would allow more. On a 4-vCPU GitHub Actions runner with concurrency factor
// 4.0, this creates 16 workers but caps the pool at 2 connections per node.
// Workers queue for connections rather than opening new ones.
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
	// of concurrency factor (16 workers on 4 CPUs). The pool queues excess
	// workers. Allow no margin — the cap must hold strictly.
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
