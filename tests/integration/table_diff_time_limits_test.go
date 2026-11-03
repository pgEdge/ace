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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const (
	// slowHashRows is the number of rows of the test table. With a block
	// size of 1000 and slowHashSleep per row, one block hash takes about
	// 20 seconds.
	slowHashRows  = 3000
	slowHashSleep = "0.02"
)

// setupSlowHashTable creates a table and a filter function that is slow
// only inside the block hash query of table-diff. The planning and the cut
// of the table stay fast, so the test controls which operation runs long.
// It returns the qualified table name and the filter to use.
func setupSlowHashTable(t *testing.T, tableName string) (string, string) {
	t.Helper()
	ctx := context.Background()
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	funcName := fmt.Sprintf("%s.%s_slow", testSchema, tableName)

	// The block hash query is the only ACE query with the alias _rh_.
	funcSQL := fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s() RETURNS boolean
LANGUAGE plpgsql VOLATILE AS $f$
BEGIN
    IF position('_rh_' in current_query()) > 0 THEN
        PERFORM pg_sleep(%s);
    END IF;
    RETURN true;
END
$f$`, funcName, slowHashSleep)

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.NoError(t, createTestTable(ctx, pool, testSchema, tableName))
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO "%s"."%s" SELECT * FROM "%s"."customers_1M" WHERE index <= %d ON CONFLICT DO NOTHING`,
			testSchema, tableName, testSchema, slowHashRows))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, funcSQL)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(`ANALYZE "%s"."%s"`, testSchema, tableName))
		require.NoError(t, err)
	}

	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s" CASCADE`, testSchema, tableName)); err != nil {
				t.Logf("cleanup: drop table: %v", err)
			}
			if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP FUNCTION IF EXISTS %s()`, funcName)); err != nil {
				t.Logf("cleanup: drop function: %v", err)
			}
		}
		files, _ := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
		for _, f := range files {
			os.Remove(f)
		}
	})

	return qualified, funcName + "()"
}

// requireNoActiveHashQueries checks that no backend on any node still runs
// a query that calls filterFunc. When table-diff stops on a time limit, its
// queries must stop on the server too, not only on the client.
func requireNoActiveHashQueries(t *testing.T, filterFunc string) {
	t.Helper()
	ctx := context.Background()
	const check = `SELECT count(*) FROM pg_stat_activity
WHERE pid <> pg_backend_pid() AND state = 'active' AND position($1 in query) > 0`

	deadline := time.Now().Add(2 * time.Second)
	for {
		total := 0
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			var n int
			require.NoError(t, pool.QueryRow(ctx, check, filterFunc).Scan(&n))
			total += n
		}
		if total == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d hash queries are still active on the nodes after table-diff stopped", total)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func intPtr(v int) *int { return &v }

func TestTableDiffBlockTimeout(t *testing.T) {
	qualified, filter := setupSlowHashTable(t, "time_limit_block")

	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.TableFilter = filter
	task.BlockTimeout = intPtr(1)
	task.RunTimeout = intPtr(0)
	require.NoError(t, task.RunChecks(false))

	start := time.Now()
	err := task.ExecuteTask()
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "block timeout of 1s reached")
	require.Less(t, elapsed, 15*time.Second, "table-diff must stop soon after the block timeout")
	requireNoActiveHashQueries(t, filter)
}

func TestTableDiffRunTimeout(t *testing.T) {
	qualified, filter := setupSlowHashTable(t, "time_limit_run")

	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.TableFilter = filter
	task.BlockTimeout = intPtr(0)
	task.RunTimeout = intPtr(3)
	require.NoError(t, task.RunChecks(false))

	start := time.Now()
	err := task.ExecuteTask()
	elapsed := time.Since(start)

	require.Error(t, err)
	require.Contains(t, err.Error(), "run timeout of 3s reached")
	require.Less(t, elapsed, 15*time.Second, "table-diff must stop soon after the run timeout")
	requireNoActiveHashQueries(t, filter)
}

// With no limits, the same slow table is compared to the end. This shows
// that the time limits, not the test setup, stop the other tests.
func TestTableDiffNoTimeLimits(t *testing.T) {
	qualified, filter := setupSlowHashTable(t, "time_limit_none")

	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.TableFilter = filter + " AND index <= 100"
	task.BlockTimeout = intPtr(0)
	task.RunTimeout = intPtr(0)
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.ExecuteTask())
	require.Empty(t, task.DiffResult.NodeDiffs)
}
