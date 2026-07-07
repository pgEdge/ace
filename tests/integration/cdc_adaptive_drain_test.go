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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// dirtyLeafCounts returns (dirty, total) leaf-block counts for a table's
// merkle tree on the given node.
func dirtyLeafCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName string) (int64, int64) {
	t.Helper()
	mtreeIdent := pgx.Identifier{
		config.Get().MTree.Schema,
		fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
	}.Sanitize()
	var dirty, total int64
	err := pool.QueryRow(ctx, fmt.Sprintf( // nosemgrep
		`SELECT count(*) FILTER (WHERE dirty), count(*) FROM %s WHERE node_level = 0`, mtreeIdent,
	)).Scan(&dirty, &total)
	require.NoError(t, err, "count dirty leaves for %s", tableName)
	return dirty, total
}

// TestAdaptiveDrainEscalation exercises the three regimes of the adaptive
// bounded drain in one pass over a shared slot:
//   - flood table: changes >> threshold  -> escalate: ALL leaves marked dirty
//   - single table: one changed row      -> exactly one leaf dirty (cheap path)
//   - idle table:   no changes           -> zero leaves dirty (untouched)
//
// and then verifies end-to-end correctness: DiffMtree finds exactly the
// diverged rows for both changed tables.
func TestAdaptiveDrainEscalation(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tables := []struct {
		name string
		rows int
	}{
		{"adaptive_flood", 50000},
		{"adaptive_single", 5000},
		{"adaptive_idle", 5000},
	}

	src := pgx.Identifier{testSchema, "customers_1M"}.Sanitize()
	tasks := make(map[string]*mtree.MerkleTreeTask, len(tables))

	t.Cleanup(func() {
		for _, tbl := range tables {
			if task := tasks[tbl.name]; task != nil {
				if err := task.MtreeTeardown(); err != nil {
					t.Logf("cleanup: MtreeTeardown(%s) failed: %v", tbl.name, err)
				}
			}
			for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
				ident := pgx.Identifier{testSchema, tbl.name}.Sanitize()
				if _, err := pool.Exec(context.Background(), fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, ident)); err != nil { // nosemgrep
					t.Logf("cleanup: drop %s failed: %v", tbl.name, err)
				}
			}
		}
	})

	// Seed identical data on both nodes (fresh tables are in no spock
	// replication set, so per-node inserts stay local), then init once and
	// build the merkle tree for each table. MtreeInit drops and recreates the
	// SHARED publication and slot (SetupPublication/SetupReplicationSlot), so
	// it must run exactly once; running it per table would unpublish the
	// tables already built. BuildMtree adds each table to the publication and
	// CDC metadata itself.
	for i, tbl := range tables {
		ident := pgx.Identifier{testSchema, tbl.name}.Sanitize()
		for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
			require.NoError(t, createTestTable(ctx, pool, testSchema, tbl.name))
			_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
				`INSERT INTO %s SELECT * FROM %s WHERE index <= %d`, ident, src, tbl.rows))
			require.NoError(t, err, "seed %s", tbl.name)
			// ANALYZE so BuildMtree's row estimate (and therefore the
			// escalation threshold's metadata row count) is accurate.
			_, err = pool.Exec(ctx, "ANALYZE "+ident) // nosemgrep
			require.NoError(t, err)
		}
		task := env.newMerkleTreeTask(t, fmt.Sprintf("%s.%s", testSchema, tbl.name),
			[]string{env.ServiceN1, env.ServiceN2})
		// Pin the diff cap: MaxDiffRows==0 falls back to config, and a low
		// configured cap would break the exact-count assertions below.
		task.MaxDiffRows = int64(tbl.rows)
		require.NoError(t, task.RunChecks(false), "RunChecks(%s)", tbl.name)
		if i == 0 {
			require.NoError(t, task.MtreeInit(), "MtreeInit (once, first table)")
		}
		require.NoError(t, task.BuildMtree(), "BuildMtree(%s)", tbl.name)
		tasks[tbl.name] = task
	}

	// Diverge on n2 only, under repair mode (captured by n2's CDC slot, not
	// replicated to n1). Flood: 20k changed rows on a 50k-row table -- far
	// over the escalation threshold max(1000, 0.01*50000=500) = 1000.
	// Single: exactly one row.
	env.withRepairMode(t, ctx, env.N2Pool, func(conn *pgxpool.Conn) {
		floodIdent := pgx.Identifier{testSchema, "adaptive_flood"}.Sanitize()
		_, err := conn.Exec(ctx, fmt.Sprintf( // nosemgrep
			`UPDATE %s SET email = 'flood_' || index::text WHERE index <= 20000`, floodIdent))
		require.NoError(t, err, "flood divergence")
		singleIdent := pgx.Identifier{testSchema, "adaptive_single"}.Sanitize()
		_, err = conn.Exec(ctx, fmt.Sprintf( // nosemgrep
			`UPDATE %s SET email = 'single_change' WHERE index = 42`, singleIdent))
		require.NoError(t, err, "single-row divergence")
	})

	// Drain n2's slot directly (no hash recompute) so the dirty flags are
	// observable before UpdateMtree clears them. ClusterNodes[1] is n2.
	require.NoError(t, cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[1]),
		"bounded CDC drain on n2")

	// Flood table escalated: every leaf dirty.
	dirty, total := dirtyLeafCounts(t, ctx, env.N2Pool, "adaptive_flood")
	require.Greater(t, total, int64(0))
	require.Equal(t, total, dirty,
		"flood table must escalate to mark-all-dirty (dirty=%d of %d leaves)", dirty, total)

	// Single-change table stayed on the per-PK path: exactly one leaf dirty.
	dirty, total = dirtyLeafCounts(t, ctx, env.N2Pool, "adaptive_single")
	require.Greater(t, total, int64(1))
	require.Equal(t, int64(1), dirty,
		"single-row table must have exactly one dirty leaf, got %d of %d", dirty, total)

	// Idle table untouched: zero leaves dirty.
	dirty, _ = dirtyLeafCounts(t, ctx, env.N2Pool, "adaptive_idle")
	require.Equal(t, int64(0), dirty, "idle table must have no dirty leaves")

	// End-to-end correctness: the escalated table's diff finds every diverged
	// row, and the single-change table finds exactly one.
	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}
	countDiffs := func(task *mtree.MerkleTreeTask) int {
		nodeDiffs, ok := task.DiffResult.NodeDiffs[pairKey]
		if !ok {
			return 0
		}
		return len(nodeDiffs.Rows[env.ServiceN1])
	}

	require.NoError(t, tasks["adaptive_flood"].DiffMtree(), "DiffMtree(flood)")
	require.Equal(t, 20000, countDiffs(tasks["adaptive_flood"]),
		"escalated table must report every diverged row")

	require.NoError(t, tasks["adaptive_single"].DiffMtree(), "DiffMtree(single)")
	require.Equal(t, 1, countDiffs(tasks["adaptive_single"]),
		"single-change table must report exactly one diverged row")
}
