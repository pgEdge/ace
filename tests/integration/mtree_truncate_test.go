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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestMerkleTreeDiffAfterTruncate reproduces a field report: after an initial
// build+diff, TRUNCATE on one node followed by another mtree table-diff
// reported the nodes as in sync. pgoutput publishes TRUNCATE as a single
// TruncateMessage carrying only relation OIDs — no per-row DELETEs — so a CDC
// drain that only handles Insert/Update/Delete messages dirties no leaves,
// leaves the truncated node's tree at its pre-truncate state, and the diff
// compares two identical stale trees. The drain must react to the truncate by
// marking every leaf of the table dirty so the next update rehashes from the
// (now empty) live table.
func TestMerkleTreeDiffAfterTruncate(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	tableName := "mtree_truncate_diff"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
	safeTable := pgx.Identifier{testSchema, tableName}.Sanitize()

	const rowCount = 20
	seedIdenticalTruncateTable(t, ctx, env, safeTable, qualifiedTable, rowCount)

	mtreeTask := env.newMerkleTreeTask(t, qualifiedTable,
		[]string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, mtreeTask.BuildMtree())
	require.NoError(t, mtreeTask.DiffMtree())

	pair := env.pairKey()
	if nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pair]; ok {
		require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]),
			"nodes seeded identically; first diff must be clean")
		require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]),
			"nodes seeded identically; first diff must be clean")
	}

	// The divergence under test: TRUNCATE on n2 only. repair_mode keeps
	// Spock from replicating it to n1 even if the table were picked up.
	env.withRepairMode(t, ctx, env.N2Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, "TRUNCATE TABLE "+safeTable) // nosemgrep
		require.NoError(t, err, "truncate %s on n2", qualifiedTable)
	})

	// DiffMtree drains CDC (via UpdateMtree) before comparing, mirroring the
	// CLI's `ace mtree table-diff`. All rows now exist only on n1.
	require.NoError(t, mtreeTask.DiffMtree())

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pair]
	require.True(t, ok,
		"diff after TRUNCATE on n2 reported the nodes as identical; "+
			"the CDC drain dropped the TruncateMessage. Full result: %+v",
		mtreeTask.DiffResult)

	gotN1 := extractDiffIDs(nodeDiffs.Rows[env.ServiceN1])
	require.Len(t, gotN1, rowCount,
		"every seeded row exists only on n1 after n2's truncate")
	require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]),
		"n2 is empty after truncate; it can contribute no rows")
}

// seedIdenticalTruncateTable creates safeTable on every node, empties it, and
// seeds rowCount identical rows so a first mtree diff is clean. It registers
// table + diff-file cleanup on t. Conventions mirror
// TestMerkleTreeBidirectionalDiff: IF NOT EXISTS in case Spock DDL replication
// propagates the first CREATE, and the table stays outside any repset so writes
// to one node cannot leak to the other.
func seedIdenticalTruncateTable(t *testing.T, ctx context.Context, env *testEnv, safeTable, qualifiedTable string, rowCount int) {
	t.Helper()
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, // nosemgrep
			"CREATE TABLE IF NOT EXISTS "+safeTable+" (id INT PRIMARY KEY, payload TEXT)") // nosemgrep
		require.NoError(t, err, "create %s", qualifiedTable)
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safeTable+" CASCADE") // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})

	for _, pool := range env.pools() {
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+safeTable) // nosemgrep
			require.NoError(t, err, "pre-test truncate %s", qualifiedTable)
		})
	}

	for _, pool := range env.pools() {
		for id := 1; id <= rowCount; id++ {
			_, err := pool.Exec(ctx, // nosemgrep
				"INSERT INTO "+safeTable+" (id, payload) VALUES ($1, $2)", // nosemgrep
				id, fmt.Sprintf("row-%d", id))
			require.NoError(t, err, "seed id=%d", id)
		}
	}
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "ANALYZE "+safeTable) // nosemgrep
		require.NoError(t, err)
	}
}
