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
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// The earlier row text, concat_ws('|', COALESCE(col::text, ''), ...) joined
// with '|' over the block, gave the same text on n1 and n2 for the changes
// below, so table-diff and mtree table-diff reported the tables as equal.
// TestBlockHashEncoding checks the hashes on one node; these tests check
// that both diff commands report the rows across two nodes.

// createCollisionTable creates the table on n1 and n2 with the same rows.
func createCollisionTable(t *testing.T, ctx context.Context, env *testEnv, table string) (qualified string) {
	t.Helper()
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+ident) // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+ident+" (id int PRIMARY KEY, a text, b text)") // nosemgrep
		require.NoError(t, err)
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+ident) // nosemgrep
			require.NoError(t, err)
			_, err = conn.Exec(ctx, "INSERT INTO "+ident+` VALUES
				(1, NULL, 'y'),
				(2, 'a|b', 'c'),
				(3, 'p', 'q'),
				(4, 'r', 's')`) // nosemgrep
			require.NoError(t, err)
		})
	}
	return testSchema + "." + table
}

// makeCollidingChanges changes the rows on n2 only. Each change gave the same
// text as the row on n1 in the earlier encoding.
func makeCollidingChanges(t *testing.T, ctx context.Context, env *testEnv, table string) {
	t.Helper()
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	env.withRepairMode(t, ctx, env.N2Pool, func(conn *pgxpool.Conn) {
		for _, sql := range []string{
			// NULL against an empty string.
			"UPDATE " + ident + " SET a = '' WHERE id = 1",
			// The delimiter moves from one column to the next.
			"UPDATE " + ident + " SET a = 'a', b = 'b|c' WHERE id = 2",
			// Rows 3 and 4 join into row 3: "3|p|q|4|r|s" on both nodes.
			"UPDATE " + ident + " SET b = 'q|4|r|s' WHERE id = 3",
			"DELETE FROM " + ident + " WHERE id = 4",
		} {
			_, err := conn.Exec(ctx, sql) // nosemgrep
			require.NoError(t, err, "exec %q on n2", sql)
		}
	})
}

// requireCollidingRowsReported checks that the diff reports every changed
// row, and that row 1 is NULL on n1 and an empty string on n2.
func requireCollidingRowsReported(t *testing.T, env *testEnv, nodeDiffs map[string]types.DiffByNodePair) {
	t.Helper()
	pair, ok := nodeDiffs[env.pairKey()]
	require.True(t, ok, "the diff found no difference between n1 and n2")
	require.Equal(t, []int{1, 2, 3, 4}, extractDiffIDs(pair.Rows[env.ServiceN1]))
	require.Equal(t, []int{1, 2, 3}, extractDiffIDs(pair.Rows[env.ServiceN2]), "row 4 is on n1 only")

	columnA := func(rows []types.OrderedMap) any {
		for _, row := range rows {
			ids := extractDiffIDs([]types.OrderedMap{row})
			require.Len(t, ids, 1, "a reported row has no integer id")
			if ids[0] == 1 {
				v, ok := row.Get("a")
				require.True(t, ok, "row 1 has no column a")
				return v
			}
		}
		t.Fatalf("row 1 not reported")
		return nil
	}
	require.Nil(t, columnA(pair.Rows[env.ServiceN1]), "row 1 on n1 is NULL")
	require.Equal(t, "", columnA(pair.Rows[env.ServiceN2]), "row 1 on n2 is an empty string")
}

// TestTableDiffFindsRowsThatCollidedInTheOldEncoding runs table-diff across
// two nodes on rows that only the earlier encoding could not tell apart.
func TestTableDiffFindsRowsThatCollidedInTheOldEncoding(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "hash_collision_td"
	qualified := createCollisionTable(t, ctx, env, table)
	makeCollidingChanges(t, ctx, env, table)

	task := env.newTableDiffTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.ExecuteTask())
	requireCollidingRowsReported(t, env, task.DiffResult.NodeDiffs)
}

// TestMerkleTreeFindsRowsThatCollidedInTheOldEncoding builds the tree before
// the changes, so they reach it through CDC, and then runs mtree table-diff.
// The row hashes of the mtree diff used the same lossy encoding, so the rows
// must also be told apart after the leaf hashes differ.
func TestMerkleTreeFindsRowsThatCollidedInTheOldEncoding(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "hash_collision_mt"
	qualified := createCollisionTable(t, ctx, env, table)

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() {
		if err := task.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, task.BuildMtree())
	require.NoError(t, task.DiffMtree())
	if pair, ok := task.DiffResult.NodeDiffs[env.pairKey()]; ok {
		require.Empty(t, pair.Rows[env.ServiceN1], "nodes seeded identically; first diff must be clean")
		require.Empty(t, pair.Rows[env.ServiceN2], "nodes seeded identically; first diff must be clean")
	}

	makeCollidingChanges(t, ctx, env, table)

	require.NoError(t, task.DiffMtree())
	requireCollidingRowsReported(t, env, task.DiffResult.NodeDiffs)
}
