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
	"github.com/stretchr/testify/require"
)

// createPartitionedTable creates a table partitioned by range on "created"
// with two partitions on every node, and seeds the same rows on each node.
func createPartitionedTable(t *testing.T, ctx context.Context, env *testEnv, table string) (parent string) {
	t.Helper()
	parent = pgx.Identifier{testSchema, table}.Sanitize()
	leaf2026 := pgx.Identifier{testSchema, table + "_2026"}.Sanitize()
	leaf2027 := pgx.Identifier{testSchema, table + "_2027"}.Sanitize()

	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+parent+" CASCADE") // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})

	for _, pool := range env.pools() {
		stmts := []string{
			"CREATE TABLE IF NOT EXISTS " + parent + " (id int, created date, payload text, PRIMARY KEY (id, created)) PARTITION BY RANGE (created)",
			"CREATE TABLE IF NOT EXISTS " + leaf2026 + " PARTITION OF " + parent + " FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
			"CREATE TABLE IF NOT EXISTS " + leaf2027 + " PARTITION OF " + parent + " FOR VALUES FROM ('2027-01-01') TO ('2028-01-01')",
		}
		for _, sql := range stmts {
			_, err := pool.Exec(ctx, sql) // nosemgrep
			require.NoError(t, err, "exec %q", sql)
		}
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+parent) // nosemgrep
			require.NoError(t, err)
			_, err = conn.Exec(ctx, // nosemgrep
				`INSERT INTO `+parent+` (id, created, payload)
				 SELECT g, CASE WHEN g % 2 = 1 THEN DATE '2026-06-01' ELSE DATE '2027-06-01' END, 'row-' || g
				 FROM generate_series(1, 1000) g`)
			require.NoError(t, err, "seed %s", parent)
		})
	}
	return parent
}

// TestMerkleTreeRefusesPartitionedParent checks that mtree refuses a
// partitioned table and names its partitions (ACE-215), while table-diff
// still compares the whole table.
func TestMerkleTreeRefusesPartitionedParent(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_part_parent"
	createPartitionedTable(t, ctx, env, table)
	nodes := []string{env.ServiceN1, env.ServiceN2}

	task := env.newMerkleTreeTask(t, testSchema+"."+table, nodes)
	err := task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a partitioned table")
	require.Contains(t, err.Error(), "public.mtree_part_parent_2026, public.mtree_part_parent_2027")
	require.NotContains(t, err.Error(), "0 rows")

	diffTask := env.newTableDiffTask(t, testSchema+"."+table, nodes)
	require.NoError(t, diffTask.RunChecks(false))
	require.NoError(t, diffTask.ExecuteTask())
}

// TestMerkleTreeOnPartition checks the supported way: a tree on one
// partition sees a change made through the parent.
func TestMerkleTreeOnPartition(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_part_leaf"
	parent := createPartitionedTable(t, ctx, env, table)

	task := env.newMerkleTreeTask(t, testSchema+"."+table+"_2027", []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() {
		if err := task.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, task.BuildMtree())
	require.NoError(t, task.DiffMtree())
	if nodeDiffs, ok := task.DiffResult.NodeDiffs[env.pairKey()]; ok {
		require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]), "nodes seeded identically; first diff must be clean")
		require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]), "nodes seeded identically; first diff must be clean")
	}

	// Written through the parent; the row is stored in the 2027 partition.
	env.withRepairMode(t, ctx, env.N2Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, "UPDATE "+parent+" SET payload = 'changed' WHERE id = 150") // nosemgrep
		require.NoError(t, err)
	})

	require.NoError(t, task.DiffMtree())
	nodeDiffs, ok := task.DiffResult.NodeDiffs[env.pairKey()]
	require.True(t, ok, "the change on n2 was not seen")
	require.Equal(t, []int{150}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]))
	require.Equal(t, []int{150}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]))
}

// TestMerkleTreeRefusesInheritanceParent checks that a plain table with
// inheritance children is refused.
func TestMerkleTreeRefusesInheritanceParent(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	parent := pgx.Identifier{testSchema, "mtree_inh_parent"}.Sanitize()
	child := pgx.Identifier{testSchema, "mtree_inh_child"}.Sanitize()
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+parent+" CASCADE") // nosemgrep
		}
	})
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+parent+" (id int PRIMARY KEY, payload text)") // nosemgrep
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+child+" () INHERITS ("+parent+")") // nosemgrep
		require.NoError(t, err)
	}

	task := env.newMerkleTreeTask(t, testSchema+".mtree_inh_parent", []string{env.ServiceN1, env.ServiceN2})
	err := task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "inheritance (public.mtree_inh_child)")
}
