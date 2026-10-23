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
	"github.com/pgedge/ace/pkg/config"
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
	require.Contains(t, err.Error(), "table-diff")
}

// TestMerkleTreeOnPartitionSeesRowsMovedBetweenPartitions checks that a tree
// on a partition sees a row that an UPDATE of the partition key moves into
// or out of it. CDC reports such a row as a delete in one partition and an
// insert in the other.
func TestMerkleTreeOnPartitionSeesRowsMovedBetweenPartitions(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_part_move"
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

	// Odd ids are stored in the 2026 partition, even ids in 2027. On n2,
	// 151 moves into 2027 and 152 moves out of it.
	env.withRepairMode(t, ctx, env.N2Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, "UPDATE "+parent+" SET created = DATE '2027-06-01' WHERE id = 151") // nosemgrep
		require.NoError(t, err)
		_, err = conn.Exec(ctx, "UPDATE "+parent+" SET created = DATE '2026-06-01' WHERE id = 152") // nosemgrep
		require.NoError(t, err)
	})

	require.NoError(t, task.DiffMtree())
	nodeDiffs, ok := task.DiffResult.NodeDiffs[env.pairKey()]
	require.True(t, ok, "the rows moved on n2 were not seen")
	require.Equal(t, []int{152}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]), "152 left the partition on n2 only")
	require.Equal(t, []int{151}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]), "151 entered the partition on n2 only")
}

// TestMerkleTreeRefusesTablePartitionedOnOneNode checks that mtree checks
// every node: a table that is plain on n1 but partitioned on n2 is refused,
// and the message names n2.
func TestMerkleTreeRefusesTablePartitionedOnOneNode(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	parent := pgx.Identifier{testSchema, "mtree_part_drift"}.Sanitize()
	leaf := pgx.Identifier{testSchema, "mtree_part_drift_2026"}.Sanitize()
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+parent+" CASCADE") // nosemgrep
		}
	})

	_, err := env.N1Pool.Exec(ctx, "CREATE TABLE "+parent+" (id int, created date, payload text, PRIMARY KEY (id, created))") // nosemgrep
	require.NoError(t, err)
	for _, sql := range []string{
		"CREATE TABLE " + parent + " (id int, created date, payload text, PRIMARY KEY (id, created)) PARTITION BY RANGE (created)",
		"CREATE TABLE " + leaf + " PARTITION OF " + parent + " FOR VALUES FROM ('2026-01-01') TO ('2027-01-01')",
	} {
		_, err := env.N2Pool.Exec(ctx, sql) // nosemgrep
		require.NoError(t, err, "exec %q on n2", sql)
	}

	task := env.newMerkleTreeTask(t, testSchema+".mtree_part_drift", []string{env.ServiceN1, env.ServiceN2})
	err = task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a partitioned table")
	require.Contains(t, err.Error(), "(node "+env.ServiceN2+")")
}

// TestMerkleTreeTeardownTableAfterTableBecameParent covers a tree on a table
// that became an inheritance parent after the tree was built: mtree update
// and table-diff refuse the table, and mtree teardown-table still removes the
// tree and takes the table out of the publication. An earlier ACE could build
// a tree on a parent, and ALTER PUBLICATION ADD TABLE then added the children
// too, so teardown must also take out a child that is in the publication.
func TestMerkleTreeTeardownTableAfterTableBecameParent(t *testing.T) {
	t.Run("child not in the publication", func(t *testing.T) {
		testTeardownTableOfParent(t, "mtree_late_parent", false)
	})
	t.Run("child in the publication", func(t *testing.T) {
		testTeardownTableOfParent(t, "mtree_old_parent", true)
	})
}

func testTeardownTableOfParent(t *testing.T, tableName string, childPublished bool) {
	ctx := context.Background()
	env := newSpockEnv()
	childName := tableName + "_child"
	parent := pgx.Identifier{testSchema, tableName}.Sanitize()
	child := pgx.Identifier{testSchema, childName}.Sanitize()
	publication := config.Cfg.MTree.CDC.PublicationName
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+parent+" CASCADE") // nosemgrep
		}
	})
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+parent+" (id int PRIMARY KEY, payload text)") // nosemgrep
		require.NoError(t, err)
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "INSERT INTO "+parent+" SELECT g, 'row-' || g FROM generate_series(1, 100) g") // nosemgrep
			require.NoError(t, err)
		})
	}

	task := env.newMerkleTreeTask(t, testSchema+"."+tableName, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() {
		if err := task.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, task.BuildMtree())

	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TABLE "+child+" () INHERITS ("+parent+")") // nosemgrep
		require.NoError(t, err)
		if childPublished {
			_, err = pool.Exec(ctx, "ALTER PUBLICATION "+pgx.Identifier{publication}.Sanitize()+" ADD TABLE ONLY "+child) // nosemgrep
			require.NoError(t, err)
		}
	}

	// mtree table-diff runs these checks before the diff.
	err := task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has child tables through inheritance")
	err = task.UpdateMtree(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "has child tables through inheritance")

	require.NoError(t, task.MtreeTeardownTable())

	aceSchema := config.Cfg.MTree.Schema
	meta := pgx.Identifier{aceSchema, "ace_mtree_metadata"}.Sanitize()
	mt := pgx.Identifier{aceSchema, "ace_mtree_" + testSchema + "_" + tableName}.Sanitize()
	for i, pool := range env.pools() {
		var rows int
		require.NoError(t, pool.QueryRow(ctx, "SELECT count(*) FROM "+meta+" WHERE schema_name = $1 AND table_name = $2", // nosemgrep
			testSchema, tableName).Scan(&rows))
		require.Zero(t, rows, "metadata left on node %d", i+1)

		var treeExists bool
		require.NoError(t, pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", mt).Scan(&treeExists))
		require.False(t, treeExists, "tree table left on node %d", i+1)

		pubRows, err := pool.Query(ctx,
			"SELECT tablename FROM pg_publication_tables WHERE pubname = $1 AND schemaname = $2 AND tablename IN ($3, $4)",
			publication, testSchema, tableName, childName)
		require.NoError(t, err)
		published, err := pgx.CollectRows(pubRows, pgx.RowTo[string])
		require.NoError(t, err)
		require.Empty(t, published, "tables left in the publication on node %d", i+1)
	}
}
