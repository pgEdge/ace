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
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fdwPlainSchema = "fdwplain" // heap tables, a foreign table, views
	fdwMixedSchema = "fdwmixed" // heap parent with a foreign child
	fdwPartSchema  = "fdwpart"  // partitioned parent with a foreign partition
)

// setupNonHeapFixtures creates, on one node, the relations ACE must refuse
// or skip: a file_fdw table, a view over a renamed table (the coldfront
// layout), a plain view, a heap parent with a foreign child, a heap parent
// with only heap children, and a partitioned table with a foreign
// partition. The CSV the foreign tables read is written inside the
// container with a server-side COPY.
func setupNonHeapFixtures(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	stmts := []string{
		"CREATE EXTENSION IF NOT EXISTS file_fdw",
		"CREATE SERVER IF NOT EXISTS ace_test_csv FOREIGN DATA WRAPPER file_fdw",
		"COPY (SELECT * FROM (VALUES (101,'c1'),(102,'c2')) v(id, val)) TO '/tmp/ace_fdw_rows.csv' CSV",

		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", fdwPlainSchema),
		fmt.Sprintf("CREATE SCHEMA %s", fdwPlainSchema),
		fmt.Sprintf("CREATE TABLE %s.t_heap (id int PRIMARY KEY, val text)", fdwPlainSchema),
		fmt.Sprintf("INSERT INTO %s.t_heap VALUES (1,'a'),(2,'b')", fdwPlainSchema),
		fmt.Sprintf("CREATE FOREIGN TABLE %s.t_foreign (id int, val text) SERVER ace_test_csv OPTIONS (filename '/tmp/ace_fdw_rows.csv', format 'csv')", fdwPlainSchema),
		fmt.Sprintf("CREATE TABLE %s._orders (id bigint PRIMARY KEY, ts timestamptz)", fdwPlainSchema),
		fmt.Sprintf("CREATE VIEW %s.orders AS SELECT id, ts FROM %s._orders", fdwPlainSchema, fdwPlainSchema),
		fmt.Sprintf("CREATE VIEW %s.plainview AS SELECT id FROM %s.t_heap", fdwPlainSchema, fdwPlainSchema),
		fmt.Sprintf("CREATE TABLE %s.heap_parent (id int PRIMARY KEY, val text)", fdwPlainSchema),
		fmt.Sprintf("INSERT INTO %s.heap_parent VALUES (1,'p1')", fdwPlainSchema),
		fmt.Sprintf("CREATE TABLE %s.heap_child (PRIMARY KEY (id)) INHERITS (%s.heap_parent)", fdwPlainSchema, fdwPlainSchema),
		fmt.Sprintf("INSERT INTO %s.heap_child VALUES (11,'h1')", fdwPlainSchema),

		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", fdwMixedSchema),
		fmt.Sprintf("CREATE SCHEMA %s", fdwMixedSchema),
		fmt.Sprintf("CREATE TABLE %s.parent (id int PRIMARY KEY, val text)", fdwMixedSchema),
		fmt.Sprintf("CREATE TABLE %s.child_heap (PRIMARY KEY (id)) INHERITS (%s.parent)", fdwMixedSchema, fdwMixedSchema),
		fmt.Sprintf("CREATE FOREIGN TABLE %s.child_fdw () INHERITS (%s.parent) SERVER ace_test_csv OPTIONS (filename '/tmp/ace_fdw_rows.csv', format 'csv')", fdwMixedSchema, fdwMixedSchema),

		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", fdwPartSchema),
		fmt.Sprintf("CREATE SCHEMA %s", fdwPartSchema),
		fmt.Sprintf("CREATE TABLE %s.part_parent (id int, val text) PARTITION BY RANGE (id)", fdwPartSchema),
		fmt.Sprintf("CREATE TABLE %s.part_heap PARTITION OF %s.part_parent FOR VALUES FROM (0) TO (100)", fdwPartSchema, fdwPartSchema),
		fmt.Sprintf("CREATE FOREIGN TABLE %s.part_fdw PARTITION OF %s.part_parent FOR VALUES FROM (100) TO (200) SERVER ace_test_csv OPTIONS (filename '/tmp/ace_fdw_rows.csv', format 'csv')", fdwPartSchema, fdwPartSchema),
	}
	for _, s := range stmts {
		_, err := pool.Exec(ctx, s)
		require.NoError(t, err, "statement: %s", s)
	}
}

// TestNativePGNonHeapRelations checks that ACE refuses foreign tables and
// views with a message that says why, reads a parent with a foreign child
// without it, and leaves ordinary tables next to them unaffected.
func TestNativePGNonHeapRelations(t *testing.T) {
	state := setupNativeCluster(t)
	t.Cleanup(func() { state.teardown(t) })
	state.writeClusterConfig(t)
	ctx := context.Background()
	env := newNativeEnv(state)
	nodes := []string{env.ServiceN1, env.ServiceN2}

	setupNonHeapFixtures(t, ctx, state.n1Pool)
	setupNonHeapFixtures(t, ctx, state.n2Pool)

	expectCheckError := func(t *testing.T, table string, fragments ...string) {
		t.Helper()
		task := env.newTableDiffTask(t, table, nodes)
		err := task.RunChecks(false)
		require.Error(t, err, "table-diff must refuse %s", table)
		for _, f := range fragments {
			assert.Contains(t, err.Error(), f)
		}
	}

	t.Run("ForeignTableRefused", func(t *testing.T) {
		expectCheckError(t, fdwPlainSchema+".t_foreign", "is a foreign table")
	})

	t.Run("ViewRefusedWithHotTableHint", func(t *testing.T) {
		expectCheckError(t, fdwPlainSchema+".orders", "is a view", fdwPlainSchema+"._orders")
	})

	t.Run("PlainViewRefusedWithoutHint", func(t *testing.T) {
		task := env.newTableDiffTask(t, fdwPlainSchema+".plainview", nodes)
		err := task.RunChecks(false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is a view")
		assert.NotContains(t, err.Error(), "_plainview")
	})

	t.Run("ParentWithForeignChildIsReadWithoutIt", func(t *testing.T) {
		// Covered in depth by TestNativePGForeignTables; here only that the
		// pre-check accepts the parent and records the skipped child.
		task := env.newTableDiffTask(t, fdwMixedSchema+".parent", nodes)
		require.NoError(t, task.RunChecks(false))
		assert.Equal(t, []string{fdwMixedSchema + ".child_fdw"}, task.ExcludedRelations[env.ServiceN1])
	})

	t.Run("PartitionedWithForeignPartitionExplainsMissingKey", func(t *testing.T) {
		expectCheckError(t, fdwPartSchema+".part_parent", "cannot have a primary key", fdwPartSchema+".part_fdw")
	})

	t.Run("MtreeBuildRefusesParentWithForeignChild", func(t *testing.T) {
		task := env.newMerkleTreeTask(t, fdwMixedSchema+".parent", nodes)
		err := task.RunChecks(false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), fdwMixedSchema+".child_fdw")
	})

	t.Run("HeapParentWithHeapChildrenStillDiffs", func(t *testing.T) {
		env.assertNoTableDiff(t, fdwPlainSchema+".heap_parent")
	})

	t.Run("SchemaDiffSkipsForeignTablesAndViews", func(t *testing.T) {
		task := diff.NewSchemaDiffTask()
		task.ClusterName = env.ClusterName
		task.DBName = env.DBName
		task.SchemaName = fdwPlainSchema
		task.Nodes = "all"
		task.Output = "json"
		task.Quiet = true
		task.SkipDBUpdate = true
		task.BlockSize = 10000
		task.CompareUnitSize = 100
		task.ConcurrencyFactor = 1
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.SchemaTableDiff())
	})
}
