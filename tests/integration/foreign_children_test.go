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
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const fdwSchema = "fdwtest"

// setupForeignTables creates, on one node, a heap table, a file_fdw table
// with the same columns, and an inheritance parent with a heap child, a
// foreign child, and a heap grandchild. csvRows is what the foreign child
// serves on this node.
func setupForeignTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool, csvRows string) {
	t.Helper()
	stmts := []string{
		"CREATE EXTENSION IF NOT EXISTS file_fdw",
		fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", fdwSchema),
		fmt.Sprintf("CREATE SCHEMA %s", fdwSchema),
		"CREATE SERVER IF NOT EXISTS ace_test_csv FOREIGN DATA WRAPPER file_fdw",
		fmt.Sprintf("CREATE TABLE %s.t_heap (id int PRIMARY KEY, val text)", fdwSchema),
		fmt.Sprintf("INSERT INTO %s.t_heap VALUES (1,'a'),(2,'b')", fdwSchema),
		// Write the CSV inside the container, then point a foreign table at it.
		fmt.Sprintf("COPY (SELECT * FROM (VALUES %s) v(id, val)) TO '/tmp/ace_fdw_child.csv' CSV", csvRows),
		fmt.Sprintf("CREATE FOREIGN TABLE %s.t_foreign (id int, val text) SERVER ace_test_csv OPTIONS (filename '/tmp/ace_fdw_child.csv', format 'csv')", fdwSchema),
		fmt.Sprintf("CREATE TABLE %s.parent (id int PRIMARY KEY, val text)", fdwSchema),
		fmt.Sprintf("INSERT INTO %s.parent VALUES (1,'p1'),(2,'p2')", fdwSchema),
		fmt.Sprintf("CREATE TABLE %s.child_heap (PRIMARY KEY (id)) INHERITS (%s.parent)", fdwSchema, fdwSchema),
		fmt.Sprintf("INSERT INTO %s.child_heap VALUES (11,'h1'),(12,'h2')", fdwSchema),
		fmt.Sprintf("CREATE TABLE %s.grandchild_heap (PRIMARY KEY (id)) INHERITS (%s.child_heap)", fdwSchema, fdwSchema),
		fmt.Sprintf("INSERT INTO %s.grandchild_heap VALUES (21,'g1')", fdwSchema),
		fmt.Sprintf("CREATE FOREIGN TABLE %s.child_fdw () INHERITS (%s.parent) SERVER ace_test_csv OPTIONS (filename '/tmp/ace_fdw_child.csv', format 'csv')", fdwSchema, fdwSchema),
	}
	for _, s := range stmts {
		_, err := pool.Exec(ctx, s)
		require.NoError(t, err, "statement: %s", s)
	}
}

func readDiffFile(t *testing.T, path string) types.DiffOutput {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var out types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &out))
	return out
}

func TestNativePGForeignTables(t *testing.T) {
	state := setupNativeCluster(t)
	t.Cleanup(func() { state.teardown(t) })
	state.writeClusterConfig(t)
	ctx := context.Background()
	env := newNativeEnv(state)
	nodes := []string{env.ServiceN1, env.ServiceN2}

	// Foreign data differs between nodes on id 103. Heap data is identical.
	setupForeignTables(t, ctx, state.n1Pool, "(101,'c1'),(102,'c2'),(103,'c3')")
	setupForeignTables(t, ctx, state.n2Pool, "(101,'c1'),(102,'c2'),(103,'c3_DIFF')")

	parent := fdwSchema + ".parent"

	t.Run("StandaloneForeignTableRejected", func(t *testing.T) {
		task := env.newTableDiffTask(t, fdwSchema+".t_foreign", nodes)
		err := task.RunChecks(false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "is a foreign table")
	})

	t.Run("ParentMatchesWhenOnlyForeignChildDiffers", func(t *testing.T) {
		task := env.newTableDiffTask(t, parent, nodes)
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.ExecuteTask())
		assert.Empty(t, task.DiffResult.NodeDiffs)
		assert.Equal(t, []string{fdwSchema + ".child_fdw"}, task.DiffResult.Summary.ExcludedRelations[env.ServiceN1])
		assert.False(t, task.DiffResult.Summary.ForeignLayoutMismatch)
	})

	t.Run("HeapChildAndGrandchildDifferencesReportedWithPlacement", func(t *testing.T) {
		_, err := state.n2Pool.Exec(ctx, fmt.Sprintf("UPDATE %s.child_heap SET val='h2_DIFF' WHERE id=12", fdwSchema))
		require.NoError(t, err)
		_, err = state.n2Pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s.grandchild_heap WHERE id=21", fdwSchema))
		require.NoError(t, err)

		diffFile := env.runTableDiff(t, parent, nodes)
		out := readDiffFile(t, diffFile)
		pair := out.NodeDiffs[env.pairKey()]
		require.Len(t, pair.Rows[env.ServiceN1], 2, "n1 should report the modified row and the row n2 lacks")
		require.Len(t, pair.Rows[env.ServiceN2], 1)

		rels := map[string]bool{}
		for _, row := range pair.Rows[env.ServiceN1] {
			for _, kv := range row {
				if kv.Key == "_spock_metadata_" {
					meta := kv.Value.(map[string]any)
					rels[fmt.Sprint(meta["storage_relation"])] = true
				}
			}
		}
		assert.True(t, rels[fdwSchema+".child_heap"], "rels=%v", rels)
		assert.True(t, rels[fdwSchema+".grandchild_heap"], "rels=%v", rels)

		// Repair from n1 puts both rows back where they belong on n2.
		env.repairTable(t, parent, env.ServiceN1)

		var inParentOnly int
		require.NoError(t, state.n2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM ONLY %s.parent WHERE id IN (12,21)", fdwSchema)).Scan(&inParentOnly))
		assert.Equal(t, 0, inParentOnly, "repair must not write child rows into the parent")

		var rel string
		require.NoError(t, state.n2Pool.QueryRow(ctx, fmt.Sprintf("SELECT tableoid::regclass::text FROM %s.parent WHERE id=21", fdwSchema)).Scan(&rel))
		assert.Equal(t, fdwSchema+".grandchild_heap", rel, "missing row goes to the same-named heap relation")

		env.assertNoTableDiff(t, parent)
	})

	t.Run("MtreeBuildRejectsParentWithForeignChild", func(t *testing.T) {
		task := env.newMerkleTreeTask(t, parent, nodes)
		err := task.RunChecks(false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "child_fdw")
	})

	t.Run("SchemaDiffSkipsForeignTablesAndSucceeds", func(t *testing.T) {
		task := diff.NewSchemaDiffTask()
		task.ClusterName = env.ClusterName
		task.DBName = env.DBName
		task.SchemaName = fdwSchema
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

	t.Run("ForeignChildOnOneNodeOnly_WarnsAndRefusesRepair", func(t *testing.T) {
		_, err := state.n2Pool.Exec(ctx, fmt.Sprintf("DROP FOREIGN TABLE %s.child_fdw", fdwSchema))
		require.NoError(t, err)
		_, err = state.n2Pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s.child_heap WHERE id=11", fdwSchema))
		require.NoError(t, err)

		task := env.newTableDiffTask(t, parent, nodes)
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.ExecuteTask())
		assert.True(t, task.DiffResult.Summary.ForeignLayoutMismatch)

		diffFile := getLatestDiffFile(t)
		repairTask := env.newTableRepairTask(env.ServiceN1, parent, diffFile)
		err = repairTask.Run(false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "allow-foreign-layout-mismatch")

		repairTask = env.newTableRepairTask(env.ServiceN1, parent, diffFile)
		repairTask.AllowForeignLayoutMismatch = true
		require.NoError(t, repairTask.Run(false))
		env.assertNoTableDiff(t, parent)
	})

	// Keep this last: part_parent has no primary key, so a schema diff run
	// after it exists would report an error for that table.
	t.Run("PartitionedTableWithForeignPartitionExplainsMissingKey", func(t *testing.T) {
		for _, pool := range env.pools() {
			for _, s := range []string{
				fmt.Sprintf("CREATE TABLE %s.part_parent (id int, val text) PARTITION BY RANGE (id)", fdwSchema),
				fmt.Sprintf("CREATE TABLE %s.part_heap PARTITION OF %s.part_parent FOR VALUES FROM (0) TO (100)", fdwSchema, fdwSchema),
				fmt.Sprintf("CREATE FOREIGN TABLE %s.part_fdw PARTITION OF %s.part_parent FOR VALUES FROM (100) TO (200) SERVER ace_test_csv OPTIONS (filename '/tmp/ace_fdw_child.csv', format 'csv')", fdwSchema, fdwSchema),
			} {
				_, err := pool.Exec(ctx, s)
				require.NoError(t, err, s)
			}
		}
		task := env.newTableDiffTask(t, fdwSchema+".part_parent", nodes)
		err := task.RunChecks(false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot have a primary key")
		assert.Contains(t, err.Error(), "part_fdw")
	})
}
