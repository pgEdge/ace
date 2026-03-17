// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/stretchr/testify/require"
)

// newTestSchemaDiffTask builds a SchemaDiffCmd wired to the shared test cluster.
func newTestSchemaDiffTask(schemaName, nodes string) *diff.SchemaDiffCmd {
	task := diff.NewSchemaDiffTask()
	task.ClusterName = pgCluster.ClusterName
	task.DBName = dbName
	task.SchemaName = schemaName
	task.Nodes = nodes
	task.Output = "json"
	task.Quiet = true
	task.SkipDBUpdate = true
	task.BlockSize = 10000
	task.CompareUnitSize = 100
	task.ConcurrencyFactor = 1
	return task
}

// createDivergentTable creates tableName on both nodes and inserts rows only on
// node1, so a table-diff run will always find data present on n1 but missing
// on n2.  A t.Cleanup drops the table on both nodes automatically.
func createDivergentTable(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id   INT PRIMARY KEY,
			val  TEXT
		)`, testSchema, tableName)

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err, "create table %s", tableName)
	}

	// Insert rows on node1 only (node2 stays empty → guaranteed data diff).
	_, err := pgCluster.Node1Pool.Exec(ctx,
		fmt.Sprintf(`INSERT INTO %s.%s (id, val) VALUES (1, 'divergent')`, testSchema, tableName),
	)
	require.NoError(t, err, "insert into %s on node1", tableName)

	t.Cleanup(func() {
		dropSQL := fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, testSchema, tableName)
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, dropSQL) //nolint:errcheck – best-effort cleanup
		}
		// Remove any diff files left by this table.
		files, _ := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
		for _, f := range files {
			os.Remove(f)
		}
	})
}

// diffFilesForTable returns all json diff files whose name starts with tableName.
func diffFilesForTable(t *testing.T, tableName string) []string {
	t.Helper()
	files, err := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
	require.NoError(t, err)
	return files
}

// TestSchemaDiff_SkipTablesFlag verifies the core fix: a table listed in
// --skip-tables is not diffed by SchemaTableDiff.
//
// Strategy:
//  1. Create a table that has a data difference between node1 and node2.
//  2. Run SchemaTableDiff with that table in SkipTables → no diff file is
//     created, proving the table was never handed to TableDiffTask.
//  3. Run SchemaTableDiff WITHOUT SkipTables → a diff file IS created,
//     proving the table is diffed normally when not excluded.
func TestSchemaDiff_SkipTablesFlag(t *testing.T) {
	const skipTableName = "schema_diff_skip_test"

	createDivergentTable(t, skipTableName)

	nodes := fmt.Sprintf("%s,%s", serviceN1, serviceN2)

	t.Run("SkippedTableProducesNoDiffFile", func(t *testing.T) {
		task := newTestSchemaDiffTask(testSchema, nodes)
		task.SkipTables = skipTableName

		err := task.SchemaTableDiff()
		require.NoError(t, err)

		files := diffFilesForTable(t, skipTableName)
		require.Empty(t, files,
			"expected no diff file for skipped table %q, found: %v", skipTableName, files)
	})

	t.Run("NotSkippedTableProducesDiffFile", func(t *testing.T) {
		t.Cleanup(func() {
			for _, f := range diffFilesForTable(t, skipTableName) {
				os.Remove(f)
			}
		})

		task := newTestSchemaDiffTask(testSchema, nodes)
		// SkipTables intentionally not set.

		err := task.SchemaTableDiff()
		require.NoError(t, err)

		files := diffFilesForTable(t, skipTableName)
		require.NotEmpty(t, files,
			"expected a diff file for table %q when not skipped", skipTableName)
	})
}

// TestSchemaDiff_SkipTablesFile verifies that --skip-file works end-to-end:
// a file listing a table name suppresses that table just like the inline flag.
func TestSchemaDiff_SkipTablesFile(t *testing.T) {
	const skipTableName = "schema_diff_skipfile_test"

	createDivergentTable(t, skipTableName)

	// Write a skip file containing the table name.
	skipFile, err := os.CreateTemp(t.TempDir(), "skip-*.txt")
	require.NoError(t, err)
	_, err = fmt.Fprintln(skipFile, skipTableName)
	require.NoError(t, err)
	skipFile.Close()

	nodes := fmt.Sprintf("%s,%s", serviceN1, serviceN2)
	task := newTestSchemaDiffTask(testSchema, nodes)
	task.SkipFile = skipFile.Name()

	err = task.SchemaTableDiff()
	require.NoError(t, err)

	files := diffFilesForTable(t, skipTableName)
	require.Empty(t, files,
		"expected no diff file for table listed in skip file, found: %v", files)
}
