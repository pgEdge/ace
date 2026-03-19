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
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRepsetDiffTask builds a RepsetDiffCmd wired to the shared test cluster.
func newTestRepsetDiffTask(repsetName string) *diff.RepsetDiffCmd {
	task := diff.NewRepsetDiffTask()
	task.ClusterName = pgCluster.ClusterName
	task.DBName = dbName
	task.RepsetName = repsetName
	task.Nodes = "all"
	task.Output = "json"
	task.Quiet = true
	task.SkipDBUpdate = true
	task.BlockSize = 10000
	task.CompareUnitSize = 100
	task.ConcurrencyFactor = 1
	return task
}

// createRepsetDiffTable creates a table on both nodes and adds it to the given
// replication set. Data is inserted BEFORE joining the repset so spock does not
// replicate it, allowing controlled divergence.
//
// When divergent is true, node1 gets an extra row (id=99) that node2 does not
// have. Returns the schema-qualified table name. Cleanup is automatic.
func createRepsetDiffTable(t *testing.T, tableName, repsetName string, divergent bool) string {
	t.Helper()
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id   INT PRIMARY KEY,
			val  TEXT
		)`, qualifiedTableName)

	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}
	nodeNames := []string{serviceN1, serviceN2}

	for i, pool := range pools {
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err, "create table on %s", nodeNames[i])
	}

	// Insert baseline rows on both nodes BEFORE adding to repset.
	for i, pool := range pools {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, val) VALUES (1, 'same'), (2, 'same')
			 ON CONFLICT DO NOTHING`, qualifiedTableName))
		require.NoError(t, err, "insert baseline on %s", nodeNames[i])
	}

	if divergent {
		_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, val) VALUES (99, 'only_on_n1')`, qualifiedTableName))
		require.NoError(t, err, "insert divergent row on n1")
	}

	// Now add to repset.
	for i, pool := range pools {
		_, err := pool.Exec(ctx,
			fmt.Sprintf(`SELECT spock.repset_add_table('%s', '%s');`, repsetName, qualifiedTableName))
		require.NoError(t, err, "add table to repset on %s", nodeNames[i])
	}

	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Exec(ctx, fmt.Sprintf(
				`SELECT spock.repset_remove_table('%s', '%s');`, repsetName, qualifiedTableName)) //nolint:errcheck
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qualifiedTableName))   //nolint:errcheck
		}
		files, _ := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
		for _, f := range files {
			os.Remove(f)
		}
	})

	return qualifiedTableName
}

// repsetDiffFilesForTable returns all json diff files for the given table.
func repsetDiffFilesForTable(t *testing.T, tableName string) []string {
	t.Helper()
	files, err := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
	require.NoError(t, err)
	return files
}

// readDiffOutput parses a diff JSON file into a DiffOutput struct.
func readDiffOutput(t *testing.T, path string) types.DiffOutput {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err, "read diff file %s", path)
	var out types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &out), "parse diff file %s", path)
	return out
}

// totalDiffRows returns the sum of row counts across all node pairs in a DiffOutput.
func totalDiffRows(d types.DiffOutput) int {
	total := 0
	for _, pair := range d.NodeDiffs {
		for _, rows := range pair.Rows {
			total += len(rows)
		}
	}
	return total
}

// TestRepsetDiff_IdenticalTables verifies that repset-diff produces no diff
// files when tables in the replication set are identical across nodes.
func TestRepsetDiff_IdenticalTables(t *testing.T) {
	const tableName = "repset_diff_identical"
	createRepsetDiffTable(t, tableName, "default", false)

	task := newTestRepsetDiffTask("default")
	err := diff.RepsetDiff(task)
	require.NoError(t, err)

	files := repsetDiffFilesForTable(t, tableName)
	require.Empty(t, files,
		"expected no diff file for identical table, found: %v", files)
}

// TestRepsetDiff_DivergentTable verifies that repset-diff detects the extra
// row on node1, reports the correct node pair, diff count, and row content.
func TestRepsetDiff_DivergentTable(t *testing.T) {
	const tableName = "repset_diff_divergent"
	createRepsetDiffTable(t, tableName, "default", true)

	task := newTestRepsetDiffTask("default")
	err := diff.RepsetDiff(task)
	require.NoError(t, err)

	files := repsetDiffFilesForTable(t, tableName)
	require.Len(t, files, 1, "expected exactly one diff file")

	diffOut := readDiffOutput(t, files[0])

	// Summary should reference the correct schema/table.
	assert.Equal(t, testSchema, diffOut.Summary.Schema)
	assert.Equal(t, tableName, diffOut.Summary.Table)
	assert.Contains(t, diffOut.Summary.PrimaryKey, "id")

	// Exactly one diff row (id=99 exists on n1 but not n2).
	assert.Equal(t, 1, totalDiffRows(diffOut),
		"expected 1 diff row (id=99 on n1 only)")

	// The row should appear under the n1 key of the node pair.
	for _, pair := range diffOut.NodeDiffs {
		n1Rows := pair.Rows[serviceN1]
		assert.Len(t, n1Rows, 1, "n1 should have 1 extra row")
		if len(n1Rows) > 0 {
			id, _ := n1Rows[0].Get("id")
			assert.Equal(t, float64(99), id, "divergent row should be id=99")
			val, _ := n1Rows[0].Get("val")
			assert.Equal(t, "only_on_n1", val)
		}
		n2Rows := pair.Rows[serviceN2]
		assert.Empty(t, n2Rows, "n2 should have no extra rows")
	}
}

// TestRepsetDiff_SkipTables verifies that --skip-tables suppresses diffing for
// the named table (no diff file), and that without it the diff is produced with
// correct content.
func TestRepsetDiff_SkipTables(t *testing.T) {
	const tableName = "repset_diff_skip"
	qualifiedTableName := createRepsetDiffTable(t, tableName, "default", true)

	t.Run("SkippedTableProducesNoDiffFile", func(t *testing.T) {
		for _, f := range repsetDiffFilesForTable(t, tableName) {
			_ = os.Remove(f)
		}

		task := newTestRepsetDiffTask("default")
		task.SkipTables = qualifiedTableName

		err := diff.RepsetDiff(task)
		require.NoError(t, err)

		files := repsetDiffFilesForTable(t, tableName)
		require.Empty(t, files,
			"expected no diff file for skipped table, found: %v", files)
	})

	t.Run("NotSkippedTableProducesDiffFile", func(t *testing.T) {
		for _, f := range repsetDiffFilesForTable(t, tableName) {
			_ = os.Remove(f)
		}
		t.Cleanup(func() {
			for _, f := range repsetDiffFilesForTable(t, tableName) {
				os.Remove(f)
			}
		})

		task := newTestRepsetDiffTask("default")

		err := diff.RepsetDiff(task)
		require.NoError(t, err)

		files := repsetDiffFilesForTable(t, tableName)
		require.NotEmpty(t, files,
			"expected diff file for table %q when not skipped", tableName)

		diffOut := readDiffOutput(t, files[0])
		assert.Equal(t, 1, totalDiffRows(diffOut),
			"expected 1 diff row (id=99 on n1 only)")
	})
}

// TestRepsetDiff_SkipTablesFile verifies that --skip-file works end-to-end.
func TestRepsetDiff_SkipTablesFile(t *testing.T) {
	const tableName = "repset_diff_skipfile"
	qualifiedTableName := createRepsetDiffTable(t, tableName, "default", true)

	skipFile, err := os.CreateTemp(t.TempDir(), "skip-*.txt")
	require.NoError(t, err)
	_, err = fmt.Fprintln(skipFile, qualifiedTableName)
	require.NoError(t, err)
	skipFile.Close()

	task := newTestRepsetDiffTask("default")
	task.SkipFile = skipFile.Name()

	err = diff.RepsetDiff(task)
	require.NoError(t, err)

	files := repsetDiffFilesForTable(t, tableName)
	require.Empty(t, files,
		"expected no diff file for table listed in skip file, found: %v", files)
}

// TestRepsetDiff_NonExistentRepset verifies that referencing a replication set
// that does not exist produces a clear error.
func TestRepsetDiff_NonExistentRepset(t *testing.T) {
	task := newTestRepsetDiffTask("does_not_exist_repset")

	err := diff.RepsetDiff(task)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found",
		"error should indicate repset was not found")
}

// TestRepsetDiff_MultipleTables verifies that repset-diff processes all tables
// in the replication set: no diff file for the identical table, and a diff file
// with correct content for the divergent one.
func TestRepsetDiff_MultipleTables(t *testing.T) {
	const identicalTable = "repset_multi_identical"
	const divergentTable = "repset_multi_divergent"

	createRepsetDiffTable(t, identicalTable, "default", false)
	createRepsetDiffTable(t, divergentTable, "default", true)

	task := newTestRepsetDiffTask("default")
	err := diff.RepsetDiff(task)
	require.NoError(t, err)

	identicalFiles := repsetDiffFilesForTable(t, identicalTable)
	require.Empty(t, identicalFiles,
		"expected no diff file for identical table, found: %v", identicalFiles)

	divergentFiles := repsetDiffFilesForTable(t, divergentTable)
	require.Len(t, divergentFiles, 1, "expected exactly one diff file for divergent table")

	diffOut := readDiffOutput(t, divergentFiles[0])
	assert.Equal(t, divergentTable, diffOut.Summary.Table)
	assert.Equal(t, 1, totalDiffRows(diffOut),
		"expected 1 diff row for divergent table")

	// Verify the specific row.
	for _, pair := range diffOut.NodeDiffs {
		n1Rows := pair.Rows[serviceN1]
		require.Len(t, n1Rows, 1)
		id, _ := n1Rows[0].Get("id")
		assert.Equal(t, float64(99), id)
	}
}
