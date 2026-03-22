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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemaDiff_Summary verifies that the summary printed at the end of
// schema-diff correctly reports identical, differing, errored, and
// missing-from-node tables.
//
// Setup:
//   - matching_tbl:  identical data on both nodes     → "identical"
//   - divergent_tbl: data only on node1               → "differences"
//   - nopk_tbl:      no primary key on either node    → "error"
//   - n1only_tbl:    exists on node1 only             → "not present on all nodes"
func TestSchemaDiff_Summary(t *testing.T) {
	ctx := context.Background()
	nodes := fmt.Sprintf("%s,%s", serviceN1, serviceN2)

	const (
		matchingTable  = "summary_match_tbl"
		divergentTable = "summary_divergent_tbl"
		noPKTable      = "summary_nopk_tbl"
		n1OnlyTable    = "summary_n1only_tbl"
	)

	ourTables := map[string]bool{
		matchingTable:  true,
		divergentTable: true,
		noPKTable:      true,
		n1OnlyTable:    true,
	}

	// --- Create matching table (identical on both nodes) ---
	createMatchSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id  INT PRIMARY KEY,
			val TEXT
		)`, testSchema, matchingTable)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createMatchSQL)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s.%s (id, val) VALUES (1, 'same') ON CONFLICT DO NOTHING`,
			testSchema, matchingTable))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, testSchema, matchingTable))
		}
	})

	// --- Create divergent table (data only on node1) ---
	createDivergentSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id  INT PRIMARY KEY,
			val TEXT
		)`, testSchema, divergentTable)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createDivergentSQL)
		require.NoError(t, err)
	}
	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s.%s (id, val) VALUES (1, 'only_on_n1') ON CONFLICT DO NOTHING`,
		testSchema, divergentTable))
	require.NoError(t, err)
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, testSchema, divergentTable))
		}
		for _, f := range diffFilesForTable(t, divergentTable) {
			os.Remove(f)
		}
	})

	// --- Create table with no primary key (will error during diff) ---
	createNoPKSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id  INT,
			val TEXT
		)`, testSchema, noPKTable)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createNoPKSQL)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, testSchema, noPKTable))
		}
	})

	// --- Create table on node1 only (missing from node2) ---
	createN1OnlySQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s (
			id  INT PRIMARY KEY,
			val TEXT
		)`, testSchema, n1OnlyTable)
	_, err = pgCluster.Node1Pool.Exec(ctx, createN1OnlySQL)
	require.NoError(t, err)
	t.Cleanup(func() {
		pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, testSchema, n1OnlyTable))
	})

	// --- Capture logger output ---
	r, w, err := os.Pipe()
	require.NoError(t, err)
	logger.SetOutput(w)
	t.Cleanup(func() {
		logger.SetOutput(os.Stderr)
	})

	// --- Run schema-diff ---
	task := newTestSchemaDiffTask(testSchema, nodes)
	task.SkipDBUpdate = true

	// Skip tables that belong to other tests so only our tables are diffed.
	// Query both nodes for the union (since RunChecks now does that).
	skipList := ""
	allTables := getAllTablesUnion(t,
		[]*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}, testSchema)
	for _, tbl := range allTables {
		if !ourTables[tbl] {
			if skipList != "" {
				skipList += ","
			}
			skipList += tbl
		}
	}
	task.SkipTables = skipList

	diffErr := task.SchemaTableDiff()

	// --- Read captured log output ---
	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	logOutput := buf.String()
	t.Logf("Captured log output:\n%s", logOutput)

	// --- Assertions ---

	// Should return an error because nopk table failed
	require.Error(t, diffErr, "expected error due to table without primary key")
	assert.Contains(t, diffErr.Error(), "failed during Schema diff")

	// Task status should be FAILED
	assert.Equal(t, taskstore.StatusFailed, task.Task.TaskStatus,
		"task status should be FAILED when a table errors")

	// Extract the summary section (everything after "Schema diff summary:").
	summarySection := extractSummarySection(t, logOutput)

	// Verify each table appears in its correct section within the summary.
	identicalSection := extractBetween(summarySection, "table(s) are identical:", "table(s)")
	assert.Contains(t, identicalSection, matchingTable,
		"matching table should appear in the identical section")

	skippedSection := extractBetween(summarySection, "table(s) were skipped:", "table(s)")
	assert.NotEmpty(t, skippedSection, "should have a skipped section")

	diffSection := extractBetween(summarySection, "table(s) have differences:", "table(s)")
	assert.Contains(t, diffSection, divergentTable,
		"divergent table should appear in the differences section")

	missingSection := extractBetween(summarySection, "not present on all nodes:", "table(s)")
	assert.Contains(t, missingSection, n1OnlyTable,
		"node1-only table should appear in the missing section")
	assert.Contains(t, missingSection, fmt.Sprintf("missing from [%s]", serviceN2),
		"missing section should say which node the table is missing from")

	errorSection := extractBetween(summarySection, "encountered errors and could not be compared:", "")
	assert.Contains(t, errorSection, noPKTable,
		"no-PK table should appear in the error section")
	assert.Contains(t, errorSection,
		fmt.Sprintf("%s: no primary key found", noPKTable),
		"error reason should appear next to the failed table in the error section")

	// Diff file should exist for the divergent table
	files := diffFilesForTable(t, divergentTable)
	assert.NotEmpty(t, files, "expected a diff file for the divergent table")

	// No diff file for the matching table
	matchFiles, _ := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, matchingTable))
	assert.Empty(t, matchFiles, "no diff file expected for matching table")
}

// extractSummarySection returns everything in logOutput from "summary:" onward.
func extractSummarySection(t *testing.T, logOutput string) string {
	t.Helper()
	lines := strings.Split(logOutput, "\n")
	var result []string
	found := false
	for _, line := range lines {
		if strings.Contains(line, "summary:") {
			found = true
		}
		if found {
			result = append(result, line)
		}
	}
	require.True(t, found, "summary section not found in log output")
	return strings.Join(result, "\n")
}

// extractBetween returns the text between a startMarker and the next line
// containing endMarker. If endMarker is empty, returns everything from
// startMarker to the end. Both markers are searched within the summary text.
func extractBetween(text, startMarker, endMarker string) string {
	lines := strings.Split(text, "\n")
	var result []string
	capturing := false
	for _, line := range lines {
		if !capturing {
			if strings.Contains(line, startMarker) {
				capturing = true
			}
			continue
		}
		// Stop at next section header (if endMarker is set and found).
		if endMarker != "" && strings.Contains(line, endMarker) {
			break
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

// getAllTablesUnion returns the union of table names across multiple node pools.
func getAllTablesUnion(t *testing.T, pools []*pgxpool.Pool, schema string) []string {
	t.Helper()
	seen := make(map[string]bool)
	for _, pool := range pools {
		rows, err := pool.Query(context.Background(),
			`SELECT table_name FROM information_schema.tables
			 WHERE table_schema = $1 AND table_type = 'BASE TABLE'
			 ORDER BY table_name`, schema)
		require.NoError(t, err)
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			seen[name] = true
		}
		rows.Close()
	}
	tables := make([]string, 0, len(seen))
	for name := range seen {
		tables = append(tables, name)
	}
	return tables
}
