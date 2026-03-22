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
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRepsetDiff_UniDirectional verifies that repset-diff works when the
// repset only exists on one node (publisher), simulating a uni-directional
// replication setup. The table should still be compared across both nodes.
func TestRepsetDiff_UniDirectional(t *testing.T) {
	ctx := context.Background()
	const tableName = "repset_unidir_tbl"
	const repsetName = "default"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	// Create table on both nodes with identical data.
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id  INT PRIMARY KEY,
			val TEXT
		)`, qualifiedTableName)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO %s (id, val) VALUES (1, 'same') ON CONFLICT DO NOTHING`,
			qualifiedTableName))
		require.NoError(t, err)
	}

	// Add to repset on node1 ONLY (simulates publisher-only / uni-directional).
	_, err := pgCluster.Node1Pool.Exec(ctx,
		fmt.Sprintf(`SELECT spock.repset_add_table('%s', '%s');`, repsetName, qualifiedTableName))
	require.NoError(t, err)

	t.Cleanup(func() {
		pgCluster.Node1Pool.Exec(ctx,
			fmt.Sprintf(`SELECT spock.repset_remove_table('%s', '%s');`, repsetName, qualifiedTableName))
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qualifiedTableName))
		}
		files, _ := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
		for _, f := range files {
			os.Remove(f)
		}
	})

	// Capture logger output.
	r, w, err := os.Pipe()
	require.NoError(t, err)
	logger.SetOutput(w)
	t.Cleanup(func() { logger.SetOutput(os.Stderr) })

	task := newTestRepsetDiffTask(repsetName)
	diffErr := diff.RepsetDiff(task)

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	logOutput := buf.String()
	t.Logf("Captured log output:\n%s", logOutput)

	// Should succeed — the table exists on both nodes even though it's only
	// in the repset on node1. Our code discovers it from node1's repset and
	// still compares data across both nodes.
	require.NoError(t, diffErr)

	// The table should have been compared and found identical.
	assert.Contains(t, logOutput, "table(s) are identical",
		"summary should show identical tables")
	assert.Contains(t, logOutput, qualifiedTableName,
		"the uni-directional table should appear in the summary")
}

// TestRepsetDiff_Summary verifies that the repset-diff summary correctly
// reports identical, differing, skipped, and errored tables.
func TestRepsetDiff_Summary(t *testing.T) {
	ctx := context.Background()
	const repsetName = "default"

	const (
		identicalTable = "repset_summary_match"
		divergentTable = "repset_summary_divergent"
		noPKTable      = "repset_summary_nopk"
		skippedTable   = "repset_summary_skip"
	)

	// Create identical table.
	createRepsetDiffTable(t, identicalTable, repsetName, false)

	// Create divergent table.
	createRepsetDiffTable(t, divergentTable, repsetName, true)

	// Create skipped table (divergent, but will be skipped).
	skippedQualified := createRepsetDiffTable(t, skippedTable, repsetName, true)

	// Create table with PK, add to repset, then drop PK — will fail during diff
	// because table-diff requires a primary key. Spock requires a PK to add
	// a table to a repset, so we add first, then remove the constraint.
	noPKQualified := fmt.Sprintf("%s.%s", testSchema, noPKTable)
	createNoPKSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id  INT PRIMARY KEY,
			val TEXT
		)`, noPKQualified)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createNoPKSQL)
		require.NoError(t, err)
		_, err = pool.Exec(ctx,
			fmt.Sprintf(`SELECT spock.repset_add_table('%s', '%s');`, repsetName, noPKQualified))
		require.NoError(t, err)
		// Drop the PK constraint after adding to the repset.
		_, err = pool.Exec(ctx,
			fmt.Sprintf(`ALTER TABLE %s DROP CONSTRAINT %s_pkey`, noPKQualified, noPKTable))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf(
				`SELECT spock.repset_remove_table('%s', '%s');`, repsetName, noPKQualified))
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, noPKQualified))
		}
	})

	// Capture logger output.
	r, w, err := os.Pipe()
	require.NoError(t, err)
	logger.SetOutput(w)
	t.Cleanup(func() { logger.SetOutput(os.Stderr) })

	task := newTestRepsetDiffTask(repsetName)
	task.SkipTables = skippedQualified

	diffErr := diff.RepsetDiff(task)

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	logOutput := buf.String()
	t.Logf("Captured log output:\n%s", logOutput)

	// Should return error because nopk table failed.
	require.Error(t, diffErr)
	assert.Contains(t, diffErr.Error(), "failed during Repset diff")

	// Task status should be FAILED.
	assert.Equal(t, taskstore.StatusFailed, task.Task.TaskStatus)

	// Extract the summary section and verify each table in its correct section.
	summarySection := extractSummarySection(t, logOutput)

	identicalSection := extractBetween(summarySection, "table(s) are identical:", "table(s)")
	assert.Contains(t, identicalSection, identicalTable,
		"identical table should appear in the identical section")

	skippedSection := extractBetween(summarySection, "table(s) were skipped:", "table(s)")
	assert.Contains(t, skippedSection, skippedTable,
		"skipped table should appear in the skipped section")

	diffSection := extractBetween(summarySection, "table(s) have differences:", "table(s)")
	assert.Contains(t, diffSection, divergentTable,
		"divergent table should appear in the differences section")

	errorSection := extractBetween(summarySection, "encountered errors and could not be compared:", "")
	assert.Contains(t, errorSection, noPKTable,
		"no-PK table should appear in the error section")
	assert.Contains(t, errorSection,
		fmt.Sprintf("%s: no primary key found", noPKTable),
		"error reason should appear next to the failed table in the error section")
}

// TestSchemaDiff_SkippedTableNamesInSummary verifies that skipped table names
// are listed in the summary output, not just a count.
func TestSchemaDiff_SkippedTableNamesInSummary(t *testing.T) {
	const skipTableName = "summary_skip_named_tbl"
	nodes := fmt.Sprintf("%s,%s", serviceN1, serviceN2)

	createDivergentTable(t, skipTableName)

	// Capture logger output.
	r, w, err := os.Pipe()
	require.NoError(t, err)
	logger.SetOutput(w)
	t.Cleanup(func() { logger.SetOutput(os.Stderr) })

	task := newTestSchemaDiffTask(testSchema, nodes)
	task.SkipTables = skipTableName

	err = task.SchemaTableDiff()

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	logOutput := buf.String()
	t.Logf("Captured log output:\n%s", logOutput)

	require.NoError(t, err)

	// The skipped table name should appear in the summary, not just a count.
	assert.Contains(t, logOutput, "table(s) were skipped:")
	assert.Contains(t, logOutput, fmt.Sprintf("public.%s", skipTableName),
		"skipped table name should be listed in summary")
}
