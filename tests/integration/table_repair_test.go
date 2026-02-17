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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pkgLogger "github.com/pgedge/ace/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: Verify replication works and only then assert for correct counts.
// setupDivergence prepares the 'customers' table with a known set of differences between node1 and node2.
// - 5 common rows
// - 2 rows only on node1 (IDs 1001, 1002)
// - 2 rows only on node2 (IDs 2001, 2002)
// - 2 common rows modified on node2 (IDs 1, 2)
func setupDivergence(t *testing.T, ctx context.Context, qualifiedTableName string, composite bool) {
	t.Helper()
	log.Println("Setting up data divergence for", qualifiedTableName)

	// Truncate on both nodes
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err, "Failed to enable repair mode on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
		require.NoError(t, err, "Failed to truncate table on node %s", nodeName)
		_, err = pool.Exec(ctx, "SELECT spock.repair_mode(false)")
		require.NoError(t, err, "Failed to disable repair mode on %s", nodeName)
	}

	// Insert common rows (always populate customer_id to support composite PK later)
	for i := 1; i <= 5; i++ {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(ctx, "SELECT spock.repair_mode(true)")
			require.NoError(t, err)
			_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email) VALUES ($1, $2, $3, $4, $5)", qualifiedTableName),
				i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("FirstName%d", i), fmt.Sprintf("LastName%d", i), fmt.Sprintf("email%d@example.com", i))
			require.NoError(t, err)
			_, err = pool.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err)
		}
	}

	// Insert rows only on node1 (always include customer_id)
	_, err := pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	for i := 1001; i <= 1002; i++ {
		_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email) VALUES ($1, $2, $3, $4, $5)", qualifiedTableName),
			i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("N1OnlyFirst%d", i), fmt.Sprintf("N1OnlyLast%d", i), fmt.Sprintf("n1.only%d@example.com", i))
		require.NoError(t, err)
	}
	_, err = pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)

	// Insert rows only on node2 (always include customer_id)
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	for i := 2001; i <= 2002; i++ {
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email) VALUES ($1, $2, $3, $4, $5)", qualifiedTableName),
			i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("N2OnlyFirst%d", i), fmt.Sprintf("N2OnlyLast%d", i), fmt.Sprintf("n2.only%d@example.com", i))
		require.NoError(t, err)
	}
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)

	// Modify rows on node2
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	for i := 1; i <= 2; i++ {
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = $1 WHERE index = $2", qualifiedTableName),
			fmt.Sprintf("modified.email%d@example.com", i), i)
		require.NoError(t, err)
	}
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)

	log.Println("Data divergence setup complete.")
}

// runTableDiff executes a table-diff task and returns the path to the latest diff file.
func runTableDiff(t *testing.T, qualifiedTableName string, nodesToCompare []string) string {
	t.Helper()
	// Clean up any old diff files to ensure we get the correct one
	files, _ := filepath.Glob("*_diffs-*.json")
	for _, f := range files {
		os.Remove(f)
	}

	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)
	err := tdTask.RunChecks(false)
	require.NoError(t, err, "table-diff validation failed")
	err = tdTask.ExecuteTask()
	require.NoError(t, err, "table-diff execution failed")

	latestDiffFile := getLatestDiffFile(t)
	require.NotEmpty(t, latestDiffFile, "No diff file was generated")

	return latestDiffFile
}

// getLatestDiffFile finds the most recently modified diff file.
func getLatestDiffFile(t *testing.T) string {
	t.Helper()
	files, err := filepath.Glob("*_diffs-*.json")
	require.NoError(t, err, "Failed to glob for diff files")
	if len(files) == 0 {
		return ""
	}

	sort.Slice(files, func(i, j int) bool {
		fi, errI := os.Stat(files[i])
		require.NoError(t, errI)
		fj, errJ := os.Stat(files[j])
		require.NoError(t, errJ)
		return fi.ModTime().After(fj.ModTime())
	})

	return files[0]
}

// assertNoTableDiff runs a diff and asserts that there are no differences.
func assertNoTableDiff(t *testing.T, qualifiedTableName string) {
	t.Helper()
	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err := tdTask.RunChecks(false)
	require.NoError(t, err, "assertNoTableDiff: validation failed")

	err = tdTask.ExecuteTask()
	require.NoError(t, err, "assertNoTableDiff: execution failed")

	assert.Empty(t, tdTask.DiffResult.NodeDiffs, "Expected no differences after repair, but diffs were found")
}

// captureOutput executes a function while capturing its stdout and stderr.
func captureOutput(t *testing.T, task func()) string {
	t.Helper()
	oldStdout := os.Stdout
	oldStderr := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	os.Stderr = w

	// Redirect package logger to capture WARN logs
	pkgLogger.SetOutput(w)

	task()

	err = w.Close()
	require.NoError(t, err)
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	// Restore package logger
	pkgLogger.SetOutput(oldStderr)

	var buf bytes.Buffer
	_, err = io.Copy(&buf, r)
	require.NoError(t, err)
	return buf.String()
}

// getTableCounts returns the row counts for a table on both nodes.
func getTableCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, qualifiedTableName string) int {
	var count int
	err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&count)
	require.NoError(t, err, "Failed to count rows")
	return count
}

func setupNullDivergence(t *testing.T, ctx context.Context, qualifiedTableName string) {
	t.Helper()
	log.Println("Setting up null divergence for", qualifiedTableName)

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err, "Failed to enable repair mode on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
		require.NoError(t, err, "Failed to truncate table on node %s", nodeName)
		_, err = pool.Exec(ctx, "SELECT spock.repair_mode(false)")
		require.NoError(t, err, "Failed to disable repair mode on %s", nodeName)
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (index, customer_id, first_name, last_name, city) VALUES ($1, $2, $3, $4, $5)",
		qualifiedTableName,
	)

	// Node1 rows: missing city for id 1, missing first_name for id 2
	_, err := pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	_, err = pgCluster.Node1Pool.Exec(ctx, insertSQL, 1, "CUST-1", "Michael", "Schumacher", nil)
	require.NoError(t, err)
	_, err = pgCluster.Node1Pool.Exec(ctx, insertSQL, 2, "CUST-2", nil, "Alonso", "Oviedo")
	require.NoError(t, err)
	_, err = pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)

	// Node2 rows: missing last_name for id 1, missing city for id 2
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, insertSQL, 1, "CUST-1", "Michael", nil, "Austria")
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, insertSQL, 2, "CUST-2", "Fernando", "Alonso", nil)
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
}

type nameCity struct {
	first *string
	last  *string
	city  *string
}

func getNameCity(t *testing.T, ctx context.Context, pool *pgxpool.Pool, qualifiedTableName string, index int, customerID string) nameCity {
	t.Helper()
	var first, last, city *string
	err := pool.QueryRow(
		ctx,
		fmt.Sprintf("SELECT first_name, last_name, city FROM %s WHERE index = $1 AND customer_id = $2", qualifiedTableName),
		index, customerID,
	).Scan(&first, &last, &city)
	require.NoError(t, err, "Failed to fetch row %d/%s", index, customerID)
	return nameCity{first: first, last: last, city: city}
}

func TestTableRepair_UnidirectionalDefault(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			setupDivergence(t, ctx, qualifiedTableName, tc.composite)
			t.Cleanup(func() {
				repairTable(t, qualifiedTableName, serviceN1)
			})

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

			diffData, err := os.ReadFile(diffFile)
			require.NoError(t, err)
			assert.Contains(t, string(diffData), "_spock_metadata_", "Diff file should contain spock metadata before repair")

			repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
			err = repairTask.Run(false)
			require.NoError(t, err, "Table repair failed")

			log.Println("Verifying repair for TestTableRepair_UnidirectionalDefault")
			assertNoTableDiff(t, qualifiedTableName)
			count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
			count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
			assert.Equal(t, count1, count2, "Row counts should be equal after default repair")
			assert.Equal(t, 7, count1, "Expected 7 rows on node1")
		})
	}
}

func TestTableRepair_InsertOnly(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			setupDivergence(t, ctx, qualifiedTableName, tc.composite)
			t.Cleanup(func() {
				repairTable(t, qualifiedTableName, serviceN1)
			})

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

			repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
			repairTask.InsertOnly = true
			err := repairTask.Run(false)
			require.NoError(t, err, "Table repair (insert-only) failed")

			count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
			count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
			assert.Equal(t, 7, count1)
			assert.Equal(t, 9, count2)

			tdTask := newTestTableDiffTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
			err = tdTask.RunChecks(false)
			require.NoError(t, err)
			err = tdTask.ExecuteTask()
			require.NoError(t, err)

			pairKey := serviceN1 + "/" + serviceN2
			if strings.Compare(serviceN1, serviceN2) > 0 {
				pairKey = serviceN2 + "/" + serviceN1
			}
			// After insert-only repair with n1 as source (DO NOTHING on conflict):
			// - Rows 1001, 1002 (n1-only) are inserted into n2
			// - Rows 1, 2 (conflicting) are NOT overwritten on n2 (DO NOTHING), so they still differ
			// - n1 has 2 differing rows (its versions of rows 1, 2)
			// - n2 has 4 differing rows (its versions of rows 1, 2 + unique rows 2001, 2002)
			// - Total diff count is 4
			assert.Equal(t, 2, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN1]))
			assert.Equal(t, 4, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN2]))
			assert.Equal(t, 4, tdTask.DiffResult.Summary.DiffRowsCount[pairKey])
		})
	}
}

func TestTableRepair_UpsertOnly(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			setupDivergence(t, ctx, qualifiedTableName, tc.composite)
			t.Cleanup(func() {
				repairTable(t, qualifiedTableName, serviceN1)
			})

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

			repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
			repairTask.UpsertOnly = true
			err := repairTask.Run(false)
			require.NoError(t, err, "Table repair (upsert-only) failed")

			count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
			count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
			assert.Equal(t, 7, count1)
			assert.Equal(t, 9, count2)

			tdTask := newTestTableDiffTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
			err = tdTask.RunChecks(false)
			require.NoError(t, err)
			err = tdTask.ExecuteTask()
			require.NoError(t, err)

			pairKey := serviceN1 + "/" + serviceN2
			if strings.Compare(serviceN1, serviceN2) > 0 {
				pairKey = serviceN2 + "/" + serviceN1
			}
			assert.Equal(t, 0, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN1]))
			assert.Equal(t, 2, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN2]))
			assert.Equal(t, 2, tdTask.DiffResult.Summary.DiffRowsCount[pairKey])
		})
	}
}

func TestTableRepair_Bidirectional(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			t.Cleanup(func() {
				repairTable(t, qualifiedTableName, serviceN1)
			})

			log.Println("Setting up data for bidirectional test")
			for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
				_, err := pool.Exec(ctx, "SELECT spock.repair_mode(true)")
				require.NoError(t, err)
				_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
				require.NoError(t, err)
				_, err = pool.Exec(ctx, "SELECT spock.repair_mode(false)")
				require.NoError(t, err)
			}
			_, err := pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
			require.NoError(t, err)
			for i := 3001; i <= 3003; i++ {
				_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name) VALUES ($1, $2, $3)", qualifiedTableName), i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("N1-Bi-%d", i))
				require.NoError(t, err)
			}
			_, err = pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err)

			_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
			require.NoError(t, err)
			for i := 4001; i <= 4002; i++ {
				_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name) VALUES ($1, $2, $3)", qualifiedTableName), i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("N2-Bi-%d", i))
				require.NoError(t, err)
			}
			_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err)

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})
			repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
			repairTask.Bidirectional = true
			err = repairTask.Run(false)
			require.NoError(t, err, "Table repair (bidirectional) failed")

			log.Println("Verifying repair for TestTableRepair_Bidirectional")
			assertNoTableDiff(t, qualifiedTableName)
			count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
			count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
			assert.Equal(t, 5, count1, "Expected 5 rows on node1 after bidirectional repair")
			assert.Equal(t, 5, count2, "Expected 5 rows on node2 after bidirectional repair")
		})
	}
}

func TestTableRepair_DryRun(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			setupDivergence(t, ctx, qualifiedTableName, tc.composite)
			t.Cleanup(func() {
				repairTable(t, qualifiedTableName, serviceN1)
			})
			count1Before, count2Before := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName), getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})
			repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
			repairTask.DryRun = true

			output := captureOutput(t, func() {
				err := repairTask.Run(false)
				require.NoError(t, err, "Table repair (dry-run) failed")
			})

			assert.Contains(t, output, "DRY RUN")
			assert.Contains(t, output, fmt.Sprintf("Node %s: Would attempt to UPSERT 4 rows and DELETE 2 rows.", serviceN2))

			count1After, count2After := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName), getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
			assert.Equal(t, count1Before, count1After, "Node1 count should not change after dry run")
			assert.Equal(t, count2Before, count2After, "Node2 count should not change after dry run")
		})
	}
}

func TestTableRepair_GenerateReport(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()
	reportDir := "reports"

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			t.Cleanup(func() {
				os.RemoveAll(reportDir)
				repairTable(t, qualifiedTableName, serviceN1)
			})
			os.RemoveAll(reportDir)

			setupDivergence(t, ctx, qualifiedTableName, tc.composite)

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})
			repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
			repairTask.GenerateReport = true

			err := repairTask.Run(false)
			require.NoError(t, err, "Table repair (generate-report) failed")

			dateFolderName := time.Now().Format("2006-01-02")
			reportPath := filepath.Join(reportDir, dateFolderName)
			files, err := os.ReadDir(reportPath)
			require.NoError(t, err, "Failed to read report directory")
			assert.NotEmpty(t, files, "Report directory should not be empty")

			var reportFile string
			for _, file := range files {
				if strings.HasPrefix(file.Name(), "repair_report_") && strings.HasSuffix(file.Name(), ".json") {
					reportFile = filepath.Join(reportPath, file.Name())
					break
				}
			}
			require.NotEmpty(t, reportFile, "No repair report file found")

			log.Printf("Found report file: %s", reportFile)
			data, err := os.ReadFile(reportFile)
			require.NoError(t, err)
			assert.True(t, len(data) > 0, "Report file is empty")
			assert.Contains(t, string(data), "\"operation_type\": \"table-repair\"")
			assert.Contains(t, string(data), fmt.Sprintf("\"source_of_truth\": \"%s\"", serviceN1))
		})
	}
}

func TestTableRepair_VariousDataTypes(t *testing.T) {
	ctx := context.Background()
	tableName := "data_type_repair"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	refTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	createDataTypeTableSQL := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS "%s";
CREATE TABLE IF NOT EXISTS %s.%s (
    id INT PRIMARY KEY,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_numeric NUMERIC(10, 2),
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_varchar VARCHAR(100),
    col_text TEXT,
    col_char CHAR(10),
    col_boolean BOOLEAN,
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    col_interval INTERVAL,
    col_jsonb JSONB,
    col_json JSON,
    col_bytea BYTEA,
    col_int_array INT[],
	col_text_array TEXT[]
);`, testSchema, testSchema, tableName)

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, createDataTypeTableSQL)
		require.NoErrorf(t, err, "Failed to create data type table on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
		require.NoErrorf(t, err, "Failed to truncate data type table on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf(`SELECT spock.repset_add_table('default', '%s');`, qualifiedTableName))
		require.NoErrorf(t, err, "Failed to add table to repset on %s", nodeName)
	}

	t.Cleanup(func() {
		_, _ = pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(`SELECT spock.repset_remove_table('default', '%s');`, qualifiedTableName))
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifiedTableName))
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			_ = os.Remove(f)
		}
	})

	insertRow := func(pool *pgxpool.Pool, data map[string]any) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err)

		_, err = tx.Exec(
			ctx,
			fmt.Sprintf(`INSERT INTO %s (id, col_smallint, col_integer, col_bigint, col_numeric, col_real, col_double, col_varchar, col_text, col_char, col_boolean, col_date, col_timestamp, col_timestamptz, col_interval, col_jsonb, col_json, col_bytea, col_int_array, col_text_array)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)`, qualifiedTableName),
			data["id"], data["col_smallint"], data["col_integer"], data["col_bigint"],
			data["col_numeric"], data["col_real"], data["col_double"], data["col_varchar"],
			data["col_text"], data["col_char"], data["col_boolean"], data["col_date"],
			data["col_timestamp"], data["col_timestamptz"], data["col_interval"], data["col_jsonb"], data["col_json"],
			data["col_bytea"], data["col_int_array"], data["col_text_array"],
		)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}

	row1 := map[string]any{
		"id": 1, "col_smallint": int16(10), "col_integer": int32(100), "col_bigint": int64(1000),
		"col_numeric": "123.45", "col_real": float32(1.23), "col_double": float64(4.56789),
		"col_varchar": "varchar_data", "col_text": "text_data", "col_char": "char_data",
		"col_boolean": true, "col_date": refTime.Format("2006-01-02"),
		"col_timestamp": refTime, "col_timestamptz": refTime, "col_interval": "1 day 02:03:04",
		"col_jsonb": `{"key": "value1"}`, "col_json": `{"key": "value1"}`,
		"col_bytea": []byte("bytea_row1"), "col_int_array": []int32{1, 2, 3},
		"col_text_array": []string{"alpha", "beta"},
	}
	row2OnlyN1 := map[string]any{
		"id": 2, "col_smallint": int16(20), "col_integer": int32(200), "col_bigint": int64(2000),
		"col_numeric": "222.22", "col_real": float32(2.34), "col_double": float64(5.6789),
		"col_varchar": "only_on_n1", "col_text": "text_row2", "col_char": "char_row2",
		"col_boolean": false, "col_date": refTime.AddDate(0, 1, 0).Format("2006-01-02"),
		"col_timestamp": refTime.Add(time.Hour), "col_timestamptz": refTime.Add(time.Hour), "col_interval": "2 days",
		"col_jsonb": `{"row": 2}`, "col_json": `{"row": 2}`,
		"col_bytea": []byte("bytea_row2"), "col_int_array": []int32{4, 5},
		"col_text_array": []string{"only", "n1"},
	}
	row3Base := map[string]any{
		"id": 3, "col_smallint": int16(30), "col_integer": int32(300), "col_bigint": int64(3000),
		"col_numeric": "333.33", "col_real": float32(3.45), "col_double": float64(6.789),
		"col_varchar": "baseline_varchar", "col_text": "baseline_text", "col_char": "char_row3",
		"col_boolean": true, "col_date": refTime.AddDate(0, 2, 0).Format("2006-01-02"),
		"col_timestamp": refTime.Add(2 * time.Hour), "col_timestamptz": refTime.Add(2 * time.Hour), "col_interval": "3 hours",
		"col_jsonb": `{"status": "good"}`, "col_json": `{"status": "good"}`,
		"col_bytea": []byte("bytea_row3"), "col_int_array": []int32{7, 8, 9},
		"col_text_array": []string{"one", "two", "three"},
	}
	row3ModifiedN2 := map[string]any{
		"id": 3, "col_smallint": int16(30), "col_integer": int32(300), "col_bigint": int64(3000),
		"col_numeric": "999.99", "col_real": float32(9.99), "col_double": float64(99.99),
		"col_varchar": "modified_on_n2", "col_text": "modified_text", "col_char": "char_mod",
		"col_boolean": false, "col_date": refTime.AddDate(0, 3, 0).Format("2006-01-02"),
		"col_timestamp": refTime.Add(3 * time.Hour), "col_timestamptz": refTime.Add(3 * time.Hour), "col_interval": "9 hours",
		"col_jsonb": `{"status": "bad"}`, "col_json": `{"status": "bad"}`,
		"col_bytea": []byte("bytea_row3_mod"), "col_int_array": []int32{9, 9, 9},
		"col_text_array": []string{"nine", "nine", "nine"},
	}
	row4OnlyN2 := map[string]any{
		"id": 4, "col_smallint": int16(40), "col_integer": int32(400), "col_bigint": int64(4000),
		"col_numeric": "444.44", "col_varchar": "only_on_n2",
	}

	insertRow(pgCluster.Node1Pool, row1)
	insertRow(pgCluster.Node2Pool, row1)
	insertRow(pgCluster.Node1Pool, row2OnlyN1)
	insertRow(pgCluster.Node1Pool, row3Base)
	insertRow(pgCluster.Node2Pool, row3ModifiedN2)
	insertRow(pgCluster.Node2Pool, row4OnlyN2)

	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
	err := repairTask.Run(false)
	require.NoError(t, err, "Table repair for various data types failed")

	assertNoTableDiff(t, qualifiedTableName)

	checkRow3 := func(pool *pgxpool.Pool) map[string]any {
		var (
			colNumeric  string
			colJSONB    string
			colBytea    []byte
			colIntArr   []int32
			colTxtArr   []string
			colInterval string
		)
		err := pool.QueryRow(
			ctx,
			fmt.Sprintf(`SELECT col_numeric::text, col_jsonb::text, col_bytea, col_int_array, col_text_array, col_interval::text FROM %s WHERE id = 3`, qualifiedTableName),
		).Scan(&colNumeric, &colJSONB, &colBytea, &colIntArr, &colTxtArr, &colInterval)
		require.NoError(t, err)
		var jsonVal map[string]any
		require.NoError(t, json.Unmarshal([]byte(colJSONB), &jsonVal))
		return map[string]any{
			"col_numeric":  colNumeric,
			"col_jsonb":    jsonVal,
			"col_bytea":    colBytea,
			"col_int_arr":  colIntArr,
			"col_txt_arr":  colTxtArr,
			"col_interval": colInterval,
		}
	}

	row3N1 := checkRow3(pgCluster.Node1Pool)
	row3N2 := checkRow3(pgCluster.Node2Pool)

	assert.Equal(t, row3N1["col_numeric"], row3N2["col_numeric"])
	assert.Equal(t, row3N1["col_jsonb"], row3N2["col_jsonb"])
	assert.Equal(t, row3N1["col_int_arr"], row3N2["col_int_arr"])
	assert.Equal(t, row3N1["col_txt_arr"], row3N2["col_txt_arr"])
	assert.Equal(t, row3N1["col_interval"], row3N2["col_interval"])
	assert.True(t, bytes.Equal(row3N1["col_bytea"].([]byte), row3N2["col_bytea"].([]byte)))

	var row4Count int
	err = pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = 4", qualifiedTableName)).Scan(&row4Count)
	require.NoError(t, err)
	assert.Equal(t, 0, row4Count, "Row present only on node2 should be deleted")
}

func TestTableRepair_FixNulls(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			setupNullDivergence(t, ctx, qualifiedTableName)

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

			repairTask := newTestTableRepairTask("", qualifiedTableName, diffFile)
			repairTask.SourceOfTruth = ""
			repairTask.FixNulls = true

			err := repairTask.Run(false)
			require.NoError(t, err, "Table repair (fix-nulls) failed")

			assertNoTableDiff(t, qualifiedTableName)

			row1N1 := getNameCity(t, ctx, pgCluster.Node1Pool, qualifiedTableName, 1, "CUST-1")
			row1N2 := getNameCity(t, ctx, pgCluster.Node2Pool, qualifiedTableName, 1, "CUST-1")
			row2N1 := getNameCity(t, ctx, pgCluster.Node1Pool, qualifiedTableName, 2, "CUST-2")
			row2N2 := getNameCity(t, ctx, pgCluster.Node2Pool, qualifiedTableName, 2, "CUST-2")

			require.NotNil(t, row1N1.city)
			require.NotNil(t, row1N2.last)
			require.NotNil(t, row2N1.first)
			require.NotNil(t, row2N2.city)

			assert.Equal(t, "Austria", *row1N1.city)
			assert.Equal(t, "Schumacher", *row1N2.last)
			assert.Equal(t, "Fernando", *row2N1.first)
			assert.Equal(t, "Oviedo", *row2N2.city)
		})
	}
}

func TestTableRepair_FixNulls_DryRun(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			setupNullDivergence(t, ctx, qualifiedTableName)

			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})
			repairTask := newTestTableRepairTask("", qualifiedTableName, diffFile)
			repairTask.SourceOfTruth = ""
			repairTask.FixNulls = true
			repairTask.DryRun = true

			output := captureOutput(t, func() {
				err := repairTask.Run(false)
				require.NoError(t, err, "Table repair (fix-nulls dry-run) failed")
			})

			assert.Contains(t, output, "fix-nulls")
			assert.Contains(t, output, "Would update")

			// Ensure data unchanged
			row1N1 := getNameCity(t, ctx, pgCluster.Node1Pool, qualifiedTableName, 1, "CUST-1")
			row1N2 := getNameCity(t, ctx, pgCluster.Node2Pool, qualifiedTableName, 1, "CUST-1")
			row2N1 := getNameCity(t, ctx, pgCluster.Node1Pool, qualifiedTableName, 2, "CUST-2")
			row2N2 := getNameCity(t, ctx, pgCluster.Node2Pool, qualifiedTableName, 2, "CUST-2")

			assert.Nil(t, row1N1.city)
			assert.Nil(t, row1N2.last)
			assert.Nil(t, row2N1.first)
			assert.Nil(t, row2N2.city)

			// Cleanup: actually repair to leave table consistent for subsequent tests
			fixTask := newTestTableRepairTask("", qualifiedTableName, diffFile)
			fixTask.SourceOfTruth = ""
			fixTask.FixNulls = true
			err := fixTask.Run(false)
			require.NoError(t, err, "Cleanup fix-nulls repair failed")
		})
	}
}

// TestTableRepair_FixNulls_BidirectionalUpdate tests that when both nodes have NULLs
// in different columns for the same row, fix-nulls performs bidirectional updates.
// This verifies the behavior discussed in code review: each node updates the other
// with its non-NULL values.
//
// Example scenario:
//
//	Node1: {id: 1, col_a: NULL, col_b: "value_b", col_c: NULL}
//	Node2: {id: 1, col_a: "value_a", col_b: NULL, col_c: "value_c"}
//
// Expected result after fix-nulls:
//
//	Node1: {id: 1, col_a: "value_a", col_b: "value_b", col_c: "value_c"}
//	Node2: {id: 1, col_a: "value_a", col_b: "value_b", col_c: "value_c"}
func TestTableRepair_FixNulls_BidirectionalUpdate(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	testCases := []struct {
		name      string
		composite bool
		setup     func()
		teardown  func()
	}{
		{name: "simple_primary_key", composite: false, setup: func() {}, teardown: func() {}},
		{
			name:      "composite_primary_key",
			composite: true,
			setup: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
			teardown: func() {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					_err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
					require.NoError(t, _err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup()
			t.Cleanup(tc.teardown)

			log.Println("Setting up bidirectional NULL divergence for", qualifiedTableName)

			// Clean table on both nodes
			for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
				nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
				_, err := pool.Exec(ctx, "SELECT spock.repair_mode(true)")
				require.NoError(t, err, "Failed to enable repair mode on %s", nodeName)
				_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
				require.NoError(t, err, "Failed to truncate table on node %s", nodeName)
				_, err = pool.Exec(ctx, "SELECT spock.repair_mode(false)")
				require.NoError(t, err, "Failed to disable repair mode on %s", nodeName)
			}

			// Insert row with complementary NULLs on each node
			// Row 1: Node1 has NULL in first_name and city, Node2 has NULL in last_name and email
			// Row 2: Node1 has NULL in last_name and email, Node2 has NULL in first_name and city
			insertSQL := fmt.Sprintf(
				"INSERT INTO %s (index, customer_id, first_name, last_name, city, email) VALUES ($1, $2, $3, $4, $5, $6)",
				qualifiedTableName,
			)

			// Node1 data
			_, err := pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
			require.NoError(t, err)
			// Row 1 on Node1: NULL first_name and city
			_, err = pgCluster.Node1Pool.Exec(ctx, insertSQL, 100, "CUST-100", nil, "LastName100", nil, "email100@example.com")
			require.NoError(t, err)
			// Row 2 on Node1: NULL last_name and email
			_, err = pgCluster.Node1Pool.Exec(ctx, insertSQL, 200, "CUST-200", "FirstName200", nil, "City200", nil)
			require.NoError(t, err)
			_, err = pgCluster.Node1Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err)

			// Node2 data
			_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
			require.NoError(t, err)
			// Row 1 on Node2: NULL last_name and email
			_, err = pgCluster.Node2Pool.Exec(ctx, insertSQL, 100, "CUST-100", "FirstName100", nil, "City100", nil)
			require.NoError(t, err)
			// Row 2 on Node2: NULL first_name and city
			_, err = pgCluster.Node2Pool.Exec(ctx, insertSQL, 200, "CUST-200", nil, "LastName200", nil, "email200@example.com")
			require.NoError(t, err)
			_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err)

			// Run table-diff to detect the NULL differences
			diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

			// Run fix-nulls repair
			repairTask := newTestTableRepairTask("", qualifiedTableName, diffFile)
			repairTask.SourceOfTruth = ""
			repairTask.FixNulls = true

			err = repairTask.Run(false)
			require.NoError(t, err, "Table repair (fix-nulls bidirectional) failed")

			// Verify bidirectional updates happened
			// Helper to fetch all columns for a row
			type fullRow struct {
				firstName *string
				lastName  *string
				city      *string
				email     *string
			}
			getFullRow := func(pool *pgxpool.Pool, index int, customerID string) fullRow {
				var fr fullRow
				err := pool.QueryRow(
					ctx,
					fmt.Sprintf("SELECT first_name, last_name, city, email FROM %s WHERE index = $1 AND customer_id = $2", qualifiedTableName),
					index, customerID,
				).Scan(&fr.firstName, &fr.lastName, &fr.city, &fr.email)
				require.NoError(t, err, "Failed to fetch row %d/%s", index, customerID)
				return fr
			}

			// Check Row 1 (id=100) on both nodes
			row1N1 := getFullRow(pgCluster.Node1Pool, 100, "CUST-100")
			row1N2 := getFullRow(pgCluster.Node2Pool, 100, "CUST-100")

			// Node1's NULLs (first_name, city) should be filled from Node2
			require.NotNil(t, row1N1.firstName, "Node1 row 100 first_name should be filled from Node2")
			require.NotNil(t, row1N1.city, "Node1 row 100 city should be filled from Node2")
			assert.Equal(t, "FirstName100", *row1N1.firstName, "Node1 row 100 first_name should match Node2's value")
			assert.Equal(t, "City100", *row1N1.city, "Node1 row 100 city should match Node2's value")

			// Node2's NULLs (last_name, email) should be filled from Node1
			require.NotNil(t, row1N2.lastName, "Node2 row 100 last_name should be filled from Node1")
			require.NotNil(t, row1N2.email, "Node2 row 100 email should be filled from Node1")
			assert.Equal(t, "LastName100", *row1N2.lastName, "Node2 row 100 last_name should match Node1's value")
			assert.Equal(t, "email100@example.com", *row1N2.email, "Node2 row 100 email should match Node1's value")

			// Both nodes should now have complete row 1
			assert.Equal(t, "FirstName100", *row1N1.firstName)
			assert.Equal(t, "FirstName100", *row1N2.firstName)
			assert.Equal(t, "LastName100", *row1N1.lastName)
			assert.Equal(t, "LastName100", *row1N2.lastName)
			assert.Equal(t, "City100", *row1N1.city)
			assert.Equal(t, "City100", *row1N2.city)
			assert.Equal(t, "email100@example.com", *row1N1.email)
			assert.Equal(t, "email100@example.com", *row1N2.email)

			// Check Row 2 (id=200) on both nodes
			row2N1 := getFullRow(pgCluster.Node1Pool, 200, "CUST-200")
			row2N2 := getFullRow(pgCluster.Node2Pool, 200, "CUST-200")

			// Node1's NULLs (last_name, email) should be filled from Node2
			require.NotNil(t, row2N1.lastName, "Node1 row 200 last_name should be filled from Node2")
			require.NotNil(t, row2N1.email, "Node1 row 200 email should be filled from Node2")
			assert.Equal(t, "LastName200", *row2N1.lastName, "Node1 row 200 last_name should match Node2's value")
			assert.Equal(t, "email200@example.com", *row2N1.email, "Node1 row 200 email should match Node2's value")

			// Node2's NULLs (first_name, city) should be filled from Node1
			require.NotNil(t, row2N2.firstName, "Node2 row 200 first_name should be filled from Node1")
			require.NotNil(t, row2N2.city, "Node2 row 200 city should be filled from Node1")
			assert.Equal(t, "FirstName200", *row2N2.firstName, "Node2 row 200 first_name should match Node1's value")
			assert.Equal(t, "City200", *row2N2.city, "Node2 row 200 city should match Node1's value")

			// Both nodes should now have complete row 2
			assert.Equal(t, "FirstName200", *row2N1.firstName)
			assert.Equal(t, "FirstName200", *row2N2.firstName)
			assert.Equal(t, "LastName200", *row2N1.lastName)
			assert.Equal(t, "LastName200", *row2N2.lastName)
			assert.Equal(t, "City200", *row2N1.city)
			assert.Equal(t, "City200", *row2N2.city)
			assert.Equal(t, "email200@example.com", *row2N1.email)
			assert.Equal(t, "email200@example.com", *row2N2.email)

			// Verify no diffs remain
			assertNoTableDiff(t, qualifiedTableName)

			log.Println("Bidirectional fix-nulls test completed successfully")
		})
	}
}

// TestTableRepair_PreserveOrigin tests that the preserve-origin flag correctly preserves
// both replication origin metadata and commit timestamps during table repair recovery operations.
// This test verifies the fix for maintaining original transaction metadata to prevent
// replication conflicts when the origin node returns to the cluster.
func TestTableRepair_PreserveOrigin(t *testing.T) {
	ctx := context.Background()
	tableName := "preserve_origin_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	// Create table on all 3 nodes and add to repset
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, data TEXT, created_at TIMESTAMP DEFAULT NOW());`, qualifiedTableName)
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err, "Failed to create table on %s", nodeName)

		addToRepSetSQL := fmt.Sprintf(`SELECT spock.repset_add_table('default', '%s');`, qualifiedTableName)
		_, err = pool.Exec(ctx, addToRepSetSQL)
		require.NoError(t, err, "Failed to add table to repset on %s", nodeName)
	}

	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", qualifiedTableName))
		}
	})

	// Insert test data on n3 (so replication origin metadata is available)
	// When data originates from n3 and replicates to n1/n2, those nodes will have node_origin='node_n3'
	insertedIDs := []int{101, 102, 103, 104, 105, 106, 107, 108, 109, 110}
	log.Printf("Inserting %d test rows on n3", len(insertedIDs))
	for _, id := range insertedIDs {
		_, err := pgCluster.Node3Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", qualifiedTableName), id, fmt.Sprintf("test_data_%d", id))
		require.NoError(t, err, "Failed to insert row %d on n3", id)
	}

	// Wait for replication to n1 and n2
	log.Println("Waiting for replication to n1...")
	assertEventually(t, 30*time.Second, func() error {
		var count int
		if err := pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualifiedTableName), insertedIDs).Scan(&count); err != nil {
			return err
		}
		if count < len(insertedIDs) {
			return fmt.Errorf("expected %d rows on n1, got %d", len(insertedIDs), count)
		}
		return nil
	})

	log.Println("Waiting for replication to n2...")
	assertEventually(t, 30*time.Second, func() error {
		var count int
		if err := pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualifiedTableName), insertedIDs).Scan(&count); err != nil {
			return err
		}
		if count < len(insertedIDs) {
			return fmt.Errorf("expected %d rows on n2, got %d", len(insertedIDs), count)
		}
		return nil
	})

	// Wait a bit to ensure original timestamps are in the past before we repair
	log.Println("Waiting 3 seconds to ensure original timestamps are clearly in the past...")
	time.Sleep(3 * time.Second)

	// --- DEEP VERIFICATION: Capture baseline state from n1 (source of truth) ---
	// n1 has the same data and metadata we expect to restore on n2 after preserve-origin repair.
	originalTimestamps := make(map[int]time.Time)
	originalOrigins := make(map[int]string)
	originalRowData := make(map[int]string) // id -> data column value
	sampleIDs := []int{101, 102, 103, 104, 105}
	log.Printf("Capturing baseline state from n1 for sample rows: %v (timestamps, origins, row data)", sampleIDs)
	for _, id := range sampleIDs {
		ts := getCommitTimestamp(t, ctx, pgCluster.Node1Pool, qualifiedTableName, id)
		originalTimestamps[id] = ts
		origin := getReplicationOrigin(t, ctx, pgCluster.Node1Pool, qualifiedTableName, id)
		originalOrigins[id] = origin
		var data string
		err := pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT data FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&data)
		require.NoError(t, err, "Failed to get row data for id %d on n1", id)
		originalRowData[id] = data
		log.Printf("Row %d on n1: ts=%s origin=%s data=%q", id, ts.Format(time.RFC3339Nano), origin, data)
	}
	// Baseline assertion: n1 must have node_n3 as origin for every sample (data came from n3)
	for _, id := range sampleIDs {
		require.Equal(t, "node_n3", originalOrigins[id],
			"Baseline: row %d on n1 must have origin node_n3 (got %q)", id, originalOrigins[id])
	}
	log.Println("✓ Baseline verified: all sample rows on n1 have origin node_n3")

	// Simulate data loss on n2 by deleting rows (using repair_mode to prevent replication)
	log.Println("Simulating data loss on n2...")
	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err, "Failed to begin transaction on n2")
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err, "Failed to enable repair_mode on n2")

	for _, id := range sampleIDs {
		_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", qualifiedTableName), id)
		require.NoError(t, err, "Failed to delete row %d on n2", id)
	}

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err, "Failed to disable repair_mode on n2")
	require.NoError(t, tx.Commit(ctx), "Failed to commit transaction on n2")
	log.Printf("Deleted %d rows from n2 to simulate data loss", len(sampleIDs))

	// Run table-diff to identify the differences
	log.Println("Running table-diff to identify missing rows...")
	tdTask := newTestTableDiffTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	err = tdTask.RunChecks(false)
	require.NoError(t, err, "table-diff validation failed")
	err = tdTask.ExecuteTask()
	require.NoError(t, err, "table-diff execution failed")

	latestDiffFile := getLatestDiffFile(t)
	require.NotEmpty(t, latestDiffFile, "No diff file was generated")
	log.Printf("Generated diff file: %s", latestDiffFile)

	// Test 1: Repair WITHOUT preserve-origin (control test)
	log.Println("\n=== Test 1: Repair WITHOUT preserve-origin ===")
	repairTaskWithout := newTestTableRepairTask(serviceN1, qualifiedTableName, latestDiffFile)
	repairTaskWithout.RecoveryMode = true
	repairTaskWithout.PreserveOrigin = false // Explicitly disable

	err = repairTaskWithout.Run(false)
	require.NoError(t, err, "Table repair without preserve-origin failed")
	log.Println("Repair completed (without preserve-origin)")

	// Verify timestamps are CURRENT (repair time) for control test
	time.Sleep(1 * time.Second) // Brief pause to ensure timestamp difference
	repairTime := time.Now()
	log.Println("Verifying timestamps without preserve-origin...")

	timestampsWithout := make(map[int]time.Time)
	for _, id := range sampleIDs {
		ts := getCommitTimestamp(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		timestampsWithout[id] = ts
		log.Printf("Row %d timestamp on n2 (without preserve-origin): %s", id, ts.Format(time.RFC3339))

		// Verify timestamp is RECENT (within last few seconds = repair time)
		timeSinceRepair := repairTime.Sub(ts)
		if timeSinceRepair < 0 {
			timeSinceRepair = -timeSinceRepair
		}
		// Timestamps should be very recent (within 10 seconds of repair)
		require.True(t, timeSinceRepair < 10*time.Second,
			"Row %d timestamp should be recent (repair time), but is %v old", id, timeSinceRepair)

		// Verify timestamp is DIFFERENT from original (not preserved); use tight tolerance so "not preserved" is clear
		require.False(t, compareTimestampsExact(ts, originalTimestamps[id], 50*time.Millisecond),
			"Row %d timestamp should NOT match original when preserve-origin is disabled (got %s vs original %s)", id, ts.Format(time.RFC3339Nano), originalTimestamps[id].Format(time.RFC3339Nano))
	}
	log.Println("✓ Verified: Timestamps are CURRENT (not preserved) without preserve-origin")

	// Delete rows again to prepare for Test 2
	log.Println("\nResetting: Deleting rows from n2 again for preserve-origin test...")
	tx2, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx2.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	for _, id := range sampleIDs {
		_, err = tx2.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", qualifiedTableName), id)
		require.NoError(t, err)
	}
	_, err = tx2.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx2.Commit(ctx))

	// Run table-diff again
	log.Println("Running table-diff again...")
	tdTask2 := newTestTableDiffTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	err = tdTask2.RunChecks(false)
	require.NoError(t, err)
	err = tdTask2.ExecuteTask()
	require.NoError(t, err)
	latestDiffFile2 := getLatestDiffFile(t)
	require.NotEmpty(t, latestDiffFile2)

	// Test 2: Repair WITH preserve-origin (feature test)
	log.Println("\n=== Test 2: Repair WITH preserve-origin ===")
	repairTaskWith := newTestTableRepairTask(serviceN1, qualifiedTableName, latestDiffFile2)
	repairTaskWith.RecoveryMode = true
	repairTaskWith.PreserveOrigin = true // Enable feature

	captureOutput(t, func() {
		err = repairTaskWith.Run(false)
	})
	// Note: Run() may return nil even when repair fails - it logs errors but doesn't always return them
	// Check both the error AND the task status
	require.NoError(t, err, "Table repair Run() returned unexpected error")

	// Check if repair actually succeeded by examining the task status
	if repairTaskWith.TaskStatus == "FAILED" {
		t.Fatalf("Table repair failed with unexpected error: %s", repairTaskWith.TaskContext)
	}

	log.Println("Repair completed (with preserve-origin)")

	// Ensure the deleted sample rows were actually restored by the preserve-origin repair attempt.
	var repairedSampleCount int
	err = pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualifiedTableName), sampleIDs).Scan(&repairedSampleCount)
	require.NoError(t, err)
	require.Equal(t, len(sampleIDs), repairedSampleCount, "Sample rows should be present after preserve-origin repair")

	// Verify timestamps are PRESERVED (match original from n1)
	log.Println("Verifying per-row timestamp preservation with preserve-origin...")
	timestampsWith := make(map[int]time.Time)
	preservedCount := 0
	failedRows := []int{}

	for _, id := range sampleIDs {
		ts := getCommitTimestamp(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		timestampsWith[id] = ts
		originalTs := originalTimestamps[id]
		timeDiff := ts.Sub(originalTs)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}

		log.Printf("Row %d - Repaired: %s, Original: %s, Diff: %v",
			id, ts.Format(time.RFC3339Nano), originalTs.Format(time.RFC3339Nano), timeDiff)

		// Verify timestamp MATCHES original (is preserved)
		// PostgreSQL timestamp precision is microseconds, so we truncate both timestamps
		// to microsecond precision and check for exact equality
		tsTrunc := ts.Truncate(time.Microsecond)
		originalTsTrunc := originalTs.Truncate(time.Microsecond)
		if tsTrunc.Equal(originalTsTrunc) {
			preservedCount++
			log.Printf("  ✓ Row %d timestamp PRESERVED", id)
		} else {
			failedRows = append(failedRows, id)
			log.Printf("  ✗ Row %d timestamp NOT preserved (diff: %v)", id, timeDiff)
		}
	}

	// Require 100% timestamp preservation when preserve-origin is enabled
	require.Equal(t, len(sampleIDs), preservedCount,
		"All sample rows must have commit timestamp preserved when preserve-origin is enabled: preserved %d/%d, failed rows: %v",
		preservedCount, len(sampleIDs), failedRows)
	log.Printf("\nTimestamp Preservation: %d/%d rows match n1 baseline (exact at microsecond precision)", preservedCount, len(sampleIDs))

	// --- DEEP: Require 100% replication origin preservation and match n1 baseline ---
	for _, id := range sampleIDs {
		actualOrigin := getReplicationOrigin(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		require.Equal(t, originalOrigins[id], actualOrigin,
			"Row %d on n2 must have same replication origin as n1: n1=%q n2=%q", id, originalOrigins[id], actualOrigin)
		log.Printf("  ✓ Row %d origin preserved (n2=%s matches n1)", id, actualOrigin)
	}
	log.Printf("Origin Preservation: all %d sample rows on n2 match n1 origin (node_n3)", len(sampleIDs))

	// --- DEEP: Require row content (id, data) on n2 to match n1 exactly ---
	log.Println("\nDeep verification: row content (id, data) on n2 must match n1...")
	for _, id := range sampleIDs {
		var dataOnN2 string
		err := pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT data FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&dataOnN2)
		require.NoError(t, err, "Failed to get row data for id %d on n2", id)
		require.Equal(t, originalRowData[id], dataOnN2,
			"Row %d data on n2 must match n1: n1=%q n2=%q", id, originalRowData[id], dataOnN2)
		log.Printf("  ✓ Row %d data preserved: %q", id, dataOnN2)
	}
	log.Printf("Row content: all %d sample rows on n2 match n1 (id, data)", len(sampleIDs))

	// Summary: what was deeply verified
	expectedOrigin := "node_n3"
	log.Println("\n--- DEEP VERIFICATION SUMMARY ---")
	log.Printf("  Baseline (n1): timestamps, origins (%s), and row data captured for sample rows", expectedOrigin)
	log.Printf("  After preserve-origin repair (n2):")
	log.Printf("    - Commit timestamp: 100%% match n1 (microsecond precision)")
	log.Printf("    - Replication origin: 100%% match n1 (%s)", expectedOrigin)
	log.Printf("    - Row content (id, data): 100%% match n1")
	log.Println("  ---")

	// Final verification: Ensure all rows are present
	var finalCount int
	err = pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualifiedTableName), insertedIDs).Scan(&finalCount)
	require.NoError(t, err)
	require.Equal(t, len(insertedIDs), finalCount, "All rows should be present after repair")

	log.Println("\n✓ TestTableRepair_PreserveOrigin COMPLETED")
	log.Println("  - WITHOUT preserve-origin: Timestamps are current (repair time) ✓")
	log.Printf("  - WITH preserve-origin: 100%% timestamps preserved (%d/%d), 100%% origins preserved (%s)", preservedCount, len(sampleIDs), expectedOrigin)
	log.Printf("  - Verified data integrity: all %d rows present after repair", len(insertedIDs))
}

// TestTableRepair_FixNulls_PreserveOrigin verifies that fix-nulls with PreserveOrigin=true
// preserves replication origin (and commit timestamp when available) on the target node.
// Fix-nulls UPDATE scenario: only NULL columns are updated, not a full row INSERT.
// Source row (n1) has origin metadata (node_n3); target row (n2) has a NULL column.
// Preserve-origin must extract origin/timestamp from source and apply during UPDATE.
// Validates origin preservation for UPDATE operations, not just INSERT operations.
func TestTableRepair_FixNulls_PreserveOrigin(t *testing.T) {
	t.Skip("KNOWN LIMITATION: fix-nulls uses UPDATE statements which don't preserve origin. " +
		"Preserve-origin only works for INSERT operations (via pg_replication_origin_xact_setup). " +
		"Fix-nulls would need to use DELETE+INSERT or a different mechanism to preserve origin during UPDATE.")
}

// TestTableRepair_Bidirectional_PreserveOrigin verifies that bidirectional
// repair with PreserveOrigin=true preserves replication origins.
//
// Scenario: two rows originate on n3 and replicate to n1 and n2 with
// origin=node_n3.  Then, in repair_mode, row 1 is deleted from n2 and
// row 2 is deleted from n1 — creating symmetric divergence.
// Bidirectional repair must copy row 2 → n1 and row 1 → n2, preserving
// the original node_n3 origin on both sides.
func TestTableRepair_Bidirectional_PreserveOrigin(t *testing.T) {
	tableName := "bi_preserve_origin_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	// Create table on all 3 nodes and add to repset
	for i, pool := range []*pgxpool.Pool{
		pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool,
	} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s
			 (id INT PRIMARY KEY, label TEXT);`,
			qualifiedTableName))
		require.NoError(t, err, "create table on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			`SELECT spock.repset_add_table('default', '%s');`,
			qualifiedTableName))
		require.NoError(t, err, "add to repset on %s", nodeName)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{
			pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool,
		} {
			pool.Exec(ctx, fmt.Sprintf(
				"DROP TABLE IF EXISTS %s CASCADE;",
				qualifiedTableName))
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})

	// Insert 2 rows on n3 so they replicate with origin=node_n3
	_, err := pgCluster.Node3Pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, label) VALUES (1, 'row_one')",
		qualifiedTableName))
	require.NoError(t, err)
	_, err = pgCluster.Node3Pool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, label) VALUES (2, 'row_two')",
		qualifiedTableName))
	require.NoError(t, err)

	// Wait for replication to both nodes
	for _, info := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{"n1", pgCluster.Node1Pool},
		{"n2", pgCluster.Node2Pool},
	} {
		assertEventually(t, 30*time.Second, func() error {
			var c int
			if err := info.pool.QueryRow(ctx, fmt.Sprintf(
				"SELECT count(*) FROM %s", qualifiedTableName),
			).Scan(&c); err != nil {
				return err
			}
			if c < 2 {
				return fmt.Errorf(
					"expected 2 rows on %s, got %d", info.name, c)
			}
			return nil
		})
	}

	// Delete row 2 from n1, row 1 from n2 (symmetric divergence)
	tx1, err := pgCluster.Node1Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx1.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	_, err = tx1.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE id = 2", qualifiedTableName))
	require.NoError(t, err)
	_, err = tx1.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx1.Commit(ctx))

	tx2, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx2.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	_, err = tx2.Exec(ctx, fmt.Sprintf(
		"DELETE FROM %s WHERE id = 1", qualifiedTableName))
	require.NoError(t, err)
	_, err = tx2.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx2.Commit(ctx))

	// Verify divergence: each node has exactly 1 row
	var count1, count2 int
	err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s", qualifiedTableName)).Scan(&count1)
	require.NoError(t, err)
	err = pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT count(*) FROM %s", qualifiedTableName)).Scan(&count2)
	require.NoError(t, err)
	require.Equal(t, 1, count1, "n1 should have 1 row before repair")
	require.Equal(t, 1, count2, "n2 should have 1 row before repair")

	// Run bidirectional repair with preserve-origin
	diffFile := runTableDiff(t, qualifiedTableName,
		[]string{serviceN1, serviceN2})
	repairTask := newTestTableRepairTask(
		serviceN1, qualifiedTableName, diffFile)
	repairTask.Bidirectional = true
	repairTask.PreserveOrigin = true

	err = repairTask.Run(false)
	require.NoError(t, err,
		"bidirectional repair with preserve-origin failed")
	require.NotEqual(t, "FAILED", repairTask.TaskStatus,
		"repair task should not be FAILED")

	// Verify tables match and both have 2 rows
	assertNoTableDiff(t, qualifiedTableName)
	count1 = getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	count2 = getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	require.Equal(t, 2, count1, "node1 should have 2 rows")
	require.Equal(t, 2, count2, "node2 should have 2 rows")

	// Both rows originated on n3. After bidirectional repair with
	// preserve-origin, both nodes should show origin=node_n3.
	for _, nodeInfo := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{"n1", pgCluster.Node1Pool},
		{"n2", pgCluster.Node2Pool},
	} {
		for _, id := range []int{1, 2} {
			origin := getReplicationOrigin(
				t, ctx, nodeInfo.pool, qualifiedTableName, id)
			require.Equal(t, "node_n3", origin,
				"row %d on %s should have origin node_n3",
				id, nodeInfo.name)
		}
	}
}

// TestTableRepair_MixedOps_PreserveOrigin verifies that a standard unidirectional repair
// with PreserveOrigin=true correctly handles DELETE, INSERT, and UPDATE operations within
// a single transaction. This is the most realistic real-world scenario: a source-of-truth
// repair where some rows must be deleted from the target, some inserted, and some updated,
// all while preserving the original replication origin and commit timestamp metadata.
//
// Scenario (n1 = source of truth, n2 = target, data originates on n3):
//   - 5 common rows on both nodes (originated on n3) — no repair needed
//   - 2 rows only on n1 (originated on n3) → INSERT into n2 with preserved origin/timestamp
//   - 2 rows only on n2 (inserted locally) → DELETE from n2
//   - 2 common rows modified on n2 → UPDATE on n2 with preserved origin/timestamp from n1
//
// After repair: both nodes have 7 identical rows, and the inserted/updated rows on n2
// retain the original node_n3 origin and commit timestamps from n1.
func TestTableRepair_MixedOps_PreserveOrigin(t *testing.T) {
	ctx := context.Background()
	tableName := "mixed_ops_preserve_origin_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	// Create table on all 3 nodes and add to repset
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, data TEXT, category TEXT);`, qualifiedTableName)
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err, "Failed to create table on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf(`SELECT spock.repset_add_table('default', '%s');`, qualifiedTableName))
		require.NoError(t, err, "Failed to add table to repset on %s", nodeName)
	}

	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", qualifiedTableName))
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})

	// --- Phase 1: Insert 9 rows on n3 so they replicate to n1 and n2 with origin=node_n3 ---
	// IDs 1-5: common rows (will remain unchanged)
	// IDs 6-7: will become "only on n1" after we delete them from n2
	// IDs 8-9: will become "modified on n2" after we update them on n2
	allIDs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	log.Printf("Inserting %d rows on n3", len(allIDs))
	for _, id := range allIDs {
		_, err := pgCluster.Node3Pool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s (id, data, category) VALUES ($1, $2, $3)", qualifiedTableName),
			id, fmt.Sprintf("original_data_%d", id), "original")
		require.NoError(t, err, "Failed to insert row %d on n3", id)
	}

	// Wait for replication to n1 and n2
	for _, nodeInfo := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{"n1", pgCluster.Node1Pool},
		{"n2", pgCluster.Node2Pool},
	} {
		log.Printf("Waiting for replication to %s...", nodeInfo.name)
		assertEventually(t, 30*time.Second, func() error {
			var count int
			if err := nodeInfo.pool.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualifiedTableName), allIDs).Scan(&count); err != nil {
				return err
			}
			if count < len(allIDs) {
				return fmt.Errorf("expected %d rows on %s, got %d", len(allIDs), nodeInfo.name, count)
			}
			return nil
		})
	}

	// Wait to ensure original timestamps are clearly in the past
	log.Println("Waiting 3 seconds to ensure original timestamps are in the past...")
	time.Sleep(3 * time.Second)

	// --- Phase 2: Capture baseline origin metadata from n1 (source of truth) ---
	// We capture for rows that will be inserted (6,7) and updated (8,9) on n2
	verifyIDs := []int{6, 7, 8, 9}
	originalTimestamps := make(map[int]time.Time)
	originalOrigins := make(map[int]string)
	originalData := make(map[int]string)

	log.Printf("Capturing baseline state from n1 for rows: %v", verifyIDs)
	for _, id := range verifyIDs {
		ts := getCommitTimestamp(t, ctx, pgCluster.Node1Pool, qualifiedTableName, id)
		originalTimestamps[id] = ts
		origin := getReplicationOrigin(t, ctx, pgCluster.Node1Pool, qualifiedTableName, id)
		originalOrigins[id] = origin
		var data string
		err := pgCluster.Node1Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT data FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&data)
		require.NoError(t, err)
		originalData[id] = data
		log.Printf("Baseline row %d on n1: ts=%s origin=%s data=%q", id, ts.Format(time.RFC3339Nano), origin, data)
	}
	// Verify baseline: all rows should have origin node_n3
	for _, id := range verifyIDs {
		require.Equal(t, "node_n3", originalOrigins[id],
			"Baseline: row %d on n1 must have origin node_n3", id)
	}

	// --- Phase 3: Create divergence on n2 using repair_mode (no replication) ---
	log.Println("Creating divergence on n2...")
	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	// Delete rows 6,7 from n2 → these will need to be INSERTED back during repair
	for _, id := range []int{6, 7} {
		_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", qualifiedTableName), id)
		require.NoError(t, err, "Failed to delete row %d on n2", id)
	}

	// Modify rows 8,9 on n2 → these will need to be UPDATED during repair
	for _, id := range []int{8, 9} {
		_, err = tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET data = $1, category = $2 WHERE id = $3", qualifiedTableName),
			fmt.Sprintf("modified_on_n2_%d", id), "modified", id)
		require.NoError(t, err, "Failed to modify row %d on n2", id)
	}

	// Insert rows 1001,1002 only on n2 → these will need to be DELETED during repair
	for _, id := range []int{1001, 1002} {
		_, err = tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, data, category) VALUES ($1, $2, $3)", qualifiedTableName),
			id, fmt.Sprintf("n2_only_%d", id), "n2_extra")
		require.NoError(t, err, "Failed to insert row %d on n2", id)
	}

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	log.Println("Divergence created: 2 deleted, 2 modified, 2 extra rows on n2")

	// Verify divergence state
	n1Count := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	n2Count := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	require.Equal(t, 9, n1Count, "n1 should have 9 rows")
	require.Equal(t, 9, n2Count, "n2 should have 9 rows (7 original + 2 extra - 2 deleted)")

	// --- Phase 4: Run table-diff and repair with preserve-origin ---
	log.Println("Running table-diff...")
	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	log.Println("Running repair with preserve-origin...")
	repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
	repairTask.PreserveOrigin = true

	err = repairTask.Run(false)
	require.NoError(t, err, "Table repair failed")
	require.NotEqual(t, "FAILED", repairTask.TaskStatus,
		"Repair task should not be FAILED: %s", repairTask.TaskContext)

	// --- Phase 5: Verify repair correctness ---
	log.Println("Verifying repair results...")

	// 5a. Row counts should match (9 rows each, extra rows deleted from n2)
	n1CountAfter := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	n2CountAfter := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	require.Equal(t, 9, n1CountAfter, "n1 should still have 9 rows")
	require.Equal(t, 9, n2CountAfter, "n2 should have 9 rows after repair")

	// 5b. No diffs should remain
	assertNoTableDiff(t, qualifiedTableName)

	// 5c. Extra rows (1001, 1002) should be deleted from n2
	for _, id := range []int{1001, 1002} {
		var count int
		err := pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 0, count, "Row %d should have been deleted from n2", id)
	}

	// 5d. Verify origin and timestamp preservation for INSERTED rows (6, 7)
	log.Println("Verifying origin/timestamp preservation for INSERTED rows (6, 7)...")
	for _, id := range []int{6, 7} {
		// Verify data was restored correctly
		var data string
		err := pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT data FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&data)
		require.NoError(t, err, "Failed to get data for row %d on n2", id)
		require.Equal(t, originalData[id], data,
			"Row %d data on n2 should match n1 baseline", id)

		// Verify origin preserved
		origin := getReplicationOrigin(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		require.Equal(t, "node_n3", origin,
			"Inserted row %d on n2 should have origin node_n3 preserved (got %q)", id, origin)

		// Verify timestamp preserved (microsecond precision)
		ts := getCommitTimestamp(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		tsTrunc := ts.Truncate(time.Microsecond)
		originalTsTrunc := originalTimestamps[id].Truncate(time.Microsecond)
		require.True(t, tsTrunc.Equal(originalTsTrunc),
			"Inserted row %d timestamp should be preserved: got %s, expected %s",
			id, ts.Format(time.RFC3339Nano), originalTimestamps[id].Format(time.RFC3339Nano))

		log.Printf("  Row %d (INSERT): origin=%s ts=%s data=%q", id, origin, ts.Format(time.RFC3339Nano), data)
	}

	// 5e. Verify origin and timestamp preservation for UPDATED rows (8, 9)
	log.Println("Verifying origin/timestamp preservation for UPDATED rows (8, 9)...")
	for _, id := range []int{8, 9} {
		// Verify data was restored to original
		var data, category string
		err := pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT data, category FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&data, &category)
		require.NoError(t, err, "Failed to get data for row %d on n2", id)
		require.Equal(t, originalData[id], data,
			"Row %d data on n2 should match n1 baseline (was modified)", id)
		require.Equal(t, "original", category,
			"Row %d category on n2 should be restored to 'original'", id)

		// Verify origin preserved
		origin := getReplicationOrigin(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		require.Equal(t, "node_n3", origin,
			"Updated row %d on n2 should have origin node_n3 preserved (got %q)", id, origin)

		// Verify timestamp preserved (microsecond precision)
		ts := getCommitTimestamp(t, ctx, pgCluster.Node2Pool, qualifiedTableName, id)
		tsTrunc := ts.Truncate(time.Microsecond)
		originalTsTrunc := originalTimestamps[id].Truncate(time.Microsecond)
		require.True(t, tsTrunc.Equal(originalTsTrunc),
			"Updated row %d timestamp should be preserved: got %s, expected %s",
			id, ts.Format(time.RFC3339Nano), originalTimestamps[id].Format(time.RFC3339Nano))

		log.Printf("  Row %d (UPDATE): origin=%s ts=%s data=%q", id, origin, ts.Format(time.RFC3339Nano), data)
	}

	// 5f. Verify common rows (1-5) are untouched
	log.Println("Verifying common rows (1-5) are unchanged...")
	for _, id := range []int{1, 2, 3, 4, 5} {
		var data string
		err := pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT data FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&data)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("original_data_%d", id), data,
			"Common row %d should be unchanged", id)
	}

	log.Println("TestTableRepair_MixedOps_PreserveOrigin PASSED")
	log.Println("  - DELETE: 2 extra rows removed from n2")
	log.Println("  - INSERT: 2 missing rows restored with preserved origin/timestamp")
	log.Println("  - UPDATE: 2 modified rows corrected with preserved origin/timestamp")
}
