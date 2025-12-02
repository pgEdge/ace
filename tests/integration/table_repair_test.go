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

	task()

	err = w.Close()
	require.NoError(t, err)
	os.Stdout = oldStdout
	os.Stderr = oldStderr
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
			fmt.Sprintf(`INSERT INTO %s (id, col_smallint, col_integer, col_bigint, col_numeric, col_real, col_double, col_varchar, col_text, col_char, col_boolean, col_date, col_timestamp, col_timestamptz, col_jsonb, col_json, col_bytea, col_int_array, col_text_array)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`, qualifiedTableName),
			data["id"], data["col_smallint"], data["col_integer"], data["col_bigint"],
			data["col_numeric"], data["col_real"], data["col_double"], data["col_varchar"],
			data["col_text"], data["col_char"], data["col_boolean"], data["col_date"],
			data["col_timestamp"], data["col_timestamptz"], data["col_jsonb"], data["col_json"],
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
		"col_timestamp": refTime, "col_timestamptz": refTime,
		"col_jsonb": `{"key": "value1"}`, "col_json": `{"key": "value1"}`,
		"col_bytea": []byte("bytea_row1"), "col_int_array": []int32{1, 2, 3},
		"col_text_array": []string{"alpha", "beta"},
	}
	row2OnlyN1 := map[string]any{
		"id": 2, "col_smallint": int16(20), "col_integer": int32(200), "col_bigint": int64(2000),
		"col_numeric": "222.22", "col_real": float32(2.34), "col_double": float64(5.6789),
		"col_varchar": "only_on_n1", "col_text": "text_row2", "col_char": "char_row2",
		"col_boolean": false, "col_date": refTime.AddDate(0, 1, 0).Format("2006-01-02"),
		"col_timestamp": refTime.Add(time.Hour), "col_timestamptz": refTime.Add(time.Hour),
		"col_jsonb": `{"row": 2}`, "col_json": `{"row": 2}`,
		"col_bytea": []byte("bytea_row2"), "col_int_array": []int32{4, 5},
		"col_text_array": []string{"only", "n1"},
	}
	row3Base := map[string]any{
		"id": 3, "col_smallint": int16(30), "col_integer": int32(300), "col_bigint": int64(3000),
		"col_numeric": "333.33", "col_real": float32(3.45), "col_double": float64(6.789),
		"col_varchar": "baseline_varchar", "col_text": "baseline_text", "col_char": "char_row3",
		"col_boolean": true, "col_date": refTime.AddDate(0, 2, 0).Format("2006-01-02"),
		"col_timestamp": refTime.Add(2 * time.Hour), "col_timestamptz": refTime.Add(2 * time.Hour),
		"col_jsonb": `{"status": "good"}`, "col_json": `{"status": "good"}`,
		"col_bytea": []byte("bytea_row3"), "col_int_array": []int32{7, 8, 9},
		"col_text_array": []string{"one", "two", "three"},
	}
	row3ModifiedN2 := map[string]any{
		"id": 3, "col_smallint": int16(30), "col_integer": int32(300), "col_bigint": int64(3000),
		"col_numeric": "999.99", "col_real": float32(9.99), "col_double": float64(99.99),
		"col_varchar": "modified_on_n2", "col_text": "modified_text", "col_char": "char_mod",
		"col_boolean": false, "col_date": refTime.AddDate(0, 3, 0).Format("2006-01-02"),
		"col_timestamp": refTime.Add(3 * time.Hour), "col_timestamptz": refTime.Add(3 * time.Hour),
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
			colNumeric string
			colJSONB   string
			colBytea   []byte
			colIntArr  []int32
			colTxtArr  []string
		)
		err := pool.QueryRow(
			ctx,
			fmt.Sprintf(`SELECT col_numeric::text, col_jsonb::text, col_bytea, col_int_array, col_text_array FROM %s WHERE id = 3`, qualifiedTableName),
		).Scan(&colNumeric, &colJSONB, &colBytea, &colIntArr, &colTxtArr)
		require.NoError(t, err)
		var jsonVal map[string]any
		require.NoError(t, json.Unmarshal([]byte(colJSONB), &jsonVal))
		return map[string]any{
			"col_numeric": colNumeric,
			"col_jsonb":   jsonVal,
			"col_bytea":   colBytea,
			"col_int_arr": colIntArr,
			"col_txt_arr": colTxtArr,
		}
	}

	row3N1 := checkRow3(pgCluster.Node1Pool)
	row3N2 := checkRow3(pgCluster.Node2Pool)

	assert.Equal(t, row3N1["col_numeric"], row3N2["col_numeric"])
	assert.Equal(t, row3N1["col_jsonb"], row3N2["col_jsonb"])
	assert.Equal(t, row3N1["col_int_arr"], row3N2["col_int_arr"])
	assert.Equal(t, row3N1["col_txt_arr"], row3N2["col_txt_arr"])
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
