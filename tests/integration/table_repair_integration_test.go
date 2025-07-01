package integration

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TODO: Verify replication works and only then assert for correct counts.
// setupDivergence prepares the 'customers' table with a known set of differences between node1 and node2.
// - 5 common rows
// - 2 rows only on node1 (IDs 1001, 1002)
// - 2 rows only on node2 (IDs 2001, 2002)
// - 2 common rows modified on node2 (IDs 1, 2)
func setupDivergence(t *testing.T, ctx context.Context, qualifiedTableName string) {
	t.Helper()
	log.Println("Setting up data divergence for", qualifiedTableName)

	// Truncate on both nodes
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
		require.NoError(t, err, "Failed to truncate table on node %s", nodeName)
	}

	// Insert common rows
	for i := 1; i <= 5; i++ {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, first_name, last_name, email) VALUES ($1, $2, $3, $4)", qualifiedTableName),
				i, fmt.Sprintf("FirstName%d", i), fmt.Sprintf("LastName%d", i), fmt.Sprintf("email%d@example.com", i))
			require.NoError(t, err)
		}
	}

	// Insert rows only on node1
	for i := 1001; i <= 1002; i++ {
		_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, first_name, last_name, email) VALUES ($1, $2, $3, $4)", qualifiedTableName),
			i, fmt.Sprintf("N1OnlyFirst%d", i), fmt.Sprintf("N1OnlyLast%d", i), fmt.Sprintf("n1.only%d@example.com", i))
		require.NoError(t, err)
	}

	// Insert rows only on node2
	for i := 2001; i <= 2002; i++ {
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, first_name, last_name, email) VALUES ($1, $2, $3, $4)", qualifiedTableName),
			i, fmt.Sprintf("N2OnlyFirst%d", i), fmt.Sprintf("N2OnlyLast%d", i), fmt.Sprintf("n2.only%d@example.com", i))
		require.NoError(t, err)
	}

	// Modify rows on node2
	for i := 1; i <= 2; i++ {
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = $1 WHERE index = $2", qualifiedTableName),
			fmt.Sprintf("modified.email%d@example.com", i), i)
		require.NoError(t, err)
	}

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
	err = tdTask.ExecuteTask(false)
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

	err = tdTask.ExecuteTask(false)
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

func TestTableRepair_UnidirectionalDefault(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	setupDivergence(t, ctx, qualifiedTableName)
	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
	err := repairTask.Run(false)
	require.NoError(t, err, "Table repair failed")

	log.Println("Verifying repair for TestTableRepair_UnidirectionalDefault")
	assertNoTableDiff(t, qualifiedTableName)
	count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	assert.Equal(t, count1, count2, "Row counts should be equal after default repair")
	// 5 common + 2 n1_only = 7 rows on n1. After repair, n2 should have 7 too.
	assert.Equal(t, 7, count1, "Expected 7 rows on node1")
}

func TestTableRepair_InsertOnly(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	setupDivergence(t, ctx, qualifiedTableName)
	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
	repairTask.InsertOnly = true
	err := repairTask.Run(false)
	require.NoError(t, err, "Table repair (insert-only) failed")

	// Verification
	count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	// N1 has 5 common + 2 N1-only = 7.
	// N2 has 5 common + 2 N2-only = 7.
	// After insert-only repair, 2 rows from N1 are inserted to N2.
	// N2 now has 7 + 2 = 9 rows. Modified rows are not touched, extra rows are not deleted.
	assert.Equal(t, 7, count1)
	assert.Equal(t, 9, count2)

	// A new diff should find 2 modified rows and 2 rows only on node2. Total 4 diffs.
	tdTask := newTestTableDiffTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	err = tdTask.RunChecks(false)
	require.NoError(t, err)
	err = tdTask.ExecuteTask(false)
	require.NoError(t, err)

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}
	// 2 modified (appear on both sides of diff) + 2 only on node2 = 4 rows in diff for node2
	assert.Equal(t, 2, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN1]))
	assert.Equal(t, 4, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN2]))
	assert.Equal(t, 4, tdTask.DiffResult.Summary.DiffRowsCount[pairKey])
}

func TestTableRepair_UpsertOnly(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	setupDivergence(t, ctx, qualifiedTableName)
	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
	repairTask.UpsertOnly = true
	err := repairTask.Run(false)
	require.NoError(t, err, "Table repair (upsert-only) failed")

	// Verification
	count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	// N1 has 7 rows.
	// N2 gets 2 rows inserted, 2 rows updated. Deletes are skipped.
	// N2 started with 7 (5 common + 2 n2-only). It gets 2 n1-only rows. So 9 rows.
	assert.Equal(t, 7, count1)
	assert.Equal(t, 9, count2)

	// A new diff should only find the 2 rows that exist only on node2.
	tdTask := newTestTableDiffTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	err = tdTask.RunChecks(false)
	require.NoError(t, err)
	err = tdTask.ExecuteTask(false)
	require.NoError(t, err)

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}
	assert.Equal(t, 0, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN1]))
	assert.Equal(t, 2, len(tdTask.DiffResult.NodeDiffs[pairKey].Rows[serviceN2]))
	assert.Equal(t, 2, tdTask.DiffResult.Summary.DiffRowsCount[pairKey])
}

func TestTableRepair_Bidirectional(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	// Setup for bidirectional: N1 has some unique rows, N2 has others.
	log.Println("Setting up data for bidirectional test")
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
		require.NoError(t, err)
	}
	for i := 3001; i <= 3003; i++ { // 3 rows on N1
		_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, first_name) VALUES ($1, $2)", qualifiedTableName), i, fmt.Sprintf("N1-Bi-%d", i))
		require.NoError(t, err)
	}
	for i := 4001; i <= 4002; i++ { // 2 rows on N2
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (index, first_name) VALUES ($1, $2)", qualifiedTableName), i, fmt.Sprintf("N2-Bi-%d", i))
		require.NoError(t, err)
	}

	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})
	repairTask := newTestTableRepairTask(serviceN1, qualifiedTableName, diffFile)
	repairTask.Bidirectional = true
	err := repairTask.Run(false)
	require.NoError(t, err, "Table repair (bidirectional) failed")

	log.Println("Verifying repair for TestTableRepair_Bidirectional")
	assertNoTableDiff(t, qualifiedTableName)
	count1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	count2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	assert.Equal(t, 5, count1, "Expected 5 rows on node1 after bidirectional repair")
	assert.Equal(t, 5, count2, "Expected 5 rows on node2 after bidirectional repair")
}

func TestTableRepair_DryRun(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()

	setupDivergence(t, ctx, qualifiedTableName)
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

	// Verify no data was changed
	count1After, count2After := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName), getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	assert.Equal(t, count1Before, count1After, "Node1 count should not change after dry run")
	assert.Equal(t, count2Before, count2After, "Node2 count should not change after dry run")
}

func TestTableRepair_GenerateReport(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	ctx := context.Background()
	reportDir := "reports"

	t.Cleanup(func() {
		os.RemoveAll(reportDir)
		repairTable(t, qualifiedTableName, serviceN1)
	})
	os.RemoveAll(reportDir) // Clean up before test too

	setupDivergence(t, ctx, qualifiedTableName)

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
}
