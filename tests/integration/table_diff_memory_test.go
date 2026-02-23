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
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestTableDiffMemoryUsage runs table-diff against a large table with widespread
// mismatches and measures peak memory usage. This test validates that the
// recursive diff semaphore prevents unbounded goroutine fan-out from causing OOM.
//
// The test creates a dedicated 100k-row table, truncates node2 to create a
// worst-case scenario where every row is a diff, then runs the diff and records
// heap memory usage.
func TestTableDiffMemoryUsage(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping memory test in short mode")
	}

	const targetRows = 100000

	ctx := context.Background()
	tableName := "customers_memory_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	// Create the test table on both nodes and populate with 100k rows from customers_1M
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf("failed to create table on %s: %v", nodeName, err)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO "%s"."%s" SELECT * FROM "%s"."customers_1M" WHERE index <= %d`,
			testSchema, tableName, testSchema, targetRows,
		))
		if err != nil {
			t.Fatalf("failed to populate table on %s: %v", nodeName, err)
		}
		log.Printf("Populated %s with %d rows on %s", qualifiedTableName, targetRows, nodeName)
	}

	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s" CASCADE`, testSchema, tableName))
			if err != nil {
				t.Logf("cleanup: failed to drop table: %v", err)
			}
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
		log.Println("Cleanup: memory test table dropped")
	})

	// Verify row count on node1
	var rowCount int
	err := pgCluster.Node1Pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM "%s"."%s"`, testSchema, tableName),
	).Scan(&rowCount)
	require.NoError(t, err, "failed to count rows on node1")
	require.Equal(t, targetRows, rowCount, "expected %d rows on node1", targetRows)

	// Truncate node2 to create maximum mismatches (all rows only on node1)
	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction on node2")
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err, "failed to enable repair mode on node2")
	_, err = tx.Exec(ctx, fmt.Sprintf(`TRUNCATE TABLE "%s"."%s" CASCADE`, testSchema, tableName))
	require.NoError(t, err, "failed to truncate table on node2")
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err, "failed to disable repair mode on node2")
	require.NoError(t, tx.Commit(ctx), "failed to commit truncate on node2")

	log.Printf("Truncated %s on node2 to create %d mismatched rows", qualifiedTableName, rowCount)

	// Force GC and capture baseline memory
	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	// Configure the diff task
	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := diff.NewTableDiffTask()
	tdTask.ClusterName = "test_cluster"
	tdTask.DBName = dbName
	tdTask.QualifiedTableName = qualifiedTableName
	tdTask.Nodes = strings.Join(nodesToCompare, ",")
	tdTask.Output = "json"
	tdTask.BlockSize = 10000
	tdTask.CompareUnitSize = 500
	tdTask.ConcurrencyFactor = 1
	tdTask.MaxDiffRows = math.MaxInt64

	tdTask.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Nodes:             nodesToCompare,
			BlockSize:         tdTask.BlockSize,
			CompareUnitSize:   tdTask.CompareUnitSize,
			ConcurrencyFactor: tdTask.ConcurrencyFactor,
			DiffRowsCount:     make(map[string]int),
		},
	}

	err = tdTask.RunChecks(false)
	require.NoError(t, err, "RunChecks failed")

	log.Printf("Starting table-diff for %s (%d rows, all mismatched)...", qualifiedTableName, rowCount)
	err = tdTask.ExecuteTask()
	require.NoError(t, err, "ExecuteTask failed")

	// Capture memory after diff
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Calculate memory usage
	heapAllocDelta := int64(memAfter.TotalAlloc) - int64(memBefore.TotalAlloc)
	heapInUseAfter := memAfter.HeapInuse
	sysAfter := memAfter.Sys

	log.Printf("=== Memory Usage Report ===")
	log.Printf("Heap before:     %d MB", memBefore.HeapInuse/(1024*1024))
	log.Printf("Heap after:      %d MB", heapInUseAfter/(1024*1024))
	log.Printf("Total allocated: %d MB", heapAllocDelta/(1024*1024))
	log.Printf("System memory:   %d MB", sysAfter/(1024*1024))
	log.Printf("Num goroutines:  %d", runtime.NumGoroutine())
	log.Printf("===========================")

	// Verify correctness
	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := tdTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "expected diffs for pair %s", pairKey)

	node1OnlyRows := len(nodeDiffs.Rows[serviceN1])
	node2OnlyRows := len(nodeDiffs.Rows[serviceN2])
	log.Printf("Diff results: %d rows on node1 only, %d rows on node2 only", node1OnlyRows, node2OnlyRows)

	require.Equal(t, rowCount, node1OnlyRows,
		"expected all %d rows to appear as node1-only diffs", rowCount)
	require.Equal(t, 0, node2OnlyRows,
		"expected 0 rows on node2 after truncation")

	// Memory guard: the diff of 100k rows should stay under 512 MB of heap.
	// Without the semaphore fix, unbounded goroutine fan-out pushes heap to 2+ GB.
	// With the fix, heap stays around ~370 MB; 512 MB gives headroom for CI variance.
	maxAllowedHeapMB := uint64(512)
	require.Less(t, heapInUseAfter/(1024*1024), maxAllowedHeapMB,
		"heap usage %d MB exceeds %d MB threshold — possible goroutine fan-out OOM",
		heapInUseAfter/(1024*1024), maxAllowedHeapMB)

	log.Println("TestTableDiffMemoryUsage completed successfully.")
}
