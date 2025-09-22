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
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestMerkleTreeIntegration(t *testing.T) {
	testCases := []struct {
		name      string
		tableName string
		setup     func(t *testing.T)
		teardown  func(t *testing.T)
	}{
		{
			name:      "simple_primary_key",
			tableName: "customers",
			setup:     func(t *testing.T) {},
			teardown:  func(t *testing.T) {},
		},
		{
			name:      "composite_primary_key",
			tableName: "customers",
			setup: func(t *testing.T) {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					err := alterTableToCompositeKey(context.Background(), pool, testSchema, "customers")
					require.NoError(t, err)
				}
			},
			teardown: func(t *testing.T) {
				for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
					err := revertTableToSimpleKey(context.Background(), pool, testSchema, "customers")
					require.NoError(t, err)
				}
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)
			t.Cleanup(func() {
				tc.teardown(t)
			})
			runMerkleTreeTests(t, tc.tableName)
		})
	}
}

func runMerkleTreeTests(t *testing.T, tableName string) {
	t.Run("TestMerkleTree_Init", func(t *testing.T) {
		testMerkleTreeInit(t, tableName)
	})
	t.Run("TestMerkleTree_Build", func(t *testing.T) {
		testMerkleTreeBuild(t, tableName)
	})
	if tableName == "customers" {
		t.Run("TestMerkleTree_Diff_DataOnlyOnNode1", func(t *testing.T) {
			testMerkleTreeDiffDataOnlyOnNode1(t, tableName)
		})
		t.Run("TestMerkleTree_Diff_ModifiedRows", func(t *testing.T) {
			testMerkleTreeDiffModifiedRows(t, tableName)
		})
		t.Run("TestMerkleTree_Diff_BoundaryModifications", func(t *testing.T) {
			testMerkleTreeDiffBoundaryModifications(t, tableName)
		})
	}
	t.Run("TestMerkleTree_Teardown", func(t *testing.T) {
		testMerkleTreeTeardown(t, tableName)
	})
}

func newTestMerkleTreeTask(t *testing.T, qualifiedTableName string, nodes []string) *core.MerkleTreeTask {
	t.Helper()
	task := core.NewMerkleTreeTask()
	task.ClusterName = "test_cluster"
	task.DBName = dbName
	task.QualifiedTableName = qualifiedTableName
	task.Nodes = strings.Join(nodes, ",")
	task.BlockSize = 1000
	return task
}

func testMerkleTreeInit(t *testing.T, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")

	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
	})

	aceSchema := config.Cfg.MTree.Schema
	cdcPubName := config.Cfg.MTree.CDC.PublicationName
	cdcSlotName := config.Cfg.MTree.CDC.SlotName

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.True(t, schemaExists(t, ctx, pool, aceSchema), "Schema '%s' should exist", aceSchema)
		require.True(t, functionExists(t, ctx, pool, "bytea_xor", aceSchema), "Function 'bytea_xor' should exist in schema '%s'", aceSchema)
		require.True(t, tableExists(t, ctx, pool, "ace_cdc_metadata", aceSchema), "Table 'ace_cdc_metadata' should exist in schema '%s'", aceSchema)
		require.True(t, publicationExists(t, ctx, pool, cdcPubName), "Publication '%s' should exist", cdcPubName)
		require.True(t, replicationSlotExists(t, ctx, pool, cdcSlotName), "Replication slot '%s' should exist", cdcSlotName)
	}
}

func testMerkleTreeBuild(t *testing.T, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")

	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed before Build")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
	})

	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)
	pool := pgCluster.Node1Pool

	require.True(t, tableExists(t, ctx, pool, mtreeTableName, aceSchema), "Merkle tree table '%s' should exist", mtreeTableName)

	var numBlocks int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT num_blocks FROM %s.ace_mtree_metadata WHERE schema_name = $1 AND table_name = $2", aceSchema), testSchema, tableName).Scan(&numBlocks)
	require.NoError(t, err, "Should be able to get num_blocks from metadata")
	require.Greater(t, numBlocks, 0, "Number of blocks should be positive")

	var leafNodeCount int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCount)
	require.NoError(t, err, "Should be able to count leaf nodes")

	require.Equal(t, numBlocks+1, leafNodeCount, "Number of leaf nodes should match num_blocks in metadata")

	var totalNodeCount int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s", aceSchema, mtreeTableName)).Scan(&totalNodeCount)
	require.NoError(t, err, "Should be able to count total nodes")
	require.Greater(t, totalNodeCount, leafNodeCount, "Total nodes should be greater than leaf nodes")
}

func testMerkleTreeDiffDataOnlyOnNode1(t *testing.T, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		repairTable(t, qualifiedTableName, serviceN2)
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})
	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	tx, err := pgCluster.Node1Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	updateSQL := fmt.Sprintf("UPDATE %s SET email = 'updated.on.n1@example.com' WHERE index = 1", qualifiedTableName)
	_, err = tx.Exec(ctx, updateSQL)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found. Result: %+v", pairKey, mtreeTask.DiffResult)

	require.Equal(t, 1, len(nodeDiffs.Rows[serviceN1]), "Expected 1 modified row on %s, got %d", serviceN1, len(nodeDiffs.Rows[serviceN1]))
	require.Equal(t, 1, len(nodeDiffs.Rows[serviceN2]), "Expected 1 original row on %s, got %d", serviceN2, len(nodeDiffs.Rows[serviceN2]))

	require.Equal(t, 1, mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be 1")

	diffRowN1 := nodeDiffs.Rows[serviceN1][0]
	indexValN1, ok := diffRowN1.Get("index")
	require.True(t, ok, "index not found in diff row for node1")
	require.Equal(t, int32(1), indexValN1, "Incorrect index found in diff row for node1")
	emailValN1, ok := diffRowN1.Get("email")
	require.True(t, ok, "email not found in diff row for node1")
	require.Equal(t, "updated.on.n1@example.com", emailValN1, "Incorrect email found in diff row for node1")

	diffRowN2 := nodeDiffs.Rows[serviceN2][0]
	indexValN2, ok := diffRowN2.Get("index")
	require.True(t, ok, "index not found in diff row for node2")
	require.Equal(t, int32(1), indexValN2, "Incorrect index found in diff row for node2")
	emailValN2, ok := diffRowN2.Get("email")
	require.True(t, ok, "email not found in diff row for node2")
	require.Equal(t, "tinaevans@dalton.com", emailValN2, "Incorrect email found in diff row for node2")
}

type compositeBoundaryKey struct {
	Index      int32
	CustomerID string
}

func testMerkleTreeDiffBoundaryModifications(t *testing.T, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		if mtreeTask.SimplePrimaryKey {
			repairTable(t, qualifiedTableName, serviceN1)
		} else {
			// Repair logic might be different or need careful handling for composite keys
			t.Logf("Skipping repair for composite key table '%s'", qualifiedTableName)
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})
	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)

	var boundaryPkeys []any
	if mtreeTask.SimplePrimaryKey {
		query := fmt.Sprintf("SELECT range_start FROM %s.%s WHERE node_level = 0 AND range_start IS NOT NULL UNION SELECT range_end FROM %s.%s WHERE node_level=0 AND range_end IS NOT NULL", aceSchema, mtreeTableName, aceSchema, mtreeTableName)
		rows, err := pgCluster.Node1Pool.Query(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var pkey int32
			err := rows.Scan(&pkey)
			require.NoError(t, err)
			boundaryPkeys = append(boundaryPkeys, pkey)
		}
	} else {
		// For composite keys, we get the text representation and parse it.
		query := fmt.Sprintf("SELECT range_start::text FROM %s.%s WHERE node_level = 0 AND range_start IS NOT NULL UNION SELECT range_end::text FROM %s.%s WHERE node_level=0 AND range_end IS NOT NULL", aceSchema, mtreeTableName, aceSchema, mtreeTableName)
		rows, err := pgCluster.Node1Pool.Query(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		re := regexp.MustCompile(`^\((\d+),"?([^",]+)"?\)$`)

		for rows.Next() {
			var pkeyStr string
			err := rows.Scan(&pkeyStr)
			require.NoError(t, err)
			matches := re.FindStringSubmatch(pkeyStr)
			if len(matches) == 3 {
				index, err := strconv.Atoi(matches[1])
				require.NoError(t, err)
				boundaryPkeys = append(boundaryPkeys, compositeBoundaryKey{
					Index:      int32(index),
					CustomerID: matches[2],
				})
			}
		}
	}

	require.NotEmpty(t, boundaryPkeys, "Should have found some boundary pkeys")

	// Sample up to 5 keys to modify
	var pkeysToModify []any
	if len(boundaryPkeys) > 5 {
		for i := 0; i < 5; i++ {
			pkeysToModify = append(pkeysToModify, boundaryPkeys[rand.Intn(len(boundaryPkeys))])
		}
	} else {
		pkeysToModify = boundaryPkeys
	}

	// Make pkeysToModify unique
	uniquePkeys := make(map[any]bool)
	var uniquePkeysList []any
	for _, pkey := range pkeysToModify {
		if !uniquePkeys[pkey] {
			uniquePkeys[pkey] = true
			uniquePkeysList = append(uniquePkeysList, pkey)
		}
	}
	pkeysToModify = uniquePkeysList

	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	if mtreeTask.SimplePrimaryKey {
		for _, pkey := range pkeysToModify {
			updateSQL := fmt.Sprintf("UPDATE %s SET email = 'boundary.update.%v@example.com' WHERE index = $1", qualifiedTableName, pkey)
			_, err := tx.Exec(ctx, updateSQL, pkey)
			require.NoError(t, err)
		}
	} else {
		for _, pkey := range pkeysToModify {
			ckey := pkey.(compositeBoundaryKey)
			updateSQL := fmt.Sprintf("UPDATE %s SET email = 'boundary.update.%v@example.com' WHERE index = $1 AND customer_id = $2", qualifiedTableName, ckey.Index)
			_, err := tx.Exec(ctx, updateSQL, ckey.Index, ckey.CustomerID)
			require.NoError(t, err)
		}
	}

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found.", pairKey)

	expectedDiffCount := len(pkeysToModify)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[serviceN1]), "Expected %d modified rows on %s", expectedDiffCount, serviceN1)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[serviceN2]), "Expected %d modified rows on %s", expectedDiffCount, serviceN2)
	require.Equal(t, expectedDiffCount, mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be %d", expectedDiffCount)

	if mtreeTask.SimplePrimaryKey {
		for _, pkey := range pkeysToModify {
			foundN1 := false
			foundN2 := false
			for _, row := range nodeDiffs.Rows[serviceN1] {
				if rIndex, ok := row.Get("index"); ok && rIndex == pkey {
					foundN1 = true
					break
				}
			}
			for _, row := range nodeDiffs.Rows[serviceN2] {
				if rIndex, ok := row.Get("index"); ok && rIndex == pkey {
					foundN2 = true
					emailVal, _ := row.Get("email")
					expectedEmail := fmt.Sprintf("boundary.update.%v@example.com", pkey)
					require.Equal(t, expectedEmail, emailVal)
					break
				}
			}
			require.True(t, foundN1, "Did not find original row for index %v on node1", pkey)
			require.True(t, foundN2, "Did not find modified row for index %v on node2", pkey)
		}
	} else {
		for _, pkey := range pkeysToModify {
			ckey := pkey.(compositeBoundaryKey)
			foundN1 := false
			foundN2 := false
			for _, row := range nodeDiffs.Rows[serviceN1] {
				rIndex, _ := row.Get("index")
				rCustomerID, _ := row.Get("customer_id")
				if rIndex == ckey.Index && rCustomerID == ckey.CustomerID {
					foundN1 = true
					break
				}
			}
			for _, row := range nodeDiffs.Rows[serviceN2] {
				rIndex, _ := row.Get("index")
				rCustomerID, _ := row.Get("customer_id")
				if rIndex == ckey.Index && rCustomerID == ckey.CustomerID {
					foundN2 = true
					emailVal, _ := row.Get("email")
					expectedEmail := fmt.Sprintf("boundary.update.%v@example.com", ckey.Index)
					require.Equal(t, expectedEmail, emailVal)
					break
				}
			}
			require.True(t, foundN1, "Did not find original row for index %v on node1", ckey)
			require.True(t, foundN2, "Did not find modified row for index %v on node2", ckey)
		}
	}
}

func testMerkleTreeDiffModifiedRows(t *testing.T, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		repairTable(t, qualifiedTableName, serviceN1)
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})
	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	modifications := []struct {
		indexVal int
		field    string
		value    string
	}{
		{
			indexVal: rand.Intn(10000) + 1,
			field:    "email",
			value:    "john.doe.updated.mtree@example.com",
		},
		{
			indexVal: rand.Intn(10000) + 1,
			field:    "first_name",
			value:    "PeterMtreeUpdated",
		},
	}

	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	for _, mod := range modifications {
		updateSQL := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE index = $2", qualifiedTableName, mod.field)
		_, err := tx.Exec(ctx, updateSQL, mod.value, mod.indexVal)
		require.NoError(t, err)
	}

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found.", pairKey)

	expectedDiffCount := len(modifications)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[serviceN1]), "Expected %d modified rows on %s", expectedDiffCount, serviceN1)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[serviceN2]), "Expected %d modified rows on %s", expectedDiffCount, serviceN2)

	require.Equal(t, expectedDiffCount, mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be %d", expectedDiffCount)

	for _, mod := range modifications {
		foundN1 := false
		foundN2 := false
		for _, row := range nodeDiffs.Rows[serviceN1] {
			if rIndex, ok := row.Get("index"); ok && rIndex.(int32) == int32(mod.indexVal) {
				foundN1 = true
				fieldVal, _ := row.Get(mod.field)
				require.NotEqual(t, mod.value, fieldVal, "Original row on node1 should not have the modified value")
				break
			}
		}
		for _, row := range nodeDiffs.Rows[serviceN2] {
			if rIndex, ok := row.Get("index"); ok && rIndex.(int32) == int32(mod.indexVal) {
				foundN2 = true
				fieldVal, _ := row.Get(mod.field)
				require.Equal(t, mod.value, fieldVal, "Modified row on node2 does not contain the new value")
				break
			}
		}
		require.True(t, foundN1, "Did not find original row for index %d on node1", mod.indexVal)
		require.True(t, foundN2, "Did not find modified row for index %d on node2", mod.indexVal)
	}
}

func testMerkleTreeTeardown(t *testing.T, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")

	err = mtreeTask.MtreeTeardown()
	require.NoError(t, err, "MtreeTeardown should succeed")

	aceSchema := config.Cfg.MTree.Schema
	cdcPubName := config.Cfg.MTree.CDC.PublicationName
	cdcSlotName := config.Cfg.MTree.CDC.SlotName

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.False(t, tableExists(t, ctx, pool, "ace_cdc_metadata", aceSchema), "Table 'ace_cdc_metadata' should NOT exist after teardown")
		require.False(t, publicationExists(t, ctx, pool, cdcPubName), "Publication '%s' should NOT exist after teardown", cdcPubName)
		require.False(t, replicationSlotExists(t, ctx, pool, cdcSlotName), "Replication slot '%s' should NOT exist after teardown", cdcSlotName)

		require.True(t, schemaExists(t, ctx, pool, aceSchema), "Schema '%s' should still exist after teardown", aceSchema)
		require.True(t, functionExists(t, ctx, pool, "bytea_xor", aceSchema), "Function 'bytea_xor' should still exist after teardown")
	}
}

func schemaExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)", schemaName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func functionExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, functionName, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 
			FROM pg_proc p 
			JOIN pg_namespace n ON p.pronamespace = n.oid 
			WHERE p.proname = $1 AND n.nspname = $2
		)`, functionName, schemaName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func tableExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2
		)`, schemaName, tableName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func publicationExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", pubName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func replicationSlotExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slotName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	require.NoError(t, err)
	return exists
}
