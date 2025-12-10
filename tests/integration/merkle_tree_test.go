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
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestMerkleTreeSimplePK(t *testing.T) {
	tableName := "customers"
	runMerkleTreeTests(t, tableName)
}

func TestMerkleTreeCompositePK(t *testing.T) {
	tableName := "customers"
	ctx := context.Background()
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
			require.NoError(t, err)
		}
	})
	runMerkleTreeTests(t, tableName)
}

func runMerkleTreeTests(t *testing.T, tableName string) {
	t.Run("Init", func(t *testing.T) {
		testMerkleTreeInit(t, tableName)
	})
	t.Run("Build", func(t *testing.T) {
		testMerkleTreeBuild(t, tableName)
	})
	if tableName == "customers" {
		t.Run("Diff_DataOnlyOnNode1", func(t *testing.T) {
			testMerkleTreeDiffDataOnlyOnNode1(t, tableName)
		})
		t.Run("Diff_ModifiedRows", func(t *testing.T) {
			testMerkleTreeDiffModifiedRows(t, tableName)
		})
		t.Run("Diff_BoundaryModifications", func(t *testing.T) {
			testMerkleTreeDiffBoundaryModifications(t, tableName)
		})
		t.Run("MergeInitialRanges", func(t *testing.T) {
			testMerkleTreeMergeInitialRanges(t, tableName)
		})
		t.Run("MergeMiddleRanges", func(t *testing.T) {
			testMerkleTreeMergeMiddleRanges(t, tableName)
		})
		t.Run("MergeLastRanges", func(t *testing.T) {
			testMerkleTreeMergeLastRanges(t, tableName)
		})
		t.Run("SplitInitialRanges", func(t *testing.T) {
			testMerkleTreeSplitInitialRanges(t, tableName)
		})
		t.Run("SplitMiddleRanges", func(t *testing.T) {
			testMerkleTreeSplitMiddleRanges(t, tableName)
		})
		t.Run("SplitLastRanges", func(t *testing.T) {
			testMerkleTreeSplitLastRanges(t, tableName)
		})
		t.Run("ContinuousCDC", func(t *testing.T) {
			testMerkleTreeContinuousCDC(t, tableName)
		})
	}
	t.Run("Teardown", func(t *testing.T) {
		testMerkleTreeTeardown(t, tableName)
	})
}

func newTestMerkleTreeTask(t *testing.T, qualifiedTableName string, nodes []string) *mtree.MerkleTreeTask {
	t.Helper()
	task := mtree.NewMerkleTreeTask()
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

	require.GreaterOrEqual(t, leafNodeCount, numBlocks, "Number of leaf nodes should match num_blocks in metadata")

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

	if mtreeTask.SimplePrimaryKey {
		updateSQL := fmt.Sprintf("UPDATE %s SET email = 'updated.on.n1@example.com' WHERE index = 1", qualifiedTableName)
		_, err = tx.Exec(ctx, updateSQL)
		require.NoError(t, err)
	} else {
		var customerID string
		err := pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT customer_id FROM %s WHERE index = 1 LIMIT 1", qualifiedTableName)).Scan(&customerID)
		require.NoError(t, err, "could not get customer_id for index 1")
		updateSQL := fmt.Sprintf("UPDATE %s SET email = 'updated.on.n1@example.com' WHERE index = 1 AND customer_id = $1", qualifiedTableName)
		_, err = tx.Exec(ctx, updateSQL, customerID)
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
		_, _ = pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = regexp_replace(email, '\\\\.(cdc_hw|cdc_fallback)$', '')", qualifiedTableName))
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

func testMerkleTreeContinuousCDC(t *testing.T, tableName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	largeTableName := "customers_1M"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1}
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodeInfo := pgCluster.ClusterNodes[0]
		cdc.ListenForChanges(ctx, nodeInfo)
	}()

	time.Sleep(3 * time.Second)

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)
	pool := pgCluster.Node1Pool

	var leafNodeCount int
	err = pool.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCount)
	require.NoError(t, err)
	require.Greater(t, leafNodeCount, 3, "Not enough leaf nodes for CDC test")

	firstBlockPos := 0
	middleBlockPos := leafNodeCount / 2
	lastBlockPos := leafNodeCount - 2

	tx, err := pool.Begin(context.Background())
	require.NoError(t, err)
	defer tx.Rollback(context.Background())

	_, err = tx.Exec(context.Background(), "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	if mtreeTask.SimplePrimaryKey {
		var firstBlockStart, middleBlockStart, lastBlockStart int32
		err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT range_start FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), firstBlockPos).Scan(&firstBlockStart)
		require.NoError(t, err)
		err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT range_start FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), middleBlockPos).Scan(&middleBlockStart)
		require.NoError(t, err)
		err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT range_start FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), lastBlockPos).Scan(&lastBlockStart)
		require.NoError(t, err)

		// DELETE + INSERT in first block
		_, err = tx.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE index = $1", qualifiedTableName), firstBlockStart)
		require.NoError(t, err)
		insertSQL := fmt.Sprintf("INSERT INTO %s  SELECT * FROM %s WHERE index = $1", qualifiedTableName, pgx.Identifier{largeTableName}.Sanitize())
		_, err = tx.Exec(context.Background(), insertSQL, firstBlockStart)
		require.NoError(t, err)

		// UPDATE in middle block
		updateSQL := fmt.Sprintf("UPDATE %s SET email = 'cdc.update.test@example.com' WHERE index = $1", qualifiedTableName)
		_, err = tx.Exec(context.Background(), updateSQL, middleBlockStart)
		require.NoError(t, err)

		// DELETE in last block
		_, err = tx.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE index = $1", qualifiedTableName), lastBlockStart)
		require.NoError(t, err)

	} else { // Composite Primary Key
		re := regexp.MustCompile(`^\((\d+),"?([^",]+)"?\)$`)
		parseKey := func(keyStr string) (int32, string) {
			matches := re.FindStringSubmatch(keyStr)
			require.Len(t, matches, 3, "could not parse composite key: %s", keyStr)
			index, err := strconv.Atoi(matches[1])
			require.NoError(t, err)
			return int32(index), matches[2]
		}

		var firstKeyStr, middleKeyStr, lastKeyStr string
		err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT range_start::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), firstBlockPos).Scan(&firstKeyStr)
		require.NoError(t, err)
		err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT range_start::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), middleBlockPos).Scan(&middleKeyStr)
		require.NoError(t, err)
		err = tx.QueryRow(context.Background(), fmt.Sprintf("SELECT range_start::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), lastBlockPos).Scan(&lastKeyStr)
		require.NoError(t, err)

		firstIdx, firstCustId := parseKey(firstKeyStr)
		middleIdx, middleCustId := parseKey(middleKeyStr)
		lastIdx, lastCustId := parseKey(lastKeyStr)

		// DELETE + INSERT in first block
		_, err = tx.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE index = $1 AND customer_id = $2", qualifiedTableName), firstIdx, firstCustId)
		require.NoError(t, err)
		insertSQL := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE index = $1 AND customer_id = $2", qualifiedTableName, pgx.Identifier{largeTableName}.Sanitize())
		_, err = tx.Exec(context.Background(), insertSQL, firstIdx, firstCustId)
		require.NoError(t, err)

		// UPDATE in middle block
		updateSQL := fmt.Sprintf("UPDATE %s SET email = 'cdc.update.test.composite@example.com' WHERE index = $1 AND customer_id = $2", qualifiedTableName)
		_, err = tx.Exec(context.Background(), updateSQL, middleIdx, middleCustId)
		require.NoError(t, err)

		// DELETE in last block
		_, err = tx.Exec(context.Background(), fmt.Sprintf("DELETE FROM %s WHERE index = $1 AND customer_id = $2", qualifiedTableName), lastIdx, lastCustId)
		require.NoError(t, err)
	}

	_, err = tx.Exec(context.Background(), "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(context.Background()))

	time.Sleep(5 * time.Second)

	type blockCounters struct {
		Inserts int
		Deletes int
		Dirty   bool
	}
	var counters blockCounters

	// Verify first block (DELETE + INSERT)
	err = pool.QueryRow(context.Background(), fmt.Sprintf("SELECT inserts_since_tree_update, deletes_since_tree_update, dirty FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), firstBlockPos).Scan(&counters.Inserts, &counters.Deletes, &counters.Dirty)
	require.NoError(t, err, "failed to query counters for first block")
	require.Equal(t, 1, counters.Inserts, "Expected 1 insert in first block")
	require.Equal(t, 1, counters.Deletes, "Expected 1 delete in first block")
	require.True(t, counters.Dirty, "First block should be dirty")

	// Verify middle block (UPDATE)
	err = pool.QueryRow(context.Background(), fmt.Sprintf("SELECT inserts_since_tree_update, deletes_since_tree_update, dirty FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), middleBlockPos).Scan(&counters.Inserts, &counters.Deletes, &counters.Dirty)
	require.NoError(t, err, "failed to query counters for middle block")
	require.Equal(t, 0, counters.Inserts, "Expected 0 inserts in middle block")
	require.Equal(t, 0, counters.Deletes, "Expected 0 deletes in middle block")
	require.True(t, counters.Dirty, "Middle block should be dirty after update")

	// Verify last block (DELETE)
	err = pool.QueryRow(context.Background(), fmt.Sprintf("SELECT inserts_since_tree_update, deletes_since_tree_update, dirty FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), lastBlockPos).Scan(&counters.Inserts, &counters.Deletes, &counters.Dirty)
	require.NoError(t, err, "failed to query counters for last block")
	require.Equal(t, 0, counters.Inserts, "Expected 0 inserts in last block")
	require.Equal(t, 1, counters.Deletes, "Expected 1 delete in last block")
	require.True(t, counters.Dirty, "Last block should be dirty")

	cancel()
	wg.Wait()

	nodes = []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodes)
	err = tdTask.RunChecks(false)
	require.NoError(t, err, "RunChecks after CDC should succeed")

	err = tdTask.ExecuteTask()
	require.NoError(t, err, "ExecuteTask should succeed")
}

type mergeCase string

const (
	initial mergeCase = "initial"
	middle  mergeCase = "middle"
	last    mergeCase = "last"
)

type splitCase string

const (
	splitInitial splitCase = "initial"
	splitMiddle  splitCase = "middle"
	splitLast    splitCase = "last"
)

func testMerkleTreeMergeInitialRanges(t *testing.T, tableName string) {
	runMerkleTreeMergeTest(t, tableName, initial)
}

func testMerkleTreeMergeMiddleRanges(t *testing.T, tableName string) {
	runMerkleTreeMergeTest(t, tableName, middle)
}

func testMerkleTreeMergeLastRanges(t *testing.T, tableName string) {
	runMerkleTreeMergeTest(t, tableName, last)
}

func testMerkleTreeSplitInitialRanges(t *testing.T, tableName string) {
	runMerkleTreeSplitTest(t, tableName, splitInitial)
}

func testMerkleTreeSplitMiddleRanges(t *testing.T, tableName string) {
	runMerkleTreeSplitTest(t, tableName, splitMiddle)
}

func testMerkleTreeSplitLastRanges(t *testing.T, tableName string) {
	runMerkleTreeSplitTest(t, tableName, splitLast)
}

func runMerkleTreeMergeTest(t *testing.T, tableName string, mc mergeCase) {
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

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)

	var leafNodeCountBefore int
	err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountBefore)
	require.NoError(t, err)

	var startPos, endPos int
	switch mc {
	case initial:
		require.GreaterOrEqual(t, leafNodeCountBefore, 2, "Not enough leaf nodes to test initial merge")
		startPos, endPos = 0, 1
	case middle:
		require.GreaterOrEqual(t, leafNodeCountBefore, 4, "Not enough leaf nodes to test middle merge")
		startPos = leafNodeCountBefore / 2
		endPos = startPos + 1
	case last:
		require.GreaterOrEqual(t, leafNodeCountBefore, 2, "Not enough leaf nodes to test last range merge")
		startPos = leafNodeCountBefore - 2
		endPos = -1 // Indicates deletion to the end
	}

	var deletedCount int64
	var startRange, endRange []any

	if mtreeTask.SimplePrimaryKey {
		var startKey int32
		err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_start FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), startPos).Scan(&startKey)
		require.NoError(t, err)
		startRange = []any{startKey}

		if endPos != -1 {
			var endKey int32
			err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_end FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), endPos).Scan(&endKey)
			require.NoError(t, err)
			endRange = []any{endKey}
		}
	} else {
		re := regexp.MustCompile(`^\((\d+),"?([^",]+)"?\)$`)
		var startStr string
		err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_start::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), startPos).Scan(&startStr)
		require.NoError(t, err)
		startMatches := re.FindStringSubmatch(startStr)
		require.Len(t, startMatches, 3, "should parse composite key from string")
		startIndex, _ := strconv.Atoi(startMatches[1])
		startRange = []any{int32(startIndex), startMatches[2]}

		if endPos != -1 {
			var endStr string
			err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_end::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), endPos).Scan(&endStr)
			require.NoError(t, err)
			endMatches := re.FindStringSubmatch(endStr)
			require.Len(t, endMatches, 3, "should parse composite key from string")
			endIndex, _ := strconv.Atoi(endMatches[1])
			endRange = []any{int32(endIndex), endMatches[2]}
		}
	}

	tx, err := pgCluster.Node1Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	var cmdTag pgconn.CommandTag
	if endPos == -1 { // Deletion to the end
		if mtreeTask.SimplePrimaryKey {
			deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE index >= $1", qualifiedTableName)
			cmdTag, err = tx.Exec(ctx, deleteSQL, startRange[0])
		} else {
			deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE (index, customer_id) >= ($1, $2)", qualifiedTableName)
			cmdTag, err = tx.Exec(ctx, deleteSQL, startRange[0], startRange[1])
		}
	} else { // Deletion within a range
		if mtreeTask.SimplePrimaryKey {
			deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE index >= $1 AND index <= $2", qualifiedTableName)
			cmdTag, err = tx.Exec(ctx, deleteSQL, startRange[0], endRange[0])
		} else {
			deleteSQL := fmt.Sprintf("DELETE FROM %s WHERE (index, customer_id) >= ($1, $2) AND (index, customer_id) <= ($3, $4)", qualifiedTableName)
			cmdTag, err = tx.Exec(ctx, deleteSQL, startRange[0], startRange[1], endRange[0], endRange[1])
		}
	}
	require.NoError(t, err)
	deletedCount = cmdTag.RowsAffected()

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	require.Greater(t, deletedCount, int64(0), "should have deleted some rows")

	mtreeTask.Nodes = serviceN1
	mtreeTask.Rebalance = true
	err = mtreeTask.UpdateMtree(true)
	require.NoError(t, err, "UpdateMtree with rebalance should succeed")

	var leafNodeCountAfter int
	err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountAfter)
	require.NoError(t, err)
	require.Less(t, leafNodeCountAfter, leafNodeCountBefore, "Number of leaf nodes should decrease after merge")

	mtreeTask.Nodes = strings.Join(nodes, ",")
	err = mtreeTask.RunChecks(true)
	require.NoError(t, err, "RunChecks after re-merge should succeed")
	mtreeTask.NoCDC = true // Skip CDC in DiffMtree since changes are already flushed
	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found.", pairKey)

	require.Equal(t, 0, len(nodeDiffs.Rows[serviceN1]), "Expected 0 extra rows on %s", serviceN1)
	require.Equal(t, int(deletedCount), len(nodeDiffs.Rows[serviceN2]), "Expected %d missing rows on %s", deletedCount, serviceN1)
	require.Equal(t, int(deletedCount), mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be %d", deletedCount)
}

func runMerkleTreeSplitTest(t *testing.T, tableName string, sc splitCase) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	largeTableName := "customers_1M"
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")

	rowsToInsert := mtreeTask.BlockSize * 2
	tempTableName := fmt.Sprintf("temp_split_rows_%s", sc)

	var deleteRange []int32
	switch sc {
	case splitInitial:
		// Test uses index=1, so don't delete that.
		deleteRange = []int32{2, int32(2 + rowsToInsert - 1)}
	case splitMiddle:
		deleteRange = []int32{4001, int32(4001 + rowsToInsert - 1)}
	case splitLast:
		// No deletes needed for last split
	}
	if deleteRange != nil {
		tx, err := pgCluster.Node2Pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "DROP TABLE IF EXISTS "+tempTableName)
		require.NoError(t, err)

		createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE index >= %d AND index <= %d", tempTableName, qualifiedTableName, deleteRange[0], deleteRange[1])
		_, err = tx.Exec(ctx, createSQL)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE index >= %d AND index <= %d", qualifiedTableName, deleteRange[0], deleteRange[1]))
		require.NoError(t, err)

		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}

	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}

		_, err = pgCluster.Node2Pool.Exec(ctx, "DROP TABLE IF EXISTS "+tempTableName)
		if err != nil {
			t.Logf("Warning: failed to drop temp table %s during cleanup: %v", tempTableName, err)
		}

		err = mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
	})

	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")

	mtreeTask.Nodes = serviceN2
	mtreeTask.RunChecks(false)
	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)

	var leafNodeCountBefore int
	err = pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountBefore)
	require.NoError(t, err)

	var q string
	switch sc {
	case splitInitial, splitMiddle:
		q = fmt.Sprintf("INSERT INTO %s SELECT * FROM %s order by index", qualifiedTableName, tempTableName)
	case splitLast:
		var maxIndex int32
		err := pgCluster.Node2Pool.QueryRow(ctx, "SELECT max(index) FROM "+qualifiedTableName).Scan(&maxIndex)
		require.NoError(t, err)
		insertStartIndex := maxIndex + 1
		q = fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE index >= %d order by index LIMIT %d", qualifiedTableName, pgx.Identifier{largeTableName}.Sanitize(), insertStartIndex, rowsToInsert)
	}

	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)

	_, err = tx.Exec(ctx, q)
	require.NoError(t, err)
	if sc != splitLast {
		_, err = tx.Exec(ctx, "DROP TABLE "+tempTableName)
		require.NoError(t, err)
	}

	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	mtreeTask.Rebalance = false
	err = mtreeTask.UpdateMtree(false)
	require.NoError(t, err, "UpdateMtree should succeed")

	var leafNodeCountAfter int
	pool, err := connectToNode(pgCluster.Node2Host, pgCluster.Node2Port, pgEdgeUser, pgEdgePassword, dbName)
	require.NoError(t, err)

	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountAfter)
	require.NoError(t, err)

	require.Greater(t, leafNodeCountAfter, leafNodeCountBefore, "Number of leaf nodes should increase after split")

	// // Diff should be clean since we performed identical inserts
	// mtreeTask.NoCDC = true
	// err = mtreeTask.DiffMtree()
	// require.NoError(t, err, "DiffMtree after splits should succeed")
	// require.Empty(t, mtreeTask.DiffResult.NodeDiffs, "Merkle trees should be in sync after identical splits")
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
		_, err = tx.Exec(ctx, updateSQL, mod.value, mod.indexVal)
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
