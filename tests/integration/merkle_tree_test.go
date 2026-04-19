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
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestMerkleTreeSimplePK(t *testing.T) {
	env := newSpockEnv()
	tableName := "customers"
	runMerkleTreeTests(t, env, tableName)
}

func TestMerkleTreeCompositePK(t *testing.T) {
	env := newSpockEnv()
	tableName := "customers"
	ctx := context.Background()
	for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
		err := alterTableToCompositeKey(ctx, pool, testSchema, tableName)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
			err := revertTableToSimpleKey(ctx, pool, testSchema, tableName)
			require.NoError(t, err)
		}
	})
	runMerkleTreeTests(t, env, tableName)
}

func TestMerkleTreeUntilFilter(t *testing.T) {
	ctx := context.Background()
	// Use a dedicated small table so we can INSERT within the block range.
	// The customers table has dense indices 1-10000; inserting outside that
	// range falls beyond the last block's upper bound and is invisible to
	// processWorkItem's range-bounded query.
	untilTable := "mtree_until_test"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, untilTable)
	nodes := []string{serviceN1, serviceN2}

	// Create the table on both nodes and add to the default repset.
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, payload TEXT)", qualifiedTable))
		require.NoError(t, err, "create table on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf(
			"SELECT spock.repset_add_table('default', '%s')", qualifiedTable))
		require.NoError(t, err, "add to repset on %s", nodeName)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifiedTable))
		}
	})

	// Seed identical rows on both nodes with a gap at id=5 (inside one block).
	// The gap lets us INSERT id=5 later as a genuinely new row (not an update
	// of an existing frozen tuple) so --until can cleanly exclude it.
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err)
		for id := 1; id <= 10; id++ {
			if id == 5 {
				continue // leave a gap
			}
			_, err = tx.Exec(ctx, fmt.Sprintf(
				"INSERT INTO %s (id, payload) VALUES ($1, $2) ON CONFLICT DO NOTHING", qualifiedTable),
				id, fmt.Sprintf("row-%d", id))
			require.NoError(t, err)
		}
		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}

	// VACUUM FREEZE so baseline rows have NULL commit timestamps.
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, fmt.Sprintf("VACUUM FREEZE %s", qualifiedTable))
		require.NoError(t, err)
	}

	// Init + build the merkle tree.
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTable, nodes)
	mtreeTask.BlockSize = 1000 // single block covers all rows
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		mtreeTask.MtreeTeardown()
	})
	require.NoError(t, mtreeTask.BuildMtree())

	// Capture fence while both nodes are identical and frozen.
	var fence time.Time
	err := pgCluster.Node1Pool.QueryRow(ctx, "SELECT now()").Scan(&fence)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// INSERT id=5 on node1 only — fills the gap, within block range [1, 10].
	// This is a new row (not an update), so the frozen baseline on both nodes
	// is untouched. Node2 simply doesn't have id=5.
	tx, err := pgCluster.Node1Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	_, err = tx.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (id, payload) VALUES (5, 'divergent')", qualifiedTable))
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	// Diff WITHOUT --until first: the divergent write should be visible.
	// Running this first ensures UpdateMtree processes any CDC events.
	taskWithout := newTestMerkleTreeTask(t, qualifiedTable, nodes)
	taskWithout.Mode = "diff"
	taskWithout.Output = "json"
	taskWithout.BlockSize = 1000
	require.NoError(t, taskWithout.RunChecks(false))
	require.NoError(t, taskWithout.DiffMtree())

	totalWithout := 0
	for _, count := range taskWithout.DiffResult.Summary.DiffRowsCount {
		totalWithout += count
	}
	require.Greater(t, totalWithout, 0, "without --until, expected at least 1 diff row")

	// Diff WITH --until set to the fence: the divergent row's xmin is
	// post-fence, so it is excluded. The frozen baseline (NULL timestamps)
	// is included via IS NULL on both nodes and matches.
	taskWithUntil := newTestMerkleTreeTask(t, qualifiedTable, nodes)
	taskWithUntil.Until = fence.Format(time.RFC3339Nano)
	taskWithUntil.Mode = "diff"
	taskWithUntil.Output = "json"
	taskWithUntil.BlockSize = 1000
	require.NoError(t, taskWithUntil.RunChecks(false))
	require.NoError(t, taskWithUntil.DiffMtree())

	totalWithUntil := 0
	for _, count := range taskWithUntil.DiffResult.Summary.DiffRowsCount {
		totalWithUntil += count
	}
	require.Equal(t, 0, totalWithUntil, "with --until on frozen baseline, expected 0 diff rows")
}

func runMerkleTreeTests(t *testing.T, env *testEnv, tableName string) {
	if tableName == "customers" {
		env.resetSharedTable(t, "customers")
	}
	t.Run("Init", func(t *testing.T) {
		testMerkleTreeInit(t, env, tableName)
	})
	t.Run("Build", func(t *testing.T) {
		testMerkleTreeBuild(t, env, tableName)
	})
	if tableName == "customers" {
		t.Run("Diff_DataOnlyOnNode1", func(t *testing.T) {
			testMerkleTreeDiffDataOnlyOnNode1(t, env, tableName)
		})
		t.Run("Diff_ModifiedRows", func(t *testing.T) {
			testMerkleTreeDiffModifiedRows(t, env, tableName)
		})
		t.Run("Diff_BoundaryModifications", func(t *testing.T) {
			testMerkleTreeDiffBoundaryModifications(t, env, tableName)
		})
		t.Run("MergeInitialRanges", func(t *testing.T) {
			testMerkleTreeMergeInitialRanges(t, env, tableName)
		})
		t.Run("MergeMiddleRanges", func(t *testing.T) {
			testMerkleTreeMergeMiddleRanges(t, env, tableName)
		})
		t.Run("MergeLastRanges", func(t *testing.T) {
			testMerkleTreeMergeLastRanges(t, env, tableName)
		})
		t.Run("SplitInitialRanges", func(t *testing.T) {
			testMerkleTreeSplitInitialRanges(t, env, tableName)
		})
		t.Run("SplitMiddleRanges", func(t *testing.T) {
			testMerkleTreeSplitMiddleRanges(t, env, tableName)
		})
		t.Run("SplitLastRanges", func(t *testing.T) {
			testMerkleTreeSplitLastRanges(t, env, tableName)
		})
		t.Run("ContinuousCDC", func(t *testing.T) {
			testMerkleTreeContinuousCDC(t, env, tableName)
		})
	}
	t.Run("Teardown", func(t *testing.T) {
		testMerkleTreeTeardown(t, env, tableName)
	})
}

func testMerkleTreeInit(t *testing.T, env *testEnv, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

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

	for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
		require.True(t, schemaExists(t, ctx, pool, aceSchema), "Schema '%s' should exist", aceSchema)
		require.True(t, functionExists(t, ctx, pool, "bytea_xor", aceSchema), "Function 'bytea_xor' should exist in schema '%s'", aceSchema)
		require.True(t, tableExists(t, ctx, pool, "ace_cdc_metadata", aceSchema), "Table 'ace_cdc_metadata' should exist in schema '%s'", aceSchema)
		require.True(t, publicationExists(t, ctx, pool, cdcPubName), "Publication '%s' should exist", cdcPubName)
		require.True(t, replicationSlotExists(t, ctx, pool, cdcSlotName), "Replication slot '%s' should exist", cdcSlotName)
	}
}

func testMerkleTreeBuild(t *testing.T, env *testEnv, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{env.ServiceN1}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

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
	pool := env.N1Pool

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

func testMerkleTreeDiffDataOnlyOnNode1(t *testing.T, env *testEnv, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	env.awaitDataSync(t, qualifiedTableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		env.repairTable(t, qualifiedTableName, env.ServiceN2)
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})
	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	tx, err := env.N1Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	env.withRepairModeTx(t, ctx, tx, func() {
		if mtreeTask.SimplePrimaryKey {
			updateSQL := fmt.Sprintf("UPDATE %s SET email = 'updated.on.n1@example.com' WHERE index = 1", qualifiedTableName)
			_, err = tx.Exec(ctx, updateSQL)
			require.NoError(t, err)
		} else {
			var customerID string
			err := env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT customer_id FROM %s WHERE index = 1 LIMIT 1", qualifiedTableName)).Scan(&customerID)
			require.NoError(t, err, "could not get customer_id for index 1")
			updateSQL := fmt.Sprintf("UPDATE %s SET email = 'updated.on.n1@example.com' WHERE index = 1 AND customer_id = $1", qualifiedTableName)
			_, err = tx.Exec(ctx, updateSQL, customerID)
			require.NoError(t, err)
		}
	})
	require.NoError(t, tx.Commit(ctx))

	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found. Result: %+v", pairKey, mtreeTask.DiffResult)

	require.GreaterOrEqual(t, len(nodeDiffs.Rows[env.ServiceN1]), 1, "Expected at least 1 modified row on %s, got %d", env.ServiceN1, len(nodeDiffs.Rows[env.ServiceN1]))
	require.GreaterOrEqual(t, len(nodeDiffs.Rows[env.ServiceN2]), 1, "Expected at least 1 original row on %s, got %d", env.ServiceN2, len(nodeDiffs.Rows[env.ServiceN2]))

	require.GreaterOrEqual(t, mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], 1, "Expected summary diff count to be at least 1")

	// Find the row with index=1 in the diff (block-based diff may include other rows in the same block)
	var diffRowN1, diffRowN2 types.OrderedMap
	for _, row := range nodeDiffs.Rows[env.ServiceN1] {
		if idx, ok := row.Get("index"); ok && idx.(int32) == 1 {
			diffRowN1 = row
			break
		}
	}
	for _, row := range nodeDiffs.Rows[env.ServiceN2] {
		if idx, ok := row.Get("index"); ok && idx.(int32) == 1 {
			diffRowN2 = row
			break
		}
	}
	require.NotZero(t, diffRowN1, "Expected to find index=1 in diff rows for node1")
	require.NotZero(t, diffRowN2, "Expected to find index=1 in diff rows for node2")

	emailValN1, ok := diffRowN1.Get("email")
	require.True(t, ok, "email not found in diff row for node1")
	require.Equal(t, "updated.on.n1@example.com", emailValN1, "Incorrect email found in diff row for node1")
	emailValN2, ok := diffRowN2.Get("email")
	require.True(t, ok, "email not found in diff row for node2")
	require.Equal(t, "tinaevans@dalton.com", emailValN2, "Incorrect email found in diff row for node2")
}

type compositeBoundaryKey struct {
	Index      int32
	CustomerID string
}

func testMerkleTreeDiffBoundaryModifications(t *testing.T, env *testEnv, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	env.awaitDataSync(t, qualifiedTableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		_, _ = env.N1Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = regexp_replace(email, '\\\\.(cdc_hw|cdc_fallback)$', '')", qualifiedTableName))
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		env.repairTable(t, qualifiedTableName, env.ServiceN1)
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
		rows, err := env.N1Pool.Query(ctx, query)
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
		rows, err := env.N1Pool.Query(ctx, query)
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

	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	env.withRepairModeTx(t, ctx, tx, func() {
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
	})
	require.NoError(t, tx.Commit(ctx))

	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found.", pairKey)

	expectedDiffCount := len(pkeysToModify)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[env.ServiceN1]), "Expected %d modified rows on %s", expectedDiffCount, env.ServiceN1)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[env.ServiceN2]), "Expected %d modified rows on %s", expectedDiffCount, env.ServiceN2)
	require.Equal(t, expectedDiffCount, mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be %d", expectedDiffCount)

	if mtreeTask.SimplePrimaryKey {
		for _, pkey := range pkeysToModify {
			foundN1 := false
			foundN2 := false
			for _, row := range nodeDiffs.Rows[env.ServiceN1] {
				if rIndex, ok := row.Get("index"); ok && rIndex == pkey {
					foundN1 = true
					break
				}
			}
			for _, row := range nodeDiffs.Rows[env.ServiceN2] {
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
			for _, row := range nodeDiffs.Rows[env.ServiceN1] {
				rIndex, _ := row.Get("index")
				rCustomerID, _ := row.Get("customer_id")
				if rIndex == ckey.Index && rCustomerID == ckey.CustomerID {
					foundN1 = true
					break
				}
			}
			for _, row := range nodeDiffs.Rows[env.ServiceN2] {
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

func testMerkleTreeContinuousCDC(t *testing.T, env *testEnv, tableName string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	largeTableName := "customers_1M"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	// Skip awaitDataSync: the CDC and merkle tree work runs only on n1.
	// The final n1-vs-n2 diff (line ~721) may pick up bleed from a prior
	// subtest's cleanup, but waiting here risks 30s+ timeouts after large
	// merge/split tests whose repair replication hasn't fully settled.
	nodes := []string{env.ServiceN1}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		env.repairTable(t, qualifiedTableName, env.ServiceN2)
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
		nodeInfo := env.ClusterNodes[0]
		cdc.ListenForChanges(ctx, nodeInfo)
	}()

	time.Sleep(3 * time.Second)

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)
	pool := env.N1Pool

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

	env.withRepairModeTx(t, context.Background(), tx, func() {
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
	})
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

	nodes = []string{env.ServiceN1, env.ServiceN2}
	tdTask := env.newTableDiffTask(t, qualifiedTableName, nodes)
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

func testMerkleTreeMergeInitialRanges(t *testing.T, env *testEnv, tableName string) {
	runMerkleTreeMergeTest(t, env, tableName, initial)
}

func testMerkleTreeMergeMiddleRanges(t *testing.T, env *testEnv, tableName string) {
	runMerkleTreeMergeTest(t, env, tableName, middle)
}

func testMerkleTreeMergeLastRanges(t *testing.T, env *testEnv, tableName string) {
	runMerkleTreeMergeTest(t, env, tableName, last)
}

func testMerkleTreeSplitInitialRanges(t *testing.T, env *testEnv, tableName string) {
	runMerkleTreeSplitTest(t, env, tableName, splitInitial)
}

func testMerkleTreeSplitMiddleRanges(t *testing.T, env *testEnv, tableName string) {
	runMerkleTreeSplitTest(t, env, tableName, splitMiddle)
}

func testMerkleTreeSplitLastRanges(t *testing.T, env *testEnv, tableName string) {
	runMerkleTreeSplitTest(t, env, tableName, splitLast)
}

func runMerkleTreeMergeTest(t *testing.T, env *testEnv, tableName string, mc mergeCase) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	env.awaitDataSync(t, qualifiedTableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		env.repairTable(t, qualifiedTableName, env.ServiceN2)
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
	err = env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountBefore)
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
		err = env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_start FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), startPos).Scan(&startKey)
		require.NoError(t, err)
		startRange = []any{startKey}

		if endPos != -1 {
			var endKey int32
			err = env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_end FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), endPos).Scan(&endKey)
			require.NoError(t, err)
			endRange = []any{endKey}
		}
	} else {
		re := regexp.MustCompile(`^\((\d+),"?([^",]+)"?\)$`)
		var startStr string
		err = env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_start::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), startPos).Scan(&startStr)
		require.NoError(t, err)
		startMatches := re.FindStringSubmatch(startStr)
		require.Len(t, startMatches, 3, "should parse composite key from string")
		startIndex, _ := strconv.Atoi(startMatches[1])
		startRange = []any{int32(startIndex), startMatches[2]}

		if endPos != -1 {
			var endStr string
			err = env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT range_end::text FROM %s.%s WHERE node_level = 0 AND node_position = $1", aceSchema, mtreeTableName), endPos).Scan(&endStr)
			require.NoError(t, err)
			endMatches := re.FindStringSubmatch(endStr)
			require.Len(t, endMatches, 3, "should parse composite key from string")
			endIndex, _ := strconv.Atoi(endMatches[1])
			endRange = []any{int32(endIndex), endMatches[2]}
		}
	}

	tx, err := env.N1Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	env.withRepairModeTx(t, ctx, tx, func() {
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
	})
	require.NoError(t, tx.Commit(ctx))

	require.Greater(t, deletedCount, int64(0), "should have deleted some rows")

	mtreeTask.Nodes = env.ServiceN1
	mtreeTask.Rebalance = true
	err = mtreeTask.UpdateMtree(true)
	require.NoError(t, err, "UpdateMtree with rebalance should succeed")

	var leafNodeCountAfter int
	err = env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountAfter)
	require.NoError(t, err)
	require.Less(t, leafNodeCountAfter, leafNodeCountBefore, "Number of leaf nodes should decrease after merge")

	mtreeTask.Nodes = strings.Join(nodes, ",")
	err = mtreeTask.RunChecks(true)
	require.NoError(t, err, "RunChecks after re-merge should succeed")
	mtreeTask.NoCDC = true // Skip CDC in DiffMtree since changes are already flushed
	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found.", pairKey)

	require.Equal(t, 0, len(nodeDiffs.Rows[env.ServiceN1]), "Expected 0 extra rows on %s", env.ServiceN1)
	require.Equal(t, int(deletedCount), len(nodeDiffs.Rows[env.ServiceN2]), "Expected %d missing rows on %s", deletedCount, env.ServiceN1)
	require.Equal(t, int(deletedCount), mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be %d", deletedCount)
}

func runMerkleTreeSplitTest(t *testing.T, env *testEnv, tableName string, sc splitCase) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	env.awaitDataSync(t, qualifiedTableName)
	largeTableName := "customers_1M"
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

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
		tx, err := env.N2Pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx)

		env.withRepairModeTx(t, ctx, tx, func() {
			_, err = tx.Exec(ctx, "DROP TABLE IF EXISTS "+tempTableName)
			require.NoError(t, err)

			createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s WHERE index >= %d AND index <= %d", tempTableName, qualifiedTableName, deleteRange[0], deleteRange[1])
			_, err = tx.Exec(ctx, createSQL)
			require.NoError(t, err)

			_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE index >= %d AND index <= %d", qualifiedTableName, deleteRange[0], deleteRange[1]))
			require.NoError(t, err)
		})
		require.NoError(t, tx.Commit(ctx))
	}

	t.Cleanup(func() {
		env.repairTable(t, qualifiedTableName, env.ServiceN1)
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}

		_, err = env.N2Pool.Exec(ctx, "DROP TABLE IF EXISTS "+tempTableName)
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

	mtreeTask.Nodes = env.ServiceN2
	mtreeTask.RunChecks(false)
	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)

	var leafNodeCountBefore int
	err = env.N2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCountBefore)
	require.NoError(t, err)

	var q string
	switch sc {
	case splitInitial, splitMiddle:
		q = fmt.Sprintf("INSERT INTO %s SELECT * FROM %s order by index", qualifiedTableName, tempTableName)
	case splitLast:
		var maxIndex int32
		err := env.N2Pool.QueryRow(ctx, "SELECT max(index) FROM "+qualifiedTableName).Scan(&maxIndex)
		require.NoError(t, err)
		insertStartIndex := maxIndex + 1
		q = fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE index >= %d order by index LIMIT %d", qualifiedTableName, pgx.Identifier{largeTableName}.Sanitize(), insertStartIndex, rowsToInsert)
	}

	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	env.withRepairModeTx(t, ctx, tx, func() {
		_, err = tx.Exec(ctx, q)
		require.NoError(t, err)
		if sc != splitLast {
			_, err = tx.Exec(ctx, "DROP TABLE "+tempTableName)
			require.NoError(t, err)
		}
	})
	require.NoError(t, tx.Commit(ctx))

	mtreeTask.Rebalance = false
	err = mtreeTask.UpdateMtree(false)
	require.NoError(t, err, "UpdateMtree should succeed")

	var leafNodeCountAfter int
	pool, err := connectToNode(env.N2Host, env.N2Port, env.DBUser, env.DBPassword, env.DBName)
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

func testMerkleTreeDiffModifiedRows(t *testing.T, env *testEnv, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	env.awaitDataSync(t, qualifiedTableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")
	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		env.repairTable(t, qualifiedTableName, env.ServiceN1)
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

	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	env.withRepairModeTx(t, ctx, tx, func() {
		for _, mod := range modifications {
			updateSQL := fmt.Sprintf("UPDATE %s SET %s = $1 WHERE index = $2", qualifiedTableName, mod.field)
			_, err = tx.Exec(ctx, updateSQL, mod.value, mod.indexVal)
			require.NoError(t, err)
		}
	})
	require.NoError(t, tx.Commit(ctx))

	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "Expected diffs for pair %s, but none found.", pairKey)

	expectedDiffCount := len(modifications)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[env.ServiceN1]), "Expected %d modified rows on %s", expectedDiffCount, env.ServiceN1)
	require.Equal(t, expectedDiffCount, len(nodeDiffs.Rows[env.ServiceN2]), "Expected %d modified rows on %s", expectedDiffCount, env.ServiceN2)

	require.Equal(t, expectedDiffCount, mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey], "Expected summary diff count to be %d", expectedDiffCount)

	for _, mod := range modifications {
		foundN1 := false
		foundN2 := false
		for _, row := range nodeDiffs.Rows[env.ServiceN1] {
			if rIndex, ok := row.Get("index"); ok && rIndex.(int32) == int32(mod.indexVal) {
				foundN1 = true
				fieldVal, _ := row.Get(mod.field)
				require.NotEqual(t, mod.value, fieldVal, "Original row on node1 should not have the modified value")
				break
			}
		}
		for _, row := range nodeDiffs.Rows[env.ServiceN2] {
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

func testMerkleTreeTeardown(t *testing.T, env *testEnv, tableName string) {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")

	err = mtreeTask.MtreeTeardown()
	require.NoError(t, err, "MtreeTeardown should succeed")

	aceSchema := config.Cfg.MTree.Schema
	cdcPubName := config.Cfg.MTree.CDC.PublicationName
	cdcSlotName := config.Cfg.MTree.CDC.SlotName

	for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
		require.False(t, tableExists(t, ctx, pool, "ace_cdc_metadata", aceSchema), "Table 'ace_cdc_metadata' should NOT exist after teardown")
		require.False(t, publicationExists(t, ctx, pool, cdcPubName), "Publication '%s' should NOT exist after teardown", cdcPubName)
		require.False(t, replicationSlotExists(t, ctx, pool, cdcSlotName), "Replication slot '%s' should NOT exist after teardown", cdcSlotName)

		require.True(t, schemaExists(t, ctx, pool, aceSchema), "Schema '%s' should still exist after teardown", aceSchema)
		require.True(t, functionExists(t, ctx, pool, "bytea_xor", aceSchema), "Function 'bytea_xor' should still exist after teardown")
	}
}

// TestMerkleTreeNumericScaleInvariance verifies that mathematically equal
// numeric values with different PostgreSQL scales (e.g., 3000.00 vs 3000.0)
// produce identical merkle tree hashes and are NOT reported as differences.
func TestMerkleTreeNumericScaleInvariance(t *testing.T) {
	env := newSpockEnv()
	testMerkleTreeNumericScaleInvariance(t, env)
}

func testMerkleTreeNumericScaleInvariance(t *testing.T, env *testEnv) {
	ctx := context.Background()
	numericTable := "ace_numeric_scale_test"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, numericTable)

	// Create the table on both nodes (NUMERIC without scale so ::text can differ between nodes)
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			amount NUMERIC,
			label TEXT
		)`, qualifiedTable)
	for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err, "failed to create numeric test table")
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTable))
		}
	})

	// Insert rows with specific numeric scales using repair_mode on each node
	// independently, so each node stores different scale representations.
	// Node1: amount = 3000.00 (scale 2)
	// Node2: amount = 3000.0  (scale 1)
	// These are mathematically identical but produce different ::text output.

	rows := []struct {
		id    int
		n1Amt string // literal SQL for node1
		n2Amt string // literal SQL for node2
		label string
	}{
		{1, "3000.00", "3000.0", "scale-diff-trailing-zeros"},
		{2, "1234.5600", "1234.56", "scale-diff-extra-zeros"},
		{3, "100.10", "100.1", "scale-diff-fractional"},
		{4, "42", "42.000", "scale-diff-integer-vs-decimal"},
		{5, "0.00", "0.0", "scale-diff-zero"},
	}

	for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func(tx pgx.Tx) { _ = tx.Rollback(ctx) }(tx)
		env.withRepairModeTx(t, ctx, tx, func() {
			for _, r := range rows {
				amt := r.n1Amt
				if pool == env.N2Pool {
					amt = r.n2Amt
				}
				insertSQL := fmt.Sprintf("INSERT INTO %s (id, amount, label) VALUES ($1, %s, $2) ON CONFLICT (id) DO UPDATE SET amount = %s, label = $2", qualifiedTable, amt, amt)
				_, err := tx.Exec(ctx, insertSQL, r.id, r.label)
				require.NoError(t, err)
			}
		})
		require.NoError(t, tx.Commit(ctx))
	}

	// Verify the scales really are different in storage by checking ::text
	var n1Text, n2Text string
	err := env.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT amount::text FROM %s WHERE id = 1", qualifiedTable)).Scan(&n1Text)
	require.NoError(t, err)
	err = env.N2Pool.QueryRow(ctx, fmt.Sprintf("SELECT amount::text FROM %s WHERE id = 1", qualifiedTable)).Scan(&n2Text)
	require.NoError(t, err)
	t.Logf("Node1 amount::text = %q, Node2 amount::text = %q", n1Text, n2Text)
	// They should differ in scale representation
	require.NotEqual(t, n1Text, n2Text, "precondition: numeric ::text should differ between nodes (different scales)")

	// Now run merkle tree init + build + diff
	nodes := []string{env.ServiceN1, env.ServiceN2}
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTable, nodes)
	mtreeTask.BlockSize = 1000 // must be >= 1000 for RunChecks; table has 5 rows so 1 block

	err = mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")

	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")
	t.Cleanup(func() {
		mtreeTask.MtreeTeardown()
	})

	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	mtreeTask.NoCDC = true
	err = mtreeTask.DiffMtree()
	require.NoError(t, err, "DiffMtree should succeed")

	// The key assertion: there should be ZERO differences because trim_scale
	// normalizes numeric values before hashing.
	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	if nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pairKey]; ok {
		n1Rows := len(nodeDiffs.Rows[env.ServiceN1])
		n2Rows := len(nodeDiffs.Rows[env.ServiceN2])
		require.Equal(t, 0, n1Rows, "Expected 0 diff rows on %s, got %d (numeric scale should not cause diffs)", env.ServiceN1, n1Rows)
		require.Equal(t, 0, n2Rows, "Expected 0 diff rows on %s, got %d (numeric scale should not cause diffs)", env.ServiceN2, n2Rows)
	}
	// If pairKey is not in NodeDiffs at all, that means zero diffs — which is correct.

	diffCount := mtreeTask.DiffResult.Summary.DiffRowsCount[pairKey]
	require.Equal(t, 0, diffCount, "Expected 0 total diff rows, got %d (numeric scale differences should be normalized)", diffCount)
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
