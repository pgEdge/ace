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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/internal/infra/db"
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

// TestBuildMtreePoolLeakOnNodeError verifies that when BuildMtree errors in
// the middle of its per-node build loop, pools belonging to nodes the loop
// never reached are still closed. The row-estimate loop pre-builds a pool
// per node up front; each iteration's inner defer pool.Close only fires for
// nodes the loop actually iterates over, so pools for later nodes used to
// leak for the rest of the process lifetime on a mid-loop error. Regression
// guard for ff67d12.
func TestBuildMtreePoolLeakOnNodeError(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_mtree_leak"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	// Force BuildMtree's first iteration (n1) to fail at GetCDCMetadata,
	// so the build loop never reaches n2. n2's pool was created by the
	// row-estimate loop above and has no per-iteration defer to close it.
	// Without the outer defer added in ff67d12, n2's pool leaks.
	metaTbl := pgx.Identifier{config.Cfg.MTree.Schema, "ace_cdc_metadata"}.Sanitize()
	_, err := pgCluster.Node1Pool.Exec(ctx, "DELETE FROM "+metaTbl+" WHERE publication_name = $1", config.Cfg.MTree.CDC.PublicationName) // nosemgrep
	require.NoError(t, err)

	// Anchor the baseline at 0 by waiting for MtreeInit's n2 backend to
	// fully exit pg_stat_activity. Without this wait the baseline can
	// race with the closing MtreeInit conn — a regression that leaks a
	// single n2 conn would then match the racy baseline and pass.
	require.Eventually(t, func() bool {
		return countACEConnections(t, ctx, pgCluster.Node2Pool) == 0
	}, 5*time.Second, 100*time.Millisecond,
		"n2 ACE connections from MtreeInit should drain to 0 before measuring the leak baseline")

	err = mtreeTask.BuildMtree()
	require.Error(t, err, "BuildMtree should fail when n1 metadata is missing")
	require.Contains(t, err.Error(), "cdc metadata", "error should surface from GetCDCMetadata on n1")

	// pg_stat_activity reflects backend disconnects asynchronously; poll
	// with a generous timeout so a slow runner doesn't flake. Anchored to
	// the same 0 baseline so a single leaked conn surfaces.
	require.Eventually(t, func() bool {
		return countACEConnections(t, ctx, pgCluster.Node2Pool) == 0
	}, 5*time.Second, 100*time.Millisecond,
		"n2 pool leaked after BuildMtree errored on n1 iter (ff67d12 regressed)")
}

func countACEConnections(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_stat_activity
		WHERE application_name = 'ACE' AND datname = $1`, dbName).Scan(&n)
	require.NoError(t, err)
	return n
}

// TestBuildMtreeAvoidsRedundantDDL verifies that BuildMtree does not re-emit
// CREATE SCHEMA or CREATE OR REPLACE FUNCTION bytea_xor — these are owned
// by MtreeInit's Phase A, and redundant emission from build races with
// Spock DDL apply (SQLSTATE XX000 "tuple concurrently updated") on
// replicated clusters. We assert this catalog-side: pg_namespace.xmin and
// pg_proc.xmin for the offending objects must be unchanged across a
// BuildMtree call, since CREATE OR REPLACE FUNCTION unconditionally
// rewrites pg_proc (bumping xmin) and CREATE SCHEMA (without IF NOT EXISTS
// short-circuit miss) would do the same for pg_namespace. Regression guard
// for 2752ea5.
func TestBuildMtreeAvoidsRedundantDDL(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_mtree_idempotent"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	aceSchema := config.Cfg.MTree.Schema

	schemaXminBefore := scanString(t, ctx, pgCluster.Node1Pool,
		`SELECT xmin::text FROM pg_namespace WHERE nspname = $1`, aceSchema)
	funcXminBefore := scanString(t, ctx, pgCluster.Node1Pool,
		`SELECT p.xmin::text FROM pg_proc p
		 JOIN pg_namespace n ON n.oid = p.pronamespace
		 WHERE n.nspname = $1 AND p.proname = 'bytea_xor'`, aceSchema)
	require.NotEmpty(t, schemaXminBefore, "ace schema should exist after MtreeInit")
	require.NotEmpty(t, funcXminBefore, "bytea_xor should exist after MtreeInit")

	require.NoError(t, mtreeTask.BuildMtree())

	schemaXminAfter := scanString(t, ctx, pgCluster.Node1Pool,
		`SELECT xmin::text FROM pg_namespace WHERE nspname = $1`, aceSchema)
	funcXminAfter := scanString(t, ctx, pgCluster.Node1Pool,
		`SELECT p.xmin::text FROM pg_proc p
		 JOIN pg_namespace n ON n.oid = p.pronamespace
		 WHERE n.nspname = $1 AND p.proname = 'bytea_xor'`, aceSchema)

	require.Equal(t, schemaXminBefore, schemaXminAfter,
		"BuildMtree must not re-CREATE the ace schema")
	require.Equal(t, funcXminBefore, funcXminAfter,
		"BuildMtree must not CREATE OR REPLACE bytea_xor (races with Spock DDL apply)")
}

func scanString(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) string {
	t.Helper()
	var s string
	err := pool.QueryRow(ctx, query, args...).Scan(&s)
	require.NoError(t, err)
	return s
}

// TestUpdateMtreeReflectsInserts exercises the full UpdateMtree pipeline
// end-to-end: CDC drain picks up new WAL, counters mark affected leaves
// dirty, UpdateMtree recomputes those leaves and rolls the change up to
// the root. Closes a complete-zero-coverage gap on UpdateMtree
// (merkle.go:1560), which until now was only exercised as a black box by
// the CDC drain tests.
func TestUpdateMtreeReflectsInserts(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_mtree_update"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	mtreeTable := pgx.Identifier{
		config.Cfg.MTree.Schema,
		fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
	}.Sanitize()

	rootBefore := rootNodeHash(t, ctx, mtreeTable)
	require.NotEmpty(t, rootBefore, "BuildMtree should populate the root hash")
	require.Equal(t, 0, dirtyLeafCount(t, ctx, mtreeTable),
		"no leaves should be dirty immediately after BuildMtree")

	nodesBefore := mtreeNodeHashes(t, ctx, mtreeTable)
	require.Greater(t, len(nodesBefore), 2,
		"expected a multi-level tree (>2 nodes); a single-leaf tree means ANALYZE didn't take effect and parent-rollup isn't exercised")

	safeTbl := pgx.Identifier{testSchema, tableName}.Sanitize()
	var maxIndex int64
	require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx,
		"SELECT COALESCE(max(index), 0) FROM "+safeTbl). // nosemgrep
		Scan(&maxIndex))

	_, err := pgCluster.Node1Pool.Exec(ctx, // nosemgrep
		"INSERT INTO "+safeTbl+" (index, first_name, last_name, email) "+
			"SELECT $1 + g, 'Mtree', 'Update', 'mtree-update-' || g || '@test.com' "+
			"FROM generate_series(1, 25) g", maxIndex)
	require.NoError(t, err)

	require.NoError(t, mtreeTask.UpdateMtree(false))

	rootAfter := rootNodeHash(t, ctx, mtreeTable)
	require.NotEqual(t, rootBefore, rootAfter,
		"root node_hash must change after inserts are absorbed")
	require.Equal(t, 0, dirtyLeafCount(t, ctx, mtreeTable),
		"no leaves should remain dirty after UpdateMtree")
	require.Equal(t, int64(0), pendingInsertCounter(t, ctx, mtreeTable),
		"inserts_since_tree_update should reset to 0 after UpdateMtree")

	// Multi-level rollup proof: at least one descendant of root must also
	// have changed. A single-leaf tree would have only root changing.
	nodesAfter := mtreeNodeHashes(t, ctx, mtreeTable)
	require.GreaterOrEqual(t, countChangedNodes(nodesBefore, nodesAfter), 2,
		"expected change to propagate to root AND at least one ancestor node")
}

// mtreeTable arguments to these helpers must be sanitized via
// pgx.Identifier{...}.Sanitize() at the call site so the interpolation
// below is safe; the // nosemgrep markers tell the static analyzer that
// we've enforced the contract.
func rootNodeHash(t *testing.T, ctx context.Context, mtreeTable string) []byte {
	t.Helper()
	var h []byte
	err := pgCluster.Node1Pool.QueryRow(ctx, // nosemgrep
		"SELECT node_hash FROM "+mtreeTable+
			" WHERE node_level = (SELECT max(node_level) FROM "+mtreeTable+")"+
			" AND node_position = 0").Scan(&h)
	require.NoError(t, err)
	return h
}

func dirtyLeafCount(t *testing.T, ctx context.Context, mtreeTable string) int {
	t.Helper()
	var n int
	err := pgCluster.Node1Pool.QueryRow(ctx, // nosemgrep
		"SELECT count(*) FROM "+mtreeTable+" WHERE node_level = 0 AND dirty = true").Scan(&n)
	require.NoError(t, err)
	return n
}

func pendingInsertCounter(t *testing.T, ctx context.Context, mtreeTable string) int64 {
	t.Helper()
	var n int64
	err := pgCluster.Node1Pool.QueryRow(ctx, // nosemgrep
		"SELECT COALESCE(sum(inserts_since_tree_update), 0) FROM "+mtreeTable+
			" WHERE node_level = 0").Scan(&n)
	require.NoError(t, err)
	return n
}

func pendingDeleteCounter(t *testing.T, ctx context.Context, mtreeTable string) int64 {
	t.Helper()
	var n int64
	err := pgCluster.Node1Pool.QueryRow(ctx, // nosemgrep
		"SELECT COALESCE(sum(deletes_since_tree_update), 0) FROM "+mtreeTable+
			" WHERE node_level = 0").Scan(&n)
	require.NoError(t, err)
	return n
}

// mtreeNodeHashes snapshots every node in the mtree leaf table keyed by
// "level:position" with the hex-encoded node_hash. Snapshotting both sides
// of an UpdateMtree lets us verify the change propagated past root to at
// least one internal node — otherwise a degenerate single-leaf tree (e.g.
// missing ANALYZE) would let the root-hash-changed assertion pass without
// exercising parent rollup.
func mtreeNodeHashes(t *testing.T, ctx context.Context, mtreeTable string) map[string]string {
	t.Helper()
	rows, err := pgCluster.Node1Pool.Query(ctx, // nosemgrep
		"SELECT node_level, node_position, encode(node_hash, 'hex') FROM "+mtreeTable+
			" ORDER BY node_level, node_position")
	require.NoError(t, err)
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		var level int
		var pos int64
		var hash string
		require.NoError(t, rows.Scan(&level, &pos, &hash))
		out[fmt.Sprintf("%d:%d", level, pos)] = hash
	}
	require.NoError(t, rows.Err())
	return out
}

func countChangedNodes(before, after map[string]string) int {
	n := 0
	for k, beforeHash := range before {
		if afterHash, ok := after[k]; ok && afterHash != beforeHash {
			n++
		}
	}
	return n
}

// TestUpdateMtreeReflectsDeletes covers the deletion path through the
// UpdateMtree pipeline: deletes_since_tree_update counters increment via
// CDC, the affected leaves are recomputed, and the counters reset.
func TestUpdateMtreeReflectsDeletes(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_mtree_delete"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	mtreeTable := pgx.Identifier{
		config.Cfg.MTree.Schema,
		fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
	}.Sanitize()

	rootBefore := rootNodeHash(t, ctx, mtreeTable)
	require.NotEmpty(t, rootBefore)
	nodesBefore := mtreeNodeHashes(t, ctx, mtreeTable)
	require.Greater(t, len(nodesBefore), 2,
		"expected a multi-level tree (>2 nodes); ANALYZE may not have run")

	safeTbl := pgx.Identifier{testSchema, tableName}.Sanitize()
	res, err := pgCluster.Node1Pool.Exec(ctx, // nosemgrep
		"DELETE FROM "+safeTbl+" WHERE index BETWEEN 100 AND 124")
	require.NoError(t, err)
	require.Equal(t, int64(25), res.RowsAffected(), "expected to delete 25 customer rows")

	require.NoError(t, mtreeTask.UpdateMtree(false))

	rootAfter := rootNodeHash(t, ctx, mtreeTable)
	require.NotEqual(t, rootBefore, rootAfter,
		"root node_hash must change after deletes are absorbed")
	require.Equal(t, 0, dirtyLeafCount(t, ctx, mtreeTable),
		"no leaves should remain dirty after UpdateMtree")
	require.Equal(t, int64(0), pendingDeleteCounter(t, ctx, mtreeTable),
		"deletes_since_tree_update should reset to 0 after UpdateMtree")

	nodesAfter := mtreeNodeHashes(t, ctx, mtreeTable)
	require.GreaterOrEqual(t, countChangedNodes(nodesBefore, nodesAfter), 2,
		"expected change to propagate to root AND at least one ancestor node")
}

// TestACEConnSuppressesSpockDDLReplication verifies that every connection
// pool opened via the ACE auth layer applies the spock.enable_ddl_replication
// and spock.include_ddl_repset = off RuntimeParams. These suppress Spock's
// cross-node DDL apply and repset auto-add, which previously leaked the
// ace_cdc_metadata table into the default repset and races CREATE OR
// REPLACE FUNCTION across nodes. Regression guard for 113b0bc.
//
// We require Spock to actually be installed: without it, PostgreSQL accepts
// the dotted-name GUCs as placeholder customs and SHOW returns 'off'
// regardless, which would let this test silently pass on a misconfigured
// (Spock-less) cluster.
func TestACEConnSuppressesSpockDDLReplication(t *testing.T) {
	if !newSpockEnv().HasSpock {
		t.Skip("requires Spock-enabled cluster")
	}

	ctx := context.Background()

	pool, err := auth.GetClusterNodeConnection(ctx, pgCluster.ClusterNodes[0], auth.ConnectionOptions{})
	require.NoError(t, err)
	defer pool.Close()

	var spockInstalled bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname = 'spock')`).Scan(&spockInstalled))
	require.True(t, spockInstalled,
		"Spock extension must be installed; the suppression GUCs are no-ops without it")

	var ddlRepl, ddlRepset string
	require.NoError(t, pool.QueryRow(ctx, "SHOW spock.enable_ddl_replication").Scan(&ddlRepl))
	require.NoError(t, pool.QueryRow(ctx, "SHOW spock.include_ddl_repset").Scan(&ddlRepset))

	require.Equal(t, "off", ddlRepl,
		"ACE pool must boot with spock.enable_ddl_replication=off")
	require.Equal(t, "off", ddlRepset,
		"ACE pool must boot with spock.include_ddl_repset=off")
}

// TestMtreeInitReapsOrphanedSlot exercises the slot-leak recovery path
// described at merkle.go:893-901. If a prior MtreeInit died between
// Phase B (slot creation) and Phase C (metadata write), the slot is
// orphaned with no metadata row pointing at it. SetupReplicationSlot
// (cdc/setup.go:53) must drop the orphan before creating a fresh slot,
// otherwise re-init fails with "slot already exists" and the user is
// stuck. We simulate the orphan with pg_create_logical_replication_slot
// and assert the next MtreeInit succeeds.
func TestMtreeInitReapsOrphanedSlot(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_mtree_orphan"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	slotName := config.Cfg.MTree.CDC.SlotName

	_, err := pgCluster.Node1Pool.Exec(ctx,
		"SELECT pg_create_logical_replication_slot($1, 'pgoutput')", slotName)
	require.NoError(t, err, "could not seed an orphan replication slot")
	require.True(t,
		replicationSlotExists(t, ctx, pgCluster.Node1Pool, slotName),
		"orphan slot should be present before MtreeInit")

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	require.NoError(t, mtreeTask.MtreeInit(),
		"MtreeInit must reap the orphan slot rather than fail with slot-exists")

	require.True(t,
		replicationSlotExists(t, ctx, pgCluster.Node1Pool, slotName),
		"a fresh slot should exist after MtreeInit")

	var restartLSN string
	err = pgCluster.Node1Pool.QueryRow(ctx,
		"SELECT restart_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		slotName).Scan(&restartLSN)
	require.NoError(t, err)
	require.NotEmpty(t, restartLSN, "fresh slot should have a restart_lsn")
}

// TestBuildMtreeConcurrentSpockNodes reproduces the original production
// scenario for 2752ea5 + 113b0bc: BuildMtree fired concurrently from two
// nodes of a Spock cluster. Pre-fix, n1's CREATE OR REPLACE FUNCTION
// bytea_xor was caught by Spock's DDL replication, sent to n2, and
// raced n2's own identical DDL — producing SQLSTATE XX000 "tuple
// concurrently updated" on pg_proc. The fix removed the DDL from
// BuildMtree entirely and additionally set spock.enable_ddl_replication
// =off on all ACE connections. We assert that two concurrent
// single-node BuildMtree calls both succeed, and that
// ace_cdc_metadata was not auto-added to a Spock replication set —
// the latter being the second symptom of 113b0bc.
func TestBuildMtreeConcurrentSpockNodes(t *testing.T) {
	env := newSpockEnv()
	if !env.HasSpock {
		t.Skip("requires Spock-enabled cluster")
	}

	ctx := context.Background()
	tableName := "customers_mtree_concurrent"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	initTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	require.NoError(t, initTask.RunChecks(false))
	require.NoError(t, initTask.MtreeInit())

	t.Cleanup(func() {
		if err := initTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	taskN1 := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	taskN2 := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN2})
	require.NoError(t, taskN1.RunChecks(false))
	require.NoError(t, taskN2.RunChecks(false))

	var wg sync.WaitGroup
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = taskN1.BuildMtree()
	}()
	go func() {
		defer wg.Done()
		err2 = taskN2.BuildMtree()
	}()
	wg.Wait()

	require.NoError(t, err1, "BuildMtree on n1 must not race with concurrent build on n2")
	require.NoError(t, err2, "BuildMtree on n2 must not race with concurrent build on n1")

	// spock.tables lists every Spock-eligible table; set_name IS NULL means
	// the table is NOT in any replication set. User tables (customers,
	// customers_1M) appear here with set_name NULL on this test cluster
	// because spock.enable_ddl_replication defaults to off — so a plain
	// EXISTS check would false-positive against any table at all.
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		var inRepset bool
		err := pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM spock.tables
				WHERE nspname = $1 AND relname = 'ace_cdc_metadata'
				  AND set_name IS NOT NULL
			)`, config.Cfg.MTree.Schema).Scan(&inRepset)
		require.NoError(t, err, "node%d: query spock.tables", i+1)
		require.False(t, inRepset,
			"node%d: ace_cdc_metadata must not be auto-added to a Spock repset", i+1)
	}
}

// TestUpdateMtreeReflectsUpdates covers the UPDATE path: a non-PK column
// change must still flip the row hash, mark the containing leaf dirty,
// and propagate the new leaf hash up to the root. The schema has no
// updates_since_tree_update counter (only insert/delete), so we only
// assert the externally observable invariants: root hash changes, no
// leaves remain dirty.
func TestUpdateMtreeReflectsUpdates(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_mtree_upd"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	mtreeTable := pgx.Identifier{
		config.Cfg.MTree.Schema,
		fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
	}.Sanitize()

	rootBefore := rootNodeHash(t, ctx, mtreeTable)
	require.NotEmpty(t, rootBefore)
	nodesBefore := mtreeNodeHashes(t, ctx, mtreeTable)
	require.Greater(t, len(nodesBefore), 2,
		"expected a multi-level tree (>2 nodes); ANALYZE may not have run")

	safeTbl := pgx.Identifier{testSchema, tableName}.Sanitize()
	res, err := pgCluster.Node1Pool.Exec(ctx, // nosemgrep
		"UPDATE "+safeTbl+" SET email = email || '.updated' WHERE index BETWEEN 200 AND 224")
	require.NoError(t, err)
	require.Equal(t, int64(25), res.RowsAffected(), "expected to update 25 customer rows")

	require.NoError(t, mtreeTask.UpdateMtree(false))

	rootAfter := rootNodeHash(t, ctx, mtreeTable)
	require.NotEqual(t, rootBefore, rootAfter,
		"root node_hash must change after non-PK column updates")
	require.Equal(t, 0, dirtyLeafCount(t, ctx, mtreeTable),
		"no leaves should remain dirty after UpdateMtree")

	nodesAfter := mtreeNodeHashes(t, ctx, mtreeTable)
	require.GreaterOrEqual(t, countChangedNodes(nodesBefore, nodesAfter), 2,
		"expected change to propagate to root AND at least one ancestor node")
}

// TestMerkleTreeBidirectionalDiff is a multi-scenario reproducer for the
// asymmetric-diff bug: mtree table-diff reports rows present on n1 but
// missing on n2 while silently dropping rows present on n2 but missing on n1
// (and vice-versa once the reference role flips).
//
// Each scenario varies where the divergence sits relative to the reference
// node's pkey envelope. The reference node — the one with the most rows; on a
// tie BuildMtree's strict `count > maxRows` lets n1 win — defines the block
// ranges via GeneratePkeyOffsetsQuery, whose final range always has
// range_end IS NULL. getPkeyBatches collects only non-NULL boundaries when
// slicing, so the open-ended tail contributes no slice and any row on the
// non-reference node beyond the reference's last_row is never queried by
// processWorkItem.
//
// The scenarios are designed to falsify, not just fail:
//
//   - ExactReproducer mirrors the bug report verbatim (n1=[1], n2=[2]).
//   - N2HasRowsAboveN1Max keeps n1 as reference with extras stacked above
//     its max; the open-ended-tail theory predicts n2's extras vanish.
//   - N1HasRowAboveN2Max flips the asymmetry so n2 becomes the reference;
//     the same bug should surface from that side — if it doesn't, the
//     diagnosis is incomplete.
func TestMerkleTreeBidirectionalDiff(t *testing.T) {
	cases := []struct {
		name         string
		seedN1       []int
		seedN2       []int
		expectN1Only []int
		expectN2Only []int
	}{
		{
			name:         "ExactReproducer",
			seedN1:       []int{1},
			seedN2:       []int{2},
			expectN1Only: []int{1},
			expectN2Only: []int{2},
		},
		{
			name:         "N2HasRowsAboveN1Max",
			seedN1:       []int{1, 2, 3, 4, 5},
			seedN2:       []int{1, 1000, 2000, 3000, 4000},
			expectN1Only: []int{2, 3, 4, 5},
			expectN2Only: []int{1000, 2000, 3000, 4000},
		},
		{
			name:         "N1HasRowAboveN2Max",
			seedN1:       []int{1, 10000},
			seedN2:       []int{1, 2, 3, 4, 5, 6},
			expectN1Only: []int{10000},
			expectN2Only: []int{2, 3, 4, 5, 6},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			env := newSpockEnv()
			tableName := "mtree_bidir_" + strings.ToLower(tc.name)
			qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
			safeTable := pgx.Identifier{testSchema, tableName}.Sanitize()

			// Use IF NOT EXISTS because Spock DDL replication may
			// propagate the first node's CREATE to the second; the second
			// explicit CREATE would otherwise fail with relation exists.
			//
			// Deliberately do NOT add the table to a Spock replication set:
			// with the table outside any repset, Spock has no rule to
			// replicate INSERTs even though the rows go to WAL, so each
			// node holds exactly what we seed and the reproducer can
			// actually diverge. spock.repair_mode alone does not suffice
			// here — it suppresses apply for tables that are already
			// replicating, not for tables freshly added to the repset.
			for _, pool := range env.pools() {
				_, err := pool.Exec(ctx,
					"CREATE TABLE IF NOT EXISTS "+safeTable+" (id INT PRIMARY KEY, payload TEXT)") // nosemgrep
				require.NoError(t, err, "create %s", qualifiedTable)
			}
			t.Cleanup(func() {
				for _, pool := range env.pools() {
					_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safeTable+" CASCADE") // nosemgrep
				}
				files, _ := filepath.Glob("*_diffs-*.json")
				for _, f := range files {
					os.Remove(f)
				}
			})

			// TRUNCATE inside repair_mode in case a DDL-replicated CREATE
			// from a sibling subtest left rows behind on either node.
			for _, pool := range env.pools() {
				env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
					_, err := conn.Exec(ctx, "TRUNCATE TABLE "+safeTable) // nosemgrep
					require.NoError(t, err, "truncate %s", qualifiedTable)
				})
			}

			seed := func(pool *pgxpool.Pool, ids []int) {
				for _, id := range ids {
					_, err := pool.Exec(ctx,
						"INSERT INTO "+safeTable+" (id, payload) VALUES ($1, $2)", // nosemgrep
						id, fmt.Sprintf("row-%d", id))
					require.NoError(t, err, "seed id=%d", id)
				}
			}
			seed(env.N1Pool, tc.seedN1)
			seed(env.N2Pool, tc.seedN2)

			// ANALYZE so GetRowCountEstimate reflects the seeded counts;
			// otherwise BuildMtree falls back to pg_class.reltuples == 0
			// and the reference-node tiebreak becomes order-dependent
			// rather than count-driven.
			for _, pool := range env.pools() {
				_, err := pool.Exec(ctx, "ANALYZE "+safeTable) // nosemgrep
				require.NoError(t, err)
			}

			// Diagnostic: dump actual rows per node so a future Spock-
			// replication regression doesn't masquerade as an mtree bug.
			for nodeName, pool := range map[string]*pgxpool.Pool{
				env.ServiceN1: env.N1Pool, env.ServiceN2: env.N2Pool,
			} {
				rows, err := pool.Query(ctx, "SELECT id FROM "+safeTable+" ORDER BY id") // nosemgrep
				require.NoError(t, err)
				var got []int
				for rows.Next() {
					var id int
					require.NoError(t, rows.Scan(&id))
					got = append(got, id)
				}
				rows.Close()
				t.Logf("post-seed state on %s: ids=%v", nodeName, got)
			}

			mtreeTask := env.newMerkleTreeTask(t, qualifiedTable,
				[]string{env.ServiceN1, env.ServiceN2})
			require.NoError(t, mtreeTask.RunChecks(false))
			require.NoError(t, mtreeTask.MtreeInit())
			t.Cleanup(func() {
				if err := mtreeTask.MtreeTeardown(); err != nil {
					t.Logf("MtreeTeardown cleanup: %v", err)
				}
			})
			require.NoError(t, mtreeTask.BuildMtree())
			require.NoError(t, mtreeTask.DiffMtree())

			pair := env.pairKey()
			nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[pair]
			require.True(t, ok, "diff missing pair %s; full result: %+v",
				pair, mtreeTask.DiffResult)

			gotN1 := extractDiffIDs(nodeDiffs.Rows[env.ServiceN1])
			gotN2 := extractDiffIDs(nodeDiffs.Rows[env.ServiceN2])

			sort.Ints(tc.expectN1Only)
			sort.Ints(tc.expectN2Only)

			require.Equal(t, tc.expectN1Only, gotN1,
				"rows present only on n1 — diff under-reports n1 side")
			require.Equal(t, tc.expectN2Only, gotN2,
				"rows present only on n2 — open-ended-tail bug if empty")
		})
	}
}

func extractDiffIDs(rows []types.OrderedMap) []int {
	ids := make([]int, 0, len(rows))
	for _, row := range rows {
		v, ok := row.Get("id")
		if !ok {
			continue
		}
		switch x := v.(type) {
		case int32:
			ids = append(ids, int(x))
		case int64:
			ids = append(ids, int(x))
		case int:
			ids = append(ids, x)
		}
	}
	sort.Ints(ids)
	return ids
}

// TestMerkleTreeReferenceMaxInMultiLeafTail reproduces the residual ACE-189
// symptom Zaid reported: the reference node's single largest row — the
// range_start of the open-ended tail leaf in a MULTI-LEAF tree — is dropped
// from the diff even after the open-ended-tail fix (f9a3961). The earlier
// bidirectional tests use tiny row counts (single-leaf trees), so they never
// exercise the boundary where the last closed leaf [B, max] meets the open
// tail [max, NULL).
//
// Zaid saw it on a 3-node cluster (n3 held the cluster max; every pair
// involving n3 was short by one, n1/n2 was correct). The 3-node shape is
// incidental — the bug is "the reference node's own max is dropped" — so this
// reproduces it minimally on the 2-node test cluster, with the reference (n2,
// more rows) holding the cluster max. Row counts are above the cluster's
// min block size (1000) so the default block size still yields a multi-leaf
// tree, which is what exercises the last-closed-leaf/open-tail boundary.
//
//   n1 = 1..2000, n2 = 1..2010  → n2 is the reference (3 leaves).
// Expected n1/n2 diff = {2001..2010} (10 rows). The bug drops id=2010 (n2's
// max), yielding 9 — the same off-by-the-last-row Zaid observed.
func TestMerkleTreeReferenceMaxInMultiLeafTail(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "mtree_ref_max_tail"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
	safeTable := pgx.Identifier{testSchema, tableName}.Sanitize()

	seedCount := map[*pgxpool.Pool]int{
		env.N1Pool: 2000,
		env.N2Pool: 2010, // n2 holds the cluster max → reference node
	}

	// No repset: each node must keep exactly what we seed (see the
	// bidirectional test for why repair_mode alone is insufficient).
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx,
			"CREATE TABLE IF NOT EXISTS "+safeTable+" (id INT PRIMARY KEY, name VARCHAR)") // nosemgrep
		require.NoError(t, err, "create %s", qualifiedTable)
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safeTable+" CASCADE") // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})

	for _, pool := range env.pools() {
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+safeTable) // nosemgrep
			require.NoError(t, err, "truncate %s", qualifiedTable)
		})
		_, err := pool.Exec(ctx,
			"INSERT INTO "+safeTable+" (id, name) SELECT g, 'user_' || g FROM generate_series(1, $1) g", // nosemgrep
			seedCount[pool])
		require.NoError(t, err, "seed table")
		_, err = pool.Exec(ctx, "ANALYZE "+safeTable) // nosemgrep
		require.NoError(t, err)
	}

	mtreeTask := env.newMerkleTreeTask(t, qualifiedTable,
		[]string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, mtreeTask.BuildMtree())
	require.NoError(t, mtreeTask.DiffMtree())

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
	require.True(t, ok, "no diff for pair %s; result: %+v",
		env.pairKey(), mtreeTask.DiffResult.NodeDiffs)

	expected := make([]int, 0, 10)
	for i := 2001; i <= 2010; i++ {
		expected = append(expected, i)
	}
	require.Equal(t, expected, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]),
		"reference's max (id=2010) dropped from the open tail of a multi-leaf tree — ACE-189")
}

// writeClusterConfigJSON emits a <clusterName>.json cluster config in the
// working directory listing the given nodes, so a task pointed at clusterName
// resolves all of them via utils.ReadClusterInfo. The shared test_cluster.json
// only registers n1/n2; this lets a single test opt into a 3-node cluster
// without changing that file (which would alter Nodes="all" suite-wide).
func writeClusterConfigJSON(t *testing.T, clusterName string, nodes []types.NodeGroup) {
	t.Helper()
	cfg := types.ClusterConfig{
		JSONVersion: "1.0",
		ClusterName: clusterName,
		LogLevel:    "info",
		UpdateDate:  time.Now().Format(time.RFC3339),
	}
	cfg.PGEdge.PGVersion = 16
	cfg.PGEdge.AutoStart = "yes"
	cfg.PGEdge.Spock = types.SpockConfig{SpockVersion: "4.0.10", AutoDDL: "yes"}
	cfg.PGEdge.Databases = []types.Database{{DBName: dbName, DBUser: pgEdgeUser, DBPassword: pgEdgePassword}}
	cfg.NodeGroups = nodes

	data, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	path := clusterName + ".json"
	require.NoError(t, os.WriteFile(path, data, 0644))
	t.Cleanup(func() { os.Remove(path) })
}

// writeThreeNodeClusterConfig emits a 3-node cluster config (n1/n2/n3) for the
// isolated 3-node tests and returns the cluster name to point a task at. Keeps
// the types.NodeGroup construction in this file so other test files in the
// package can opt into a 3-node cluster without importing the config types.
func writeThreeNodeClusterConfig(t *testing.T, env *testEnv) string {
	t.Helper()
	clusterName := "test_cluster_3node"
	mkNode := func(name, host, port string) types.NodeGroup {
		ng := types.NodeGroup{Name: name, IsActive: "yes", PublicIP: host, Port: port, Path: "/usr/local/bin"}
		ng.SSH.OSUser = "pgedge"
		return ng
	}
	writeClusterConfigJSON(t, clusterName, []types.NodeGroup{
		mkNode(env.ServiceN1, pgCluster.Node1Host, pgCluster.Node1Port),
		mkNode(env.ServiceN2, pgCluster.Node2Host, pgCluster.Node2Port),
		mkNode(env.ServiceN3, pgCluster.Node3Host, pgCluster.Node3Port),
	})
	return clusterName
}

// TestMerkleTreeThreeNodeReferenceTail is the faithful 3-node reproduction of
// Zaid's ACE-189 report: n3 holds the cluster max, and every diff pair that
// involves n3 was short by exactly one row (its largest), while n1/n2 was
// correct. The shared test cluster only registers n1/n2, so this test emits
// its own 3-node cluster config and points the task at it.
//
//	n1 = 1..2000, n2 = 1..2005, n3 = 1..2010  → n3 is the reference.
//
// Counts are above the cluster min block size (1000) so the tree is multi-leaf
// (the single-leaf case is already covered and behaves differently at the
// tail). Expected: n1/n3 = {2001..2010} (10), n2/n3 = {2006..2010} (5),
// n1/n2 = {2001..2005} (5) — Zaid's 10/5/5 ratio. The bug drops id=2010 from
// both n3 pairs (9 and 4).
func TestMerkleTreeThreeNodeReferenceTail(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	if env.N3Pool == nil {
		t.Skip("requires a 3-node cluster")
	}

	clusterName := "test_cluster_3node"
	mkNode := func(name, host, port string) types.NodeGroup {
		ng := types.NodeGroup{Name: name, IsActive: "yes", PublicIP: host, Port: port, Path: "/usr/local/bin"}
		ng.SSH.OSUser = "pgedge"
		return ng
	}
	writeClusterConfigJSON(t, clusterName, []types.NodeGroup{
		mkNode(env.ServiceN1, pgCluster.Node1Host, pgCluster.Node1Port),
		mkNode(env.ServiceN2, pgCluster.Node2Host, pgCluster.Node2Port),
		mkNode(env.ServiceN3, pgCluster.Node3Host, pgCluster.Node3Port),
	})

	tableName := "mtree_three_node_tail"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
	safeTable := pgx.Identifier{testSchema, tableName}.Sanitize()

	pools := map[string]*pgxpool.Pool{
		env.ServiceN1: env.N1Pool,
		env.ServiceN2: env.N2Pool,
		env.ServiceN3: env.N3Pool,
	}
	seedCount := map[string]int{
		env.ServiceN1: 2000,
		env.ServiceN2: 2005,
		env.ServiceN3: 2010, // cluster max → reference node
	}

	// No repset: each node must keep exactly what we seed.
	for _, pool := range pools {
		_, err := pool.Exec(ctx,
			"CREATE TABLE IF NOT EXISTS "+safeTable+" (id INT PRIMARY KEY, name VARCHAR)") // nosemgrep
		require.NoError(t, err, "create %s", qualifiedTable)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safeTable+" CASCADE") // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})

	for name, pool := range pools {
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+safeTable) // nosemgrep
			require.NoError(t, err, "truncate %s on %s", qualifiedTable, name)
		})
		_, err := pool.Exec(ctx,
			"INSERT INTO "+safeTable+" (id, name) SELECT g, 'user_' || g FROM generate_series(1, $1) g", // nosemgrep
			seedCount[name])
		require.NoError(t, err, "seed %s", name)
		_, err = pool.Exec(ctx, "ANALYZE "+safeTable) // nosemgrep
		require.NoError(t, err)
	}

	mtreeTask := env.newMerkleTreeTask(t, qualifiedTable,
		[]string{env.ServiceN1, env.ServiceN2, env.ServiceN3})
	mtreeTask.ClusterName = clusterName
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, mtreeTask.BuildMtree())
	require.NoError(t, mtreeTask.DiffMtree())

	// Pair keys follow cluster-node order; look up under either ordering.
	findPair := func(a, b string) (types.DiffByNodePair, bool) {
		if d, ok := mtreeTask.DiffResult.NodeDiffs[a+"/"+b]; ok {
			return d, true
		}
		d, ok := mtreeTask.DiffResult.NodeDiffs[b+"/"+a]
		return d, ok
	}
	rangeIDs := func(lo, hi int) []int {
		ids := make([]int, 0, hi-lo+1)
		for i := lo; i <= hi; i++ {
			ids = append(ids, i)
		}
		return ids
	}

	d13, ok := findPair(env.ServiceN1, env.ServiceN3)
	require.True(t, ok, "no diff for n1/n3; result: %+v", mtreeTask.DiffResult.NodeDiffs)
	require.Equal(t, rangeIDs(2001, 2010), extractDiffIDs(d13.Rows[env.ServiceN3]),
		"n1/n3: reference's max (2010) dropped from the open tail — ACE-189")

	d23, ok := findPair(env.ServiceN2, env.ServiceN3)
	require.True(t, ok, "no diff for n2/n3; result: %+v", mtreeTask.DiffResult.NodeDiffs)
	require.Equal(t, rangeIDs(2006, 2010), extractDiffIDs(d23.Rows[env.ServiceN3]),
		"n2/n3: reference's max (2010) dropped from the open tail — ACE-189")

	d12, ok := findPair(env.ServiceN1, env.ServiceN2)
	require.True(t, ok, "no diff for n1/n2; result: %+v", mtreeTask.DiffResult.NodeDiffs)
	require.Equal(t, rangeIDs(2001, 2005), extractDiffIDs(d12.Rows[env.ServiceN2]),
		"n1/n2: rows below the reference's max should be reported in full")
}
