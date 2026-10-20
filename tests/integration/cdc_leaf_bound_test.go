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
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestCDCLeafBoundBelongsToOneLeaf checks that a key equal to the
// range_end of a leaf belongs to the next leaf only. The range_end of a leaf
// is the range_start of the next one, and the leaf hash does not include it.
// The row counts and the CDC counters must follow the same rule.
//
// Before the fix, a change to such a key was counted in both leaves. Other
// tests found it only by chance, when a random block bound fell on a changed
// key. This test changes the bound key on purpose. Its name starts with
// TestCDC, so the CDC step of the CI workflow runs it.
func TestCDCLeafBoundBelongsToOneLeaf(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_leaf_bound"
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

	pool := pgCluster.Node1Pool
	mtreeLeaf := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)}.Sanitize()
	tableIdent := pgx.Identifier{testSchema, tableName}.Sanitize()

	var leaves int64
	err := pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE node_level = 0", mtreeLeaf)).Scan(&leaves) // nosemgrep
	require.NoError(t, err)
	require.GreaterOrEqual(t, leaves, int64(2), "the test needs a bound between two leaves")

	// Each row is counted in one leaf, so the leaf row counts add up to the
	// number of rows in the table.
	var tableRows int64
	err = pool.QueryRow(ctx, "SELECT count(*) FROM "+tableIdent).Scan(&tableRows) // nosemgrep
	require.NoError(t, err)
	var leafRows int64
	for pos := int64(0); pos < leaves; pos++ {
		block, err := queries.GetBlockWithCount(ctx, pool, mtreeLeaf, testSchema, tableName, mtreeTask.Key, false, pos)
		require.NoError(t, err)
		require.NotNil(t, block, "leaf %d not found", pos)
		leafRows += block.Count
	}
	require.Equal(t, tableRows, leafRows, "leaf row counts should add up to the table row count")

	// Delete the row whose key is the bound between leaf 0 and leaf 1.
	var boundKey int64
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT range_end::bigint FROM %s WHERE node_level = 0 AND node_position = 0", mtreeLeaf)).Scan(&boundKey) // nosemgrep
	require.NoError(t, err)
	tag, err := pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE index = $1", tableIdent), boundKey) // nosemgrep
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected(), "the bound key %d should be a row of the table", boundKey)

	require.NoError(t, cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0]))

	// Only leaf 1 owns the bound key, so only leaf 1 is dirty and counts the
	// delete.
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT node_position, deletes_since_tree_update
		FROM %s
		WHERE node_level = 0 AND (dirty OR deletes_since_tree_update > 0)
		ORDER BY node_position`, mtreeLeaf)) // nosemgrep
	require.NoError(t, err)
	changed := map[int64]int64{}
	for rows.Next() {
		var pos, deletes int64
		require.NoError(t, rows.Scan(&pos, &deletes))
		changed[pos] = deletes
	}
	require.NoError(t, rows.Err())
	require.Equal(t, map[int64]int64{1: 1}, changed,
		"a delete of the bound key %d should go to leaf 1 only", boundKey)
}
