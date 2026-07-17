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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Regression: same PK exists on both nodes with different non-key data at BUILD
// time (an UPDATE-UPDATE conflict). mtree diff must report it, matching plain
// table-diff.
//
// The divergence exists before the tree is built -- unlike
// testMerkleTreeDiffModifiedRows, which builds on identical data and mutates
// afterward, so it never exercised the degenerate build-time range layout.
//
// The bug: a block's range_end is the EXCLUSIVE start of the next block, but
// leaf hashing used a closed "<=" upper bound, double-counting boundary rows
// into two adjacent leaves. A single-row table collapses into two identical
// overlapping leaves ([pk,pk] and [pk,inf)); the XOR-based parent hash cancels
// the duplicate siblings to zero on every node, so divergent data yielded
// matching root hashes and the diff reported "trees identical". The single-row
// case below reproduced it; the multi-row case guards the boundary.
func TestMtreeDiff_UpdateUpdateConflictAtBuildTime(t *testing.T) {
	t.Run("SingleRow", func(t *testing.T) {
		runMtreeUpdateConflictCase(t, "mtree_uu_conflict_1", []int{1}, 1)
	})
	t.Run("MultiRowBoundary", func(t *testing.T) {
		// The divergent row (id=3) is the middle block boundary under BlockSize=2.
		runMtreeUpdateConflictCase(t, "mtree_uu_conflict_n", []int{1, 2, 3, 4, 5}, 3)
	})
}

// runMtreeUpdateConflictCase seeds ids on both nodes, diverging divergentID's
// non-key column, then asserts both table-diff and mtree diff report exactly
// one divergent row.
func runMtreeUpdateConflictCase(t *testing.T, tableName string, ids []int, divergentID int) {
	t.Helper()
	ctx := context.Background()
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
			"CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, name VARCHAR)", qualifiedTable))
		require.NoError(t, err, "create table on %s", nodeName)
		_, err = pool.Exec(ctx, fmt.Sprintf( // nosemgrep
			"SELECT spock.repset_add_table('default', '%s')", qualifiedTable))
		require.NoError(t, err, "add to repset on %s", nodeName)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifiedTable)) // nosemgrep
		}
	})

	// Seed identical rows on both nodes, then diverge divergentID's non-key
	// column. repair_mode(true) keeps these writes from replicating.
	seed := func(pool *pgxpool.Pool, divergentName string) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer tx.Rollback(ctx) // safe to call even after Commit() in pgx
		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err)
		for _, id := range ids {
			name := fmt.Sprintf("name-%d", id)
			if id == divergentID {
				name = divergentName
			}
			_, err = tx.Exec(ctx, fmt.Sprintf( // nosemgrep
				"INSERT INTO %s (id, name) VALUES ($1, $2)", qualifiedTable), id, name)
			require.NoError(t, err)
		}
		_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
		require.NoError(t, err)
		require.NoError(t, tx.Commit(ctx))
	}
	seed(pgCluster.Node1Pool, "zaid")
	seed(pgCluster.Node2Pool, "shabbir")

	// Baseline: plain table-diff must find the divergence.
	tdTask := newTestTableDiffTask(t, qualifiedTable, nodes)
	require.NoError(t, tdTask.RunChecks(false))
	require.NoError(t, tdTask.ExecuteTask())
	require.Equal(t, 1, sumDiffRows(tdTask.DiffResult.Summary.DiffRowsCount),
		"table-diff must find the id=%d conflict", divergentID)

	// mtree diff on the same build-time divergence must ALSO find it.
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTable, nodes)
	mtreeTask.BlockSize = 2
	mtreeTask.OverrideBlockSize = true
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() { _ = mtreeTask.MtreeTeardown() })
	require.NoError(t, mtreeTask.BuildMtree())

	diffTask := newTestMerkleTreeTask(t, qualifiedTable, nodes)
	diffTask.Mode = "diff"
	diffTask.Output = "json"
	diffTask.BlockSize = 2
	diffTask.OverrideBlockSize = true
	require.NoError(t, diffTask.RunChecks(false))
	require.NoError(t, diffTask.DiffMtree())
	require.Equal(t, 1, sumDiffRows(diffTask.DiffResult.Summary.DiffRowsCount),
		"mtree diff must ALSO find the id=%d conflict", divergentID)
}

func sumDiffRows(counts map[string]int) int {
	total := 0
	for _, c := range counts {
		total += c
	}
	return total
}
