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
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestMerkleTreeSplitSliverOfBoundedLeaf checks that a split of a leaf with
// an upper bound does not count the row at that bound. The row is the first
// row of the next leaf.
//
// With 1.25 blocks minus one row in the first leaf, the split point is at
// row 1001, and the rest after it is 0.25 blocks minus one row: a sliver that
// is too small to be a leaf of its own, so the leaf must stay as it is. If
// the count also takes the row at the upper bound, the rest is exactly 0.25
// blocks and the leaf is cut in two. With 1.25 blocks the rest is 0.25 blocks
// and the leaf must be cut: this shows that the leaf is a candidate for a
// split at all.
func TestMerkleTreeSplitSliverOfBoundedLeaf(t *testing.T) {
	t.Run("one row short of a leaf", func(t *testing.T) {
		testSplitFirstLeaf(t, "mtree_split_sliver_short", -1, 0)
	})
	t.Run("exactly a quarter block more", func(t *testing.T) {
		testSplitFirstLeaf(t, "mtree_split_sliver_quarter", 0, 1)
	})
}

// testSplitFirstLeaf gives the first leaf 1.25 blocks plus extra rows, runs
// mtree update and checks that it adds newLeaves leaves.
func testSplitFirstLeaf(t *testing.T, tableName string, extra, newLeaves int) {
	ctx := context.Background()
	env := newSpockEnv()
	qualified, safe := newUnreplicatedTable(t, env, tableName, "(id INT PRIMARY KEY, payload TEXT)")

	// Keys 10, 20, ... leave free keys between them for the inserts below.
	_, err := env.N1Pool.Exec(ctx, "INSERT INTO "+safe+ // nosemgrep
		" SELECT g * 10, 'row-' || g FROM generate_series(1, 3000) g")
	require.NoError(t, err)
	analyzeTable(t, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	mtreeTask.Nodes = env.ServiceN1
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.BuildMtree())

	mtreeTable := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)}.Sanitize()
	leaves := func() int {
		var n int
		require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT count(*) FROM "+mtreeTable+ // nosemgrep
			" WHERE node_level = 0").Scan(&n))
		return n
	}
	leavesBefore := leaves()
	require.GreaterOrEqual(t, leavesBefore, 2, "the test needs a first leaf with an upper bound")

	var end int
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT range_end FROM "+mtreeTable+ // nosemgrep
		" WHERE node_level = 0 AND node_position = 0").Scan(&end))
	var exists bool
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM "+safe+ // nosemgrep
		" WHERE id = $1)", end).Scan(&exists))
	require.True(t, exists, "the upper bound of the first leaf must be a row of the table")

	// The split looks only at leaves with at least half a block of inserts.
	// Delete rows first, so that the leaf gets that many inserts and still
	// ends with the wanted number of rows. The new keys are above the
	// smallest key, so they are inside the stored range of the first leaf.
	blockSize := mtreeTask.BlockSize
	want := blockSize + blockSize/4 + extra
	var n int
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT count(*) FROM "+safe+ // nosemgrep
		" WHERE id < $1", end).Scan(&n))
	keep := want - blockSize/2 - 50
	require.Less(t, keep, n)
	_, err = env.N1Pool.Exec(ctx, "DELETE FROM "+safe+" WHERE id IN (SELECT id FROM "+safe+ // nosemgrep
		" WHERE id < $1 ORDER BY id DESC LIMIT $2)", end, n-keep)
	require.NoError(t, err)
	_, err = env.N1Pool.Exec(ctx, "INSERT INTO "+safe+" SELECT g, 'new-' || g FROM generate_series(11, $1 - 1) g"+ // nosemgrep
		" WHERE g % 10 <> 0 ORDER BY g LIMIT $2", end, want-keep)
	require.NoError(t, err)
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT count(*) FROM "+safe+ // nosemgrep
		" WHERE id < $1", end).Scan(&n))
	require.Equal(t, want, n, "rows in the first leaf")

	mtreeTask.Rebalance = false
	require.NoError(t, mtreeTask.UpdateMtree(false))

	require.Equal(t, leavesBefore+newLeaves, leaves(), "leaves after the update")
}
