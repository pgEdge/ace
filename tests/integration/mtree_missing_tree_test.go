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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/stretchr/testify/require"
)

// mtree table-diff on a table whose tree was never built must fail
// fast with an actionable message pointing at 'ace mtree build', instead of
// draining the replication stream and then surfacing a raw metadata error.
func TestMtreeDiffFailsFastWhenTreeNotBuilt(t *testing.T) {
	ctx := context.Background()
	tableName := "mtree_missing_tree_test"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
	safeTable := pgx.Identifier{testSchema, tableName}.Sanitize()
	nodes := []string{serviceN1, serviceN2}

	// Create the table on both nodes but deliberately skip mtree init/build.
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safeTable+" (id INT PRIMARY KEY, payload TEXT)") // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safeTable) // nosemgrep
		}
	})

	task := newTestMerkleTreeTask(t, qualifiedTable, nodes)
	task.Mode = "diff"
	task.Output = "json"
	require.NoError(t, task.RunChecks(false))

	err := task.DiffMtree()
	require.Error(t, err, "diff on an unbuilt tree must fail")
	require.ErrorIs(t, err, mtree.ErrMtreeNotFound)
	require.Contains(t, err.Error(), "no merkle tree found for "+qualifiedTable)
	require.Contains(t, err.Error(), "ace mtree build")
	require.NotContains(t, err.Error(), "no rows in result set",
		"raw driver error must not leak to the user")
	require.NotContains(t, err.Error(), "failed to update merkle tree before diff",
		"missing-tree error must not be buried under the update wrapper")
}
