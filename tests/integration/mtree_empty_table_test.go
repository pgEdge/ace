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
	"github.com/stretchr/testify/require"
)

// mtree build on a table that is empty on every node must fail with a clear,
// actionable "0 rows on all nodes" message -- not the internal "could not
// determine a reference node" error that leaves the user unsure whether the
// table is missing, the nodes are unreachable, or something is broken.
func TestBuildMtreeFailsClearlyOnEmptyTable(t *testing.T) {
	ctx := context.Background()
	tableName := "mtree_empty_table_test"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, tableName)
	safeTable := pgx.Identifier{testSchema, tableName}.Sanitize()
	nodes := []string{serviceN1, serviceN2}

	// Create the table on both nodes but insert no rows.
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
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { _ = task.MtreeTeardown() })

	err := task.BuildMtree()
	require.Error(t, err, "build on an empty table must fail")
	require.Contains(t, err.Error(), "has 0 rows on all nodes",
		"error must name the real cause: the table is empty")
	require.Contains(t, err.Error(), qualifiedTable,
		"error must name the offending table")
	require.NotContains(t, err.Error(), "could not determine a reference node",
		"the internal reference-node error must no longer leak for empty tables")
}
