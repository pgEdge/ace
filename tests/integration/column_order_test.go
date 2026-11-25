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
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestColumnsMatchedByName: the nodes have the same columns in another
// order, as after pg_dump of a table that inherits columns. table-diff,
// mtree and table-repair match the columns by name: equal rows are equal,
// a changed row is found and repaired, and the report lists the columns
// in the same order for both nodes.
func TestColumnsMatchedByName(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	tableName := "ace_column_order"
	table := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	defs := map[*pgxpool.Pool]string{
		env.N1Pool: "id int PRIMARY KEY, a text, b int",
		env.N2Pool: "b int, a text, id int PRIMARY KEY",
	}
	for pool, def := range defs {
		_, err := pool.Exec(ctx, "CREATE TABLE "+safe+" ("+def+")") // nosemgrep
		require.NoError(t, err)
		if env.HasSpock {
			_, err = pool.Exec(ctx, "SELECT spock.repset_add_table('default', $1)", table)
			require.NoError(t, err)
		}
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			if env.HasSpock {
				repsetRemoveTable(t, context.Background(), pool, "default", table)
			}
			execOK(t, context.Background(), pool, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
	})

	for _, pool := range env.pools() {
		cols, _, err := queries.GetColumns(ctx, pool, testSchema, tableName)
		require.NoError(t, err)
		require.Equal(t, []string{"a", "b", "id"}, cols, "the columns are sorted by name on every node")

		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		env.withRepairModeTx(t, ctx, tx, func() {
			_, err := tx.Exec(ctx, "INSERT INTO "+safe+" (id, a, b) SELECT g, 'v' || g, g * 10 FROM generate_series(1, 300) g") // nosemgrep
			require.NoError(t, err)
		})
		require.NoError(t, tx.Commit(ctx))
	}
	env.assertNoTableDiff(t, table)

	mt := env.newMerkleTreeTask(t, table, []string{env.ServiceN1, env.ServiceN2})
	mt.BlockSize = 100
	mt.OverrideBlockSize = true
	require.NoError(t, mt.RunChecks(false))
	require.NoError(t, mt.MtreeInit())
	t.Cleanup(func() { assert.NoError(t, mt.MtreeTeardown()) })
	require.NoError(t, mt.BuildMtree())
	require.NoError(t, mt.DiffMtree())
	if nodeDiffs, ok := diffPairs(t, mt)[env.pairKey()]; ok {
		require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]), "equal rows give equal leaf hashes")
		require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]), "equal rows give equal leaf hashes")
	}

	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	env.withRepairModeTx(t, ctx, tx, func() {
		_, err := tx.Exec(ctx, "UPDATE "+safe+" SET b = -1 WHERE id = 7") // nosemgrep
		require.NoError(t, err)
	})
	require.NoError(t, tx.Commit(ctx))

	require.NoError(t, mt.DiffMtree())
	nodeDiffs, ok := diffPairs(t, mt)[env.pairKey()]
	require.True(t, ok, "mtree must find the changed row")
	require.Equal(t, []int{7}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]))

	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	report := readDiffReport(t, diffFile)
	require.Equal(t, 1, report.Summary.DiffRowsCount[env.pairKey()])
	for node, rows := range report.NodeDiffs[env.pairKey()].Rows {
		require.Len(t, rows, 1, "node %s", node)
		var names []string
		for _, kv := range rows[0] {
			names = append(names, kv.Key)
		}
		require.Subset(t, names, []string{"a", "b", "id"}, "node %s", node)
		require.Less(t, slices.Index(names, "a"), slices.Index(names, "b"), "node %s: columns in name order", node)
		require.Less(t, slices.Index(names, "b"), slices.Index(names, "id"), "node %s: columns in name order", node)
	}

	task := env.newTableRepairTask(env.ServiceN1, table, diffFile)
	require.NoError(t, task.Run(false))
	env.assertNoTableDiff(t, table)
	var b int
	require.NoError(t, env.N2Pool.QueryRow(ctx, "SELECT b FROM "+safe+" WHERE id = 7").Scan(&b)) // nosemgrep
	require.Equal(t, 70, b, "the repair writes each value into the column of the same name")
}
