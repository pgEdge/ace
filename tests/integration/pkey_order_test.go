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
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/stretchr/testify/require"
)

// TestTableDiffCompositePKInIndexOrder uses a table whose primary key lists
// its columns in another order than the table: PRIMARY KEY (b, a) on a
// table (a, b, v). ACE must read the key as (b, a). The ROW(...)
// comparisons, the ORDER BY and the index walk of the slicer use the key in
// this order; in the table order they cannot use the index, and every step
// of the cut scans and sorts the whole table.
//
// The name starts with TestTableDiffCompositePK, so the composite-key step
// of the CI workflow runs it.
func TestTableDiffCompositePKInIndexOrder(t *testing.T) {
	ctx := context.Background()
	table := "pkey_index_order"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	for _, p := range pools {
		slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		slicingExec(t, ctx, p, "CREATE TABLE "+ident+" (a int, b int, v text, PRIMARY KEY (b, a))")
		slicingExec(t, ctx, p, "INSERT INTO "+ident+" SELECT a, b, 'v' FROM generate_series(1, 20) a, generate_series(1, 100) b")
		slicingExec(t, ctx, p, "ANALYZE "+ident)
	}
	t.Cleanup(func() {
		for _, p := range pools {
			slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		}
	})

	for _, p := range pools {
		key, err := queries.GetPrimaryKey(ctx, p, testSchema, table)
		require.NoError(t, err)
		require.Equal(t, []string{"b", "a"}, key, "the key columns must come in the order of the primary-key index")
	}

	// 5 changed rows and 3 rows missing on n2.
	slicingExec(t, ctx, pgCluster.Node2Pool, "UPDATE "+ident+" SET v = 'changed' WHERE a = 7 AND b IN (3, 30, 50, 70, 99)")
	slicingExec(t, ctx, pgCluster.Node2Pool, "DELETE FROM "+ident+" WHERE a = 1 AND b IN (1, 50, 100)")

	require.Equal(t, 8, slicingDiff(t, qualified, ""))
}
