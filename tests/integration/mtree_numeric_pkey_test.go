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
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestMtreeNumericPrimaryKeyDiff checks that mtree table-diff finds a changed
// row and a deleted row in a table with a numeric primary key, where n1 keeps
// the keys with trailing zeros and n2 without them. The rows of a mismatched
// block are matched by the text of the key, so the type of the key does not
// matter.
//
// The test uses one block: the order of numeric block bounds in Go is a
// separate, known limit, and with one block there is nothing to order.
//
// The table is not added to a spock replication set, so a change on one node
// stays there.
func TestMtreeNumericPrimaryKeyDiff(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "mtree_numeric_pkey"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, pool := range pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+" (id numeric PRIMARY KEY, val text)") // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
		for _, pattern := range []string{"*_diffs-*.json", "*_diffs-*.html"} {
			files, _ := filepath.Glob(pattern)
			for _, f := range files {
				os.Remove(f)
			}
		}
	})

	// n1 stores the keys with two digits after the point, n2 without the
	// trailing zero: 0.50 and 0.5 are one key.
	_, err := env.N1Pool.Exec(ctx, "INSERT INTO "+safe+" SELECT round(g / 2.0, 2), 'same' FROM generate_series(1, 200) g") // nosemgrep
	require.NoError(t, err)
	_, err = env.N2Pool.Exec(ctx, "INSERT INTO "+safe+" SELECT trim_scale(round(g / 2.0, 2)), 'same' FROM generate_series(1, 200) g") // nosemgrep
	require.NoError(t, err)

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	task.BlockSize = 1000
	task.OverrideBlockSize = true
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { _ = task.MtreeTeardown() })
	require.NoError(t, task.BuildMtree())

	tag, err := env.N1Pool.Exec(ctx, "UPDATE "+safe+" SET val = 'diverged' WHERE id = 50.5") // nosemgrep
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())
	tag, err = env.N2Pool.Exec(ctx, "DELETE FROM "+safe+" WHERE id = 7") // nosemgrep
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	require.NoError(t, task.DiffMtree(), "diff of a numeric-PK table must not fail")
	require.Empty(t, task.DiffResult.Summary.IncompletePairs,
		"the comparison must finish: a failed work item would make the result meaningless")

	total := 0
	for _, c := range task.DiffResult.Summary.DiffRowsCount {
		total += c
	}
	require.Equal(t, 2, total, "expected the changed row and the deleted row")
}
