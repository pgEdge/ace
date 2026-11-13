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
// row and a deleted row in a table with a numeric primary key whose keys have
// trailing zeros. The rows of a mismatched block are matched by the text of
// the key, so the type of the key does not matter.
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

	// Both nodes store the keys with two digits after the point: 0.50, 1.00.
	for _, pool := range pools {
		_, err := pool.Exec(ctx, "INSERT INTO "+safe+" SELECT round(g / 2.0, 2), 'same' FROM generate_series(1, 200) g") // nosemgrep
		require.NoError(t, err)
	}

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

	total := 0
	for _, c := range task.DiffResult.Summary.DiffRowsCount {
		total += c
	}
	require.Equal(t, 2, total, "expected the changed row and the deleted row")

	// The rows of each node: 50.5 on both nodes with different values, 7 on
	// n1 only.
	pair, ok := diffPairs(t, task)[env.pairKey()]
	require.True(t, ok, "no diff for %s", env.pairKey())
	// The keys are compared as the server prints them, with their scale.
	keys := func(node string) map[string]string {
		out := map[string]string{}
		for _, r := range pair.Rows[node] {
			id, _ := r.Get("id")
			val, _ := r.Get("val")
			out[fmt.Sprint(id)] = fmt.Sprint(val)
		}
		return out
	}
	require.Equal(t, map[string]string{"50.50": "diverged", "7.00": "same"}, keys(env.ServiceN1))
	require.Equal(t, map[string]string{"50.50": "same"}, keys(env.ServiceN2))
}

// TestMtreeNumericKeyScale checks a numeric primary key that n1 stores as
// 1.50 and n2 as 1.5. The server holds them as equal, but prints them in a
// different way, so mtree table-diff reports two rows: 1.50 on n1 only and
// 1.5 on n2 only.
func TestMtreeNumericKeyScale(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	safe := pgx.Identifier{testSchema, "mtree_numeric_key_scale"}.Sanitize()
	qualified := testSchema + ".mtree_numeric_key_scale"
	rows := map[*pgxpool.Pool]string{
		env.N1Pool: "(1.50, 'a'), (2, 'same')",
		env.N2Pool: "(1.5, 'a'), (2, 'same')",
	}
	for pool, values := range rows {
		_, err := pool.Exec(ctx, "CREATE TABLE "+safe+" (id numeric PRIMARY KEY, val text)") // nosemgrep
		require.NoError(t, err)
		t.Cleanup(func() {
			if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE"); err != nil { // nosemgrep
				t.Errorf("drop table: %v", err)
			}
		})
		_, err = pool.Exec(ctx, "INSERT INTO "+safe+" VALUES "+values) // nosemgrep
		require.NoError(t, err)
	}

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	task.BlockSize = 1000
	task.OverrideBlockSize = true
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() {
		if err := task.MtreeTeardown(); err != nil {
			t.Errorf("mtree teardown: %v", err)
		}
	})
	require.NoError(t, task.BuildMtree())
	require.NoError(t, task.DiffMtree())

	pair, ok := diffPairs(t, task)[env.pairKey()]
	require.True(t, ok, "no diff for %s", env.pairKey())
	keys := func(node string) []string {
		var out []string
		for _, r := range pair.Rows[node] {
			id, _ := r.Get("id")
			out = append(out, fmt.Sprint(id))
		}
		return out
	}
	require.Equal(t, []string{"1.50"}, keys(env.ServiceN1))
	require.Equal(t, []string{"1.5"}, keys(env.ServiceN2))
}
