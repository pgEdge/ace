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
	"sort"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestNumericScaleIsADifference checks that a numeric value with other
// trailing zeros on each node is a difference. The values are equal for the
// server (3000.00 = 3000.0), but the server prints them in a different way,
// and ACE compares the text of the values. Both table-diff and mtree must
// report every such row, and keep the value of each node as it is.
func TestNumericScaleIsADifference(t *testing.T) {
	env := newSpockEnv()
	testNumericScaleIsADifference(t, env)
}

func testNumericScaleIsADifference(t *testing.T, env *testEnv) {
	ctx := context.Background()
	table := "ace_numeric_scale_block_test"
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, table)

	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY,
			amount NUMERIC,
			amounts NUMERIC[],
			label TEXT
		)`, qualifiedTable)
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, createSQL) // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualifiedTable) // nosemgrep
		}
	})

	// Rows 1-5 differ only in the scale of amount, row 6 only in the scale of
	// an element of amounts. Row 7 is the same on both nodes.
	rows := []struct {
		id           int
		n1Amt, n2Amt string
		n1Arr, n2Arr string
	}{
		{1, "3000.00", "3000.0", "{1.50,2}", "{1.50,2}"},
		{2, "1234.5600", "1234.56", "{1.50,2}", "{1.50,2}"},
		{3, "100.10", "100.1", "{1.50,2}", "{1.50,2}"},
		{4, "42", "42.000", "{1.50,2}", "{1.50,2}"},
		{5, "0.00", "0.0", "{1.50,2}", "{1.50,2}"},
		{6, "7.50", "7.50", "{1.50,2}", "{1.5,2}"},
		{7, "7.50", "7.50", "{1.50,2}", "{1.50,2}"},
	}
	for _, pool := range env.pools() {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func(tx pgx.Tx) { _ = tx.Rollback(ctx) }(tx)
		env.withRepairModeTx(t, ctx, tx, func() {
			for _, r := range rows {
				amt, arr := r.n1Amt, r.n1Arr
				if pool == env.N2Pool {
					amt, arr = r.n2Amt, r.n2Arr
				}
				sql := fmt.Sprintf("INSERT INTO %s VALUES ($1, %s, '%s', 'same')", qualifiedTable, amt, arr) // nosemgrep
				_, err := tx.Exec(ctx, sql, r.id)
				require.NoError(t, err)
			}
		})
		require.NoError(t, tx.Commit(ctx))
	}

	nodes := []string{env.ServiceN1, env.ServiceN2}
	pairKey := env.pairKey()
	check := func(t *testing.T, res types.DiffOutput) {
		t.Helper()
		requireOnlyDiffIDs(t, res, pairKey, env.ServiceN1, env.ServiceN2, "1", "2", "3", "4", "5", "6")
		require.Equal(t, "3000.00", diffAmount(t, res, pairKey, env.ServiceN1, "1"))
		require.Equal(t, "3000.0", diffAmount(t, res, pairKey, env.ServiceN2, "1"))
	}

	t.Run("table-diff", func(t *testing.T) {
		task := env.newTableDiffTask(t, qualifiedTable, nodes)
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.ExecuteTask())
		check(t, diffResultOf(t, task))
	})

	t.Run("mtree", func(t *testing.T) {
		task := env.newMerkleTreeTask(t, qualifiedTable, nodes)
		task.BlockSize = 1000
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.MtreeInit())
		t.Cleanup(func() { task.MtreeTeardown() })
		require.NoError(t, task.BuildMtree())
		task.NoCDC = true
		require.NoError(t, task.DiffMtree())
		check(t, diffResultOf(t, task))
	})
}

// TestNumericKeyRepair checks a numeric primary key without a fixed scale.
// The same key can be stored as 1.50 on one node and as 1.5 on the other.
// The server holds them as equal, but prints them in a different way, so
// table-diff reports two rows: 1.50 on one node only and 1.5 on the other
// node only.
//
// Repair from n1 deletes 1.5 on n2 first and then inserts 1.50, so one
// repair makes the nodes equal, with the keys as n1 prints them.
func TestNumericKeyRepair(t *testing.T) {
	env := newSpockEnv()
	testNumericKeyRepair(t, env)
}

func testNumericKeyRepair(t *testing.T, env *testEnv) {
	cases := []struct {
		name    string
		defs    string
		cols    string
		n1, n2  []string
		checkSQ string
	}{
		{
			name: "simple",
			defs: "id numeric PRIMARY KEY, v text",
			cols: "id, v",
			// 1: the key differs only in scale, the row is the same.
			// 2: the key differs only in scale, the data differs.
			// 4.10: only on n1. 5.00: only on n2.
			n1:      []string{"(1.50, 'a')", "(2.000, 'b')", "(3, 'c')", "(4.10, 'd')"},
			n2:      []string{"(1.5, 'a')", "(2.0, 'changed')", "(3, 'c')", "(5.00, 'extra')"},
			checkSQ: "SELECT id::text || ':' || v FROM %s ORDER BY id",
		},
		{
			name:    "composite",
			defs:    "id numeric, tag text, v text, PRIMARY KEY (id, tag)",
			cols:    "id, tag, v",
			n1:      []string{"(1.50, 'x', 'a')", "(2.000, 'x', 'b')", "(3, 'x', 'c')", "(4.10, 'x', 'd')"},
			n2:      []string{"(1.5, 'x', 'a')", "(2.0, 'x', 'changed')", "(3, 'x', 'c')", "(5.00, 'x', 'extra')"},
			checkSQ: "SELECT id::text || ':' || tag || ':' || v FROM %s ORDER BY id, tag",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			table := "ace_numeric_key_" + tc.name
			qualifiedTable := typedKeyTable(t, env, table, tc.defs, tc.cols, tc.n1, tc.n2)
			nodes := []string{env.ServiceN1, env.ServiceN2}

			diffFile := env.runTableDiff(t, qualifiedTable, nodes)
			pair := readDiffReport(t, diffFile).NodeDiffs[env.pairKey()]
			ids := func(node string) []string {
				var out []string
				for _, row := range pair.Rows[node] {
					id, ok := row.Get("id")
					require.True(t, ok)
					out = append(out, fmt.Sprint(id))
				}
				sort.Strings(out)
				return out
			}
			require.Equal(t, []string{"1.50", "2.000", "4.10"}, ids(env.ServiceN1))
			require.Equal(t, []string{"1.5", "2.0", "5.00"}, ids(env.ServiceN2))

			task := env.newTableRepairTask(env.ServiceN1, qualifiedTable, diffFile)
			require.NoError(t, task.Run(false))
			require.Empty(t, task.FailedOps)
			env.assertNoTableDiff(t, qualifiedTable)

			read := func(pool *pgxpool.Pool) []string {
				rows, err := pool.Query(ctx, fmt.Sprintf(tc.checkSQ, qualifiedTable)) // nosemgrep
				require.NoError(t, err)
				defer rows.Close()
				var got []string
				for rows.Next() {
					var s string
					require.NoError(t, rows.Scan(&s))
					got = append(got, s)
				}
				require.NoError(t, rows.Err())
				return got
			}
			n1Rows := read(env.N1Pool)
			require.Len(t, n1Rows, 4)
			require.Equal(t, n1Rows, read(env.N2Pool), "n2 after repair")
		})
	}
}

// requireOnlyDiffIDs checks that the diff of the node pair has rows with
// exactly the given ids on each of the two nodes.
func requireOnlyDiffIDs(t *testing.T, res types.DiffOutput, pairKey, n1, n2 string, wantIDs ...string) {
	t.Helper()
	pair, ok := res.NodeDiffs[pairKey]
	require.True(t, ok, "no diff for the pair %s, want rows %v", pairKey, wantIDs)
	for _, node := range []string{n1, n2} {
		var got []string
		for _, row := range pair.Rows[node] {
			id, ok := row.Get("id")
			require.True(t, ok, "diff row on %s has no id: %v", node, row)
			got = append(got, fmt.Sprint(id))
		}
		sort.Strings(got)
		require.Equal(t, wantIDs, got, "diff rows on %s", node)
	}
}

// diffAmount returns the text of the amount column of the diff row with the
// given id on node.
func diffAmount(t *testing.T, res types.DiffOutput, pairKey, node, id string) string {
	t.Helper()
	for _, row := range res.NodeDiffs[pairKey].Rows[node] {
		if v, _ := row.Get("id"); fmt.Sprint(v) == id {
			amt, ok := row.Get("amount")
			require.True(t, ok, "diff row on %s has no amount: %v", node, row)
			return fmt.Sprint(amt)
		}
	}
	t.Fatalf("no diff row with id %s on %s", id, node)
	return ""
}

// TestNumericNamedEnum runs table-diff on a table with a column of an enum
// whose name starts with "numeric". The column must be read as it is.
func TestNumericNamedEnum(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	enumType := testSchema + ".numeric_level"
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TYPE "+enumType+" AS ENUM ('low', 'high')") // nosemgrep
		require.NoError(t, err)
	}
	// Cleanups run in reverse order: the type is dropped after the table.
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(context.Background(), "DROP TYPE IF EXISTS "+enumType) // nosemgrep
		}
	})
	qualifiedTable := typedKeyTable(t, env, "ace_numeric_enum", "id int PRIMARY KEY, level "+enumType, "id, level",
		[]string{"(1, 'low')", "(2, 'low')"}, []string{"(1, 'low')", "(2, 'high')"})

	diffFile := env.runTableDiff(t, qualifiedTable, []string{env.ServiceN1, env.ServiceN2})
	report := readDiffReport(t, diffFile)
	require.Equal(t, 1, report.Summary.DiffRowsCount[env.pairKey()])
}
