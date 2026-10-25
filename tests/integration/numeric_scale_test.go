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
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestNumericScaleInMismatchedBlock checks that a numeric value with other
// trailing zeros is not a difference, also when its block differs because of
// another row.
//
// The block hash ignores trailing zeros (trim_scale), so a block where only
// the scale differs is never fetched. Here one more row of the block really
// differs, so ACE fetches every row of the block and compares them one by
// one. That comparison must use the same rule as the hash, or the rows that
// differ only in scale are reported too.
func TestNumericScaleInMismatchedBlock(t *testing.T) {
	env := newSpockEnv()
	testNumericScaleInMismatchedBlock(t, env)
}

func testNumericScaleInMismatchedBlock(t *testing.T, env *testEnv) {
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

	// Rows 1-5 differ only in the scale of amount. Row 6 has a real
	// difference in label. All rows are in one block.
	rows := []struct {
		id           int
		n1Amt, n2Amt string
		n1Lbl, n2Lbl string
	}{
		{1, "3000.00", "3000.0", "a", "a"},
		{2, "1234.5600", "1234.56", "b", "b"},
		{3, "100.10", "100.1", "c", "c"},
		{4, "42", "42.000", "d", "d"},
		{5, "0.00", "0.0", "e", "e"},
		{6, "7.50", "7.50", "f", "changed"},
	}
	for _, pool := range env.pools() {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func(tx pgx.Tx) { _ = tx.Rollback(ctx) }(tx)
		env.withRepairModeTx(t, ctx, tx, func() {
			for _, r := range rows {
				amt, lbl := r.n1Amt, r.n1Lbl
				if pool == env.N2Pool {
					amt, lbl = r.n2Amt, r.n2Lbl
				}
				// The array is the same on both nodes: trim_scale does not
				// apply to arrays, so it must not change the result either.
				sql := fmt.Sprintf("INSERT INTO %s VALUES ($1, %s, '{1.50,2}', $2)", qualifiedTable, amt) // nosemgrep
				_, err := tx.Exec(ctx, sql, r.id, lbl)
				require.NoError(t, err)
			}
		})
		require.NoError(t, tx.Commit(ctx))
	}

	var n1Text, n2Text string
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT amount::text FROM "+qualifiedTable+" WHERE id = 1").Scan(&n1Text)) // nosemgrep
	require.NoError(t, env.N2Pool.QueryRow(ctx, "SELECT amount::text FROM "+qualifiedTable+" WHERE id = 1").Scan(&n2Text)) // nosemgrep
	require.NotEqual(t, n1Text, n2Text, "precondition: the nodes must store different scales")

	nodes := []string{env.ServiceN1, env.ServiceN2}
	pairKey := env.pairKey()

	t.Run("table-diff", func(t *testing.T) {
		task := env.newTableDiffTask(t, qualifiedTable, nodes)
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.ExecuteTask())
		requireOnlyDiffIDs(t, task.DiffResult, pairKey, env.ServiceN1, env.ServiceN2, "6")
		requireDiffAmount(t, task.DiffResult, pairKey, "7.5")
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
		requireOnlyDiffIDs(t, task.DiffResult, pairKey, env.ServiceN1, env.ServiceN2, "6")
		requireDiffAmount(t, task.DiffResult, pairKey, "7.5")
	})
}

// TestNumericKeyRepair checks a numeric primary key without a fixed scale.
// The same key can be stored as 1.50 on one node and as 1.5 on the other.
// For the server these are one key (1.50 = 1.5), so ACE must match them as
// one row: a diff must not report them as a row missing on each side, and
// repair must find the row on the target by the key from the diff report.
// Otherwise repair inserts the key it thinks is missing, hits the existing
// row, and then deletes it as an extra row.
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
		wantN1  int // diff rows on n1
		wantN2  int // diff rows on n2
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
			wantN1:  2, // 2 (modified), 4.1 (n1 only)
			wantN2:  2, // 2 (modified), 5 (n2 only)
			checkSQ: "SELECT trim_scale(id)::text || ':' || v FROM %s ORDER BY id",
		},
		{
			name:    "composite",
			defs:    "id numeric, tag text, v text, PRIMARY KEY (id, tag)",
			cols:    "id, tag, v",
			n1:      []string{"(1.50, 'x', 'a')", "(2.000, 'x', 'b')", "(3, 'x', 'c')", "(4.10, 'x', 'd')"},
			n2:      []string{"(1.5, 'x', 'a')", "(2.0, 'x', 'changed')", "(3, 'x', 'c')", "(5.00, 'x', 'extra')"},
			wantN1:  2,
			wantN2:  2,
			checkSQ: "SELECT trim_scale(id)::text || ':' || tag || ':' || v FROM %s ORDER BY id, tag",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			table := "ace_numeric_key_" + tc.name
			qualifiedTable := fmt.Sprintf("%s.%s", testSchema, table)

			for _, pool := range env.pools() {
				_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", qualifiedTable, tc.defs)) // nosemgrep
				require.NoError(t, err)
				if env.HasSpock {
					_, err = pool.Exec(ctx, "SELECT spock.repset_add_table('default', $1)", qualifiedTable)
					require.NoError(t, err)
				}
			}
			t.Cleanup(func() {
				for _, pool := range env.pools() {
					if env.HasSpock {
						_, _ = pool.Exec(context.Background(), "SELECT spock.repset_remove_table('default', $1)", qualifiedTable)
					}
					_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualifiedTable+" CASCADE") // nosemgrep
				}
			})

			for _, side := range []struct {
				pool *pgxpool.Pool
				rows []string
			}{{env.N1Pool, tc.n1}, {env.N2Pool, tc.n2}} {
				tx, err := side.pool.Begin(ctx)
				require.NoError(t, err)
				env.withRepairModeTx(t, ctx, tx, func() {
					_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", qualifiedTable, tc.cols, strings.Join(side.rows, ", "))) // nosemgrep
					require.NoError(t, err)
				})
				require.NoError(t, tx.Commit(ctx))
			}

			diffFile := env.runTableDiff(t, qualifiedTable, []string{env.ServiceN1, env.ServiceN2})
			data, err := os.ReadFile(diffFile)
			require.NoError(t, err)
			var out types.DiffOutput
			require.NoError(t, json.Unmarshal(data, &out))
			pair := out.NodeDiffs[env.pairKey()]
			require.Len(t, pair.Rows[env.ServiceN1], tc.wantN1, "diff rows on n1: %v", pair.Rows[env.ServiceN1])
			require.Len(t, pair.Rows[env.ServiceN2], tc.wantN2, "diff rows on n2: %v", pair.Rows[env.ServiceN2])

			env.repairTable(t, qualifiedTable, env.ServiceN1)
			env.assertNoTableDiff(t, qualifiedTable)

			// n2 must now hold the rows of n1, up to the scale of the key.
			// In particular row 1 must still be there: it was never different.
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

// requireDiffAmount checks the text of the amount column of every diff row of
// the pair. The report keeps a numeric value without trailing zeros, as the
// row hash compares it.
func requireDiffAmount(t *testing.T, res types.DiffOutput, pairKey, want string) {
	t.Helper()
	for node, rows := range res.NodeDiffs[pairKey].Rows {
		for _, row := range rows {
			amt, ok := row.Get("amount")
			require.True(t, ok, "diff row on %s has no amount: %v", node, row)
			require.Equal(t, want, fmt.Sprint(amt), "amount of the diff row on %s", node)
		}
	}
}

// TestNumericNamedEnum runs table-diff on a table with a column of an enum
// whose name starts with "numeric". The enum is not a numeric type and has no
// trim_scale(), so the column must be read as it is.
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
