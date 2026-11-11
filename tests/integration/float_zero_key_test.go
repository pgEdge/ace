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

// TestFloatZeroKeyDiffAndRepair runs table-diff and table-repair on a float8
// primary key that is -0 on n1 and 0 on n2. The server holds -0 = 0, so this
// is one row, but the two keys print in a different way.
//
// The test records what ACE does today:
//   - table-diff finds two key texts. Each text finds only the row that
//     prints it, so the report has the row of n1 as a row on n1 only and the
//     row of n2 as a row on n2 only.
//   - table-repair matches the rows of the report by key text. The key of the
//     target is not in the source rows, so repair deletes it, and the key of
//     the source is not in the target rows, so repair writes it. The delete
//     runs first and removes the row, as -0 = 0. The upsert then inserts the
//     source row with its own key. After this the nodes are equal.
//   - with --upsert-only there is no delete. The upsert finds the target row
//     through the conflict on the key, changes the other columns and keeps the
//     key of the target, so the next diff reports the row again.
func TestFloatZeroKeyDiffAndRepair(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	nodes := []string{env.ServiceN1, env.ServiceN2}
	n1Rows := []string{"('-0', 'a')", "(1, 'b')"}
	n2Rows := []string{"('0', 'x')", "(1, 'b')"}

	// read returns the rows of the table as "k:v", with k as the server
	// prints it.
	read := func(t *testing.T, pool *pgxpool.Pool, table string) []string {
		t.Helper()
		rows, err := pool.Query(ctx, "SELECT k::text || ':' || v FROM "+table+" ORDER BY v") // nosemgrep
		require.NoError(t, err)
		defer rows.Close()
		var out []string
		for rows.Next() {
			var s string
			require.NoError(t, rows.Scan(&s))
			out = append(out, s)
		}
		require.NoError(t, rows.Err())
		return out
	}

	// diff runs table-diff and checks that each node has its row only.
	diff := func(t *testing.T, table string) string {
		t.Helper()
		diffFile := env.runTableDiff(t, table, nodes)
		report := readDiffReport(t, diffFile)
		require.Equal(t, 2, report.Summary.DiffRowsCount[env.pairKey()], "a row on n1 only and a row on n2 only")
		pair := report.NodeDiffs[env.pairKey()]
		for node, want := range map[string]string{env.ServiceN1: "-0:a", env.ServiceN2: "0:x"} {
			var got []string
			for _, row := range pair.Rows[node] {
				k, _ := row.Get("k")
				v, _ := row.Get("v")
				got = append(got, fmt.Sprintf("%v:%v", k, v))
			}
			require.Equal(t, []string{want}, got, "diff rows on %s", node)
		}
		return diffFile
	}

	cases := []struct {
		name       string
		source     string
		upsertOnly bool
		want       []string // rows on both nodes after repair, or on the target
		equal      bool     // the nodes are equal after repair
	}{
		{name: "source_n1", source: env.ServiceN1, want: []string{"-0:a", "1:b"}, equal: true},
		{name: "source_n2", source: env.ServiceN2, want: []string{"1:b", "0:x"}, equal: true},
		{name: "upsert_only", source: env.ServiceN1, upsertOnly: true, want: []string{"0:a", "1:b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := typedKeyTable(t, env, "ace_float_zero_"+tc.name, "k float8 PRIMARY KEY, v text", "k, v", n1Rows, n2Rows)
			diffFile := diff(t, table)

			task := env.newTableRepairTask(tc.source, table, diffFile)
			task.UpsertOnly = tc.upsertOnly
			require.NoError(t, task.Run(false))

			if tc.equal {
				require.Equal(t, tc.want, read(t, env.N1Pool, table), "n1 after repair")
				require.Equal(t, tc.want, read(t, env.N2Pool, table), "n2 after repair")
				env.assertNoTableDiff(t, table)
				return
			}
			require.Equal(t, []string{"-0:a", "1:b"}, read(t, env.N1Pool, table), "n1 after repair")
			require.Equal(t, tc.want, read(t, env.N2Pool, table), "n2 after repair: v is fixed, the key is not")

			td := env.newTableDiffTask(t, table, nodes)
			require.NoError(t, td.RunChecks(false))
			require.NoError(t, td.ExecuteTask())
			require.Equal(t, 2, td.DiffResult.Summary.DiffRowsCount[env.pairKey()], "the row is still reported")
		})
	}
}
