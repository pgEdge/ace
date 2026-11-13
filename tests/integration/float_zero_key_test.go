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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// TestFloatZeroKeyDiffAndRepair runs table-diff and table-repair on a float8
// primary key that is -0 on n1 and 0 on n2. The server holds -0 = 0, so this
// is one row, but the two keys print in a different way.
//
//   - table-diff finds two key texts. Each text finds only the row that
//     prints it, so the report has the row of n1 as a row on n1 only and the
//     row of n2 as a row on n2 only.
//   - table-repair with a source of truth deletes the key of the target and
//     then inserts the key of the source, so the nodes are equal after one
//     repair.
//   - with --upsert-only, --insert-only or --bidirectional there is no delete.
//     Each insert finds the row of the other key and fails. Repair records the
//     failed insert, leaves the row as it is, and reports an error.
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
		name   string
		source string
		mode   string // "", "upsert_only", "insert_only" or "bidirectional"
		// failed lists the failed row operations as "node:operation:key".
		failed []string
		// want, if set, is the content of both nodes after the repair, which
		// must succeed.
		want []string
	}{
		{name: "source_n1", source: env.ServiceN1, want: []string{"-0:a", "1:b"}},
		{name: "source_n2", source: env.ServiceN2, want: []string{"1:b", "0:x"}},
		{name: "upsert_only", source: env.ServiceN1, mode: "upsert_only",
			failed: []string{env.ServiceN2 + ":insert:-0"}},
		{name: "insert_only", source: env.ServiceN1, mode: "insert_only",
			failed: []string{env.ServiceN2 + ":insert:-0"}},
		{name: "bidirectional", mode: "bidirectional",
			failed: []string{env.ServiceN1 + ":insert:0", env.ServiceN2 + ":insert:-0"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := typedKeyTable(t, env, "ace_float_zero_"+tc.name, "k float8 PRIMARY KEY, v text", "k, v", n1Rows, n2Rows)
			diffFile := diff(t, table)

			task := env.newTableRepairTask(tc.source, table, diffFile)
			task.UpsertOnly = tc.mode == "upsert_only"
			task.InsertOnly = tc.mode == "insert_only" || tc.mode == "bidirectional"
			task.Bidirectional = tc.mode == "bidirectional"
			err := task.Run(false)

			if tc.want != nil {
				require.NoError(t, err)
				require.Empty(t, task.FailedOps)
				require.Equal(t, tc.want, read(t, env.N1Pool, table), "n1 after repair")
				require.Equal(t, tc.want, read(t, env.N2Pool, table), "n2 after repair")
				env.assertNoTableDiff(t, table)
				return
			}

			require.Error(t, err)
			var failed []string
			for _, f := range task.FailedOps {
				failed = append(failed, fmt.Sprintf("%s:%s:%v", f.Node, f.Operation, f.Key["k"]))
				require.Contains(t, f.Reason, "23505", "a unique violation: %s", f.Reason)
			}
			sort.Strings(failed)
			require.Equal(t, tc.failed, failed)

			require.Equal(t, []string{"-0:a", "1:b"}, read(t, env.N1Pool, table), "n1 after repair")
			require.Equal(t, []string{"1:b", "0:x"}, read(t, env.N2Pool, table), "n2 after repair")
			diff(t, table)
		})
	}
}
