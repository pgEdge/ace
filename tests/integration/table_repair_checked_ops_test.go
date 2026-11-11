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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// execOnN2 runs sql on n2 in one transaction in repair mode, so that the
// change is not replicated to n1.
func execOnN2(t *testing.T, ctx context.Context, env *testEnv, sql string) {
	t.Helper()
	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(ctx) }()
	env.withRepairModeTx(t, ctx, tx, func() {
		_, err := tx.Exec(ctx, sql) // nosemgrep
		require.NoError(t, err)
	})
	require.NoError(t, tx.Commit(ctx))
}

// TestTableRepairCheckedOps changes the target after the diff, so that one
// row operation of each command kind cannot be done as the report says.
// Repair runs each kind of row operation as a separate command in its own
// transaction. The command with the failed row is rolled back and all its
// rows are recorded as failed; the commands of the other kinds are applied.
//
// Rows on n1: 1, 3, 4, 5. Rows on n2: 2, 3 and 4 with other values, 6. With
// n1 as the source of truth, repair inserts 1 and 5, deletes 2 and 6, and
// updates 3 and 4 on n2.
func TestTableRepairCheckedOps(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()

	cases := []struct {
		name  string
		after string // SQL run on n2 after the diff, %s is the table
		// failed lists the failed row operations as "operation:id:reason",
		// where reason is "culprit" for the row that failed and "rolled back"
		// for the other rows of its command.
		failed []string
		want   []string // rows on n2 after repair, as "id:v"
	}{
		{
			name:   "insert_conflict",
			after:  "INSERT INTO %s VALUES (1, 'new')",
			failed: []string{"insert:1:culprit", "insert:5:rolled back"},
			want:   []string{"1:new", "3:c", "4:d"},
		},
		{
			name:   "delete_gone",
			after:  "DELETE FROM %s WHERE id = 2",
			failed: []string{"delete:2:culprit", "delete:6:rolled back"},
			want:   []string{"1:a", "3:c", "4:d", "5:e", "6:y"},
		},
		{
			// A constraint that only the target has: the key is right, but
			// the row cannot be written.
			name:   "check_constraint",
			after:  "ALTER TABLE %s ADD CONSTRAINT v_not_e CHECK (v <> 'e') NOT VALID",
			failed: []string{"insert:1:rolled back", "insert:5:culprit"},
			want:   []string{"3:c", "4:d"},
		},
		{
			name:   "update_gone",
			after:  "DELETE FROM %s WHERE id = 3",
			failed: []string{"update:3:culprit", "update:4:rolled back"},
			want:   []string{"1:a", "4:d2", "5:e"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := typedKeyTable(t, env, "ace_checked_ops_"+tc.name, "id int PRIMARY KEY, v text", "id, v",
				[]string{"(1, 'a')", "(3, 'c')", "(4, 'd')", "(5, 'e')"},
				[]string{"(2, 'x')", "(3, 'c2')", "(4, 'd2')", "(6, 'y')"})
			diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
			execOnN2(t, ctx, env, fmt.Sprintf(tc.after, table))

			task := env.newTableRepairTask(env.ServiceN1, table, diffFile)
			require.Error(t, task.Run(false))

			var failed []string
			for _, f := range task.FailedOps {
				require.Equal(t, env.ServiceN2, f.Node)
				reason := "culprit"
				if strings.HasPrefix(f.Reason, "rolled back") {
					reason = "rolled back"
				}
				failed = append(failed, fmt.Sprintf("%s:%v:%s", f.Operation, f.Key["id"], reason))
			}
			sort.Strings(failed)
			require.Equal(t, tc.failed, failed, "failed row operations: %+v", task.FailedOps)

			rows, err := env.N2Pool.Query(ctx, "SELECT id::text || ':' || v FROM "+table+" ORDER BY id") // nosemgrep
			require.NoError(t, err)
			defer rows.Close()
			var got []string
			for rows.Next() {
				var s string
				require.NoError(t, rows.Scan(&s))
				got = append(got, s)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, tc.want, got, "rows on n2 after repair")
		})
	}
}

// TestTableRepairUniqueValueMoves checks a repair in which a value of a
// unique column moves from one key to another. The source has (1, 'A'); the
// target has (1, 'B') and (2, 'A'). The delete of key 2 frees 'A', so it must
// run before the update of key 1. One repair makes the nodes equal.
func TestTableRepairUniqueValueMoves(t *testing.T) {
	env := newSpockEnv()
	table := typedKeyTable(t, env, "ace_repair_unique_moves", "id int PRIMARY KEY, code text UNIQUE", "id, code",
		[]string{"(1, 'A')"}, []string{"(1, 'B')", "(2, 'A')"})
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})

	task := env.newTableRepairTask(env.ServiceN1, table, diffFile)
	require.NoError(t, task.Run(false))
	require.Empty(t, task.FailedOps)
	env.assertNoTableDiff(t, table)
}

// TestTableRepairFixNullsChecked checks that a NULL fix changes only a value
// that is still NULL. A value set on the target after the diff stops the
// command of that node; the other node is repaired.
func TestTableRepairFixNullsChecked(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	table := typedKeyTable(t, env, "ace_checked_fix_nulls", "id int PRIMARY KEY, a text, b text", "id, a, b",
		[]string{"(1, 'x', NULL)", "(2, 'y', NULL)"},
		[]string{"(1, NULL, 'p')", "(2, NULL, 'q')"})
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	execOnN2(t, ctx, env, fmt.Sprintf("UPDATE %s SET a = 'new' WHERE id = 1", table))

	task := env.newTableRepairTask("", table, diffFile)
	task.FixNulls = true
	require.Error(t, task.Run(false))

	var failed []string
	for _, f := range task.FailedOps {
		reason := "culprit"
		if strings.HasPrefix(f.Reason, "rolled back") {
			reason = "rolled back"
		}
		failed = append(failed, fmt.Sprintf("%s:%s:%v:%s", f.Node, f.Operation, f.Key["id"], reason))
	}
	sort.Strings(failed)
	require.Equal(t, []string{
		env.ServiceN2 + ":update_nulls:1:culprit",
		env.ServiceN2 + ":update_nulls:2:rolled back",
	}, failed, "failed row operations: %+v", task.FailedOps)

	read := func(pool *pgxpool.Pool) []string {
		rows, err := pool.Query(ctx, "SELECT id::text || ':' || coalesce(a, '-') || ':' || coalesce(b, '-') FROM "+table+" ORDER BY id") // nosemgrep
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
	require.Equal(t, []string{"1:x:p", "2:y:q"}, read(env.N1Pool), "n1 got the values of b")
	require.Equal(t, []string{"1:new:p", "2:-:q"}, read(env.N2Pool), "n2 is not changed")
}
