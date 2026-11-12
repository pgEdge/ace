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
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/stretchr/testify/require"
)

// createTypesOnBoth runs the statements on both nodes and drops the objects
// in drops at the end of the test. The drops must not fail on an object
// that does not exist (IF EXISTS): they are registered before the
// statements run, so that a statement that fails on one node does not
// leave the objects of another node for the next run.
func createTypesOnBoth(t *testing.T, env *testEnv, stmts, drops []string) {
	t.Helper()
	ctx := context.Background()
	// Cleanups run in reverse order: the types are dropped after the tables
	// that use them.
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			for _, s := range drops {
				_, _ = pool.Exec(context.Background(), s) // nosemgrep
			}
		}
	})
	for _, pool := range env.pools() {
		for _, s := range stmts {
			_, err := pool.Exec(ctx, s) // nosemgrep
			require.NoError(t, err, "exec %q", s)
		}
	}
}

// rowTexts returns the text of every row of the table, as the server prints
// it, in key order.
func rowTexts(t *testing.T, pool *pgxpool.Pool, table, orderBy string) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), fmt.Sprintf("SELECT t::text FROM %s AS t ORDER BY %s", table, orderBy)) // nosemgrep
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

// TestTextValuesRoundTrip runs table-diff and table-repair on values that a
// conversion to a Go type and back would change or refuse. After the repair,
// the output of every value on the target must be the output on the source,
// byte for byte. Rows 1-6 exist on n1 only and are inserted; rows 11-16 have
// other values on n2 and are updated.
func TestTextValuesRoundTrip(t *testing.T) {
	env := newSpockEnv()
	createTypesOnBoth(t, env, []string{
		"CREATE EXTENSION IF NOT EXISTS citext",
		"CREATE TYPE " + testSchema + ".rt_mood AS ENUM ('sad', 'ok', 'happy')",
		"CREATE TYPE " + testSchema + ".rt_pair AS (a int, b text)",
		"CREATE DOMAIN " + testSchema + ".rt_pair_dom AS " + testSchema + ".rt_pair",
	}, []string{
		"DROP DOMAIN IF EXISTS " + testSchema + ".rt_pair_dom",
		"DROP TYPE IF EXISTS " + testSchema + ".rt_pair",
		"DROP TYPE IF EXISTS " + testSchema + ".rt_mood",
	})

	defs := "id int PRIMARY KEY, n numeric, n52 numeric(5,2), f float8, ts timestamp, tstz timestamptz, d date, " +
		"iv interval, m money, b bytea, c char(5), flag boolean, ip inet, arr int[], j jsonb, " +
		"mood " + testSchema + ".rt_mood, pair " + testSchema + ".rt_pair, pdom " + testSchema + ".rt_pair_dom, " +
		"r int4range, parr " + testSchema + ".rt_pair[], vc varchar(10), ci citext"
	cols := "id, n, n52, f, ts, tstz, d, iv, m, b, c, flag, ip, arr, j, mood, pair, pdom, r, parr, vc, ci"
	values := []string{
		`'1.50', '1.50', '-0', 'infinity', '2026-03-29 00:59:59.999999+00', '-infinity', '1 day 02:03:04.5', '12.34', ` +
			`'\x00ff00', 'ab', true, '10.0.0.1', '{1,NULL,3}', '{"a": [1, 2.50], "b": null}', 'ok', '(1,"x y")', '(2,z)', ` +
			`'[1,5)', '{"(1,a)","(2,\"b c\")"}', 'abc', 'MiXeD'`,
		`'NaN', '0.00', 'NaN', '-infinity', '2026-10-25 01:00:00+00', 'infinity', '-3 mons -2 days', '-0.01', ` +
			`'\x', 'a b  ', false, '::1/128', '{}', '[]', 'sad', '(,)', '(,"")', 'empty', '{}', ' x ', ''`,
		`'Infinity', '-999.99', 'Infinity', '2000-02-29 12:00:00.000001', '1999-12-31 23:59:59+00', '2000-01-01', ` +
			`'0', '0', '\x5c22', '"', NULL, '192.168.0.0/16', '{{1,2},{3,4}}', '"text"', 'happy', '(-1,"q\"r")', NULL, ` +
			`'(,)', '{NULL}', 'a''b', 'x'`,
		`'-Infinity', NULL, '1e-300', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, ` +
			`NULL, NULL, NULL, NULL, NULL`,
		// A NULL and an empty string must stay different.
		`NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '\x', '', NULL, NULL, '{NULL}', 'null', NULL, NULL, NULL, ` +
			`NULL, NULL, '', ''`,
		`'123456789012345678901234567890.123456789', '0', '1.7976931348623157e308', NULL, NULL, NULL, '1 year', ` +
			`'-92233720368547758.08', NULL, NULL, NULL, '0.0.0.0/0', NULL, '{"k": 1.0}', NULL, NULL, NULL, '[,)', NULL, NULL, NULL`,
	}
	var n1Rows, n2Rows []string
	for i, v := range values {
		n1Rows = append(n1Rows, fmt.Sprintf("(%d, %s)", i+1, v), fmt.Sprintf("(%d, %s)", i+11, v))
		n2Rows = append(n2Rows, fmt.Sprintf("(%d, %s)", i+11, strings.Repeat("NULL, ", 20)+"'other'"))
	}
	table := typedKeyTable(t, env, "ace_text_values", defs, cols, n1Rows, n2Rows)

	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	report := readDiffReport(t, diffFile)
	pair := report.NodeDiffs[env.pairKey()]
	require.Len(t, pair.Rows[env.ServiceN1], 2*len(values))
	for _, row := range pair.Rows[env.ServiceN1] {
		for _, kv := range row {
			if kv.Key == "_spock_metadata_" {
				continue
			}
			if kv.Value != nil {
				require.IsType(t, "", kv.Value, "column %s of the report must be text", kv.Key)
			}
		}
		if id, _ := row.Get("id"); id == "5" {
			vc, _ := row.Get("vc")
			require.Equal(t, "", vc, "an empty string stays an empty string")
			n, _ := row.Get("n")
			require.Nil(t, n, "a NULL stays NULL")
			b, _ := row.Get("b")
			require.Equal(t, `\x`, b, "an empty bytea is its output")
		}
		if id, _ := row.Get("id"); id == "1" {
			n, _ := row.Get("n")
			require.Equal(t, "1.50", n, "a numeric keeps its scale")
			f, _ := row.Get("f")
			require.Equal(t, "-0", f)
			c, _ := row.Get("c")
			require.Equal(t, "ab   ", c, "bpchar keeps its padding")
			flag, _ := row.Get("flag")
			require.Equal(t, "t", flag, "boolean is its output, not ::text")
		}
	}

	repair := env.newTableRepairTask(env.ServiceN1, table, diffFile)
	require.NoError(t, repair.Run(false))
	require.Empty(t, repair.FailedOps)
	env.assertNoTableDiff(t, table)
	require.Equal(t, rowTexts(t, env.N1Pool, table, "id"), rowTexts(t, env.N2Pool, table, "id"))
}

// TestTextValuesExplicitCast checks that every text goes to the server
// through the input function of its type, and never through a cast from
// text: the enum here has a cast from text that turns every text into
// 'bad', while its input function reads 'ok' as 'ok'. The first table has
// the enum as the key and as a value: the row read must find the key 'ok',
// and the repair must write 'ok'. The second table checks a when comparison
// of a repair plan and the stale check of a rule on the enum key.
func TestTextValuesExplicitCast(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	mood := testSchema + ".ec_mood"
	createTypesOnBoth(t, env, []string{
		"CREATE TYPE " + mood + " AS ENUM ('ok', 'bad')",
		"CREATE FUNCTION " + testSchema + ".ec_mood_from_text(text) RETURNS " + mood +
			" LANGUAGE sql IMMUTABLE AS $$ SELECT 'bad'::" + mood + " $$",
		"CREATE CAST (text AS " + mood + ") WITH FUNCTION " + testSchema + ".ec_mood_from_text(text)",
	}, []string{
		"DROP CAST IF EXISTS (text AS " + mood + ")",
		"DROP FUNCTION IF EXISTS " + testSchema + ".ec_mood_from_text(text)",
		"DROP TYPE IF EXISTS " + mood,
	})
	for _, pool := range env.pools() {
		var got string
		require.NoError(t, pool.QueryRow(ctx, "SELECT format('%s', 'ok'::text::"+mood+")").Scan(&got)) // nosemgrep
		require.Equal(t, "bad", got, "the cast from text must be in place")
	}

	t.Run("diff and repair", func(t *testing.T) {
		table := typedKeyTable(t, env, "ace_explicit_cast", "k "+mood+" PRIMARY KEY, v "+mood+", w text", "k, v, w",
			[]string{"('ok', 'ok', 'a')", "('bad', 'ok', 'a')"},
			[]string{"('ok', 'bad', 'b')"})

		diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
		pair := readDiffReport(t, diffFile).NodeDiffs[env.pairKey()]
		require.Len(t, pair.Rows[env.ServiceN1], 2)
		require.Len(t, pair.Rows[env.ServiceN2], 1, "the key 'ok' is found on n2")
		k, _ := pair.Rows[env.ServiceN2][0].Get("k")
		v, _ := pair.Rows[env.ServiceN2][0].Get("v")
		require.Equal(t, "ok", k)
		require.Equal(t, "bad", v)

		repair := env.newTableRepairTask(env.ServiceN1, table, diffFile)
		require.NoError(t, repair.Run(false))
		require.Empty(t, repair.FailedOps)
		env.assertNoTableDiff(t, table)
		require.Equal(t, []string{"(ok,ok,a)", "(bad,ok,a)"}, rowTexts(t, env.N2Pool, table, "k"))
	})

	// planRepair diffs a table with the rows 'ok' and 'bad' on both nodes,
	// runs change, and then repairs with a plan: 'ok' takes the row of n1,
	// with the stale check of its target n2, and 'bad' takes the row of n2.
	// 'bad' = 'ok' is false for the enum, but true if both texts went
	// through the cast.
	planRepair := func(t *testing.T, name string, change func(table string)) string {
		table := typedKeyTable(t, env, name, "k "+mood+" PRIMARY KEY, w text", "k, w",
			[]string{"('ok', 'n1')", "('bad', 'n1')"},
			[]string{"('ok', 'n2')", "('bad', 'n2')"})
		plan := fmt.Sprintf(`
version: 1
tables:
  %s:
    default_action: { type: keep_n2 }
    rules:
      - name: ok_rows
        when: "n1.k = 'ok'"
        action: { type: keep_n1, allow_stale_repairs: false }
`, table)
		planPath := filepath.Join(t.TempDir(), "plan.yaml")
		require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o600))

		diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
		change(table)
		task := env.newTableRepairTask("", table, diffFile)
		task.RepairPlanPath = planPath
		require.NoError(t, task.Run(false))
		require.Empty(t, task.FailedOps)
		return table
	}

	t.Run("repair plan", func(t *testing.T) {
		table := planRepair(t, "ace_explicit_cast_plan", func(string) {})
		env.assertNoTableDiff(t, table)
		require.Equal(t, []string{"(ok,n1)", "(bad,n2)"}, rowTexts(t, env.N1Pool, table, "k"))
	})

	// The row 'ok' on n2 gets a newer commit after the diff. The stale check
	// must find it by its enum key and skip it; if the key went through the
	// cast, the check would find no row and the repair would overwrite it.
	t.Run("stale check", func(t *testing.T) {
		table := planRepair(t, "ace_explicit_cast_stale", func(table string) {
			time.Sleep(10 * time.Millisecond)
			tx, err := env.N2Pool.Begin(ctx)
			require.NoError(t, err)
			env.withRepairModeTx(t, ctx, tx, func() {
				_, err := tx.Exec(ctx, "UPDATE "+table+" SET w = 'n2 new' WHERE k = 'ok'") // nosemgrep
				require.NoError(t, err)
			})
			require.NoError(t, tx.Commit(ctx))
		})
		require.Equal(t, []string{"(ok,\"n2 new\")", "(bad,n2)"}, rowTexts(t, env.N2Pool, table, "k"))
		require.Equal(t, []string{"(ok,n1)", "(bad,n2)"}, rowTexts(t, env.N1Pool, table, "k"))
	})
}

// TestTextValuesAsKeys repairs tables whose key holds values that a Go
// conversion would change: numeric NaN and Infinity, timestamps with
// infinity, bytea, padded bpchar, boolean, inet and a composite key. The
// rows of n1 are on n2 with another value, and one row is on n1 only.
func TestTextValuesAsKeys(t *testing.T) {
	env := newSpockEnv()
	for _, tc := range []struct {
		name string
		typ  string
		keys []string
	}{
		{"numeric", "numeric", []string{"'1.50'", "'NaN'", "'Infinity'", "'-Infinity'", "'0.0'"}},
		{"timestamptz", "timestamptz", []string{"'infinity'", "'-infinity'", "'2026-03-29 01:30:00.000001+00'"}},
		{"bytea", "bytea", []string{`'\x00'`, `'\x'`, `'\xff00'`}},
		{"bpchar", "char(4)", []string{"'ab'", "''", "'a b'"}},
		{"boolean", "boolean", []string{"true", "false"}},
		{"inet", "inet", []string{"'10.0.0.1'", "'10.0.0.0/8'", "'::1'"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var n1Rows, n2Rows []string
			for i, k := range tc.keys {
				n1Rows = append(n1Rows, fmt.Sprintf("(%s, 'a')", k))
				if i > 0 {
					n2Rows = append(n2Rows, fmt.Sprintf("(%s, 'b')", k))
				}
			}
			table := typedKeyTable(t, env, "ace_text_key_"+tc.name, "k "+tc.typ+" PRIMARY KEY, v text", "k, v", n1Rows, n2Rows)
			env.repairTable(t, table, env.ServiceN1)
			env.assertNoTableDiff(t, table)
			require.Equal(t, rowTexts(t, env.N1Pool, table, "k"), rowTexts(t, env.N2Pool, table, "k"))
		})
	}

	t.Run("composite", func(t *testing.T) {
		table := typedKeyTable(t, env, "ace_text_key_composite", "a text, b inet, v text, PRIMARY KEY (a, b)", "a, b, v",
			[]string{`('x|y', '10.0.0.1', 'a')`, `('"q"', '::1', 'a')`, `('', '10.0.0.0/8', 'a')`},
			[]string{`('x|y', '10.0.0.1', 'b')`, `('', '10.0.0.0/8', 'b')`})
		env.repairTable(t, table, env.ServiceN1)
		env.assertNoTableDiff(t, table)
		require.Equal(t, rowTexts(t, env.N1Pool, table, "a, b"), rowTexts(t, env.N2Pool, table, "a, b"))
	})
}

// TestTextValuesFixNulls checks that --fix-nulls fills a NULL with the text
// of the other node: an empty string stays an empty string, and a numeric
// keeps its scale.
func TestTextValuesFixNulls(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := typedKeyTable(t, env, "ace_text_fix_nulls", "id int PRIMARY KEY, a text, b numeric", "id, a, b",
		[]string{"(1, NULL, 1.50)"}, []string{"(1, '', NULL)"})
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	task := env.newTableRepairTask("", table, diffFile)
	task.FixNulls = true
	require.NoError(t, task.Run(false))

	for _, pool := range env.pools() {
		var a *string
		var b string
		require.NoError(t, pool.QueryRow(ctx, "SELECT a, b::text FROM "+table+" WHERE id = 1").Scan(&a, &b)) // nosemgrep
		require.NotNil(t, a, "a is not NULL after the fix")
		require.Equal(t, "", *a)
		require.Equal(t, "1.50", b)
	}
}

// TestTextValuesRepairPlan checks a repair plan on text values. The keys of
// an override are the key text as the server prints it: "1.50" for a
// numeric key, "t" for a boolean. A when comparison uses the type of the
// column: 9.5 < 10 is true for a numeric, though "9.5" < "10" is false for
// text, and a timestamptz compares instants, not texts.
//
// In each table the override that must not match comes first, and the row
// matches no rule: so a match of equal values instead of equal texts gives
// a delete, and no match at all gives the default keep_n1. Only an exact
// match of the key text gives keep_n2.
func TestTextValuesRepairPlan(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := typedKeyTable(t, env, "ace_text_plan", "id numeric PRIMARY KEY, amount numeric, at timestamptz, v text",
		"id, amount, at, v",
		[]string{
			"(1.50, 100, '2026-01-01 06:00+00', 'n1')",
			"(2, 9.5, '2026-01-01 00:00+00', 'n1')",
			"(3, 100, '2026-01-01 05:00+00', 'n1')",
			"(4, 100, '2026-01-01 04:00+00', 'n1')",
		},
		[]string{
			"(1.50, 100, '2026-01-01 06:00+00', 'n2')",
			"(2, 9.5, '2026-01-01 00:00+00', 'n2')",
			"(3, 100, '2026-01-01 05:00+00', 'n2')",
			"(4, 100, '2026-01-01 04:00+00', 'n2')",
		})
	keyTable := typedKeyTable(t, env, "ace_text_plan_key", "at timestamptz, flag boolean, v text, PRIMARY KEY (at, flag)",
		"at, flag, v",
		[]string{"('2026-01-01 05:00+00', true, 'n1')", "('2026-01-01 05:00+00', false, 'n1')"},
		[]string{"('2026-01-01 05:00+00', true, 'n2')", "('2026-01-01 05:00+00', false, 'n2')"})

	// Rows that get keep_n2 take the value of n2 on n1; the others keep_n1.
	plan := fmt.Sprintf(`
version: 1
tables:
  %[1]s:
    default_action: { type: keep_n1 }
    rules:
      - name: small_amount
        when: "n1.amount < 10"
        action: { type: keep_n2 }
      - name: early
        when: "n1.at < '2026-01-01 10:00:00+05'"
        action: { type: keep_n2 }
    row_overrides:
      - pk: { id: 1.5 }
        action: { type: delete }
      - pk: { id: 1.50 }
        action: { type: keep_n2 }
  %[2]s:
    default_action: { type: keep_n1 }
    row_overrides:
      - pk: { at: "2026-01-01 05:00:00+00", flag: "true" }
        action: { type: delete }
      - pk: { at: "2026-01-01 05:00:00+00", flag: t }
        action: { type: keep_n2 }
`, table, keyTable)
	planPath := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o600))

	for _, tc := range []struct {
		table string
		order string
		want  []string
	}{
		// 1.50: the override with the key text "1.50"; "1.5" matches no row,
		// and 06:00 UTC matches no rule. 2: 9.5 < 10 as numbers. 3: 05:00 UTC is not before 10:00+05.
		// 4: 04:00 UTC is before it.
		{table, "id", []string{"1.50:n2", "2:n2", "3:n1", "4:n2"}},
		// The boolean key prints as "t", so "true" matches no row.
		{keyTable, "flag", []string{"false:n1", "true:n2"}},
	} {
		diffFile := env.runTableDiff(t, tc.table, []string{env.ServiceN1, env.ServiceN2})
		task := env.newTableRepairTask("", tc.table, diffFile)
		task.RepairPlanPath = planPath
		require.NoError(t, task.Run(false), "repair of %s", tc.table)
		env.assertNoTableDiff(t, tc.table)

		key := "id::text"
		if tc.table == keyTable {
			key = "flag::text"
		}
		rows, err := env.N1Pool.Query(ctx, fmt.Sprintf("SELECT %s || ':' || v FROM %s ORDER BY %s", key, tc.table, tc.order)) // nosemgrep
		require.NoError(t, err)
		var got []string
		for rows.Next() {
			var s string
			require.NoError(t, rows.Scan(&s))
			got = append(got, s)
		}
		rows.Close()
		require.NoError(t, rows.Err())
		require.Equal(t, tc.want, got, "rows of %s on n1 after the repair", tc.table)
	}
}

// TestTextValuesCustomTemplate checks the templates of a custom action. A
// template with a misspelled column stops the repair before any write, so
// that the column is not cleared on both nodes. A valid template and a YAML
// null are written.
func TestTextValuesCustomTemplate(t *testing.T) {
	env := newSpockEnv()
	table := typedKeyTable(t, env, "ace_custom_template", "id int PRIMARY KEY, email text, note text", "id, email, note",
		[]string{"(1, 'a@example.com', 'n1')"}, []string{"(1, 'b@example.com', 'n2')"})
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})

	run := func(customRow string) error {
		plan := fmt.Sprintf(`
version: 1
tables:
  %s:
    rules:
      - name: merge
        diff_type: [row_mismatch]
        action:
          type: custom
          custom_row: { %s }
          helpers: { coalesce_priority: [n1] }
`, table, customRow)
		planPath := filepath.Join(t.TempDir(), "plan.yaml")
		require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o600))
		task := env.newTableRepairTask("", table, diffFile)
		task.RepairPlanPath = planPath
		return task.Run(false)
	}

	err := run(`email: "{{n1.emali}}"`)
	require.ErrorContains(t, err, `rule merge: custom_row.email: template {{n1.emali}} names column "emali"`)
	require.Equal(t, []string{"(1,a@example.com,n1)"}, rowTexts(t, env.N1Pool, table, "id"))
	require.Equal(t, []string{"(1,b@example.com,n2)"}, rowTexts(t, env.N2Pool, table, "id"))

	require.NoError(t, run(`email: "{{n2.email}}", note: null`))
	for _, pool := range env.pools() {
		require.Equal(t, []string{"(1,b@example.com,)"}, rowTexts(t, pool, table, "id"))
	}
}

// TestTextValuesPickFreshestOneNode repairs a row that only n2 has, with a
// custom action of pick_freshest alone. The key of the row is NULL and the
// tie side is n1, where the row is missing. The row of n2 must win: n1 gets
// the full row, and n2 keeps its columns.
func TestTextValuesPickFreshestOneNode(t *testing.T) {
	env := newSpockEnv()
	table := typedKeyTable(t, env, "ace_freshest_one_node", "id int PRIMARY KEY, email text, updated_at timestamp",
		"id, email, updated_at", nil, []string{"(1, 'b@example.com', NULL)"})
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})

	plan := fmt.Sprintf(`
version: 1
tables:
  %s:
    rules:
      - name: freshest
        diff_type: [missing_on_n1]
        action:
          type: custom
          helpers: { pick_freshest: { key: updated_at, tie: n1 } }
`, table)
	planPath := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o600))
	task := env.newTableRepairTask("", table, diffFile)
	task.RepairPlanPath = planPath
	require.NoError(t, task.Run(false))
	require.Empty(t, task.FailedOps)
	for _, pool := range env.pools() {
		require.Equal(t, []string{"(1,b@example.com,)"}, rowTexts(t, pool, table, "id"))
	}
}

// TestCastTypesHaveSchema checks that a cast type outside pg_catalog has its
// schema in the name even when the session that reads the catalog sees the
// type through its search_path, so that a session with another search_path
// resolves the same name. A pg_catalog type keeps its plain name, and an
// array of a user type is named by its array type.
func TestCastTypesHaveSchema(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	createTypesOnBoth(t, env, []string{
		`CREATE SCHEMA "ace cast s"`,
		`CREATE TYPE "ace cast s".mood AS ENUM ('ok', 'bad')`,
		`CREATE DOMAIN "ace cast s".mood_dom AS "ace cast s".mood`,
	}, []string{
		`DROP SCHEMA IF EXISTS "ace cast s" CASCADE`,
	})
	table := typedKeyTable(t, env, "ace_cast_schema",
		`id int PRIMARY KEY, m "ace cast s".mood, ms "ace cast s".mood[], md "ace cast s".mood_dom, v varchar(5), c char(3)`,
		"id", nil, nil)
	name := strings.TrimPrefix(table, testSchema+".")

	conn, err := env.N1Pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()
	_, err = conn.Exec(ctx, `SET search_path = "ace cast s", public`)
	require.NoError(t, err)
	got, err := queries.GetCastTypes(ctx, conn, testSchema, name)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, `RESET search_path`)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"id": "integer",
		"m":  `"ace cast s".mood`,
		"ms": `"ace cast s"._mood`,
		"md": `"ace cast s".mood`,
		"v":  "character varying",
		"c":  "bpchar",
	}, got)

	// A session without the schema in its search_path reads the names.
	var out string
	require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf("SELECT format('%%s|%%s', $1::%s, $2::%s)", got["m"], got["ms"]), // nosemgrep
		"ok", "{ok,bad}").Scan(&out))
	require.Equal(t, "ok|{ok,bad}", out)
}
