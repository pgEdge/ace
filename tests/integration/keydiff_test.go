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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/keydiff"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// keydiffExec runs sql on pool in repair mode, so that nothing is replicated
// and each node gets its own rows.
func keydiffExec(t *testing.T, ctx context.Context, env *testEnv, pool *pgxpool.Pool, sqls ...string) {
	t.Helper()
	env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
		for _, sql := range sqls {
			_, err := conn.Exec(ctx, sql) // nosemgrep
			require.NoError(t, err, "exec %q", sql)
		}
	})
}

// keydiffTable reads the columns, the key and the column types of the table
// on both nodes.
func keydiffTable(t *testing.T, ctx context.Context, env *testEnv, table string) (keydiff.Table, map[string]string, map[string]string) {
	t.Helper()
	cols, types1, err := queries.GetColumns(ctx, env.N1Pool, testSchema, table)
	require.NoError(t, err)
	key, err := queries.GetPrimaryKey(ctx, env.N1Pool, testSchema, table)
	require.NoError(t, err)
	types2, err := queries.GetColumnTypes(ctx, env.N2Pool, testSchema, table)
	require.NoError(t, err)
	return keydiff.Table{Schema: testSchema, Name: table, Key: key, Cols: cols, HashTypes: types1}, types1, types2
}

// keydiffChanged is a changed row that a comparison found.
type keydiffChanged struct {
	Row1, Row2 types.OrderedMap
}

// keydiffRows holds the rows that a comparison gave to its sink, by kind,
// and the counts that it returned.
type keydiffRows struct {
	keydiff.Result
	Only1   []types.OrderedMap
	Only2   []types.OrderedMap
	Changed []keydiffChanged
}

// compareRows runs keydiff.Compare and keeps every row it reports. It also
// checks that the counts of the result agree with the rows.
func compareRows(ctx context.Context, tbl keydiff.Table, block keydiff.Block, s1, s2 keydiff.Side, o keydiff.Options) (keydiffRows, error) {
	var out keydiffRows
	res, err := keydiff.Compare(ctx, tbl, block, s1, s2, o, func(entries []keydiff.Entry) error {
		for _, e := range entries {
			switch e.Kind {
			case keydiff.KindOnly1:
				out.Only1 = append(out.Only1, e.Row1)
			case keydiff.KindOnly2:
				out.Only2 = append(out.Only2, e.Row2)
			case keydiff.KindChanged:
				out.Changed = append(out.Changed, keydiffChanged{Row1: e.Row1, Row2: e.Row2})
			}
		}
		return nil
	})
	out.Result = res
	if err == nil && (res.Only1 != len(out.Only1) || res.Only2 != len(out.Only2) || res.Changed != len(out.Changed)) {
		return out, fmt.Errorf("the counts %d/%d/%d do not match the rows %d/%d/%d",
			res.Only1, res.Only2, res.Changed, len(out.Only1), len(out.Only2), len(out.Changed))
	}
	return out, err
}

func keydiffCompare(t *testing.T, ctx context.Context, env *testEnv, table string, o keydiff.Options) (keydiffRows, error) {
	t.Helper()
	tbl, types1, types2 := keydiffTable(t, ctx, env, table)
	return compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"},
		keydiff.Side{Name: env.ServiceN1, Conn: env.N1Pool, ColTypes: types1},
		keydiff.Side{Name: env.ServiceN2, Conn: env.N2Pool, ColTypes: types2},
		o)
}

// columnValues returns the values of the column col in rows, as sorted text.
func columnValues(t *testing.T, rows []types.OrderedMap, col string) []string {
	t.Helper()
	var out []string
	for _, r := range rows {
		v, ok := r.Get(col)
		require.True(t, ok, "row has no column %s", col)
		out = append(out, fmt.Sprint(v))
	}
	sort.Strings(out)
	return out
}

// dropOnBoth drops objects on both nodes now and again when the test ends.
// Each entry is the text after DROP, such as "TABLE IF EXISTS s.t".
func dropOnBoth(t *testing.T, ctx context.Context, env *testEnv, objects ...string) {
	t.Helper()
	drop := func() {
		for _, pool := range env.pools() {
			for _, o := range objects {
				_, _ = pool.Exec(ctx, "DROP "+o+" CASCADE") // nosemgrep
			}
		}
	}
	drop()
	t.Cleanup(drop)
}

// TestKeyDiffKeyTypes checks a changed row, a row on n1 only and a row on n2
// only, for keys of many types. The key texts go to the client and back to
// the server, so each type must survive this.
func TestKeyDiffKeyTypes(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	cases := []struct {
		name    string
		keyDef  string
		prepare string
		keys    [4]string // SQL literals: a, b, c on both nodes, d on n2 only
	}{
		{"int", "integer", "", [4]string{"1", "2", "3", "4"}},
		{"bigint", "bigint", "", [4]string{"9000000000001", "9000000000002", "9000000000003", "-9000000000004"}},
		{"numeric", "numeric(12,2)", "", [4]string{"1.50", "2.5", "3.00", "4.10"}},
		{"text_c", `text COLLATE "C"`, "", [4]string{`'a,b'`, `'b"q'`, `'(c)'`, `'d d'`}},
		{"varchar", "varchar(20)", "", [4]string{`'Привет'`, `'日本'`, `'x\y'`, `''`}},
		{"bpchar", "char(5)", "", [4]string{`'ab'`, `'cd '`, `'e'`, `'f'`}},
		{"uuid", "uuid", "", [4]string{`'00000000-0000-0000-0000-000000000001'`, `'00000000-0000-0000-0000-000000000002'`, `'00000000-0000-0000-0000-000000000003'`, `'00000000-0000-0000-0000-000000000004'`}},
		{"timestamptz", "timestamptz", "", [4]string{`'2024-01-01 10:00:00.123456+03'`, `'2024-06-01 00:00:00+00'`, `'1999-12-31 23:59:59-08'`, `'2030-01-01 00:00:00+00'`}},
		{"date", "date", "", [4]string{`'2024-01-01'`, `'2024-02-29'`, `'1970-01-01'`, `'2100-12-31'`}},
		{"bytea", "bytea", "", [4]string{`'\x01'`, `'\x0203'`, `'\x'`, `'\xff00'`}},
		{"enum", "keydiff_mood", "CREATE TYPE keydiff_mood AS ENUM ('a', 'b', 'c', 'd')", [4]string{`'a'`, `'b'`, `'c'`, `'d'`}},
		{"bool", "boolean", "", [4]string{"true", "false", "", ""}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := "keydiff_" + tc.name
			ident := pgx.Identifier{testSchema, table}.Sanitize()
			objects := []string{"TABLE IF EXISTS " + ident}
			if tc.prepare != "" {
				objects = append(objects, "TYPE IF EXISTS keydiff_mood")
			}
			dropOnBoth(t, ctx, env, objects...)
			for _, pool := range env.pools() {
				if tc.prepare != "" {
					keydiffExec(t, ctx, env, pool, tc.prepare)
				}
				keydiffExec(t, ctx, env, pool, fmt.Sprintf("CREATE TABLE %s (k %s PRIMARY KEY, v text)", ident, tc.keyDef))
			}

			if tc.name == "bool" {
				// Only two values: one changed row and one row on n1 only.
				keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("INSERT INTO %s VALUES (true, 'a'), (false, 'b')", ident))
				keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("INSERT INTO %s VALUES (true, 'a2')", ident))
				res, err := keydiffCompare(t, ctx, env, table, keydiff.Options{})
				require.NoError(t, err)
				require.Len(t, res.Changed, 1)
				require.Equal(t, []string{"b"}, columnValues(t, res.Only1, "v"))
				require.Empty(t, res.Only2)
				return
			}

			k := tc.keys
			keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("INSERT INTO %s VALUES (%s, 'a'), (%s, 'b'), (%s, 'c')", ident, k[0], k[1], k[2]))
			keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("INSERT INTO %s VALUES (%s, 'a'), (%s, 'b2'), (%s, 'd')", ident, k[0], k[1], k[3]))

			res, err := keydiffCompare(t, ctx, env, table, keydiff.Options{})
			require.NoError(t, err)
			require.Equal(t, []string{"c"}, columnValues(t, res.Only1, "v"), "row on n1 only")
			require.Equal(t, []string{"d"}, columnValues(t, res.Only2, "v"), "row on n2 only")
			require.Len(t, res.Changed, 1, "changed row")
			v1, _ := res.Changed[0].Row1.Get("v")
			v2, _ := res.Changed[0].Row2.Get("v")
			require.Equal(t, "b", v1)
			require.Equal(t, "b2", v2)
			for _, r := range append(append(res.Only1, res.Only2...), res.Changed[0].Row1, res.Changed[0].Row2) {
				_, ok := r.Get("commit_ts")
				require.True(t, ok, "row has no commit_ts")
				_, ok = r.Get("node_origin")
				require.True(t, ok, "row has no node_origin")
			}
		})
	}
}

// TestKeyDiffNumericScale checks that a numeric key stored as 1.50 on one
// node and 1.5 on the other is one row.
func TestKeyDiffNumericScale(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "keydiff_numeric_scale"
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool, fmt.Sprintf("CREATE TABLE %s (k numeric PRIMARY KEY, v text)", ident))
	}
	keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("INSERT INTO %s VALUES (1.50, 'a'), (2.50, 'b')", ident))
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("INSERT INTO %s VALUES (1.5, 'a'), (2.5, 'b2')", ident))

	res, err := keydiffCompare(t, ctx, env, table, keydiff.Options{})
	require.NoError(t, err)
	require.Empty(t, res.Only1)
	require.Empty(t, res.Only2)
	require.Len(t, res.Changed, 1)
	v1, _ := res.Changed[0].Row1.Get("v")
	require.Equal(t, "b", v1)
}

// TestKeyDiffEqualKeysPrintedDifferently checks a key that the server holds
// as equal on both nodes but prints in a different way: -0 and 0 in a float
// column. The key texts differ, and each text finds only the row that prints
// it, so the comparison reports a row on each node only.
func TestKeyDiffEqualKeysPrintedDifferently(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "keydiff_float_zero"
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool, fmt.Sprintf("CREATE TABLE %s (k float8 PRIMARY KEY, v text)", ident))
	}
	keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("INSERT INTO %s VALUES ('-0', 'a'), (1, 'b')", ident))
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("INSERT INTO %s VALUES ('0', 'a'), (1, 'b')", ident))

	for _, batch := range []int{0, 1} {
		res, err := keydiffCompare(t, ctx, env, table, keydiff.Options{BatchSize: batch})
		require.NoError(t, err)
		require.Equal(t, 2, res.DiffKeys, "-0 and 0 are two key texts")
		require.Equal(t, []string{"a"}, columnValues(t, res.Only1, "v"), "batch size %d", batch)
		require.Equal(t, []string{"a"}, columnValues(t, res.Only2, "v"), "batch size %d", batch)
		require.Empty(t, res.Changed, "batch size %d", batch)
	}
}

// TestKeyDiffCompositeKey checks a key of two columns, with a block that
// covers only a part of the table.
func TestKeyDiffCompositeKey(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "keydiff_composite"
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (a int, b text, v text, PRIMARY KEY (a, b))", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g / 10, 'k' || (g %% 10), 'v' || g FROM generate_series(1, 1000) g", ident))
	}
	keydiffExec(t, ctx, env, env.N2Pool,
		fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE a = 20 AND b = 'k5'", ident),
		fmt.Sprintf("DELETE FROM %s WHERE a = 30 AND b = 'k1'", ident),
		fmt.Sprintf("UPDATE %s SET v = 'outside' WHERE a = 90", ident))

	tbl, types1, types2 := keydiffTable(t, ctx, env, table)
	block := keydiff.Block{Where: "ROW(a, b) >= ROW($1, $2) AND ROW(a, b) < ROW($3, $4)", Args: []any{10, "k0", 50, "k0"}}
	res, err := compareRows(ctx, tbl, block,
		keydiff.Side{Name: env.ServiceN1, Conn: env.N1Pool, ColTypes: types1},
		keydiff.Side{Name: env.ServiceN2, Conn: env.N2Pool, ColTypes: types2},
		keydiff.Options{})
	require.NoError(t, err)
	require.Len(t, res.Changed, 1)
	require.Equal(t, []string{"v301"}, columnValues(t, res.Only1, "v"))
	require.Empty(t, res.Only2)
	require.Equal(t, 2, res.DiffKeys)
}

// TestKeyDiffBuildSide checks that the node with fewer rows goes into the
// map, and that an empty build side gives every probe row.
func TestKeyDiffBuildSide(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "keydiff_build_side"
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", ident))
	}
	keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("INSERT INTO %s SELECT g, 'v' FROM generate_series(1, 100) g", ident))
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("INSERT INTO %s SELECT g, 'v' FROM generate_series(1, 90) g", ident))

	tbl, types1, types2 := keydiffTable(t, ctx, env, table)
	s1 := keydiff.Side{Name: env.ServiceN1, Conn: env.N1Pool, ColTypes: types1, Rows: 100, RowsKnown: true}
	s2 := keydiff.Side{Name: env.ServiceN2, Conn: env.N2Pool, ColTypes: types2, Rows: 90, RowsKnown: true}
	res, err := compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"}, s1, s2, keydiff.Options{})
	require.NoError(t, err)
	require.Equal(t, env.ServiceN2, res.BuildNode)
	require.Len(t, res.Only1, 10)

	keydiffExec(t, ctx, env, env.N2Pool, "TRUNCATE "+ident)
	s2.Rows = 0
	res, err = compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"}, s1, s2, keydiff.Options{})
	require.NoError(t, err)
	require.Equal(t, env.ServiceN2, res.BuildNode)
	require.Len(t, res.Only1, 100)
}

// TestKeyDiffInheritance checks a parent and a child with rows of the same
// key: the primary key of the parent does not cover the child.
func TestKeyDiffInheritance(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	parent := "keydiff_inh_parent"
	pIdent := pgx.Identifier{testSchema, parent}.Sanitize()
	cIdent := pgx.Identifier{testSchema, "keydiff_inh_child"}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+pIdent)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v text)", pIdent),
			fmt.Sprintf("CREATE TABLE %s () INHERITS (%s)", cIdent, pIdent),
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'p1'), (2, 'p2')", pIdent),
			fmt.Sprintf("INSERT INTO %s VALUES (1, 'c1')", cIdent))
	}
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("DELETE FROM ONLY %s WHERE id = 1", cIdent))

	tbl, types1, types2 := keydiffTable(t, ctx, env, parent)
	for _, scan := range []bool{false, true} {
		for _, batch := range []int{0, 1, 2} {
			tbl.ScanChildren = scan
			res, err := compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"},
				keydiff.Side{Name: env.ServiceN1, Conn: env.N1Pool, ColTypes: types1},
				keydiff.Side{Name: env.ServiceN2, Conn: env.N2Pool, ColTypes: types2},
				keydiff.Options{BatchSize: batch})
			require.NoError(t, err)
			require.Equal(t, []string{"c1"}, columnValues(t, res.Only1, "v"), "scan %v, batch %d", scan, batch)
			require.Empty(t, res.Only2)
			require.Empty(t, res.Changed)
		}
	}
}

// TestKeyDiffReadFilter checks that the row read applies the filter of the
// block: a row that matches the filter on n1 only exists on n1 only, even if
// n2 has a row with the same key.
func TestKeyDiffReadFilter(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "keydiff_read_filter"
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, status text, v text)", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, 'open', 'v' || g FROM generate_series(1, 100) g", ident))
	}
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("UPDATE %s SET status = 'closed' WHERE id = 7", ident))

	tbl, types1, types2 := keydiffTable(t, ctx, env, table)
	filter := `"status" = 'open'`
	res, err := compareRows(ctx, tbl, keydiff.Block{Where: filter, ReadFilter: filter},
		keydiff.Side{Name: env.ServiceN1, Conn: env.N1Pool, ColTypes: types1},
		keydiff.Side{Name: env.ServiceN2, Conn: env.N2Pool, ColTypes: types2},
		keydiff.Options{})
	require.NoError(t, err)
	require.Equal(t, []string{"v7"}, columnValues(t, res.Only1, "v"))
	require.Empty(t, res.Only2)
	require.Empty(t, res.Changed, "without the filter in the row read, row 7 would be a changed row")
}

// TestKeyDiffMemoryLimit checks the memory limit before and during the
// build.
func TestKeyDiffMemoryLimit(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "keydiff_memory"
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (k text PRIMARY KEY, v int)", ident),
			// A long key that does not compress, so that avg_width in the
			// statistics is the real size.
			fmt.Sprintf("INSERT INTO %s SELECT (SELECT string_agg(md5(g::text || '-' || i::text), '') "+
				"FROM generate_series(1, 40) i), g FROM generate_series(1, 1000) g", ident),
			"ANALYZE "+ident)
	}
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("UPDATE %s SET v = -v WHERE v = 1", ident))

	tbl, types1, types2 := keydiffTable(t, ctx, env, table)
	o := keydiff.Options{MaxMemory: 1 << 20}

	// Before the build: from the row count and the statistics.
	width, err := keydiff.KeyWidth(ctx, env.N1Pool, testSchema, table, tbl.Key)
	require.NoError(t, err)
	require.Greater(t, width, int64(0))
	s1 := keydiff.Side{Name: env.ServiceN1, Conn: env.N1Pool, ColTypes: types1, Rows: 1000, RowsKnown: true, KeyWidth: width}
	s2 := keydiff.Side{Name: env.ServiceN2, Conn: env.N2Pool, ColTypes: types2, Rows: 1000, RowsKnown: true, KeyWidth: width}
	_, err = compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"}, s1, s2, o)
	var me *keydiff.MemoryLimitError
	require.True(t, errors.As(err, &me), "got %v", err)
	require.True(t, me.Estimated)
	require.Contains(t, err.Error(), env.ServiceN1)

	// During the build: no statistics.
	s1.KeyWidth, s2.KeyWidth = 0, 0
	_, err = compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"}, s1, s2, o)
	require.True(t, errors.As(err, &me), "got %v", err)
	require.False(t, me.Estimated)

	// With enough memory the comparison works.
	o.MaxMemory = 64 << 20
	res, err := compareRows(ctx, tbl, keydiff.Block{Where: "TRUE"}, s1, s2, o)
	require.NoError(t, err)
	require.Len(t, res.Changed, 1)
}

// planScans reports whether the plan, or any plan under it, scans one of the
// relations sequentially, and whether it reads one of them through an index.
func planScans(plan map[string]any, rels map[string]bool) (seq, index bool) {
	nodeType, _ := plan["Node Type"].(string)
	rel, _ := plan["Relation Name"].(string)
	if rels[rel] {
		switch nodeType {
		case "Seq Scan":
			seq = true
		case "Index Scan", "Index Only Scan", "Bitmap Heap Scan":
			index = true
		}
	}
	if subs, ok := plan["Plans"].([]any); ok {
		for _, s := range subs {
			if m, ok := s.(map[string]any); ok {
				ss, ii := planScans(m, rels)
				seq = seq || ss
				index = index || ii
			}
		}
	}
	return seq, index
}

// seqScanPerKey reports whether a Seq Scan runs on the inner side of a
// nested loop, that is once for every key.
func seqScanPerKey(plan map[string]any, inner bool) bool {
	nodeType, _ := plan["Node Type"].(string)
	if inner && nodeType == "Seq Scan" {
		return true
	}
	subs, _ := plan["Plans"].([]any)
	for _, sub := range subs {
		m, ok := sub.(map[string]any)
		if !ok {
			continue
		}
		rel, _ := m["Parent Relationship"].(string)
		if seqScanPerKey(m, inner || (nodeType == "Nested Loop" && rel == "Inner")) {
			return true
		}
	}
	return false
}

// TestKeyDiffReadUsesIndex checks that the row read by key never scans the
// whole table, for a plain table, a text key with a non-default collation
// and a partitioned table.
func TestKeyDiffReadUsesIndex(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	const rows = 1000000
	const keys = 2000

	cases := []struct {
		name  string
		setup []string
		rels  []string
		key   func(i int) string
		// scanChildren: the table has a child without an index, and the
		// read must not scan it once per key.
		scanChildren bool
	}{
		{
			name: "plain",
			setup: []string{
				"CREATE TABLE %[1]s (id bigint PRIMARY KEY, v text)",
				"INSERT INTO %[1]s SELECT g, md5(g::text) FROM generate_series(1, %[2]d) g",
			},
			key: func(i int) string { return strconv.Itoa(i * 401) },
		},
		{
			name: "collation",
			setup: []string{
				`CREATE TABLE %[1]s (id text COLLATE "C" PRIMARY KEY, v text)`,
				"INSERT INTO %[1]s SELECT 'k' || g, md5(g::text) FROM generate_series(1, %[2]d) g",
			},
			key: func(i int) string { return "k" + strconv.Itoa(i*401) },
		},
		{
			// The child has no index on the key: without the lookup fence
			// the planner reads it once for all keys.
			name: "inheritance",
			setup: []string{
				"CREATE TABLE %[1]s (id bigint PRIMARY KEY, v text)",
				"CREATE TABLE %[3]s_c () INHERITS (%[1]s)",
				"INSERT INTO %[3]s_c SELECT g, md5(g::text) FROM generate_series(1, %[2]d) g",
				"ANALYZE %[3]s_c",
			},
			key:          func(i int) string { return strconv.Itoa(i * 401) },
			scanChildren: true,
		},
		{
			name: "partitioned",
			setup: []string{
				"CREATE TABLE %[1]s (id bigint PRIMARY KEY, v text) PARTITION BY RANGE (id)",
				"CREATE TABLE %[3]s_p1 PARTITION OF %[1]s FOR VALUES FROM (MINVALUE) TO (250000)",
				"CREATE TABLE %[3]s_p2 PARTITION OF %[1]s FOR VALUES FROM (250000) TO (500000)",
				"CREATE TABLE %[3]s_p3 PARTITION OF %[1]s FOR VALUES FROM (500000) TO (750000)",
				"CREATE TABLE %[3]s_p4 PARTITION OF %[1]s FOR VALUES FROM (750000) TO (MAXVALUE)",
				"INSERT INTO %[1]s SELECT g, md5(g::text) FROM generate_series(1, %[2]d) g",
			},
			key: func(i int) string { return strconv.Itoa(i * 401) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			table := "keydiff_plan_" + tc.name
			ident := pgx.Identifier{testSchema, table}.Sanitize()
			dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
			var sqls []string
			for _, s := range tc.setup {
				sqls = append(sqls, fmt.Sprintf(s, ident, rows, testSchema+"."+table))
			}
			sqls = append(sqls, "ANALYZE "+ident)
			keydiffExec(t, ctx, env, env.N1Pool, sqls...)

			// The table exists on n1 only.
			cols, types1, err := queries.GetColumns(ctx, env.N1Pool, testSchema, table)
			require.NoError(t, err)
			key, err := queries.GetPrimaryKey(ctx, env.N1Pool, testSchema, table)
			require.NoError(t, err)
			tbl := keydiff.Table{Schema: testSchema, Name: table, Key: key, Cols: cols, HashTypes: types1,
				ScanChildren: tc.scanChildren}
			q, err := tbl.ReadQuery(types1, "")
			require.NoError(t, err)

			ids := make([]string, keys)
			for i := range ids {
				ids[i] = tc.key(i + 1)
			}
			var planJSON []byte
			require.NoError(t, env.N1Pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+q, ids).Scan(&planJSON)) // nosemgrep
			var plans []map[string]any
			require.NoError(t, json.Unmarshal(planJSON, &plans))
			require.NotEmpty(t, plans)

			rels := map[string]bool{table: true}
			for _, p := range []string{"_p1", "_p2", "_p3", "_p4"} {
				rels[table+p] = true
			}
			if tc.scanChildren {
				require.False(t, seqScanPerKey(plans[0]["Plan"].(map[string]any), false),
					"the row read scans the child once per key: %s", planJSON)
			} else {
				seq, index := planScans(plans[0]["Plan"].(map[string]any), rels)
				require.False(t, seq, "the row read scans the table sequentially: %s", planJSON)
				require.True(t, index, "the row read does not use an index: %s", planJSON)
			}

			// The read must also return the rows.
			rowsRead, err := env.N1Pool.Query(ctx, q, ids) // nosemgrep
			require.NoError(t, err)
			n := 0
			for rowsRead.Next() {
				n++
			}
			rowsRead.Close()
			require.NoError(t, rowsRead.Err())
			require.Equal(t, keys, n)
		})
	}
}
