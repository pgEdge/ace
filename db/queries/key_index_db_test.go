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

package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// planNode is the part of an EXPLAIN (FORMAT JSON) node that the test
// checks.
type planNode struct {
	NodeType     string     `json:"Node Type"`
	RelationName string     `json:"Relation Name"`
	IndexName    string     `json:"Index Name"`
	IndexCond    string     `json:"Index Cond"`
	Filter       string     `json:"Filter"`
	Plans        []planNode `json:"Plans"`
}

// keyIndexCheck checks a plan: the table is read only through its primary
// key index, the index condition holds both bounds of the key range on all
// key columns, and no node of the table filters rows.
func keyIndexCheck(plan []planNode, table, index string, key []string) error {
	var (
		found bool
		walk  func(n planNode) error
	)
	walk = func(n planNode) error {
		if n.RelationName == table && n.Filter != "" {
			return fmt.Errorf("%s of %s filters rows: %s", n.NodeType, table, n.Filter)
		}
		if n.NodeType == "Seq Scan" && n.RelationName == table {
			return fmt.Errorf("a sequential scan of %s", table)
		}
		if n.IndexName == index {
			if n.Filter != "" {
				return fmt.Errorf("%s on %s filters rows: %s", n.NodeType, index, n.Filter)
			}
			for _, op := range []string{">", "<"} {
				if !strings.Contains(n.IndexCond, op) {
					return fmt.Errorf("index condition has no %q bound: %s", op, n.IndexCond)
				}
			}
			if len(key) > 1 {
				row := "ROW(" + strings.Join(key, ", ") + ")"
				if strings.Count(n.IndexCond, row) < 2 {
					return fmt.Errorf("index condition does not hold %s in both bounds: %s", row, n.IndexCond)
				}
			}
			found = true
		}
		for _, c := range n.Plans {
			if err := walk(c); err != nil {
				return err
			}
		}
		return nil
	}
	for _, p := range plan {
		if err := walk(p); err != nil {
			return err
		}
	}
	if !found {
		return fmt.Errorf("no scan of index %s", index)
	}
	return nil
}

// explainGeneric returns the generic plan of sql with args, as ACE runs it:
// a prepared statement with parameters.
func explainGeneric(t *testing.T, ctx context.Context, conn *pgxpool.Conn, sql string, args []any) []planNode {
	t.Helper()
	for _, set := range []string{
		"SET plan_cache_mode = force_generic_plan",
		// The test tables are small, so a scan of the whole table can cost
		// less than the index. The check is about the index condition, not
		// about the cost.
		"SET enable_seqscan = off",
	} {
		if _, err := conn.Exec(ctx, set); err != nil {
			t.Fatal(err)
		}
	}
	types := make([]string, len(args))
	for i := range args {
		types[i] = "text"
		if _, ok := args[i].(string); !ok {
			types[i] = "bigint"
		}
	}
	prep := "PREPARE ace_plan_test"
	if len(types) > 0 {
		prep += "(" + strings.Join(types, ", ") + ")"
	}
	if _, err := conn.Exec(ctx, prep+" AS "+sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	defer func() {
		if _, err := conn.Exec(ctx, "DEALLOCATE ace_plan_test"); err != nil {
			t.Errorf("DEALLOCATE ace_plan_test: %v", err)
		}
	}()
	lits := make([]string, len(args))
	for i, a := range args {
		if s, ok := a.(string); ok {
			lits[i] = "'" + strings.ReplaceAll(s, "'", "''") + "'"
		} else {
			lits[i] = fmt.Sprint(a)
		}
	}
	exec := "EXPLAIN (FORMAT JSON) EXECUTE ace_plan_test"
	if len(lits) > 0 {
		exec += "(" + strings.Join(lits, ", ") + ")"
	}
	var raw []byte
	if err := conn.QueryRow(ctx, exec).Scan(&raw); err != nil {
		t.Fatalf("%s: %v", exec, err)
	}
	var out []struct {
		Plan planNode `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	plans := make([]planNode, len(out))
	for i, o := range out {
		plans[i] = o.Plan
	}
	return plans
}

// Every query that takes a key range as $n::type must read the range
// through the primary key index with the whole bound in the index
// condition: the cast types are the types of the key columns, and the
// parameters have no COLLATE, so the comparison takes the collation of the
// column and of its index.
func TestKeyRangeUsesPrimaryKeyIndex(t *testing.T) {
	pool := rangesTestPool(t)
	ctx := context.Background()

	// Without a linguistic collation only the k_coll cases are skipped.
	coll := linguisticCollation(t, pool)
	setup := []string{
		`CREATE TYPE ace_ranges_test.color AS ENUM ('red', 'green', 'blue')`,
		`CREATE DOMAIN ace_ranges_test.code AS varchar(20)`,
		`CREATE TABLE ace_ranges_test.k_enum (c ace_ranges_test.color, n int, v text, PRIMARY KEY (c, n))`,
		`INSERT INTO ace_ranges_test.k_enum SELECT (ARRAY['red','green','blue'])[1 + g % 3]::ace_ranges_test.color, g, 'x' FROM generate_series(1, 20000) g`,
		`CREATE TABLE ace_ranges_test.k_dom (k ace_ranges_test.code PRIMARY KEY, v text)`,
		`INSERT INTO ace_ranges_test.k_dom SELECT 'c' || g, 'x' FROM generate_series(1, 20000) g`,
		`ANALYZE ace_ranges_test.k_enum, ace_ranges_test.k_dom`,
	}
	if coll != "" {
		setup = append(setup,
			fmt.Sprintf(`CREATE TABLE ace_ranges_test.k_coll (a int, b text COLLATE %s, v text, PRIMARY KEY (a, b))`, coll),
			`INSERT INTO ace_ranges_test.k_coll SELECT g % 100, md5(g::text), 'x' FROM generate_series(1, 20000) g`,
			`ANALYZE ace_ranges_test.k_coll`)
	}
	for _, sql := range setup {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()

	tables := []struct {
		table        string
		key          []string
		lower, upper []string
	}{
		{"k_coll", []string{"a", "b"}, []string{"10", "a"}, []string{"11", "f"}},
		{"k_enum", []string{"c", "n"}, []string{"green", "100"}, []string{"green", "500"}},
		{"k_dom", []string{"k"}, []string{"c100"}, []string{"c1000"}},
	}
	for _, tc := range tables {
		if tc.table == "k_coll" && coll == "" {
			t.Log("no linguistic collation on this server: k_coll cases skipped")
			continue
		}
		castTypes, err := GetCastTypes(ctx, conn, "ace_ranges_test", tc.table)
		if err != nil {
			t.Fatal(err)
		}
		index := tc.table + "_pkey"
		ranges := map[string]KeyRange{
			"half-open":                {Lower: tc.lower, Upper: tc.upper},
			"lower open, upper closed": {Lower: tc.lower, Upper: tc.upper, LowerOpen: true, UpperClosed: true},
		}
		for name, r := range ranges {
			where, args, err := BlockWhere(tc.key, castTypes, r, "")
			if err != nil {
				t.Fatal(err)
			}
			queriesOf := map[string]string{
				"count": fmt.Sprintf("SELECT count(*) FROM ace_ranges_test.%s WHERE %s", tc.table, where),
			}
			for _, mode := range []string{"TD_BLOCK_HASH", "MTREE_LEAF_HASH"} {
				sql, err := BlockHashSQL("ace_ranges_test", tc.table, mode, where, append(append([]string{}, tc.key...), "v"), HashFormBytes)
				if err != nil {
					t.Fatal(err)
				}
				queriesOf[mode] = sql
			}
			for q, sql := range queriesOf {
				t.Run(tc.table+"/"+name+"/"+q, func(t *testing.T) {
					plan := explainGeneric(t, ctx, conn, sql, args)
					if err := keyIndexCheck(plan, tc.table, index, tc.key); err != nil {
						t.Errorf("%v\n%s", err, sql)
					}
				})
			}
		}
	}

	// The check must see a bound that is only partly in the index
	// condition: with a COLLATE other than the one of the index on the
	// parameters, only the leading column stays in it.
	t.Run("negative control", func(t *testing.T) {
		if coll == "" {
			t.Skip("no linguistic collation on this server")
		}
		sql := `SELECT count(*) FROM ace_ranges_test.k_coll
			WHERE ROW("a", "b") > ROW($1::integer, $2::text COLLATE "C")
			AND ROW("a", "b") <= ROW($3::integer, $4::text COLLATE "C")`
		plan := explainGeneric(t, ctx, conn, sql, []any{"10", "a", "11", "f"})
		if err := keyIndexCheck(plan, "k_coll", "k_coll_pkey", []string{"a", "b"}); err == nil {
			t.Error("the check accepted a plan with a partial index condition")
		}
	})
}
