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
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/types"
)

// These tests need a PostgreSQL server. Set ACE_TEST_DSN to a connection
// string of a role that can create a schema.
func rangesTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ACE_TEST_DSN")
	if dsn == "" {
		t.Skip("ACE_TEST_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for _, sql := range []string{
		`DROP SCHEMA IF EXISTS ace_ranges_test CASCADE`,
		`CREATE SCHEMA ace_ranges_test`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS ace_ranges_test CASCADE`); err != nil {
			t.Errorf("drop schema ace_ranges_test: %v", err)
		}
	})
	return pool
}

// linguisticCollation returns the schema-qualified name of a linguistic
// collation that the current database can use, or "" if there is none.
func linguisticCollation(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	var coll string
	err := pool.QueryRow(context.Background(), `
		SELECT quote_ident(n.nspname) || '.' || quote_ident(c.collname)
		FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace
		WHERE c.collname IN ('en_US.utf8', 'en_US.UTF-8', 'en_US', 'und-x-icu')
			AND c.collencoding IN (-1, pg_char_to_encoding(getdatabaseencoding()))
		ORDER BY c.collname LIMIT 1`).Scan(&coll)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return coll
}

// diffRanges creates the table, reads the cast types and the collations of
// its columns, and builds the ranges for leaves.
func diffRanges(t *testing.T, pool *pgxpool.Pool, table, ddl string, key []string, leaves []types.LeafRange) []KeyRange {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, ddl); err != nil {
		t.Fatalf("%s: %v", ddl, err)
	}
	castTypes, err := GetCastTypes(ctx, pool, "ace_ranges_test", table)
	if err != nil {
		t.Fatal(err)
	}
	collations, err := GetCollations(ctx, pool, "ace_ranges_test", table)
	if err != nil {
		t.Fatal(err)
	}
	got, err := MtreeDiffRanges(ctx, pool, key, castTypes, collations, leaves)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func k(vals ...string) []string { return vals }

// checkRanges reports an error if got is not exactly want, in the same order.
func checkRanges(t *testing.T, got, want []KeyRange) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ranges:\n got %+v\nwant %+v", got, want)
	}
}

// The ranges come in key order, the open-start range first, and a range
// that starts where the one before it ends excludes its lower bound, so
// that a row on a shared bound is compared once. A gap that no leaf covers
// is not compared.
func TestMtreeDiffRangesOrderAndSharedBounds(t *testing.T) {
	pool := rangesTestPool(t)
	got := diffRanges(t, pool, "t_int",
		`CREATE TABLE ace_ranges_test.t_int (id int PRIMARY KEY)`, []string{"id"},
		[]types.LeafRange{
			{RangeEnd: k("10")},
			{RangeStart: k("10"), RangeEnd: k("20")},
			{RangeStart: k("30"), RangeEnd: k("40")},
			{RangeStart: k("40")},
		})
	checkRanges(t, got, []KeyRange{
		{Upper: k("10"), UpperClosed: true},
		{Lower: k("10"), Upper: k("20"), LowerOpen: true, UpperClosed: true},
		{Lower: k("30"), Upper: k("40"), UpperClosed: true},
		{Lower: k("40"), LowerOpen: true, UpperClosed: true},
	})
}

// One leaf with no bounds gives one range with no bounds.
func TestMtreeDiffRangesOneOpenLeaf(t *testing.T) {
	pool := rangesTestPool(t)
	got := diffRanges(t, pool, "t_open",
		`CREATE TABLE ace_ranges_test.t_open (id int PRIMARY KEY)`, []string{"id"},
		[]types.LeafRange{{}, {}})
	checkRanges(t, got, []KeyRange{{UpperClosed: true}})
}

// Bounds of a numeric key with another scale on the other node are one
// bound, and NaN sorts above every number.
func TestMtreeDiffRangesNumeric(t *testing.T) {
	pool := rangesTestPool(t)
	got := diffRanges(t, pool, "t_num",
		`CREATE TABLE ace_ranges_test.t_num (id numeric PRIMARY KEY)`, []string{"id"},
		[]types.LeafRange{
			{RangeStart: k("1.0"), RangeEnd: k("9")},
			{RangeStart: k("10"), RangeEnd: k("NaN")},
			{RangeStart: k("1.00"), RangeEnd: k("9.000")},
		})
	if len(got) != 2 {
		t.Fatalf("got %d ranges, want 2: %+v", len(got), got)
	}
	if got[0].LowerOpen || got[1].LowerOpen || got[1].Lower[0] != "10" || got[1].Upper[0] != "NaN" {
		t.Errorf("ranges: %+v", got)
	}
}

// An enum key is ordered by the order of the enum, not by its label.
func TestMtreeDiffRangesEnum(t *testing.T) {
	pool := rangesTestPool(t)
	if _, err := pool.Exec(context.Background(),
		`CREATE TYPE ace_ranges_test.color AS ENUM ('red', 'green', 'blue')`); err != nil {
		t.Fatal(err)
	}
	got := diffRanges(t, pool, "t_enum",
		`CREATE TABLE ace_ranges_test.t_enum (id ace_ranges_test.color PRIMARY KEY)`, []string{"id"},
		[]types.LeafRange{
			{RangeStart: k("red"), RangeEnd: k("green")},
			{RangeStart: k("green"), RangeEnd: k("blue")},
		})
	checkRanges(t, got, []KeyRange{
		{Lower: k("red"), Upper: k("green"), UpperClosed: true},
		{Lower: k("green"), Upper: k("blue"), LowerOpen: true, UpperClosed: true},
	})
}

// A key column of a composite type can hold a value with a NULL field. Such
// a bound is a key, not an open bound.
func TestMtreeDiffRangesCompositeTypeWithNullField(t *testing.T) {
	pool := rangesTestPool(t)
	if _, err := pool.Exec(context.Background(),
		`CREATE TYPE ace_ranges_test.pair AS (x int, y int)`); err != nil {
		t.Fatal(err)
	}
	got := diffRanges(t, pool, "t_pair",
		`CREATE TABLE ace_ranges_test.t_pair (id ace_ranges_test.pair PRIMARY KEY)`, []string{"id"},
		[]types.LeafRange{
			{RangeStart: k("(1,)"), RangeEnd: k("(2,)")},
			{RangeStart: k("(1,)"), RangeEnd: k("(2,)")},
		})
	checkRanges(t, got, []KeyRange{
		{Lower: k("(1,)"), Upper: k("(2,)"), UpperClosed: true},
	})
}

// A composite key with a text column in a linguistic collation is ordered
// by that collation: 'b' comes before 'B', while their bytes are in the
// opposite order.
func TestMtreeDiffRangesCollation(t *testing.T) {
	pool := rangesTestPool(t)
	coll := linguisticCollation(t, pool)
	if coll == "" {
		t.Skip("no linguistic collation on this server")
	}
	ddl := fmt.Sprintf(`CREATE TABLE ace_ranges_test.t_coll (a int, b text COLLATE %s, PRIMARY KEY (a, b))`, coll)
	got := diffRanges(t, pool, "t_coll", ddl, []string{"a", "b"},
		[]types.LeafRange{
			// Node 1.
			{RangeEnd: k("1", "a")},
			{RangeStart: k("1", "a"), RangeEnd: k("1", "B")},
			{RangeStart: k("5", "x")},
			// Node 2.
			{RangeStart: k("1", "a"), RangeEnd: k("1", "b")},
			{RangeStart: k("3", "c"), RangeEnd: k("5", "x")},
		})
	checkRanges(t, got, []KeyRange{
		{Upper: k("1", "a"), UpperClosed: true},
		{Lower: k("1", "a"), Upper: k("1", "b"), LowerOpen: true, UpperClosed: true},
		{Lower: k("1", "b"), Upper: k("1", "B"), LowerOpen: true, UpperClosed: true},
		{Lower: k("3", "c"), Upper: k("5", "x"), UpperClosed: true},
		{Lower: k("5", "x"), LowerOpen: true, UpperClosed: true},
	})
}
