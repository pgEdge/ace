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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Two collations of the same name order values in another way when their
// definitions differ: 'und-u-kn' sorts "2" before "10", 'und' does not. The
// texts that tell collation orders apart must differ for them, and the name
// for SQL must not.
func TestCollationOrdersTellDefinitionsApart(t *testing.T) {
	pool := rangesTestPool(t)
	ctx := context.Background()

	order1, name1 := readKordCollation(t, ctx, pool, "provider = icu, locale = 'und'")
	order2, name2 := readKordCollation(t, ctx, pool, "provider = icu, locale = 'und-u-kn'")
	if order1 == order2 {
		t.Errorf("collations with other locales give the same order text %q", order1)
	}
	if name1 != name2 || name1 != "ace_ranges_test.kord" {
		t.Errorf("names for SQL: %q and %q, want ace_ranges_test.kord", name1, name2)
	}
	for _, o := range []string{order1, order2} {
		if !strings.HasPrefix(o, "ace_ranges_test.kord (") {
			t.Errorf("order text %q does not start with the name", o)
		}
	}
}

// readKordCollation creates the collation ace_ranges_test.kord with the
// definition def and a table with a key in it, and returns the order text and
// the name of the collation of the key. The test is skipped if the server
// cannot create the collation.
func readKordCollation(t *testing.T, ctx context.Context, pool *pgxpool.Pool, def string) (order, name string) {
	t.Helper()
	for _, sql := range []string{
		`DROP TABLE IF EXISTS ace_ranges_test.t_kord`,
		`DROP COLLATION IF EXISTS ace_ranges_test.kord`,
		`CREATE COLLATION ace_ranges_test.kord (` + def + `)`,
		`CREATE TABLE ace_ranges_test.t_kord (k text COLLATE ace_ranges_test.kord PRIMARY KEY)`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			if strings.Contains(sql, "CREATE COLLATION") {
				t.Skipf("the server cannot create a collation with %s: %v", def, err)
			}
			t.Fatalf("%s: %v", sql, err)
		}
	}
	orders, err := GetCollationOrders(ctx, pool, "ace_ranges_test", "t_kord")
	if err != nil {
		t.Fatal(err)
	}
	names, err := GetCollations(ctx, pool, "ace_ranges_test", "t_kord")
	if err != nil {
		t.Fatal(err)
	}
	return orders["k"], names["k"]
}

// The C library reads en_US.UTF-8 and en_US.utf8 as one locale, and the
// catalog keeps the spelling of the CREATE statement. The two spellings must
// give the same order text.
func TestCollationOrdersLibcLocaleSpelling(t *testing.T) {
	pool := rangesTestPool(t)
	ctx := context.Background()

	order1, _ := readKordCollation(t, ctx, pool, "provider = libc, locale = 'en_US.UTF-8'")
	order2, _ := readKordCollation(t, ctx, pool, "provider = libc, locale = 'en_US.utf8'")
	if order1 != order2 {
		t.Errorf("one libc locale in two spellings gives other order texts: %q and %q", order1, order2)
	}
	if order1 != "ace_ranges_test.kord (libc en_US.utf8)" {
		t.Errorf("order text %q, want ace_ranges_test.kord (libc en_US.utf8)", order1)
	}
}

// A column without COLLATE has the default collation, and its order text
// comes from the locale of the database.
func TestCollationOrdersDefaultCollation(t *testing.T) {
	pool := rangesTestPool(t)
	ctx := context.Background()
	for _, sql := range []string{
		`DROP TABLE IF EXISTS ace_ranges_test.t_kdef`,
		`CREATE TABLE ace_ranges_test.t_kdef (k text PRIMARY KEY)`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	var provider, collate, locale, rules string
	if err := pool.QueryRow(ctx, `SELECT datlocprovider::text, datcollate,
			coalesce(pg_catalog.to_jsonb(d)->>'datlocale', pg_catalog.to_jsonb(d)->>'daticulocale', ''),
			coalesce(pg_catalog.to_jsonb(d)->>'daticurules', '')
		FROM pg_catalog.pg_database d WHERE datname = current_database()`).Scan(&provider, &collate, &locale, &rules); err != nil {
		t.Fatal(err)
	}
	icu := "icu " + locale
	if rules != "" {
		icu += ", rules " + rules
	}
	want := map[string]string{
		"c": "default (libc " + canonicalLibcLocale(collate) + ")",
		"i": "default (" + icu + ")",
		"b": "default (builtin " + locale + ")",
	}[provider]

	orders, err := GetCollationOrders(ctx, pool, "ace_ranges_test", "t_kdef")
	if err != nil {
		t.Fatal(err)
	}
	if want != "" && orders["k"] != want {
		t.Errorf("order text %q, want %q", orders["k"], want)
	}
	if !strings.HasPrefix(orders["k"], "default (") {
		t.Errorf("order text %q does not start with default", orders["k"])
	}
}
