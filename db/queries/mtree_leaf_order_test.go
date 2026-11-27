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

import "testing"

// The bounds of a leaf are compared in the order of the key: with the
// collation of each key column, which the tree table columns do not have.
func TestBoundInKeyOrder(t *testing.T) {
	coll := map[string]string{"b": `pg_catalog."en_US"`}
	if got, want := boundInKeyOrder("range_start", []string{"b"}, false, coll), `range_start COLLATE pg_catalog."en_US"`; got != want {
		t.Errorf("simple key: got %s, want %s", got, want)
	}
	if got, want := boundInKeyOrder("range_end", []string{"a"}, false, coll), `range_end`; got != want {
		t.Errorf("key without a collation: got %s, want %s", got, want)
	}
	if got, want := boundInKeyOrder("range_end", []string{"a", "b"}, true, coll),
		`ROW((range_end)."a", (range_end)."b" COLLATE pg_catalog."en_US")`; got != want {
		t.Errorf("composite key: got %s, want %s", got, want)
	}
}
