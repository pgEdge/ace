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

package schema

import "testing"

// TestSortProperties_OrdersByNameThenValue checks that entries with
// distinct Names sort by Name, and entries sharing a Name (constraints, all
// named "constraint") fall back to sorting by Value, i.e. by definition
// text.
func TestSortProperties_OrdersByNameThenValue(t *testing.T) {
	props := []Property{
		{Name: "type", Value: "text"},
		{Name: "constraint", Value: "CHECK (b > 0)"},
		{Name: "constraint", Value: "CHECK (a > 0)"},
		{Name: "notnull", Value: "true"},
	}
	sortProperties(props)

	want := []Property{
		{Name: "constraint", Value: "CHECK (a > 0)"},
		{Name: "constraint", Value: "CHECK (b > 0)"},
		{Name: "notnull", Value: "true"},
		{Name: "type", Value: "text"},
	}
	for i := range want {
		if props[i] != want[i] {
			t.Fatalf("position %d: got %+v, want %+v", i, props[i], want[i])
		}
	}
}
