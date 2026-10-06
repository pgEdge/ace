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

// The snapshot transaction must use the same output settings as every ACE
// connection, plus search_path, and must not touch client_encoding.
func TestDeparseSettingsFollowConnectionSettings(t *testing.T) {
	want := map[string]bool{
		"SET LOCAL bytea_output = 'hex'":               true,
		"SET LOCAL datestyle = 'ISO, MDY'":             true,
		"SET LOCAL extra_float_digits = '3'":           true,
		"SET LOCAL intervalstyle = 'postgres'":         true,
		"SET LOCAL lc_monetary = 'C'":                  true,
		"SET LOCAL search_path = 'pg_catalog'":         true,
		"SET LOCAL standard_conforming_strings = 'on'": true,
		"SET LOCAL timezone = 'UTC'":                   true,
	}
	if len(deparseSettings) != len(want) {
		t.Fatalf("deparseSettings has %d statements, want %d: %q", len(deparseSettings), len(want), deparseSettings)
	}
	for _, stmt := range deparseSettings {
		if !want[stmt] {
			t.Errorf("unexpected statement %q", stmt)
		}
	}
}
