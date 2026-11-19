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
	"reflect"
	"testing"
)

func TestKeyExpr(t *testing.T) {
	if got := KeyExpr("", []string{"id"}); got != `"id"` {
		t.Errorf("simple key: got %s", got)
	}
	if got := KeyExpr("t", []string{"a", "B"}); got != `ROW(t."a", t."B")` {
		t.Errorf("composite key: got %s", got)
	}
}

func TestKeyParams(t *testing.T) {
	types := map[string]string{"a": "integer", "b": "text"}
	got, err := KeyParams(3, []string{"a"}, types)
	if err != nil || got != "$3::integer" {
		t.Errorf("simple key: got %q, %v", got, err)
	}
	got, err = KeyParams(1, []string{"a", "b"}, types)
	if err != nil || got != "ROW($1::integer, $2::text)" {
		t.Errorf("composite key: got %q, %v", got, err)
	}
	if _, err := KeyParams(1, []string{"a", "c"}, types); err == nil {
		t.Error("a key column without a cast type must be an error")
	}
}

func TestKeyRangeCond(t *testing.T) {
	key := []string{"a", "b"}
	types := map[string]string{"a": "integer", "b": "text"}
	tests := []struct {
		name string
		r    KeyRange
		cond string
		args []any
	}{
		{"no bounds", KeyRange{}, "TRUE", nil},
		{
			"half-open",
			KeyRange{Lower: []string{"1", "x"}, Upper: []string{"2", "y"}},
			`ROW("a", "b") >= ROW($5::integer, $6::text) AND ROW("a", "b") < ROW($7::integer, $8::text)`,
			[]any{"1", "x", "2", "y"},
		},
		{
			"lower open, upper closed",
			KeyRange{Lower: []string{"1", "x"}, Upper: []string{"2", "y"}, LowerOpen: true, UpperClosed: true},
			`ROW("a", "b") > ROW($5::integer, $6::text) AND ROW("a", "b") <= ROW($7::integer, $8::text)`,
			[]any{"1", "x", "2", "y"},
		},
		{
			"upper only",
			KeyRange{Upper: []string{"2", "y"}},
			`ROW("a", "b") < ROW($5::integer, $6::text)`,
			[]any{"2", "y"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cond, args, err := tc.r.Cond("", key, types, 5)
			if err != nil {
				t.Fatal(err)
			}
			if cond != tc.cond {
				t.Errorf("cond:\n got %s\nwant %s", cond, tc.cond)
			}
			if !reflect.DeepEqual(args, tc.args) {
				t.Errorf("args: got %v, want %v", args, tc.args)
			}
		})
	}

	if _, _, err := (KeyRange{Lower: []string{"1"}}).Cond("", key, types, 1); err == nil {
		t.Error("a bound with too few values must be an error")
	}
}
