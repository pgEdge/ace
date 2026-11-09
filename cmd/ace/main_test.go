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

package main

import "testing"

func TestConfigOptional(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{[]string{"diff-report", "--diff-file", "x"}, true},
		{[]string{"--debug", "diff-report", "-f", "x"}, true},
		{[]string{"table-diff", "c", "public.t"}, false},
		{[]string{"--", "diff-report"}, false},
		{nil, false},
	} {
		if got := configOptional(tc.args); got != tc.want {
			t.Errorf("configOptional(%q) = %v, want %v", tc.args, got, tc.want)
		}
	}
}
