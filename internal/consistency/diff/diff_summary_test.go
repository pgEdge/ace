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

package diff

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func noSelect(table, node string) error {
	return &NoSelectPrivilegeError{User: "u", Table: table, Node: node}
}

func TestSkipIfUnreadable(t *testing.T) {
	var s DiffSummary

	// RunChecks may wrap the error on its way out.
	wrapped := fmt.Errorf("checks failed: %w", noSelect("public.t", "n2"))
	if !s.SkipIfUnreadable("public.t", wrapped, true) {
		t.Fatal("a wrapped NoSelectPrivilegeError should be skipped")
	}
	if s.SkipIfUnreadable("public.u", errors.New("no primary key found"), true) {
		t.Fatal("other errors should not be skipped")
	}

	want := []string{"public.t (no SELECT privilege on n2)"}
	if len(s.SkippedTables) != 1 || s.SkippedTables[0] != want[0] {
		t.Fatalf("SkippedTables = %q, want %q", s.SkippedTables, want)
	}
	if s.NoPrivilegeSkips != 1 {
		t.Fatalf("NoPrivilegeSkips = %d, want 1", s.NoPrivilegeSkips)
	}
}

func TestPrintAndFinalizeWithPrivilegeSkips(t *testing.T) {
	tests := []struct {
		name    string
		summary func() *DiffSummary
		wantErr string // empty means no error
	}{
		{
			name: "skips and a compared table",
			summary: func() *DiffSummary {
				s := &DiffSummary{MatchedTables: []string{"public.a"}}
				s.SkipIfUnreadable("public.b", noSelect("public.b", "n1"), true)
				return s
			},
		},
		{
			name: "skips and a table with differences",
			summary: func() *DiffSummary {
				s := &DiffSummary{DifferedTables: []string{"public.a"}}
				s.SkipIfUnreadable("public.b", noSelect("public.b", "n1"), true)
				return s
			},
		},
		{
			name: "only skips",
			summary: func() *DiffSummary {
				s := &DiffSummary{}
				s.SkipIfUnreadable("public.b", noSelect("public.b", "n1"), true)
				return s
			},
			wantErr: "compared no tables",
		},
		{
			// --skip-tables entries are the user's choice, not a privilege
			// problem, so skipping everything that way still succeeds.
			name: "only skip-list skips",
			summary: func() *DiffSummary {
				return &DiffSummary{SkippedTables: []string{"public.b"}}
			},
		},
		{
			name: "skips and a failed table",
			summary: func() *DiffSummary {
				s := &DiffSummary{FailedTables: []FailedTableInfo{
					{Table: "public.a", Err: errors.New("no primary key found")},
				}}
				s.SkipIfUnreadable("public.b", noSelect("public.b", "n1"), true)
				return s
			},
			wantErr: "1 table(s) failed",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.summary().PrintAndFinalize("Schema diff", "schema public")
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tt.wantErr)
			}
		})
	}
}
