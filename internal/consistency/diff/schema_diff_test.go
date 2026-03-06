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
	"os"
	"path/filepath"
	"testing"
)

// writeSkipFile is a helper that writes lines to a temp file and returns its path.
func writeSkipFile(t *testing.T, lines ...string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "skip-*.txt")
	if err != nil {
		t.Fatalf("create temp skip file: %v", err)
	}
	for _, line := range lines {
		if _, err := f.WriteString(line + "\n"); err != nil {
			t.Fatalf("write skip file: %v", err)
		}
	}
	f.Close()
	return f.Name()
}

// TestParseSkipList_Empty verifies that an empty SkipTables / SkipFile leaves
// skipTablesList as nil (no allocations, no errors).
func TestParseSkipList_Empty(t *testing.T) {
	cmd := &SchemaDiffCmd{}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.skipTablesList) != 0 {
		t.Errorf("skipTablesList = %v, want empty", cmd.skipTablesList)
	}
}

// TestParseSkipList_FromFlag verifies comma-separated tables in SkipTables are
// split into individual entries.
func TestParseSkipList_FromFlag(t *testing.T) {
	cmd := &SchemaDiffCmd{SkipTables: "public.orders,public.audit_log,public.sessions"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"public.orders", "public.audit_log", "public.sessions"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList len = %d, want %d", len(cmd.skipTablesList), len(want))
	}
	for i, w := range want {
		if cmd.skipTablesList[i] != w {
			t.Errorf("skipTablesList[%d] = %q, want %q", i, cmd.skipTablesList[i], w)
		}
	}
}

// TestParseSkipList_FromFile verifies that tables listed one-per-line in a
// skip file are read and stored correctly.
func TestParseSkipList_FromFile(t *testing.T) {
	path := writeSkipFile(t, "public.orders", "public.audit_log")
	cmd := &SchemaDiffCmd{SkipFile: path}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"public.orders", "public.audit_log"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList len = %d, want %d", len(cmd.skipTablesList), len(want))
	}
	for i, w := range want {
		if cmd.skipTablesList[i] != w {
			t.Errorf("skipTablesList[%d] = %q, want %q", i, cmd.skipTablesList[i], w)
		}
	}
}

// TestParseSkipList_FromBoth verifies that entries from both SkipTables and
// SkipFile are merged, with the flag entries first.
func TestParseSkipList_FromBoth(t *testing.T) {
	path := writeSkipFile(t, "public.from_file")
	cmd := &SchemaDiffCmd{
		SkipTables: "public.from_flag",
		SkipFile:   path,
	}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"public.from_flag", "public.from_file"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList = %v, want %v", cmd.skipTablesList, want)
	}
	for i, w := range want {
		if cmd.skipTablesList[i] != w {
			t.Errorf("skipTablesList[%d] = %q, want %q", i, cmd.skipTablesList[i], w)
		}
	}
}

// TestParseSkipList_MissingFile verifies that a non-existent SkipFile path
// returns an error rather than silently succeeding.
func TestParseSkipList_MissingFile(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SkipFile: filepath.Join(t.TempDir(), "does-not-exist.txt"),
	}
	if err := cmd.parseSkipList(); err == nil {
		t.Fatal("expected error for missing skip file, got nil")
	}
}

// TestParseSkipList_SingleEntry verifies a single table name with no comma
// is stored as one entry (regression guard: Split("x", ",") → ["x"]).
func TestParseSkipList_SingleEntry(t *testing.T) {
	cmd := &SchemaDiffCmd{SkipTables: "public.orders"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.skipTablesList) != 1 {
		t.Fatalf("skipTablesList len = %d, want 1", len(cmd.skipTablesList))
	}
	if cmd.skipTablesList[0] != "public.orders" {
		t.Errorf("skipTablesList[0] = %q, want %q", cmd.skipTablesList[0], "public.orders")
	}
}
