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
	"strings"
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
// skipTablesList empty (no allocations, no errors).
func TestParseSkipList_Empty(t *testing.T) {
	cmd := &SchemaDiffCmd{SchemaName: "public"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.skipTablesList) != 0 {
		t.Errorf("skipTablesList = %v, want empty", cmd.skipTablesList)
	}
}

// TestParseSkipList_FromFlag verifies comma-separated tables in SkipTables are
// split into individual entries. Schema-qualified entries are stripped.
func TestParseSkipList_FromFlag(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SchemaName: "public",
		SkipTables: "public.orders,public.audit_log,public.sessions",
	}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"orders", "audit_log", "sessions"}
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
// skip file are read and stored correctly, with schema prefix stripped.
func TestParseSkipList_FromFile(t *testing.T) {
	path := writeSkipFile(t, "public.orders", "public.audit_log")
	cmd := &SchemaDiffCmd{SchemaName: "public", SkipFile: path}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"orders", "audit_log"}
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
		SchemaName: "public",
		SkipTables: "public.from_flag",
		SkipFile:   path,
	}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"from_flag", "from_file"}
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
		SchemaName: "public",
		SkipFile:   filepath.Join(t.TempDir(), "does-not-exist.txt"),
	}
	if err := cmd.parseSkipList(); err == nil {
		t.Fatal("expected error for missing skip file, got nil")
	}
}

// TestParseSkipList_SingleEntry verifies a single table name with no comma
// is stored as one entry (regression guard: Split("x", ",") -> ["x"]).
func TestParseSkipList_SingleEntry(t *testing.T) {
	cmd := &SchemaDiffCmd{SchemaName: "public", SkipTables: "public.orders"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.skipTablesList) != 1 {
		t.Fatalf("skipTablesList len = %d, want 1", len(cmd.skipTablesList))
	}
	if cmd.skipTablesList[0] != "orders" {
		t.Errorf("skipTablesList[0] = %q, want %q", cmd.skipTablesList[0], "orders")
	}
}

// TestParseSkipList_UnqualifiedNames verifies that bare table names (without
// schema prefix) pass through unchanged.
func TestParseSkipList_UnqualifiedNames(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SchemaName: "public",
		SkipTables: "orders,audit_log",
	}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"orders", "audit_log"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList len = %d, want %d", len(cmd.skipTablesList), len(want))
	}
	for i, w := range want {
		if cmd.skipTablesList[i] != w {
			t.Errorf("skipTablesList[%d] = %q, want %q", i, cmd.skipTablesList[i], w)
		}
	}
}

// TestParseSkipList_WrongSchema verifies that a schema-qualified entry with a
// mismatched schema returns an error.
func TestParseSkipList_WrongSchema(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SchemaName: "public",
		SkipTables: "other_schema.orders",
	}
	err := cmd.parseSkipList()
	if err == nil {
		t.Fatal("expected error for mismatched schema, got nil")
	}
	if !strings.Contains(err.Error(), "does not match target schema") {
		t.Errorf("error = %q, want it to mention schema mismatch", err.Error())
	}
}

// TestParseSkipList_MixedQualifiedAndBare verifies that a mix of
// schema-qualified and bare table names is handled correctly.
func TestParseSkipList_MixedQualifiedAndBare(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SchemaName: "myschema",
		SkipTables: "myschema.orders,audit_log,myschema.sessions",
	}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"orders", "audit_log", "sessions"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList = %v, want %v", cmd.skipTablesList, want)
	}
	for i, w := range want {
		if cmd.skipTablesList[i] != w {
			t.Errorf("skipTablesList[%d] = %q, want %q", i, cmd.skipTablesList[i], w)
		}
	}
}

// TestParseSkipList_WhitespaceHandling verifies that leading/trailing
// whitespace is trimmed and empty entries are dropped.
func TestParseSkipList_WhitespaceHandling(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SchemaName: "public",
		SkipTables: " orders , public.audit_log ",
	}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"orders", "audit_log"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList = %v, want %v", cmd.skipTablesList, want)
	}
	for i, w := range want {
		if cmd.skipTablesList[i] != w {
			t.Errorf("skipTablesList[%d] = %q, want %q", i, cmd.skipTablesList[i], w)
		}
	}
}

// TestParseSkipList_EmptyLinesInFile verifies that blank lines in a skip file
// are silently ignored.
func TestParseSkipList_EmptyLinesInFile(t *testing.T) {
	path := writeSkipFile(t, "orders", "", "audit_log", "")
	cmd := &SchemaDiffCmd{SchemaName: "public", SkipFile: path}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"orders", "audit_log"}
	if len(cmd.skipTablesList) != len(want) {
		t.Fatalf("skipTablesList = %v (len=%d), want %v", cmd.skipTablesList, len(cmd.skipTablesList), want)
	}
}

// TestParseSkipList_TrailingComma verifies that a trailing comma doesn't create
// a phantom empty entry.
func TestParseSkipList_TrailingComma(t *testing.T) {
	cmd := &SchemaDiffCmd{SchemaName: "public", SkipTables: "orders,"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.skipTablesList) != 1 {
		t.Fatalf("skipTablesList = %v (len=%d), want 1 entry", cmd.skipTablesList, len(cmd.skipTablesList))
	}
	if cmd.skipTablesList[0] != "orders" {
		t.Errorf("skipTablesList[0] = %q, want %q", cmd.skipTablesList[0], "orders")
	}
}

// TestParseSkipList_EmptyTableAfterSchema verifies that a schema-qualified
// entry with an empty table name (e.g. "public.") returns an error.
func TestParseSkipList_EmptyTableAfterSchema(t *testing.T) {
	cmd := &SchemaDiffCmd{
		SchemaName: "public",
		SkipTables: "public.",
	}
	err := cmd.parseSkipList()
	if err == nil {
		t.Fatal("expected error for empty table name after schema qualifier, got nil")
	}
	if !strings.Contains(err.Error(), "missing table name") {
		t.Errorf("error = %q, want it to mention missing table name", err.Error())
	}
}
