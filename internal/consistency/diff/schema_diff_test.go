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
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pgedge/ace/internal/consistency/schema"
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

// TestValidate_StructureModeRejectsExplicitHTMLOutput verifies that
// --output=html is rejected for --compare=structure when the user actually
// passed --output: structure mode has no per-table diff files to render as
// html, only findings.
func TestValidate_StructureModeRejectsExplicitHTMLOutput(t *testing.T) {
	cmd := &SchemaDiffCmd{
		ClusterName:    "c1",
		SchemaName:     "public",
		Nodes:          "n1,n2",
		Compare:        CompareStructure,
		Output:         "html",
		OutputExplicit: true,
	}
	err := cmd.Validate()
	if err == nil {
		t.Fatal("expected an error for --output=html with --compare=structure, got nil")
	}
	if !strings.Contains(err.Error(), "not supported with --compare=structure") {
		t.Errorf("error = %q, want it to mention --compare=structure", err.Error())
	}
}

// TestValidate_StructureModeRejectsAnyExplicitNonJSONOutput verifies the
// guard is not special-cased to "html" alone: any explicit value other than
// "json" is rejected, matching the docs' claim that structure mode only
// accepts json.
func TestValidate_StructureModeRejectsAnyExplicitNonJSONOutput(t *testing.T) {
	cmd := &SchemaDiffCmd{
		ClusterName:    "c1",
		SchemaName:     "public",
		Nodes:          "n1,n2",
		Compare:        CompareStructure,
		Output:         "xml",
		OutputExplicit: true,
	}
	err := cmd.Validate()
	if err == nil {
		t.Fatal("expected an error for --output=xml with --compare=structure, got nil")
	}
	if !strings.Contains(err.Error(), "not supported with --compare=structure") {
		t.Errorf("error = %q, want it to mention --compare=structure", err.Error())
	}
}

// TestValidate_StructureModeAllowsDefaultOutputValue verifies the guard only
// fires when the user actually passed --output: the flag's own default value
// reaching Output with OutputExplicit left false must not trip it, or every
// --compare=structure run that never mentions --output would fail.
func TestValidate_StructureModeAllowsDefaultOutputValue(t *testing.T) {
	cmd := &SchemaDiffCmd{
		ClusterName: "c1",
		SchemaName:  "public",
		Nodes:       "n1,n2",
		Compare:     CompareStructure,
		Output:      "html", // the flag's default value, not user-chosen here
		// OutputExplicit intentionally left false.
	}
	if err := cmd.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidate_StructureModeAllowsExplicitJSONOutput verifies the one
// explicit value structure mode does support.
func TestValidate_StructureModeAllowsExplicitJSONOutput(t *testing.T) {
	cmd := &SchemaDiffCmd{
		ClusterName:    "c1",
		SchemaName:     "public",
		Nodes:          "n1,n2",
		Compare:        CompareStructure,
		Output:         "json",
		OutputExplicit: true,
	}
	if err := cmd.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestValidate_DataModeIgnoresOutputCompareGuard verifies the guard is
// specific to --compare=structure: the default per-table data diff has
// always accepted --output=html and must keep doing so.
func TestValidate_DataModeIgnoresOutputCompareGuard(t *testing.T) {
	cmd := &SchemaDiffCmd{
		ClusterName:    "c1",
		SchemaName:     "public",
		Nodes:          "n1,n2",
		Compare:        CompareData,
		Output:         "html",
		OutputExplicit: true,
	}
	if err := cmd.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// TestStructureDiffReport_JSONFieldNames pins the --output=json wire format:
// a script parsing this depends on these exact key names, so a rename here
// would be a breaking change that should show up as a failing test, not
// silently ship.
func TestStructureDiffReport_JSONFieldNames(t *testing.T) {
	report := StructureDiffReport{
		Schema: "public",
		Nodes:  []string{"n1", "n2"},
		MissingTables: []MissingTableInfo{
			{Table: "public.foo", PresentOn: []string{"n1"}, MissingFrom: []string{"n2"}},
		},
		Comparisons: []StructureComparisonReport{
			{
				NodeA: "n1", NodeB: "n2",
				Divergences: []schema.Divergence{
					{
						Object: "public.t.x", Kind: "column", Property: "type",
						NodeA: "n1", NodeB: "n2", ValueOnA: "integer", ValueOnB: "bigint",
						Rank: schema.RankNarrowed, NarrowSide: "n1",
					},
				},
			},
		},
		ExitCode: schema.ExitNarrowed,
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	for _, key := range []string{"schema", "nodes", "missing_tables", "comparisons", "exit_code"} {
		if _, ok := decoded[key]; !ok {
			t.Errorf("encoded report is missing top-level key %q: %s", key, encoded)
		}
	}

	comparisons, _ := decoded["comparisons"].([]any)
	if len(comparisons) != 1 {
		t.Fatalf("comparisons = %v, want 1 entry", comparisons)
	}
	comparison, _ := comparisons[0].(map[string]any)
	for _, key := range []string{"node_a", "node_b", "divergences"} {
		if _, ok := comparison[key]; !ok {
			t.Errorf("comparison entry is missing key %q: %s", key, encoded)
		}
	}

	divs, _ := comparison["divergences"].([]any)
	if len(divs) != 1 {
		t.Fatalf("divergences = %v, want 1 entry", divs)
	}
	div, _ := divs[0].(map[string]any)
	for _, key := range []string{
		"object", "kind", "property", "node_a", "node_b",
		"value_on_a", "value_on_b", "rank", "narrow_side",
	} {
		if _, ok := div[key]; !ok {
			t.Errorf("divergence entry is missing key %q: %s", key, encoded)
		}
	}
}

// TestStructureDiffReport_OmitsEmptyMissingTables verifies missing_tables is
// left out entirely (not printed as null or []) when nothing was missing -
// the common case, and the one that should read as clean JSON.
func TestStructureDiffReport_OmitsEmptyMissingTables(t *testing.T) {
	report := StructureDiffReport{
		Schema:      "public",
		Nodes:       []string{"n1", "n2"},
		Comparisons: []StructureComparisonReport{{NodeA: "n1", NodeB: "n2"}},
		ExitCode:    schema.ExitIdentical,
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "missing_tables") {
		t.Errorf("expected missing_tables to be omitted when empty, got: %s", encoded)
	}
}
