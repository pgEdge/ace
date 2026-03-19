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

// ---------------------------------------------------------------------------
// Validate
// ---------------------------------------------------------------------------

func TestRepsetDiffCmd_Validate(t *testing.T) {
	tests := []struct {
		name        string
		cmd         RepsetDiffCmd
		wantErr     bool
		errContains string
	}{
		{
			name:        "missing cluster name",
			cmd:         RepsetDiffCmd{RepsetName: "default"},
			wantErr:     true,
			errContains: "cluster name is required",
		},
		{
			name:        "missing repset name",
			cmd:         RepsetDiffCmd{ClusterName: "test_cluster"},
			wantErr:     true,
			errContains: "repset name is required",
		},
		{
			name: "valid",
			cmd:  RepsetDiffCmd{ClusterName: "test_cluster", RepsetName: "default"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cmd.Validate()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tc.errContains)
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// parseSkipList
// ---------------------------------------------------------------------------

func TestRepsetDiffCmd_ParseSkipList_Empty(t *testing.T) {
	cmd := &RepsetDiffCmd{}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cmd.skipTablesList) != 0 {
		t.Errorf("skipTablesList = %v, want empty", cmd.skipTablesList)
	}
}

func TestRepsetDiffCmd_ParseSkipList_FromFlag(t *testing.T) {
	cmd := &RepsetDiffCmd{SkipTables: "orders,audit_log,sessions"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertStringSlice(t, cmd.skipTablesList, []string{"orders", "audit_log", "sessions"})
}

func TestRepsetDiffCmd_ParseSkipList_FromFile(t *testing.T) {
	path := writeRepsetSkipFile(t, "orders", "audit_log")
	cmd := &RepsetDiffCmd{SkipFile: path}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertStringSlice(t, cmd.skipTablesList, []string{"orders", "audit_log"})
}

func TestRepsetDiffCmd_ParseSkipList_FromBoth(t *testing.T) {
	path := writeRepsetSkipFile(t, "from_file")
	cmd := &RepsetDiffCmd{SkipTables: "from_flag", SkipFile: path}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertStringSlice(t, cmd.skipTablesList, []string{"from_flag", "from_file"})
}

func TestRepsetDiffCmd_ParseSkipList_MissingFile(t *testing.T) {
	cmd := &RepsetDiffCmd{
		SkipFile: filepath.Join(t.TempDir(), "does-not-exist.txt"),
	}
	if err := cmd.parseSkipList(); err == nil {
		t.Fatal("expected error for missing skip file, got nil")
	}
}

// TestRepsetDiffCmd_ParseSkipList_SchemaQualified verifies that schema-qualified
// names pass through unchanged. Unlike SchemaDiffCmd, repset's parseSkipList
// does NOT strip the schema prefix.
func TestRepsetDiffCmd_ParseSkipList_SchemaQualified(t *testing.T) {
	cmd := &RepsetDiffCmd{SkipTables: "public.orders,myschema.audit_log"}
	if err := cmd.parseSkipList(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertStringSlice(t, cmd.skipTablesList, []string{"public.orders", "myschema.audit_log"})
}

// ---------------------------------------------------------------------------
// Skip matching (simulates the loop in RepsetDiff lines 284-298)
//
// GetTablesInRepSet returns schema-qualified names ("public.orders"), so the
// skip list must also use schema-qualified names to match. These tests verify
// that behavior and document the mismatch when bare names are used.
// ---------------------------------------------------------------------------

// simulateSkipMatching reproduces the skip-matching loop from RepsetDiff.
// Returns the tables that would actually be diffed (i.e. not skipped).
func simulateSkipMatching(tableList, skipTablesList []string) []string {
	var diffed []string
	for _, tableName := range tableList {
		var skipped bool
		for _, skip := range skipTablesList {
			if strings.TrimSpace(skip) == tableName {
				skipped = true
				break
			}
		}
		if !skipped {
			diffed = append(diffed, tableName)
		}
	}
	return diffed
}

func TestSkipMatching_SchemaQualifiedMatch(t *testing.T) {
	// GetTablesInRepSet returns schema-qualified names.
	tableList := []string{"public.orders", "public.users", "public.audit_log"}
	skipList := []string{"public.users"}

	diffed := simulateSkipMatching(tableList, skipList)
	assertStringSlice(t, diffed, []string{"public.orders", "public.audit_log"})
}

func TestSkipMatching_BareNameDoesNotMatchQualified(t *testing.T) {
	// A bare "users" will NOT match "public.users" — this documents current behavior.
	tableList := []string{"public.orders", "public.users"}
	skipList := []string{"users"}

	diffed := simulateSkipMatching(tableList, skipList)
	// "users" doesn't match "public.users", so nothing is skipped.
	assertStringSlice(t, diffed, []string{"public.orders", "public.users"})
}

func TestSkipMatching_MultipleSkips(t *testing.T) {
	tableList := []string{"public.a", "public.b", "public.c", "public.d"}
	skipList := []string{"public.b", "public.d"}

	diffed := simulateSkipMatching(tableList, skipList)
	assertStringSlice(t, diffed, []string{"public.a", "public.c"})
}

func TestSkipMatching_EmptySkipList(t *testing.T) {
	tableList := []string{"public.orders", "public.users"}

	diffed := simulateSkipMatching(tableList, nil)
	assertStringSlice(t, diffed, []string{"public.orders", "public.users"})
}

func TestSkipMatching_AllSkipped(t *testing.T) {
	tableList := []string{"public.orders"}
	skipList := []string{"public.orders"}

	diffed := simulateSkipMatching(tableList, skipList)
	if len(diffed) != 0 {
		t.Errorf("expected empty, got %v", diffed)
	}
}

func TestSkipMatching_WhitespaceInSkipEntry(t *testing.T) {
	tableList := []string{"public.orders"}
	skipList := []string{"  public.orders  "}

	diffed := simulateSkipMatching(tableList, skipList)
	if len(diffed) != 0 {
		t.Errorf("expected skip to match after trimming, got %v", diffed)
	}
}

func TestSkipMatching_DifferentSchemas(t *testing.T) {
	tableList := []string{"public.orders", "sales.orders"}
	skipList := []string{"public.orders"}

	diffed := simulateSkipMatching(tableList, skipList)
	assertStringSlice(t, diffed, []string{"sales.orders"})
}

// ---------------------------------------------------------------------------
// NewRepsetDiffTask / CloneForSchedule
// ---------------------------------------------------------------------------

func TestRepsetDiffCmd_NewTask(t *testing.T) {
	task := NewRepsetDiffTask()
	if task.TaskID == "" {
		t.Error("TaskID should not be empty")
	}
	if task.TaskType != "REPSET_DIFF" {
		t.Errorf("TaskType = %q, want %q", task.TaskType, "REPSET_DIFF")
	}
	if task.TaskStatus != "PENDING" {
		t.Errorf("TaskStatus = %q, want %q", task.TaskStatus, "PENDING")
	}
	if task.Ctx == nil {
		t.Error("Ctx should not be nil")
	}
}

func TestRepsetDiffCmd_CloneForSchedule(t *testing.T) {
	original := &RepsetDiffCmd{
		ClusterName:       "prod",
		DBName:            "mydb",
		RepsetName:        "default",
		Nodes:             "n1,n2",
		SkipTables:        "foo,bar",
		SkipFile:          "/tmp/skip.txt",
		Quiet:             true,
		BlockSize:         5000,
		ConcurrencyFactor: 0.75,
		CompareUnitSize:   50,
		Output:            "json",
		TableFilter:       "id > 10",
		OverrideBlockSize: true,
		SkipDBUpdate:      true,
		TaskStorePath:     "/tmp/store.db",
	}

	clone := original.CloneForSchedule(original.Ctx)

	// All config fields should be copied.
	if clone.ClusterName != original.ClusterName {
		t.Errorf("ClusterName = %q, want %q", clone.ClusterName, original.ClusterName)
	}
	if clone.DBName != original.DBName {
		t.Errorf("DBName = %q, want %q", clone.DBName, original.DBName)
	}
	if clone.RepsetName != original.RepsetName {
		t.Errorf("RepsetName = %q, want %q", clone.RepsetName, original.RepsetName)
	}
	if clone.Nodes != original.Nodes {
		t.Errorf("Nodes = %q, want %q", clone.Nodes, original.Nodes)
	}
	if clone.SkipTables != original.SkipTables {
		t.Errorf("SkipTables = %q, want %q", clone.SkipTables, original.SkipTables)
	}
	if clone.SkipFile != original.SkipFile {
		t.Errorf("SkipFile = %q, want %q", clone.SkipFile, original.SkipFile)
	}
	if clone.Quiet != original.Quiet {
		t.Errorf("Quiet = %v, want %v", clone.Quiet, original.Quiet)
	}
	if clone.BlockSize != original.BlockSize {
		t.Errorf("BlockSize = %d, want %d", clone.BlockSize, original.BlockSize)
	}
	if clone.ConcurrencyFactor != original.ConcurrencyFactor {
		t.Errorf("ConcurrencyFactor = %f, want %f", clone.ConcurrencyFactor, original.ConcurrencyFactor)
	}
	if clone.CompareUnitSize != original.CompareUnitSize {
		t.Errorf("CompareUnitSize = %d, want %d", clone.CompareUnitSize, original.CompareUnitSize)
	}
	if clone.Output != original.Output {
		t.Errorf("Output = %q, want %q", clone.Output, original.Output)
	}
	if clone.TableFilter != original.TableFilter {
		t.Errorf("TableFilter = %q, want %q", clone.TableFilter, original.TableFilter)
	}
	if clone.OverrideBlockSize != original.OverrideBlockSize {
		t.Errorf("OverrideBlockSize = %v, want %v", clone.OverrideBlockSize, original.OverrideBlockSize)
	}
	if clone.SkipDBUpdate != original.SkipDBUpdate {
		t.Errorf("SkipDBUpdate = %v, want %v", clone.SkipDBUpdate, original.SkipDBUpdate)
	}
	if clone.TaskStorePath != original.TaskStorePath {
		t.Errorf("TaskStorePath = %q, want %q", clone.TaskStorePath, original.TaskStorePath)
	}
	// Clone gets a fresh TaskID.
	if clone.TaskID == original.TaskID {
		t.Error("clone should have a new TaskID")
	}
}

// ---------------------------------------------------------------------------
// Getter/setter coverage
// ---------------------------------------------------------------------------

func TestRepsetDiffCmd_Getters(t *testing.T) {
	cmd := &RepsetDiffCmd{
		ClusterName: "mycluster",
		DBName:      "mydb",
		Nodes:       "n1,n2",
	}

	if cmd.GetClusterName() != "mycluster" {
		t.Errorf("GetClusterName() = %q", cmd.GetClusterName())
	}
	if cmd.GetDBName() != "mydb" {
		t.Errorf("GetDBName() = %q", cmd.GetDBName())
	}
	if cmd.GetNodes() != "n1,n2" {
		t.Errorf("GetNodes() = %q", cmd.GetNodes())
	}

	cmd.SetDBName("otherdb")
	if cmd.GetDBName() != "otherdb" {
		t.Errorf("after SetDBName: GetDBName() = %q", cmd.GetDBName())
	}

	cmd.SetNodeList([]string{"a", "b"})
	assertStringSlice(t, cmd.GetNodeList(), []string{"a", "b"})

	nodes := []map[string]any{{"Name": "n1"}}
	cmd.SetClusterNodes(nodes)
	if len(cmd.GetClusterNodes()) != 1 || cmd.GetClusterNodes()[0]["Name"] != "n1" {
		t.Errorf("GetClusterNodes() = %v", cmd.GetClusterNodes())
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writeRepsetSkipFile(t *testing.T, lines ...string) string {
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

func assertStringSlice(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %v (len=%d), want %v (len=%d)", got, len(got), want, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
