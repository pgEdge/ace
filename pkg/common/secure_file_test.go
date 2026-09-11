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

package common

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pgedge/ace/pkg/types"
)

func assertOwnerOnly(t *testing.T, path string) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if st.Mode().Perm()&0o077 != 0 {
		t.Errorf("%s has mode %v, want no group/other bits", path, st.Mode().Perm())
	}
}

func TestWriteFileSecure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "diff.json")
	if err := WriteFileSecure(path, []byte("rows")); err != nil {
		t.Fatalf("WriteFileSecure: %v", err)
	}
	assertOwnerOnly(t, path)

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if string(got) != "rows" {
		t.Errorf("content = %q, want %q", got, "rows")
	}
}

// A file an older ACE build left world-readable must be tightened on rewrite:
// O_CREATE's mode is ignored for an existing file, so a stale 0644 would
// otherwise survive.
func TestWriteFileSecureTightensExistingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stale.json")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("seed mode: %v", err)
	}
	if err := WriteFileSecure(path, []byte("new")); err != nil {
		t.Fatalf("WriteFileSecure: %v", err)
	}
	assertOwnerOnly(t, path)
}

func TestMkdirAllSecure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "reports", "2026-01-01")
	if err := MkdirAllSecure(dir); err != nil {
		t.Fatalf("MkdirAllSecure: %v", err)
	}
	assertOwnerOnly(t, dir)
}

// The real writer: WriteDiffReport emits row data, so neither the JSON nor the
// HTML report may be readable by other local users.
func TestWriteDiffReportIsNotWorldReadable(t *testing.T) {
	for _, format := range []string{"json", "html"} {
		t.Run(format, func(t *testing.T) {
			cwd, err := os.Getwd()
			if err != nil {
				t.Fatalf("getwd: %v", err)
			}
			if err := os.Chdir(t.TempDir()); err != nil {
				t.Fatalf("chdir: %v", err)
			}
			t.Cleanup(func() { _ = os.Chdir(cwd) })

			diff := types.DiffOutput{
				NodeDiffs: map[string]types.DiffByNodePair{
					"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": {}, "n2": {}}},
				},
				Summary: types.DiffSummary{
					Schema: "public", Table: "customers",
					Nodes: []string{"n1", "n2"}, PrimaryKey: []string{"id"},
					DiffRowsCount: map[string]int{"n1/n2": 0},
				},
			}

			jsonPath, htmlPath, err := WriteDiffReport(diff, "public", "customers", format)
			if err != nil {
				t.Fatalf("WriteDiffReport: %v", err)
			}
			assertOwnerOnly(t, jsonPath)
			if format == "html" {
				assertOwnerOnly(t, htmlPath)
			}
		})
	}
}
