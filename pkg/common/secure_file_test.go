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
	"context"
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

// The real writer: the diff result holds row data, so neither its directory
// and files nor the JSON and HTML reports may be readable by other local
// users.
func TestDiffResultIsNotWorldReadable(t *testing.T) {
	for _, format := range []string{"json", "html"} {
		t.Run(format, func(t *testing.T) {
			t.Chdir(t.TempDir())
			w := NewDiffResultWriter("public", "customers", []string{"id"})
			b := w.OpenBlock("n1/n2", "n1", "n2", 0, 0)
			row := types.OrderedMap{{Key: "id", Value: 1}, {Key: "v", Value: "a"}}
			if err := b.Add(DiffKindNode1Only, row, nil, []byte{1, 2, 3, 4, 5, 6, 7, 8}, nil); err != nil {
				t.Fatal(err)
			}
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
			paths, err := w.Finish(context.Background(), DiffResultManifest{
				Schema: "public", Table: "customers", PrimaryKey: []string{"id"},
				Columns: []DiffResultColumn{{Name: "id", Type: "integer"}, {Name: "v", Type: "text"}},
				Summary: types.DiffSummary{Schema: "public", Table: "customers", PrimaryKey: []string{"id"},
					DiffRowsCount: map[string]int{"n1/n2": 1}},
			}, DiffReportOptions{Format: format})
			if err != nil {
				t.Fatalf("Finish: %v", err)
			}
			assertOwnerOnly(t, paths.Dir)
			assertOwnerOnly(t, filepath.Join(paths.Dir, "n1_n2"))
			assertOwnerOnly(t, filepath.Join(paths.Dir, "n1_n2", DiffBlockName(0, 0)))
			assertOwnerOnly(t, filepath.Join(paths.Dir, DiffResultManifestName))
			assertOwnerOnly(t, paths.JSON)
			if format == "html" {
				assertOwnerOnly(t, paths.HTML)
			}
		})
	}
}
