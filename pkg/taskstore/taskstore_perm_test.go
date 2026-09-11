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

package taskstore

import (
	"os"
	"path/filepath"
	"testing"
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

// ace_tasks.db records task context and diff file paths, so the driver's
// umask-masked 0666 must not survive — on a new database or an existing one an
// older build left readable.
func TestNewCreatesOwnerOnlyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ace_tasks.db")

	store, err := New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	assertOwnerOnly(t, path)

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("loosen mode: %v", err)
	}
	reopened, err := New(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	assertOwnerOnly(t, path)
}
