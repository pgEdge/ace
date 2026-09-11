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

//go:build unix

package common

import (
	"path/filepath"
	"syscall"
	"testing"
)

// The defect was that the mode came from the ambient umask rather than from
// ACE. Pinning the umask wide open isolates that. Not parallel: umask is
// process-global.
func TestSecureWritesIgnoreUmask(t *testing.T) {
	old := syscall.Umask(0)
	t.Cleanup(func() { syscall.Umask(old) })

	dir := t.TempDir()

	file := filepath.Join(dir, "diff.json")
	if err := WriteFileSecure(file, []byte("rows")); err != nil {
		t.Fatalf("WriteFileSecure: %v", err)
	}
	assertOwnerOnly(t, file)

	sub := filepath.Join(dir, "reports", "2026-01-01")
	if err := MkdirAllSecure(sub); err != nil {
		t.Fatalf("MkdirAllSecure: %v", err)
	}
	assertOwnerOnly(t, sub)
}
