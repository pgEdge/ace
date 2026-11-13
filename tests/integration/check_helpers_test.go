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

package integration

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// execer is a pool, a connection or a transaction.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// execOK runs sql and marks the test as failed if it fails. The test goes
// on, so it can be used in a cleanup function.
func execOK(t testing.TB, ctx context.Context, db execer, sql string, args ...any) {
	t.Helper()
	if _, err := db.Exec(ctx, sql, args...); err != nil {
		t.Errorf("exec %q: %v", sql, err)
	}
}

// repsetRemoveTable removes the relation rel from the replication set set and
// marks the test as failed if it cannot. A relation that does not exist, or
// that is not in the set, is skipped.
func repsetRemoveTable(t testing.TB, ctx context.Context, db execer, set, rel string) {
	t.Helper()
	execOK(t, ctx, db, `SELECT spock.repset_remove_table(s.set_name, s.relid)
		FROM spock.tables s WHERE s.set_name = $1 AND s.relid = to_regclass($2)`, set, rel)
}

// removeGlob removes the files and directories that match pattern and marks
// the test as failed if it cannot.
func removeGlob(t testing.TB, pattern string) {
	t.Helper()
	files, err := filepath.Glob(pattern)
	if err != nil {
		t.Errorf("glob %q: %v", pattern, err)
		return
	}
	for _, f := range files {
		if err := os.RemoveAll(f); err != nil {
			t.Errorf("remove %s: %v", f, err)
		}
	}
}

// removeFile removes path and marks the test as failed if it cannot. A
// file that does not exist is not an error.
func removeFile(t testing.TB, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Errorf("remove %s: %v", path, err)
	}
}

// rollback rolls tx back and marks the test as failed if it cannot. A
// transaction that is already committed or rolled back is not an error.
func rollback(t testing.TB, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Errorf("rollback: %v", err)
	}
}
