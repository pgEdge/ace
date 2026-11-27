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

package queries

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// TxBeginner is a connection or pool that can begin a transaction.
type TxBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

// MtreeTableRef names the table that a Merkle tree was built for.
type MtreeTableRef struct {
	Schema string
	Table  string
}

// MtreeLeafIndexDDL returns the command that creates the leaf index of the
// tree table mtreeTable, a quoted name.
func MtreeLeafIndexDDL(mtreeTable string) (string, error) {
	sql, err := RenderSQL(SQLTemplates.CreateMtreeLeafIndex, map[string]any{"MtreeTable": mtreeTable})
	if err != nil {
		return "", fmt.Errorf("failed to render CreateMtreeLeafIndex SQL: %w", err)
	}
	return strings.TrimSpace(sql), nil
}

// CreateMtreeLeafIndex creates the leaf index of the tree table mtreeTable.
func CreateMtreeLeafIndex(ctx context.Context, db DBQuerier, mtreeTable string) error {
	sql, err := MtreeLeafIndexDDL(mtreeTable)
	if err != nil {
		return err
	}
	if _, err := db.Exec(ctx, sql); err != nil {
		return fmt.Errorf("query to create the leaf index of '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

// MtreeLeafIndexState reports whether the tree table mtreeTable, a quoted
// name, exists, and whether it has the leaf index.
func MtreeLeafIndexState(ctx context.Context, db DBQuerier, mtreeTable string) (exists, indexed bool, err error) {
	sql, err := RenderSQL(SQLTemplates.MtreeLeafIndexState, nil)
	if err != nil {
		return false, false, fmt.Errorf("failed to render MtreeLeafIndexState SQL: %w", err)
	}
	if err := db.QueryRow(ctx, sql, mtreeTable).Scan(&exists, &indexed); err != nil {
		return false, false, fmt.Errorf("query to find the leaf index of '%s' failed: %w", mtreeTable, err)
	}
	return exists, indexed, nil
}

// EnsureMtreeLeafIndex creates the leaf index of the tree table mtreeTable,
// a quoted name, if the table exists and has no such index. It waits for
// the lock on the table for at most lockTimeout; a lock that is not granted
// in time gives an error with SQLSTATE 55P03. It reports whether it created
// the index.
func EnsureMtreeLeafIndex(ctx context.Context, db TxBeginner, mtreeTable string, lockTimeout time.Duration) (bool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if need, err := needsLeafIndex(ctx, tx, mtreeTable); !need {
		return false, err
	}
	if err := lockForLeafIndex(ctx, tx, mtreeTable, lockTimeout); err != nil {
		return false, err
	}
	// Another session can have created the index while this one waited
	// for the lock.
	if need, err := needsLeafIndex(ctx, tx, mtreeTable); !need {
		return false, err
	}
	if err := CreateMtreeLeafIndex(ctx, tx, mtreeTable); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("failed to commit the leaf index of '%s': %w", mtreeTable, err)
	}
	return true, nil
}

// needsLeafIndex reports whether the tree table mtreeTable exists and has no
// leaf index. It is false on an error.
func needsLeafIndex(ctx context.Context, db DBQuerier, mtreeTable string) (bool, error) {
	exists, indexed, err := MtreeLeafIndexState(ctx, db, mtreeTable)
	return err == nil && exists && !indexed, err
}

// lockForLeafIndex locks, in tx, the tree table mtreeTable against other
// sessions that create its leaf index and against writes, waiting for at most
// lockTimeout. A lock that is not granted in time gives SQLSTATE 55P03.
func lockForLeafIndex(ctx context.Context, tx pgx.Tx, mtreeTable string, lockTimeout time.Duration) error {
	if _, err := tx.Exec(ctx, "SELECT pg_catalog.set_config('lock_timeout', $1::bigint::text, true)",
		max(lockTimeout.Milliseconds(), 1)); err != nil {
		return fmt.Errorf("failed to set lock_timeout: %w", err)
	}
	lockSQL, err := RenderSQL(SQLTemplates.LockMtreeTableForIndex, map[string]any{"MtreeTable": mtreeTable})
	if err != nil {
		return fmt.Errorf("failed to render LockMtreeTableForIndex SQL: %w", err)
	}
	if _, err := tx.Exec(ctx, lockSQL); err != nil { // nosemgrep
		return fmt.Errorf("failed to lock '%s': %w", mtreeTable, err)
	}
	return nil
}

// ListTreesWithoutLeafIndex returns the tables whose Merkle tree table, in
// the schema mtreeSchema, exists and has no leaf index. A node without the
// metadata table has no trees.
func ListTreesWithoutLeafIndex(ctx context.Context, db DBQuerier, mtreeSchema string) ([]MtreeTableRef, error) {
	sql, err := RenderSQL(SQLTemplates.ListTreesWithoutLeafIndex, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render ListTreesWithoutLeafIndex SQL: %w", err)
	}
	isMissingTable := func(err error) bool {
		var pgErr *pgconn.PgError
		return errors.As(err, &pgErr) && pgErr.Code == "42P01"
	}
	rows, err := db.Query(ctx, sql, pgx.Identifier{mtreeSchema}.Sanitize())
	if err != nil {
		if isMissingTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("query to list Merkle trees without the leaf index failed: %w", err)
	}
	refs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (MtreeTableRef, error) {
		var r MtreeTableRef
		err := row.Scan(&r.Schema, &r.Table)
		return r, err
	})
	if err != nil {
		if isMissingTable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("query to list Merkle trees without the leaf index failed: %w", err)
	}
	return refs, nil
}
