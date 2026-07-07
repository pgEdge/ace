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

package mtree

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrMtreeNotFound reports that no Merkle tree exists for the target table.
// Callers detect it with errors.Is to surface the message without extra
// wrapping.
var ErrMtreeNotFound = errors.New("no merkle tree found")

// isMissingTreeErr reports whether err indicates the Merkle tree metadata for
// the table is absent: the metadata row is missing (tree never built),
// ace_mtree_metadata itself does not exist (mtree init never ran, SQLSTATE
// 42P01 undefined_table), or the ace schema itself is absent (mtree init
// never ran on a fresh cluster, SQLSTATE 3F000 invalid_schema_name).
func isMissingTreeErr(err error) bool {
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && (pgErr.Code == "42P01" || pgErr.Code == "3F000")
}

// missingTreeError builds the user-facing fail-fast error for a missing tree,
// wrapping ErrMtreeNotFound and pointing at the exact command(s) to run.
// cause is the underlying error that isMissingTreeErr matched: when it is a
// SQLSTATE 3F000 (invalid_schema_name), the pgedge_ace schema itself is
// absent, meaning mtree init never ran, and "ace mtree build" alone would
// fail again since only init creates the schema. In that case the message
// also points at "ace mtree init" first. All other shapes (missing metadata
// row, undefined table) keep the build-only suggestion, since build creates
// the metadata table when the schema already exists.
func (m *MerkleTreeTask) missingTreeError(nodeName any, cause error) error {
	dbnameFlag := ""
	if m.DBName != "" {
		dbnameFlag = " --dbname " + m.DBName
	}
	buildCmd := fmt.Sprintf("ace mtree build %s %s%s", m.ClusterName, m.QualifiedTableName, dbnameFlag)

	var pgErr *pgconn.PgError
	if errors.As(cause, &pgErr) && pgErr.Code == "3F000" {
		initCmd := fmt.Sprintf("ace mtree init %s%s", m.ClusterName, dbnameFlag)
		return fmt.Errorf("%w for %s on node %v: run '%s' and then '%s' first",
			ErrMtreeNotFound, m.QualifiedTableName, nodeName, initCmd, buildCmd)
	}

	return fmt.Errorf("%w for %s on node %v: run '%s' first",
		ErrMtreeNotFound, m.QualifiedTableName, nodeName, buildCmd)
}
