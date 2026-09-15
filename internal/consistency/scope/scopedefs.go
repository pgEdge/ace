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

// Package scope resolves which tables a comparison run covers. schema-diff,
// table-diff and repset-diff each arrive at a table list differently (a
// namespace, an explicit name, a Spock replication set, a native
// publication); this package gives each a common interface, so the
// comparison layer (internal/consistency/schema) never needs to know which
// one produced the list, and never imports anything Spock-specific.
//
// Only the "schema" source is implemented so far: all base tables of one
// namespace, matching schema-diff's existing behaviour. Adding another
// source is meant to be one new file here plus a flag in the owning command.
package scope

import (
	"context"

	"github.com/pgedge/ace/db/queries"
)

// QualifiedName identifies one table by its namespace and name. It does not
// carry a node: the same QualifiedName is used to look the table up on every
// node being compared.
type QualifiedName struct {
	Schema string
	Table  string
}

// String renders the qualified name the way it is shown to a person:
// "schema.table".
func (q QualifiedName) String() string {
	return q.Schema + "." + q.Table
}

// Scope is the list of tables a comparison run should cover, as resolved on
// one node. Columns is only populated when the source restricts which
// columns are in scope (a Spock replication set with a column list, a
// publication with attnames); a missing entry means "all columns of that
// table are in scope".
type Scope struct {
	// Tables lists every table this source resolved on the node it was
	// asked to read. Order is not significant; callers that need a stable
	// order (for printing, for hashing) must sort it themselves.
	Tables []QualifiedName

	// Columns restricts the columns considered in scope for a table, when
	// the source knows such a restriction. A table with no entry here is
	// read in full.
	Columns map[QualifiedName][]string

	// Source names which Provider produced this Scope, for the report
	// header. Not interpreted by the comparison layer.
	Source string
}

// Provider resolves a Scope by reading one node. Each source is one
// implementation of this interface in its own file. DBQuerier is
// db/queries.DBQuerier, so a Provider can be handed a pool, a transaction,
// or anything else that already satisfies it, with no adapter needed.
type Provider interface {
	// Resolve reads the node behind db and returns the tables (and, where
	// applicable, columns) within this source's area. It reads only the
	// node it is given; reconciling disagreement between nodes is the
	// caller's job.
	Resolve(ctx context.Context, db queries.DBQuerier) (Scope, error)

	// Describe returns a short human-readable label for the report header,
	// e.g. "schema public" or "repset \"default\"".
	Describe() string
}
