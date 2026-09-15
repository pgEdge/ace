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

package scope

import (
	"context"
	"fmt"

	"github.com/pgedge/ace/db/queries"
)

// SchemaProvider resolves every base table of one PostgreSQL namespace.
// Resolve delegates to queries.GetTablesInSchema, the same query
// schema-diff's data comparison already uses, so both share one definition
// of what a schema's area means.
type SchemaProvider struct {
	SchemaName string
}

var _ Provider = SchemaProvider{}

// Resolve reads the base tables of SchemaProvider.SchemaName from the node
// behind db. An unknown schema resolves to an empty Scope; existence is
// checked by the caller.
func (p SchemaProvider) Resolve(ctx context.Context, db queries.DBQuerier) (Scope, error) {
	tables, err := queries.GetTablesInSchema(ctx, db, p.SchemaName)
	if err != nil {
		return Scope{}, fmt.Errorf("could not list tables in schema %q: %w", p.SchemaName, err)
	}

	scope := Scope{
		Tables: make([]QualifiedName, 0, len(tables)),
		Source: p.Describe(),
	}
	for _, table := range tables {
		scope.Tables = append(scope.Tables, QualifiedName{Schema: p.SchemaName, Table: table})
	}
	return scope, nil
}

// Describe labels this source for a report header, and for the Source field
// Resolve records on the Scope it returns. It names the namespace rather
// than the tables found in it, so the label is the same on every node even
// when the nodes disagree about what the namespace holds.
func (p SchemaProvider) Describe() string {
	return fmt.Sprintf("schema %s", p.SchemaName)
}
