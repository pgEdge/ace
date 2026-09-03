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
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// DefaultMaxUnionBranches caps the heap relations a UNION ALL table source
// may combine. Measured on PG 17: a left-deep UNION ALL fails past ~8,000
// branches with the default 2 MB max_stack_depth; the balanced form used here
// reaches 65,000 but planning takes over a minute there.
const DefaultMaxUnionBranches = 4000

// DefaultSourceAlias is the alias a UNION ALL source gets when the caller
// does not name one. A subquery in FROM must have an alias.
const DefaultSourceAlias = "_src_"

// TableSource is the thing a diff query reads from. For an ordinary table it
// renders as schema.table. For a table whose inheritance tree contains
// foreign relations it renders as a UNION ALL of SELECT ... FROM ONLY over
// the heap relations, so the foreign ones are never scanned. Each branch
// projects the data columns plus xmin and tableoid, so callers can keep
// referring to those system columns by name.
type TableSource struct {
	Schema   string
	Table    string
	Columns  []string       // projected by every branch; union only
	Branches []RelationInfo // heap relations; empty means plain
}

// PlainTableSource is the ordinary schema.table reference.
func PlainTableSource(schema, table string) TableSource {
	return TableSource{Schema: schema, Table: table}
}

// UnionTableSource builds a source over the given heap relations. It refuses
// more than maxBranches relations; maxBranches <= 0 means the default.
func UnionTableSource(schema, table string, heapLeaves []RelationInfo, cols []string, maxBranches int) (TableSource, error) {
	if len(heapLeaves) == 0 {
		return TableSource{}, fmt.Errorf("%s.%s has no heap relations to read from", schema, table)
	}
	if len(cols) == 0 {
		return TableSource{}, fmt.Errorf("a UNION ALL source for %s.%s needs the column list", schema, table)
	}
	if maxBranches <= 0 {
		maxBranches = DefaultMaxUnionBranches
	}
	if len(heapLeaves) > maxBranches {
		return TableSource{}, fmt.Errorf("%s.%s has %d heap relations in its inheritance tree, more than max_inheritance_branches (%d)",
			schema, table, len(heapLeaves), maxBranches)
	}
	return TableSource{Schema: schema, Table: table, Columns: cols, Branches: heapLeaves}, nil
}

// IsUnion reports whether the source renders as a UNION ALL subquery.
func (s TableSource) IsUnion() bool {
	return len(s.Branches) > 0
}

// FromClause renders the source for a FROM clause. alias may be empty for a
// plain source; a union source always gets an alias.
func (s TableSource) FromClause(alias string) string {
	return s.render(alias, "")
}

// FromClauseSampled is FromClause with a TABLESAMPLE clause attached to the
// table, or to every branch of a union.
func (s TableSource) FromClauseSampled(alias, sampleClause string) string {
	return s.render(alias, sampleClause)
}

func (s TableSource) render(alias, sampleClause string) string {
	if !s.IsUnion() {
		out := pgx.Identifier{s.Schema, s.Table}.Sanitize()
		if sampleClause != "" {
			out += " " + sampleClause
		}
		if alias != "" {
			out += " AS " + alias
		}
		return out
	}
	if alias == "" {
		alias = DefaultSourceAlias
	}
	colIdents := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		colIdents[i] = pgx.Identifier{c}.Sanitize()
	}
	projection := strings.Join(colIdents, ", ") + ", xmin, tableoid"

	branches := make([]string, len(s.Branches))
	for i, leaf := range s.Branches {
		b := "SELECT " + projection + " FROM ONLY " + pgx.Identifier{leaf.Schema, leaf.Name}.Sanitize()
		if sampleClause != "" {
			b += " " + sampleClause
		}
		branches[i] = b
	}
	return "(" + balancedUnion(branches) + ") AS " + alias
}

// balancedUnion joins branches with UNION ALL as a balanced binary tree.
// The parser recurses once per nesting level, so a left-deep chain of N
// branches needs N stack frames while this needs log2(N).
func balancedUnion(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	half := len(parts) / 2
	return "(" + balancedUnion(parts[:half]) + " UNION ALL " + balancedUnion(parts[half:]) + ")"
}
