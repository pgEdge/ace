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
	"fmt"
	"strings"
)

// RelationInfo is one relation in an inheritance tree.
type RelationInfo struct {
	Schema  string
	Name    string
	RelKind string // r = heap, p = partitioned, f = foreign
	Depth   int    // 0 for the table the tree was built from
	Parent  string // qualified parent name, "" for the root
}

// Qualified returns schema.name without quoting.
func (r RelationInfo) Qualified() string {
	return r.Schema + "." + r.Name
}

// RelationTree is a table together with every relation that inherits from it,
// directly or through intermediate parents.
type RelationTree struct {
	Root        RelationInfo
	Descendants []RelationInfo // ordered by depth, then name
}

// HasForeign reports whether the root or any descendant is a foreign table.
func (t *RelationTree) HasForeign() bool {
	if t.Root.RelKind == "f" {
		return true
	}
	for _, d := range t.Descendants {
		if d.RelKind == "f" {
			return true
		}
	}
	return false
}

// ForeignRelations lists the qualified names of every foreign relation in
// the tree, root first, then in tree order.
func (t *RelationTree) ForeignRelations() []string {
	var out []string
	if t.Root.RelKind == "f" {
		out = append(out, t.Root.Qualified())
	}
	for _, d := range t.Descendants {
		if d.RelKind == "f" {
			out = append(out, d.Qualified())
		}
	}
	return out
}

// HeapLeaves lists the relations that actually store rows: the root if it is
// a heap table, then every heap descendant. Partitioned relations hold no
// rows and are skipped; foreign relations are skipped.
func (t *RelationTree) HeapLeaves() []RelationInfo {
	var out []RelationInfo
	if t.Root.RelKind == "r" {
		out = append(out, t.Root)
	}
	for _, d := range t.Descendants {
		if d.RelKind == "r" {
			out = append(out, d)
		}
	}
	return out
}

// IsInherited reports whether anything inherits from the root.
func (t *RelationTree) IsInherited() bool {
	return len(t.Descendants) > 0
}

// GetRelationTree runs the recursive pg_inherits query for schema.table.
// It returns nil, nil when the table does not exist. A relation reachable
// through more than one parent (multiple inheritance) is listed once.
func GetRelationTree(ctx context.Context, db DBQuerier, schema, table string) (*RelationTree, error) {
	sql, err := RenderSQL(SQLTemplates.GetRelationTree, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetRelationTree SQL: %w", err)
	}
	// The query text is a fixed template; schema and table travel as bind
	// parameters, so nothing from the caller is spliced into the SQL.
	rows, err := db.Query(ctx, sql, schema, table) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to get relation tree for %s.%s failed: %w", schema, table, err)
	}
	defer rows.Close()

	var relations []RelationInfo
	for rows.Next() {
		var r RelationInfo
		if err := rows.Scan(&r.Schema, &r.Name, &r.RelKind, &r.Depth, &r.Parent); err != nil {
			return nil, fmt.Errorf("failed to scan relation tree row: %w", err)
		}
		relations = append(relations, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating relation tree rows: %w", err)
	}
	tree, err := buildRelationTree(relations)
	if err != nil {
		return nil, fmt.Errorf("relation tree for %s.%s: %w", schema, table, err)
	}
	return tree, nil
}

// relationKey identifies a relation by its separate schema and name so that
// "a"."b.c" and "a.b"."c" never collide the way their dotted rendering does.
type relationKey struct {
	schema string
	name   string
}

// buildRelationTree folds the query's rows, ordered by depth with the root
// first, into a tree. A relation that appears more than once (reachable
// through several parents) is kept once. It returns nil, nil for no rows.
func buildRelationTree(relations []RelationInfo) (*RelationTree, error) {
	var tree *RelationTree
	seen := make(map[relationKey]bool)
	for _, r := range relations {
		key := relationKey{schema: r.Schema, name: r.Name}
		if r.Depth == 0 {
			tree = &RelationTree{Root: r}
			seen[key] = true
			continue
		}
		if tree == nil {
			return nil, fmt.Errorf("has a descendant before its root")
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		tree.Descendants = append(tree.Descendants, r)
	}
	return tree, nil
}

// UnsupportedReason says why ACE cannot compare the tree's root relation, or
// returns "" for an ordinary heap or partitioned table whose tree holds no
// foreign relations. The text reads as a predicate on the table name, e.g.
// "'s.t' is a foreign table; ...".
func (t *RelationTree) UnsupportedReason() string {
	switch t.Root.RelKind {
	case "f":
		return "is a foreign table; ACE does not compare foreign tables"
	case "v", "m":
		return "is a view; ACE compares tables"
	}
	if t.HasForeign() {
		return fmt.Sprintf("has foreign relations in its inheritance tree (%s); ACE does not yet compare tables with foreign children or partitions",
			strings.Join(t.ForeignRelations(), ", "))
	}
	return ""
}

// HotTableHint looks for a table named "_<table>" next to a view. Some
// tiering extensions, coldfront among them, rename the real table that way
// and put a view in its place, so the underscore table may be the data the
// user meant to compare. When found, it returns a sentence mentioning that
// table; otherwise "".
func HotTableHint(ctx context.Context, db DBQuerier, schema, table string) (string, error) {
	hot, err := GetRelationTree(ctx, db, schema, "_"+table)
	if err != nil {
		return "", err
	}
	if hot == nil || (hot.Root.RelKind != "r" && hot.Root.RelKind != "p") {
		return "", nil
	}
	return fmt.Sprintf(" A table named '%s' also exists. It may be the table behind this view (tiering extensions such as coldfront use this layout); if so, compare '%s' instead.",
		hot.Root.Qualified(), hot.Root.Qualified()), nil
}
