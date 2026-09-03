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

package repair

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/db/queries"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/types"
)

// relationGroup is the subset of a node's repair rows that go to one
// relation.
type relationGroup struct {
	Ident pgx.Identifier
	Rows  map[string]map[string]any
	// Only is true when statements against Ident must carry ONLY. It is
	// false for a node whose tree has no foreign relations: that node's rows
	// carry no placement, so a DELETE must reach the parent's children the
	// ordinary way.
	Only bool
}

// inheritanceActive reports whether repair statements must target specific
// heap relations instead of the parent.
func (t *TableRepairTask) inheritanceActive() bool {
	return len(t.heapLeaves) > 0
}

// sourceFor returns what read-only queries against nodeName should read
// from: the plain table, or the UNION ALL over heap relations.
func (t *TableRepairTask) sourceFor(nodeName string) queries.TableSource {
	if s, ok := t.Sources[nodeName]; ok {
		return s
	}
	return queries.PlainTableSource(t.Schema, t.Table)
}

// targetRelation picks the relation a repair statement for one row should
// name on nodeName.
//
//  1. No inheritance in play: the table itself.
//  2. The row exists on nodeName: the relation it lives in there.
//  3. The row is missing on nodeName: the relation it lives in on some other
//     node, if nodeName has a heap relation of that name; otherwise the
//     parent. INSERT into an inheritance parent lands in the parent, so the
//     fallback never writes to a foreign relation.
func (t *TableRepairTask) targetRelation(nodeName, pkeyStr string) pgx.Identifier {
	parent := pgx.Identifier{t.Schema, t.Table}
	if !t.inheritanceActive() {
		return parent
	}
	if rel, ok := t.rowPlacement[nodeName][pkeyStr]; ok {
		if id, ok := splitQualified(rel); ok {
			return id
		}
	}
	for node, byKey := range t.rowPlacement {
		if node == nodeName {
			continue
		}
		rel, ok := byKey[pkeyStr]
		if !ok {
			continue
		}
		if t.heapLeaves[nodeName][rel] {
			if id, ok := splitQualified(rel); ok {
				return id
			}
		}
	}
	return parent
}

// groupByTargetRelation splits a node's repair rows by the relation each
// row should be written to. Keys are the sanitized identifiers.
func (t *TableRepairTask) groupByTargetRelation(nodeName string, rows map[string]map[string]any) map[string]relationGroup {
	groups := make(map[string]relationGroup)
	only := len(t.heapLeaves[nodeName]) > 0
	for pkeyStr, row := range rows {
		ident := t.targetRelation(nodeName, pkeyStr)
		key := ident.Sanitize()
		g, ok := groups[key]
		if !ok {
			g = relationGroup{Ident: ident, Rows: make(map[string]map[string]any), Only: only}
		}
		g.Rows[pkeyStr] = row
		groups[key] = g
	}
	return groups
}

// buildRowPlacement reads storage_relation from every row in the diff file
// and records, per node, where each row lives on that node.
func buildRowPlacement(diffs types.DiffOutput, key []string) (map[string]map[string]string, error) {
	out := make(map[string]map[string]string)
	for _, pair := range diffs.NodeDiffs {
		for node, rows := range pair.Rows {
			for _, row := range rows {
				rowMap := utils.OrderedMapToMap(row)
				meta, ok := rowMap["_spock_metadata_"].(map[string]any)
				if !ok {
					continue
				}
				rel, ok := meta["storage_relation"].(string)
				if !ok || rel == "" {
					continue
				}
				pkeyStr, err := utils.StringifyOrderedMapKey(row, key)
				if err != nil {
					return nil, fmt.Errorf("stringify pkey while reading row placement on %s: %w", node, err)
				}
				if out[node] == nil {
					out[node] = make(map[string]string)
				}
				out[node][pkeyStr] = rel
			}
		}
	}
	return out, nil
}

// splitQualified turns "schema.name" into an identifier. Names from
// tableoid::regclass::text are quoted only when they need it, so strip one
// layer of double quotes from each part.
func splitQualified(q string) (pgx.Identifier, bool) {
	parts := strings.SplitN(q, ".", 2)
	if len(parts) != 2 {
		return nil, false
	}
	return pgx.Identifier{strings.Trim(parts[0], `"`), strings.Trim(parts[1], `"`)}, true
}
