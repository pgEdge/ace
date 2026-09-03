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
	"slices"
	"sort"
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
//
// Step 3 checks the source-of-truth node first (its placement is the one the
// repair is driving toward), then the remaining nodes in a fixed, sorted
// order, so the choice is deterministic across runs when several other nodes
// disagree on where the row lives.
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

	candidates := make([]string, 0, len(t.rowPlacement))
	for node := range t.rowPlacement {
		if node == nodeName {
			continue
		}
		candidates = append(candidates, node)
	}
	sort.Strings(candidates)
	if t.SourceOfTruth != "" && t.SourceOfTruth != nodeName {
		candidates = append([]string{t.SourceOfTruth}, slices.DeleteFunc(candidates, func(n string) bool {
			return n == t.SourceOfTruth
		})...)
	}

	for _, node := range candidates {
		rel, ok := t.rowPlacement[node][pkeyStr]
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

// placementRequired reports an error when a diff's summary carries no
// storage_relation placement for a table whose inheritance tree has foreign
// relations on at least one node. table-diff always sets ExcludedRelations
// (possibly to an empty map, never nil) whenever it ran the union-source
// path, so a nil map here means the diff predates that support, or was taken
// before a foreign relation appeared in the tree: every row would then have
// no recorded placement and route to the parent, which duplicate-inserts a
// row already present in a heap leaf and no-ops a delete that should have
// reached that leaf.
func placementRequired(schema, table string, summary types.DiffSummary) error {
	if summary.ExcludedRelations == nil {
		return fmt.Errorf("the diff file for %s.%s carries no storage_relation placement (it predates foreign-relation support or was taken before a foreign relation appeared); re-run table-diff and repair from the new file", schema, table)
	}
	return nil
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

// buildDeleteSQL renders one DELETE batch: the statement text and its
// positional arguments, in order. ident is the relation to delete from;
// only prefixes the statement with ONLY. keyCols is the primary key's
// column list; when simplePK is true batchKeys holds one scalar per row,
// otherwise each entry must be a []any of len(keyCols) values in keyCols
// order.
func buildDeleteSQL(ident pgx.Identifier, only bool, simplePK bool, keyCols []string, batchKeys []any) (string, []any, error) {
	var sql strings.Builder
	var args []any
	paramIdx := 1

	onlyClause := ""
	if only {
		onlyClause = "ONLY "
	}
	sql.WriteString(fmt.Sprintf("DELETE FROM %s%s WHERE ", onlyClause, ident.Sanitize()))

	if simplePK {
		sql.WriteString(fmt.Sprintf("%s IN (", pgx.Identifier{keyCols[0]}.Sanitize()))
		for j, key := range batchKeys {
			if j > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("$%d", paramIdx))
			args = append(args, key)
			paramIdx++
		}
		sql.WriteString(")")
		return sql.String(), args, nil
	}

	keyColSanitised := make([]string, len(keyCols))
	for k, keyCol := range keyCols {
		keyColSanitised[k] = pgx.Identifier{keyCol}.Sanitize()
	}
	sql.WriteString(fmt.Sprintf("(%s) IN (", strings.Join(keyColSanitised, ", ")))

	for j, key := range batchKeys {
		compositeKey, ok := key.([]any)
		if !ok {
			return "", nil, fmt.Errorf("expected composite key to be []interface{}, got %T", key)
		}
		if len(compositeKey) != len(keyCols) {
			return "", nil, fmt.Errorf("composite key length mismatch: expected %d, got %d", len(keyCols), len(compositeKey))
		}
		if j > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString("(")
		for k, val := range compositeKey {
			if k > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(fmt.Sprintf("$%d", paramIdx))
			args = append(args, val)
			paramIdx++
		}
		sql.WriteString(")")
	}
	sql.WriteString(")")

	return sql.String(), args, nil
}

// splitQualified turns "schema.name" into an identifier. The diff engines
// build storage_relation from pg_namespace.nspname || '.' || pg_class.relname
// (not tableoid::regclass::text, whose output omits the schema when the
// relation is search_path-visible), so both parts are the raw, unquoted
// catalog names and a plain split on the first dot is exact: neither a
// schema nor a relation name may itself contain a literal '.'.
func splitQualified(q string) (pgx.Identifier, bool) {
	schema, name, ok := strings.Cut(q, ".")
	if !ok || schema == "" || name == "" {
		return nil, false
	}
	return pgx.Identifier{schema, name}, true
}
