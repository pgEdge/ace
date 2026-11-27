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

	"github.com/jackc/pgx/v5"
)

// FindLeafOutOfOrder returns the position of the first leaf of the tree
// table mtreeTable that does not follow the leaf before it. found is false
// when the leaves cover the key space once, without gaps and overlaps. key
// is the key of the table, composite tells whether the tree stores it as a
// composite value, and collations maps a key column to its collation as a
// quoted name. A column without a collation in the map is compared with the
// collation of the tree column.
func FindLeafOutOfOrder(ctx context.Context, db DBQuerier, mtreeTable string, key []string, composite bool, collations map[string]string) (pos int64, found bool, err error) {
	sql, err := RenderSQL(SQLTemplates.FindLeafOutOfOrder, map[string]any{
		"MtreeTable": mtreeTable,
		"StartKey":   boundInKeyOrder("range_start", key, composite, collations),
		"EndKey":     boundInKeyOrder("range_end", key, composite, collations),
		"PrevKey":    boundInKeyOrder("prev_end", key, composite, collations),
	})
	if err != nil {
		return 0, false, fmt.Errorf("failed to render FindLeafOutOfOrder SQL: %w", err)
	}
	err = db.QueryRow(ctx, sql).Scan(&pos) // nosemgrep
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("query to check the leaf ranges of '%s' failed: %w", mtreeTable, err)
	}
	return pos, true, nil
}

// boundInKeyOrder returns the bound column of a tree table as an expression
// that compares in the order of the key: each key column with its own
// collation, and a composite key as a row of its fields.
func boundInKeyOrder(bound string, key []string, composite bool, collations map[string]string) string {
	field := func(col string) string {
		expr := bound
		if composite {
			expr = "(" + bound + ")." + pgx.Identifier{col}.Sanitize()
		}
		if coll, ok := collations[col]; ok && coll != "" {
			expr += " COLLATE " + coll
		}
		return expr
	}
	if !composite {
		return field(key[0])
	}
	fields := make([]string, len(key))
	for i, col := range key {
		fields[i] = field(col)
	}
	return "ROW(" + strings.Join(fields, ", ") + ")"
}
