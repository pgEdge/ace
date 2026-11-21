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

// TextParam returns $n::typ: the value comes as the output of its type, the
// server gives the parameter the type typ and reads the text with the input
// function of typ. $n::text::typ must not be used: a type can have its own
// cast from text, and such a cast takes the place of the input function. typ
// must have no type modifier, or the cast can round or cut the value.
func TextParam(n int, typ string) string {
	return fmt.Sprintf("$%d::%s", n, typ)
}

// KeyExpr returns the key columns as one value: the column for a simple key
// and ROW(...) of the columns for a composite key. The columns are qualified
// with alias if it is not empty.
//
// A comparison of two ROW values follows the order of the primary key index,
// and btree uses the whole comparison as an index condition: the scan starts
// at the lower bound and stops at the upper bound. A range written column by
// column needs an OR and is not an index condition.
func KeyExpr(alias string, key []string) string {
	cols := make([]string, len(key))
	for i, k := range key {
		cols[i] = pgx.Identifier{k}.Sanitize()
		if alias != "" {
			cols[i] = alias + "." + cols[i]
		}
	}
	if len(cols) == 1 {
		return cols[0]
	}
	return "ROW(" + strings.Join(cols, ", ") + ")"
}

// KeyParams returns a key value built from the parameters $first,
// $first+1, ...: for a simple key one parameter read as text and cast to
// the cast type of the column, for a composite key ROW(...) of such
// parameters.
//
// Do not add COLLATE to the parameters. Without it the comparison with a key
// column takes the collation of the column, which is the collation of the
// primary key index, and the whole bound is an index condition. With another
// collation only the leading column stays in the index condition.
func KeyParams(first int, key []string, castTypes map[string]string) (string, error) {
	params := make([]string, len(key))
	for i, k := range key {
		typ, ok := castTypes[k]
		if !ok || strings.TrimSpace(typ) == "" {
			return "", fmt.Errorf("no cast type for key column %s", k)
		}
		params[i] = TextParam(first+i, typ)
	}
	if len(params) == 1 {
		return params[0], nil
	}
	return "ROW(" + strings.Join(params, ", ") + ")", nil
}

// KeyRange is a range of a primary key. Lower and Upper hold the text of each
// key column, the output of its type on the server; nil means that the range
// has no bound on that side. By default the range is [Lower, Upper).
type KeyRange struct {
	Lower []string
	Upper []string
	// LowerOpen excludes the rows of the lower bound from the range.
	LowerOpen bool
	// UpperClosed puts the rows of the upper bound into the range.
	UpperClosed bool
}

// Cond returns the condition of r on the key columns and its arguments. The
// parameters start at $first, the lower bound first. The columns are
// qualified with alias if it is not empty. castTypes gives the type of each
// key column on the node that runs the query. A range with no bounds gives
// "TRUE".
func (r KeyRange) Cond(alias string, key []string, castTypes map[string]string, first int) (string, []any, error) {
	if len(key) == 0 {
		return "", nil, fmt.Errorf("the key has no columns")
	}
	expr := KeyExpr(alias, key)
	var (
		conds []string
		args  []any
	)
	bound := func(vals []string, op string) error {
		if vals == nil {
			return nil
		}
		if len(vals) != len(key) {
			return fmt.Errorf("a key bound has %d values for %d key columns", len(vals), len(key))
		}
		params, err := KeyParams(first+len(args), key, castTypes)
		if err != nil {
			return err
		}
		conds = append(conds, fmt.Sprintf("%s %s %s", expr, op, params))
		for _, v := range vals {
			args = append(args, v)
		}
		return nil
	}
	lowerOp, upperOp := ">=", "<"
	if r.LowerOpen {
		lowerOp = ">"
	}
	if r.UpperClosed {
		upperOp = "<="
	}
	if err := bound(r.Lower, lowerOp); err != nil {
		return "", nil, err
	}
	if err := bound(r.Upper, upperOp); err != nil {
		return "", nil, err
	}
	if len(conds) == 0 {
		return "TRUE", nil, nil
	}
	return strings.Join(conds, " AND "), args, nil
}
