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
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/internal/consistency/repair/plan/parser"
)

// metaColTypes are the types of the columns that a repair plan can read from
// the spock metadata of a row.
var metaColTypes = map[string]string{
	"commit_ts":   "timestamp with time zone",
	"node_origin": "text",
}

// textComparison is a comparison of two texts, each read with a type.
type textComparison struct {
	op                  string
	left, right         string
	leftType, rightType string
}

// sqlComparisonOps are the operators that a comparison can have.
var sqlComparisonOps = map[string]bool{"=": true, "<>": true, "<": true, "<=": true, ">": true, ">=": true}

// maxComparisonsPerQuery limits the size of one query of comparisons.
const maxComparisonsPerQuery = 10000

// comparer decides the comparisons of a repair plan on the server: a
// pk_in range, a comparison of a when expression and the choice of
// pick_freshest. The client does not convert the values to Go types: the
// server reads both texts with the type of the column and compares them.
// The comparisons are first collected with add, then sent in a few queries
// by flush, and then read with compare.
type comparer struct {
	// colTypes are the cast types of the columns of the table.
	colTypes map[string]string
	// run decides comparisons, in the order of the slice.
	run func(cmps []textComparison) ([]bool, error)

	pending []textComparison
	queued  map[textComparison]bool
	results map[textComparison]bool
}

func newComparer(colTypes map[string]string, run func(cmps []textComparison) ([]bool, error)) *comparer {
	return &comparer{
		colTypes: colTypes,
		run:      run,
		queued:   make(map[textComparison]bool),
		results:  make(map[textComparison]bool),
	}
}

// colType returns the type of a column of the table, or of a column of the
// spock metadata.
func (c *comparer) colType(col string) (string, error) {
	if t, ok := c.colTypes[col]; ok && t != "" {
		return t, nil
	}
	if t, ok := metaColTypes[col]; ok {
		return t, nil
	}
	if c.colTypes == nil {
		return "", fmt.Errorf("no types to compare column %q with: no node of the pair is connected", col)
	}
	return "", fmt.Errorf("column %q is not a column of the table", col)
}

// typed returns the comparison with the types of its sides. A literal is
// read with the type of the column on the other side.
func (c *comparer) typed(pc parser.Comparison) (textComparison, error) {
	if !sqlComparisonOps[pc.Op] {
		return textComparison{}, fmt.Errorf("unknown comparison operator %q", pc.Op)
	}
	tc := textComparison{op: pc.Op, left: pc.Left, right: pc.Right}
	var err error
	if pc.LeftColumn != "" {
		if tc.leftType, err = c.colType(pc.LeftColumn); err != nil {
			return textComparison{}, err
		}
	}
	if pc.RightColumn != "" {
		if tc.rightType, err = c.colType(pc.RightColumn); err != nil {
			return textComparison{}, err
		}
	}
	if tc.leftType == "" {
		tc.leftType = tc.rightType
	}
	if tc.rightType == "" {
		tc.rightType = tc.leftType
	}
	if tc.leftType == "" {
		return textComparison{}, fmt.Errorf("comparison %s %s %s has no column", pc.Left, pc.Op, pc.Right)
	}
	return tc, nil
}

// add queues a comparison for the next flush.
func (c *comparer) add(pc parser.Comparison) error {
	tc, err := c.typed(pc)
	if err != nil {
		return err
	}
	if _, done := c.results[tc]; done || c.queued[tc] {
		return nil
	}
	c.queued[tc] = true
	c.pending = append(c.pending, tc)
	return nil
}

// flush decides the queued comparisons.
func (c *comparer) flush() error {
	for len(c.pending) > 0 {
		n := min(len(c.pending), maxComparisonsPerQuery)
		batch := c.pending[:n]
		if c.run == nil {
			return fmt.Errorf("no node to compare values on")
		}
		res, err := c.run(batch)
		if err != nil {
			return err
		}
		if len(res) != len(batch) {
			return fmt.Errorf("%d results for %d comparisons", len(res), len(batch))
		}
		for i, tc := range batch {
			c.results[tc] = res[i]
			delete(c.queued, tc)
		}
		c.pending = c.pending[n:]
	}
	return nil
}

// compare returns the result of a comparison that a flush has decided.
func (c *comparer) compare(pc parser.Comparison) (bool, error) {
	tc, err := c.typed(pc)
	if err != nil {
		return false, err
	}
	r, ok := c.results[tc]
	if !ok {
		return false, fmt.Errorf("comparison %s %s %s was not computed", pc.Left, pc.Op, pc.Right)
	}
	return r, nil
}

// comparisonQuerier runs a query on a node.
type comparisonQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// runComparisons decides comparisons on a node in one query. The
// comparisons with the same operator and types are one part of the query:
// the texts of their sides are two text[] parameters, and the part returns
// the positions of the comparisons that are true. A text that the type does
// not accept fails the query.
func runComparisons(ctx context.Context, q comparisonQuerier, cmps []textComparison) ([]bool, error) {
	type group struct {
		op, leftType, rightType string
		idx                     []int
		left, right             []string
	}
	var groups []*group
	byKey := make(map[[3]string]*group)
	for i, c := range cmps {
		k := [3]string{c.op, c.leftType, c.rightType}
		g, ok := byKey[k]
		if !ok {
			g = &group{op: c.op, leftType: c.leftType, rightType: c.rightType}
			byKey[k] = g
			groups = append(groups, g)
		}
		g.idx = append(g.idx, i)
		g.left = append(g.left, c.left)
		g.right = append(g.right, c.right)
	}

	parts := make([]string, len(groups))
	args := make([]any, 0, 2*len(groups))
	for gi, g := range groups {
		if !sqlComparisonOps[g.op] {
			return nil, fmt.Errorf("unknown comparison operator %q", g.op)
		}
		parts[gi] = fmt.Sprintf(
			"SELECT %d AS g, c.i FROM unnest($%d::text[], $%d::text[]) WITH ORDINALITY AS c(l, r, i) WHERE c.l::%s %s c.r::%s",
			gi, 2*gi+1, 2*gi+2, g.leftType, g.op, g.rightType)
		args = append(args, g.left, g.right)
	}
	rows, err := q.Query(ctx, strings.Join(parts, " UNION ALL "), args...) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("compare values on the server: %w", err)
	}
	defer rows.Close()

	out := make([]bool, len(cmps))
	for rows.Next() {
		var g int
		var i int64
		if err := rows.Scan(&g, &i); err != nil {
			return nil, fmt.Errorf("compare values on the server: %w", err)
		}
		if g < 0 || g >= len(groups) || i < 1 || i > int64(len(groups[g].idx)) {
			return nil, fmt.Errorf("compare values on the server: bad result %d/%d", g, i)
		}
		out[groups[g].idx[i-1]] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("compare values on the server: %w", err)
	}
	return out, nil
}

// valueText returns the text of a value of a diff entry, and false for a
// NULL. A diff result holds every value as text; a result made by a tool
// from Go values can also hold a JSON number or a boolean, which is given in
// its JSON form.
func valueText(v any) (string, bool) {
	switch x := v.(type) {
	case nil:
		return "", false
	case string:
		return x, true
	case json.Number:
		return x.String(), true
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return fmt.Sprint(x), true
		}
		return string(b), true
	}
}
