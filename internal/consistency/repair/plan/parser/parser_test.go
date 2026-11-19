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

package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func values(m map[string]Value) ValueProvider {
	return func(source, column string) Value {
		v, ok := m[source+"."+column]
		if !ok {
			return Value{Null: true}
		}
		return v
	}
}

// The comparisons keep the text of the literals and name the columns, so
// that the server can read both sides with the type of the column. A NULL
// side is decided without the comparator.
func TestWhenComparisons(t *testing.T) {
	w, err := CompileWhenExpression(`n1.amount < 1.50 AND (n2.status IN ('a', 'b') OR n1.flag = true) AND n2.note IS NULL`)
	require.NoError(t, err)
	row := values(map[string]Value{
		"n1.amount": {Text: "1.5"},
		"n2.status": {Text: "b"},
		"n1.flag":   {Text: "t"},
	})
	got := w.Comparisons(row)
	require.Equal(t, []Comparison{
		{Op: "<", Left: "1.5", LeftColumn: "amount", Right: "1.50"},
		{Op: "=", Left: "b", LeftColumn: "status", Right: "a"},
		{Op: "=", Left: "b", LeftColumn: "status", Right: "b"},
		{Op: "=", Left: "t", LeftColumn: "flag", Right: "true"},
	}, got)

	answers := map[Comparison]bool{got[0]: false, got[1]: false, got[2]: true, got[3]: true}
	cmp := func(c Comparison) (bool, error) { return answers[c], nil }
	ok, err := w.Eval(row, cmp)
	require.NoError(t, err)
	require.False(t, ok, "1.5 < 1.50 is false for numeric")
	answers[got[0]] = true
	ok, err = w.Eval(row, cmp)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestWhenNulls(t *testing.T) {
	w, err := CompileWhenExpression(`n1.a = n2.a`)
	require.NoError(t, err)
	none := func(Comparison) (bool, error) {
		t.Fatal("a NULL side must not reach the comparator")
		return false, nil
	}
	ok, err := w.Eval(values(nil), none)
	require.NoError(t, err)
	require.True(t, ok, "NULL = NULL is true in a when expression")
	require.Empty(t, w.Comparisons(values(nil)))

	w, err = CompileWhenExpression(`n1.a < 5`)
	require.NoError(t, err)
	_, err = w.Eval(values(nil), none)
	require.Error(t, err, "< with a NULL side")
}

// A comparison of two literals has no column type to compare with.
func TestWhenNeedsAColumn(t *testing.T) {
	_, err := CompileWhenExpression(`1 = 1`)
	require.ErrorContains(t, err, "needs a column")
	_, err = CompileWhenExpression(`'a' IN ('a', 'b')`)
	require.ErrorContains(t, err, "needs a column")
}
