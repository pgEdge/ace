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

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTextRuleBpchar: two values are identical when their value::text is
// identical. The cast of bpchar to text removes trailing spaces, so a bpchar
// key or value with trailing spaces on one node and without them on the
// other is the same row.
func TestTextRuleBpchar(t *testing.T) {
	env := newSpockEnv()
	table := typedKeyTable(t, env, "ace_text_rule_bpchar", "k bpchar PRIMARY KEY, v bpchar, w text", "k, v, w",
		[]string{"('a  ', 'x  ', 'w')", "('b', 'y', 'w')"},
		[]string{"('a', 'x', 'w')", "('b  ', 'y', 'w')"})
	env.assertNoTableDiff(t, table)

	// A real difference in another column of such a row is found, and the
	// row is read by its key on both nodes.
	ctx := context.Background()
	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	env.withRepairModeTx(t, ctx, tx, func() {
		_, err := tx.Exec(ctx, "UPDATE "+table+" SET w = 'changed' WHERE k = 'a'") // nosemgrep
		require.NoError(t, err)
	})
	require.NoError(t, tx.Commit(ctx))
	report := readDiffReport(t, env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2}))
	require.Equal(t, 1, report.Summary.DiffRowsCount[env.pairKey()], "one changed row")
	pair := report.NodeDiffs[env.pairKey()]
	require.Len(t, pair.Rows[env.ServiceN1], 1)
	require.Len(t, pair.Rows[env.ServiceN2], 1)
	w, _ := pair.Rows[env.ServiceN2][0].Get("w")
	require.Equal(t, "changed", w)
}
