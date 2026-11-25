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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestGeneratedColumnsNotCompared: generated columns are computed from the
// other columns, so they are not compared. A different generation
// expression on the nodes is not a difference, and a repair writes only
// the other columns.
func TestGeneratedColumnsNotCompared(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	table := fmt.Sprintf("%s.%s", testSchema, "ace_generated_cols")
	defs := map[string]string{
		env.ServiceN1: "id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 2) STORED",
		env.ServiceN2: "id int PRIMARY KEY, a int, g int GENERATED ALWAYS AS (a * 3) STORED",
	}
	for _, side := range []struct {
		node string
		exec func(string) error
	}{
		{env.ServiceN1, func(q string) error { _, err := env.N1Pool.Exec(ctx, q); return err }},
		{env.ServiceN2, func(q string) error { _, err := env.N2Pool.Exec(ctx, q); return err }},
	} {
		require.NoError(t, side.exec(fmt.Sprintf("CREATE TABLE %s (%s)", table, defs[side.node]))) // nosemgrep
		if env.HasSpock {
			require.NoError(t, side.exec(fmt.Sprintf("SELECT spock.repset_add_table('default', '%s')", table))) // nosemgrep
		}
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			if env.HasSpock {
				repsetRemoveTable(t, context.Background(), pool, "default", table)
			}
			execOK(t, context.Background(), pool, "DROP TABLE IF EXISTS "+table+" CASCADE") // nosemgrep
		}
	})
	for _, pool := range env.pools() {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		env.withRepairModeTx(t, ctx, tx, func() {
			_, err := tx.Exec(ctx, "INSERT INTO "+table+" (id, a) VALUES (1, 10), (2, 20)") // nosemgrep
			require.NoError(t, err)
		})
		require.NoError(t, tx.Commit(ctx))
	}
	env.assertNoTableDiff(t, table)

	// A difference in a regular column is found and repaired; the target
	// computes its generated column itself.
	tx, err := env.N2Pool.Begin(ctx)
	require.NoError(t, err)
	env.withRepairModeTx(t, ctx, tx, func() {
		_, err := tx.Exec(ctx, "UPDATE "+table+" SET a = 21 WHERE id = 2") // nosemgrep
		require.NoError(t, err)
	})
	require.NoError(t, tx.Commit(ctx))
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	report := readDiffReport(t, diffFile)
	require.Equal(t, 1, report.Summary.DiffRowsCount[env.pairKey()])
	for _, rows := range report.NodeDiffs[env.pairKey()].Rows {
		for _, row := range rows {
			_, has := row.Get("g")
			require.False(t, has, "a generated column is not in the report")
		}
	}

	task := env.newTableRepairTask(env.ServiceN1, table, diffFile)
	require.NoError(t, task.Run(false))
	env.assertNoTableDiff(t, table)
	var g int
	require.NoError(t, env.N2Pool.QueryRow(ctx, "SELECT g FROM "+table+" WHERE id = 2").Scan(&g)) // nosemgrep
	require.Equal(t, 60, g, "the target computes its own generated column")
}

// TestGeneratedKeyColumnRefused: a generated column is not read, so ACE
// cannot find the rows of a table whose primary key has one. table-diff
// stops with an error that names the column instead of a report without
// the key.
func TestGeneratedKeyColumnRefused(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	table := fmt.Sprintf("%s.%s", testSchema, "ace_generated_key")
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TABLE "+table+" (a int, g int GENERATED ALWAYS AS (a + 1) STORED PRIMARY KEY)") // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			execOK(t, context.Background(), pool, "DROP TABLE IF EXISTS "+table+" CASCADE") // nosemgrep
		}
	})

	task := env.newTableDiffTask(t, table, []string{env.ServiceN1, env.ServiceN2})
	task.Ctx = ctx
	err := task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "generated column g")
}
