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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// These tests cover the detailed error table-diff raises when its cheap
// column/key-name check finds the two nodes disagree: RunChecks hands off
// to diagnoseSchemaMismatch, which collects both nodes' structure and
// reports what actually differs instead of just saying that something does.

const mismatchTable = "ace_mismatch_test"

// setupKeyMismatch gives both nodes the same columns but different primary
// keys, which is what makes RunChecks' key comparison disagree while its
// column comparison does not.
func setupKeyMismatch(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	drop := fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`, testSchema, mismatchTable)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, drop)
		require.NoError(t, err)
	}

	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s.%s (id int NOT NULL, tenant int NOT NULL, note text, PRIMARY KEY (id))`,
		testSchema, mismatchTable))
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s.%s (id int NOT NULL, tenant int NOT NULL, note text, PRIMARY KEY (id, tenant))`,
		testSchema, mismatchTable))
	require.NoError(t, err)

	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, drop) //nolint:errcheck // best-effort cleanup
		}
	})
}

// TestTableDiffSchemaMismatch_KeyColumnsAreReadable: key_columns is
// compared in the packed form, so a key of the columns (a, b) stays
// distinct from a key of the one column named "a,b". That form must not
// reach the error text: a two-column key reads "id, tenant".
func TestTableDiffSchemaMismatch_KeyColumnsAreReadable(t *testing.T) {
	setupKeyMismatch(t)

	qualified := fmt.Sprintf("%s.%s", testSchema, mismatchTable)
	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.Ctx = context.Background()

	err := task.RunChecks(false)
	require.Error(t, err, "the two nodes have different primary keys, so the check must fail")

	msg := err.Error()
	require.Contains(t, msg, "key_columns",
		"the error should name the property that differs: %s", msg)
	require.Contains(t, msg, "id, tenant",
		"a two-column key must be readable: %s", msg)
	require.NotContains(t, msg, "6:tenant",
		"the packed form escaped into the error: %s", msg)
}

// TestTableDiffSchemaMismatch_NarrowingNamesTheNarrowSide: a narrowing is
// only actionable if the message says which node is the narrow one.
func TestTableDiffSchemaMismatch_NarrowingNamesTheNarrowSide(t *testing.T) {
	ctx := context.Background()
	drop := fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s`, testSchema, mismatchTable)
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, drop)
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			pool.Exec(ctx, drop) //nolint:errcheck // best-effort cleanup
		}
	})

	// Different column sets, so RunChecks' column comparison disagrees, and
	// a differing type on the column they share, so the collected structure
	// has a narrowing in it to report.
	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s.%s (id int PRIMARY KEY, qty int8)`, testSchema, mismatchTable))
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE %s.%s (id int PRIMARY KEY, qty int4, extra text)`, testSchema, mismatchTable))
	require.NoError(t, err)

	qualified := fmt.Sprintf("%s.%s", testSchema, mismatchTable)
	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.Ctx = context.Background()

	err = task.RunChecks(false)
	require.Error(t, err)

	msg := err.Error()
	require.Contains(t, msg, "narrower on",
		"a narrowing must name the narrow node: %s", msg)
	require.Contains(t, msg, serviceN2,
		"node2 holds the int4, so it is the narrow side: %s", msg)
}
