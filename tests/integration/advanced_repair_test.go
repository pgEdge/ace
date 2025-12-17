// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package integration

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests exercise advanced repair plans (selectors + actions) end-to-end against the
// dockerised cluster spun up by the integration suite.

func TestAdvancedRepairPlan_MixedSelectorsAndActions(t *testing.T) {
	ctx := context.Background()
	qualifiedTableName := "public.customers"

	setupDivergence(t, ctx, qualifiedTableName, false)
	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	plan := `
version: 1
default_action: { type: keep_n1 }
tables:
  public.customers:
    rules:
      - name: prefer_n2_for_modified
        diff_type: [row_mismatch]
        pk_in:
          - equals: [1]
          - equals: [2]
        action: { type: keep_n2 }
      - name: insert_missing_on_n2
        diff_type: [missing_on_n2]
        action:
          type: apply_from
          from: n1
          mode: insert
      - name: insert_missing_on_n1
        diff_type: [missing_on_n1]
        action:
          type: apply_from
          from: n2
          mode: insert
      - name: coalesce_email
        columns_changed: [email]
        action:
          type: custom
          helpers:
            coalesce_priority: [n2, n1]
`
	planPath := filepath.Join(t.TempDir(), "repair.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))

	task := newTestTableRepairTask("", qualifiedTableName, diffFile)
	task.RepairPlanPath = planPath

	require.NoError(t, task.ValidateAndPrepare())
	require.NoError(t, task.Run(true))

	assertNoTableDiff(t, qualifiedTableName)

	// Rows 1 and 2 should now reflect the n2 version (modified emails) on both nodes.
	var email1 string
	require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx, "SELECT email FROM "+qualifiedTableName+" WHERE index = 1").Scan(&email1))
	require.Equal(t, "modified.email1@example.com", email1)
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, "SELECT email FROM "+qualifiedTableName+" WHERE index = 1").Scan(&email1))
	require.Equal(t, "modified.email1@example.com", email1)
}

func TestAdvancedRepairPlan_DeleteAndWhenPredicate(t *testing.T) {
	ctx := context.Background()
	qualifiedTableName := "public.customers"

	setupDivergence(t, ctx, qualifiedTableName, false)
	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	plan := `
version: 1
tables:
  public.customers:
    default_action: { type: keep_n1 }
    rules:
      - name: delete_extras_on_n2
        diff_type: [missing_on_n1]
        action: { type: delete }
      - name: insert_missing_on_n2
        diff_type: [missing_on_n2]
        action:
          type: apply_from
          from: n1
          mode: insert
      - name: prefer_modified_from_n2
        diff_type: [row_mismatch]
        columns_changed: [email]
        when: "NOT (n2.email = n1.email)"
        action: { type: keep_n2 }
`
	planPath := filepath.Join(t.TempDir(), "repair.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))

	task := newTestTableRepairTask("", qualifiedTableName, diffFile)
	task.RepairPlanPath = planPath

	require.NoError(t, task.ValidateAndPrepare())
	require.NoError(t, task.Run(true))

	// Extras on n2 should be deleted instead of copied, so counts should match the n1 baseline (7 rows).
	countN1 := getTableCount(t, ctx, pgCluster.Node1Pool, qualifiedTableName)
	countN2 := getTableCount(t, ctx, pgCluster.Node2Pool, qualifiedTableName)
	require.Equal(t, countN1, countN2)
	require.Equal(t, 7, countN1)

	assertNoTableDiff(t, qualifiedTableName)

	// Modified emails from n2 should be present on both sides due to the when predicate.
	var email1 string
	require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx, "SELECT email FROM "+qualifiedTableName+" WHERE index = 1").Scan(&email1))
	require.Equal(t, "modified.email1@example.com", email1)
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, "SELECT email FROM "+qualifiedTableName+" WHERE index = 1").Scan(&email1))
	require.Equal(t, "modified.email1@example.com", email1)
}
