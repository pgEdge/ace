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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// These tests exercise advanced repair plans (selectors + actions) end-to-end against the
// dockerised cluster spun up by the integration suite.

func TestAdvancedRepairPlan_MixedSelectorsAndActions(t *testing.T) {
	ctx := context.Background()
	qualifiedTableName := "public.customers"

	setupDivergence(t, ctx, qualifiedTableName)
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

	setupDivergence(t, ctx, qualifiedTableName)
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

func TestAdvancedRepairPlan_StaleRepairsSkipped(t *testing.T) {
	ctx := context.Background()
	qualifiedTableName := "public.customers"

	setupDivergence(t, ctx, qualifiedTableName)
	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	beforeLogs := listStaleSkipLogs(t)

	var beforeRow1 time.Time
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE index = 1", qualifiedTableName)).Scan(&beforeRow1))
	var beforeRow2001 time.Time
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE index = 2001", qualifiedTableName)).Scan(&beforeRow2001))

	time.Sleep(10 * time.Millisecond)

	_, err := pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = $1 WHERE index = 1", qualifiedTableName), "stale.email1@example.com")
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET last_name = $1 WHERE index = 2001", qualifiedTableName), "StaleLast")
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err)

	var afterRow1 time.Time
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE index = 1", qualifiedTableName)).Scan(&afterRow1))
	require.True(t, afterRow1.After(beforeRow1), "expected row 1 commit timestamp to advance")
	var afterRow2001 time.Time
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE index = 2001", qualifiedTableName)).Scan(&afterRow2001))
	require.True(t, afterRow2001.After(beforeRow2001), "expected row 2001 commit timestamp to advance")

	plan := `
version: 1
tables:
  public.customers:
    default_action: { type: skip }
    rules:
      - name: stale_keep_n1_for_pk1
        diff_type: [row_mismatch]
        pk_in:
          - equals: [1]
        action:
          type: keep_n1
          allow_stale_repairs: false
      - name: stale_delete_missing_on_n1
        diff_type: [missing_on_n1]
        action:
          type: delete
          allow_stale_repairs: false
`
	planPath := filepath.Join(t.TempDir(), "repair.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))

	task := newTestTableRepairTask("", qualifiedTableName, diffFile)
	task.RepairPlanPath = planPath

	require.NoError(t, task.ValidateAndPrepare())
	require.NoError(t, task.Run(true))

	var email1 string
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, "SELECT email FROM "+qualifiedTableName+" WHERE index = 1").Scan(&email1))
	require.Equal(t, "stale.email1@example.com", email1)

	var count int
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, "SELECT count(*) FROM "+qualifiedTableName+" WHERE index = 2001").Scan(&count))
	require.Equal(t, 1, count)
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, "SELECT count(*) FROM "+qualifiedTableName+" WHERE index = 2002").Scan(&count))
	require.Equal(t, 0, count)

	logPath := findNewStaleSkipLog(t, beforeLogs)
	entries := readStaleSkipEntries(t, logPath)
	require.True(t, hasStaleSkipEntry(entries, 1, "upsert", "stale_keep_n1_for_pk1"))
	require.True(t, hasStaleSkipEntry(entries, 2001, "delete", "stale_delete_missing_on_n1"))
}

// TestAdvancedRepairPlan_PickFreshestByCommitTS verifies the one-pass
// latest-commit-wins plan: pick_freshest with key: commit_ts keeps the row from
// the side with the newer Spock commit timestamp. It also guards the regression
// where pick_freshest read commit_ts from the (stripped) row instead of the
// spock metadata and therefore always fell back to `tie`.
func TestAdvancedRepairPlan_PickFreshestByCommitTS(t *testing.T) {
	ctx := context.Background()
	qualifiedTableName := "public.customers"

	setupDivergence(t, ctx, qualifiedTableName)
	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	// Precondition: setupDivergence UPDATEs index 1 & 2 on n2 after the initial
	// inserts, so n2's commit_ts for those rows is strictly newer than n1's.
	// That ordering is what "latest commit wins" must resolve to.
	for _, idx := range []int{1, 2} {
		var tsN1, tsN2 time.Time
		require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE index = %d", qualifiedTableName, idx)).Scan(&tsN1))
		require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE index = %d", qualifiedTableName, idx)).Scan(&tsN2))
		require.Truef(t, tsN2.After(tsN1),
			"precondition for index %d: n2 commit_ts %s should be newer than n1 %s", idx, tsN2, tsN1)
	}

	// One-pass plan: pick_freshest by commit_ts resolves the modified rows to
	// the newer (n2) side; apply_from rules propagate the single-sided inserts
	// so the tables fully converge.
	plan := `
version: 1
tables:
  public.customers:
    rules:
      - name: latest_commit_wins
        diff_type: [row_mismatch]
        action:
          type: custom
          helpers:
            pick_freshest: { key: commit_ts, tie: n1 }
      - name: insert_missing_on_n2
        diff_type: [missing_on_n2]
        action: { type: apply_from, from: n1, mode: insert }
      - name: insert_missing_on_n1
        diff_type: [missing_on_n1]
        action: { type: apply_from, from: n2, mode: insert }
`
	planPath := filepath.Join(t.TempDir(), "repair.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))

	task := newTestTableRepairTask("", qualifiedTableName, diffFile)
	task.RepairPlanPath = planPath
	require.NoError(t, task.ValidateAndPrepare())
	require.NoError(t, task.Run(true))

	assertNoTableDiff(t, qualifiedTableName)

	// The newer (n2) side won for the modified rows, on BOTH nodes.
	for _, idx := range []int{1, 2} {
		want := fmt.Sprintf("modified.email%d@example.com", idx)
		var e1, e2 string
		require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT email FROM %s WHERE index = %d", qualifiedTableName, idx)).Scan(&e1))
		require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT email FROM %s WHERE index = %d", qualifiedTableName, idx)).Scan(&e2))
		require.Equalf(t, want, e1, "index %d on n1 should be the newer (n2) value", idx)
		require.Equalf(t, want, e2, "index %d on n2 should be the newer (n2) value", idx)
	}
}

// TestAdvancedRepairPlan_PickFreshestByAppColumn verifies pick_freshest with an
// application-level timestamp column (subscription_date). Unlike commit_ts this
// is ordinary row data, so it is immune to freezing and works even where
// commit_ts would be NULL. index 1 is newer on n2, index 2 is newer on n1, so
// the winning side differs per row.
func TestAdvancedRepairPlan_PickFreshestByAppColumn(t *testing.T) {
	ctx := context.Background()
	qualifiedTableName := "public.customers"
	env := newSpockEnv()
	t.Cleanup(func() { resetSharedTable(t, "customers") })

	// Clean slate on both nodes.
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
			require.NoError(t, err)
		})
	}

	// Same PKs on both nodes but differing first_name and subscription_date, so
	// each row is a row_mismatch resolvable by freshness of subscription_date.
	insert := func(pool *pgxpool.Pool, idx int, firstName, subDate string) {
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email, subscription_date) VALUES ($1,$2,$3,$4,$5,$6)", qualifiedTableName),
				idx, fmt.Sprintf("CUST-%d", idx), firstName, "Last", fmt.Sprintf("e%d@example.com", idx), subDate)
			require.NoError(t, err)
		})
	}
	insert(pgCluster.Node1Pool, 1, "N1", "2020-01-01 00:00:00") // index 1: n2 newer -> n2 wins
	insert(pgCluster.Node2Pool, 1, "N2", "2021-01-01 00:00:00")
	insert(pgCluster.Node1Pool, 2, "N1", "2023-01-01 00:00:00") // index 2: n1 newer -> n1 wins
	insert(pgCluster.Node2Pool, 2, "N2", "2022-01-01 00:00:00")

	diffFile := runTableDiff(t, qualifiedTableName, []string{serviceN1, serviceN2})

	plan := `
version: 1
tables:
  public.customers:
    rules:
      - name: latest_by_subscription_date
        diff_type: [row_mismatch]
        action:
          type: custom
          helpers:
            pick_freshest: { key: subscription_date, tie: n1 }
`
	planPath := filepath.Join(t.TempDir(), "repair.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))

	task := newTestTableRepairTask("", qualifiedTableName, diffFile)
	task.RepairPlanPath = planPath
	require.NoError(t, task.ValidateAndPrepare())
	require.NoError(t, task.Run(true))

	assertNoTableDiff(t, qualifiedTableName)

	// index 1 -> n2 (newer subscription_date) won; index 2 -> n1 won. The whole
	// winning row is applied, so first_name identifies the side that won.
	want := map[int]string{1: "N2", 2: "N1"}
	for idx, wantName := range want {
		var f1, f2 string
		require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT first_name FROM %s WHERE index = %d", qualifiedTableName, idx)).Scan(&f1))
		require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT first_name FROM %s WHERE index = %d", qualifiedTableName, idx)).Scan(&f2))
		require.Equalf(t, wantName, f1, "index %d on n1 should be the fresher side", idx)
		require.Equalf(t, wantName, f2, "index %d on n2 should be the fresher side", idx)
	}
}

func listStaleSkipLogs(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("reports", "*", "stale_repair_skips_*.json"))
	require.NoError(t, err)
	return paths
}

func findNewStaleSkipLog(t *testing.T, before []string) string {
	t.Helper()
	beforeSet := make(map[string]struct{}, len(before))
	for _, path := range before {
		beforeSet[path] = struct{}{}
	}
	after := listStaleSkipLogs(t)

	var newest string
	var newestMod time.Time
	for _, path := range after {
		if _, ok := beforeSet[path]; ok {
			continue
		}
		info, err := os.Stat(path)
		require.NoError(t, err)
		if newest == "" || info.ModTime().After(newestMod) {
			newest = path
			newestMod = info.ModTime()
		}
	}
	require.NotEmpty(t, newest, "expected stale repair skip log file")
	return newest
}

func readStaleSkipEntries(t *testing.T, path string) []map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var entries []map[string]any
	require.NoError(t, json.Unmarshal(data, &entries))
	return entries
}

func hasStaleSkipEntry(entries []map[string]any, index int, operation, rule string) bool {
	for _, entry := range entries {
		op, _ := entry["operation"].(string)
		if op != operation {
			continue
		}
		if rule != "" {
			if entryRule, _ := entry["rule"].(string); entryRule != rule {
				continue
			}
		}
		pk, ok := entry["pk"].(map[string]any)
		if !ok {
			continue
		}
		rawIdx, ok := pk["index"]
		if !ok {
			continue
		}
		idx, ok := rawIdx.(float64)
		if !ok || int(idx) != index {
			continue
		}
		if reason, _ := entry["reason"].(string); reason != "target_row_newer_than_diff" {
			continue
		}
		return true
	}
	return false
}
