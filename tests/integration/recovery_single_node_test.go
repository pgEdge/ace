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
//
// TestCatastrophicSingleNodeFailure exercises Scenario 1 from the Spock
// catastrophic-node-failure recovery documentation:
//
//	https://github.com/pgEdge/spock/blob/main/docs/recovery/catastrophic_node_failure.md
//
// Lag is produced realistically: n2's Spock subscription to n1 is disabled
// before the inserts/update so n2 genuinely never processes those WAL records.
// n1 is then stopped before n2 can catch up, leaving n2 with a real LSN gap
// rather than artificially deleted rows.
//
// Covered cases:
//
//  1. Missing inserts   – rows inserted on n1 while n2's subscription is down;
//     n3 receives them, n2 never does.
//  2. Stale update      – a row that exists on both nodes in version v1; n1
//     updates it to v2 while n2 is paused; n3 receives the update so n3 is
//     genuinely ahead of n2 for that row.
//  3. Preserve-origin   – after repair the rows on n2 carry n1's origin ID
//     and the original commit timestamp (to microsecond precision).
//
// Documented gaps and bugs found during testing are captured inline below.

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/repair"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// writeN2N3ClusterJSON writes a temporary cluster JSON containing only n2 and
// n3.  We need this because the shared test_cluster.json only contains n1 and
// n2, but the diff/repair in this test runs on the n2+n3 survivor pair (n1 is
// the failed origin node).  The file is removed in t.Cleanup.
func writeN2N3ClusterJSON(t *testing.T) string {
	t.Helper()
	clusterName := fmt.Sprintf("test_cluster_n2_n3_%d", time.Now().UnixNano())
	cfg := types.ClusterConfig{
		JSONVersion: "1.0",
		ClusterName: clusterName,
		LogLevel:    "info",
		UpdateDate:  time.Now().Format(time.RFC3339),
		PGEdge: struct {
			PGVersion int               `json:"pg_version"`
			AutoStart string            `json:"auto_start"`
			Spock     types.SpockConfig `json:"spock"`
			Databases []types.Database  `json:"databases"`
		}{
			PGVersion: 16,
			AutoStart: "yes",
			Spock: types.SpockConfig{
				SpockVersion: "4.0.10",
				AutoDDL:      "yes",
			},
			Databases: []types.Database{
				{
					DBName:     dbName,
					DBUser:     pgEdgeUser,
					DBPassword: pgEdgePassword,
				},
			},
		},
		NodeGroups: []types.NodeGroup{
			{
				Name:     serviceN2,
				IsActive: "yes",
				PublicIP: pgCluster.Node2Host,
				Port:     pgCluster.Node2Port,
				Path:     "/usr/local/bin",
				SSH: struct {
					OSUser     string `json:"os_user"`
					PrivateKey string `json:"private_key"`
				}{OSUser: "pgedge"},
			},
			{
				Name:     serviceN3,
				IsActive: "yes",
				PublicIP: pgCluster.Node3Host,
				Port:     pgCluster.Node3Port,
				Path:     "/usr/local/bin",
				SSH: struct {
					OSUser     string `json:"os_user"`
					PrivateKey string `json:"private_key"`
				}{OSUser: "pgedge"},
			},
		},
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	path := clusterName + ".json"
	require.NoError(t, os.WriteFile(path, data, 0644))
	t.Cleanup(func() { _ = os.Remove(path) })
	return clusterName
}

// findSubscriptionName returns the Spock subscription name on pool that pulls
// from the node named originNodeName.
func findSubscriptionName(t *testing.T, pool *pgxpool.Pool, originNodeName string) string {
	t.Helper()
	var subName string
	err := pool.QueryRow(context.Background(), `
		SELECT s.sub_name
		FROM spock.subscription s
		JOIN spock.node n ON s.sub_origin = n.node_id
		WHERE n.node_name = $1
	`, originNodeName).Scan(&subName)
	require.NoError(t, err, "could not find subscription for origin %q", originNodeName)
	return subName
}

// TestCatastrophicSingleNodeFailure tests the single-node-failure recovery
// scenario from the Spock catastrophic failure documentation.
//
// DOCUMENTATION BUG FOUND (Phase 3):
//
//	The doc shows: ./ace table-diff --preserve-origin n1 --until <t> ...
//	Correct flag:  --against-origin  (--preserve-origin is a table-repair flag)
//
// ADDITIONAL GAP FOUND:
//
//	--against-origin expects the Spock node name as known to the surviving
//	nodes' spock.node catalog (e.g. "n1"), not the host/service name
//	(e.g. "postgres-n1"). The documentation uses abstract names that happen to
//	match but does not explicitly state this distinction.
func TestCatastrophicSingleNodeFailure(t *testing.T) {
	ctx := context.Background()

	// Ensure n1 and n3 are up in case a previous test stopped them.
	_ = startService(ctx, serviceN1)
	_ = startService(ctx, serviceN3)

	tableName := "recovery_customers"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)

	allPools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool}
	allServices := []string{serviceN1, serviceN2, serviceN3}

	// -----------------------------------------------------------------------
	// Setup: create the table on all three nodes and add to the default repset.
	// -----------------------------------------------------------------------
	for i, pool := range allPools {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, payload TEXT, version INT);`,
			qualified,
		))
		require.NoError(t, err, "create table on %s", allServices[i])

		_, err = pool.Exec(ctx, fmt.Sprintf(
			`SELECT spock.repset_add_table('default', '%s');`, qualified,
		))
		if err != nil {
			t.Logf("repset_add_table on %s (may already exist): %v", allServices[i], err)
		}
	}
	t.Cleanup(func() {
		_ = startService(ctx, serviceN1)
		for _, pool := range allPools {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", qualified)) //nolint:errcheck
		}
	})

	// -----------------------------------------------------------------------
	// Baseline: insert a row that exists on ALL nodes before we induce lag.
	// This row will be updated later (the "n3 ahead of n2" case).
	// -----------------------------------------------------------------------
	updatedID := 40
	_, err := pgCluster.Node1Pool.Exec(ctx,
		fmt.Sprintf("INSERT INTO %s (id, payload, version) VALUES ($1,$2,$3)", qualified),
		updatedID, fmt.Sprintf("payload-%d-v1", updatedID), 1,
	)
	require.NoError(t, err)

	// Wait until all three nodes have the baseline row.
	assertEventually(t, 30*time.Second, func() error {
		for _, pool := range allPools {
			var count int
			if err := pool.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s WHERE id=$1", qualified), updatedID,
			).Scan(&count); err != nil {
				return err
			}
			if count == 0 {
				return fmt.Errorf("baseline row not yet on all nodes")
			}
		}
		return nil
	})
	log.Printf("Baseline row id=%d present on all nodes", updatedID)

	// -----------------------------------------------------------------------
	// Induce real lag on n2: disable its Spock subscription to n1.
	//
	// From this point forward any WAL emitted by n1 will be queued in n1's
	// replication slot for n2 but n2's apply worker will not consume it.
	// n3's subscription to n1 remains active so n3 stays current.
	// -----------------------------------------------------------------------
	subName := findSubscriptionName(t, pgCluster.Node2Pool, "n1")
	_, err = pgCluster.Node2Pool.Exec(ctx,
		fmt.Sprintf("SELECT spock.sub_disable('%s', true)", subName),
	)
	require.NoError(t, err, "disable n2 subscription to n1 (%s)", subName)
	t.Cleanup(func() {
		_, _ = pgCluster.Node2Pool.Exec(ctx,
			fmt.Sprintf("SELECT spock.sub_enable('%s')", subName))
	})
	log.Printf("Disabled n2 subscription %q to n1 – real lag begins", subName)

	// -----------------------------------------------------------------------
	// While n2 is lagging, insert new rows and update the baseline on n1.
	// n3 receives everything; n2 genuinely never sees these WAL records.
	// -----------------------------------------------------------------------
	missingOnN2IDs := []int{50, 60}
	for _, id := range missingOnN2IDs {
		_, err = pgCluster.Node1Pool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s (id, payload, version) VALUES ($1,$2,$3)", qualified),
			id, fmt.Sprintf("payload-%d-v1", id), 1,
		)
		require.NoError(t, err, "insert id=%d on n1 while n2 is lagging", id)
	}

	// Update the baseline row to v2 – n3 will receive this; n2 will not.
	// This is the "n3 is ahead of n2" case the requirement asks for.
	_, err = pgCluster.Node1Pool.Exec(ctx,
		fmt.Sprintf("UPDATE %s SET payload=$1, version=2 WHERE id=$2", qualified),
		fmt.Sprintf("payload-%d-v2", updatedID), updatedID,
	)
	require.NoError(t, err, "update id=%d on n1 while n2 is lagging", updatedID)

	// Wait until n3 has received both the new rows and the update.
	assertEventually(t, 30*time.Second, func() error {
		var count int
		if err := pgCluster.Node3Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualified),
			missingOnN2IDs,
		).Scan(&count); err != nil {
			return err
		}
		if count < len(missingOnN2IDs) {
			return fmt.Errorf("n3 waiting for missing rows: got %d of %d", count, len(missingOnN2IDs))
		}
		var ver int
		if err := pgCluster.Node3Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT version FROM %s WHERE id=$1", qualified), updatedID,
		).Scan(&ver); err != nil {
			return err
		}
		if ver != 2 {
			return fmt.Errorf("n3 waiting for version=2 on id=%d, got %d", updatedID, ver)
		}
		return nil
	})

	// Capture n3's commit timestamp for the updated row – this is the
	// canonical n1-originated timestamp that preserve-origin must reproduce.
	var n3UpdatedTS time.Time
	require.NoError(t, pgCluster.Node3Pool.QueryRow(ctx,
		fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE id=$1", qualified),
		updatedID,
	).Scan(&n3UpdatedTS))
	log.Printf("n3 has all lagged rows and update (id=%d v2 commit_ts=%s)", updatedID, n3UpdatedTS)

	// Confirm n2 still has the old version and is missing the inserted rows –
	// this is genuine WAL lag, not artificial deletion.
	var n2Version int
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
		fmt.Sprintf("SELECT version FROM %s WHERE id=$1", qualified), updatedID,
	).Scan(&n2Version))
	require.Equal(t, 1, n2Version, "n2 should still have v1 (subscription was disabled)")

	for _, id := range missingOnN2IDs {
		var count int
		require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE id=$1", qualified), id,
		).Scan(&count))
		require.Zero(t, count, "id=%d should not be on n2 yet (genuine lag)", id)
	}
	log.Printf("Confirmed: n2 has v1 for id=%d and is missing ids=%v (real lag)", updatedID, missingOnN2IDs)

	// -----------------------------------------------------------------------
	// Phase 2 (doc): n1 fails – stop it now before n2 can catch up.
	// n2's subscription was disabled so its replication slot on n1 still holds
	// all the undelivered WAL, but with n1 gone that data is unreachable.
	// -----------------------------------------------------------------------
	failureTime := time.Now().Add(5 * time.Minute).UTC()
	require.NoError(t, stopService(ctx, serviceN1), "stop n1 to simulate catastrophic failure")
	t.Cleanup(func() {
		if err := startService(ctx, serviceN1); err != nil {
			t.Logf("cleanup: failed to restart n1: %v", err)
		}
	})
	log.Printf("n1 stopped. --until fence: %s", failureTime.Format(time.RFC3339))

	// -----------------------------------------------------------------------
	// Phase 3 (doc): Identify missing data with table-diff.
	//
	// DOCUMENTATION BUG: the doc writes "--preserve-origin n1" for table-diff.
	// The correct flag is "--against-origin".  --preserve-origin is only valid
	// for table-repair.
	// -----------------------------------------------------------------------
	clusterName := writeN2N3ClusterJSON(t)

	diffTask := diff.NewTableDiffTask()
	diffTask.ClusterName = clusterName
	diffTask.QualifiedTableName = qualified
	diffTask.DBName = dbName
	diffTask.Nodes = strings.Join([]string{serviceN2, serviceN3}, ",")
	diffTask.Output = "json"
	diffTask.BlockSize = 1000
	diffTask.CompareUnitSize = 100
	diffTask.ConcurrencyFactor = 1
	diffTask.MaxDiffRows = math.MaxInt64
	// --against-origin takes the Spock node name ("n1"), not the service name.
	diffTask.AgainstOrigin = "n1"
	diffTask.Until = failureTime.Format(time.RFC3339)
	diffTask.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Nodes:             []string{serviceN2, serviceN3},
			BlockSize:         diffTask.BlockSize,
			CompareUnitSize:   diffTask.CompareUnitSize,
			ConcurrencyFactor: diffTask.ConcurrencyFactor,
			DiffRowsCount:     make(map[string]int),
		},
	}

	require.NoError(t, diffTask.Validate())
	require.NoError(t, diffTask.RunChecks(false))
	require.NoError(t, diffTask.ExecuteTask())

	require.NotEmpty(t, diffTask.DiffFilePath, "diff should produce a file")
	require.NotEmpty(t, diffTask.DiffResult.Summary.AgainstOrigin, "summary should record against_origin")
	require.NotEmpty(t, diffTask.DiffResult.Summary.Until, "summary should record until")

	pairKey := serviceN2 + "/" + serviceN3
	if strings.Compare(serviceN2, serviceN3) > 0 {
		pairKey = serviceN3 + "/" + serviceN2
	}
	nodeDiffs, ok := diffTask.DiffResult.NodeDiffs[pairKey]
	require.True(t, ok, "expected diffs for pair %s", pairKey)

	// n3 is ahead: it has the rows n2 never received.
	n3Rows := nodeDiffs.Rows[serviceN3]
	n2Rows := nodeDiffs.Rows[serviceN2]

	// For missing inserts (rows 50, 60): only appear on n3 side.
	// For the stale update (row 40): BOTH sides carry their version of the row
	// (n3=v2 on the n3 side, n2=v1 on the n2 side) — this is how ACE
	// represents a mismatch that exists on both nodes with different values.
	// n3 side: 2 missing inserts + 1 mismatch = 3 rows total.
	// n2 side: 1 mismatch (the stale v1 row) = 1 row.
	require.Len(t, n3Rows, len(missingOnN2IDs)+1,
		"n3 should show %d rows (missing inserts + stale update)", len(missingOnN2IDs)+1)
	require.Len(t, n2Rows, 1,
		"n2 side should show exactly 1 row (the stale version of id=%d)", updatedID)

	// The single n2-side row must be the stale version of updatedID.
	staleID, _ := n2Rows[0].Get("id")
	require.Equal(t, int32(updatedID), staleID,
		"n2-side diff row should be id=%d (the stale update)", updatedID)
	staleVersion, _ := n2Rows[0].Get("version")
	require.Equal(t, int32(1), staleVersion,
		"n2-side diff row should still be version=1 (the old value)")

	// Verify _spock_metadata_ is populated in the diff rows.
	for _, row := range n3Rows {
		metaRaw, ok := row.Get("_spock_metadata_")
		require.True(t, ok, "_spock_metadata_ must be present in every diff row")
		meta, ok := metaRaw.(map[string]any)
		require.True(t, ok, "_spock_metadata_ must be a map, got %T", metaRaw)
		require.Contains(t, meta, "node_origin", "_spock_metadata_ must include node_origin")
		require.Contains(t, meta, "commit_ts", "_spock_metadata_ must include commit_ts")
	}
	log.Printf("Phase 3 complete: diff found %d diffs on n3 vs n2 for origin n1", len(n3Rows))

	// -----------------------------------------------------------------------
	// Phase 4 (doc): Repair n2 from n3 with --recovery-mode --preserve-origin.
	// -----------------------------------------------------------------------
	repairTask := repair.NewTableRepairTask()
	repairTask.ClusterName = clusterName
	repairTask.QualifiedTableName = qualified
	repairTask.DBName = dbName
	repairTask.Nodes = strings.Join([]string{serviceN2, serviceN3}, ",")
	repairTask.DiffFilePath = diffTask.DiffFilePath
	repairTask.RecoveryMode = true
	repairTask.SourceOfTruth = serviceN3
	repairTask.PreserveOrigin = true
	repairTask.Ctx = context.Background()

	require.NoError(t, repairTask.ValidateAndPrepare())
	require.NoError(t, repairTask.Run(true))
	log.Printf("Phase 4 complete: repair with preserve-origin=true finished")

	// -----------------------------------------------------------------------
	// Phase 5 (doc): Validate – full diff without --against-origin must show
	// no differences.
	// -----------------------------------------------------------------------
	validateTask := diff.NewTableDiffTask()
	validateTask.ClusterName = clusterName
	validateTask.QualifiedTableName = qualified
	validateTask.DBName = dbName
	validateTask.Nodes = strings.Join([]string{serviceN2, serviceN3}, ",")
	validateTask.Output = "json"
	validateTask.BlockSize = 1000
	validateTask.CompareUnitSize = 100
	validateTask.ConcurrencyFactor = 1
	validateTask.MaxDiffRows = math.MaxInt64
	validateTask.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Nodes:             []string{serviceN2, serviceN3},
			BlockSize:         validateTask.BlockSize,
			CompareUnitSize:   validateTask.CompareUnitSize,
			ConcurrencyFactor: validateTask.ConcurrencyFactor,
			DiffRowsCount:     make(map[string]int),
		},
	}
	require.NoError(t, validateTask.Validate())
	require.NoError(t, validateTask.RunChecks(false))
	require.NoError(t, validateTask.ExecuteTask())

	require.Empty(t, validateTask.DiffResult.NodeDiffs,
		"n2 and n3 should fully match after recovery")
	log.Printf("Phase 5 complete: n2 and n3 are in full agreement")

	// -----------------------------------------------------------------------
	// Post-repair assertions
	// -----------------------------------------------------------------------

	// Missing inserts are now on n2.
	for _, id := range missingOnN2IDs {
		var count int
		require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT count(*) FROM %s WHERE id=$1", qualified), id,
		).Scan(&count))
		require.Equal(t, 1, count, "id=%d should be present on n2 after recovery", id)
	}

	// The stale row on n2 is now at version 2 (forwarded from n3).
	var finalVersion int
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx,
		fmt.Sprintf("SELECT version FROM %s WHERE id=$1", qualified), updatedID,
	).Scan(&finalVersion))
	require.Equal(t, 2, finalVersion,
		"id=%d: n3 was ahead with v2; after repair n2 should also have v2", updatedID)

	// Origin must be preserved as node_n1 on all repaired rows.
	for _, id := range append(missingOnN2IDs, updatedID) {
		origin := getReplicationOrigin(t, ctx, pgCluster.Node2Pool, qualified, id)
		ts := getCommitTimestamp(t, ctx, pgCluster.Node2Pool, qualified, id)
		log.Printf("n2 id=%d: origin=%q commit_ts=%s", id, origin, ts.Format(time.RFC3339Nano))
		require.Equal(t, "node_n1", origin,
			"id=%d on n2 should carry n1 origin after preserve-origin repair", id)
	}

	// Commit timestamp for the updated row must match n3's reference exactly.
	n2UpdatedTS := getCommitTimestamp(t, ctx, pgCluster.Node2Pool, qualified, updatedID)
	require.False(t, n2UpdatedTS.IsZero(), "commit_ts for id=%d on n2 must be non-zero", updatedID)
	delta := n2UpdatedTS.Sub(n3UpdatedTS)
	if delta < 0 {
		delta = -delta
	}
	require.LessOrEqual(t, delta, time.Microsecond,
		"id=%d commit_ts must be preserved to microsecond precision: n3=%s n2=%s",
		updatedID, n3UpdatedTS.Format(time.RFC3339Nano), n2UpdatedTS.Format(time.RFC3339Nano))

	log.Println("TestCatastrophicSingleNodeFailure COMPLETED")
}
