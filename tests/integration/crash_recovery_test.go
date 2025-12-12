package integration

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/repair"
	"github.com/stretchr/testify/require"
)

func TestTableDiffOnlyOriginWithUntil(t *testing.T) {
	ctx := context.Background()

	// Light netem delay on n3 to mimic slower replication
	if err := addNetemDelay(ctx, serviceN3, "200ms"); err != nil {
		t.Logf("warning: failed to add netem delay on %s: %v (continuing)", serviceN3, err)
	}
	defer removeNetemDelay(ctx, serviceN3)

	tableName := "crash_customers"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)

	// Create table across nodes and add to default repset
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		createSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, payload TEXT);`, qualified)
		if _, err := pool.Exec(ctx, createSQL); err != nil {
			t.Fatalf("failed to create table on %s: %v", nodeName, err)
		}
		addToRepSetSQL := fmt.Sprintf(`SELECT spock.repset_add_table('default', '%s');`, qualified)
		if _, err := pool.Exec(ctx, addToRepSetSQL); err != nil {
			t.Fatalf("failed to add table to repset on %s: %v", nodeName, err)
		}
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool, pgCluster.Node3Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE;", qualified))
		}
	})

	// Insert a few rows on n3; record last commit ts (used for reference)
	insertedIDs := []int{1001, 1002, 1003}
	var lastCommitTs time.Time
	for _, id := range insertedIDs {
		_, err := pgCluster.Node3Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, payload) VALUES ($1, $2)", qualified), id, fmt.Sprintf("from n3 %d", id))
		if err != nil {
			t.Fatalf("insert on n3 failed: %v", err)
		}
		// capture commit timestamp for this row
		var ts time.Time
		if err := pgCluster.Node3Pool.QueryRow(ctx, fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE id=$1", qualified), id).Scan(&ts); err != nil {
			t.Fatalf("failed to read commit ts for id %d: %v", id, err)
		}
		if ts.After(lastCommitTs) {
			lastCommitTs = ts
		}
	}

	// Allow replication to catch on n1
	assertEventually(t, 30*time.Second, func() error {
		var count int
		if err := pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualified), insertedIDs).Scan(&count); err != nil {
			return err
		}
		if count < len(insertedIDs) {
			return fmt.Errorf("expected %d rows on n1, got %d", len(insertedIDs), count)
		}
		return nil
	})

	// Allow replication to catch on n2
	assertEventually(t, 30*time.Second, func() error {
		var count int
		if err := pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id = ANY($1)", qualified), insertedIDs).Scan(&count); err != nil {
			return err
		}
		if count < len(insertedIDs) {
			return fmt.Errorf("expected %d rows on n2, got %d", len(insertedIDs), count)
		}
		return nil
	})

	// Simulate failed replication to n2 by deleting one row locally (repair_mode prevents replication)
	tx, err := pgCluster.Node2Pool.Begin(ctx)
	require.NoError(t, err, "begin tx on n2")
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(true)")
	require.NoError(t, err, "enable repair_mode on n2")
	_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id=$1", qualified), insertedIDs[0])
	require.NoError(t, err, "delete row on n2")
	_, err = tx.Exec(ctx, "SELECT spock.repair_mode(false)")
	require.NoError(t, err, "disable repair_mode on n2")
	require.NoError(t, tx.Commit(ctx), "commit delete on n2")

	// Stop n3 to mimic failure
	if err := stopService(ctx, serviceN3); err != nil {
		t.Fatalf("failed to stop %s: %v", serviceN3, err)
	}
	t.Cleanup(func() {
		// best-effort restart to not break subsequent tests
		if err := startService(ctx, serviceN3); err != nil {
			t.Logf("cleanup: failed to restart %s: %v", serviceN3, err)
		}
	})

	// Run table-diff on survivors focusing on origin n3 up to last commit ts
	task := diff.NewTableDiffTask()
	task.ClusterName = pgCluster.ClusterName
	task.QualifiedTableName = qualified
	task.DBName = dbName
	task.Nodes = strings.Join([]string{serviceN1, serviceN2}, ",")
	task.Output = "json"
	task.BlockSize = 1000
	task.CompareUnitSize = 100
	task.ConcurrencyFactor = 1
	task.MaxDiffRows = 100
	task.OnlyOrigin = "n3"
	fence := time.Now().Add(5 * time.Minute)
	task.Until = fence.Format(time.RFC3339)

	require.NoError(t, task.Validate())
	require.NoError(t, task.RunChecks(false))
	err = task.ExecuteTask()
	require.NoError(t, err)

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := task.DiffResult.NodeDiffs[pairKey]
	if !ok {
		t.Fatalf("expected diffs for %s", pairKey)
	}
	if got := len(nodeDiffs.Rows[serviceN1]); got != 1 {
		t.Fatalf("expected exactly 1 row present on %s (missing on %s) for origin n3, got %d", serviceN1, serviceN2, got)
	}
	if got := len(nodeDiffs.Rows[serviceN2]); got != 0 {
		t.Fatalf("expected no rows present only on %s for origin n3 (they should be missing), got %d", serviceN2, got)
	}
	if val, _ := nodeDiffs.Rows[serviceN1][0].Get("id"); val != int32(insertedIDs[0]) {
		t.Fatalf("expected missing id %d on %s, got %v", insertedIDs[0], serviceN2, val)
	}
	if metaVal, ok := nodeDiffs.Rows[serviceN1][0].Get("_spock_metadata_"); ok {
		if m, ok := metaVal.(map[string]any); ok {
			require.Contains(t, m, "node_origin", "spock metadata should include node_origin")
		}
	}
	if task.DiffResult.Summary.OnlyOrigin == "" {
		t.Fatalf("diff summary should record only_origin")
	}
	if got := task.DiffResult.Summary.Until; got == "" {
		t.Fatalf("diff summary should record until cutoff")
	}
	log.Printf("only-origin diff detected %d row missing on %s (origin n3)", len(nodeDiffs.Rows[serviceN1]), serviceN2)

	// Run recovery-mode repair using the diff file and ensure n2 is healed
	repairTask := repair.NewTableRepairTask()
	repairTask.ClusterName = pgCluster.ClusterName
	repairTask.QualifiedTableName = qualified
	repairTask.DBName = dbName
	repairTask.Nodes = strings.Join([]string{serviceN1, serviceN2}, ",")
	repairTask.DiffFilePath = task.DiffFilePath
	repairTask.RecoveryMode = true
	repairTask.SourceOfTruth = serviceN1 // explicit SoT to avoid relying on LSN availability in test container setup
	repairTask.Ctx = context.Background()

	require.NoError(t, repairTask.ValidateAndPrepare())
	require.NoError(t, repairTask.Run(true))

	var countAfter int
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id=$1", qualified), insertedIDs[0]).Scan(&countAfter))
	require.Equal(t, 1, countAfter, "recovery repair should restore missing row on n2")
}

func addNetemDelay(ctx context.Context, service, delay string) error {
	container, err := pgCluster.Cluster.ServiceContainer(ctx, service)
	if err != nil {
		return err
	}
	_, _, err = container.Exec(ctx, []string{"tc", "qdisc", "add", "dev", "eth0", "root", "netem", "delay", delay})
	return err
}

func removeNetemDelay(ctx context.Context, service string) {
	container, err := pgCluster.Cluster.ServiceContainer(ctx, service)
	if err != nil {
		return
	}
	container.Exec(ctx, []string{"tc", "qdisc", "del", "dev", "eth0", "root"})
}

func stopService(ctx context.Context, service string) error {
	container, err := pgCluster.Cluster.ServiceContainer(ctx, service)
	if err != nil {
		return err
	}
	timeout := 5 * time.Second
	return container.Stop(ctx, &timeout)
}

func startService(ctx context.Context, service string) error {
	container, err := pgCluster.Cluster.ServiceContainer(ctx, service)
	if err != nil {
		return err
	}
	return container.Start(ctx)
}

func assertEventually(t *testing.T, timeout time.Duration, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if err := fn(); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}
		time.Sleep(1 * time.Second)
	}
}
