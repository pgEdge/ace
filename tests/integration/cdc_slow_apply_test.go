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
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// TestCDCDrainSlowApplyKeepsWalsender checks a bounded drain whose apply
// waits longer than wal_sender_timeout. The drain must keep answering the
// server, so the walsender is not ended, and its status updates must not
// confirm the transaction that waits.
func TestCDCDrainSlowApplyKeepsWalsender(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_cdc_slow_apply"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	slotName := config.Cfg.MTree.CDC.SlotName
	pool := pgCluster.Node1Pool

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Errorf("MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	// A short timeout on node 1 only. The Spock walsenders of the cluster use
	// the same setting, so it is reset at the end.
	const walSenderTimeout = 2 * time.Second
	_, err := pool.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET wal_sender_timeout = '%dms'", walSenderTimeout.Milliseconds()))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, "SELECT pg_reload_conf()")
	require.NoError(t, err)
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "ALTER SYSTEM RESET wal_sender_timeout"); err != nil {
			t.Errorf("failed to reset wal_sender_timeout: %v", err)
		}
		if _, err := pool.Exec(context.Background(), "SELECT pg_reload_conf()"); err != nil {
			t.Errorf("failed to reload the configuration: %v", err)
		}
	})

	beforeUpdate := walInsertLSN(t, ctx)
	_, err = pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = email || '.slow' WHERE index = 1", qualifiedTableName)) // nosemgrep
	require.NoError(t, err)

	// Another transaction holds every leaf of the tree, so the apply of the
	// update waits for it.
	mtreeTable := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)}.Sanitize()
	lockTx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer lockTx.Rollback(context.Background())
	_, err = lockTx.Exec(ctx, fmt.Sprintf("SELECT 1 FROM %s WHERE node_level = 0 FOR UPDATE", mtreeTable)) // nosemgrep
	require.NoError(t, err)

	drainCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	drainErr := make(chan error, 1)
	go func() {
		drainErr <- cdc.UpdateFromCDC(drainCtx, pgCluster.ClusterNodes[0])
	}()

	require.Eventually(t, func() bool {
		var n int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE wait_event_type = 'Lock' AND query LIKE '%ace_mtree_%' AND pid <> pg_backend_pid()`).Scan(&n)
		return err == nil && n > 0
	}, 30*time.Second, 50*time.Millisecond, "the apply should wait for the lock on the tree")

	var pidBefore *int32
	require.NoError(t, pool.QueryRow(ctx, "SELECT active_pid FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&pidBefore))
	require.NotNil(t, pidBefore, "the drain should hold the slot")

	// Three timeouts: without status updates the walsender would end.
	time.Sleep(3 * walSenderTimeout)

	var pidAfter *int32
	var confirmed string
	require.NoError(t, pool.QueryRow(ctx, "SELECT active_pid, confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&pidAfter, &confirmed))
	require.NotNil(t, pidAfter, "the walsender of the drain should still be active")
	require.Equal(t, *pidBefore, *pidAfter, "the walsender of the drain should not be ended")
	require.LessOrEqual(t, mustParseLSN(t, confirmed), beforeUpdate,
		"the slot should not confirm the transaction whose apply waits")

	require.NoError(t, lockTx.Rollback(ctx))
	select {
	case err := <-drainErr:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("the drain did not finish after the lock was released")
	}

	require.Greater(t, metadataStartLSN(t, ctx), beforeUpdate, "start_lsn should pass the update")
	var dirty int
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE node_level = 0 AND dirty", mtreeTable)).Scan(&dirty)) // nosemgrep
	require.Equal(t, 1, dirty, "the leaf of the updated row should be dirty")
}

func walInsertLSN(t *testing.T, ctx context.Context) pglogrepl.LSN {
	t.Helper()
	var lsnStr string
	require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn()").Scan(&lsnStr))
	return mustParseLSN(t, lsnStr)
}
