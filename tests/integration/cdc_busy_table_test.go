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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestCDCDrainCompletesWithBusyTable(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_busy"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.NoError(t, createTestTable(ctx, pool, testSchema, tableName))
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE %s.%s", testSchema, tableName))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.customers", testSchema, tableName, testSchema))
		require.NoError(t, err)
	}

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	taskCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	mtreeTask.Ctx = taskCtx

	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	var wg sync.WaitGroup
	stopCh := make(chan struct{})
	stopOnce := sync.Once{}
	stopWriter := func() {
		stopOnce.Do(func() {
			close(stopCh)
			wg.Wait()
		})
	}
	t.Cleanup(func() {
		stopWriter()
		mtreeTask.Ctx = context.Background()
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", testSchema, tableName))
			if err != nil {
				t.Logf("Warning: failed to drop table %s.%s during cleanup: %v", testSchema, tableName, err)
			}
		}
	})

	startWriter := func(counter *uint64, stop <-chan struct{}) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					_, err := pgCluster.Node1Pool.Exec(context.Background(), fmt.Sprintf("UPDATE %s SET email = 'busy-' || clock_timestamp() WHERE index = 1", qualifiedTableName))
					if err == nil {
						atomic.AddUint64(counter, 1)
					}
				}
			}
		}()
	}

	var writeCount uint64
	startWriter(&writeCount, stopCh)

	waitDeadline := time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&writeCount) == 0 && time.Now().Before(waitDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.Greater(t, atomic.LoadUint64(&writeCount), uint64(0), "background writes should have started before CDC drain")

	lsnBefore := currentMetadataLSN(t, ctx)

	start := time.Now()
	err := mtreeTask.UpdateMtree(false)
	stopWriter()

	require.NoError(t, err)
	require.Less(t, time.Since(start), 15*time.Second, "CDC drain should respect the caller timeout on a busy table")

	lsnAfter := currentMetadataLSN(t, ctx)
	require.Greater(t, lsnAfter, lsnBefore, "CDC metadata LSN should advance after drain")

	// Run a second drain while writes continue to ensure we don't re-chase new WAL endlessly.
	var writeCount2 uint64
	stopCh2 := make(chan struct{})
	startWriter(&writeCount2, stopCh2)
	waitDeadline = time.Now().Add(2 * time.Second)
	for atomic.LoadUint64(&writeCount2) == 0 && time.Now().Before(waitDeadline) {
		time.Sleep(50 * time.Millisecond)
	}
	require.Greater(t, atomic.LoadUint64(&writeCount2), uint64(0), "second batch of writes should start")

	start2 := time.Now()
	err = mtreeTask.UpdateMtree(false)
	close(stopCh2)
	wg.Wait()

	require.NoError(t, err)
	require.Less(t, time.Since(start2), 10*time.Second, "Second CDC drain should complete promptly")
	lsnAfter2 := currentMetadataLSN(t, ctx)
	require.GreaterOrEqual(t, lsnAfter2, lsnAfter, "CDC metadata LSN should not go backwards")
}

func TestCDCUpdateHighWater(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_cdc_hw"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	startBefore := metadataStartLSN(t, ctx)

	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = email || '.cdc_hw' WHERE index = 1", qualifiedTableName))
	require.NoError(t, err)

	targetFlush := walFlushLSN(t, ctx, pgCluster.Node1Pool)

	nodeInfo := pgCluster.ClusterNodes[0]
	require.NoError(t, cdc.UpdateFromCDC(context.Background(), nodeInfo))

	startAfter := metadataStartLSN(t, ctx)
	require.True(t, startAfter > startBefore, "start_lsn should advance after processing WAL")
	require.True(t, startAfter >= targetFlush, "start_lsn should reach at least the snapshot wal_flush_lsn")
}

func TestCDCFallbackToSlotLSN(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_cdc_fallback"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	slotConfirmed := slotConfirmedFlushLSN(t, ctx, pgCluster.Node1Pool, config.Cfg.MTree.CDC.SlotName)
	var bumpedStart string
	err := pgCluster.Node1Pool.QueryRow(ctx, "SELECT ($1::pg_lsn + 16)::text", slotConfirmed).Scan(&bumpedStart)
	require.NoError(t, err)

	_, err = pgCluster.Node1Pool.Exec(ctx, "UPDATE spock.ace_cdc_metadata SET start_lsn = $1 WHERE publication_name = $2", bumpedStart, config.Cfg.MTree.CDC.PublicationName)
	require.NoError(t, err)

	_, err = pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET email = email || '.cdc_fallback' WHERE index = 2", qualifiedTableName))
	require.NoError(t, err)

	targetFlush := walFlushLSN(t, ctx, pgCluster.Node1Pool)

	nodeInfo := pgCluster.ClusterNodes[0]
	require.NoError(t, cdc.UpdateFromCDC(context.Background(), nodeInfo))

	startAfter := metadataStartLSN(t, ctx)
	require.True(t, startAfter >= mustParseLSN(t, slotConfirmed), "start_lsn should be at least the slot confirmed_flush_lsn")
	require.True(t, startAfter >= targetFlush, "start_lsn should catch up to the high-water LSN")
}

func TestCDCSLotInUseError(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_cdc_slot"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	require.NoError(t, mtreeTask.BuildMtree())

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	nodeInfo := pgCluster.ClusterNodes[0]
	listenCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		cdc.ListenForChanges(listenCtx, nodeInfo)
	}()

	waitForSlotActive(t, ctx, pgCluster.Node1Pool, config.Cfg.MTree.CDC.SlotName)

	err := cdc.UpdateFromCDC(context.Background(), nodeInfo)
	require.Error(t, err, "UpdateFromCDC should fail when slot is already active")
	require.Contains(t, err.Error(), "replication slot", "error should indicate slot is active")

	cancel()
	wg.Wait()
}

func currentMetadataLSN(t *testing.T, ctx context.Context) pglogrepl.LSN {
	t.Helper()
	tx, err := pgCluster.Node1Pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	_, lsnStr, _, err := queries.GetCDCMetadata(ctx, tx, config.Cfg.MTree.CDC.PublicationName)
	require.NoError(t, err)

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	require.NoError(t, err)
	return lsn
}

func metadataStartLSN(t *testing.T, ctx context.Context) pglogrepl.LSN {
	t.Helper()
	var lsnStr string
	err := pgCluster.Node1Pool.QueryRow(ctx, "SELECT start_lsn FROM spock.ace_cdc_metadata WHERE publication_name = $1", config.Cfg.MTree.CDC.PublicationName).Scan(&lsnStr)
	require.NoError(t, err)
	return mustParseLSN(t, lsnStr)
}

func slotConfirmedFlushLSN(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slotName string) string {
	t.Helper()
	var lsnStr string
	err := pool.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&lsnStr)
	require.NoError(t, err)
	return lsnStr
}

func walFlushLSN(t *testing.T, ctx context.Context, pool *pgxpool.Pool) pglogrepl.LSN {
	t.Helper()
	var lsnStr string
	err := pool.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()").Scan(&lsnStr)
	require.NoError(t, err)
	return mustParseLSN(t, lsnStr)
}

func mustParseLSN(t *testing.T, lsnStr string) pglogrepl.LSN {
	t.Helper()
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	require.NoError(t, err)
	return lsn
}

func waitForSlotActive(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slotName string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var pid int
		err := pool.QueryRow(ctx, "SELECT COALESCE(active_pid, 0) FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&pid)
		require.NoError(t, err)
		if pid != 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("slot %s did not become active", slotName)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func setupCDCTestTable(t *testing.T, ctx context.Context, tableName string) {
	t.Helper()
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.NoError(t, createTestTable(ctx, pool, testSchema, tableName))
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE %s.%s", testSchema, tableName))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.customers", testSchema, tableName, testSchema))
		require.NoError(t, err)
	}
}

func dropCDCTestTable(t *testing.T, tableName string) {
	t.Helper()
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", testSchema, tableName))
		if err != nil {
			t.Logf("Warning: failed to drop table %s.%s during cleanup: %v", testSchema, tableName, err)
		}
	}
}
