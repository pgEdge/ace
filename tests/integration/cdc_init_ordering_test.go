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

// Regression coverage for SQLSTATE 42704 "publication \"ace_mtree_pub\"
// does not exist" during mtree table-diff. MtreeInit was creating the
// replication slot before the publication was committed, so the slot's
// consistent point preceded the publication. pgoutput's
// get_publication_oid then failed during replay. The fix splits init into
// three phases so the slot's consistent point sits at or beyond the
// publication's commit LSN, and adds a pub_commit_lsn guard in
// processReplicationStream that rejects streams whose start_lsn predates
// it.

package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// cdcMetadataTable returns the safely-quoted "schema"."ace_cdc_metadata"
// identifier. Built once with pgx.Identifier.Sanitize() so call sites can
// concatenate it into SQL without fmt.Sprintf — matches the production
// convention in queries package and keeps Codacy/Semgrep's
// concat-sqli pattern quiet (schema is config-loaded, never user input).
func cdcMetadataTable() string {
	return pgx.Identifier{config.Cfg.MTree.Schema, "ace_cdc_metadata"}.Sanitize()
}

// TestMtreeInitSlotIsAfterPublication verifies the init-ordering fix: the replication
// slot's consistent point sits at or beyond the publication's commit LSN, so
// pgoutput can always find the publication in the historical catalog snapshot.
func TestMtreeInitSlotIsAfterPublication(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_pub_ordering"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1, serviceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	for _, np := range []struct {
		name string
		pool *pgxpool.Pool
	}{
		{serviceN1, pgCluster.Node1Pool},
		{serviceN2, pgCluster.Node2Pool},
	} {
		// pub_commit_lsn must be populated by the new InitCDCMetadata path.
		var pubCommitLSN string
		// FP: identifier sanitised by pgx.Identifier; user values via $N.
		// nosemgrep
		err := np.pool.QueryRow(ctx,
			"SELECT COALESCE(pub_commit_lsn, '') FROM "+cdcMetadataTable()+" WHERE publication_name = $1",
			config.Cfg.MTree.CDC.PublicationName,
		).Scan(&pubCommitLSN)
		require.NoError(t, err, "read pub_commit_lsn on %s", np.name)
		require.NotEmpty(t, pubCommitLSN, "pub_commit_lsn must be set on %s (proves new InitCDCMetadata ran)", np.name)

		// The slot's confirmed_flush_lsn is the consistent point of the
		// replication slot. It must be >= the publication's commit LSN, or
		// pgoutput would replay from a snapshot that doesn't see the
		// publication. This is the core ordering invariant the fix restores.
		var slotConfirmed string
		err = np.pool.QueryRow(ctx,
			"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
			config.Cfg.MTree.CDC.SlotName,
		).Scan(&slotConfirmed)
		require.NoError(t, err, "read slot confirmed_flush_lsn on %s", np.name)

		var aheadOrEqual bool
		err = np.pool.QueryRow(ctx,
			"SELECT $1::pg_lsn >= $2::pg_lsn",
			slotConfirmed, pubCommitLSN,
		).Scan(&aheadOrEqual)
		require.NoError(t, err, "compare LSNs on %s", np.name)
		require.True(t, aheadOrEqual,
			"slot consistent point %s must be >= publication commit LSN %s on %s",
			slotConfirmed, pubCommitLSN, np.name)
	}
}

// TestProcessReplicationStreamRejectsStaleStartLSN verifies the new
// publication-commit guard in processReplicationStream. We corrupt n1's
// start_lsn to a value strictly older than pub_commit_lsn and assert that
// UpdateFromCDC returns an explicit, actionable error rather than the
// cryptic SQLSTATE 42704 the customer originally saw.
func TestProcessReplicationStreamRejectsStaleStartLSN(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_stale_start_lsn"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	setupCDCTestTable(t, ctx, tableName)

	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, []string{serviceN1})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown during cleanup: %v", err)
		}
		dropCDCTestTable(t, tableName)
	})

	// Read pub_commit_lsn and craft a stale start_lsn strictly below it.
	var pubCommitLSN string
	// FP: identifier sanitised by pgx.Identifier; user values via $N.
	// nosemgrep
	err := pgCluster.Node1Pool.QueryRow(ctx,
		"SELECT pub_commit_lsn FROM "+cdcMetadataTable()+" WHERE publication_name = $1",
		config.Cfg.MTree.CDC.PublicationName,
	).Scan(&pubCommitLSN)
	require.NoError(t, err)
	require.NotEmpty(t, pubCommitLSN, "fix relies on pub_commit_lsn being populated")

	var staleLSN string
	err = pgCluster.Node1Pool.QueryRow(ctx,
		"SELECT ($1::pg_lsn - 16)::text", pubCommitLSN,
	).Scan(&staleLSN)
	require.NoError(t, err)

	// FP: identifier sanitised by pgx.Identifier; user values via $N.
	// nosemgrep
	_, err = pgCluster.Node1Pool.Exec(ctx,
		"UPDATE "+cdcMetadataTable()+" SET start_lsn = $1 WHERE publication_name = $2",
		staleLSN, config.Cfg.MTree.CDC.PublicationName,
	)
	require.NoError(t, err, "inject stale start_lsn")

	nodeInfo := pgCluster.ClusterNodes[0]
	err = cdc.UpdateFromCDC(context.Background(), nodeInfo)
	require.Error(t, err, "UpdateFromCDC must refuse a stream whose start_lsn precedes the publication commit")

	msg := err.Error()
	require.Contains(t, msg, "publication commit LSN",
		"error must mention the publication-commit invariant, got: %s", msg)
	require.NotContains(t, strings.ToLower(msg), "42704",
		"caller should see the new guard's actionable error, not the raw 42704 from PostgreSQL: %s", msg)
}
