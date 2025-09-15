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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

func newTestMerkleTreeTask(t *testing.T, qualifiedTableName string, nodes []string) *core.MerkleTreeTask {
	t.Helper()
	task := core.NewMerkleTreeTask()
	task.ClusterName = "test_cluster"
	task.DBName = dbName
	task.QualifiedTableName = qualifiedTableName
	task.Nodes = strings.Join(nodes, ",")
	task.BlockSize = 1000
	return task
}

func TestMerkleTree_Init(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")

	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
	})

	aceSchema := config.Cfg.MTree.Schema
	cdcPubName := config.Cfg.MTree.CDC.PublicationName
	cdcSlotName := config.Cfg.MTree.CDC.SlotName

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.True(t, schemaExists(t, ctx, pool, aceSchema), "Schema '%s' should exist", aceSchema)
		require.True(t, functionExists(t, ctx, pool, "bytea_xor", aceSchema), "Function 'bytea_xor' should exist in schema '%s'", aceSchema)
		require.True(t, tableExists(t, ctx, pool, "ace_cdc_metadata", aceSchema), "Table 'ace_cdc_metadata' should exist in schema '%s'", aceSchema)
		require.True(t, publicationExists(t, ctx, pool, cdcPubName), "Publication '%s' should exist", cdcPubName)
		require.True(t, replicationSlotExists(t, ctx, pool, cdcSlotName), "Replication slot '%s' should exist", cdcSlotName)
	}
}

func TestMerkleTree_Build(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.RunChecks(false)
	require.NoError(t, err, "RunChecks should succeed")

	err = mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed before Build")
	t.Cleanup(func() {
		err := mtreeTask.MtreeTeardown()
		if err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
	})

	err = mtreeTask.BuildMtree()
	require.NoError(t, err, "BuildMtree should succeed")

	aceSchema := config.Cfg.MTree.Schema
	mtreeTableName := fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)
	pool := pgCluster.Node1Pool

	require.True(t, tableExists(t, ctx, pool, mtreeTableName, aceSchema), "Merkle tree table '%s' should exist", mtreeTableName)

	var numBlocks int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT num_blocks FROM %s.ace_mtree_metadata WHERE schema_name = $1 AND table_name = $2", aceSchema), testSchema, tableName).Scan(&numBlocks)
	require.NoError(t, err, "Should be able to get num_blocks from metadata")
	require.Greater(t, numBlocks, 0, "Number of blocks should be positive")

	var leafNodeCount int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE node_level = 0", aceSchema, mtreeTableName)).Scan(&leafNodeCount)
	require.NoError(t, err, "Should be able to count leaf nodes")

	// Using +1 here because the last leaf node is always (max_key) -> (NULL)
	require.Equal(t, numBlocks+1, leafNodeCount, "Number of leaf nodes should match num_blocks in metadata")

	var totalNodeCount int
	err = pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s.%s", aceSchema, mtreeTableName)).Scan(&totalNodeCount)
	require.NoError(t, err, "Should be able to count total nodes")
	require.Greater(t, totalNodeCount, leafNodeCount, "Total nodes should be greater than leaf nodes")
	// A complete merkle tree has 2*N-1 nodes for N leaves. It might not be complete.
	require.Less(t, totalNodeCount, 2*leafNodeCount, "Total nodes should be less than 2 * leaf nodes")
}

func TestMerkleTree_Teardown(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{serviceN1, serviceN2}
	mtreeTask := newTestMerkleTreeTask(t, qualifiedTableName, nodes)

	err := mtreeTask.MtreeInit()
	require.NoError(t, err, "MtreeInit should succeed")

	err = mtreeTask.MtreeTeardown()
	require.NoError(t, err, "MtreeTeardown should succeed")

	aceSchema := config.Cfg.MTree.Schema
	cdcPubName := config.Cfg.MTree.CDC.PublicationName
	cdcSlotName := config.Cfg.MTree.CDC.SlotName

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		require.False(t, tableExists(t, ctx, pool, "ace_cdc_metadata", aceSchema), "Table 'ace_cdc_metadata' should NOT exist after teardown")
		require.False(t, publicationExists(t, ctx, pool, cdcPubName), "Publication '%s' should NOT exist after teardown", cdcPubName)
		require.False(t, replicationSlotExists(t, ctx, pool, cdcSlotName), "Replication slot '%s' should NOT exist after teardown", cdcSlotName)

		require.True(t, schemaExists(t, ctx, pool, aceSchema), "Schema '%s' should still exist after teardown", aceSchema)
		require.True(t, functionExists(t, ctx, pool, "bytea_xor", aceSchema), "Function 'bytea_xor' should still exist after teardown")
	}
}

func schemaExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)", schemaName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func functionExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, functionName, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 
			FROM pg_proc p 
			JOIN pg_namespace n ON p.pronamespace = n.oid 
			WHERE p.proname = $1 AND n.nspname = $2
		)`, functionName, schemaName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func tableExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tableName, schemaName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2
		)`, schemaName, tableName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func publicationExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, pubName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)", pubName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func replicationSlotExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, slotName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", slotName).Scan(&exists)
	require.NoError(t, err)
	return exists
}
