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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A tree records the basis encoding of its leaf hashes. When the nodes need
// another basis encoding, the leaves cannot be compared with the leaves of
// the other nodes: mtree update computes all of them again and records the
// new basis encoding.
func TestMtreeBasisEncodingChange(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "mtree_hash_encoding"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, pool := range pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+" (id INT PRIMARY KEY, name VARCHAR)") // nosemgrep
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO "+safe+" (id, name) SELECT g, 'user_'||g FROM generate_series(1,500) g") // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			execOK(t, ctx, pool, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
	})

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	task.BlockSize = 100
	task.OverrideBlockSize = true
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { assert.NoError(t, task.MtreeTeardown()) })
	require.NoError(t, task.BuildMtree())

	aceSchema := config.Cfg.MTree.Schema
	mt := pgx.Identifier{aceSchema, "ace_mtree_" + testSchema + "_" + tableName}.Sanitize()
	meta := pgx.Identifier{aceSchema, "ace_mtree_metadata"}.Sanitize()
	hashEncoding := func(pool *pgxpool.Pool) (e string) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT coalesce(hash_encoding, '') FROM "+meta+" WHERE schema_name = $1 AND table_name = $2", // nosemgrep
			testSchema, tableName).Scan(&e))
		return
	}
	treeState := func(pool *pgxpool.Pool) (s string) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT string_agg(node_level || ':' || node_position || ':' || encode(node_hash, 'hex'), ',' ORDER BY node_level, node_position) FROM "+mt).Scan(&s)) // nosemgrep
		return
	}

	for _, pool := range pools {
		require.Equal(t, "UTF8", hashEncoding(pool), "build records the basis encoding")
	}

	// n2 looks like its leaves were hashed in another basis encoding, and
	// one leaf has a hash that no current leaf can have.
	_, err := env.N2Pool.Exec(ctx, "UPDATE "+meta+" SET hash_encoding = 'LATIN1' WHERE schema_name = $1 AND table_name = $2", // nosemgrep
		testSchema, tableName)
	require.NoError(t, err)
	_, err = env.N2Pool.Exec(ctx, "UPDATE "+mt+" SET node_hash = decode('deadbeef', 'hex'), leaf_hash = decode('deadbeef', 'hex') WHERE node_level = 0 AND node_position = 1") // nosemgrep
	require.NoError(t, err)

	require.NoError(t, task.UpdateMtree(false))
	require.Equal(t, "UTF8", hashEncoding(env.N2Pool), "update records the new basis encoding")
	require.Equal(t, treeState(env.N1Pool), treeState(env.N2Pool), "update must compute every leaf again")

	// A metadata table made by an older ACE has no hash_encoding column.
	// Build must add it before it writes the metadata of the tree.
	for _, pool := range pools {
		_, err := pool.Exec(ctx, "ALTER TABLE "+meta+" DROP COLUMN IF EXISTS hash_encoding") // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			execOK(t, context.Background(), pool, "ALTER TABLE "+meta+" ADD COLUMN IF NOT EXISTS hash_encoding text") // nosemgrep
		}
	})
	require.NoError(t, task.BuildMtree())
	for _, pool := range pools {
		require.Equal(t, "UTF8", hashEncoding(pool), "build adds the column and records the basis encoding")
	}
}
