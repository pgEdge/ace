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
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// A tree with a hash version newer than this build was written by a newer
// ACE. mtree update and mtree table-diff must refuse it and change nothing:
// recomputing its dirty leaves with an older algorithm would leave hashes of
// two algorithms under the newer version number. mtree build rebuilds the
// tree with this build's algorithm and version, and mtree update upgrades a
// tree with an older version.
func TestMtreeRefusesNewerHashVersion(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "mtree_hash_version"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, pool := range pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+" (id INT PRIMARY KEY, name VARCHAR)") // nosemgrep
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO "+safe+" (id, name) SELECT g, 'user_'||g FROM generate_series(1,1000) g") // nosemgrep
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "ANALYZE "+safe) // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
	})

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	task.BlockSize = 100
	task.OverrideBlockSize = true
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { _ = task.MtreeTeardown() })
	require.NoError(t, task.BuildMtree())

	aceSchema := config.Cfg.MTree.Schema
	mt := pgx.Identifier{aceSchema, "ace_mtree_" + testSchema + "_" + tableName}.Sanitize()
	meta := pgx.Identifier{aceSchema, "ace_mtree_metadata"}.Sanitize()
	hashVersion := func(pool *pgxpool.Pool) (v int) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT hash_version FROM "+meta+" WHERE schema_name = $1 AND table_name = $2", // nosemgrep
			testSchema, tableName).Scan(&v))
		return
	}
	treeState := func(pool *pgxpool.Pool) (s string) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT string_agg(node_level || ':' || node_position || ':' || encode(node_hash, 'hex') || ':' || dirty, ',' ORDER BY node_level, node_position) FROM "+mt).Scan(&s)) // nosemgrep
		return
	}

	// n2 looks like a newer ACE wrote its tree. One leaf is dirty, so an
	// update without the guard would recompute it.
	newer := queries.CurrentHashVersion + 1
	_, err := env.N2Pool.Exec(ctx, "UPDATE "+meta+" SET hash_version = $1 WHERE schema_name = $2 AND table_name = $3", // nosemgrep
		newer, testSchema, tableName)
	require.NoError(t, err)
	_, err = env.N2Pool.Exec(ctx, "UPDATE "+mt+" SET dirty = true WHERE node_level = 0 AND node_position = 0") // nosemgrep
	require.NoError(t, err)
	before := map[*pgxpool.Pool]string{env.N1Pool: treeState(env.N1Pool), env.N2Pool: treeState(env.N2Pool)}

	err = task.UpdateMtree(false)
	require.Error(t, err, "mtree update must refuse a tree with a newer hash version")
	require.Contains(t, err.Error(), fmt.Sprintf("has hash version %d", newer))
	require.Contains(t, err.Error(), "mtree build")

	err = task.DiffMtree()
	require.Error(t, err, "mtree table-diff must refuse a tree with a newer hash version")
	require.Contains(t, err.Error(), fmt.Sprintf("has hash version %d", newer))

	for pool, state := range before {
		require.Equal(t, state, treeState(pool), "a refused update must not change any tree")
	}
	require.Equal(t, newer, hashVersion(env.N2Pool), "a refused update must keep the version marker")

	// mtree build is the way back: it rewrites every leaf and the version.
	require.NoError(t, task.BuildMtree())
	for _, pool := range pools {
		require.Equal(t, queries.CurrentHashVersion, hashVersion(pool))
	}
	require.NoError(t, task.UpdateMtree(false))

	// An older version is the upgrade path: mtree update recomputes every
	// leaf and writes the current version. A leaf hash that the older
	// algorithm would have left behind must be gone.
	_, err = env.N2Pool.Exec(ctx, "UPDATE "+meta+" SET hash_version = $1 WHERE schema_name = $2 AND table_name = $3", // nosemgrep
		queries.CurrentHashVersion-1, testSchema, tableName)
	require.NoError(t, err)
	_, err = env.N2Pool.Exec(ctx, "UPDATE "+mt+" SET node_hash = decode('deadbeef', 'hex') WHERE node_level = 0 AND node_position = 1") // nosemgrep
	require.NoError(t, err)
	require.NoError(t, task.UpdateMtree(false))
	require.Equal(t, queries.CurrentHashVersion, hashVersion(env.N2Pool))
	require.Equal(t, treeState(env.N1Pool), treeState(env.N2Pool), "the upgrade must recompute every leaf")
}
