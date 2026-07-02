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
	"github.com/stretchr/testify/require"
)

// A stale leaf hash that row comparison resolves as a false positive is
// refreshed during the diff, so the "Found N mismatched blocks" phantom does
// not recur on the next diff.
func TestMerkleTreeStaleLeafHeals(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "mtree_stale_leaf"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, pool := range pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+" (id INT PRIMARY KEY, name VARCHAR)") // nosemgrep
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "INSERT INTO "+safe+" (id, name) SELECT g, 'user_'||g FROM generate_series(1,1000) g") // nosemgrep
		require.NoError(t, err)
		// The build sizes its block ranges from planner estimates.
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

	mt := pgx.Identifier{config.Cfg.MTree.Schema, "ace_mtree_" + testSchema + "_" + tableName}.Sanitize()
	rootHash := func(pool *pgxpool.Pool) (h string) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT encode(node_hash,'hex') FROM "+mt+ // nosemgrep
			" WHERE node_level=(SELECT max(node_level) FROM "+mt+") AND node_position=0").Scan(&h))
		return
	}
	require.Equal(t, rootHash(env.N1Pool), rootHash(env.N2Pool), "sanity: trees match after build")

	var leafCount int
	require.NoError(t, env.N2Pool.QueryRow(ctx, "SELECT count(*) FROM "+mt+" WHERE node_level = 0").Scan(&leafCount)) // nosemgrep
	require.GreaterOrEqual(t, leafCount, 5, "sanity: need enough leaves to corrupt position 4")

	leafHash := func(pool *pgxpool.Pool, pos int) (h string) {
		require.NoError(t, pool.QueryRow(ctx, "SELECT encode(node_hash,'hex') FROM "+mt+ // nosemgrep
			" WHERE node_level = 0 AND node_position = $1", pos).Scan(&h))
		return
	}
	builtLeafHash := leafHash(env.N1Pool, 4)

	// Simulate a stale tree on n2: one leaf (and, so the divergence is visible
	// from the root down, its ancestor levels) carries a hash that no longer
	// reflects the identical live data -- the state a missed CDC drain leaves
	// behind.
	_, err := env.N2Pool.Exec(ctx, "UPDATE "+mt+" SET node_hash = decode('deadbeef','hex') WHERE node_level = 0 AND node_position = 4") // nosemgrep
	require.NoError(t, err)
	_, err = env.N2Pool.Exec(ctx, "UPDATE "+mt+" SET node_hash = decode('deadbeef','hex') WHERE node_level > 0") // nosemgrep
	require.NoError(t, err)
	require.NotEqual(t, rootHash(env.N1Pool), rootHash(env.N2Pool), "sanity: staleness visible at the root")

	// Diff #1 resolves the mismatch as a false positive: no row differences...
	require.NoError(t, task.DiffMtree())
	total := 0
	for _, c := range task.DiffResult.Summary.DiffRowsCount {
		total += c
	}
	require.Zero(t, total, "tables are identical; the mismatch is stale hashes only")

	// ...heals the stale leaf back to the build-time hash (not merely to some
	// value both refresh passes agree on), reports the refresh in the summary,
	// and leaves the trees agreeing without a rebuild or dirty leftovers.
	require.Equal(t, builtLeafHash, leafHash(env.N2Pool, 4), "healed leaf must match the hash built from identical data")
	require.Equal(t, rootHash(env.N1Pool), rootHash(env.N2Pool), "diff must refresh the stale blocks")
	require.NotEmpty(t, task.DiffResult.Summary.StaleBlocksRefreshed, "refresh must be visible in the diff summary")
	var dirtyLeft int
	require.NoError(t, env.N2Pool.QueryRow(ctx, "SELECT count(*) FROM "+mt+" WHERE node_level = 0 AND dirty").Scan(&dirtyLeft)) // nosemgrep
	require.Zero(t, dirtyLeft, "refresh must clear the dirty flags it set")

	// Diff #2 finds nothing to traverse: the phantom is gone.
	require.NoError(t, task.DiffMtree())
	require.Empty(t, task.DiffResult.NodeDiffs, "no phantom mismatch on the next diff")
}
