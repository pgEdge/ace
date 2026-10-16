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
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// The tests in this file check that the first leaf block of a Merkle tree has
// no lower bound. The block bounds come from the reference node only, so rows
// on another node can have keys below the first block's range_start. The diff
// must still compare these rows.

// firstBlockKeyShapes gives the two primary key shapes to test. Both tables
// have an "id" column, so extractDiffIDs works for both.
var firstBlockKeyShapes = []struct {
	name string
	ddl  string
	ins  string // INSERT ... SELECT over generate_series($1, $2) g
}{
	{
		name: "simple",
		ddl:  "(id INT PRIMARY KEY, payload TEXT)",
		ins:  "(id, payload) SELECT g, 'row-' || g FROM generate_series($1::int, $2::int) g",
	},
	{
		name: "composite",
		ddl:  "(id INT, k INT, payload TEXT, PRIMARY KEY (id, k))",
		ins:  "(id, k, payload) SELECT g, 1, 'row-' || g FROM generate_series($1::int, $2::int) g",
	},
}

// newUnreplicatedTable creates an empty table on n1 and n2 that is in no
// replication set, so each node keeps only the rows the test inserts on it.
// See TestMerkleTreeBidirectionalDiff for why repair_mode alone is not enough.
func newUnreplicatedTable(t *testing.T, env *testEnv, tableName, ddl string) (qualified, safe string) {
	t.Helper()
	ctx := context.Background()
	qualified = fmt.Sprintf("%s.%s", testSchema, tableName)
	safe = pgx.Identifier{testSchema, tableName}.Sanitize()

	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+" "+ddl) // nosemgrep
		require.NoError(t, err, "create %s", qualified)
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
	})
	for _, pool := range env.pools() {
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+safe) // nosemgrep
			require.NoError(t, err, "truncate %s", qualified)
		})
	}
	return qualified, safe
}

func insertIDRange(t *testing.T, pool *pgxpool.Pool, safe, ins string, from, to int) {
	t.Helper()
	_, err := pool.Exec(context.Background(), "INSERT INTO "+safe+" "+ins, from, to) // nosemgrep
	require.NoError(t, err, "insert ids %d..%d", from, to)
}

func analyzeTable(t *testing.T, env *testEnv, safe string) {
	t.Helper()
	for _, pool := range env.pools() {
		_, err := pool.Exec(context.Background(), "ANALYZE "+safe) // nosemgrep
		require.NoError(t, err)
	}
}

func idRange(from, to int) []int {
	ids := make([]int, 0, to-from+1)
	for i := from; i <= to; i++ {
		ids = append(ids, i)
	}
	return ids
}

// TestMerkleTreeRowsBelowReferenceMin builds a multi-leaf tree where the
// non-reference node has rows below the smallest key of the reference node.
//
//	n1 = 101..2200 (2100 rows, the reference), n2 = 1..2000.
//
// The diff must report 1..100 on n2 and 2001..2200 on n1. The rows 1..100 are
// below the first block's range_start and only the first block covers them.
func TestMerkleTreeRowsBelowReferenceMin(t *testing.T) {
	for _, shape := range firstBlockKeyShapes {
		t.Run(shape.name, func(t *testing.T) {
			env := newSpockEnv()
			qualified, safe := newUnreplicatedTable(t, env,
				"mtree_below_ref_min_"+shape.name, shape.ddl)

			insertIDRange(t, env.N1Pool, safe, shape.ins, 101, 2200)
			insertIDRange(t, env.N2Pool, safe, shape.ins, 1, 2000)
			analyzeTable(t, env, safe)

			mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
			require.NoError(t, mtreeTask.RunChecks(false))
			require.NoError(t, mtreeTask.MtreeInit())
			t.Cleanup(func() {
				if err := mtreeTask.MtreeTeardown(); err != nil {
					t.Logf("MtreeTeardown cleanup: %v", err)
				}
			})
			require.NoError(t, mtreeTask.BuildMtree())
			require.NoError(t, mtreeTask.DiffMtree())

			nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
			require.True(t, ok, "no diff for pair %s; result: %+v",
				env.pairKey(), mtreeTask.DiffResult.NodeDiffs)

			require.Equal(t, idRange(2001, 2200), extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]),
				"rows present only on n1 (the reference)")
			require.Equal(t, idRange(1, 100), extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]),
				"rows on n2 below the smallest key of the reference were not compared")
		})
	}
}

// TestMerkleTreeFirstLeafHashUpgrade checks a tree built by an older version,
// where the first leaf hash still used the lower bound. The test makes such a
// tree by hand: it sets an older hash version and gives the first leaf the
// same stale hash on both nodes, so that leaf matches and hides n2's rows
// below the reference's smallest key. The next update must compute the leaf
// hashes again, and the diff must then find these rows.
func TestMerkleTreeFirstLeafHashUpgrade(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	shape := firstBlockKeyShapes[0]
	tableName := "mtree_first_leaf_upgrade"
	qualified, safe := newUnreplicatedTable(t, env, tableName, shape.ddl)

	insertIDRange(t, env.N1Pool, safe, shape.ins, 101, 2200)
	insertIDRange(t, env.N2Pool, safe, shape.ins, 1, 2000)
	analyzeTable(t, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, mtreeTask.BuildMtree())

	aceSchema := config.Cfg.MTree.Schema
	mtreeTable := pgx.Identifier{aceSchema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName)}.Sanitize()
	metadataTable := pgx.Identifier{aceSchema, "ace_mtree_metadata"}.Sanitize()

	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, "UPDATE "+metadataTable+ // nosemgrep
			" SET hash_version = $1 WHERE schema_name = $2 AND table_name = $3",
			queries.CurrentHashVersion-1, testSchema, tableName)
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "UPDATE "+mtreeTable+ // nosemgrep
			" SET leaf_hash = '\\x00'::bytea, node_hash = '\\x00'::bytea WHERE node_level = 0 AND node_position = 0")
		require.NoError(t, err)
	}

	require.NoError(t, mtreeTask.DiffMtree())

	for _, pool := range env.pools() {
		var version int
		require.NoError(t, pool.QueryRow(ctx, "SELECT hash_version FROM "+metadataTable+ // nosemgrep
			" WHERE schema_name = $1 AND table_name = $2", testSchema, tableName).Scan(&version))
		require.Equal(t, queries.CurrentHashVersion, version, "hash version after update")
	}

	nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
	require.True(t, ok, "no diff for pair %s; result: %+v",
		env.pairKey(), mtreeTask.DiffResult.NodeDiffs)
	require.Equal(t, idRange(2001, 2200), extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]))
	require.Equal(t, idRange(1, 100), extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]))
}

// TestMerkleTreeSplitFirstBlockBelowMin checks the update path. After the
// build, both nodes get 1500 new rows below the first block's range_start.
// They all fall into the first block, so mtree update splits it. The split
// must count the rows below range_start, and the first block must keep the
// smallest range_start, or the leaves would be numbered in the wrong order.
// One of the new rows then differs between the nodes, and the diff must find
// it.
func TestMerkleTreeSplitFirstBlockBelowMin(t *testing.T) {
	ctx := context.Background()
	for _, shape := range firstBlockKeyShapes {
		t.Run(shape.name, func(t *testing.T) {
			env := newSpockEnv()
			tableName := "mtree_split_first_" + shape.name
			qualified, safe := newUnreplicatedTable(t, env, tableName, shape.ddl)

			for _, pool := range env.pools() {
				insertIDRange(t, pool, safe, shape.ins, 2001, 4000)
			}
			analyzeTable(t, env, safe)

			mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
			require.NoError(t, mtreeTask.RunChecks(false))
			require.NoError(t, mtreeTask.MtreeInit())
			t.Cleanup(func() {
				if err := mtreeTask.MtreeTeardown(); err != nil {
					t.Logf("MtreeTeardown cleanup: %v", err)
				}
			})
			require.NoError(t, mtreeTask.BuildMtree())

			mtreeTable := pgx.Identifier{
				config.Cfg.MTree.Schema,
				fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
			}.Sanitize()
			leafCount := func(pool *pgxpool.Pool) int {
				var n int
				require.NoError(t, pool.QueryRow(ctx,
					"SELECT count(*) FROM "+mtreeTable+" WHERE node_level = 0").Scan(&n)) // nosemgrep
				return n
			}
			leavesBefore := leafCount(env.N1Pool)

			for _, pool := range env.pools() {
				insertIDRange(t, pool, safe, shape.ins, 1, 1500)
			}
			_, err := env.N2Pool.Exec(ctx,
				"UPDATE "+safe+" SET payload = 'changed on n2' WHERE id = 7") // nosemgrep
			require.NoError(t, err)

			require.NoError(t, mtreeTask.DiffMtree())

			for name, pool := range map[string]*pgxpool.Pool{
				env.ServiceN1: env.N1Pool, env.ServiceN2: env.N2Pool,
			} {
				require.Greater(t, leafCount(pool), leavesBefore,
					"first block was not split on %s", name)

				// The first block must still be the one with the smallest
				// range_start, and it must not be above the smallest key.
				var firstPos int64
				var firstStart string
				require.NoError(t, pool.QueryRow(ctx,
					"SELECT node_position, range_start::text FROM "+mtreeTable+ // nosemgrep
						" WHERE node_level = 0 ORDER BY range_start LIMIT 1").Scan(&firstPos, &firstStart))
				require.Equal(t, int64(0), firstPos,
					"block with the smallest range_start is not at position 0 on %s", name)
				if shape.name == "simple" {
					require.Equal(t, "1", firstStart, "first block range_start on %s", name)
				} else {
					require.Equal(t, "(1,1)", firstStart, "first block range_start on %s", name)
				}
			}

			nodeDiffs, ok := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
			require.True(t, ok, "no diff for pair %s; result: %+v",
				env.pairKey(), mtreeTask.DiffResult.NodeDiffs)
			require.Equal(t, []int{7}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]))
			require.Equal(t, []int{7}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]))
		})
	}
}

// TestMerkleTreeMergeCountsRowsBelowFirstBlockStart checks the merge path.
// After the build, both nodes get 900 rows below the first block's
// range_start and lose almost all rows of its stored range. Counted with its
// lower bound, the first block looks almost empty, and update with rebalance
// would merge it into the next block. With the rows below range_start it holds
// about 950 rows, so it must stay as it is.
func TestMerkleTreeMergeCountsRowsBelowFirstBlockStart(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	shape := firstBlockKeyShapes[0]
	tableName := "mtree_merge_first_block"
	qualified, safe := newUnreplicatedTable(t, env, tableName, shape.ddl)

	for _, pool := range env.pools() {
		insertIDRange(t, pool, safe, shape.ins, 2001, 5000)
	}
	analyzeTable(t, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	mtreeTask.Rebalance = true
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, mtreeTask.BuildMtree())

	mtreeTable := pgx.Identifier{
		config.Cfg.MTree.Schema,
		fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
	}.Sanitize()
	type layout struct {
		leaves    int
		firstEnd  int32
		firstFrom int32
	}
	readLayout := func(pool *pgxpool.Pool) layout {
		var l layout
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT count(*) FROM "+mtreeTable+" WHERE node_level = 0").Scan(&l.leaves)) // nosemgrep
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT range_start, range_end FROM "+mtreeTable+ // nosemgrep
				" WHERE node_level = 0 AND node_position = 0").Scan(&l.firstFrom, &l.firstEnd))
		return l
	}
	before := readLayout(env.N1Pool)
	require.Equal(t, int32(2001), before.firstFrom)
	require.Greater(t, before.leaves, 2, "expected at least three leaves")

	for _, pool := range env.pools() {
		insertIDRange(t, pool, safe, shape.ins, 1, 900)
		_, err := pool.Exec(ctx, "DELETE FROM "+safe+" WHERE id >= 2001 AND id < $1", // nosemgrep
			before.firstEnd-50)
		require.NoError(t, err)
	}

	require.NoError(t, mtreeTask.DiffMtree())

	for name, pool := range map[string]*pgxpool.Pool{
		env.ServiceN1: env.N1Pool, env.ServiceN2: env.N2Pool,
	} {
		after := readLayout(pool)
		require.Equal(t, before.leaves, after.leaves,
			"the first block was merged on %s although it holds about 950 rows", name)
		require.Equal(t, before.firstEnd, after.firstEnd,
			"range_end of the first block changed on %s", name)
	}

	nodeDiffs := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
	require.Empty(t, nodeDiffs.Rows[env.ServiceN1], "nodes hold the same rows")
	require.Empty(t, nodeDiffs.Rows[env.ServiceN2], "nodes hold the same rows")
}

// TestMerkleTreeSplitFirstBlockOnOneNode checks the case where only one node
// gets rows below the first block's range_start. That node splits its first
// block and the other does not, so the leaf positions of the two trees no
// longer match. The diff must still report exactly the new rows.
func TestMerkleTreeSplitFirstBlockOnOneNode(t *testing.T) {
	ctx := context.Background()
	for _, shape := range firstBlockKeyShapes {
		t.Run(shape.name, func(t *testing.T) {
			env := newSpockEnv()
			tableName := "mtree_split_first_one_" + shape.name
			qualified, safe := newUnreplicatedTable(t, env, tableName, shape.ddl)

			for _, pool := range env.pools() {
				insertIDRange(t, pool, safe, shape.ins, 2001, 6000)
			}
			analyzeTable(t, env, safe)

			mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
			require.NoError(t, mtreeTask.RunChecks(false))
			require.NoError(t, mtreeTask.MtreeInit())
			t.Cleanup(func() {
				if err := mtreeTask.MtreeTeardown(); err != nil {
					t.Logf("MtreeTeardown cleanup: %v", err)
				}
			})
			require.NoError(t, mtreeTask.BuildMtree())

			insertIDRange(t, env.N2Pool, safe, shape.ins, 1, 1500)
			require.NoError(t, mtreeTask.DiffMtree())

			mtreeTable := pgx.Identifier{
				config.Cfg.MTree.Schema,
				fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
			}.Sanitize()
			leaves := func(pool *pgxpool.Pool) int {
				var n int
				require.NoError(t, pool.QueryRow(ctx,
					"SELECT count(*) FROM "+mtreeTable+" WHERE node_level = 0").Scan(&n)) // nosemgrep
				return n
			}
			require.Greater(t, leaves(env.N2Pool), leaves(env.N1Pool),
				"only n2 should have split its first block")

			nodeDiffs := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
			require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]))
			require.Equal(t, idRange(1, 1500), extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]))
		})
	}
}

// TestMerkleTreeFirstLeafKeepsPositionOnTie builds a tree from a reference
// node with one row, key 100. The build gives two leaves with the same
// range_start: [100, 100] and [100, NULL]. A split of the second leaf numbers
// the leaves again, and [100, 100] must stay at position 0. Then a row below
// 100 on n2 only must be reported.
func TestMerkleTreeFirstLeafKeepsPositionOnTie(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	shape := firstBlockKeyShapes[0]
	tableName := "mtree_first_leaf_tie"
	qualified, safe := newUnreplicatedTable(t, env, tableName, shape.ddl)

	for _, pool := range env.pools() {
		insertIDRange(t, pool, safe, shape.ins, 100, 100)
	}
	analyzeTable(t, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})
	require.NoError(t, mtreeTask.BuildMtree())

	mtreeTable := pgx.Identifier{
		config.Cfg.MTree.Schema,
		fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
	}.Sanitize()
	var sameStart int
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT count(*) FROM "+mtreeTable+ // nosemgrep
		" WHERE node_level = 0 AND range_start = 100").Scan(&sameStart))
	require.Equal(t, 2, sameStart, "the build should give two leaves that start at 100")

	for _, pool := range env.pools() {
		insertIDRange(t, pool, safe, shape.ins, 101, 1700)
	}
	require.NoError(t, mtreeTask.DiffMtree())

	for name, pool := range map[string]*pgxpool.Pool{
		env.ServiceN1: env.N1Pool, env.ServiceN2: env.N2Pool,
	} {
		var leaves int
		require.NoError(t, pool.QueryRow(ctx,
			"SELECT count(*) FROM "+mtreeTable+" WHERE node_level = 0").Scan(&leaves)) // nosemgrep
		require.Greater(t, leaves, 2, "the last leaf was not split on %s", name)
		var firstEnd int32
		require.NoError(t, pool.QueryRow(ctx, "SELECT range_end FROM "+mtreeTable+ // nosemgrep
			" WHERE node_level = 0 AND node_position = 0").Scan(&firstEnd))
		require.Equal(t, int32(100), firstEnd, "leaf [100, 100] left position 0 on %s", name)
	}

	insertIDRange(t, env.N2Pool, safe, shape.ins, 50, 50)
	require.NoError(t, mtreeTask.DiffMtree())
	nodeDiffs := mtreeTask.DiffResult.NodeDiffs[env.pairKey()]
	require.Empty(t, extractDiffIDs(nodeDiffs.Rows[env.ServiceN1]))
	require.Equal(t, []int{50}, extractDiffIDs(nodeDiffs.Rows[env.ServiceN2]))
}
