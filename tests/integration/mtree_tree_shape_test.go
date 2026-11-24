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
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shapeTestTask returns a task for qualified on both nodes after mtree init.
func shapeTestTask(t *testing.T, env *testEnv, qualified string) *mtree.MerkleTreeTask {
	t.Helper()
	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	task.BlockSize = 100
	task.OverrideBlockSize = true
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { assert.NoError(t, task.MtreeTeardown()) })
	return task
}

// treeState reads the state of the tree of table on a node.
func treeState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) queries.TreeState {
	t.Helper()
	st, err := queries.GetTreeState(ctx, pool, testSchema, table)
	require.NoError(t, err)
	return st
}

// TestMtreeBuildReplicaIdentity checks that mtree build refuses a table
// whose replica identity is not its primary key, before the table goes into
// the publication, and accepts USING INDEX on an index with the key columns.
func TestMtreeBuildReplicaIdentity(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_build_replica_identity"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (a int PRIMARY KEY, b int NOT NULL, c text)", ident),
			fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (b)", pgx.Identifier{table + "_b"}.Sanitize(), ident),
			fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (a)", pgx.Identifier{table + "_a"}.Sanitize(), ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, g, 'x' FROM generate_series(1, 500) g", ident),
			"ANALYZE "+ident)
	}
	task := shapeTestTask(t, env, qualified)

	inPublication := func(pool *pgxpool.Pool) bool {
		exists, rels, err := queries.GetPublicationRelations(ctx, pool, config.Cfg.MTree.CDC.PublicationName)
		require.NoError(t, err)
		if !exists {
			return false
		}
		for _, r := range rels {
			if r.Schema == testSchema && r.Name == table {
				return true
			}
		}
		return false
	}

	refused := []struct {
		identity string
		message  string
	}{
		{"FULL", "the replica identity is FULL"},
		{"NOTHING", "the replica identity is NOTHING"},
		{"USING INDEX " + pgx.Identifier{table + "_b"}.Sanitize(), "the replica identity is an index on (b), not on the primary key columns (a)"},
	}
	for _, tc := range refused {
		t.Run(tc.identity, func(t *testing.T) {
			keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY %s", ident, tc.identity))
			t.Cleanup(func() {
				keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY DEFAULT", ident))
			})
			err := task.BuildMtree()
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.message)
			require.Contains(t, err.Error(), "on node "+env.ServiceN2)
			for _, pool := range env.pools() {
				require.False(t, inPublication(pool), "a refused build must not add the table to the publication")
			}
		})
	}

	// The same replica identity is needed on every node.
	keydiffExec(t, ctx, env, env.N2Pool,
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s", ident, pgx.Identifier{table + "_a"}.Sanitize()))
	err := task.BuildMtree()
	require.Error(t, err)
	require.Contains(t, err.Error(), "use the same replica identity on every node")

	// USING INDEX on another index with the primary key columns is fine.
	keydiffExec(t, ctx, env, env.N1Pool,
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s", ident, pgx.Identifier{table + "_a"}.Sanitize()))
	require.NoError(t, task.BuildMtree())
	for _, pool := range env.pools() {
		st := treeState(t, ctx, pool, table)
		require.Equal(t, queries.TreeStateValid, st.State)
		require.NotNil(t, st.Shape)
		require.Equal(t, "i", st.Shape.ReplicaIdentity)
		require.Equal(t, []string{"a"}, st.Shape.KeyNames())
		require.NotEmpty(t, st.ValidFromLSN)
	}
	require.NoError(t, task.UpdateMtree(false))
}

// TestMtreeBuildKeyChangedAfterChecks checks that mtree build refuses a
// table whose key type or key collation on a node changed after the checks
// of the command, before the table goes into the publication. The tree would
// record the new key on that node, and later runs would not see that the
// nodes differ.
func TestMtreeBuildKeyChangedAfterChecks(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_build_key_after_checks"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf(`CREATE TABLE %s (k text COLLATE "C" PRIMARY KEY, v int)`, ident),
			fmt.Sprintf("INSERT INTO %s SELECT 'k' || g, g FROM generate_series(1, 500) g", ident),
			"ANALYZE "+ident)
	}

	cases := []struct {
		name    string
		alter   string
		message string
	}{
		{"collation", `ALTER COLUMN k TYPE text COLLATE "POSIX"`, `primary key column "k" has collation`},
		{"type", `ALTER COLUMN k TYPE varchar COLLATE "C"`, `primary key column "k" has type text`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			task := shapeTestTask(t, env, qualified)
			keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("ALTER TABLE %s %s", ident, tc.alter))
			t.Cleanup(func() {
				keydiffExec(t, ctx, env, env.N2Pool,
					fmt.Sprintf(`ALTER TABLE %s ALTER COLUMN k TYPE text COLLATE "C"`, ident))
			})
			err := task.BuildMtree()
			require.Error(t, err)
			require.Contains(t, err.Error(), "changed on node "+env.ServiceN2+" after the checks of this command")
			require.Contains(t, err.Error(), tc.message)
			for _, pool := range env.pools() {
				exists, rels, err := queries.GetPublicationRelations(ctx, pool, config.Cfg.MTree.CDC.PublicationName)
				require.NoError(t, err)
				if exists {
					for _, r := range rels {
						require.False(t, r.Schema == testSchema && r.Name == table,
							"a refused build must not add the table to the publication")
					}
				}
			}
		})
	}
}

// TestMtreeTreeShapeKeyChange checks that mtree update and mtree table-diff
// refuse a tree after a change of the primary key, mark it as not valid, and
// that mtree build makes it valid again. Other DDL does not change the state.
func TestMtreeTreeShapeKeyChange(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_tree_shape_key"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	pkey := pgx.Identifier{table + "_pkey"}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (a int, b int, v text, PRIMARY KEY (a, b))", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, g, 'x' FROM generate_series(1, 500) g", ident),
			"ANALYZE "+ident)
	}
	task := shapeTestTask(t, env, qualified)
	require.NoError(t, task.BuildMtree())

	onBoth := func(sqls ...string) {
		t.Helper()
		for _, pool := range env.pools() {
			keydiffExec(t, ctx, env, pool, sqls...)
		}
	}
	requireInvalid := func(err error, parts ...string) {
		t.Helper()
		require.Error(t, err)
		require.True(t, errors.Is(err, mtree.ErrMtreeInvalid), "want ErrMtreeInvalid, got %v", err)
		for _, p := range parts {
			require.Contains(t, err.Error(), p)
		}
	}

	// An index, a grant and a comment do not change the shape of the table.
	onBoth(fmt.Sprintf("CREATE INDEX ON %s (v)", ident),
		fmt.Sprintf("GRANT SELECT ON %s TO PUBLIC", ident),
		fmt.Sprintf("COMMENT ON TABLE %s IS 'shape test'", ident))
	require.NoError(t, task.UpdateMtree(false))

	// The same columns in another order are another key: the leaves were
	// cut in the order (a, b).
	onBoth(fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s, ADD PRIMARY KEY (b, a)", ident, pkey))
	require.NoError(t, task.RunChecks(false))
	requireInvalid(task.UpdateMtree(false), "the primary key was (a, b), now (b, a)", "ace mtree build")
	st := treeState(t, ctx, env.N1Pool, table)
	require.Equal(t, queries.TreeStateInvalid, st.State)
	require.Equal(t, "the primary key was (a, b), now (b, a)", st.InvalidReason)
	requireInvalid(task.DiffMtree(), "is not valid", "the primary key was (a, b), now (b, a)")

	require.NoError(t, task.BuildMtree())
	for _, pool := range env.pools() {
		st := treeState(t, ctx, pool, table)
		require.Equal(t, queries.TreeStateValid, st.State)
		require.Empty(t, st.InvalidReason)
		require.Equal(t, []string{"b", "a"}, st.Shape.KeyNames())
	}
	require.NoError(t, task.UpdateMtree(false))

	// The last key column dropped and added again with the same name and
	// type: the names, the order and the types are the same, but all its
	// values are new.
	// A change on one node only: another replica identity on n2. The tree
	// on n1 still fits its table.
	keydiffExec(t, ctx, env, env.N2Pool,
		fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (b, a)", pgx.Identifier{table + "_ba"}.Sanitize(), ident),
		fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s", ident, pgx.Identifier{table + "_ba"}.Sanitize()))
	requireInvalid(task.UpdateMtree(false), "on node "+env.ServiceN2, "the replica identity was DEFAULT, now USING INDEX on (b, a)")
	require.Equal(t, queries.TreeStateValid, treeState(t, ctx, env.N1Pool, table).State)
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY DEFAULT", ident))
	require.NoError(t, task.BuildMtree())

	// One statement: without a primary key the table in the publication
	// would refuse UPDATE and DELETE.
	onBoth(fmt.Sprintf("ALTER TABLE %s DROP COLUMN a, ADD COLUMN a int NOT NULL DEFAULT 0, ADD PRIMARY KEY (b, a)", ident))
	require.NoError(t, task.RunChecks(false))
	requireInvalid(task.UpdateMtree(false), "the primary key column a was dropped and added again")

	// A tree of an older ACE has no recorded shape.
	require.NoError(t, task.BuildMtree())
	_, err := env.N2Pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET table_shape = NULL WHERE schema_name = $1 AND table_name = $2", // nosemgrep
		pgx.Identifier{config.Cfg.MTree.Schema, "ace_mtree_metadata"}.Sanitize()), testSchema, table)
	require.NoError(t, err)
	requireInvalid(task.UpdateMtree(false), "on node "+env.ServiceN2, "older ACE version")
	require.Equal(t, queries.TreeStateInvalid, treeState(t, ctx, env.N2Pool, table).State)
}

// TestMtreeTreeShapeColumnChange checks that a change of a non-key column
// that rewrites the values on one node, with no row change in the CDC
// stream, makes mtree compute all leaves of that node again, so that
// mtree table-diff finds the changed rows.
func TestMtreeTreeShapeColumnChange(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_tree_shape_columns"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, price numeric(10,2))", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, g + 0.5 FROM generate_series(1, 500) g", ident),
			"ANALYZE "+ident)
	}
	task := shapeTestTask(t, env, qualified)
	require.NoError(t, task.BuildMtree())
	require.NoError(t, task.DiffMtree())
	for _, n := range task.DiffResult.Summary.DiffRowsCount {
		require.Zero(t, n)
	}

	// The rewrite rounds every price on n1.
	keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN price TYPE numeric(10,0)", ident))
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.DiffMtree())
	total := 0
	for _, n := range task.DiffResult.Summary.DiffRowsCount {
		total += n
	}
	require.Equal(t, 500, total, "every rounded row must be found")

	for _, pool := range env.pools() {
		st := treeState(t, ctx, pool, table)
		require.Equal(t, queries.TreeStateValid, st.State, "a change of a non-key column keeps the tree valid")
	}
	shape, err := queries.GetTableShape(ctx, env.N1Pool, testSchema, table)
	require.NoError(t, err)
	require.Equal(t, shape, *treeState(t, ctx, env.N1Pool, table).Shape, "the new shape is recorded with the new hashes")
}

// TestMtreeTreeShapeRewriteUndone checks a change of a column type that
// rounds the values and is then undone, so that the definition is the same
// as at the build. Neither change sends rows to CDC; only the new file of
// the table shows that the leaf hashes are stale.
func TestMtreeTreeShapeRewriteUndone(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_tree_shape_rewrite"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, price numeric(10,2))", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, g + 0.5 FROM generate_series(1, 500) g", ident),
			"ANALYZE "+ident)
	}
	task := shapeTestTask(t, env, qualified)
	require.NoError(t, task.BuildMtree())
	built := *treeState(t, ctx, env.N1Pool, table).Shape

	keydiffExec(t, ctx, env, env.N1Pool,
		fmt.Sprintf("ALTER TABLE %s ALTER COLUMN price TYPE numeric(10,0)", ident),
		fmt.Sprintf("ALTER TABLE %s ALTER COLUMN price TYPE numeric(10,2)", ident))
	shape, err := queries.GetTableShape(ctx, env.N1Pool, testSchema, table)
	require.NoError(t, err)
	require.False(t, built.ColumnsChanged(shape), "the definition is the same as at the build")
	require.True(t, built.Rewritten(shape))

	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.DiffMtree())
	total := 0
	for _, n := range task.DiffResult.Summary.DiffRowsCount {
		total += n
	}
	require.Equal(t, 500, total, "every rounded row must be found")
	require.Equal(t, queries.TreeStateValid, treeState(t, ctx, env.N1Pool, table).State)
	require.Equal(t, shape, *treeState(t, ctx, env.N1Pool, table).Shape, "the shape is recorded with the new hashes")
}

// TestMtreeTreeShapeGenerated checks that a generated column is part of the
// recorded columns: adding one keeps the trees equal, and a new generation
// expression on one node, which rewrites its values with no row change in
// the CDC stream, is found by mtree table-diff.
func TestMtreeTreeShapeGenerated(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_tree_shape_generated"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, v int)", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, g FROM generate_series(1, 500) g", ident),
			"ANALYZE "+ident)
	}
	task := shapeTestTask(t, env, qualified)
	require.NoError(t, task.BuildMtree())

	diffRows := func() int {
		t.Helper()
		require.NoError(t, task.RunChecks(false))
		require.NoError(t, task.DiffMtree())
		total := 0
		for _, n := range task.DiffResult.Summary.DiffRowsCount {
			total += n
		}
		return total
	}

	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool, fmt.Sprintf("ALTER TABLE %s ADD COLUMN g int GENERATED ALWAYS AS (v * 2) STORED", ident))
	}
	require.Zero(t, diffRows())
	for _, pool := range env.pools() {
		st := treeState(t, ctx, pool, table)
		require.Equal(t, []string{"id", "v", "g"}, st.Shape.ColumnNames())
		require.Equal(t, "s", st.Shape.Columns[2].Generated)
	}

	var version int
	require.NoError(t, env.N1Pool.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&version))
	if version < 170000 {
		t.Skip("ALTER COLUMN ... SET EXPRESSION needs PostgreSQL 17")
	}
	keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN g SET EXPRESSION AS (v * 3)", ident))
	require.Equal(t, 500, diffRows(), "every row with the new expression must be found")
}
