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
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cdcKeyTree builds the tree of qualified on n1 and drops it at the end of
// the test.
func cdcKeyTree(t *testing.T, qualified string) *mtree.MerkleTreeTask {
	t.Helper()
	task := newTestMerkleTreeTask(t, qualified, []string{serviceN1})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { assert.NoError(t, task.MtreeTeardown()) })
	require.NoError(t, task.BuildMtree())
	return task
}

// leafCounts returns the leaves of mtreeTable with inserts or deletes since
// the last tree update, and their counters.
func leafCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, mtreeTable string) (map[int64]int64, map[int64]int64) {
	t.Helper()
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT node_position, inserts_since_tree_update, deletes_since_tree_update
		FROM %s
		WHERE node_level = 0 AND (inserts_since_tree_update > 0 OR deletes_since_tree_update > 0)`, mtreeTable)) // nosemgrep
	require.NoError(t, err)
	defer rows.Close()
	inserts, deletes := map[int64]int64{}, map[int64]int64{}
	for rows.Next() {
		var pos, ins, del int64
		require.NoError(t, rows.Scan(&pos, &ins, &del))
		if ins > 0 {
			inserts[pos] = ins
		}
		if del > 0 {
			deletes[pos] = del
		}
	}
	require.NoError(t, rows.Err())
	return inserts, deletes
}

// leafOfKey returns the leaf of mtreeTable that holds the key given by the
// SQL expression key. The server finds it.
func leafOfKey(t *testing.T, ctx context.Context, pool *pgxpool.Pool, mtreeTable, key string) int64 {
	t.Helper()
	var pos int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf(`
		SELECT node_position FROM %s
		WHERE node_level = 0
			AND (node_position = 0 OR %s >= range_start)
			AND (range_end IS NULL OR %s < range_end)`, mtreeTable, key, key)).Scan(&pos)) // nosemgrep
	return pos
}

// TestCDCKeyTexts checks that CDC finds the leaf of a change by the key of
// the row when the key texts have characters that a record literal reads in
// its own way, and when the order of the primary key is not the order of
// the columns in the table.
func TestCDCKeyTexts(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "cdc_key_texts"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	// The column a comes before b, and the primary key is (b, a). Both
	// columns are text, so a key in the wrong order gives another leaf and no
	// error.
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (a text, b text, v text, PRIMARY KEY (b, a))", ident),
			fmt.Sprintf("INSERT INTO %s SELECT 'x', lpad(g::text, 5, '0'), 'old' FROM generate_series(1, 5000) g", ident),
			"ANALYZE "+ident)
	}
	cdcKeyTree(t, qualified)

	pool := pgCluster.Node1Pool
	mtreeTable := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, table)}.Sanitize()
	keyType := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("%s_%s_key_type", testSchema, table)}.Sanitize()
	var leaves int64
	require.NoError(t, pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE node_level = 0", mtreeTable)).Scan(&leaves)) // nosemgrep
	require.GreaterOrEqual(t, leaves, int64(3), "the test needs several leaves")

	// Each text goes with three values of b over the key range, so the new
	// rows fall into several leaves.
	texts := []string{`a,b`, `a"b`, `(x)`, `a\b`, ``, `NULL`, ` a b `}
	for _, s := range texts {
		for _, g := range []int{500, 2500, 4500} {
			_, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ($1, lpad($2, 5, '0'), 'new')", ident), s, fmt.Sprint(g)) // nosemgrep
			require.NoError(t, err)
		}
	}
	tag, err := pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE b = '03000' AND a = 'x'", ident)) // nosemgrep
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())

	require.NoError(t, cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0]))

	// The server finds the leaf of each new row and of the deleted key by
	// the key itself.
	leafOf := fmt.Sprintf(`
		JOIN %s mt ON mt.node_level = 0
			AND (mt.node_position = 0 OR k.key >= mt.range_start)
			AND (mt.range_end IS NULL OR k.key < mt.range_end)`, mtreeTable)
	wantInserts := map[int64]int64{}
	rows, err := pool.Query(ctx, fmt.Sprintf(`
		SELECT mt.node_position, count(*)
		FROM (SELECT ROW(b, a)::%s AS key FROM %s WHERE v = 'new') k %s
		GROUP BY mt.node_position`, keyType, ident, leafOf)) // nosemgrep
	require.NoError(t, err)
	for rows.Next() {
		var pos, n int64
		require.NoError(t, rows.Scan(&pos, &n))
		wantInserts[pos] = n
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Greater(t, len(wantInserts), 1, "the new rows should fall into several leaves")
	deleteLeaf := leafOfKey(t, ctx, pool, mtreeTable, fmt.Sprintf("ROW('03000', 'x')::%s", keyType))

	inserts, deletes := leafCounts(t, ctx, pool, mtreeTable)
	require.Equal(t, wantInserts, inserts, "each insert should be counted in the leaf of its key")
	require.Equal(t, map[int64]int64{deleteLeaf: 1}, deletes, "the delete should be counted in the leaf of its key")
}

// TestCDCKeyTypeOutsideSearchPath checks a simple key whose type is in a
// schema out of the search_path and has a name that needs quotes.
func TestCDCKeyTypeOutsideSearchPath(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	schema := pgx.Identifier{"CdcKeyTypes"}.Sanitize()
	table := "cdc_key_enum"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident, "SCHEMA IF EXISTS "+schema)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			"CREATE SCHEMA "+schema,
			fmt.Sprintf(`CREATE TYPE %s."Mood" AS ENUM ('sad', 'ok', 'happy')`, schema),
			fmt.Sprintf(`CREATE TABLE %s (m %s."Mood" PRIMARY KEY, v text)`, ident, schema),
			fmt.Sprintf("INSERT INTO %s VALUES ('sad', 'old'), ('ok', 'old')", ident))
	}
	cdcKeyTree(t, qualified)

	pool := pgCluster.Node1Pool
	_, err := pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ('happy', 'new')", ident)) // nosemgrep
	require.NoError(t, err)
	_, err = pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE m = 'sad'", ident)) // nosemgrep
	require.NoError(t, err)

	require.NoError(t, cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0]))

	mtreeTable := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, table)}.Sanitize()
	mood := schema + `."Mood"`
	inserts, deletes := leafCounts(t, ctx, pool, mtreeTable)
	require.Equal(t, map[int64]int64{leafOfKey(t, ctx, pool, mtreeTable, "'happy'::"+mood): 1}, inserts)
	require.Equal(t, map[int64]int64{leafOfKey(t, ctx, pool, mtreeTable, "'sad'::"+mood): 1}, deletes)
}

// TestCDCKeyColumnRenamed checks a backlog with a change before a RENAME
// COLUMN of a key column. The change comes with the old column name, so CDC
// stops with an error and does not guess which column is the key.
func TestCDCKeyColumnRenamed(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "cdc_key_renamed"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (v text, id int PRIMARY KEY)", ident),
			fmt.Sprintf("INSERT INTO %s SELECT 'old', g FROM generate_series(1, 10) g", ident))
	}
	cdcKeyTree(t, qualified)

	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES ('new', 11)", ident)) // nosemgrep
	require.NoError(t, err)
	for _, p := range env.pools() {
		keydiffExec(t, ctx, env, p, fmt.Sprintf("ALTER TABLE %s RENAME COLUMN id TO key_id", ident))
	}

	err = cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary key has the columns (key_id)")
}

// TestCDCIdentityNotKeyAtCommit checks a DELETE that is committed while the
// replica identity is an index on a column out of the primary key. The
// replica identity becomes the primary key again and the old identity column
// gets a new name before CDC reads the change. CDC still stops: the change
// itself has no key columns.
func TestCDCIdentityNotKeyAtCommit(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "cdc_key_identity_at_commit"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	index := pgx.Identifier{table + "_alt_key"}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, alt int NOT NULL UNIQUE)", ident),
			fmt.Sprintf("INSERT INTO %s SELECT g, 100 + g FROM generate_series(1, 10) g", ident))
	}
	cdcKeyTree(t, qualified)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY USING INDEX %s", ident, index))
	}

	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = 1", ident)) // nosemgrep
	require.NoError(t, err)
	for _, pool := range env.pools() {
		keydiffExec(t, ctx, env, pool,
			fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY DEFAULT", ident),
			fmt.Sprintf("ALTER TABLE %s RENAME COLUMN alt TO alt_renamed", ident))
	}

	err = cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0])
	require.Error(t, err)
	require.Contains(t, err.Error(), "has the columns (alt) in the change")
}

// TestCDCReplicaIdentityFull checks that CDC stops on a table whose replica
// identity is FULL: on a table with a column out of the key, on a table
// with only key columns, where FULL gives the identity columns of the key,
// and on a TRUNCATE, which marks all leaves and needs no key.
func TestCDCReplicaIdentityFull(t *testing.T) {
	for _, tc := range []struct {
		table   string
		columns string
		change  string
	}{
		{"cdc_key_identity_full", "id int PRIMARY KEY, v text", "INSERT INTO %s VALUES (11, 'new')"},
		{"cdc_key_identity_full_key_only", "id int PRIMARY KEY", "INSERT INTO %s VALUES (11)"},
		{"cdc_key_identity_full_truncate", "id int PRIMARY KEY, v text", "TRUNCATE %s"},
	} {
		t.Run(tc.table, func(t *testing.T) {
			ctx := context.Background()
			env := newSpockEnv()
			qualified := testSchema + "." + tc.table
			ident := pgx.Identifier{testSchema, tc.table}.Sanitize()
			dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
			for _, pool := range env.pools() {
				keydiffExec(t, ctx, env, pool,
					fmt.Sprintf("CREATE TABLE %s (%s)", ident, tc.columns),
					fmt.Sprintf("ALTER TABLE %s REPLICA IDENTITY FULL", ident),
					fmt.Sprintf("INSERT INTO %s (id) SELECT g FROM generate_series(1, 10) g", ident))
			}
			cdcKeyTree(t, qualified)

			_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(tc.change, ident)) // nosemgrep
			require.NoError(t, err)

			err = cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0])
			require.Error(t, err)
			require.Contains(t, err.Error(), "replica identity")
		})
	}
}
