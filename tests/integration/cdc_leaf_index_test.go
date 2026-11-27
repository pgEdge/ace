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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/pkg/config"
	"github.com/stretchr/testify/require"
)

// The tests in this file check how CDC finds the leaf of a changed key: the
// leaf with the largest range_start that is not greater than the key, found
// with one probe of the leaf index. The first three tests build tree tables
// by hand in a transaction that is rolled back, so they leave nothing behind
// on the node.

const leafLookupSchema = "ace_leaf_lookup_test"

// scratchTx begins a transaction on n1 for objects that only this test sees.
// Spock must not replicate them to other nodes.
func scratchTx(t *testing.T, ctx context.Context, pool *pgxpool.Pool) pgx.Tx {
	t.Helper()
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	_, err = tx.Exec(ctx, `SELECT set_config('spock.enable_ddl_replication', 'off', true),
		set_config('spock.include_ddl_repset', 'off', true)`)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "CREATE SCHEMA "+leafLookupSchema) // nosemgrep
	require.NoError(t, err)
	return tx
}

// noDDLReplicationPool opens a pool to n1 whose sessions do not let Spock
// replicate DDL, as ACE connections do, for DDL that is committed.
func noDDLReplicationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	cfg := pgCluster.Node1Pool.Config().Copy()
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["spock.enable_ddl_replication"] = "off"
	cfg.ConnConfig.RuntimeParams["spock.include_ddl_repset"] = "off"
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

type leafRow struct {
	pos        int64
	start, end string // SQL literals; "NULL" for no bound
}

// insertLeaves adds the leaves to the tree table mtreeTable in tx.
func insertLeaves(t *testing.T, ctx context.Context, tx pgx.Tx, mtreeTable string, leaves []leafRow) {
	t.Helper()
	for _, l := range leaves {
		_, err := tx.Exec(ctx, fmt.Sprintf( // nosemgrep
			`INSERT INTO %s (node_level, node_position, range_start, range_end) VALUES (0, $1, %s, %s)`,
			mtreeTable, l.start, l.end), l.pos)
		require.NoError(t, err)
	}
}

type leafCounters struct {
	dirty            bool
	inserts, deletes int64
}

// readLeafCounters returns the CDC marks of the leaves of mtreeTable that
// have any, by leaf position.
func readLeafCounters(t *testing.T, ctx context.Context, tx pgx.Tx, mtreeTable string) map[int64]leafCounters {
	t.Helper()
	rows, err := tx.Query(ctx, fmt.Sprintf( // nosemgrep
		`SELECT node_position, dirty, inserts_since_tree_update, deletes_since_tree_update
		   FROM %s WHERE node_level = 0 AND (dirty OR inserts_since_tree_update <> 0 OR deletes_since_tree_update <> 0)`,
		mtreeTable))
	require.NoError(t, err)
	got := map[int64]leafCounters{}
	for rows.Next() {
		var pos int64
		var c leafCounters
		require.NoError(t, rows.Scan(&pos, &c.dirty, &c.inserts, &c.deletes))
		got[pos] = c
	}
	require.NoError(t, rows.Err())
	return got
}

// TestCDCLeafLookupEdgeKeys checks the leaf that CDC marks for keys below the
// first range_start, on a bound, inside a leaf and after the last
// range_start, with two leaves that have the same range_start. Of those two,
// the first is empty, so the key goes to the second.
func TestCDCLeafLookupEdgeKeys(t *testing.T) {
	ctx := context.Background()
	want := map[int64]leafCounters{
		0: {dirty: true, inserts: 1, deletes: 1},
		2: {dirty: true, inserts: 1, deletes: 0},
		3: {dirty: true, inserts: 1, deletes: 1},
	}

	t.Run("simple", func(t *testing.T) {
		tx := scratchTx(t, ctx, pgCluster.Node1Pool)
		mtreeTable := pgx.Identifier{leafLookupSchema, "ace_mtree_simple"}.Sanitize()
		require.NoError(t, queries.CreateSimpleMtreeTable(ctx, tx, mtreeTable, "integer"))
		require.NoError(t, queries.CreateMtreeLeafIndex(ctx, tx, mtreeTable))
		insertLeaves(t, ctx, tx, mtreeTable, []leafRow{
			{0, "100", "200"},
			{1, "200", "200"},
			{2, "200", "300"},
			{3, "300", "NULL"},
		})
		// Inserts: below the first leaf, on a bound, after the last.
		// Deletes: the first range_start, the last range_start.
		// Updates: inside leaf 0 and leaf 2.
		require.NoError(t, queries.UpdateMtreeCounters(ctx, tx, mtreeTable, false, "", "integer",
			[]string{"5", "200", "10000"},
			[]string{"100", "300"},
			[]string{"199", "250"}))
		require.Equal(t, want, readLeafCounters(t, ctx, tx, mtreeTable))
	})

	// The empty leaf can also come after the leaf that owns its range_start.
	t.Run("empty leaf last in a tie", func(t *testing.T) {
		tx := scratchTx(t, ctx, pgCluster.Node1Pool)
		mtreeTable := pgx.Identifier{leafLookupSchema, "ace_mtree_tie"}.Sanitize()
		require.NoError(t, queries.CreateSimpleMtreeTable(ctx, tx, mtreeTable, "integer"))
		require.NoError(t, queries.CreateMtreeLeafIndex(ctx, tx, mtreeTable))
		insertLeaves(t, ctx, tx, mtreeTable, []leafRow{
			{0, "100", "200"},
			{1, "200", "300"},
			{2, "200", "200"},
			{3, "300", "NULL"},
		})
		require.NoError(t, queries.UpdateMtreeCounters(ctx, tx, mtreeTable, false, "", "integer",
			[]string{"200"}, nil, []string{"250"}))
		require.Equal(t, map[int64]leafCounters{1: {dirty: true, inserts: 1}},
			readLeafCounters(t, ctx, tx, mtreeTable))
	})

	t.Run("composite", func(t *testing.T) {
		tx := scratchTx(t, ctx, pgCluster.Node1Pool)
		keyType := pgx.Identifier{leafLookupSchema, "kt"}.Sanitize()
		_, err := tx.Exec(ctx, "CREATE TYPE "+keyType+" AS (a int, b text)") // nosemgrep
		require.NoError(t, err)
		mtreeTable := pgx.Identifier{leafLookupSchema, "ace_mtree_composite"}.Sanitize()
		require.NoError(t, queries.CreateCompositeMtreeTable(ctx, tx, mtreeTable, keyType))
		require.NoError(t, queries.CreateMtreeLeafIndex(ctx, tx, mtreeTable))
		k := func(a int, b string) string { return fmt.Sprintf("ROW(%d, '%s')::%s", a, b, keyType) }
		insertLeaves(t, ctx, tx, mtreeTable, []leafRow{
			{0, k(1, "b"), k(1, "m")},
			{1, k(1, "m"), k(1, "m")},
			{2, k(1, "m"), k(2, "a")},
			{3, k(2, "a"), "NULL"},
		})
		require.NoError(t, queries.UpdateMtreeCounters(ctx, tx, mtreeTable, true, keyType, "",
			[]string{"(1,a)", "(1,m)", "(9,z)"},
			[]string{"(1,b)", "(2,a)"},
			[]string{"(1,c)", "(1,z)"}))
		require.Equal(t, want, readLeafCounters(t, ctx, tx, mtreeTable))
	})
}

// leafTreeShape fills a tree table with n contiguous leaves and gives the CDC
// key text of a key inside leaf i.
type leafTreeShape struct {
	name      string
	composite bool
	pkeyType  string // the type of the key in the lookup
	ddlType   string // the type of the tree columns, if not pkeyType
	// fill is an INSERT ... SELECT: %[1]s is the tree table, %[2]s the key
	// type, $1 is n.
	fill string
	key  func(i int) string
}

var leafTreeShapes = []leafTreeShape{
	{
		name: "integer", pkeyType: "integer",
		fill: `INSERT INTO %[1]s (node_level, node_position, range_start, range_end)
			SELECT 0, g, g * 10, CASE WHEN g = $1 - 1 THEN NULL ELSE (g + 1) * 10 END
			FROM generate_series(0, $1 - 1) g`,
		key: func(i int) string { return fmt.Sprint(i*10 + 5) },
	},
	{
		name: "text", pkeyType: "text",
		fill: `INSERT INTO %[1]s (node_level, node_position, range_start, range_end)
			SELECT 0, g, lpad(g::text, 8, '0'), CASE WHEN g = $1 - 1 THEN NULL ELSE lpad((g + 1)::text, 8, '0') END
			FROM generate_series(0, $1 - 1) g`,
		key: func(i int) string { return fmt.Sprintf("%08dx", i) },
	},
	// The collation of the tree columns is not the database default. The
	// comparison then takes the collation of the column, so the lookup can
	// still use the index.
	{
		name: "text_collate_c", pkeyType: "text", ddlType: `text COLLATE "C"`,
		fill: `INSERT INTO %[1]s (node_level, node_position, range_start, range_end)
			SELECT 0, g, lpad(g::text, 8, '0'), CASE WHEN g = $1 - 1 THEN NULL ELSE lpad((g + 1)::text, 8, '0') END
			FROM generate_series(0, $1 - 1) g`,
		key: func(i int) string { return fmt.Sprintf("%08dx", i) },
	},
	{
		name: "composite", composite: true,
		fill: `INSERT INTO %[1]s (node_level, node_position, range_start, range_end)
			SELECT 0, g, ROW(g, 'k')::%[2]s, CASE WHEN g = $1 - 1 THEN NULL ELSE ROW(g + 1, 'k')::%[2]s END
			FROM generate_series(0, $1 - 1) g`,
		key: func(i int) string { return fmt.Sprintf("(%d,m)", i) },
	},
}

// makeLeafTree creates a tree table of the shape with n leaves and its leaf
// index, and returns the table, the key type for the lookup and the index name.
func makeLeafTree(t *testing.T, ctx context.Context, tx pgx.Tx, shape leafTreeShape, n int) (mtreeTable, keyType, indexName string) {
	t.Helper()
	relname := fmt.Sprintf("ace_mtree_%s_%d", shape.name, n)
	mtreeTable = pgx.Identifier{leafLookupSchema, relname}.Sanitize()
	keyType = shape.pkeyType
	if shape.composite {
		keyType = pgx.Identifier{leafLookupSchema, fmt.Sprintf("kt_%d", n)}.Sanitize()
		_, err := tx.Exec(ctx, "CREATE TYPE "+keyType+" AS (a int, b text)") // nosemgrep
		require.NoError(t, err)
		require.NoError(t, queries.CreateCompositeMtreeTable(ctx, tx, mtreeTable, keyType))
	} else {
		ddlType := shape.ddlType
		if ddlType == "" {
			ddlType = keyType
		}
		require.NoError(t, queries.CreateSimpleMtreeTable(ctx, tx, mtreeTable, ddlType))
	}
	_, err := tx.Exec(ctx, fmt.Sprintf(shape.fill, mtreeTable, keyType), n) // nosemgrep
	require.NoError(t, err)
	require.NoError(t, queries.CreateMtreeLeafIndex(ctx, tx, mtreeTable))
	_, err = tx.Exec(ctx, "ANALYZE "+mtreeTable) // nosemgrep
	require.NoError(t, err)
	err = tx.QueryRow(ctx, `SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
		WHERE i.indrelid = $1::regclass AND NOT i.indisprimary`, mtreeTable).Scan(&indexName)
	require.NoError(t, err)
	return mtreeTable, keyType, indexName
}

type planNode struct {
	NodeType     string     `json:"Node Type"`
	RelationName string     `json:"Relation Name"`
	IndexName    string     `json:"Index Name"`
	Parent       string     `json:"Parent Relationship"`
	SharedHit    int64      `json:"Shared Hit Blocks"`
	SharedRead   int64      `json:"Shared Read Blocks"`
	Plans        []planNode `json:"Plans"`
}

// walk calls f for p and for every plan node below it.
func (p planNode) walk(f func(planNode)) {
	f(p)
	for _, c := range p.Plans {
		c.walk(f)
	}
}

// explainSeq makes the name of each prepared statement unique: a statement
// stays in the session after its savepoint is rolled back.
var explainSeq int

// explainLookup runs the CDC counter update for keys as a prepared statement
// with the given plan_cache_mode, under EXPLAIN ANALYZE, and undoes it. It
// returns the plan.
func explainLookup(t *testing.T, ctx context.Context, tx pgx.Tx, mtreeTable, keyType string, composite bool, mode string, keys []string) planNode {
	t.Helper()
	data := map[string]any{"MtreeTable": mtreeTable, "IsComposite": composite}
	if composite {
		data["CompositeTypeName"] = keyType
	} else {
		data["PkeyType"] = keyType
	}
	sql, err := queries.RenderSQL(queries.SQLTemplates.UpdateMtreeCounters, data)
	require.NoError(t, err)
	sql = strings.NewReplacer("@inserts", "$1", "@deletes", "$2", "@updates", "$3").Replace(sql)

	sp, err := tx.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = sp.Rollback(ctx) }()
	_, err = sp.Exec(ctx, "SELECT set_config('plan_cache_mode', $1, true)", mode)
	require.NoError(t, err)
	explainSeq++
	name := fmt.Sprintf("leaf_lookup_%d", explainSeq)
	_, err = sp.Exec(ctx, "PREPARE "+name+"(text[], text[], text[]) AS "+sql) // nosemgrep
	require.NoError(t, err)
	defer func() { _, _ = sp.Exec(ctx, "DEALLOCATE "+name) }() // nosemgrep

	quoted := make([]string, len(keys))
	for i, k := range keys {
		quoted[i] = `"` + k + `"`
	}
	arr := "'{" + strings.ReplaceAll(strings.Join(quoted, ","), "'", "''") + "}'"
	var out string
	err = sp.QueryRow(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) EXECUTE "+name+"('{}', '{}', "+arr+")").Scan(&out) // nosemgrep
	require.NoError(t, err)
	var plans []struct {
		Plan planNode `json:"Plan"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &plans))
	require.Len(t, plans, 1)
	return plans[0].Plan
}

// requireLookupByIndex checks that the lookup of the leaf of a key, a subplan
// of the plan, probes the index and never scans the leaves. The update of the
// found leaves may scan them when the planner finds that cheaper, as on a
// small tree; the buffer check of the caller catches a scan on a large one.
func requireLookupByIndex(t *testing.T, plan planNode, relname, index, label string) {
	t.Helper()
	lookups, usesIndex := 0, false
	plan.walk(func(p planNode) {
		if p.Parent != "SubPlan" {
			return
		}
		lookups++
		p.walk(func(q planNode) {
			if q.IndexName == index {
				usesIndex = true
			}
			require.False(t, q.NodeType == "Seq Scan" && q.RelationName == relname,
				"%s: the lookup scans the leaves", label)
		})
	})
	require.Equal(t, 1, lookups, "%s: want one lookup subplan", label)
	require.True(t, usesIndex, "%s: the lookup does not use %s", label, index)
}

// TestCDCLeafLookupUsesIndex checks that the CDC lookup finds the leaf of a
// key with the leaf index and never scans the leaves, with a custom and a
// generic plan, for one key and for many. It also checks that the cost of
// one key does not grow with the number of leaves: a tree of 20000 leaves
// reads about as many buffers as a tree of 2000 leaves. Buffers, not time,
// so the check does not depend on the speed of the machine.
func TestCDCLeafLookupUsesIndex(t *testing.T) {
	ctx := context.Background()
	const small, large = 2000, 20000

	for _, shape := range leafTreeShapes {
		t.Run(shape.name, func(t *testing.T) {
			tx := scratchTx(t, ctx, pgCluster.Node1Pool)
			type tree struct{ table, keyType, index, relname string }
			trees := map[int]tree{}
			for _, n := range []int{small, large} {
				table, keyType, index := makeLeafTree(t, ctx, tx, shape, n)
				trees[n] = tree{table, keyType, index, fmt.Sprintf("ace_mtree_%s_%d", shape.name, n)}
			}

			for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
				buffers := map[int]int64{}
				for _, n := range []int{small, large} {
					tr := trees[n]
					many := make([]string, 0, 100)
					for i := 0; i < 100; i++ {
						many = append(many, shape.key(i*(n/100)))
					}
					for _, keys := range [][]string{{shape.key(n / 2)}, many} {
						plan := explainLookup(t, ctx, tx, tr.table, tr.keyType, shape.composite, mode, keys)
						requireLookupByIndex(t, plan, tr.relname, tr.index,
							fmt.Sprintf("%s, %d leaves, %d keys", mode, n, len(keys)))
						if len(keys) == 1 {
							buffers[n] = plan.SharedHit + plan.SharedRead
						}
					}
				}
				require.LessOrEqual(t, buffers[large], buffers[small]+8,
					"%s: one key reads %d buffers with %d leaves and %d with %d leaves",
					mode, buffers[small], small, buffers[large], large)
			}
		})
	}
}

// TestCDCEnsureLeafIndex checks the creation of the leaf index for a tree
// built without it: only the owner of the tree table can create it, a second
// call finds it, and a tree table that does not exist is skipped.
func TestCDCEnsureLeafIndex(t *testing.T) {
	ctx := context.Background()
	tx := scratchTx(t, ctx, pgCluster.Node1Pool)
	mtreeTable := pgx.Identifier{leafLookupSchema, "ace_mtree_old"}.Sanitize()
	require.NoError(t, queries.CreateSimpleMtreeTable(ctx, tx, mtreeTable, "integer"))

	// Indexes on the same columns that cannot give the order of the lookup
	// are not the leaf index.
	for _, def := range []string{
		"(range_start) INCLUDE (node_position) WHERE node_level = 0",
		"(range_start DESC, node_position) WHERE node_level = 0",
		"(range_start, node_position)",
	} {
		_, err := tx.Exec(ctx, "CREATE INDEX ON "+mtreeTable+" "+def) // nosemgrep
		require.NoError(t, err)
	}
	exists, indexed, err := queries.MtreeLeafIndexState(ctx, tx, mtreeTable)
	require.NoError(t, err)
	require.True(t, exists)
	require.False(t, indexed)
	_, err = tx.Exec(ctx, "SET LOCAL quote_all_identifiers = on")
	require.NoError(t, err)
	_, indexed, err = queries.MtreeLeafIndexState(ctx, tx, mtreeTable)
	require.NoError(t, err)
	require.False(t, indexed, "a look-alike index is taken with quote_all_identifiers")
	_, err = tx.Exec(ctx, "RESET quote_all_identifiers")
	require.NoError(t, err)

	// A role that may write the tree table but does not own it.
	_, err = tx.Exec(ctx, "CREATE ROLE ace_leaf_index_other NOLOGIN")
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "GRANT USAGE ON SCHEMA "+leafLookupSchema+" TO ace_leaf_index_other") // nosemgrep
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "GRANT SELECT, UPDATE ON "+mtreeTable+" TO ace_leaf_index_other") // nosemgrep
	require.NoError(t, err)
	func() {
		sp, err := tx.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = sp.Rollback(ctx) }()
		_, err = sp.Exec(ctx, "SET LOCAL ROLE ace_leaf_index_other")
		require.NoError(t, err)
		created, err := queries.EnsureMtreeLeafIndex(ctx, sp, mtreeTable, time.Second)
		require.False(t, created)
		var pgErr *pgconn.PgError
		require.True(t, errors.As(err, &pgErr), "want a server error, got %v", err)
		require.Equal(t, "42501", pgErr.Code, "want insufficient_privilege, got %v", err)
	}()

	created, err := queries.EnsureMtreeLeafIndex(ctx, tx, mtreeTable, time.Second)
	require.NoError(t, err)
	require.True(t, created)
	_, indexed, err = queries.MtreeLeafIndexState(ctx, tx, mtreeTable)
	require.NoError(t, err)
	require.True(t, indexed)

	created, err = queries.EnsureMtreeLeafIndex(ctx, tx, mtreeTable, time.Second)
	require.NoError(t, err)
	require.False(t, created, "the index exists, so it must not be created again")

	// With quote_all_identifiers, pg_get_indexdef quotes every name. The
	// index must still be found, or each drain would create one more.
	_, err = tx.Exec(ctx, "SET LOCAL quote_all_identifiers = on")
	require.NoError(t, err)
	_, indexed, err = queries.MtreeLeafIndexState(ctx, tx, mtreeTable)
	require.NoError(t, err)
	require.True(t, indexed, "the leaf index is not found with quote_all_identifiers")
	created, err = queries.EnsureMtreeLeafIndex(ctx, tx, mtreeTable, time.Second)
	require.NoError(t, err)
	require.False(t, created, "the index exists, so it must not be created again")
	_, err = tx.Exec(ctx, "RESET quote_all_identifiers")
	require.NoError(t, err)

	missing := pgx.Identifier{leafLookupSchema, "ace_mtree_missing"}.Sanitize()
	created, err = queries.EnsureMtreeLeafIndex(ctx, tx, missing, time.Second)
	require.NoError(t, err)
	require.False(t, created)
}

// TestCDCLeafIndexForOldTree checks that a CDC drain gives the leaf index to
// a tree that has none, as a tree built by an older version, also when the
// drain is for the changes of another table.
func TestCDCLeafIndexForOldTree(t *testing.T) {
	ctx := context.Background()
	pool := pgCluster.Node1Pool
	names := []string{"leaf_index_changed", "leaf_index_old"}
	tasks := make([]*mtree.MerkleTreeTask, 0, len(names))
	t.Cleanup(func() {
		for _, task := range tasks {
			if err := task.MtreeTeardownTable(); err != nil {
				t.Errorf("MtreeTeardownTable(%s) failed during cleanup: %v", task.QualifiedTableName, err)
			}
		}
		if len(tasks) > 0 {
			if err := tasks[0].MtreeTeardown(); err != nil {
				t.Errorf("MtreeTeardown failed during cleanup: %v", err)
			}
		}
		for _, name := range names {
			dropCDCTestTable(t, name)
		}
	})

	mtreeTables := make([]string, len(names))
	for i, name := range names {
		setupCDCTestTable(t, ctx, name)
		task := newTestMerkleTreeTask(t, fmt.Sprintf("%s.%s", testSchema, name), []string{serviceN1})
		require.NoError(t, task.RunChecks(false))
		if i == 0 {
			require.NoError(t, task.MtreeInit())
		}
		require.NoError(t, task.BuildMtree())
		tasks = append(tasks, task)
		mtreeTables[i] = pgx.Identifier{config.Get().MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, name)}.Sanitize()

		_, indexed, err := queries.MtreeLeafIndexState(ctx, pool, mtreeTables[i])
		require.NoError(t, err)
		require.True(t, indexed, "mtree build must create the leaf index of %s", mtreeTables[i])
	}

	// Make the second tree look like a tree of an older version.
	var index string
	err := pool.QueryRow(ctx, `SELECT i.indexrelid::regclass::text FROM pg_index i
		WHERE i.indrelid = $1::regclass AND NOT i.indisprimary`, mtreeTables[1]).Scan(&index)
	require.NoError(t, err)
	_, err = noDDLReplicationPool(t).Exec(ctx, "DROP INDEX "+index) // nosemgrep
	require.NoError(t, err)

	// Change a row of the first table only, and drain.
	changed := pgx.Identifier{testSchema, names[0]}.Sanitize()
	tag, err := pool.Exec(ctx, "UPDATE "+changed+" SET email = 'leaf-index' WHERE index = 42") // nosemgrep
	require.NoError(t, err)
	require.Equal(t, int64(1), tag.RowsAffected())
	require.NoError(t, cdc.UpdateFromCDC(ctx, pgCluster.ClusterNodes[0]))

	_, indexed, err := queries.MtreeLeafIndexState(ctx, pool, mtreeTables[1])
	require.NoError(t, err)
	require.True(t, indexed, "the drain must create the leaf index of %s", mtreeTables[1])

	var dirty int64
	err = pool.QueryRow(ctx, "SELECT count(*) FROM "+mtreeTables[0]+" WHERE node_level = 0 AND dirty").Scan(&dirty) // nosemgrep
	require.NoError(t, err)
	require.Equal(t, int64(1), dirty, "the drain must mark the leaf of the changed row")
}

// TestCDCEnsureLeafIndexLockTimeout checks that the creation of the leaf
// index does not wait for a long tree update: while another transaction
// writes the tree table, it gives up after the lock timeout with SQLSTATE
// 55P03, and it creates the index once the table is free.
func TestCDCEnsureLeafIndexLockTimeout(t *testing.T) {
	ctx := context.Background()
	pool := noDDLReplicationPool(t)
	const schema = "ace_leaf_lock_test"
	inTx := func(stmts ...func(pgx.Tx) error) {
		tx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(ctx) }()
		for _, stmt := range stmts {
			require.NoError(t, stmt(tx))
		}
		require.NoError(t, tx.Commit(ctx))
	}

	mtreeTable := pgx.Identifier{schema, "ace_mtree_locked"}.Sanitize()
	inTx(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, "CREATE SCHEMA "+schema) // nosemgrep
		return err
	}, func(tx pgx.Tx) error {
		return queries.CreateSimpleMtreeTable(ctx, tx, mtreeTable, "integer")
	})
	t.Cleanup(func() {
		inTx(func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") // nosemgrep
			return err
		})
	})

	writer, err := pool.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = writer.Rollback(context.Background()) })
	_, err = writer.Exec(ctx, "LOCK TABLE "+mtreeTable+" IN ROW EXCLUSIVE MODE") // nosemgrep
	require.NoError(t, err)

	created, err := queries.EnsureMtreeLeafIndex(ctx, pool, mtreeTable, 200*time.Millisecond)
	require.False(t, created)
	var pgErr *pgconn.PgError
	require.True(t, errors.As(err, &pgErr), "want a server error, got %v", err)
	require.Equal(t, "55P03", pgErr.Code, "want lock_not_available, got %v", err)

	require.NoError(t, writer.Rollback(ctx))
	created, err = queries.EnsureMtreeLeafIndex(ctx, pool, mtreeTable, 200*time.Millisecond)
	require.NoError(t, err)
	require.True(t, created)
}
