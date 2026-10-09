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
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// These tests check how table-diff and mtree build cut a table into blocks
// (see internal/consistency/slicer): no block on the anchor node holds more
// than block_size rows, and the blocks cover the whole key space on every
// node, so table-diff finds rows outside the key range of the anchor.

const slicingBlockSize = 500

// slicingKeyType describes the primary key of one test table. Every row
// has an integer n; the key is built from n, but only the int key sorts in
// the order of n. The checks do not depend on key order.
type slicingKeyType struct {
	name    string
	columns string // column definitions, including the primary key
	insert  string // INSERT ... SELECT over generate_series(lo, hi) as g
	keys    []string
	where   string // the column that holds the integer n
}

func slicingKeyTypes(t *testing.T, ctx context.Context) []slicingKeyType {
	coll := ""
	for _, c := range []string{"und-x-icu", "en_US.utf8", "en_US"} {
		var ok bool
		err := pgCluster.Node1Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = $1)`, c).Scan(&ok)
		require.NoError(t, err)
		if ok {
			coll = c
			break
		}
	}

	out := []slicingKeyType{
		{
			name:    "int",
			columns: "id bigint PRIMARY KEY, n int, v text",
			insert:  "INSERT INTO %s SELECT g, g, md5(g::text) FROM generate_series(%d, %d) g",
			keys:    []string{"id"},
			where:   "n",
		},
		{
			name:    "uuid",
			columns: "id uuid PRIMARY KEY, n int, v text",
			insert:  "INSERT INTO %s SELECT md5(g::text)::uuid, g, md5(g::text) FROM generate_series(%d, %d) g",
			keys:    []string{"id"},
			where:   "n",
		},
		{
			name:    "composite",
			columns: "a int, b text, n int, v text, PRIMARY KEY (a, b)",
			insert:  "INSERT INTO %s SELECT g / 7, 'k' || g, g, md5(g::text) FROM generate_series(%d, %d) g",
			keys:    []string{"a", "b"},
			where:   "n",
		},
	}
	if coll != "" {
		out = append(out, slicingKeyType{
			name:    "text " + coll,
			columns: fmt.Sprintf("id text COLLATE %q PRIMARY KEY, n int, v text", coll),
			insert:  "INSERT INTO %s SELECT CASE WHEN g %% 2 = 0 THEN 'a' ELSE 'B' END || lpad(g::text, 8, '0'), g, md5(g::text) FROM generate_series(%d, %d) g",
			keys:    []string{"id"},
			where:   "n",
		})
	} else {
		t.Log("no non-C collation on the cluster; text key case skipped")
	}
	return out
}

func slicingExec(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sql string) {
	t.Helper()
	newSpockEnv().withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, sql) // nosemgrep
		require.NoError(t, err, sql)
	})
}

// slicingCut cuts the table on n1 as table-diff does when n1 is the anchor,
// checks that the blocks follow each other with no gap and that the first
// and the last block are open, and returns the plan and the statistics.
func slicingCut(t *testing.T, ctx context.Context, table string, key []string, filter string) (*slicer.Plan, *slicer.Stats) {
	t.Helper()
	cfg := slicer.Config{Schema: testSchema, Table: table, Key: key, Filter: filter, BlockSize: slicingBlockSize, Workers: 2}
	plan, err := slicer.PlanParts(ctx, pgCluster.Node1Pool, cfg)
	require.NoError(t, err)

	out := make(chan slicer.Block, 64)
	var blocks []slicer.Block
	done := make(chan struct{})
	go func() {
		for b := range out {
			blocks = append(blocks, b)
		}
		close(done)
	}()
	st, err := slicer.Cut(ctx, pgCluster.Node1Pool, cfg, plan, out)
	close(out)
	<-done
	require.NoError(t, err)

	slicer.SortBlocks(blocks)
	require.Nil(t, blocks[0].Start, "the first block has no lower bound")
	require.Nil(t, blocks[len(blocks)-1].End, "the last block has no upper bound")
	for i := 1; i < len(blocks); i++ {
		require.True(t, reflect.DeepEqual(blocks[i-1].End, blocks[i].Start), "gap after block %d", i-1)
	}
	return plan, st
}

// slicingDiff runs table-diff and returns the number of differing rows.
func slicingDiff(t *testing.T, qualified string, filter string) int {
	t.Helper()
	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.BlockSize = slicingBlockSize
	task.TableFilter = filter
	task.DiffResult.Summary.BlockSize = slicingBlockSize
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.ExecuteTask())
	if task.DiffFilePath == "" {
		return 0
	}
	t.Cleanup(func() { os.Remove(task.DiffFilePath) })

	data, err := os.ReadFile(task.DiffFilePath)
	require.NoError(t, err)
	var out types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &out))
	total := 0
	for _, n := range out.Summary.DiffRowsCount {
		total += n
	}
	return total
}

// TestTableDiffBlockSlicing builds two nodes that differ inside the key
// range of the anchor node and on both sides of it, then checks that no
// block holds more than block_size rows on the anchor and that table-diff
// finds every difference.
func TestTableDiffBlockSlicing(t *testing.T) {
	ctx := context.Background()

	type statsMode struct {
		name       string
		analyze    bool
		stale      bool
		wantSource string
	}
	modes := []statsMode{
		{name: "analyzed", analyze: true, wantSource: "histogram"},
		{name: "no statistics", wantSource: "sample"},
		{name: "stale statistics", analyze: true, stale: true, wantSource: "histogram"},
	}

	for _, kt := range slicingKeyTypes(t, ctx) {
		for _, mode := range modes {
			t.Run(kt.name+"/"+mode.name, func(t *testing.T) {
				table := "slicing_" + strings.NewReplacer(" ", "_", "-", "_", ".", "_").Replace(kt.name)
				qualified := testSchema + "." + table
				ident := pgx.Identifier{testSchema, table}.Sanitize()
				pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

				for _, p := range pools {
					slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
					slicingExec(t, ctx, p, fmt.Sprintf("CREATE TABLE %s (%s) WITH (autovacuum_enabled = false)", ident, kt.columns))
				}
				t.Cleanup(func() {
					for _, p := range pools {
						slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
					}
				})

				// Both nodes: 1000..9000. n1 also has 20001..40000, so it has
				// the most rows and is the anchor. n2 also has 1..999 and
				// 50001..50300, below and above every key of n1; it lacks
				// 3000..4999 and has 8 changed rows.
				hi := 9000
				if mode.stale {
					// ANALYZE sees only a small table; most rows come
					// after it.
					hi = 2000
				}
				for _, p := range pools {
					slicingExec(t, ctx, p, fmt.Sprintf(kt.insert, ident, 1000, hi))
				}
				if !mode.stale {
					slicingExec(t, ctx, pgCluster.Node1Pool, fmt.Sprintf(kt.insert, ident, 20001, 40000))
				}
				if mode.analyze {
					for _, p := range pools {
						slicingExec(t, ctx, p, "ANALYZE "+ident)
					}
				}
				if mode.stale {
					for _, p := range pools {
						slicingExec(t, ctx, p, fmt.Sprintf(kt.insert, ident, 2001, 9000))
					}
					slicingExec(t, ctx, pgCluster.Node1Pool, fmt.Sprintf(kt.insert, ident, 20001, 40000))
				}
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf(kt.insert, ident, 1, 999))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf(kt.insert, ident, 50001, 50300))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN 3000 AND 4999", ident, kt.where))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE %s %% 700 = 0 AND %s BETWEEN 1000 AND 9000", ident, kt.where, kt.where))

				// Rows only on n1: 20000 + 2000 deleted on n2. Rows only on
				// n2: 999 + 300. Changed: the 11 multiples of 700 in
				// 1000..9000 minus the deleted 3500, 4200 and 4900.
				const wantDiffs = 20000 + 2000 + 999 + 300 + 8

				plan, st := slicingCut(t, ctx, table, kt.keys, "")
				require.Equal(t, mode.wantSource, plan.Source, "reason: %s", plan.Reason)
				require.LessOrEqual(t, st.MaxBlockRows, int64(slicingBlockSize))
				require.Equal(t, int64(8001+20000), st.Rows, "rows seen on the anchor")

				diffs := slicingDiff(t, qualified, "")
				require.Equal(t, wantDiffs, diffs)
			})
		}
	}
}

// TestTableDiffBlockSlicingEdges covers an empty anchor, a table with one
// row and a table filter.
func TestTableDiffBlockSlicingEdges(t *testing.T) {
	ctx := context.Background()
	table := "slicing_edges"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	reset := func() {
		for _, p := range pools {
			slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
			slicingExec(t, ctx, p, fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, n int) WITH (autovacuum_enabled = false)", ident))
		}
	}
	t.Cleanup(func() {
		for _, p := range pools {
			slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		}
	})

	t.Run("one row", func(t *testing.T) {
		reset()
		for _, p := range pools {
			slicingExec(t, ctx, p, "INSERT INTO "+ident+" VALUES (42, 1)")
		}
		slicingExec(t, ctx, pgCluster.Node2Pool, "INSERT INTO "+ident+" VALUES (1, 1), (100, 1)")
		diffs := slicingDiff(t, qualified, "")
		require.Equal(t, 2, diffs, "rows below and above the only row of the anchor")
	})

	t.Run("empty anchor", func(t *testing.T) {
		reset()
		// n1 has a larger estimate (more pages) but no live rows.
		slicingExec(t, ctx, pgCluster.Node1Pool, "INSERT INTO "+ident+" SELECT g, 1 FROM generate_series(1, 20000) g")
		slicingExec(t, ctx, pgCluster.Node1Pool, "DELETE FROM "+ident)
		slicingExec(t, ctx, pgCluster.Node2Pool, "INSERT INTO "+ident+" SELECT g, 1 FROM generate_series(1, 50) g")
		diffs := slicingDiff(t, qualified, "")
		require.Equal(t, 50, diffs, "every row of n2 is a difference")
	})

	t.Run("filter", func(t *testing.T) {
		reset()
		for _, p := range pools {
			slicingExec(t, ctx, p, "INSERT INTO "+ident+" SELECT g, g % 5 FROM generate_series(1, 20000) g")
		}
		// Extra rows on n1 make it the anchor: the anchor is the node with
		// the most rows that match the filter. 4000 of them match.
		slicingExec(t, ctx, pgCluster.Node1Pool, "INSERT INTO "+ident+" SELECT g, g % 5 FROM generate_series(20001, 40000) g")
		for _, p := range pools {
			slicingExec(t, ctx, p, "ANALYZE "+ident)
		}
		slicingExec(t, ctx, pgCluster.Node2Pool, "UPDATE "+ident+" SET id = -id WHERE id % 1000 = 0")
		_, st := slicingCut(t, ctx, table, []string{"id"}, "n = 0")
		require.LessOrEqual(t, st.MaxBlockRows, int64(slicingBlockSize))
		require.Equal(t, int64(8000), st.Rows, "only rows that match the filter count")

		diffs := slicingDiff(t, qualified, "n = 0")
		require.Equal(t, 4000+40, diffs, "4000 rows only on n1; 20 rows moved below the key range on n2: 20 missing, 20 extra")
	})
}

// TestBuildMtreeBlockSlicing checks the leaves of a new Merkle tree: they
// follow each other with no gap, the last one has no upper bound, and no
// leaf holds more than block_size rows on the reference node.
func TestBuildMtreeBlockSlicing(t *testing.T) {
	ctx := context.Background()
	table := "slicing_mtree"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	for _, p := range pools {
		slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		slicingExec(t, ctx, p, fmt.Sprintf("CREATE TABLE %s (a int, b int, v text, PRIMARY KEY (a, b))", ident))
		slicingExec(t, ctx, p, "INSERT INTO "+ident+" SELECT g % 4, g, md5(g::text) FROM generate_series(1, 30000) g")
		slicingExec(t, ctx, p, "ANALYZE "+ident)
	}

	task := newTestMerkleTreeTask(t, qualified, []string{serviceN1, serviceN2})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() {
		if err := task.MtreeTeardown(); err != nil {
			t.Logf("Warning: MtreeTeardown failed during cleanup: %v", err)
		}
		for _, p := range pools {
			slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		}
	})
	require.NoError(t, task.BuildMtree())

	mtreeTable := pgx.Identifier{config.Cfg.MTree.Schema, fmt.Sprintf("ace_mtree_%s_%s", testSchema, table)}.Sanitize()
	for i, p := range pools {
		var leaves, bad, gaps, openEnd, total int64
		var maxRows int64
		err := p.QueryRow(ctx, fmt.Sprintf(`
			WITH l AS (
				SELECT node_position, range_start, range_end,
					lead(range_start) OVER (ORDER BY node_position) AS next_start
				FROM %[1]s WHERE node_level = 0
			), c AS (
				SELECT l.*, (SELECT count(*) FROM %[2]s t
					WHERE ROW(t.a, t.b) >= ROW((l.range_start).a, (l.range_start).b)
					  AND (l.range_end IS NULL OR ROW(t.a, t.b) < ROW((l.range_end).a, (l.range_end).b))) AS n
				FROM l
			)
			SELECT count(*),
				count(*) FILTER (WHERE n > %[3]d),
				count(*) FILTER (WHERE next_start IS NOT NULL AND next_start IS DISTINCT FROM range_end),
				count(*) FILTER (WHERE range_end IS NULL),
				sum(n), max(n)
			FROM c`, mtreeTable, ident, task.BlockSize)).Scan(&leaves, &bad, &gaps, &openEnd, &total, &maxRows) // nosemgrep
		require.NoError(t, err)
		t.Logf("node %d: %d leaves, largest %d rows", i+1, leaves, maxRows)
		require.Greater(t, leaves, int64(10))
		require.Zero(t, gaps, "leaves must follow each other with no gap")
		require.Equal(t, int64(1), openEnd, "only the last leaf has no upper bound")
		require.Equal(t, int64(30000), total, "leaves hold every row")
		if i == 0 {
			require.Zero(t, bad, "no leaf holds more than block_size rows on the reference node")
		}
	}
}
