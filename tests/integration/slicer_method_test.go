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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// slicingDiffWith runs table-diff with the given way to cut and returns the
// number of differing rows and the summary of the run.
func slicingDiffWith(t *testing.T, qualified, method string) (int, types.DiffSummary) {
	t.Helper()
	return slicingDiffWithConns(t, qualified, method, 0)
}

// slicingDiffWithConns is slicingDiffWith with a strict number of
// connections per node; 0 means the default.
func slicingDiffWithConns(t *testing.T, qualified, method string, conns int) (int, types.DiffSummary) {
	t.Helper()
	task := newTestTableDiffTask(t, qualified, []string{serviceN1, serviceN2})
	task.BlockSize = slicingBlockSize
	task.Slicer = method
	task.MaxConnections = conns
	task.DiffResult.Summary.BlockSize = slicingBlockSize
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.ExecuteTask())
	summary := task.DiffResult.Summary
	if task.DiffFilePath == "" {
		return 0, summary
	}
	t.Cleanup(func() { os.Remove(task.DiffFilePath) })

	data, err := os.ReadFile(task.DiffFilePath)
	require.NoError(t, err)
	var out types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &out))
	require.NotNil(t, out.Summary.Slicer, "the diff file describes the slicer")
	require.Equal(t, summary.Slicer.Method, out.Summary.Slicer.Method)
	total := 0
	for _, n := range out.Summary.DiffRowsCount {
		total += n
	}
	return total, summary
}

// TestTableDiffSlicerMethods checks that table-diff finds the same
// differences with every way to cut, also below and above the key range of
// the anchor node, and that the auto mode takes the sample only for a key
// that does not follow the heap order.
func TestTableDiffSlicerMethods(t *testing.T) {
	ctx := context.Background()

	// The auto mode needs at least 100 blocks of rows and a table whose
	// pages are not all-visible: the tables are analyzed but never
	// vacuumed. A key built from g follows the heap order; a uuid from md5
	// does not. The text key is left out of the check of auto: its
	// correlation depends on the collation.
	wantAuto := map[string]string{
		"int":       slicer.MethodCutter,
		"uuid":      slicer.MethodSample,
		"composite": slicer.MethodCutter,
	}

	for _, kt := range slicingKeyTypes(t, ctx) {
		for _, method := range []string{slicer.MethodSample, slicer.MethodCutter, slicer.MethodAuto} {
			t.Run(kt.name+"/"+method, func(t *testing.T) {
				table := "slicer_" + strings.NewReplacer(" ", "_", "-", "_", ".", "_").Replace(kt.name)
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

				// Both nodes: 1000..9000. n1 also has 20001..80000, so it is
				// the anchor. n2 also has 1..999 and 90001..90300, below and
				// above every key of n1; it lacks 3000..4999 and has 8
				// changed rows.
				for _, p := range pools {
					slicingExec(t, ctx, p, fmt.Sprintf(kt.insert, ident, 1000, 9000))
				}
				slicingExec(t, ctx, pgCluster.Node1Pool, fmt.Sprintf(kt.insert, ident, 20001, 80000))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf(kt.insert, ident, 1, 999))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf(kt.insert, ident, 90001, 90300))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf("DELETE FROM %s WHERE %s BETWEEN 3000 AND 4999", ident, kt.where))
				slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE %s %% 700 = 0 AND %s BETWEEN 1000 AND 9000", ident, kt.where, kt.where))
				for _, p := range pools {
					slicingExec(t, ctx, p, "ANALYZE "+ident)
				}
				const wantDiffs = 60000 + 2000 + 999 + 300 + 8

				diffs, summary := slicingDiffWith(t, qualified, method)
				require.Equal(t, wantDiffs, diffs)
				require.Equal(t, int64(8001+60000), summary.TotalRowsChecked, "rows of the anchor node")
				require.NotNil(t, summary.Slicer)
				require.Equal(t, method, summary.Slicer.Requested)
				if method != slicer.MethodAuto {
					require.Equal(t, method, summary.Slicer.Method)
				} else if want, ok := wantAuto[kt.name]; ok {
					require.Equal(t, want, summary.Slicer.Method, "reason: %s", summary.Slicer.Reason)
				}
				require.NotNil(t, summary.Slicer.KeyCorrelation, "the key columns are analyzed")
				require.NotNil(t, summary.Slicer.NotAllVisible)
			})
		}
	}
}

// TestTableDiffSampleFallsBackOnClusteredHeap covers a heap that the catalog
// cannot tell from a random one: 64 key ranges are written in turn, 50 rows
// at a time, so the correlation of the key is low, but the rows with near
// keys share heap pages. The auto mode takes the sample from the catalog,
// sees in the sample that neighbour keys lie on one page, and takes the
// cutter before any block is hashed.
func TestTableDiffSampleFallsBackOnClusteredHeap(t *testing.T) {
	ctx := context.Background()
	table := "slicer_chunked"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	for _, p := range pools {
		slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		slicingExec(t, ctx, p, fmt.Sprintf("CREATE TABLE %s (id bigint PRIMARY KEY, n int, v text) WITH (autovacuum_enabled = false)", ident))
		slicingExec(t, ctx, p, fmt.Sprintf(`INSERT INTO %s SELECT ((g - 1) / 50 %% 64) * 1000 + (g - 1) / 3200 * 50 + (g - 1) %% 50, g, md5(g::text)
			FROM generate_series(1, 64000) g`, ident))
		slicingExec(t, ctx, p, "ANALYZE "+ident)
	}
	t.Cleanup(func() {
		for _, p := range pools {
			slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		}
	})
	slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE n %% 20000 = 7", ident))

	diffs, summary := slicingDiffWith(t, qualified, slicer.MethodAuto)
	require.Equal(t, 4, diffs, "n = 7, 20007, 40007, 60007")
	require.NotNil(t, summary.Slicer)
	require.Equal(t, slicer.MethodCutter, summary.Slicer.Method, "reason: %s", summary.Slicer.Reason)
	require.Equal(t, "rows with near keys share heap pages", summary.Slicer.Reason)
	require.NotNil(t, summary.Slicer.KeyCorrelation)
	require.Less(t, *summary.Slicer.KeyCorrelation, 0.1, "the catalog must not show the clustering")
	require.NotNil(t, summary.Slicer.SamplePageShare)
	require.Greater(t, *summary.Slicer.SamplePageShare, 0.5)
}

// TestTableDiffSlicerFewConnections checks both ways to cut with one and two
// connections per node. With one connection, the anchor has no hash worker
// until the connection of the cut is free: the cutter gives it back when
// its parts are cut, and the sample is read before the hashing starts.
func TestTableDiffSlicerFewConnections(t *testing.T) {
	ctx := context.Background()
	table := "slicer_few_conns"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	for _, p := range pools {
		slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		slicingExec(t, ctx, p, fmt.Sprintf("CREATE TABLE %s (id uuid PRIMARY KEY, n int, v text) WITH (autovacuum_enabled = false)", ident))
		slicingExec(t, ctx, p, fmt.Sprintf("INSERT INTO %s SELECT md5(g::text)::uuid, g, md5(g::text) FROM generate_series(1, 20000) g", ident))
	}
	t.Cleanup(func() {
		for _, p := range pools {
			slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident)
		}
	})
	// n1 has 100 more rows, so it is the anchor; n2 has 5 changed rows.
	slicingExec(t, ctx, pgCluster.Node1Pool, fmt.Sprintf("INSERT INTO %s SELECT md5(g::text)::uuid, g, md5(g::text) FROM generate_series(20001, 20100) g", ident))
	slicingExec(t, ctx, pgCluster.Node2Pool, fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE n %% 4000 = 1", ident))
	for _, p := range pools {
		slicingExec(t, ctx, p, "ANALYZE "+ident)
	}

	for _, conns := range []int{1, 2} {
		for _, method := range []string{slicer.MethodSample, slicer.MethodCutter} {
			t.Run(fmt.Sprintf("%s/%d", method, conns), func(t *testing.T) {
				diffs, summary := slicingDiffWithConns(t, qualified, method, conns)
				require.Equal(t, 100+5, diffs)
				require.Equal(t, int64(20100), summary.TotalRowsChecked, "rows of the anchor node")
				require.NotNil(t, summary.Slicer)
				require.Equal(t, method, summary.Slicer.Method)
			})
		}
	}
}
