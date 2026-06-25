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
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestMtreeVsTableDiffPerformance compares the wall-clock cost of a brute-force
// `table-diff` against an `mtree table-diff` (DiffMtree) as divergence grows,
// using SCATTERED (not contiguous) divergence to mimic real replication drift.
//
// The premise: brute-force table-diff hashes and compares every block of the
// table regardless of how little diverged, while mtree only drains the CDC
// delta, recomputes the leaf hashes of the *dirty* blocks, and fetches rows
// from the mismatched leaf ranges. mtree's cost therefore tracks the number of
// DIRTY BLOCKS, which for scattered changes grows with the divergence rate:
// because mtree's minimum block size is 1000 (so 1M rows => ~1000 leaves), the
// dirty-block fraction is roughly divergence% * blockSize, i.e. ~10% of leaves
// at 0.01%, saturating toward all leaves by ~0.1-1%.
//
// Divergence is deterministic and evenly scattered: (index * 2654435761) %
// 1000000 < threshold. The multiplier is coprime to 1,000,000, so the predicate
// selects exactly `threshold` rows spread uniformly across the key space (a
// low-discrepancy set). Even scatter is the conservative worst case for mtree
// (it maximises distinct blocks touched per change), so a pass here is a strong
// claim. The rates nest (0.01% subset of 0.1% subset of 1%), so each round adds
// more divergence on top of the last.
//
// Three rounds run; only the smallest (0.01%) is a pass/fail gate, where the
// dirty-block fraction is low enough that mtree should clearly win. The 0.1% and
// 1% rounds are informational, showing how the advantage changes as scattered
// divergence saturates the blocks.
//
// A large table matters: at 100k rows the brute-force full scan is cheap enough
// that mtree's largely-fixed per-diff overhead never pays off; mtree only wins
// once the avoided full scan is expensive, hence 1M rows here.
func TestMtreeVsTableDiffPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping performance comparison test in short mode")
	}

	const (
		targetRows = 1000000
		// mtree requires block size >= 1000; with targetRows=1M that yields
		// ~1000 leaf blocks.
		blockSize       = 1000
		compareUnitSize = 100
		// scatterMod must match targetRows; multiplier is coprime to it so the
		// predicate (index*mult)%scatterMod < threshold selects exactly
		// `threshold` rows, evenly scattered.
		scatterMult = 2654435761
		scatterMod  = 1000000
	)

	ctx := context.Background()
	env := newSpockEnv()
	tableName := "customers_perf_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodes := []string{env.ServiceN1, env.ServiceN2}

	// --- Seed both nodes with identical data -------------------------------
	// The freshly created table is not in any spock replication set, so the
	// per-node inserts stay local and do not replicate (same approach as
	// TestTableDiffMemoryUsage).
	for i, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf("failed to create table on %s: %v", nodeName, err)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf(
			`INSERT INTO "%s"."%s" SELECT * FROM "%s"."customers_1M" WHERE index <= %d`,
			testSchema, tableName, testSchema, targetRows,
		))
		if err != nil {
			t.Fatalf("failed to populate table on %s: %v", nodeName, err)
		}
		log.Printf("Populated %s with %d rows on %s", qualifiedTableName, targetRows, nodeName)
	}

	// --- Build the merkle tree on the synced data (one-time setup) ----------
	mtreeTask := env.newMerkleTreeTask(t, qualifiedTableName, nodes)
	mtreeTask.BlockSize = blockSize
	mtreeTask.MaxCpuRatio = 1.0 // parallelise build/recompute across CPUs

	require.NoError(t, mtreeTask.RunChecks(false), "mtree RunChecks should succeed")
	require.NoError(t, mtreeTask.MtreeInit(), "MtreeInit should succeed")

	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Logf("cleanup: MtreeTeardown failed: %v", err)
		}
		for _, pool := range []*pgxpool.Pool{env.N1Pool, env.N2Pool} {
			if _, err := pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"."%s" CASCADE`, testSchema, tableName)); err != nil {
				t.Logf("cleanup: failed to drop table: %v", err)
			}
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
		log.Println("Cleanup: perf test table and merkle tree torn down")
	})

	buildStart := time.Now()
	require.NoError(t, mtreeTask.BuildMtree(), "BuildMtree should succeed")
	buildDur := time.Since(buildStart)
	log.Printf("[setup] BuildMtree (one-time, %d rows, blockSize=%d): %s", targetRows, blockSize, buildDur)

	// pairKey is the canonical node-pair key used in DiffResult.NodeDiffs.
	pairKey := env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	countDiffs := func(result types.DiffOutput) int {
		nodeDiffs, ok := result.NodeDiffs[pairKey]
		if !ok {
			return 0
		}
		// Modified rows appear on both sides; the per-node count reflects the
		// number of diverged rows.
		return len(nodeDiffs.Rows[env.ServiceN1])
	}

	// runComparison times an mtree DiffMtree and a brute-force table-diff
	// against the current (already diverged) table state and returns their
	// durations and diff counts.
	runComparison := func(label string) (mtreeDur, bruteDur time.Duration, mtreeCount, bruteCount int) {
		// mtree table-diff. DiffMtree internally runs UpdateMtree (drains CDC,
		// recomputes dirty leaves) before traversing, and resets DiffResult on
		// entry, so the same task can be reused across rounds.
		mStart := time.Now()
		require.NoError(t, mtreeTask.DiffMtree(), "[%s] DiffMtree should succeed", label)
		mtreeDur = time.Since(mStart)
		mtreeCount = countDiffs(mtreeTask.DiffResult)

		// Brute-force table-diff. Fresh task each round for a clean DiffResult.
		td := diff.NewTableDiffTask()
		td.ClusterName = env.ClusterName
		td.DBName = env.DBName
		td.QualifiedTableName = qualifiedTableName
		td.Nodes = strings.Join(nodes, ",")
		td.Output = "json"
		td.BlockSize = blockSize
		td.CompareUnitSize = compareUnitSize
		td.ConcurrencyFactor = 1
		td.MaxDiffRows = math.MaxInt64
		td.DiffResult = types.DiffOutput{
			NodeDiffs: make(map[string]types.DiffByNodePair),
			Summary: types.DiffSummary{
				Nodes:             nodes,
				BlockSize:         td.BlockSize,
				CompareUnitSize:   td.CompareUnitSize,
				ConcurrencyFactor: td.ConcurrencyFactor,
				DiffRowsCount:     make(map[string]int),
			},
		}
		require.NoError(t, td.RunChecks(false), "[%s] table-diff RunChecks should succeed", label)

		bStart := time.Now()
		require.NoError(t, td.ExecuteTask(), "[%s] table-diff ExecuteTask should succeed", label)
		bruteDur = time.Since(bStart)
		bruteCount = countDiffs(td.DiffResult)

		ratio := float64(bruteDur) / float64(mtreeDur)
		log.Printf("=== Perf comparison @ %s scattered divergence ===", label)
		log.Printf("  mtree  table-diff: %-14s (%d diffed rows)", mtreeDur, mtreeCount)
		log.Printf("  brute  table-diff: %-14s (%d diffed rows)", bruteDur, bruteCount)
		log.Printf("  speedup (brute/mtree): %.2fx", ratio)
		log.Printf("=================================================")
		return
	}

	// divergeOnN2 updates a deterministic, evenly-scattered subset of `threshold`
	// rows on node2 only, under repair mode so the change is not replicated to
	// node1 but is still captured by node2's CDC slot for DiffMtree to drain.
	divergeOnN2 := func(threshold int) {
		env.withRepairMode(t, ctx, env.N2Pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, fmt.Sprintf(
				`UPDATE "%s"."%s" SET email = 'scatter%d_' || index::text WHERE (index * %d) %% %d < %d`,
				testSchema, tableName, threshold, scatterMult, scatterMod, threshold,
			))
			require.NoError(t, err, "failed to diverge rows on node2")
		})
	}

	rounds := []struct {
		label        string
		threshold    int // also the approx number of diverged rows (rates nest)
		assertFaster bool
	}{
		{"0.01%", 100, true},  // ~10% of leaf blocks dirty -> mtree should win
		{"0.1%", 1000, false}, // informational
		{"1%", 10000, false},  // informational
	}

	for _, r := range rounds {
		divergeOnN2(r.threshold)
		m, b, mc, bc := runComparison(r.label)
		require.Greater(t, mc, 0, "[%s] mtree should detect the divergence", r.label)
		require.Greater(t, bc, 0, "[%s] brute-force should detect the divergence", r.label)
		if r.assertFaster {
			require.Less(t, m, b,
				"at %s scattered divergence mtree table-diff (%s) should be faster than brute-force (%s)",
				r.label, m, b)
		} else if m < b {
			t.Logf("INFO: at %s scattered divergence mtree was still faster (%s < %s)", r.label, m, b)
		} else {
			t.Logf("INFO: at %s scattered divergence mtree was no longer faster (%s >= %s)", r.label, m, b)
		}
	}

	log.Println("TestMtreeVsTableDiffPerformance completed.")
}
