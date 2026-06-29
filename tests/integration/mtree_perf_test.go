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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// perfScatterMult/perfScatterMod define the deterministic scatter predicate
// used to diverge rows: (index * mult) % mod < threshold. The multiplier is
// coprime to mod (= the row count), so the predicate is a bijection that
// selects exactly `threshold` rows spread uniformly across the key space.
const (
	perfScatterMult = 2654435761
	perfScatterMod  = 1000000
)

// mtreePerf bundles the shared state for the perf comparison so the helpers do
// not each need to capture a dozen variables (and so the test function stays
// within the cyclomatic-complexity and length limits).
type mtreePerf struct {
	t                  *testing.T
	ctx                context.Context
	env                *testEnv
	task               *mtree.MerkleTreeTask
	tableName          string
	qualifiedTableName string
	tableIdent         string // pgx-sanitized "schema"."table" for raw SQL
	nodes              []string
	pairKey            string
	blockSize          int
	compareUnitSize    int
}

// newMtreePerf creates and seeds the table on both nodes, registers cleanup,
// and builds the merkle tree on the synced data. Cleanup is registered BEFORE
// any DB-mutating setup so a mid-setup failure cannot leak the table or partial
// mtree state into the rest of the run.
func newMtreePerf(t *testing.T, ctx context.Context, env *testEnv, tableName string, rows, blockSize, compareUnitSize int) *mtreePerf {
	t.Helper()
	h := &mtreePerf{
		t:                  t,
		ctx:                ctx,
		env:                env,
		tableName:          tableName,
		qualifiedTableName: fmt.Sprintf("%s.%s", testSchema, tableName),
		tableIdent:         pgx.Identifier{testSchema, tableName}.Sanitize(),
		nodes:              []string{env.ServiceN1, env.ServiceN2},
		blockSize:          blockSize,
		compareUnitSize:    compareUnitSize,
	}
	h.pairKey = env.ServiceN1 + "/" + env.ServiceN2
	if strings.Compare(env.ServiceN1, env.ServiceN2) > 0 {
		h.pairKey = env.ServiceN2 + "/" + env.ServiceN1
	}

	// newMerkleTreeTask does no DB work; safe to build before cleanup is set.
	h.task = env.newMerkleTreeTask(t, h.qualifiedTableName, h.nodes)
	h.task.BlockSize = blockSize
	h.task.MaxCpuRatio = 1.0 // parallelise build/recompute across CPUs

	t.Cleanup(h.cleanup)
	h.seed(rows)

	require.NoError(t, h.task.RunChecks(false), "mtree RunChecks should succeed")
	require.NoError(t, h.task.MtreeInit(), "MtreeInit should succeed")

	start := time.Now()
	require.NoError(t, h.task.BuildMtree(), "BuildMtree should succeed")
	log.Printf("[setup] BuildMtree (one-time, %d rows, blockSize=%d): %s", rows, blockSize, time.Since(start))
	return h
}

// cleanup tears down the merkle tree, drops the table on both nodes, and
// removes this table's diff reports. MtreeTeardown and DROP IF EXISTS both
// tolerate partial/absent objects, so it is safe even on a mid-setup failure.
func (h *mtreePerf) cleanup() {
	if err := h.task.MtreeTeardown(); err != nil {
		h.t.Logf("cleanup: MtreeTeardown failed: %v", err)
	}
	for _, pool := range []*pgxpool.Pool{h.env.N1Pool, h.env.N2Pool} {
		if _, err := pool.Exec(h.ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, h.tableIdent)); err != nil { // nosemgrep
			h.t.Logf("cleanup: failed to drop table: %v", err)
		}
	}
	// Diff writers name reports "<schema>_<table>_diffs-<ts>.json"
	// (pkg/common/utils.go); scope cleanup to this table only.
	files, _ := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, h.tableName))
	for _, f := range files {
		os.Remove(f)
	}
	log.Println("Cleanup: perf test table and merkle tree torn down")
}

// seed creates the table and loads `rows` identical rows on both nodes. The
// freshly created table is not in any spock replication set, so the per-node
// inserts stay local and do not replicate.
func (h *mtreePerf) seed(rows int) {
	sourceIdent := pgx.Identifier{testSchema, "customers_1M"}.Sanitize()
	for i, pool := range []*pgxpool.Pool{h.env.N1Pool, h.env.N2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := createTestTable(h.ctx, pool, testSchema, h.tableName); err != nil {
			h.t.Fatalf("failed to create table on %s: %v", nodeName, err)
		}
		if _, err := pool.Exec(h.ctx, fmt.Sprintf( // nosemgrep
			`INSERT INTO %s SELECT * FROM %s WHERE index <= %d`,
			h.tableIdent, sourceIdent, rows,
		)); err != nil {
			h.t.Fatalf("failed to populate table on %s: %v", nodeName, err)
		}
		log.Printf("Populated %s with %d rows on %s", h.qualifiedTableName, rows, nodeName)
	}
}

// countDiffs returns the number of diverged rows reported for the node pair.
// Modified rows appear on both sides, so the per-node count reflects the count.
func (h *mtreePerf) countDiffs(result types.DiffOutput) int {
	nodeDiffs, ok := result.NodeDiffs[h.pairKey]
	if !ok {
		return 0
	}
	return len(nodeDiffs.Rows[h.env.ServiceN1])
}

// divergeOnN2 updates a deterministic, evenly-scattered subset of `threshold`
// rows on node2 only, under repair mode so the change is not replicated to
// node1 but is still captured by node2's CDC slot for DiffMtree to drain.
func (h *mtreePerf) divergeOnN2(threshold int) {
	h.env.withRepairMode(h.t, h.ctx, h.env.N2Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(h.ctx, fmt.Sprintf( // nosemgrep
			`UPDATE %s SET email = 'scatter%d_' || index::text WHERE (index * %d) %% %d < %d`,
			h.tableIdent, threshold, perfScatterMult, perfScatterMod, threshold,
		))
		require.NoError(h.t, err, "failed to diverge rows on node2")
	})
}

// compare times an mtree DiffMtree against a brute-force table-diff on the
// current (already diverged) table state, logs the result, and returns the
// durations and diff counts. DiffMtree internally runs UpdateMtree (drains CDC,
// recomputes dirty leaves) and resets its DiffResult, so the task is reusable.
func (h *mtreePerf) compare(label string) (mtreeDur, bruteDur time.Duration, mtreeCount, bruteCount int) {
	mStart := time.Now()
	require.NoError(h.t, h.task.DiffMtree(), "[%s] DiffMtree should succeed", label)
	mtreeDur = time.Since(mStart)
	mtreeCount = h.countDiffs(h.task.DiffResult)

	// Fresh brute-force task each round for a clean DiffResult.
	td := diff.NewTableDiffTask()
	td.ClusterName = h.env.ClusterName
	td.DBName = h.env.DBName
	td.QualifiedTableName = h.qualifiedTableName
	td.Nodes = strings.Join(h.nodes, ",")
	td.Output = "json"
	td.BlockSize = h.blockSize
	td.CompareUnitSize = h.compareUnitSize
	td.ConcurrencyFactor = 1
	td.MaxDiffRows = math.MaxInt64
	td.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Nodes:             h.nodes,
			BlockSize:         td.BlockSize,
			CompareUnitSize:   td.CompareUnitSize,
			ConcurrencyFactor: td.ConcurrencyFactor,
			DiffRowsCount:     make(map[string]int),
		},
	}
	require.NoError(h.t, td.RunChecks(false), "[%s] table-diff RunChecks should succeed", label)

	bStart := time.Now()
	require.NoError(h.t, td.ExecuteTask(), "[%s] table-diff ExecuteTask should succeed", label)
	bruteDur = time.Since(bStart)
	bruteCount = h.countDiffs(td.DiffResult)

	log.Printf("=== Perf comparison @ %s scattered divergence ===", label)
	log.Printf("  mtree  table-diff: %-14s (%d diffed rows)", mtreeDur, mtreeCount)
	log.Printf("  brute  table-diff: %-14s (%d diffed rows)", bruteDur, bruteCount)
	log.Printf("  speedup (brute/mtree): %.2fx", float64(bruteDur)/float64(mtreeDur))
	log.Printf("=================================================")
	return
}

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
// Only the smallest round (0.01%) is a pass/fail gate, where the dirty-block
// fraction is low enough that mtree should clearly win. The 0.1% and 1% rounds
// are informational, showing how the advantage changes as scattered divergence
// saturates the blocks.
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
		// mtree requires block size >= 1000; with targetRows=1M => ~1000 leaves.
		blockSize       = 1000
		compareUnitSize = 100
	)

	h := newMtreePerf(t, context.Background(), newSpockEnv(), "customers_perf_test", targetRows, blockSize, compareUnitSize)

	rounds := []struct {
		label        string
		threshold    int // exact number of diverged rows selected (rates nest)
		assertFaster bool
	}{
		{"0.01%", 100, true},  // ~10% of leaf blocks dirty -> mtree should win
		{"0.1%", 1000, false}, // informational
		{"1%", 10000, false},  // informational
	}

	for _, r := range rounds {
		h.divergeOnN2(r.threshold)
		m, b, mc, bc := h.compare(r.label)
		// The predicate selects exactly r.threshold rows and only n2 is
		// modified, so both paths must report exactly that many diffs.
		require.Equal(t, r.threshold, mc, "[%s] mtree should detect every divergent row", r.label)
		require.Equal(t, r.threshold, bc, "[%s] brute-force should detect every divergent row", r.label)
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
