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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// The unit tests in internal/consistency/mtree check the worker and pool
// numbers that mtree computes. These tests check what the database sees:
// the ACE connections on every node while mtree build, update and
// table-diff run with several workers.
//
// They sample pg_stat_activity with monitorConnections (see
// repset_diff_conntrack_test.go) and measure the rise above a per-node
// baseline, so a backend that a slower node has not yet dropped does not
// count against the phase.

const (
	connCapRows     = 300000
	connCapInterval = 10 * time.Millisecond
	// connCap is the max_connections of TestMtreeMaxConnectionsCapHolds.
	connCap = 3
	// connCapSettle lets pg_stat_activity catch up after a phase ends.
	connCapSettle = 200 * time.Millisecond
)

// connCapPeaks is the most ACE connections a phase added on each node, above
// the baseline taken before it. pooled are client backends, walsenders the
// replication connections of the CDC drain. samples counts the readings taken
// of each node.
type connCapPeaks struct {
	pooled     map[string]int
	walsenders map[string]int
	samples    map[string]int
}

// aceConnBreakdown reads the ACE connections on one node, split into pooled
// client backends and the walsenders of the CDC drain.
func aceConnBreakdown(ctx context.Context, pool *pgxpool.Pool) (pooled, walsenders int, err error) {
	err = pool.QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE backend_type = 'client backend'),
		       count(*) FILTER (WHERE backend_type = 'walsender')
		FROM pg_stat_activity
		WHERE application_name = 'ACE' AND pid <> pg_backend_pid()`).Scan(&pooled, &walsenders)
	return pooled, walsenders, err
}

// connCapEnv sets up a table on all three nodes and a Merkle tree task with
// max_cpu_ratio 1.0, so that without a cap mtree runs one worker per CPU.
// The cap tests need a third node. A node in two node pairs (n1/n2 and n2/n3)
// is where the diff used to open more pools than the cap allowed. With two
// nodes the diff stays at one connection per worker plus one, which the old
// code also held.
type connCapEnv struct {
	env       *testEnv
	pools     map[string]*pgxpool.Pool
	nodes     []string
	table     string
	qualified string
	task      *mtree.MerkleTreeTask
	// Baseline ACE connections per node, taken after init. Each phase
	// measures the rise above this.
	basePooled     map[string]int
	baseWalsenders map[string]int
	baseTotal      map[string]int
}

// newConnCapEnv creates table with connCapRows equal rows on n1, n2 and n3,
// and a Merkle tree task for it on the three-node cluster with
// max_cpu_ratio 1.0 and the given max_connections (0 means no cap). It runs
// the checks and mtree init, records the baseline connections, and tears the
// tree and the table down when the test ends.
func newConnCapEnv(t *testing.T, ctx context.Context, table string, maxConnections int) *connCapEnv {
	t.Helper()
	env := newSpockEnv()
	require.NotNil(t, env.N3Pool, "the connection cap tests need three nodes")
	c := &connCapEnv{
		env:       env,
		pools:     map[string]*pgxpool.Pool{env.ServiceN1: env.N1Pool, env.ServiceN2: env.N2Pool, env.ServiceN3: env.N3Pool},
		nodes:     []string{env.ServiceN1, env.ServiceN2, env.ServiceN3},
		table:     table,
		qualified: testSchema + "." + table,
	}
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	t.Cleanup(func() {
		for _, pool := range c.pools {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+ident) // nosemgrep
		}
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
		removeDiffResults(t, c.qualified)
	})
	for _, pool := range c.pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+ident+" (id int PRIMARY KEY, payload text)") // nosemgrep
		require.NoError(t, err)
		env.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, "TRUNCATE TABLE "+ident) // nosemgrep
			require.NoError(t, err)
			_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s SELECT g, md5(g::text) FROM generate_series(1, %d) g", ident, connCapRows)) // nosemgrep
			require.NoError(t, err)
		})
		_, err = pool.Exec(ctx, "ANALYZE "+ident) // nosemgrep
		require.NoError(t, err)
	}

	c.task = env.newMerkleTreeTask(t, c.qualified, c.nodes)
	c.task.ClusterName = writeThreeNodeClusterConfig(t, env)
	c.task.MaxCpuRatio = 1.0
	c.task.MaxConnections = maxConnections
	c.task.Ctx = ctx
	require.NoError(t, c.task.RunChecks(false))
	require.NoError(t, c.task.MtreeInit())
	t.Cleanup(func() {
		if err := c.task.MtreeTeardown(); err != nil {
			t.Logf("MtreeTeardown cleanup: %v", err)
		}
	})

	c.basePooled = map[string]int{}
	c.baseWalsenders = map[string]int{}
	c.baseTotal = map[string]int{}
	for node, pool := range c.pools {
		pooled, walsenders, err := aceConnBreakdown(ctx, pool)
		require.NoError(t, err)
		c.basePooled[node] = pooled
		c.baseWalsenders[node] = walsenders
		c.baseTotal[node] = pooled + walsenders
	}
	return c
}

// run runs one mtree phase while it watches the connections on every node,
// and returns the peak rise above the baseline.
func (c *connCapEnv) run(t *testing.T, ctx context.Context, phase string, fn func() error) connCapPeaks {
	t.Helper()
	// Wait for the previous phase to drain back to the baseline first, so a
	// backend it has not dropped yet does not count against this phase.
	waitForACEConns(t, ctx, c.pools, c.baseTotal)

	snapshots, stop := monitorConnections(ctx, c.pools, connCapInterval)
	err := fn()
	stop()
	time.Sleep(connCapSettle)
	require.NoError(t, err, phase)

	peaks := connCapPeaks{pooled: map[string]int{}, walsenders: map[string]int{}, samples: map[string]int{}}
	for _, s := range *snapshots {
		peaks.pooled[s.Node] = max(peaks.pooled[s.Node], s.Pooled-c.basePooled[s.Node])
		peaks.walsenders[s.Node] = max(peaks.walsenders[s.Node], s.Walsenders-c.baseWalsenders[s.Node])
		peaks.samples[s.Node]++
	}
	// A node without a sample would be left out of requireAtMost, so the cap
	// would go unchecked on it.
	for node := range c.pools {
		require.NotZero(t, peaks.samples[node], "%s: no pg_stat_activity sample from %s", phase, node)
	}
	t.Logf("%s: peak pooled ACE connections above baseline per node %v, walsenders %v, samples %v", phase, peaks.pooled, peaks.walsenders, peaks.samples)
	return peaks
}

// diverge changes every 500th row on n2 only, so that most blocks differ and
// the diff compares them with all its workers at once. n2 is then in two
// mismatched node pairs, n1/n2 and n2/n3.
func (c *connCapEnv) diverge(t *testing.T, ctx context.Context) (changed []int) {
	t.Helper()
	ident := pgx.Identifier{testSchema, c.table}.Sanitize()
	c.env.withRepairMode(t, ctx, c.env.N2Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, "UPDATE "+ident+" SET payload = 'changed' WHERE id % 500 = 0") // nosemgrep
		require.NoError(t, err)
	})
	for id := 500; id <= connCapRows; id += 500 {
		changed = append(changed, id)
	}
	return changed
}

// requireDiffFound checks that the diff reported every changed row in both
// pairs with n2 and nothing for n1/n3, so that the capped and the uncapped
// runs are both still correct. The diff writes its rows to a report, so the
// result is read back with diffPairs, not from memory. Pair keys follow the
// cluster-node order, so a pair is looked up under either ordering.
func (c *connCapEnv) requireDiffFound(t *testing.T, changed []int) {
	t.Helper()
	pairs := diffPairs(t, c.task)
	findPair := func(a, b string) (types.DiffByNodePair, bool) {
		if d, ok := pairs[a+"/"+b]; ok {
			return d, true
		}
		d, ok := pairs[b+"/"+a]
		return d, ok
	}
	n1, n2, n3 := c.env.ServiceN1, c.env.ServiceN2, c.env.ServiceN3
	for _, other := range []string{n1, n3} {
		pair, ok := findPair(n2, other)
		require.True(t, ok, "no diff for n2/%s; result: %+v", other, pairs)
		require.Equal(t, changed, extractDiffIDs(pair.Rows[n2]), "rows of n2 vs %s", other)
		require.Equal(t, changed, extractDiffIDs(pair.Rows[other]), "rows of %s vs n2", other)
	}
	if pair, ok := findPair(n1, n3); ok {
		require.Empty(t, pair.Rows[n1], "n1 and n3 hold the same rows")
		require.Empty(t, pair.Rows[n3], "n1 and n3 hold the same rows")
	}
}

// requireAtMost checks the pooled connections a phase added against limit.
// The walsenders are checked separately: they are not pooled, and the CDC
// drain needs one per node.
func requireAtMost(t *testing.T, phase string, peaks connCapPeaks, limit int) {
	t.Helper()
	for node, n := range peaks.pooled {
		require.LessOrEqual(t, n, limit, "%s: %d pooled ACE connections above baseline on %s, limit %d", phase, n, node, limit)
	}
	for node, n := range peaks.walsenders {
		require.LessOrEqual(t, n, 1, "%s: %d ACE walsenders on %s; the CDC drain needs one per node", phase, n, node)
	}
}

// TestMtreeMaxConnectionsCapHolds runs mtree build, update and table-diff on
// three nodes with one worker per CPU but max_connections = 3. No node may
// add more than 3 pooled ACE connections in any phase, and the diff must
// still find every changed row.
func TestMtreeMaxConnectionsCapHolds(t *testing.T) {
	ctx := context.Background()
	c := newConnCapEnv(t, ctx, "mtree_conn_cap", connCap)

	peaks := c.run(t, ctx, "mtree build", c.task.BuildMtree)
	requireAtMost(t, "mtree build", peaks, connCap)

	changed := c.diverge(t, ctx)

	peaks = c.run(t, ctx, "mtree update", func() error { return c.task.UpdateMtree(false) })
	requireAtMost(t, "mtree update", peaks, connCap)

	// The update above has drained CDC and recomputed the leaves, so the diff
	// spends its time in the traversal and the range comparison.
	peaks = c.run(t, ctx, "mtree table-diff", c.task.DiffMtree)
	requireAtMost(t, "mtree table-diff", peaks, connCap)
	c.requireDiffFound(t, changed)
}

// TestMtreeWorkersWithoutCap runs the same phases with one worker per CPU and
// no cap, so that CI runs mtree with several workers at all: every other
// mtree integration test uses one. The pools must stay within the sizes the
// mtree code documents, and the diff must find every changed row.
func TestMtreeWorkersWithoutCap(t *testing.T) {
	ctx := context.Background()
	c := newConnCapEnv(t, ctx, "mtree_conn_nocap", 0)

	// Hash workers are a share of the CPUs; the build adds cutting workers
	// and the tree transaction (see buildWorkerPlan).
	cpus := runtime.NumCPU()
	hash := max(1, int(math.Round(float64(cpus)*c.task.MaxCpuRatio)))
	buildLimit := hash + slicer.DefaultWorkers(hash) + 1

	peaks := c.run(t, ctx, "mtree build", c.task.BuildMtree)
	requireAtMost(t, "mtree build", peaks, buildLimit)

	changed := c.diverge(t, ctx)

	// Without a cap, update uses pgx's default pool size.
	peaks = c.run(t, ctx, "mtree update", func() error { return c.task.UpdateMtree(false) })
	requireAtMost(t, "mtree update", peaks, max(4, cpus))

	// The diff opens one pool per node: one connection per worker plus one
	// (see diffPoolSize).
	peaks = c.run(t, ctx, "mtree table-diff", c.task.DiffMtree)
	requireAtMost(t, "mtree table-diff", peaks, hash+1)
	c.requireDiffFound(t, changed)
}
