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
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
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

const (
	connCapRows     = 300000
	connCapInterval = 10 * time.Millisecond
	// connCap is the max_connections of TestMtreeMaxConnectionsCapHolds.
	connCap = 3
)

// aceConnPeaks is the most ACE connections seen on each node during one
// phase. Pooled are client backends; walsenders are the replication
// connections of the CDC drain, which are not part of any pool. Combined is
// the most of both seen in one sample.
type aceConnPeaks struct {
	pooled     map[string]int
	walsenders map[string]int
	combined   map[string]int
	samples    int
}

// watchACEConnections samples pg_stat_activity on every node until stop is
// called, and returns the peaks.
func watchACEConnections(ctx context.Context, pools map[string]*pgxpool.Pool) (stop func() aceConnPeaks) {
	peaks := aceConnPeaks{pooled: map[string]int{}, walsenders: map[string]int{}, combined: map[string]int{}}
	var mu sync.Mutex
	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	sample := func() {
		for name, pool := range pools {
			var pooled, walsenders int
			err := pool.QueryRow(watchCtx, `
				SELECT count(*) FILTER (WHERE backend_type = 'client backend'),
				       count(*) FILTER (WHERE backend_type = 'walsender')
				FROM pg_stat_activity
				WHERE application_name = 'ACE' AND pid <> pg_backend_pid()`).Scan(&pooled, &walsenders)
			if err != nil {
				continue // the context was cancelled
			}
			mu.Lock()
			peaks.pooled[name] = max(peaks.pooled[name], pooled)
			peaks.walsenders[name] = max(peaks.walsenders[name], walsenders)
			peaks.combined[name] = max(peaks.combined[name], pooled+walsenders)
			peaks.samples++
			mu.Unlock()
		}
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(connCapInterval)
		defer ticker.Stop()
		for {
			sample()
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() aceConnPeaks {
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		return peaks
	}
}

// threeNodeCluster writes an ACE cluster file with n1, n2 and n3 and returns
// its cluster name. The cluster file of the other tests has n1 and n2 only.
// The cap needs a third node: a node in two node pairs is where the diff used
// to open more pools than the cap allowed. With two nodes, W compare workers
// and the traversal use exactly W + 1 connections, so the old code also held.
func threeNodeCluster(t *testing.T) string {
	t.Helper()
	const name = "test_cluster_3n"
	data, err := os.ReadFile(pgCluster.ClusterName + ".json")
	require.NoError(t, err)
	var cluster types.ClusterConfig
	require.NoError(t, json.Unmarshal(data, &cluster))
	require.Len(t, cluster.NodeGroups, 2)
	n3 := cluster.NodeGroups[1]
	n3.Name = serviceN3
	n3.PublicIP = pgCluster.Node3Host
	n3.Port = pgCluster.Node3Port
	cluster.NodeGroups = append(cluster.NodeGroups, n3)
	cluster.ClusterName = name
	data, err = json.MarshalIndent(cluster, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(name+".json", data, 0o600))
	t.Cleanup(func() { os.Remove(name + ".json") })
	return name
}

// connCapEnv sets up a table on all three nodes and a Merkle tree task with
// max_cpu_ratio 1.0, so that without a cap mtree runs one worker per CPU.
type connCapEnv struct {
	env       *testEnv
	pools     map[string]*pgxpool.Pool
	nodes     []string
	table     string
	qualified string
	task      *mtree.MerkleTreeTask
}

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
	c.task.ClusterName = threeNodeCluster(t)
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
	return c
}

// run runs one mtree phase while it watches the connections on every node.
func (c *connCapEnv) run(t *testing.T, ctx context.Context, phase string, fn func() error) aceConnPeaks {
	t.Helper()
	stop := watchACEConnections(ctx, c.pools)
	err := fn()
	peaks := stop()
	require.NoError(t, err, phase)
	require.NotZero(t, peaks.samples, "%s: no pg_stat_activity samples", phase)
	t.Logf("%s: peak pooled ACE connections per node %v, walsenders %v, both at once %v", phase, peaks.pooled, peaks.walsenders, peaks.combined)
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
// runs are both still correct.
func (c *connCapEnv) requireDiffFound(t *testing.T, changed []int) {
	t.Helper()
	pairKey := func(a, b string) string {
		pair := []string{a, b}
		sort.Strings(pair)
		return pair[0] + "/" + pair[1]
	}
	n1, n2, n3 := c.env.ServiceN1, c.env.ServiceN2, c.env.ServiceN3
	for _, other := range []string{n1, n3} {
		pair, ok := c.task.DiffResult.NodeDiffs[pairKey(n2, other)]
		require.True(t, ok, "no diff for %s", pairKey(n2, other))
		require.Equal(t, changed, extractDiffIDs(pair.Rows[n2]), "rows of n2 in %s", pairKey(n2, other))
		require.Equal(t, changed, extractDiffIDs(pair.Rows[other]), "rows of %s in %s", other, pairKey(n2, other))
	}
	if pair, ok := c.task.DiffResult.NodeDiffs[pairKey(n1, n3)]; ok {
		require.Empty(t, pair.Rows[n1], "n1 and n3 hold the same rows")
		require.Empty(t, pair.Rows[n3], "n1 and n3 hold the same rows")
	}
}

// requireAtMost checks the pooled connections of every node against limit.
// The walsenders are checked separately: they are not pooled.
func requireAtMost(t *testing.T, phase string, peaks aceConnPeaks, limit int) {
	t.Helper()
	for node, n := range peaks.pooled {
		require.LessOrEqual(t, n, limit, "%s: %d pooled ACE connections on %s, limit %d", phase, n, node, limit)
	}
	for node, n := range peaks.walsenders {
		require.LessOrEqual(t, n, 1, "%s: %d ACE walsenders on %s; the CDC drain needs one per node", phase, n, node)
	}
}

// TestMtreeMaxConnectionsCapHolds runs mtree build, update and table-diff on
// three nodes with one worker per CPU but max_connections = 3. No node may
// see more than 3 pooled ACE connections in any phase, and the diff must
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
	if hash >= connCap {
		// With several CPUs the build must have run more than the capped
		// test allows; otherwise that test proves nothing.
		require.Greater(t, maxOf(peaks.pooled), connCap,
			"mtree build never opened more than %d connections on any node with %d hash workers", connCap, hash)
	}

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

func maxOf(m map[string]int) int {
	n := 0
	for _, v := range m {
		n = max(n, v)
	}
	return n
}
