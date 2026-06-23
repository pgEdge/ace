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
	"os"
	"path/filepath"
	"testing"

	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression for issue #124: spock.node.node_id / spock.subscription.sub_id are
// `oid` columns and fail to scan into int64 in the binary protocol. Needs a real
// spock cluster with rows; the native-PG "spock not installed" test never reaches
// this scan.
func TestGetSpockNodeAndSubInfo_ScansOIDColumns(t *testing.T) {
	ctx := context.Background()

	infos, err := queries.GetSpockNodeAndSubInfo(ctx, pgCluster.Node1Pool)
	require.NoError(t, err,
		"scanning spock.node/spock.subscription must not fail on oid columns")

	// The mesh guarantees rows; without them the scan path wouldn't run and the
	// test would pass even with the bug present.
	require.NotEmpty(t, infos,
		"expected subscription rows on a spock mesh node")

	for _, info := range infos {
		assert.NotZero(t, info.NodeID, "node_id should be populated")
		assert.NotEmpty(t, info.NodeName, "node_name should be populated")
		assert.NotEmpty(t, info.SubName, "sub_name should be populated (query filters NULLs)")
	}
}

// End-to-end regression for issue #124: the full `ace spock-diff` command path
// must complete against a real spock cluster (it previously failed at the first
// GetSpockNodeAndSubInfo call).
func TestSpockDiff_SucceedsOnSpockCluster(t *testing.T) {
	t.Cleanup(func() {
		files, err := filepath.Glob("spock_diffs-*.json")
		if err != nil {
			t.Logf("failed to glob spock diff artifacts: %v", err)
			return
		}
		for _, f := range files {
			if err := os.Remove(f); err != nil {
				t.Logf("failed to remove %s: %v", f, err)
			}
		}
	})

	task := diff.NewSpockDiffTask()
	task.ClusterName = pgCluster.ClusterName
	task.DBName = dbName
	task.Nodes = serviceN1 + "," + serviceN2
	task.Ctx = context.Background()
	task.SkipDBUpdate = true

	require.NoError(t, task.RunChecks(false),
		"spock-diff RunChecks should connect to the spock nodes")

	require.NoError(t, task.ExecuteTask(),
		"spock-diff must complete against a spock-enabled cluster")

	// Both selected nodes should have had their spock config fetched and scanned.
	require.Contains(t, task.DiffResult.SpockConfigs, serviceN1)
	require.Contains(t, task.DiffResult.SpockConfigs, serviceN2)

	// Two selected nodes yield exactly one comparison pair.
	require.Len(t, task.DiffResult.Diffs, 1,
		"expected a single %s/%s comparison", serviceN1, serviceN2)

	// Healthy bidirectional mesh: matching reciprocal subscriptions by node
	// identity (not name) must report no mismatch. The old name-pattern logic
	// produced false "missing subscription" diffs here.
	for pair, d := range task.DiffResult.Diffs {
		assert.False(t, d.Mismatch,
			"healthy mesh pair %s should report no differences, got: %+v", pair, d.Details)
		assert.Empty(t, d.Details.Subscriptions.MissingOnNode1, "pair %s", pair)
		assert.Empty(t, d.Details.Subscriptions.MissingOnNode2, "pair %s", pair)
	}
}
