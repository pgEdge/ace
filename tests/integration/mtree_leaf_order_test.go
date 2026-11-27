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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMerkleTreeBuildRejectsLeavesOutOfOrder checks that mtree build stops
// when the leaves from a ranges file do not follow each other. CDC takes the
// leaf of a key as the leaf with the largest start that is not greater than
// the key, so with such leaves a change could mark a leaf that does not hold
// the row, and the hash of the leaf that holds it would stay old.
func TestMerkleTreeBuildRejectsLeavesOutOfOrder(t *testing.T) {
	env := newSpockEnv()
	shape := firstBlockKeyShapes[0]
	qualified, safe := newUnreplicatedTable(t, env, "mtree_leaf_order", shape.ddl)
	for _, pool := range env.pools() {
		insertIDRange(t, pool, safe, shape.ins, 1, 30)
	}
	analyzeTable(t, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() {
		if err := mtreeTask.MtreeTeardown(); err != nil {
			t.Errorf("MtreeTeardown cleanup: %v", err)
		}
	})

	for _, tc := range []struct {
		name, ranges string
	}{
		{"decreasing", `[
			{"NodePosition": 0, "RangeStart": [10], "RangeEnd": [20]},
			{"NodePosition": 1, "RangeStart": [20], "RangeEnd": [15]},
			{"NodePosition": 2, "RangeStart": [15], "RangeEnd": null}]`},
		{"gap", `[
			{"NodePosition": 0, "RangeStart": [1], "RangeEnd": [10]},
			{"NodePosition": 1, "RangeStart": [11], "RangeEnd": null}]`},
		{"nested", `[
			{"NodePosition": 0, "RangeStart": [1], "RangeEnd": [25]},
			{"NodePosition": 1, "RangeStart": [10], "RangeEnd": [20]},
			{"NodePosition": 2, "RangeStart": [25], "RangeEnd": null}]`},
		{"bounded last leaf", `[
			{"NodePosition": 0, "RangeStart": [1], "RangeEnd": [10]},
			{"NodePosition": 1, "RangeStart": [10], "RangeEnd": [31]}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rangesFile := filepath.Join(t.TempDir(), "ranges.json")
			require.NoError(t, os.WriteFile(rangesFile, []byte(tc.ranges), 0o600))
			mtreeTask.RangesFile = rangesFile
			err := mtreeTask.BuildMtree()
			require.Error(t, err)
			require.Contains(t, err.Error(), "does not follow the leaf before it")
		})
	}

	// Leaves that follow each other build.
	rangesFile := filepath.Join(t.TempDir(), "ranges.json")
	require.NoError(t, os.WriteFile(rangesFile, []byte(`[
		{"NodePosition": 0, "RangeStart": [1], "RangeEnd": [10]},
		{"NodePosition": 1, "RangeStart": [10], "RangeEnd": null}]`), 0o600))
	mtreeTask.RangesFile = rangesFile
	require.NoError(t, mtreeTask.BuildMtree())
}
