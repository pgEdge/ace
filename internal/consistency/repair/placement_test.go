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

package repair

import (
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func placementTask() *TableRepairTask {
	t := NewTableRepairTask()
	t.Schema = "s"
	t.Table = "parent"
	t.Key = []string{"id"}
	t.heapLeaves = map[string]map[string]bool{
		"n1": {"s.parent": true, "s.child_heap": true},
		"n2": {"s.parent": true, "s.child_heap": true},
	}
	t.rowPlacement = map[string]map[string]string{
		"n1": {"1": "s.parent", "12": "s.child_heap", "50": "s.child_only_on_n1"},
		"n2": {"1": "s.parent"},
	}
	return t
}

func TestTargetRelation_PlainTableIsParent(t *testing.T) {
	task := NewTableRepairTask()
	task.Schema, task.Table = "s", "t"
	assert.Equal(t, pgx.Identifier{"s", "t"}, task.targetRelation("n1", "1"))
}

func TestTargetRelation_RowPresentOnTargetUsesItsOwnRelation(t *testing.T) {
	task := placementTask()
	assert.Equal(t, pgx.Identifier{"s", "parent"}, task.targetRelation("n2", "1"))
	assert.Equal(t, pgx.Identifier{"s", "child_heap"}, task.targetRelation("n1", "12"))
}

func TestTargetRelation_MissingRowUsesSameNamedHeapOnTarget(t *testing.T) {
	task := placementTask()
	// id 12 is missing on n2; on n1 it lives in child_heap, which n2 also has.
	assert.Equal(t, pgx.Identifier{"s", "child_heap"}, task.targetRelation("n2", "12"))
}

func TestTargetRelation_MissingRowFallsBackToParent(t *testing.T) {
	task := placementTask()
	// id 50 lives in a child that n2 does not have.
	assert.Equal(t, pgx.Identifier{"s", "parent"}, task.targetRelation("n2", "50"))
	// unknown everywhere
	assert.Equal(t, pgx.Identifier{"s", "parent"}, task.targetRelation("n2", "999"))
}

func TestGroupByTargetRelation_NodeWithoutForeignRelationsKeepsParentWithoutOnly(t *testing.T) {
	task := placementTask()
	delete(task.heapLeaves, "n3")
	groups := task.groupByTargetRelation("n3", map[string]map[string]any{"12": {"id": 12}})
	require.Len(t, groups, 1)
	g := groups[`"s"."parent"`]
	assert.False(t, g.Only)
	assert.Len(t, g.Rows, 1)
}

func TestGroupByTargetRelation(t *testing.T) {
	task := placementTask()
	rows := map[string]map[string]any{
		"1":  {"id": 1},
		"12": {"id": 12},
		"50": {"id": 50},
	}
	groups := task.groupByTargetRelation("n2", rows)
	require.Len(t, groups, 2)
	assert.Len(t, groups[`"s"."parent"`].Rows, 2)
	assert.Len(t, groups[`"s"."child_heap"`].Rows, 1)
	assert.True(t, groups[`"s"."parent"`].Only)
}

func TestBuildRowPlacement_ReadsStorageRelationMetadata(t *testing.T) {
	diffs := types.DiffOutput{NodeDiffs: map[string]types.DiffByNodePair{
		"n1/n2": {Rows: map[string][]types.OrderedMap{
			"n1": {
				{{Key: "id", Value: 12}, {Key: "val", Value: "x"}, {Key: "_spock_metadata_", Value: map[string]any{"storage_relation": "s.child_heap"}}},
			},
			"n2": {
				{{Key: "id", Value: 1}, {Key: "val", Value: "y"}, {Key: "_spock_metadata_", Value: map[string]any{"storage_relation": "s.parent"}}},
				{{Key: "id", Value: 2}, {Key: "val", Value: "z"}}, // no metadata: ignored
			},
		}},
	}}
	got, err := buildRowPlacement(diffs, []string{"id"})
	require.NoError(t, err)
	assert.Equal(t, "s.child_heap", got["n1"]["12"])
	assert.Equal(t, "s.parent", got["n2"]["1"])
	_, has := got["n2"]["2"]
	assert.False(t, has)
}

func TestSplitQualified_PlainTwoPartName(t *testing.T) {
	id, ok := splitQualified("public.child_heap")
	require.True(t, ok)
	assert.Equal(t, pgx.Identifier{"public", "child_heap"}, id)
}

func TestSplitQualified_OnePartNameFails(t *testing.T) {
	_, ok := splitQualified("child_heap")
	assert.False(t, ok)
}

func TestSplitQualified_EmptyStringFails(t *testing.T) {
	_, ok := splitQualified("")
	assert.False(t, ok)
}

// TestTargetRelation_UnquotedSchemaVisibleThroughSearchPathMatches guards
// against the bug where storage_relation came from tableoid::regclass::text:
// regclass output omits the schema when the relation is search_path-visible
// (e.g. "public"), so a value like "child_heap" (no dot) would fail to split
// and every row would silently fall back to the parent. storage_relation is
// now built from pg_namespace/pg_class directly, so it is always
// "schema.relation" even for a "public" table, and heapLeaves keys (from
// RelationInfo.Qualified()) are in the same unquoted form.
func TestTargetRelation_UnquotedSchemaVisibleThroughSearchPathMatches(t *testing.T) {
	task := NewTableRepairTask()
	task.Schema, task.Table = "public", "ptree"
	task.Key = []string{"id"}
	task.heapLeaves = map[string]map[string]bool{
		"n1": {"public.ptree": true, "public.ptree_child": true},
		"n2": {"public.ptree": true, "public.ptree_child": true},
	}
	task.rowPlacement = map[string]map[string]string{
		"n1": {"11": "public.ptree_child"},
	}
	assert.Equal(t, pgx.Identifier{"public", "ptree_child"}, task.targetRelation("n2", "11"))
}

func TestTargetRelation_SourceOfTruthWinsOverOtherNodesPlacement(t *testing.T) {
	task := placementTask()
	task.SourceOfTruth = "n1"
	// n2 has a heap relation matching both n1's and n0's placement for this
	// key, so which one wins depends purely on iteration order unless the
	// source of truth is checked first. "n0" sorts before "n1", so plain
	// sorted iteration (without the source-of-truth override) would pick
	// n0's relation instead.
	task.heapLeaves["n2"]["s.child_other"] = true
	task.heapLeaves["n0"] = map[string]bool{"s.parent": true, "s.child_other": true}
	task.rowPlacement["n0"] = map[string]string{"12": "s.child_other"}
	// id 12 is missing on n2. n1 (source of truth) says child_heap; n0 (which
	// sorts first) says child_other. The source of truth must win.
	for i := 0; i < 20; i++ {
		assert.Equal(t, pgx.Identifier{"s", "child_heap"}, task.targetRelation("n2", "12"))
	}
}

func TestPlacementRequired_NilExcludedRelationsIsRefused(t *testing.T) {
	err := placementRequired("s", "parent", types.DiffSummary{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "s.parent")
}

func TestPlacementRequired_EmptyNonNilExcludedRelationsIsAllowed(t *testing.T) {
	err := placementRequired("s", "parent", types.DiffSummary{ExcludedRelations: map[string][]string{}})
	assert.NoError(t, err)
}

func TestPlacementRequired_PopulatedExcludedRelationsIsAllowed(t *testing.T) {
	err := placementRequired("s", "parent", types.DiffSummary{ExcludedRelations: map[string][]string{"n1": {"s.child_fdw"}}})
	assert.NoError(t, err)
}

func TestBuildDeleteSQL_SimplePrimaryKeyWithOnly(t *testing.T) {
	sql, args, err := buildDeleteSQL(pgx.Identifier{"s", "child_heap"}, true, true, []string{"id"}, []any{1, 2})
	require.NoError(t, err)
	assert.Equal(t, `DELETE FROM ONLY "s"."child_heap" WHERE "id" IN ($1, $2)`, sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestBuildDeleteSQL_SimplePrimaryKeyWithoutOnly(t *testing.T) {
	sql, args, err := buildDeleteSQL(pgx.Identifier{"s", "parent"}, false, true, []string{"id"}, []any{1, 2})
	require.NoError(t, err)
	assert.Equal(t, `DELETE FROM "s"."parent" WHERE "id" IN ($1, $2)`, sql)
	assert.Equal(t, []any{1, 2}, args)
}

func TestBuildDeleteSQL_CompositeKey(t *testing.T) {
	sql, args, err := buildDeleteSQL(pgx.Identifier{"s", "child_heap"}, true, false, []string{"a", "b"}, []any{[]any{1, "x"}, []any{2, "y"}})
	require.NoError(t, err)
	assert.Equal(t, `DELETE FROM ONLY "s"."child_heap" WHERE ("a", "b") IN (($1, $2), ($3, $4))`, sql)
	assert.Equal(t, []any{1, "x", 2, "y"}, args)
}

func TestBuildDeleteSQL_CompositeKeyWrongShapeErrors(t *testing.T) {
	_, _, err := buildDeleteSQL(pgx.Identifier{"s", "child_heap"}, true, false, []string{"a", "b"}, []any{"not-a-slice"})
	require.Error(t, err)
}
