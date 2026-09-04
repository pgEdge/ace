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

package queries

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sampleTree() *RelationTree {
	return &RelationTree{
		Root: RelationInfo{Schema: "s", Name: "parent", RelKind: "r", Depth: 0},
		Descendants: []RelationInfo{
			{Schema: "s", Name: "child_fdw", RelKind: "f", Depth: 1, Parent: "s.parent"},
			{Schema: "s", Name: "child_heap", RelKind: "r", Depth: 1, Parent: "s.parent"},
			{Schema: "s", Name: "grandchild_heap", RelKind: "r", Depth: 2, Parent: "s.child_heap"},
		},
	}
}

func TestRelationTree_HasForeign(t *testing.T) {
	assert.True(t, sampleTree().HasForeign())

	plain := &RelationTree{Root: RelationInfo{Schema: "s", Name: "t", RelKind: "r"}}
	assert.False(t, plain.HasForeign())

	foreignRoot := &RelationTree{Root: RelationInfo{Schema: "s", Name: "t", RelKind: "f"}}
	assert.True(t, foreignRoot.HasForeign())
}

func TestRelationTree_ForeignRelations(t *testing.T) {
	assert.Equal(t, []string{"s.child_fdw"}, sampleTree().ForeignRelations())
}

func TestRelationTree_HeapLeaves_IncludesHeapRootAndAllHeapDescendants(t *testing.T) {
	leaves := sampleTree().HeapLeaves()
	names := make([]string, 0, len(leaves))
	for _, l := range leaves {
		names = append(names, l.Qualified())
	}
	assert.Equal(t, []string{"s.parent", "s.child_heap", "s.grandchild_heap"}, names)
}

func TestRelationTree_HeapLeaves_SkipsPartitionedRoot(t *testing.T) {
	tree := &RelationTree{
		Root: RelationInfo{Schema: "s", Name: "p", RelKind: "p"},
		Descendants: []RelationInfo{
			{Schema: "s", Name: "p_1", RelKind: "r", Depth: 1, Parent: "s.p"},
			{Schema: "s", Name: "p_2", RelKind: "f", Depth: 1, Parent: "s.p"},
		},
	}
	leaves := tree.HeapLeaves()
	assert.Len(t, leaves, 1)
	assert.Equal(t, "s.p_1", leaves[0].Qualified())
}

func TestRelationTree_IsInherited(t *testing.T) {
	assert.True(t, sampleTree().IsInherited())
	assert.False(t, (&RelationTree{Root: RelationInfo{RelKind: "r"}}).IsInherited())
}

func TestRelationTree_UnsupportedReason(t *testing.T) {
	cases := []struct {
		name string
		tree *RelationTree
		want string
	}{
		{"heap table", &RelationTree{Root: RelationInfo{Schema: "s", Name: "t", RelKind: "r"}}, ""},
		{"partitioned, heap partitions only", &RelationTree{
			Root:        RelationInfo{Schema: "s", Name: "p", RelKind: "p"},
			Descendants: []RelationInfo{{Schema: "s", Name: "p1", RelKind: "r"}},
		}, ""},
		{"foreign table", &RelationTree{Root: RelationInfo{Schema: "s", Name: "f", RelKind: "f"}}, "is a foreign table; its rows live outside PostgreSQL and it cannot have a primary key, so ACE has nothing to compare"},
		{"view", &RelationTree{Root: RelationInfo{Schema: "s", Name: "v", RelKind: "v"}}, "is a view; ACE compares tables"},
		{"materialized view", &RelationTree{Root: RelationInfo{Schema: "s", Name: "mv", RelKind: "m"}}, "is a view; ACE compares tables"},
		{"sequence", &RelationTree{Root: RelationInfo{Schema: "s", Name: "seq", RelKind: "S"}}, "is a sequence; ACE compares tables"},
		{"composite type", &RelationTree{Root: RelationInfo{Schema: "s", Name: "ct", RelKind: "c"}}, "is a composite type; ACE compares tables"},
		{"index", &RelationTree{Root: RelationInfo{Schema: "s", Name: "ix", RelKind: "i"}}, "is an index; ACE compares tables"},
		{"partitioned index", &RelationTree{Root: RelationInfo{Schema: "s", Name: "pix", RelKind: "I"}}, "is an index; ACE compares tables"},
		{"toast table", &RelationTree{Root: RelationInfo{Schema: "s", Name: "pg_toast_1", RelKind: "t"}}, "is a TOAST table; ACE compares tables"},
		{"unknown relkind", &RelationTree{Root: RelationInfo{Schema: "s", Name: "x", RelKind: "z"}}, `is not a table (relkind "z"); ACE compares tables`},
		{"heap parent with foreign child", sampleTree(),
			"has foreign relations in its inheritance tree (s.child_fdw); ACE does not yet compare tables with foreign children or partitions"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, c.tree.UnsupportedReason())
		})
	}
}
func TestBuildRelationTree_KeysBySchemaAndNameSeparately(t *testing.T) {
	// "a"."b.c" and "a.b"."c" both render as a.b.c. They must stay two
	// relations, so a foreign one among them is not dropped.
	tree, err := buildRelationTree([]RelationInfo{
		{Schema: "a", Name: "p", RelKind: "r", Depth: 0},
		{Schema: "a", Name: "b.c", RelKind: "r", Depth: 1, Parent: "a.p"},
		{Schema: "a.b", Name: "c", RelKind: "f", Depth: 1, Parent: "a.p"},
	})
	require.NoError(t, err)
	require.Len(t, tree.Descendants, 2)
	assert.True(t, tree.HasForeign())
}

func TestBuildRelationTree_DeduplicatesMultipleInheritance(t *testing.T) {
	tree, err := buildRelationTree([]RelationInfo{
		{Schema: "s", Name: "p", RelKind: "r", Depth: 0},
		{Schema: "s", Name: "child", RelKind: "r", Depth: 1, Parent: "s.p"},
		{Schema: "s", Name: "child", RelKind: "r", Depth: 1, Parent: "s.q"},
	})
	require.NoError(t, err)
	assert.Len(t, tree.Descendants, 1)
}

func TestBuildRelationTree_NoRowsMeansNoTable(t *testing.T) {
	tree, err := buildRelationTree(nil)
	require.NoError(t, err)
	assert.Nil(t, tree)
}

func TestBuildRelationTree_DescendantBeforeRootIsAnError(t *testing.T) {
	_, err := buildRelationTree([]RelationInfo{{Schema: "s", Name: "c", RelKind: "r", Depth: 1}})
	require.Error(t, err)
}

func TestHotTableHintText(t *testing.T) {
	heap := &RelationTree{Root: RelationInfo{Schema: "public", Name: "_events", RelKind: "r"}}
	assert.Contains(t, hotTableHintText("public", "events", heap), "compare 'public._events' instead")

	partitioned := &RelationTree{
		Root:        RelationInfo{Schema: "public", Name: "_events", RelKind: "p"},
		Descendants: []RelationInfo{{Schema: "public", Name: "_events_p1", RelKind: "r", Depth: 1}},
	}
	assert.Contains(t, hotTableHintText("public", "events", partitioned), "public._events")

	assert.Equal(t, "", hotTableHintText("public", "events", nil), "no underscore table")

	view := &RelationTree{Root: RelationInfo{Schema: "public", Name: "_events", RelKind: "v"}}
	assert.Equal(t, "", hotTableHintText("public", "events", view), "underscore relation is itself a view")

	withForeignChild := &RelationTree{
		Root:        RelationInfo{Schema: "public", Name: "_events", RelKind: "r"},
		Descendants: []RelationInfo{{Schema: "public", Name: "_events_cold", RelKind: "f", Depth: 1}},
	}
	assert.Equal(t, "", hotTableHintText("public", "events", withForeignChild), "ACE would refuse this table too")
}
