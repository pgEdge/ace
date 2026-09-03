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
