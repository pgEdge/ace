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

package mtree

import (
	"reflect"
	"testing"

	"github.com/pgedge/ace/db/queries"
)

func TestPublicationRelsToDrop(t *testing.T) {
	parent := &queries.RelationTree{
		Root: rel("parent", "r", 0),
		Descendants: []queries.RelationInfo{
			rel("child_a", "r", 1),
			rel("child_b", "r", 1),
		},
	}
	cases := []struct {
		name      string
		tree      *queries.RelationTree
		published []queries.RelationInfo
		tracked   []string
		want      []string
	}{
		{
			name:      "plain table",
			tree:      &queries.RelationTree{Root: rel("t", "r", 0)},
			published: []queries.RelationInfo{rel("t", "r", 0), rel("other", "r", 0)},
			tracked:   []string{"public.t", "public.other"},
			want:      []string{"public.t"},
		},
		{
			// A child added after the tree was built is not in the
			// publication; dropping the parent must not touch it.
			name:      "children not in the publication",
			tree:      parent,
			published: []queries.RelationInfo{rel("parent", "r", 0)},
			tracked:   []string{"public.parent"},
			want:      []string{"public.parent"},
		},
		{
			// ADD TABLE on the parent added the children too.
			name:      "children added with the parent",
			tree:      parent,
			published: []queries.RelationInfo{rel("parent", "r", 0), rel("child_a", "r", 1), rel("child_b", "r", 1)},
			tracked:   []string{"public.parent"},
			want:      []string{"public.parent", "public.child_a", "public.child_b"},
		},
		{
			name:      "child with a tree of its own stays",
			tree:      parent,
			published: []queries.RelationInfo{rel("parent", "r", 0), rel("child_a", "r", 1), rel("child_b", "r", 1)},
			tracked:   []string{"public.parent", "public.child_a"},
			want:      []string{"public.parent", "public.child_b"},
		},
		{
			name:      "table not in the publication",
			tree:      parent,
			published: []queries.RelationInfo{rel("child_a", "r", 1)},
			tracked:   []string{"public.child_a"},
			want:      nil,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got []string
			for _, r := range publicationRelsToDrop(c.tree, c.published, c.tracked) {
				got = append(got, r.Qualified())
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("publicationRelsToDrop() = %v, want %v", got, c.want)
			}
		})
	}
}
