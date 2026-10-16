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
	"fmt"
	"strings"
	"testing"

	"github.com/pgedge/ace/db/queries"
)

func rel(name, kind string, depth int) queries.RelationInfo {
	return queries.RelationInfo{Schema: "public", Name: name, RelKind: kind, Depth: depth}
}

func TestMtreeParentReason(t *testing.T) {
	cases := []struct {
		name     string
		tree     *queries.RelationTree
		contains []string // empty means the table is accepted
	}{
		{
			name: "plain table",
			tree: &queries.RelationTree{Root: rel("t", "r", 0)},
		},
		{
			name: "partitioned table",
			tree: &queries.RelationTree{
				Root: rel("p", "p", 0),
				Descendants: []queries.RelationInfo{
					rel("p_2026", "r", 1),
					rel("p_2027", "p", 1),
					rel("p_2027_h1", "r", 2),
				},
			},
			// The intermediate partitioned table is not listed: it holds no
			// rows, so no tree can be built on it either.
			contains: []string{"is a partitioned table", "public.p_2026, public.p_2027_h1)", "table-diff"},
		},
		{
			name:     "partitioned table without partitions",
			tree:     &queries.RelationTree{Root: rel("p", "p", 0)},
			contains: []string{"with no partitions that store rows"},
		},
		{
			name: "partitioned table whose only partition has no partitions",
			tree: &queries.RelationTree{
				Root:        rel("p", "p", 0),
				Descendants: []queries.RelationInfo{rel("p_2027", "p", 1)},
			},
			contains: []string{"with no partitions that store rows"},
		},
		{
			name: "inheritance parent",
			tree: &queries.RelationTree{
				Root:        rel("parent", "r", 0),
				Descendants: []queries.RelationInfo{rel("child", "r", 1)},
			},
			contains: []string{"inheritance (public.child)", "table-diff"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := mtreeParentReason(c.tree)
			if len(c.contains) == 0 {
				if got != "" {
					t.Fatalf("mtreeParentReason() = %q, want accepted", got)
				}
				return
			}
			for _, s := range c.contains {
				if !strings.Contains(got, s) {
					t.Errorf("mtreeParentReason() = %q, want it to contain %q", got, s)
				}
			}
		})
	}
}

func TestJoinQualifiedLimitsTheList(t *testing.T) {
	var rels []queries.RelationInfo
	for i := 0; i < 8; i++ {
		rels = append(rels, rel(fmt.Sprintf("p%d", i), "r", 1))
	}
	got := joinQualified(rels)
	want := "public.p0, public.p1, public.p2, public.p3, public.p4, and 3 more"
	if got != want {
		t.Errorf("joinQualified() = %q, want %q", got, want)
	}
}
