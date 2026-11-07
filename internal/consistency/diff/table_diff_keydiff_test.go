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

package diff

import (
	"reflect"
	"testing"
)

// The plan for inheritance children is taken when any node has children,
// not only the first one, and the check stops at the first such node.
func TestAnyNode(t *testing.T) {
	nodes := []string{"n1", "n2", "n3"}
	var asked []string
	children := map[string]bool{"n2": true}
	got := anyNode(nodes, func(node string) bool {
		asked = append(asked, node)
		return children[node]
	})
	if !got {
		t.Fatal("anyNode = false, want true when the second node has children")
	}
	if want := []string{"n1", "n2"}; !reflect.DeepEqual(asked, want) {
		t.Fatalf("asked %v, want %v", asked, want)
	}

	if anyNode(nodes, func(string) bool { return false }) {
		t.Fatal("anyNode = true, want false when no node has children")
	}
	if !anyNode(nil, func(string) bool { return false }) {
		t.Fatal("anyNode = false for no nodes, want true")
	}
}
