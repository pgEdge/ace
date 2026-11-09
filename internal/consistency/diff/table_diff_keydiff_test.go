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

// The rows of all node pairs count against one max_diff_rows limit. The
// limit is reached when the count gets to it, and a block that was in work
// at that moment still adds its rows to the count.
func TestCountDiffRowsSharedLimit(t *testing.T) {
	task := &TableDiffTask{MaxDiffRows: 3}

	task.countDiffRows(2)
	if task.diffLimitReached() {
		t.Fatal("limit reached after 2 of 3 rows")
	}
	task.countDiffRows(2)
	if !task.diffLimitReached() {
		t.Fatal("limit not reached after 4 of 3 rows")
	}
	task.countDiffRows(1)
	if got := task.diffRows.Load(); got != 5 {
		t.Fatalf("diffRows = %d, want 5", got)
	}

	noLimit := &TableDiffTask{}
	noLimit.countDiffRows(10)
	if noLimit.diffLimitReached() {
		t.Fatal("limit reached with max_diff_rows = 0")
	}
}

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
