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

	"github.com/pgedge/ace/internal/consistency/keydiff"
	"github.com/pgedge/ace/pkg/types"
)

// Two blocks of one pair can both find a difference while the pair has room
// for one row only. The block that adds its rows second must add nothing.
func TestAddDiffRowsKeepsPairLimit(t *testing.T) {
	task := &TableDiffTask{
		MaxDiffRows: 1,
		DiffResult:  types.DiffOutput{NodeDiffs: map[string]types.DiffByNodePair{}},
	}
	task.Cols = []string{"id"}
	pairKey := pairKeyFor("n1", "n2")
	row := func(id int) types.OrderedMap {
		return types.OrderedMap{{Key: "id", Value: id}}
	}

	task.addDiffRows(pairKey, "n1", "n2", keydiff.Result{Only1: []types.OrderedMap{row(1)}})
	task.addDiffRows(pairKey, "n1", "n2", keydiff.Result{
		Only1:   []types.OrderedMap{row(2)},
		Only2:   []types.OrderedMap{row(3)},
		Changed: []keydiff.ChangedRow{{Row1: row(4), Row2: row(4)}},
	})

	rows := task.DiffResult.NodeDiffs[pairKey].Rows
	if got := len(rows["n1"]) + len(rows["n2"]); got != 1 {
		t.Fatalf("pair holds %d rows, want 1", got)
	}
	if got := task.DiffResult.Summary.DiffRowsCount[pairKey]; got != 1 {
		t.Fatalf("DiffRowsCount = %d, want 1", got)
	}
	if got := task.pairCounter(pairKey).Load(); got != 1 {
		t.Fatalf("pair counter = %d, want 1", got)
	}
	if !task.DiffResult.Summary.DiffRowLimitReached {
		t.Fatal("DiffRowLimitReached = false, want true")
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
