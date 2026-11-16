package mtree

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/types"
)

// The differing rows of all node pairs count against one max_diff_rows.
// Reaching the limit, also exactly, stops the workers and marks the diff.
func TestMtreeDiffRowLimitIsForTheWholeDiff(t *testing.T) {
	m := &MerkleTreeTask{MaxDiffRows: 5}
	m.countDiffRows(3)
	if m.diffLimitHit.Load() || m.compareStopped() {
		t.Fatal("3 rows of 5 must not stop the diff")
	}
	m.countDiffRows(2)
	if !m.diffLimitHit.Load() || !m.compareStopped() {
		t.Fatal("5 rows of 5 must stop the diff")
	}
	m.countDiffRows(4)
	if got := m.diffRows.Load(); got != 9 {
		t.Fatalf("counted %d rows, want 9: a range that was in work when the limit was reached still counts", got)
	}
}

// With no limit the diff never stops on rows.
func TestMtreeDiffNoLimit(t *testing.T) {
	m := &MerkleTreeTask{}
	m.countDiffRows(1 << 30)
	if m.diffLimitHit.Load() || m.compareStopped() {
		t.Fatal("a diff with no max_diff_rows must not stop on rows")
	}
}

// A negative max_diff_rows is rejected before any work runs.
func TestMtreeDiffRejectsNegativeMaxDiffRows(t *testing.T) {
	m := &MerkleTreeTask{MaxDiffRows: -1, SkipDBUpdate: true}
	m.Ctx = context.Background()

	err := m.DiffMtree()
	if err == nil || !strings.Contains(err.Error(), "max_diff_rows must be >= 0") {
		t.Fatalf("expected max_diff_rows validation error, got %v", err)
	}
}

// TestHashVersionError checks which stored hash versions a writer of leaf
// hashes accepts. A newer version is always refused: this build must not
// recompute leaves of a tree that a newer ACE wrote. An older version is
// accepted only by mtree update, which recomputes every leaf and the version
// in one transaction; any other writer would mix two algorithms in one tree.
func TestHashVersionError(t *testing.T) {
	m := &MerkleTreeTask{}
	m.Schema = "public"
	m.Table = "t"
	cur := queries.CurrentHashVersion

	tests := []struct {
		version    int
		allowOlder bool
		wantErr    []string // nil: no error
	}{
		{cur, false, nil},
		{cur, true, nil},
		{cur - 1, true, nil},
		{cur - 1, false, []string{"public.t", "n1", fmt.Sprintf("hash version %d, older", cur-1), "mtree update"}},
		{cur + 1, true, []string{"public.t", "n1", fmt.Sprintf("hash version %d,", cur+1), "mtree build"}},
		{cur + 1, false, []string{"public.t", "n1", fmt.Sprintf("hash version %d,", cur+1), "mtree build"}},
	}
	for _, tt := range tests {
		err := m.hashVersionError(tt.version, "n1", tt.allowOlder)
		if tt.wantErr == nil {
			if err != nil {
				t.Errorf("hashVersionError(%d, allowOlder=%v) = %v, want nil", tt.version, tt.allowOlder, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("hashVersionError(%d, allowOlder=%v) = nil, want an error", tt.version, tt.allowOlder)
			continue
		}
		for _, want := range tt.wantErr {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("hashVersionError(%d, allowOlder=%v) = %q, want it to mention %q", tt.version, tt.allowOlder, err, want)
			}
		}
	}
}

// A ranges file must give the first block position 0, or the first block gets
// a lower bound. renumberBlockRanges uses the order of the blocks in the file.
func TestRenumberBlockRanges(t *testing.T) {
	tests := []struct {
		name        string
		positions   []int64
		wantChanged bool
	}{
		{"already numbered", []int64{0, 1, 2}, false},
		{"starts at 1", []int64{1, 2, 3}, true},
		{"gap", []int64{0, 2, 3}, true},
		{"missing positions", []int64{0, 0, 0}, true},
		{"empty", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ranges := make([]types.BlockRange, len(tt.positions))
			for i, p := range tt.positions {
				ranges[i] = types.BlockRange{NodePosition: p, RangeStart: types.KeyText{strconv.Itoa(i * 10)}}
			}
			if got := renumberBlockRanges(ranges); got != tt.wantChanged {
				t.Errorf("changed = %v, want %v", got, tt.wantChanged)
			}
			for i, r := range ranges {
				if r.NodePosition != int64(i) {
					t.Errorf("block %d has position %d", i, r.NodePosition)
				}
				if r.RangeStart[0] != strconv.Itoa(i*10) {
					t.Errorf("block %d moved: start %v", i, r.RangeStart[0])
				}
			}
		})
	}
}

// A primary key column with another type on a node stops the diff; other
// columns may differ.
func TestKeyTypesMatch(t *testing.T) {
	ref := map[string]string{"id": "character varying(3)", "v": "text"}
	if err := keyTypesMatch([]string{"id"}, ref, map[string]string{"id": "character varying(3)", "v": "integer"}, "n1", "n2"); err != nil {
		t.Fatalf("a non-key column of another type must not stop the diff: %v", err)
	}
	err := keyTypesMatch([]string{"id"}, ref, map[string]string{"id": "text", "v": "text"}, "n1", "n2")
	if err == nil || !strings.Contains(err.Error(), `"id" has type character varying(3) on n1 and text on n2`) {
		t.Fatalf("got %v, want an error that names the column, both types and both nodes", err)
	}
}

// The row read of a node uses the cast types of that node, and the types of
// the reference node when the node was not read.
func TestNodeCastTypes(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ColTypes = map[string]map[string]string{}
	key := []string{"id"}
	if err := m.addNodeColTypes("n1", map[string]string{"id": "integer", "v": "text"}, map[string]string{"id": "integer", "v": "text"}, key); err != nil {
		t.Fatal(err)
	}
	if err := m.addNodeColTypes("n2", map[string]string{"id": "integer", "v": "jsonb"}, map[string]string{"id": "integer", "v": "jsonb"}, key); err != nil {
		t.Fatal(err)
	}
	if got := m.nodeCastTypes("n2")["v"]; got != "jsonb" {
		t.Errorf("n2: got %s, want jsonb", got)
	}
	if got := m.nodeCastTypes("n3")["v"]; got != "text" {
		t.Errorf("n3: got %s, want the reference type text", got)
	}
}

// The types of the reference node are kept apart from the types of the
// nodes, so a node name cannot replace them.
func TestNodeNameDoesNotReplaceReferenceTypes(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ColTypes = map[string]map[string]string{}
	key := []string{"id"}
	if err := m.addNodeColTypes("n1", map[string]string{"id": "integer", "v": "text"}, nil, key); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"_ref", "ref", ""} {
		err := m.addNodeColTypes(name, map[string]string{"id": "bigint", "v": "text"}, nil, key)
		if err == nil || !strings.Contains(err.Error(), "primary key column") {
			t.Errorf("node %q with another key type: got %v, want a key type error", name, err)
		}
		if got := m.refColTypes["id"]; got != "integer" {
			t.Errorf("after node %q the reference key type is %s, want integer", name, got)
		}
	}
}
