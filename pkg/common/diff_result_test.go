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

package common

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

func testRow(id int, v string) types.OrderedMap {
	return types.OrderedMap{{Key: "id", Value: id}, {Key: "v", Value: v}}
}

func testHash(b byte) []byte {
	return []byte{b, 0, 0, 0, 0, 0, 0, b}
}

func testManifest() DiffResultManifest {
	return DiffResultManifest{
		HashVersion: 3,
		Schema:      "public",
		Table:       "t",
		PrimaryKey:  []string{"id"},
		Columns:     []DiffResultColumn{{Name: "id", Type: "integer"}, {Name: "v", Type: "text"}},
		Nodes:       []string{"n1", "n2", "n3"},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": 3, "n3/n2": 1}},
	}
}

// newTestWriter returns a writer whose result goes into a temporary
// directory.
func newTestWriter(t *testing.T) *DiffResultWriter {
	t.Helper()
	w := NewDiffResultWriter("public", "t", []string{"id"})
	w.base = filepath.Join(t.TempDir(), "public_t_diffs-20260101000000")
	return w
}

func addTestBlock(t *testing.T, w *DiffResultWriter, pair, n1, n2 string, part, seq int, add func(b *DiffBlockWriter)) {
	t.Helper()
	b := w.OpenBlock(pair, n1, n2, part, seq)
	add(b)
	require.NoError(t, b.Close())
}

// The JSON report built from the block files has the content of a report of
// format version 1: the rows of each node in block order, and the row hash
// of each row at the same position. The nodes of a pair are sorted by name,
// even when node 1 of the pair sorts after node 2.
func TestDiffResultJSONReport(t *testing.T) {
	w := newTestWriter(t)
	// Written out of order: the block names give the order.
	addTestBlock(t, w, "n1/n2", "n1", "n2", 1, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode2Only, nil, testRow(30, "b"), nil, testHash(3)))
	})
	addTestBlock(t, w, "n1/n2", "n1", "n2", 0, 7, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindChanged, testRow(10, "a"), testRow(10, "b"), testHash(1), testHash(2)))
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(20, "a"), nil, testHash(4), nil))
	})
	addTestBlock(t, w, "n1/n2", "n1", "n2", 2, 0, func(*DiffBlockWriter) {})
	addTestBlock(t, w, "n3/n2", "n3", "n2", 0, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(5, "c"), nil, testHash(5), nil))
	})

	paths, err := w.Finish(context.Background(), testManifest(), DiffReportOptions{})
	require.NoError(t, err)
	require.DirExists(t, paths.Dir)
	require.Equal(t, paths.Dir+".json", paths.JSON)
	require.NoFileExists(t, filepath.Join(paths.Dir, "n1_n2", DiffBlockName(2, 0)), "a block with no entries has no file")

	data, err := os.ReadFile(paths.JSON)
	require.NoError(t, err)
	var out types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &out))
	require.Equal(t, types.DiffFormatVersion, out.FormatVersion)
	require.Equal(t, "t", out.Summary.Table)

	ids := func(rows []types.OrderedMap) []string {
		var s []string
		for _, r := range rows {
			v, _ := r.Get("id")
			s = append(s, v.(json.Number).String())
		}
		return s
	}
	p := out.NodeDiffs["n1/n2"]
	require.Equal(t, []string{"10", "20"}, ids(p.Rows["n1"]))
	require.Equal(t, []string{"10", "30"}, ids(p.Rows["n2"]))
	require.Equal(t, []string{"0100000000000001", "0400000000000004"}, p.RowHashes["n1"])
	require.Equal(t, []string{"0200000000000002", "0300000000000003"}, p.RowHashes["n2"])

	q := out.NodeDiffs["n3/n2"]
	require.Empty(t, q.Rows["n2"])
	require.NotNil(t, q.Rows["n2"], "both nodes of a pair have a list")
	require.Equal(t, []string{"5"}, ids(q.Rows["n3"]))

	// Every row is on a line of its own.
	require.GreaterOrEqual(t, strings.Count(string(data), "\n"), 6)

	r, err := OpenDiffResult(paths.JSON)
	require.NoError(t, err)
	require.Equal(t, paths.Dir, r.Dir)
	require.Len(t, r.Manifest.Pairs, 2)
	require.Equal(t, DiffResultPair{Pair: "n1/n2", Node1: "n1", Node2: "n2", Dir: "n1_n2", Blocks: 2,
		Node1Only: 1, Node2Only: 1, Changed: 1}, r.Manifest.Pairs[0])
	require.Equal(t, 3, r.Manifest.HashVersion)
	require.True(t, r.Manifest.IntegerKey)
	files, err := r.BlockFiles(r.Manifest.Pairs[0])
	require.NoError(t, err)
	require.Equal(t, []string{DiffBlockName(0, 7), DiffBlockName(1, 0)},
		[]string{filepath.Base(files[0]), filepath.Base(files[1])})

	// No temporary file is left next to the result.
	entries, err := os.ReadDir(filepath.Dir(paths.Dir))
	require.NoError(t, err)
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.ElementsMatch(t, []string{filepath.Base(paths.Dir), filepath.Base(paths.JSON)}, names)
}

// A run with no entries writes nothing.
func TestDiffResultNoEntries(t *testing.T) {
	w := newTestWriter(t)
	addTestBlock(t, w, "n1/n2", "n1", "n2", 0, 0, func(*DiffBlockWriter) {})
	paths, err := w.Finish(context.Background(), testManifest(), DiffReportOptions{Format: "html"})
	require.NoError(t, err)
	require.Equal(t, DiffReportPaths{}, paths)
	entries, err := os.ReadDir(filepath.Dir(w.base))
	require.NoError(t, err)
	require.Empty(t, entries)
}

// Abort removes everything; a run that fails leaves no result.
func TestDiffResultAbort(t *testing.T) {
	w := newTestWriter(t)
	addTestBlock(t, w, "n1/n2", "n1", "n2", 0, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(1, "a"), nil, testHash(1), nil))
	})
	require.True(t, w.HasEntries())
	w.Abort()
	entries, err := os.ReadDir(filepath.Dir(w.base))
	require.NoError(t, err)
	require.Empty(t, entries)
	_, err = w.Finish(context.Background(), testManifest(), DiffReportOptions{})
	require.Error(t, err, "a result cannot be finished after Abort")
}

// A key that is not a whole number in any entry clears the integer key flag.
func TestDiffResultIntegerKey(t *testing.T) {
	w := newTestWriter(t)
	addTestBlock(t, w, "n1/n2", "n1", "n2", 0, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(1, "a"), nil, testHash(1), nil))
		require.NoError(t, b.Add(DiffKindNode2Only, nil, types.OrderedMap{{Key: "id", Value: "007"}, {Key: "v", Value: "b"}}, nil, testHash(2)))
	})
	paths, err := w.Finish(context.Background(), testManifest(), DiffReportOptions{})
	require.NoError(t, err)
	r, err := OpenDiffResult(paths.Dir)
	require.NoError(t, err)
	require.False(t, r.Manifest.IntegerKey)
}

// A directory without a manifest is not a result.
func TestOpenDiffResultNeedsManifest(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "x_diffs-1")
	require.NoError(t, os.Mkdir(dir, 0o700))
	_, err := OpenDiffResult(dir)
	require.ErrorContains(t, err, "not a complete diff result")
	_, err = OpenDiffResult(dir + "-missing.json")
	require.Error(t, err)
}

// Unsafe characters in node names do not reach the path, and two pairs
// never share a directory.
func TestDiffResultPairDirs(t *testing.T) {
	w := newTestWriter(t)
	addTestBlock(t, w, "a/b_c", "a", "b_c", 0, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(1, "x"), nil, testHash(1), nil))
	})
	addTestBlock(t, w, "a_b/c", "a_b", "c", 0, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(2, "x"), nil, testHash(1), nil))
	})
	addTestBlock(t, w, "../x", "..", "x", 0, 0, func(b *DiffBlockWriter) {
		require.NoError(t, b.Add(DiffKindNode1Only, testRow(3, "x"), nil, testHash(1), nil))
	})
	paths, err := w.Finish(context.Background(), testManifest(), DiffReportOptions{})
	require.NoError(t, err)
	r, err := OpenDiffResult(paths.Dir)
	require.NoError(t, err)
	seen := map[string]bool{}
	for _, p := range r.Manifest.Pairs {
		require.False(t, seen[p.Dir], "two pairs share %s", p.Dir)
		seen[p.Dir] = true
		require.NotContains(t, p.Dir, "/")
		require.DirExists(t, filepath.Join(paths.Dir, p.Dir))
	}
	require.Len(t, seen, 3)
}
