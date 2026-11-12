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
	"fmt"
	"sort"
	"strings"

	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/types"
)

// diffEntry is one entry of a diff result: a row that differs between the
// two nodes of a pair. row1 is the row on the first node of the pair and row2
// the row on the second node; one of them is nil for a row that only one
// node has. key is the key text of the row.
type diffEntry struct {
	key  string
	row1 types.OrderedMap
	row2 types.OrderedMap
}

// diffPair holds the entries of one node pair. node1 and node2 are the nodes
// in the order of the pair name, which a repair plan calls n1 and n2.
type diffPair struct {
	name         string
	node1, node2 string
	entries      []diffEntry
}

// rowsOf returns the rows that node has in the entries of p, by key.
func (p *diffPair) rowsOf(node string) map[string]types.OrderedMap {
	out := make(map[string]types.OrderedMap)
	for _, e := range p.entries {
		row := e.row1
		if node == p.node2 {
			row = e.row2
		} else if node != p.node1 {
			return out
		}
		if row != nil {
			out[e.key] = row
		}
	}
	return out
}

// other returns the node of p that is not node, and false if node is not a
// node of p.
func (p *diffPair) other(node string) (string, bool) {
	switch node {
	case p.node1:
		return p.node2, true
	case p.node2:
		return p.node1, true
	}
	return "", false
}

// diffInput is the diff result that repair works on.
type diffInput struct {
	summary types.DiffSummary
	// key is the primary key that the diff used.
	key []string
	// pairs are sorted by name.
	pairs []*diffPair
}

// nodes returns the names of the nodes of all pairs.
func (d *diffInput) nodes() map[string]bool {
	out := make(map[string]bool)
	for _, p := range d.pairs {
		out[p.node1] = true
		out[p.node2] = true
	}
	return out
}

// readDiffResult reads the entries of a diff result directory. path is the
// directory, or the JSON report next to it. Every entry keeps its kind: a
// row on both nodes is one entry, and the rows of the two nodes are not
// matched again here.
func readDiffResult(path string) (*diffInput, error) {
	r, err := utils.OpenDiffResult(path)
	if err != nil {
		return nil, err
	}
	m := r.Manifest
	if err := types.CheckFormatVersion(m.RowFormatVersion); err != nil {
		return nil, fmt.Errorf("diff result %s: %w", r.Dir, err)
	}
	if len(m.PrimaryKey) == 0 {
		return nil, fmt.Errorf("diff result %s has no primary key", r.Dir)
	}
	in := &diffInput{summary: m.Summary, key: m.PrimaryKey}
	for _, mp := range m.Pairs {
		p, err := readDiffPair(r, mp, m.PrimaryKey)
		if err != nil {
			return nil, fmt.Errorf("diff result %s: %w", r.Dir, err)
		}
		in.pairs = append(in.pairs, p)
	}
	sort.Slice(in.pairs, func(i, j int) bool { return in.pairs[i].name < in.pairs[j].name })
	return in, nil
}

// readDiffPair reads the entries of one pair. The nodes of the pair are put
// in the order of its name; an entry stores the rows in the order of the
// nodes that wrote it.
func readDiffPair(r *utils.StoredDiff, mp utils.DiffResultPair, key []string) (*diffPair, error) {
	names := strings.Split(mp.Pair, "/")
	if len(names) != 2 {
		return nil, fmt.Errorf("bad node pair %q", mp.Pair)
	}
	p := &diffPair{name: mp.Pair, node1: names[0], node2: names[1]}
	var swap bool
	switch {
	case mp.Node1 == p.node1 && mp.Node2 == p.node2:
	case mp.Node1 == p.node2 && mp.Node2 == p.node1:
		swap = true
	default:
		return nil, fmt.Errorf("pair %s has the nodes %s and %s", mp.Pair, mp.Node1, mp.Node2)
	}

	files, err := r.BlockFiles(mp)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool)
	for _, file := range files {
		err := utils.ReadDiffBlock(file, func(line *utils.DiffEntryLine) error {
			e, err := entryOf(line, key)
			if err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
			if swap {
				e.row1, e.row2 = e.row2, e.row1
			}
			// The rows of one key are one entry. Two entries of one key
			// come from a table whose key does not identify a row, such as
			// an inheritance tree, and repair cannot tell them apart.
			if seen[e.key] {
				return fmt.Errorf("%s: pair %s has more than one entry for the key %s", file, mp.Pair, e.key)
			}
			seen[e.key] = true
			p.entries = append(p.entries, e)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

// entryOf decodes one line of a block file.
func entryOf(line *utils.DiffEntryLine, key []string) (diffEntry, error) {
	var e diffEntry
	for _, part := range []struct {
		raw []byte
		row *types.OrderedMap
	}{{line.Row1, &e.row1}, {line.Row2, &e.row2}} {
		if len(part.raw) == 0 {
			continue
		}
		if err := part.row.UnmarshalJSON(part.raw); err != nil {
			return diffEntry{}, err
		}
	}
	switch line.Kind {
	case utils.DiffKindNode1Only:
		if e.row1 == nil || e.row2 != nil {
			return diffEntry{}, fmt.Errorf("entry of kind %s must have the row of node 1 only", line.Kind)
		}
	case utils.DiffKindNode2Only:
		if e.row1 != nil || e.row2 == nil {
			return diffEntry{}, fmt.Errorf("entry of kind %s must have the row of node 2 only", line.Kind)
		}
	case utils.DiffKindChanged:
		if e.row1 == nil || e.row2 == nil {
			return diffEntry{}, fmt.Errorf("entry of kind %s must have the rows of both nodes", line.Kind)
		}
	default:
		return diffEntry{}, fmt.Errorf("unknown kind of a diff entry %q", line.Kind)
	}

	row := e.row1
	if row == nil {
		row = e.row2
	}
	k, err := utils.StringifyOrderedMapKey(row, key)
	if err != nil {
		return diffEntry{}, err
	}
	if e.row1 != nil && e.row2 != nil {
		k2, err := utils.StringifyOrderedMapKey(e.row2, key)
		if err != nil {
			return diffEntry{}, err
		}
		if k2 != k {
			return diffEntry{}, fmt.Errorf("entry has the key %s on node 1 and %s on node 2", k, k2)
		}
	}
	e.key = k
	return e, nil
}
