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
	"fmt"
	"sort"
	"strings"

	"github.com/pgedge/ace/pkg/types"
)

// StoreDiffOutput writes the rows of a diff that is held in memory as a
// result directory at base, with the JSON report and, if o.Format is "html",
// the HTML report next to it. It is meant for tools and tests that have such
// a diff. The rows of the two nodes of a pair are matched by primary key: a
// key on both nodes is a changed entry. The entries of a pair keep the order
// of the input: the rows of the first node, then the rows found only on the
// second node. They are not sorted, and are cut into blocks of blockSize
// entries; 0 means one block. The rows get no row hashes.
func StoreDiffOutput(ctx context.Context, diff types.DiffOutput, base string, blockSize int, o DiffReportOptions) (DiffReportPaths, error) {
	pk := diff.Summary.PrimaryKey
	w := NewDiffResultWriter(diff.Summary.Schema, diff.Summary.Table, pk)
	w.base = base
	pairs := make([]string, 0, len(diff.NodeDiffs))
	for k := range diff.NodeDiffs {
		pairs = append(pairs, k)
	}
	sort.Strings(pairs)
	fail := func(err error) (DiffReportPaths, error) {
		w.Abort()
		return DiffReportPaths{}, err
	}
	for _, pairKey := range pairs {
		names := strings.Split(pairKey, "/")
		if len(names) != 2 {
			return fail(fmt.Errorf("bad node pair %q", pairKey))
		}
		n1, n2 := names[0], names[1]
		rows := diff.NodeDiffs[pairKey].Rows
		type entry struct {
			display    string
			id         string
			row1, row2 types.OrderedMap
		}
		byKey := map[string]*entry{}
		var order []*entry
		for i, nodeRows := range [][]types.OrderedMap{rows[n1], rows[n2]} {
			for idx, r := range nodeRows {
				id := buildRowKey(r, pk, idx)
				e, ok := byKey[id]
				if !ok {
					e = &entry{display: buildRowDisplayKey(r, pk, idx), id: id}
					byKey[id] = e
					order = append(order, e)
				}
				if i == 0 {
					e.row1 = r
				} else {
					e.row2 = r
				}
			}
		}
		size := blockSize
		if size <= 0 {
			size = len(order) + 1
		}
		for start := 0; start < len(order); start += size {
			b := w.OpenBlock(pairKey, n1, n2, 0, start/size)
			for _, e := range order[start:min(start+size, len(order))] {
				kind := DiffKindChanged
				switch {
				case e.row2 == nil:
					kind = DiffKindNode1Only
				case e.row1 == nil:
					kind = DiffKindNode2Only
				}
				if err := b.Add(kind, e.row1, e.row2, nil, nil); err != nil {
					b.Close()
					return fail(err)
				}
			}
			if err := b.Close(); err != nil {
				return fail(err)
			}
		}
	}
	m := DiffResultManifest{
		Schema:     diff.Summary.Schema,
		Table:      diff.Summary.Table,
		PrimaryKey: pk,
		Nodes:      diff.Summary.Nodes,
		Summary:    diff.Summary,
	}
	seen := map[string]bool{}
	for _, pairKey := range pairs {
		rows := diff.NodeDiffs[pairKey].Rows
		nodes := make([]string, 0, len(rows))
		for n := range rows {
			nodes = append(nodes, n)
		}
		sort.Strings(nodes)
		for _, n := range nodes {
			for _, r := range rows[n] {
				for _, kv := range r {
					if kv.Key != "_spock_metadata_" && !seen[kv.Key] {
						seen[kv.Key] = true
						m.Columns = append(m.Columns, DiffResultColumn{Name: kv.Key})
					}
				}
			}
		}
	}
	return w.Finish(ctx, m, o)
}
