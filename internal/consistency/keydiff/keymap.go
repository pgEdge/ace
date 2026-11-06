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

package keydiff

import (
	"fmt"
	"sort"
)

// rowHashSize is the number of bytes of the row hash that the key query
// returns: the first 8 bytes of the sha256 of the row. Row hashes are only
// compared for the same key, so a changed row is missed only if its two
// versions agree in these 64 bits, with a probability of 2^-64; a shorter
// hash makes the key stream of every mismatched block smaller.
const rowHashSize = 8

// RowHash is the short row hash that the key query returns for every row.
type RowHash [rowHashSize]byte

// entryOverhead is the memory, in bytes, that one key costs in a Go map
// besides the bytes of the key ID and of the row hash: the string header,
// the share of the bucket and the rounding of the allocation. It is an
// estimate. The memory counter uses it so that its value grows with the real
// memory of the comparison.
const entryOverhead = 64

// MemoryLimitError is returned when the keys of one block need more memory
// than the limit.
type MemoryLimitError struct {
	// BuildNode is the node whose keys are in the map. ProbeNode is the node
	// whose rows were checked against the map when the limit was hit; it is
	// empty if the limit was hit before that.
	BuildNode string
	ProbeNode string
	// BuildRows is the number of rows of the block on BuildNode from the
	// block hash, or -1 if it is not known.
	BuildRows int64
	// Used is the memory, in bytes, counted at the stop. If Estimated is
	// true, it is the estimate made before the block was read.
	Used      int64
	Limit     int64
	Estimated bool
}

// Error describes the stop: the node of the map, its number of rows, the
// memory counted or estimated, and the limit.
func (e *MemoryLimitError) Error() string {
	rows := "unknown"
	if e.BuildRows >= 0 {
		rows = fmt.Sprintf("%d", e.BuildRows)
	}
	if e.Estimated {
		return fmt.Sprintf("the keys of the block on node %s (%s rows) are estimated to need %s of memory, "+
			"more than the limit of %s", e.BuildNode, rows, formatMB(e.Used), formatMB(e.Limit))
	}
	where := fmt.Sprintf("the map of node %s (%s rows)", e.BuildNode, rows)
	if e.ProbeNode != "" {
		where += fmt.Sprintf(" and the differing keys of node %s", e.ProbeNode)
	}
	return fmt.Sprintf("the keys of the block need more than %s of memory, the limit (%s held %s)",
		formatMB(e.Limit), where, formatMB(e.Used))
}

// formatMB prints a number of bytes in megabytes with one decimal, or in
// bytes if it is below 0.1 MB.
func formatMB(b int64) string {
	if b < (1<<20)/10 {
		return fmt.Sprintf("%d bytes", b)
	}
	return fmt.Sprintf("%.1f MB", float64(b)/(1<<20))
}

// memCounter counts the memory of the keys of one block. It only grows: a Go
// map does not give memory back when entries are deleted, so the peak of the
// map is what the process holds until the comparison ends.
type memCounter struct {
	limit int64 // 0 means no limit
	used  int64
}

// add counts n more bytes. It returns false when the count is above the
// limit.
func (m *memCounter) add(n int64) bool {
	m.used += n
	return m.limit <= 0 || m.used <= m.limit
}

// keyCost is the memory counted for one key ID.
func keyCost(id string) int64 {
	return int64(len(id)) + rowHashSize + entryOverhead
}

// keyMap holds the keys and row hashes of the build side. A key normally
// has one row. In an inheritance tree the parent and a child can hold rows
// with the same key, so a key can have more rows: the first one is in first,
// the others in more.
type keyMap struct {
	first map[string]RowHash
	more  map[string][]RowHash
	mem   *memCounter
}

func newKeyMap(mem *memCounter) *keyMap {
	return &keyMap{first: make(map[string]RowHash), more: make(map[string][]RowHash), mem: mem}
}

// add puts one row into the map. It returns false when the memory limit is
// passed.
func (m *keyMap) add(id string, h RowHash) bool {
	if _, ok := m.first[id]; ok {
		m.more[id] = append(m.more[id], h)
		return m.mem.add(rowHashSize + entryOverhead)
	}
	m.first[id] = h
	return m.mem.add(keyCost(id))
}

// take looks for a row with the key id and the hash h and removes it.
// found says if the key is in the map, matched says if a row with this hash
// was there.
func (m *keyMap) take(id string, h RowHash) (found, matched bool) {
	h0, ok := m.first[id]
	if !ok {
		return false, false
	}
	extra := m.more[id]
	if h0 == h {
		if len(extra) > 0 {
			m.first[id] = extra[len(extra)-1]
			m.dropExtra(id, len(extra)-1)
			return true, true
		}
		delete(m.first, id)
		return true, true
	}
	for i, x := range extra {
		if x == h {
			extra[i] = extra[len(extra)-1]
			m.dropExtra(id, len(extra)-1)
			return true, true
		}
	}
	return true, false
}

// dropExtra cuts the list of extra rows of id to n entries.
func (m *keyMap) dropExtra(id string, n int) {
	if n == 0 {
		delete(m.more, id)
	} else {
		m.more[id] = m.more[id][:n]
	}
}

// sortedKeys returns the keys left in the map, in byte order, so that the
// result of a comparison does not depend on the order of a Go map.
func (m *keyMap) sortedKeys() []string {
	ids := make([]string, 0, len(m.first))
	for id := range m.first {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// keyList is the list of differing keys, without repeats, in the order they
// were found.
type keyList struct {
	ids  []string
	seen map[string]struct{}
	mem  *memCounter
}

func newKeyList(mem *memCounter) *keyList {
	return &keyList{seen: make(map[string]struct{}), mem: mem}
}

// add puts id into the list if it is not there yet. It returns false when
// the memory limit is passed. inMap says that id is the key string of an
// entry of the map, so its bytes are already counted. A key ID read from the
// probe side is a string of its own, even when the map holds the same key,
// so its bytes are counted in full.
func (l *keyList) add(id string, inMap bool) bool {
	if _, ok := l.seen[id]; ok {
		return true
	}
	l.seen[id] = struct{}{}
	l.ids = append(l.ids, id)
	cost := keyCost(id)
	if inMap {
		cost = rowHashSize + entryOverhead
	}
	return l.mem.add(cost)
}

// keyRow is one row of the key query.
type keyRow struct {
	id   string
	hash RowHash
}

// keySource is a stream of rows of the key query.
type keySource interface {
	Next() bool
	Row() keyRow
	Err() error
}

// errMemory is the error of a key search that passed the memory limit.
type errMemory struct {
	build bool // true if the limit was passed while reading the build side
	used  int64
}

func (e *errMemory) Error() string {
	return fmt.Sprintf("memory limit passed (%d bytes)", e.used)
}

// findDiffKeys reads the build side into a map, then reads the probe side
// and checks each row against the map. It returns the keys of the rows that
// are not equal on the two sides. build may be nil: then the build side has
// no rows.
func findDiffKeys(build, probe keySource, mem *memCounter) ([]string, error) {
	m := newKeyMap(mem)
	if build != nil {
		for build.Next() {
			r := build.Row()
			if !m.add(r.id, r.hash) {
				return nil, &errMemory{build: true, used: mem.used}
			}
		}
		if err := build.Err(); err != nil {
			return nil, err
		}
	}

	list := newKeyList(mem)
	for probe.Next() {
		r := probe.Row()
		_, matched := m.take(r.id, r.hash)
		if matched {
			continue
		}
		if !list.add(r.id, false) {
			return nil, &errMemory{used: mem.used}
		}
	}
	if err := probe.Err(); err != nil {
		return nil, err
	}

	for _, id := range m.sortedKeys() {
		if !list.add(id, true) {
			return nil, &errMemory{used: mem.used}
		}
	}
	return list.ids, nil
}
