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

package cdc

// escalator decides, per table, when a bounded CDC drain should stop tracking
// individual changes and instead mark the table's whole Merkle tree dirty for
// one bulk rehash from live table data. Per-change dirty-marking costs a
// range-containment join per PK (UpdateMtreeCounters); once a table's change
// count reaches a meaningful fraction of its rows, a single bulk rehash
// (~the cost of a tree build) is far cheaper. Escalating is always safe:
// leaf hashes are recomputed from live data, so over-marking dirty blocks can
// cost redundant rehash work but can never miss a change.
//
// Not goroutine-safe: bounded drains decode the stream on a single goroutine,
// which is the only place this is used.
type escalator struct {
	minChanges  int64
	fraction    float64
	rowEstimate func(schema, table string) int64
	counts      map[string]int64
	thresholds  map[string]int64
	escalated   map[string]struct{}
}

func newEscalator(minChanges int64, fraction float64, rowEstimate func(schema, table string) int64) *escalator {
	return &escalator{
		minChanges:  minChanges,
		fraction:    fraction,
		rowEstimate: rowEstimate,
		counts:      make(map[string]int64),
		thresholds:  make(map[string]int64),
		escalated:   make(map[string]struct{}),
	}
}

func escKey(schema, table string) string { return schema + "." + table }

// noteChange records one decoded change for schema.table. It returns true
// exactly while the table sits at/over its threshold and has not yet been
// escalated -- the caller performs the mark-all-dirty DB write and then calls
// markEscalated. The threshold is resolved lazily on the first change:
// max(minChanges, fraction*rowEstimate), falling back to minChanges when the
// row estimate is unavailable (<= 0).
func (e *escalator) noteChange(schema, table string) bool {
	k := escKey(schema, table)
	if _, ok := e.escalated[k]; ok {
		return false
	}
	th, ok := e.thresholds[k]
	if !ok {
		th = e.minChanges
		if rows := e.rowEstimate(schema, table); rows > 0 {
			if f := int64(e.fraction * float64(rows)); f > th {
				th = f
			}
		}
		e.thresholds[k] = th
	}
	e.counts[k]++
	return e.counts[k] >= th
}

func (e *escalator) isEscalated(schema, table string) bool {
	_, ok := e.escalated[escKey(schema, table)]
	return ok
}

func (e *escalator) markEscalated(schema, table string) {
	e.escalated[escKey(schema, table)] = struct{}{}
}

// threshold returns the resolved threshold for logging; 0 if not yet resolved.
func (e *escalator) threshold(schema, table string) int64 {
	return e.thresholds[escKey(schema, table)]
}

// purgeTableChanges drops every buffered UPDATE for schema.table across all
// in-flight transactions. Called at escalation time: the table's tree is
// about to be fully marked dirty, so buffered per-PK UPDATE work is redundant
// -- dropping it both skips the expensive containment-join applies and frees
// buffer memory. INSERTs and DELETEs are kept: their per-block counters drive
// block split/merge maintenance, which mark-all-dirty does not cover.
func purgeTableChanges(txChanges map[uint32][]cdcMsg, schema, table string) {
	for xid, msgs := range txChanges {
		kept := msgs[:0]
		for _, m := range msgs {
			if m.schema != schema || m.table != table || m.operation != "UPDATE" {
				kept = append(kept, m)
			}
		}
		txChanges[xid] = kept
	}
}
