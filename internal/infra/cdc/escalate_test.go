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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func fixedEstimate(rows int64) func(schema, table string) int64 {
	return func(schema, table string) int64 { return rows }
}

func TestEscalatorThresholdIsFractionOfRows(t *testing.T) {
	// 1% of 500k rows = 5000 > min 1000, so the fraction wins.
	e := newEscalator(1000, 0.01, fixedEstimate(500000))
	for i := 0; i < 4999; i++ {
		assert.False(t, e.noteChange("public", "t"), "change %d must not escalate", i)
	}
	assert.True(t, e.noteChange("public", "t"), "5000th change must cross the threshold")
	assert.Equal(t, int64(5000), e.threshold("public", "t"))
}

func TestEscalatorMinChangesFloor(t *testing.T) {
	// 1% of 5k rows = 50 < min 1000, so the floor wins.
	e := newEscalator(1000, 0.01, fixedEstimate(5000))
	assert.Equal(t, int64(0), e.counts["public.t"]) // untouched until first change
	for i := 0; i < 999; i++ {
		assert.False(t, e.noteChange("public", "t"))
	}
	assert.True(t, e.noteChange("public", "t"))
	assert.Equal(t, int64(1000), e.threshold("public", "t"))
}

func TestEscalatorUnknownRowEstimateUsesFloor(t *testing.T) {
	// rowEstimate <= 0 (metadata missing/unreadable) -> threshold = minChanges.
	e := newEscalator(1000, 0.01, fixedEstimate(0))
	assert.Equal(t, int64(0), e.threshold("public", "t")) // unresolved yet
	e.noteChange("public", "t")
	assert.Equal(t, int64(1000), e.threshold("public", "t"))
}

func TestEscalatorStopsCountingOnceEscalated(t *testing.T) {
	e := newEscalator(2, 0, fixedEstimate(0))
	assert.False(t, e.noteChange("public", "t"))
	assert.True(t, e.noteChange("public", "t"))
	// Caller does the DB work, then:
	e.markEscalated("public", "t")
	assert.True(t, e.isEscalated("public", "t"))
	// Further changes never re-trigger.
	assert.False(t, e.noteChange("public", "t"))
	assert.False(t, e.noteChange("public", "t"))
}

func TestEscalatorTablesAreIndependent(t *testing.T) {
	e := newEscalator(2, 0, fixedEstimate(0))
	assert.False(t, e.noteChange("public", "a"))
	assert.True(t, e.noteChange("public", "a"))
	e.markEscalated("public", "a")
	assert.False(t, e.isEscalated("public", "b"))
	assert.False(t, e.noteChange("public", "b"), "table b count must start fresh")
}

func TestPurgeTableChanges(t *testing.T) {
	txChanges := map[uint32][]cdcMsg{
		1: {
			{operation: "INSERT", schema: "public", table: "flood"},
			{operation: "INSERT", schema: "public", table: "keep"},
			{operation: "UPDATE", schema: "public", table: "flood"},
		},
		2: {
			{operation: "DELETE", schema: "public", table: "flood"},
			{operation: "UPDATE", schema: "public", table: "flood"},
		},
	}
	purgeTableChanges(txChanges, "public", "flood")
	// Only flood's UPDATEs are dropped; its INSERT/DELETE survive (they feed
	// block split/merge counters), and other tables are untouched.
	assert.Len(t, txChanges[1], 2)
	assert.Equal(t, "INSERT", txChanges[1][0].operation)
	assert.Equal(t, "flood", txChanges[1][0].table)
	assert.Equal(t, "keep", txChanges[1][1].table)
	assert.Len(t, txChanges[2], 1)
	assert.Equal(t, "DELETE", txChanges[2][0].operation)
}
