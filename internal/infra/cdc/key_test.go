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
	"context"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The relation has the columns (a, b, c); the primary key is (b, a).
var keyTestRel = &pglogrepl.RelationMessage{
	Namespace:    "public",
	RelationName: "t",
	Columns: []*pglogrepl.RelationMessageColumn{
		{Name: "a", Flags: 1},
		{Name: "b", Flags: 1},
		{Name: "c"},
	},
}

var keyTestKey = &tableKey{pos: []int{1, 0}, castTypes: []string{"text", "integer"}}

// textCol returns a column of a change with the value s in the text format.
func textCol(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Length: uint32(len(s)), Data: []byte(s)}
}

// The key comes in the order of the primary key, not of the columns, and
// the texts are kept as they are.
func TestKeyOfChangeOrder(t *testing.T) {
	tup := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol(`a,"b"`), textCol("x")}}
	key, err := keyOfChange(keyTestRel, keyTestKey, tup)
	require.NoError(t, err)
	assert.Equal(t, []string{`a,"b"`, "1"}, key)
}

// An empty text is a key value like any other, not a missing value.
func TestKeyOfChangeEmptyText(t *testing.T) {
	tup := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{textCol("1"), textCol(""), textCol("x")}}
	key, err := keyOfChange(keyTestRel, keyTestKey, tup)
	require.NoError(t, err)
	assert.Equal(t, []string{"", "1"}, key)
}

// A key column with no text value is an error, not a text such as "NULL".
func TestKeyOfChangeNoText(t *testing.T) {
	for _, dt := range []uint8{pglogrepl.TupleDataTypeNull, pglogrepl.TupleDataTypeToast, pglogrepl.TupleDataTypeBinary} {
		tup := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
			textCol("1"), {DataType: dt}, textCol("x"),
		}}
		_, err := keyOfChange(keyTestRel, keyTestKey, tup)
		assert.Error(t, err, "data type %q", dt)
	}
	_, err := keyOfChange(keyTestRel, keyTestKey, nil)
	assert.Error(t, err)
}

// A NULL in a column that is not in the key does not matter.
func TestKeyOfChangeNullOutsideKey(t *testing.T) {
	tup := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		textCol("1"), textCol("b"), {DataType: pglogrepl.TupleDataTypeNull},
	}}
	key, err := keyOfChange(keyTestRel, keyTestKey, tup)
	require.NoError(t, err)
	assert.Equal(t, []string{"b", "1"}, key)
}

// REPLICA IDENTITY FULL is an error also when every column of the relation
// is a key column, so the identity columns match the primary key. The mode
// is checked before any catalog query.
func TestLoadTableKeyFull(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Namespace:       "public",
		RelationName:    "t",
		ReplicaIdentity: 'f',
		Columns:         []*pglogrepl.RelationMessageColumn{{Name: "id", Flags: 1}},
	}
	_, err := loadTableKey(context.Background(), nil, rel)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is FULL")
}

// The cast types of the table (a, b, c) with the primary key (b, a).
var keyTestCast = map[string]string{"a": "integer", "b": "text", "c": "text"}

var keyTestPK = []string{"b", "a"}

// The key columns are found in the order of the primary key, not of the
// columns.
func TestMatchTableKeyOrder(t *testing.T) {
	k, err := matchTableKey(keyTestRel, keyTestPK, keyTestCast)
	require.NoError(t, err)
	assert.Equal(t, keyTestKey.pos, k.pos)
	assert.Equal(t, []string{"text", "integer"}, k.castTypes)
}

// A column added after the commit of the change is not in the relation
// message, and the key is found as before.
func TestMatchTableKeyColumnAdded(t *testing.T) {
	cast := map[string]string{"a": "integer", "b": "text", "c": "text", "d": "text"}
	k, err := matchTableKey(keyTestRel, keyTestPK, cast)
	require.NoError(t, err)
	assert.Equal(t, keyTestKey.pos, k.pos)
}

// A key column renamed after the commit of the change has another name in
// the relation message than in the primary key now.
func TestMatchTableKeyRenamed(t *testing.T) {
	cast := map[string]string{"a2": "integer", "b": "text", "c": "text"}
	_, err := matchTableKey(keyTestRel, []string{"b", "a2"}, cast)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in the change")
}

// An identity column out of the primary key means that the replica identity
// was not the primary key at the commit of the change.
func TestMatchTableKeyOtherColumn(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "t",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "a"}, {Name: "b", Flags: 1}, {Name: "c"},
		},
	}
	_, err := matchTableKey(rel, []string{"a"}, keyTestCast)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in the change")
}

// The identity columns of the relation message must be all the primary key
// columns, not more.
func TestMatchTableKeyCount(t *testing.T) {
	_, err := matchTableKey(keyTestRel, []string{"a"}, keyTestCast)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "in the change")
}
