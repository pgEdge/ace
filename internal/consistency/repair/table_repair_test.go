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
	"os"
	"path/filepath"
	"testing"
	"time"

	pgxv5type "github.com/jackc/pgx/v5/pgtype"
	"github.com/pgedge/ace/internal/consistency/repair/plan"
	"github.com/stretchr/testify/require"
)

func TestConvertValueForType_TimestampWrapsInPgxTimestamp(t *testing.T) {
	// When a time.Time value is passed for a "timestamp without time zone"
	// column, convertValueForType must wrap it in pgxv5type.Timestamp so
	// pgx sends the value as "timestamp" instead of "timestamptz". This prevents PostgreSQL from
	// applying a session-timezone conversion that shifts the value.
	ts := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name    string
		colType string
		wantPgx bool // true → expect pgxv5type.Timestamp; false → expect time.Time
	}{
		{"timestamp without time zone", "timestamp without time zone", true},
		{"TIMESTAMP WITHOUT TIME ZONE (upper)", "TIMESTAMP WITHOUT TIME ZONE", true},
		{"timestamp with time zone", "timestamp with time zone", false},
		{"timestamptz via format_type", "timestamp with time zone", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertValueForType(ts, tt.colType)
			require.NoError(t, err)

			if tt.wantPgx {
				pgTS, ok := got.(pgxv5type.Timestamp)
				require.True(t, ok, "expected pgxv5type.Timestamp, got %T", got)
				require.True(t, pgTS.Valid)
				require.True(t, pgTS.Time.Equal(ts))
			} else {
				tv, ok := got.(time.Time)
				require.True(t, ok, "expected time.Time, got %T", got)
				require.True(t, tv.Equal(ts))
			}
		})
	}
}

func TestConvertValueForType_StringTimestamp(t *testing.T) {
	// Values coming from diff JSON are strings. They should be parsed and
	// wrapped in pgxv5type.Timestamp for "timestamp without time zone".
	val, err := convertValueForType("2024-06-15T10:30:00Z", "timestamp without time zone")
	require.NoError(t, err)

	pgTS, ok := val.(pgxv5type.Timestamp)
	require.True(t, ok, "expected pgxv5type.Timestamp, got %T", val)
	require.True(t, pgTS.Valid)

	expected := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	require.True(t, pgTS.Time.Equal(expected))
}

func TestConvertValueForType_StringTimestamptz(t *testing.T) {
	// Values for "timestamp with time zone" columns should remain time.Time.
	val, err := convertValueForType("2024-06-15T10:30:00Z", "timestamp with time zone")
	require.NoError(t, err)

	tv, ok := val.(time.Time)
	require.True(t, ok, "expected time.Time, got %T", val)

	expected := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	require.True(t, tv.Equal(expected))
}

func TestConvertValueForType_Nil(t *testing.T) {
	val, err := convertValueForType(nil, "timestamp without time zone")
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestConvertValueForType_StringTime(t *testing.T) {
	// Values from diff JSON for "time without time zone" columns should be
	// parsed into pgxv5type.Time via ConvertToPgxType.
	val, err := convertValueForType("10:30:00.000000", "time without time zone")
	require.NoError(t, err)

	tv, ok := val.(pgxv5type.Time)
	require.True(t, ok, "expected pgxv5type.Time, got %T", val)
	require.True(t, tv.Valid)

	expectedUsec := int64(10*3_600_000_000 + 30*60_000_000)
	require.Equal(t, expectedUsec, tv.Microseconds)
}

func TestConvertValueForType_StringTimetz(t *testing.T) {
	// Values for "time with time zone" columns should remain as strings.
	val, err := convertValueForType("10:30:00-05:00", "time with time zone")
	require.NoError(t, err)

	s, ok := val.(string)
	require.True(t, ok, "expected string, got %T", val)
	require.Equal(t, "10:30:00-05:00", s)
}

// TestRowStatement checks the SQL text and the parameters of each kind of row
// operation, and the conversion of time values for a NULL fix.
func TestRowStatement(t *testing.T) {
	task := &TableRepairTask{}
	task.Schema = "public"
	task.Table = "test_table"
	task.Key = []string{"id"}
	task.Cols = []string{"id", "a", "ts_col", "time_col", "timetz_col"}
	colTypes := map[string]string{
		"id":         "integer",
		"a":          "text",
		"ts_col":     "timestamp without time zone",
		"time_col":   "time without time zone",
		"timetz_col": "time with time zone",
	}
	row := map[string]any{"id": float64(1), "a": "x", "ts_col": "2024-06-15T10:30:00Z",
		"time_col": "14:30:00.000000", "timetz_col": "14:30:00-05:00"}

	sql, args, err := task.rowStatement(opInsert, row, colTypes)
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "public"."test_table" ("id", "a", "ts_col", "time_col", "timetz_col") VALUES ($1, $2, $3, $4, $5)`, sql)
	require.Len(t, args, 5)

	sql, args, err = task.rowStatement(opUpdate, row, colTypes)
	require.NoError(t, err)
	require.Equal(t, `UPDATE "public"."test_table" SET "a" = $2, "ts_col" = $3, "time_col" = $4, "timetz_col" = $5 WHERE "id" = $1`, sql)
	require.Len(t, args, 5)

	sql, args, err = task.rowStatement(opDelete, row, colTypes)
	require.NoError(t, err)
	require.Equal(t, `DELETE FROM "public"."test_table" WHERE "id" = $1`, sql)
	require.Len(t, args, 1)

	// A NULL fix sets only the columns given in the row, and only where they
	// are still NULL.
	fix := map[string]any{"id": float64(1), "ts_col": "2024-06-15T10:30:00Z",
		"time_col": "14:30:00.000000", "timetz_col": "14:30:00-05:00"}
	sql, args, err = task.rowStatement(opFixNulls, fix, colTypes)
	require.NoError(t, err)
	require.Equal(t, `UPDATE "public"."test_table" SET "time_col" = $2, "timetz_col" = $3, "ts_col" = $4 `+
		`WHERE "id" = $1 AND "time_col" IS NULL AND "timetz_col" IS NULL AND "ts_col" IS NULL`, sql)
	require.Len(t, args, 4)

	tv, ok := args[1].(pgxv5type.Time)
	require.True(t, ok, "expected time arg to be pgxv5type.Time, got %T", args[1])
	require.Equal(t, int64(14*3_600_000_000+30*60_000_000), tv.Microseconds)

	s, ok := args[2].(string)
	require.True(t, ok, "expected timetz arg to be string, got %T", args[2])
	require.Equal(t, "14:30:00-05:00", s)

	pgTS, ok := args[3].(pgxv5type.Timestamp)
	require.True(t, ok, "expected timestamp arg to be pgxv5type.Timestamp, got %T", args[3])
	require.True(t, pgTS.Time.Equal(time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)))

	// A table with only key columns has nothing to update.
	task.Cols = []string{"id"}
	_, _, err = task.rowStatement(opUpdate, map[string]any{"id": float64(1)}, colTypes)
	require.Error(t, err)
}

func TestRepairSetsOneOperationPerKey(t *testing.T) {
	a := map[string]any{"id": 1, "v": "a"}
	b := map[string]any{"id": 1, "v": "b"}

	tests := []struct {
		name         string
		add          func(s *repairSets)
		wantInsert   bool
		wantUpdate   bool
		wantDelete   bool
		wantConflict string
	}{
		{
			name: "same update from two pairs",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.write("n2", "1", a, true)
				s.pair = "n2/n3"
				s.write("n2", "1", map[string]any{"id": 1, "v": "a"}, true)
			},
			wantUpdate: true,
		},
		{
			name: "deletes with other rows",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.remove("n2", "1", a)
				s.pair = "n2/n3"
				s.remove("n2", "1", b)
			},
			wantDelete: true,
		},
		{
			name: "update and insert",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.write("n2", "1", a, true)
				s.pair = "n2/n3"
				s.write("n2", "1", a, false)
			},
			wantConflict: "node pairs n1/n2 and n2/n3 give different row operations for this key: update and insert",
		},
		{
			name: "updates with other rows",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.write("n2", "1", a, true)
				s.pair = "n2/n3"
				s.write("n2", "1", b, true)
			},
			wantConflict: "node pairs n1/n2 and n2/n3 give different rows to update for this key",
		},
		{
			name: "delete and insert, then a third pair",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.remove("n2", "1", a)
				s.pair = "n2/n3"
				s.write("n2", "1", b, false)
				s.pair = "n2/n4"
				s.write("n2", "1", b, false)
			},
			wantConflict: "node pairs n1/n2 and n2/n3 give different row operations for this key: delete and insert",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newRepairSets()
			tc.add(s)
			_, ins := s.inserts["n2"]["1"]
			_, upd := s.updates["n2"]["1"]
			_, del := s.deletes["n2"]["1"]
			require.Equal(t, tc.wantInsert, ins, "insert")
			require.Equal(t, tc.wantUpdate, upd, "update")
			require.Equal(t, tc.wantDelete, del, "delete")
			c, ok := s.conflicts["n2"]["1"]
			require.Equal(t, tc.wantConflict != "", ok, "conflict")
			require.Equal(t, tc.wantConflict, c.reason)
			require.Equal(t, []string{"n2"}, s.nodes())
		})
	}

	task := &TableRepairTask{}
	task.Key = []string{"id"}
	s := newRepairSets()
	s.pair = "n1/n2"
	s.write("n2", "1", a, true)
	s.pair = "n2/n3"
	s.write("n2", "1", a, false)
	task.report = &RepairReport{Changes: map[string]any{}}
	task.recordConflicts(s)
	want := []FailedRowOp{{
		Node:      "n2",
		Operation: opConflict,
		Key:       map[string]any{"id": 1},
		Reason:    "node pairs n1/n2 and n2/n3 give different row operations for this key: update and insert",
	}}
	require.Equal(t, want, task.FailedOps)
	require.Equal(t, want, task.report.Changes["n2"].(map[string]any)["failed_rows"])
}

// TestPlanBidirectional checks that the bidirectional action of a plan
// inserts a row that one node has on the other node, and refuses a row that
// both nodes have.
func TestPlanBidirectional(t *testing.T) {
	a := map[string]any{"id": 1, "v": "a"}
	b := map[string]any{"id": 1, "v": "b"}
	action := &planner.RepairPlanAction{Type: planner.RepairActionBidirectional}

	mismatch := planDiffRow{pkStr: "1", node1Name: "n1", node2Name: "n2", n1Row: a, n2Row: b, diffType: "row_mismatch"}
	require.Error(t, validateActionCompatibility(action, mismatch))
	require.Error(t, ensureActionCompatible(mismatch, action))
	require.Error(t, applyPlanAction(mismatch, action, "r", newRepairSets(), nil, nil))

	tests := []struct {
		row    planDiffRow
		target string
		want   map[string]any
	}{
		{planDiffRow{pkStr: "1", node1Name: "n1", node2Name: "n2", n1Row: a, diffType: "missing_on_n2"}, "n2", a},
		{planDiffRow{pkStr: "1", node1Name: "n1", node2Name: "n2", n2Row: b, diffType: "missing_on_n1"}, "n1", b},
	}
	for _, tc := range tests {
		t.Run(tc.row.diffType, func(t *testing.T) {
			require.NoError(t, validateActionCompatibility(action, tc.row))
			require.NoError(t, ensureActionCompatible(tc.row, action))
			s := newRepairSets()
			require.NoError(t, applyPlanAction(tc.row, action, "r", s, nil, nil))
			require.Equal(t, map[string]map[string]map[string]any{tc.target: {"1": tc.want}}, s.inserts)
			require.Empty(t, s.updates)
			require.Empty(t, s.deletes)
		})
	}
}

// TestPlanBidirectionalOnMismatchRefused checks that a plan with a
// bidirectional rule for row_mismatch is refused when it is read.
func TestPlanBidirectionalOnMismatchRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`version: 1
tables:
  public.t:
    rules:
      - name: swap
        diff_type: [row_mismatch]
        action: { type: bidirectional }
`), 0o600))
	_, err := planner.LoadRepairPlanFile(path)
	require.ErrorContains(t, err, "bidirectional incompatible with diff_type row_mismatch")
}
