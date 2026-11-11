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
	"testing"
	"time"

	pgxv5type "github.com/jackc/pgx/v5/pgtype"
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
