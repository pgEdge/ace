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
	"strings"
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

func TestBuildFixNullsBatchSQL_TimestampType(t *testing.T) {
	// Verify that buildFixNullsBatchSQL generates valid SQL for timestamp
	// columns and that values are properly converted.
	task := &TableRepairTask{}
	task.Schema = "public"
	task.Table = "test_table"
	task.Key = []string{"id"}

	colTypes := map[string]string{
		"id":     "integer",
		"ts_col": "timestamp without time zone",
	}

	batch := []*nullUpdate{
		{
			pkValues: []any{float64(1)},
			columns:  map[string]any{"ts_col": "2024-06-15T10:30:00Z"},
		},
	}

	sql, args, err := task.buildFixNullsBatchSQL("ts_col", "timestamp without time zone", batch, colTypes)
	require.NoError(t, err)

	// The SQL should contain the correct type cast
	require.True(t, strings.Contains(sql, "::timestamp without time zone"),
		"expected SQL to contain '::timestamp without time zone', got: %s", sql)

	// The args should contain the converted values
	require.Len(t, args, 2) // 1 PK + 1 value

	// The timestamp value should be pgxv5type.Timestamp, not time.Time
	pgTS, ok := args[1].(pgxv5type.Timestamp)
	require.True(t, ok, "expected timestamp arg to be pgxv5type.Timestamp, got %T", args[1])
	require.True(t, pgTS.Valid)

	expected := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	require.True(t, pgTS.Time.Equal(expected))
}

func TestBuildFixNullsBatchSQL_TimestamptzType(t *testing.T) {
	// Verify that timestamptz columns use time.Time (not pgxv5type.Timestamp).
	task := &TableRepairTask{}
	task.Schema = "public"
	task.Table = "test_table"
	task.Key = []string{"id"}

	colTypes := map[string]string{
		"id":      "integer",
		"tstz_col": "timestamp with time zone",
	}

	batch := []*nullUpdate{
		{
			pkValues: []any{float64(1)},
			columns:  map[string]any{"tstz_col": "2024-06-15T10:30:00Z"},
		},
	}

	sql, args, err := task.buildFixNullsBatchSQL("tstz_col", "timestamp with time zone", batch, colTypes)
	require.NoError(t, err)

	require.True(t, strings.Contains(sql, "::timestamp with time zone"),
		"expected SQL to contain '::timestamp with time zone', got: %s", sql)

	require.Len(t, args, 2)

	// The timestamptz value should be a plain time.Time
	tv, ok := args[1].(time.Time)
	require.True(t, ok, "expected timestamptz arg to be time.Time, got %T", args[1])

	expected := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	require.True(t, tv.Equal(expected))
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

func TestBuildFixNullsBatchSQL_TimeType(t *testing.T) {
	// Verify that buildFixNullsBatchSQL generates valid SQL for time columns
	// and that values are properly converted to pgxv5type.Time.
	task := &TableRepairTask{}
	task.Schema = "public"
	task.Table = "test_table"
	task.Key = []string{"id"}

	colTypes := map[string]string{
		"id":       "integer",
		"time_col": "time without time zone",
	}

	batch := []*nullUpdate{
		{
			pkValues: []any{float64(1)},
			columns:  map[string]any{"time_col": "14:30:00.000000"},
		},
	}

	sql, args, err := task.buildFixNullsBatchSQL("time_col", "time without time zone", batch, colTypes)
	require.NoError(t, err)

	require.True(t, strings.Contains(sql, "::time without time zone"),
		"expected SQL to contain '::time without time zone', got: %s", sql)

	require.Len(t, args, 2) // 1 PK + 1 value

	tv, ok := args[1].(pgxv5type.Time)
	require.True(t, ok, "expected time arg to be pgxv5type.Time, got %T", args[1])
	require.True(t, tv.Valid)

	expectedUsec := int64(14*3_600_000_000 + 30*60_000_000)
	require.Equal(t, expectedUsec, tv.Microseconds)
}

func TestBuildFixNullsBatchSQL_TimetzType(t *testing.T) {
	// Verify that timetz columns pass through as strings.
	task := &TableRepairTask{}
	task.Schema = "public"
	task.Table = "test_table"
	task.Key = []string{"id"}

	colTypes := map[string]string{
		"id":         "integer",
		"timetz_col": "time with time zone",
	}

	batch := []*nullUpdate{
		{
			pkValues: []any{float64(1)},
			columns:  map[string]any{"timetz_col": "14:30:00-05:00"},
		},
	}

	sql, args, err := task.buildFixNullsBatchSQL("timetz_col", "time with time zone", batch, colTypes)
	require.NoError(t, err)

	require.True(t, strings.Contains(sql, "::time with time zone"),
		"expected SQL to contain '::time with time zone', got: %s", sql)

	require.Len(t, args, 2)

	s, ok := args[1].(string)
	require.True(t, ok, "expected timetz arg to be string, got %T", args[1])
	require.Equal(t, "14:30:00-05:00", s)
}
