package common

import (
	"encoding/base64"
	"testing"
	"time"

	pgxv5type "github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
)

func TestConvertToPgxType_ArrayStrings(t *testing.T) {
	val, err := ConvertToPgxType("{a,b}", "text[]")
	require.NoError(t, err)
	require.Equal(t, "{a,b}", val)
}

func TestConvertToPgxType_ArrayFromSlice(t *testing.T) {
	val, err := ConvertToPgxType([]int{1, 2, 3}, "integer[]")
	require.NoError(t, err)
	require.Equal(t, "{1,2,3}", val)
}

func TestConvertToPgxType_ArrayWithStringsNeedsQuoting(t *testing.T) {
	val, err := ConvertToPgxType([]string{"hello", "spaced value", "null"}, "text[]")
	require.NoError(t, err)
	require.Equal(t, `{"hello","spaced value","null"}`, val)
}

func TestConvertToPgxType_ArrayNilElement(t *testing.T) {
	val, err := ConvertToPgxType([]any{"a", nil, "b"}, "text[]")
	require.NoError(t, err)
	require.Equal(t, `{"a",NULL,"b"}`, val)
}

func TestConvertToPgxType_ByteaBase64(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("hello-bytea"))
	val, err := ConvertToPgxType(encoded, "bytea")
	require.NoError(t, err)
	require.Equal(t, []byte("hello-bytea"), val)
}

func TestConvertToPgxType_IntervalString(t *testing.T) {
	val, err := ConvertToPgxType("1 day 02:03:04", "interval")
	require.NoError(t, err)
	require.Equal(t, "1 day 02:03:04", val)
}

type customStringer struct {
	v string
}

func (c customStringer) String() string { return "stringer:" + c.v }

func TestConvertToPgxType_FallbackStringer(t *testing.T) {
	val, err := ConvertToPgxType(customStringer{v: "x"}, "unknown_type")
	require.NoError(t, err)
	require.Equal(t, "stringer:x", val)
}

func TestNormalizeNumericString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"3000.00", "3000"},
		{"3000.10", "3000.1"},
		{"3000", "3000"},
		{"0.0", "0"},
		{"0.00", "0"},
		{"1.23456", "1.23456"},
		{"100.0100", "100.01"},
		{"-5.50", "-5.5"},
		{"-5.00", "-5"},
		{"0", "0"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := NormalizeNumericString(tt.input)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestConvertToPgxType_TimestampReturnsPgxTimestamp(t *testing.T) {
	// For "timestamp without time zone" columns, ConvertToPgxType must return
	// pgxv5type.Timestamp so that pgx sends the value as "timestamp" instead of
	// "timestamptz". This avoids a session-timezone conversion that
	// would shift the value on non-UTC servers.
	input := "2024-06-15T10:30:00Z"
	expected := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	for _, pgType := range []string{
		"timestamp",
		"timestamp without time zone",
	} {
		t.Run(pgType, func(t *testing.T) {
			val, err := ConvertToPgxType(input, pgType)
			require.NoError(t, err)

			ts, ok := val.(pgxv5type.Timestamp)
			require.True(t, ok, "expected pgxv5type.Timestamp for type %q, got %T", pgType, val)
			require.True(t, ts.Valid)
			require.True(t, ts.Time.Equal(expected), "expected %v, got %v", expected, ts.Time)
		})
	}
}

func TestConvertToPgxType_TimestamptzReturnsTime(t *testing.T) {
	// For "timestamp with time zone" columns, ConvertToPgxType must return
	// a plain time.Time so that pgx sends the value as "timestamptz".
	input := "2024-06-15T10:30:00Z"
	expected := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)

	for _, pgType := range []string{
		"timestamptz",
		"timestamp with time zone",
	} {
		t.Run(pgType, func(t *testing.T) {
			val, err := ConvertToPgxType(input, pgType)
			require.NoError(t, err)

			tv, ok := val.(time.Time)
			require.True(t, ok, "expected time.Time for type %q, got %T", pgType, val)
			require.True(t, tv.Equal(expected), "expected %v, got %v", expected, tv)
		})
	}
}

func TestConvertToPgxType_TimestampWithPrecision(t *testing.T) {
	// format_type() returns "timestamp(3) without time zone" for TIMESTAMP(3)
	// columns. The normalizedType strips the precision, yielding "timestamp",
	// which must still return pgxv5type.Timestamp.
	input := "2024-06-15T10:30:00.123Z"

	val, err := ConvertToPgxType(input, "timestamp(3) without time zone")
	require.NoError(t, err)

	ts, ok := val.(pgxv5type.Timestamp)
	require.True(t, ok, "expected pgxv5type.Timestamp, got %T", val)
	require.True(t, ts.Valid)
}

func TestConvertToPgxType_TimestampFormats(t *testing.T) {
	// Various input formats that should all parse successfully for timestamp.
	tests := []struct {
		name  string
		input string
	}{
		{"RFC3339", "2024-06-15T10:30:00Z"},
		{"RFC3339Nano", "2024-06-15T10:30:00.123456789Z"},
		{"RFC3339 with offset", "2024-06-15T10:30:00-07:00"},
		{"space separated with offset", "2024-06-15 10:30:00.123456-07"},
		{"space separated no tz", "2024-06-15 10:30:00.123456"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ConvertToPgxType(tt.input, "timestamp without time zone")
			require.NoError(t, err)

			ts, ok := val.(pgxv5type.Timestamp)
			require.True(t, ok, "expected pgxv5type.Timestamp for input %q, got %T", tt.input, val)
			require.True(t, ts.Valid)
		})
	}
}

func TestConvertToPgxType_TimeReturnsPgxTime(t *testing.T) {
	// For "time without time zone" columns, ConvertToPgxType must return
	// pgxv5type.Time so that pgx sends the value as "time" correctly.
	input := "10:30:00.000000"
	expectedUsec := int64(10*3_600_000_000 + 30*60_000_000)

	for _, pgType := range []string{
		"time",
		"time without time zone",
	} {
		t.Run(pgType, func(t *testing.T) {
			val, err := ConvertToPgxType(input, pgType)
			require.NoError(t, err)

			tv, ok := val.(pgxv5type.Time)
			require.True(t, ok, "expected pgxv5type.Time for type %q, got %T", pgType, val)
			require.True(t, tv.Valid)
			require.Equal(t, expectedUsec, tv.Microseconds)
		})
	}
}

func TestConvertToPgxType_TimeFormats(t *testing.T) {
	// Various input formats that should parse successfully for time.
	tests := []struct {
		name     string
		input    string
		wantUsec int64
	}{
		{"HH:MM:SS", "10:30:00", 10*3_600_000_000 + 30*60_000_000},
		{"HH:MM:SS.ffffff", "10:30:00.123456", 10*3_600_000_000 + 30*60_000_000 + 123456},
		{"midnight", "00:00:00", 0},
		{"end of day", "23:59:59.999999", 23*3_600_000_000 + 59*60_000_000 + 59*1_000_000 + 999999},
		{"with fractional seconds", "14:05:30.500000", 14*3_600_000_000 + 5*60_000_000 + 30*1_000_000 + 500000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ConvertToPgxType(tt.input, "time without time zone")
			require.NoError(t, err)

			tv, ok := val.(pgxv5type.Time)
			require.True(t, ok, "expected pgxv5type.Time for input %q, got %T", tt.input, val)
			require.True(t, tv.Valid)
			require.Equal(t, tt.wantUsec, tv.Microseconds, "microseconds mismatch for input %q", tt.input)
		})
	}
}

func TestConvertToPgxType_TimeOutOfRange(t *testing.T) {
	// Out-of-range values should be rejected.
	badInputs := []struct {
		name  string
		input string
	}{
		{"hours 24", "24:00:00"},
		{"hours 99", "99:00:00"},
		{"minutes 60", "10:60:00"},
		{"seconds 60", "10:30:60"},
	}

	for _, tt := range badInputs {
		t.Run(tt.name, func(t *testing.T) {
			// Should fall back to string passthrough (not crash or produce wrong microseconds)
			val, err := ConvertToPgxType(tt.input, "time without time zone")
			require.NoError(t, err)
			_, ok := val.(string)
			require.True(t, ok, "expected string fallback for invalid input %q, got %T", tt.input, val)
		})
	}
}

func TestConvertToPgxType_TimeFractionalTruncation(t *testing.T) {
	// Fractional parts longer than 6 digits should be truncated to microseconds.
	val, err := ConvertToPgxType("10:30:00.123456789", "time without time zone")
	require.NoError(t, err)

	tv, ok := val.(pgxv5type.Time)
	require.True(t, ok, "expected pgxv5type.Time, got %T", val)
	require.True(t, tv.Valid)

	expectedUsec := int64(10*3_600_000_000 + 30*60_000_000 + 123456)
	require.Equal(t, expectedUsec, tv.Microseconds, "should truncate to 6 fractional digits")
}

func TestConvertToPgxType_TimeWithPrecision(t *testing.T) {
	// format_type() returns "time(3) without time zone" for TIME(3) columns.
	// The normalizedType strips the precision, yielding "time", which must
	// still return pgxv5type.Time.
	input := "10:30:00.123000"

	val, err := ConvertToPgxType(input, "time(3) without time zone")
	require.NoError(t, err)

	tv, ok := val.(pgxv5type.Time)
	require.True(t, ok, "expected pgxv5type.Time, got %T", val)
	require.True(t, tv.Valid)
}

func TestConvertToPgxType_TimetzReturnsString(t *testing.T) {
	// For "time with time zone" columns, ConvertToPgxType must return the
	// string as-is since timetz is not registered in the pgx default type map.
	input := "10:30:00-05:00"

	for _, pgType := range []string{
		"timetz",
		"time with time zone",
	} {
		t.Run(pgType, func(t *testing.T) {
			val, err := ConvertToPgxType(input, pgType)
			require.NoError(t, err)

			s, ok := val.(string)
			require.True(t, ok, "expected string for type %q, got %T", pgType, val)
			require.Equal(t, input, s)
		})
	}
}
