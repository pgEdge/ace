package common

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgtype"
	pgxv5type "github.com/jackc/pgx/v5/pgtype"
	"github.com/pgedge/ace/pkg/types"
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

// A time value round-trips through the diff report as HH:MM:SS, so repair can
// convert it back into a typed parameter.
func TestNormalizeScannedValue_Time(t *testing.T) {
	// 16:11:49 = 58309 s since midnight
	val := NormalizeScannedValue(pgxv5type.Time{Microseconds: 58_309_000_000, Valid: true})
	require.Equal(t, "16:11:49.000000", val)
	require.Nil(t, NormalizeScannedValue(pgxv5type.Time{Valid: false}))

	converted, err := ConvertToPgxType(val, "time without time zone")
	require.NoError(t, err)
	require.Equal(t, pgxv5type.Time{Microseconds: 58_309_000_000, Valid: true}, converted)
}

// A pgx v5 UUID ([16]byte) becomes the canonical string form, not a JSON array.
func TestNormalizeScannedValue_UUID(t *testing.T) {
	raw := [16]byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	require.Equal(t, "12345678-9abc-def0-1234-56789abcdef0", NormalizeScannedValue(raw))
}

// Plain values and nil pass through unchanged.
func TestNormalizeScannedValue_Passthrough(t *testing.T) {
	require.Nil(t, NormalizeScannedValue(nil))
	require.Equal(t, "abc", NormalizeScannedValue("abc"))
	require.Equal(t, int64(42), NormalizeScannedValue(int64(42)))
	ts := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	require.Equal(t, ts, NormalizeScannedValue(ts))
}

// A pgx v5 interval struct becomes its Postgres text form.
func TestNormalizeScannedValue_Interval(t *testing.T) {
	val := NormalizeScannedValue(pgxv5type.Interval{Days: 1, Microseconds: 7_384_000_000, Valid: true})
	s, ok := val.(string)
	require.True(t, ok, "interval should normalise to a string, got %T", val)
	require.Contains(t, s, "1 day")
	require.Nil(t, NormalizeScannedValue(pgxv5type.Interval{Valid: false}))
}

// Diff files written before values were normalised carry the raw pgx struct
// form for time; repair still accepts it.
func TestConvertToPgxType_TimeLegacyMapForm(t *testing.T) {
	val, err := ConvertToPgxType(map[string]any{"Microseconds": float64(9_316_000_000), "Valid": true}, "time without time zone")
	require.NoError(t, err)
	require.Equal(t, pgxv5type.Time{Microseconds: 9_316_000_000, Valid: true}, val)

	val, err = ConvertToPgxType(map[string]any{"Microseconds": json.Number("9316000000"), "Valid": true}, "time")
	require.NoError(t, err)
	require.Equal(t, pgxv5type.Time{Microseconds: 9_316_000_000, Valid: true}, val)

	val, err = ConvertToPgxType(map[string]any{"Microseconds": float64(0), "Valid": false}, "time")
	require.NoError(t, err)
	require.Nil(t, val)
}

// Money strings pass through as-is for Postgres to parse, without warnings.
func TestConvertToPgxType_Money(t *testing.T) {
	val, err := ConvertToPgxType("$532.96", "money")
	require.NoError(t, err)
	require.Equal(t, "$532.96", val)
}

// Complex and unknown types are fetched as ::TEXT so their Postgres text form
// round-trips through the diff report; known scalars stay native.
func TestSelectColExpr(t *testing.T) {
	tests := []struct {
		colType string
		want    string
	}{
		{"integer", `"c"`},
		{"timestamp without time zone", `"c"`},
		{"uuid", `"c"`},
		{"integer[]", `"c"::TEXT AS "c"`},
		{"jsonb", `"c"::TEXT AS "c"`},
		{"bytea", `"c"::TEXT AS "c"`},
		{"point", `"c"::TEXT AS "c"`},
		{"int4range", `"c"::TEXT AS "c"`},
		{"mood_enum", `"c"::TEXT AS "c"`},
		{"xml", `"c"::TEXT AS "c"`},
		{"bit(8)", `"c"::TEXT AS "c"`},
		{"inet", `"c"::TEXT AS "c"`},
		{"time with time zone", `"c"::TEXT AS "c"`},
	}
	for _, tt := range tests {
		t.Run(tt.colType, func(t *testing.T) {
			require.Equal(t, tt.want, SelectColExpr(`"c"`, tt.colType))
		})
	}
}

// Concurrent normalisation is race-free (a pgtype.Map must not be shared
// across goroutines; run with -race).
func TestNormalizeScannedValue_ConcurrentInterval(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = NormalizeScannedValue(pgxv5type.Interval{Days: 2, Microseconds: 3_600_000_000, Valid: true})
			}
		}()
	}
	wg.Wait()
}

// Infinite timestamps and dates survive as their Postgres text form instead of
// collapsing to a zero time.
func TestNormalizeScannedValue_Infinity(t *testing.T) {
	require.Equal(t, "infinity", NormalizeScannedValue(pgtype.Timestamp{Status: pgtype.Present, InfinityModifier: pgtype.Infinity}))
	require.Equal(t, "-infinity", NormalizeScannedValue(pgtype.Timestamptz{Status: pgtype.Present, InfinityModifier: pgtype.NegativeInfinity}))
	require.Equal(t, "-infinity", NormalizeScannedValue(pgtype.Date{Status: pgtype.Present, InfinityModifier: pgtype.NegativeInfinity}))
}

// Raw bytea payloads pass through untouched (JSON base64) instead of being
// stringified into an unrepairable "[104 101 ...]" form.
func TestNormalizeScannedValue_ByteaPassthrough(t *testing.T) {
	b := []byte{0x00, 0xff, 0x10, 0x7f}
	val := NormalizeScannedValue(b)
	require.Equal(t, b, val)

	// The full report round-trip: JSON-encode, decode, convert for repair.
	encoded, err := json.Marshal(val)
	require.NoError(t, err)
	var decoded string
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	converted, err := ConvertToPgxType(decoded, "bytea")
	require.NoError(t, err)
	require.Equal(t, b, converted)
}

// pgtype v1 JSON values normalise to their raw text without panicking.
func TestNormalizeScannedValue_JSONv1(t *testing.T) {
	require.Equal(t, `{"a":1}`, NormalizeScannedValue(pgtype.JSON{Bytes: []byte(`{"a":1}`), Status: pgtype.Present}))
	require.Nil(t, NormalizeScannedValue(pgtype.JSONB{Status: pgtype.Null}))
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

func TestConvertToPgxType_JsonNumberBigint(t *testing.T) {
	// json.Number must preserve full precision for bigint values that exceed
	// float64's 2^53 exact integer range. This is the root cause of the repair
	// corruption bug for tables with large bigint primary keys.
	tests := []struct {
		name     string
		input    json.Number
		pgType   string
		expected int64
	}{
		{"customer PK", json.Number("415588913294348289"), "bigint", 415588913294348289},
		{"near max int64", json.Number("9223372036854775806"), "int8", 9223372036854775806},
		{"small value", json.Number("42"), "integer", 42},
		{"negative", json.Number("-1234567890123456789"), "bigint", -1234567890123456789},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := ConvertToPgxType(tt.input, tt.pgType)
			require.NoError(t, err)
			i64, ok := val.(int64)
			require.True(t, ok, "expected int64, got %T", val)
			require.Equal(t, tt.expected, i64)
		})
	}
}

func TestConvertToPgxType_JsonNumberNumeric(t *testing.T) {
	// json.Number for numeric/decimal should preserve full precision via
	// pgtype.Numeric (not lossy float64 conversion).
	n := json.Number("12345678901234567.89012345")
	val, err := ConvertToPgxType(n, "numeric")
	require.NoError(t, err)
	require.NotNil(t, val)

	// Must NOT be float64 — that would lose precision
	_, isFloat := val.(float64)
	require.False(t, isFloat, "numeric json.Number must not convert to float64")

	// Verify it's a *pgtype.Numeric by checking the type name
	typeName := fmt.Sprintf("%T", val)
	require.Contains(t, typeName, "Numeric", "expected pgtype.Numeric, got %s", typeName)
}

func TestConvertToPgxType_JsonNumberFloat(t *testing.T) {
	n := json.Number("3.14")
	val, err := ConvertToPgxType(n, "double precision")
	require.NoError(t, err)

	f, ok := val.(float64)
	require.True(t, ok, "expected float64, got %T", val)
	require.InDelta(t, 3.14, f, 1e-10)
}

func TestStringifyOrderedMapKey_JsonNumber(t *testing.T) {
	// json.Number values from UseNumber() must stringify to the exact original
	// string, not scientific notation.
	row := types.OrderedMap{
		{Key: "id", Value: json.Number("415588913294348289")},
		{Key: "name", Value: "test"},
	}

	key, err := StringifyOrderedMapKey(row, []string{"id"})
	require.NoError(t, err)
	require.Equal(t, "415588913294348289", key, "PK must stringify to exact decimal, not scientific notation")
}

func TestStringifyOrderedMapKey_JsonNumberNoPKCollision(t *testing.T) {
	// Two adjacent large bigint PKs must NOT collide after stringification.
	row1 := types.OrderedMap{{Key: "id", Value: json.Number("415588913294348289")}}
	row2 := types.OrderedMap{{Key: "id", Value: json.Number("415588913294348290")}}

	key1, err := StringifyOrderedMapKey(row1, []string{"id"})
	require.NoError(t, err)
	key2, err := StringifyOrderedMapKey(row2, []string{"id"})
	require.NoError(t, err)
	require.NotEqual(t, key1, key2, "adjacent bigint PKs must not collide")
}
