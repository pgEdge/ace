package common

import (
	"encoding/base64"
	"testing"

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
