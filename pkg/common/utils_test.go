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
