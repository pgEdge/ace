package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOrderedMap_JSONRoundtrip_BigintPrecision(t *testing.T) {
	// OrderedMap must preserve full precision for large bigint values through
	// JSON marshal/unmarshal. Values > 2^53 lose precision when stored as
	// float64, so UnmarshalJSON must use json.Number.
	original := OrderedMap{
		{Key: "id", Value: json.Number("415588913294348289")},
		{Key: "name", Value: "test"},
	}

	data, err := json.Marshal(original)
	require.NoError(t, err)

	var roundtripped OrderedMap
	err = json.Unmarshal(data, &roundtripped)
	require.NoError(t, err)

	// The id value must be a json.Number with the exact original string
	idVal, ok := roundtripped.Get("id")
	require.True(t, ok)

	n, ok := idVal.(json.Number)
	require.True(t, ok, "expected json.Number after roundtrip, got %T", idVal)
	require.Equal(t, "415588913294348289", n.String())

	// Verify it converts to the correct int64
	i64, err := n.Int64()
	require.NoError(t, err)
	require.Equal(t, int64(415588913294348289), i64)
}

func TestOrderedMap_JSONRoundtrip_MultipleNumericTypes(t *testing.T) {
	// JSON containing various numeric types should all survive roundtrip
	jsonStr := `{"bigint_pk":415588913294348289,"small_int":42,"decimal":123.456,"negative":-9876543210}`

	var om OrderedMap
	err := json.Unmarshal([]byte(jsonStr), &om)
	require.NoError(t, err)

	tests := []struct {
		key      string
		expected string
	}{
		{"bigint_pk", "415588913294348289"},
		{"small_int", "42"},
		{"decimal", "123.456"},
		{"negative", "-9876543210"},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			val, ok := om.Get(tt.key)
			require.True(t, ok)

			n, ok := val.(json.Number)
			require.True(t, ok, "expected json.Number for %s, got %T", tt.key, val)
			require.Equal(t, tt.expected, n.String())
		})
	}
}
