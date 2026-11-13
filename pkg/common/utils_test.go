package common

import (
	"encoding/json"
	"testing"

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStringifyOrderedMapKey_JsonNumber(t *testing.T) {
	// json.Number values from UseNumber() must stringify to the exact original
	// string, not scientific notation.
	row := types.OrderedMap{
		{Key: "id", Value: json.Number("415588913294348289")},
		{Key: "name", Value: "test"},
	}

	key, err := StringifyOrderedMapKey(row, []string{"id"})
	require.NoError(t, err)
	require.Equal(t, RowKeyFromStrings([]string{"415588913294348289"}), key,
		"PK must stringify to exact decimal, not scientific notation")
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

// Row keys are matched across nodes, so an ambiguous encoding loses rows: with
// the parts joined raw, ("a|b","c") and ("a","b|c") produce one key and one of
// the two rows disappears from the comparison.
func TestRowKeyFromStringsIsUnambiguous(t *testing.T) {
	require.NotEqual(t,
		RowKeyFromStrings([]string{"a|b", "c"}),
		RowKeyFromStrings([]string{"a", "b|c"}),
		"composite keys that differ must not encode to the same row key")

	require.Equal(t,
		RowKeyFromStrings([]string{"a", "b"}),
		RowKeyFromStrings([]string{"a", "b"}),
		"the same parts must always encode to the same row key")
}

// StringifyKey and StringifyOrderedMapKey key the same maps in the diff and
// repair paths -- a row inserted under one has to be found under the other --
// and both must inherit the unambiguous encoding, or a pkey value containing
// the delimiter merges two distinct rows into one.
func TestStringifyKeyEncodingIsSharedAndUnambiguous(t *testing.T) {
	pkeyCols := []string{"a", "b"}

	split1 := map[string]any{"a": "x|y", "b": "z"}
	split2 := map[string]any{"a": "x", "b": "y|z"}

	key1, err := StringifyKey(split1, pkeyCols)
	require.NoError(t, err)
	key2, err := StringifyKey(split2, pkeyCols)
	require.NoError(t, err)
	require.NotEqual(t, key1, key2, "rows whose pkey values differ must not share a key")

	ordered := types.OrderedMap{{Key: "a", Value: "x|y"}, {Key: "b", Value: "z"}}
	orderedKey, err := StringifyOrderedMapKey(ordered, pkeyCols)
	require.NoError(t, err)
	require.Equal(t, key1, orderedKey, "the two stringifiers must agree on the same row")
}

// These keys reach operators: repair interpolates them into "row missing on %s
// (pk %s)". So a part is quoted only when it has to be, and an ordinary key
// still reads the way the pkey does.
func TestRowKeyFromStringsQuotesOnlyWhenNeeded(t *testing.T) {
	require.Equal(t, "123", RowKeyFromStrings([]string{"123"}),
		"a plain value must not gain quotes")
	require.Equal(t, "123|abc", RowKeyFromStrings([]string{"123", "abc"}),
		"a plain composite must read as it always did")

	// Quoting kicks in exactly where the encoding would otherwise be ambiguous.
	require.Equal(t, `"a|b"|c`, RowKeyFromStrings([]string{"a|b", "c"}))
	require.Equal(t, `a|"b|c"`, RowKeyFromStrings([]string{"a", "b|c"}))
	require.Equal(t, `""`, RowKeyFromStrings([]string{""}),
		"an empty part must still occupy its position")
	require.Equal(t, `"say \"hi\""`, RowKeyFromStrings([]string{`say "hi"`}),
		"a value holding a quote must be quoted, or it could pass for an encoded part")
}

// The encoding has to be injective, because a collision merges two rows into
// one map entry and drops a difference. Values that mix the delimiter, quotes
// and empties are where a hand-rolled rule goes wrong.
func TestRowKeyFromStringsIsInjective(t *testing.T) {
	parts := []string{"", "a", "b", "|", `"`, "a|b", "b|c", `"a`, `a"`, `"a|b"`, `\`}

	seen := make(map[string][]string)
	for _, p1 := range parts {
		for _, p2 := range parts {
			key := RowKeyFromStrings([]string{p1, p2})
			if prev, clash := seen[key]; clash {
				t.Errorf("%q and %q both encode to %s", prev, []string{p1, p2}, key)
				continue
			}
			seen[key] = []string{p1, p2}
		}
		// A one-part list must not collide with any two-part list either.
		key := RowKeyFromStrings([]string{p1})
		if prev, clash := seen[key]; clash {
			t.Errorf("%q and %q both encode to %s", prev, []string{p1}, key)
		} else {
			seen[key] = []string{p1}
		}
	}
}
