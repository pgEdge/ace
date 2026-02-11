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

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOriginBatchKey tests the originBatchKey struct and makeOriginBatchKey function
func TestOriginBatchKey(t *testing.T) {
	tests := []struct {
		name     string
		info     *rowOriginInfo
		expected originBatchKey
	}{
		{
			name: "complete origin info",
			info: &rowOriginInfo{
				nodeOrigin: "node1",
				lsn:        ptrUint64(12345678),
				commitTS:   ptrTime(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)),
			},
			expected: originBatchKey{
				nodeOrigin: "node1",
				lsn:        12345678,
				timestamp:  "2024-01-01T12:00:00Z",
			},
		},
		{
			name: "origin info without LSN",
			info: &rowOriginInfo{
				nodeOrigin: "node2",
				lsn:        nil,
				commitTS:   ptrTime(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)),
			},
			expected: originBatchKey{
				nodeOrigin: "node2",
				lsn:        0,
				timestamp:  "2024-01-01T12:00:00Z",
			},
		},
		{
			name: "origin info without timestamp",
			info: &rowOriginInfo{
				nodeOrigin: "node3",
				lsn:        ptrUint64(87654321),
				commitTS:   nil,
			},
			expected: originBatchKey{
				nodeOrigin: "node3",
				lsn:        87654321,
				timestamp:  "",
			},
		},
		{
			name: "nil origin info",
			info: nil,
			expected: originBatchKey{
				nodeOrigin: "",
				lsn:        0,
				timestamp:  "",
			},
		},
		{
			name: "empty origin node",
			info: &rowOriginInfo{
				nodeOrigin: "",
				lsn:        ptrUint64(12345),
				commitTS:   ptrTime(time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)),
			},
			expected: originBatchKey{
				nodeOrigin: "",
				lsn:        0,
				timestamp:  "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := makeOriginBatchKey(tt.info)
			assert.Equal(t, tt.expected.nodeOrigin, result.nodeOrigin, "nodeOrigin mismatch")
			assert.Equal(t, tt.expected.lsn, result.lsn, "lsn mismatch")
			assert.Equal(t, tt.expected.timestamp, result.timestamp, "timestamp mismatch")
		})
	}
}

// TestOriginBatchKeyGrouping tests that rows with identical LSN+timestamp are batched together
func TestOriginBatchKeyGrouping(t *testing.T) {
	// Create multiple rows with the same origin info
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)

	originInfo1a := &rowOriginInfo{
		nodeOrigin: "node1",
		lsn:        ptrUint64(12345678),
		commitTS:   &ts1,
	}

	originInfo1b := &rowOriginInfo{
		nodeOrigin: "node1",
		lsn:        ptrUint64(12345678),
		commitTS:   &ts1,
	}

	originInfo2 := &rowOriginInfo{
		nodeOrigin: "node1",
		lsn:        ptrUint64(12345678),
		commitTS:   &ts2, // Different timestamp
	}

	originInfo3 := &rowOriginInfo{
		nodeOrigin: "node1",
		lsn:        ptrUint64(87654321), // Different LSN
		commitTS:   &ts1,
	}

	originInfo4 := &rowOriginInfo{
		nodeOrigin: "node2", // Different node
		lsn:        ptrUint64(12345678),
		commitTS:   &ts1,
	}

	key1a := makeOriginBatchKey(originInfo1a)
	key1b := makeOriginBatchKey(originInfo1b)
	key2 := makeOriginBatchKey(originInfo2)
	key3 := makeOriginBatchKey(originInfo3)
	key4 := makeOriginBatchKey(originInfo4)

	// Test that identical origin info produces the same key
	assert.Equal(t, key1a, key1b, "Identical origin info should produce identical keys")

	// Test that different timestamps produce different keys
	assert.NotEqual(t, key1a, key2, "Different timestamps should produce different keys")

	// Test that different LSNs produce different keys
	assert.NotEqual(t, key1a, key3, "Different LSNs should produce different keys")

	// Test that different nodes produce different keys
	assert.NotEqual(t, key1a, key4, "Different nodes should produce different keys")

	// Test that keys can be used as map keys (grouping behavior)
	groups := make(map[originBatchKey][]string)
	groups[key1a] = append(groups[key1a], "row1a")
	groups[key1b] = append(groups[key1b], "row1b") // Should go to same group as row1a
	groups[key2] = append(groups[key2], "row2")
	groups[key3] = append(groups[key3], "row3")
	groups[key4] = append(groups[key4], "row4")

	// Should have 4 groups (1a/1b together, 2, 3, 4)
	assert.Equal(t, 4, len(groups), "Should have 4 distinct groups")
	assert.Equal(t, []string{"row1a", "row1b"}, groups[key1a], "row1a and row1b should be in same group")
	assert.Equal(t, []string{"row2"}, groups[key2], "row2 should be in its own group")
	assert.Equal(t, []string{"row3"}, groups[key3], "row3 should be in its own group")
	assert.Equal(t, []string{"row4"}, groups[key4], "row4 should be in its own group")
}

// TestOriginBatchKeyTimestampPrecision tests that timestamp precision is preserved
func TestOriginBatchKeyTimestampPrecision(t *testing.T) {
	// Create timestamps with nanosecond precision
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 123456789, time.UTC)
	ts2 := time.Date(2024, 1, 1, 12, 0, 0, 123456788, time.UTC) // 1 nanosecond difference

	originInfo1 := &rowOriginInfo{
		nodeOrigin: "node1",
		lsn:        ptrUint64(12345678),
		commitTS:   &ts1,
	}

	originInfo2 := &rowOriginInfo{
		nodeOrigin: "node1",
		lsn:        ptrUint64(12345678),
		commitTS:   &ts2,
	}

	key1 := makeOriginBatchKey(originInfo1)
	key2 := makeOriginBatchKey(originInfo2)

	// Keys should be different due to nanosecond precision difference
	assert.NotEqual(t, key1, key2, "Keys with 1ns timestamp difference should be different")

	// Parse timestamps back and verify precision
	parsedTS1, err := time.Parse(time.RFC3339Nano, key1.timestamp)
	assert.NoError(t, err)
	assert.Equal(t, ts1, parsedTS1, "Timestamp precision should be preserved")

	parsedTS2, err := time.Parse(time.RFC3339Nano, key2.timestamp)
	assert.NoError(t, err)
	assert.Equal(t, ts2, parsedTS2, "Timestamp precision should be preserved")

	// Test PostgreSQL microsecond precision - timestamps differing only in nanoseconds
	// below microsecond precision should be considered equal after truncation
	ts3 := time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC) // microsecond boundary
	ts4 := time.Date(2024, 1, 1, 12, 0, 0, 123456999, time.UTC) // 999 nanoseconds added

	// When truncated to microsecond precision (PostgreSQL's precision), these should be equal
	ts3Trunc := ts3.Truncate(time.Microsecond)
	ts4Trunc := ts4.Truncate(time.Microsecond)
	assert.Equal(t, ts3Trunc, ts4Trunc, "Timestamps should be equal after truncating to microsecond precision")
}

// TestParseNumericTimestamp tests that Unix timestamps in seconds, ms, µs, and ns are converted correctly.
func TestParseNumericTimestamp(t *testing.T) {
	// Unix seconds: 1704067200 = 2024-01-01 12:00:00 UTC
	sec := time.Unix(1704067200, 0)
	assert.Equal(t, sec, parseNumericTimestamp(1704067200))
	// Unix milliseconds: 1704067200000 ms
	ms := time.Unix(0, 1704067200000*1e6)
	assert.Equal(t, ms, parseNumericTimestamp(1704067200000))
	// Unix microseconds: 1704067200000000 µs -> ns = *1000
	us := time.Unix(0, 1704067200000000*1000)
	assert.Equal(t, us, parseNumericTimestamp(1704067200000000))
	// Unix nanoseconds: 1704067200000000000 ns (same instant)
	ns := time.Unix(0, 1704067200000000000)
	assert.Equal(t, ns, parseNumericTimestamp(1704067200000000000))
}

// TestExtractOriginInfoFromRow tests extraction of origin info from row metadata.
func TestExtractOriginInfoFromRow(t *testing.T) {
	t.Run("nil_row_returns_nil", func(t *testing.T) {
		result := extractOriginInfoFromRow(nil)
		require.Nil(t, result)
	})

	t.Run("rfc3339nano_timestamp", func(t *testing.T) {
		tsStr := "2024-01-01T12:00:00.123456789Z"
		row := types.OrderedMap{
			{Key: "_spock_metadata_", Value: map[string]any{
				"node_origin": "node_n3",
				"commit_ts":   tsStr,
			}},
		}
		result := extractOriginInfoFromRow(row)
		require.NotNil(t, result)
		assert.Equal(t, "node_n3", result.nodeOrigin)
		require.NotNil(t, result.commitTS)
		expected, err := time.Parse(time.RFC3339Nano, tsStr)
		require.NoError(t, err)
		assert.True(t, result.commitTS.Equal(expected), "commit_ts should parse as RFC3339Nano")
	})

	t.Run("numeric_timestamp_unix_seconds", func(t *testing.T) {
		row := types.OrderedMap{
			{Key: "_spock_metadata_", Value: map[string]any{
				"node_origin": "node_1",
				"commit_ts":   int64(1704067200), // 2024-01-01 12:00:00 UTC
			}},
		}
		result := extractOriginInfoFromRow(row)
		require.NotNil(t, result)
		assert.Equal(t, "node_1", result.nodeOrigin)
		require.NotNil(t, result.commitTS)
		assert.Equal(t, time.Unix(1704067200, 0), *result.commitTS)
	})

	t.Run("numeric_timestamp_float64", func(t *testing.T) {
		row := types.OrderedMap{
			{Key: "_spock_metadata_", Value: map[string]any{
				"node_origin": "node_2",
				"commit_ts":   float64(1704067200),
			}},
		}
		result := extractOriginInfoFromRow(row)
		require.NotNil(t, result)
		require.NotNil(t, result.commitTS)
		assert.Equal(t, time.Unix(1704067200, 0), *result.commitTS)
	})

	t.Run("graceful_fallback_origin_only_no_commit_ts", func(t *testing.T) {
		row := types.OrderedMap{
			{Key: "_spock_metadata_", Value: map[string]any{
				"node_origin": "node_n3",
				// no commit_ts - e.g. LSN unavailable
			}},
		}
		result := extractOriginInfoFromRow(row)
		require.NotNil(t, result)
		assert.Equal(t, "node_n3", result.nodeOrigin)
		assert.Nil(t, result.commitTS)
	})

	t.Run("empty_row_returns_nil", func(t *testing.T) {
		row := types.OrderedMap{}
		result := extractOriginInfoFromRow(row)
		require.Nil(t, result)
	})
}

// Helper functions for creating pointers
func ptrUint64(v uint64) *uint64 {
	return &v
}

func ptrTime(t time.Time) *time.Time {
	return &t
}
