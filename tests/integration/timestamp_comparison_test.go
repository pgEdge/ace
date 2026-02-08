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

package integration

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestCompareTimestampsExact verifies the new exact timestamp comparison function
func TestCompareTimestampsExact(t *testing.T) {
	tests := []struct {
		name      string
		t1        time.Time
		t2        time.Time
		tolerance time.Duration
		expected  bool
	}{
		{
			name:      "identical timestamps",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123456789, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 123456789, time.UTC),
			tolerance: time.Microsecond,
			expected:  true,
		},
		{
			name:      "1 nanosecond difference (within microsecond tolerance)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123456789, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 123456788, time.UTC),
			tolerance: time.Microsecond,
			expected:  true,
		},
		{
			name:      "PostgreSQL microsecond precision (exact)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC),
			tolerance: time.Microsecond,
			expected:  true,
		},
		{
			name:      "1 microsecond difference (within tolerance)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 123457000, time.UTC),
			tolerance: time.Microsecond,
			expected:  true,
		},
		{
			name:      "2 microsecond difference (exceeds tolerance)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 123458000, time.UTC),
			tolerance: time.Microsecond,
			expected:  false,
		},
		{
			name:      "1 millisecond difference (exceeds microsecond tolerance)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123000000, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 124000000, time.UTC),
			tolerance: time.Microsecond,
			expected:  false,
		},
		{
			name:      "1 second difference (exceeds tolerance)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 1, 0, time.UTC),
			tolerance: time.Microsecond,
			expected:  false,
		},
		{
			name:      "negative difference (within tolerance)",
			t1:        time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC),
			t2:        time.Date(2024, 1, 1, 12, 0, 0, 123457000, time.UTC),
			tolerance: time.Microsecond,
			expected:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareTimestampsExact(tt.t1, tt.t2, tt.tolerance)
			assert.Equal(t, tt.expected, result,
				"compareTimestampsExact(%v, %v, %v) = %v, expected %v",
				tt.t1, tt.t2, tt.tolerance, result, tt.expected)
		})
	}
}

// TestPostgreSQLMicrosecondPrecision demonstrates the correct way to compare timestamps
// when dealing with PostgreSQL's microsecond precision
func TestPostgreSQLMicrosecondPrecision(t *testing.T) {
	// PostgreSQL stores timestamps with microsecond precision
	// Go time.Time has nanosecond precision
	// When comparing, we should truncate to microseconds first
	
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 123456789, time.UTC) // has nanoseconds
	ts2 := time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC) // microsecond boundary
	
	// Direct comparison would show they're different
	assert.NotEqual(t, ts1, ts2, "Timestamps with different nanoseconds are not equal")
	
	// After truncating to PostgreSQL precision, they should be equal
	ts1Trunc := ts1.Truncate(time.Microsecond)
	ts2Trunc := ts2.Truncate(time.Microsecond)
	assert.Equal(t, ts1Trunc, ts2Trunc, "After truncating to microseconds, timestamps should be equal")
	
	t.Logf("✓ Original ts1: %v", ts1.Format(time.RFC3339Nano))
	t.Logf("✓ Original ts2: %v", ts2.Format(time.RFC3339Nano))
	t.Logf("✓ Truncated ts1: %v", ts1Trunc.Format(time.RFC3339Nano))
	t.Logf("✓ Truncated ts2: %v", ts2Trunc.Format(time.RFC3339Nano))
	t.Logf("✓ PostgreSQL precision: microseconds (6 decimal places)")
}

// TestOldVsNewComparison demonstrates the difference between old and new comparison functions
func TestOldVsNewComparison(t *testing.T) {
	// Timestamps that differ by 500 milliseconds
	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 12, 0, 0, 500000000, time.UTC)

	// Old function with 1 second tolerance would consider these equal
	oldResult := compareTimestamps(ts1, ts2, 1)
	assert.True(t, oldResult, "Old function incorrectly considers 500ms difference as equal")

	// New function with microsecond tolerance correctly identifies them as different
	newResult := compareTimestampsExact(ts1, ts2, time.Microsecond)
	assert.False(t, newResult, "New function correctly identifies 500ms difference")

	// Timestamps that differ by 1 microsecond or less should be considered equal
	ts3 := time.Date(2024, 1, 1, 12, 0, 0, 123456000, time.UTC)
	ts4 := time.Date(2024, 1, 1, 12, 0, 0, 123456500, time.UTC) // 500ns difference

	exactResult := compareTimestampsExact(ts3, ts4, time.Microsecond)
	assert.True(t, exactResult, "New function correctly considers sub-microsecond differences as equal")
}
