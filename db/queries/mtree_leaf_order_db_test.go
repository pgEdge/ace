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

package queries

import (
	"context"
	"testing"
)

// With a case-insensitive key collation, 'b' and 'B' are one key, so a leaf
// that ends at 'b' and a leaf that starts at 'B' follow each other. The
// check must compare the bounds with the key collation, also for the end of
// the leaf before, or it rejects such leaves. Without the collation the same
// leaves have a gap, and the check reports the leaf after the gap.
func TestFindLeafOutOfOrderUsesKeyCollation(t *testing.T) {
	pool := rangesTestPool(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE COLLATION ace_ranges_test.ci
		(provider = icu, locale = 'und-u-ks-level2', deterministic = false)`); err != nil {
		t.Skipf("the server cannot create a nondeterministic collation: %v", err)
	}
	for _, sql := range []string{
		`CREATE TYPE ace_ranges_test.kt AS (a int, b text)`,
		`CREATE TABLE ace_ranges_test.simple (node_level int, node_position bigint,
			range_start text, range_end text)`,
		`INSERT INTO ace_ranges_test.simple VALUES
			(0, 0, 'a', 'b'), (0, 1, 'B', 'c'), (0, 2, 'c', NULL)`,
		`CREATE TABLE ace_ranges_test.composite (node_level int, node_position bigint,
			range_start ace_ranges_test.kt, range_end ace_ranges_test.kt)`,
		`INSERT INTO ace_ranges_test.composite VALUES
			(0, 0, '(1,a)', '(1,b)'), (0, 1, '(1,B)', NULL)`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	ci := map[string]string{"b": "ace_ranges_test.ci"}
	for _, tc := range []struct {
		name       string
		table      string
		key        []string
		composite  bool
		collations map[string]string
		found      bool
		pos        int64
	}{
		{"simple key, key collation", "simple", []string{"b"}, false, ci, false, 0},
		{"simple key, no collation", "simple", []string{"b"}, false, nil, true, 1},
		{"composite key, key collation", "composite", []string{"a", "b"}, true, ci, false, 0},
		{"composite key, no collation", "composite", []string{"a", "b"}, true, nil, true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pos, found, err := FindLeafOutOfOrder(ctx, pool, "ace_ranges_test."+tc.table,
				tc.key, tc.composite, tc.collations)
			if err != nil {
				t.Fatal(err)
			}
			if found != tc.found || pos != tc.pos {
				t.Errorf("got found=%v pos=%d, want found=%v pos=%d", found, pos, tc.found, tc.pos)
			}
		})
	}
}
