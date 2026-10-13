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

package slicer

import (
	"strings"
	"testing"
)

func TestPickBounds(t *testing.T) {
	bounds := make([]string, 101)
	for i := range bounds {
		bounds[i] = string(rune('A'+i%26)) + strings.Repeat("x", i)
	}

	got := pickBounds(bounds, 4)
	want := []string{bounds[25], bounds[50], bounds[75]}
	if len(got) != len(want) {
		t.Fatalf("pickBounds(101, 4) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("pickBounds(101, 4)[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	if got := pickBounds(bounds, 1); got != nil {
		t.Fatalf("pickBounds with one part = %v, want nil", got)
	}
	if got := pickBounds([]string{"a"}, 8); got != nil {
		t.Fatalf("pickBounds with one bound = %v, want nil", got)
	}

	// More parts than histogram buckets: no duplicates, never the first
	// bound.
	got = pickBounds([]string{"a", "b", "c"}, 16)
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Fatalf("pickBounds(3, 16) = %v, want [b c]", got)
	}
}

func TestPartsFromKeys(t *testing.T) {
	parts := partsFromKeys(nil)
	if len(parts) != 1 || parts[0].Lo != nil || parts[0].Hi != nil {
		t.Fatalf("partsFromKeys(nil) = %v, want one open part", parts)
	}

	keys := [][]any{{int64(10)}, {int64(10)}, {int64(20)}}
	parts = partsFromKeys(keys)
	if len(parts) != 3 {
		t.Fatalf("partsFromKeys = %v, want 3 parts", parts)
	}
	if parts[0].Lo != nil || parts[0].Hi[0] != int64(10) {
		t.Fatalf("first part = %v, want (nil, 10)", parts[0])
	}
	if parts[1].Lo[0] != int64(10) || parts[1].Hi[0] != int64(20) {
		t.Fatalf("second part = %v, want [10, 20)", parts[1])
	}
	if parts[2].Lo[0] != int64(20) || parts[2].Hi != nil {
		t.Fatalf("last part = %v, want [20, nil)", parts[2])
	}
}

func TestSamplePercent(t *testing.T) {
	if p := samplePercent(500, 16); p != 100 {
		t.Fatalf("small table: got %v, want 100", p)
	}
	// 16 parts * 100 pages = 1600 pages out of 1,600,000.
	if p := samplePercent(1_600_000, 16); p < 0.0999 || p > 0.1001 {
		t.Fatalf("large table: got %v, want 0.1", p)
	}
	// The minimum of 1000 pages applies for few parts.
	if p := samplePercent(1_000_000, 2); p < 0.0999 || p > 0.1001 {
		t.Fatalf("few parts: got %v, want 0.1", p)
	}
}

func TestDefaults(t *testing.T) {
	cases := []struct{ hash, want int }{{0, 1}, {1, 1}, {4, 1}, {5, 2}, {16, 4}, {100, 4}}
	for _, c := range cases {
		if got := DefaultWorkers(c.hash); got != c.want {
			t.Errorf("DefaultWorkers(%d) = %d, want %d", c.hash, got, c.want)
		}
	}
	if got := DefaultParts(3); got != 12 {
		t.Errorf("DefaultParts(3) = %d, want 12", got)
	}
}

func TestCutQueries(t *testing.T) {
	cfg := &Config{Schema: "public", Table: "t", Key: []string{"a", "b"}, Filter: "x > 0", BlockSize: 10}

	sql := chunkSQL(cfg, true)
	for _, want := range []string{
		`WITH RECURSIVE _ace_b(_ace_i, "a", "b")`,
		`ROW("a", "b") >= ROW($1, $2)`,
		`ROW("a", "b") < ROW($3, $4)`,
		`ROW(_ace_t."a", _ace_t."b") >= ROW(_ace_b."a", _ace_b."b")`,
		`ROW(_ace_t."a", _ace_t."b") < ROW($3, $4)`,
		`(x > 0)`,
		`OFFSET $5 LIMIT 1`,
		`_ace_b._ace_i < $6`,
		`FROM "public"."t" AS _ace_t`,
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("chunkSQL(hi) does not contain %q:\n%s", want, sql)
		}
	}

	simple := &Config{Schema: "s", Table: "t", Key: []string{"id"}, BlockSize: 10}
	sql = chunkSQL(simple, false)
	for _, want := range []string{`"id" >= $1`, `_ace_t."id" >= _ace_b."id"`, `OFFSET $2 LIMIT 1`, `_ace_i < $3`} {
		if !strings.Contains(sql, want) {
			t.Errorf("chunkSQL(no hi) does not contain %q:\n%s", want, sql)
		}
	}
	if strings.Contains(sql, " < $") && strings.Contains(sql, `"id" < `) {
		t.Errorf("chunkSQL(no hi) has an upper bound:\n%s", sql)
	}

	if got := firstSQL(simple, false); got != `SELECT "id" FROM "s"."t" ORDER BY "id" LIMIT 1` {
		t.Errorf("firstSQL(no hi) = %s", got)
	}
	if got := firstSQL(simple, true); !strings.Contains(got, `WHERE "id" < $1`) {
		t.Errorf("firstSQL(hi) = %s", got)
	}
	if got := tailSQL(cfg, true); !strings.Contains(got, `ROW("a", "b") >= ROW($1, $2) AND ROW("a", "b") < ROW($3, $4) AND (x > 0) LIMIT $5`) {
		t.Errorf("tailSQL(hi) = %s", got)
	}
	if got := tailSQL(simple, false); !strings.Contains(got, `WHERE "id" >= $1 LIMIT $2`) {
		t.Errorf("tailSQL(no hi) = %s", got)
	}
}

func TestConfigValidate(t *testing.T) {
	c := Config{Key: []string{"id"}, BlockSize: 0}
	if err := c.validate(); err == nil {
		t.Fatal("block size 0 accepted")
	}
	c = Config{BlockSize: 10}
	if err := c.validate(); err == nil {
		t.Fatal("empty key accepted")
	}
	c = Config{Key: []string{"id"}, BlockSize: 10}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	if c.Workers != 1 || c.Parts != 4 || c.ChunkRows != DefaultChunkRows {
		t.Fatalf("defaults not applied: %+v", c)
	}
}
