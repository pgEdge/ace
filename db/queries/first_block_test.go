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
	"strings"
	"testing"
	"text/template"
)

func TestOpenFirstBlockStart(t *testing.T) {
	start := []any{int32(100)}
	if got := OpenFirstBlockStart(FirstBlockPosition, start); got != nil {
		t.Errorf("first block: got start %v, want nil", got)
	}
	got := OpenFirstBlockStart(1, start)
	if len(got) != 1 || got[0] != int32(100) {
		t.Errorf("second block: got start %v, want %v", got, start)
	}
}

// Every template that matches rows to leaf blocks must give the first block
// no lower bound. A template that keeps the bound would drop the rows below
// the first block's range_start: from a hash, from a count, or from the
// dirty marking done by CDC.
func TestFirstBlockTemplatesHaveNoLowerBound(t *testing.T) {
	simpleKey := []string{`"id"`}
	compositeKey := []string{`"a"`, `"b"`}
	base := func(extra map[string]any) map[string]any {
		data := map[string]any{
			"MtreeTable":          `"ace"."ace_mtree_public_t"`,
			"SchemaIdent":         `"public"`,
			"TableIdent":          `"t"`,
			"PkeyType":            "integer",
			"CompositeTypeName":   `"ace"."public_t_key_type"`,
			"StartAttrs":          `(range_start)."a", (range_start)."b"`,
			"EndAttrs":            `(range_end)."a", (range_end)."b"`,
			"PositionPlaceholder": "$1",
			"MergeValPlaceholder": "$2",
		}
		for k, v := range extra {
			data[k] = v
		}
		return data
	}

	cases := []struct {
		name string
		tmpl *template.Template
		data map[string]any
		want string
	}{
		{"UpdateMtreeCounters/simple", SQLTemplates.UpdateMtreeCounters,
			base(map[string]any{"IsComposite": false}), "mt.node_position = 0 OR"},
		{"UpdateMtreeCounters/composite", SQLTemplates.UpdateMtreeCounters,
			base(map[string]any{"IsComposite": true}), "mt.node_position = 0 OR"},
		{"FindBlocksToMerge/simple", SQLTemplates.FindBlocksToMerge,
			base(map[string]any{"SimplePrimaryKey": true, "Key": simpleKey}), "WHEN t1.node_position = 0"},
		{"FindBlocksToMerge/composite", SQLTemplates.FindBlocksToMerge,
			base(map[string]any{"SimplePrimaryKey": false, "Key": compositeKey}), "WHEN t1.node_position = 0"},
		{"FindBlocksToMergeExpanded", SQLTemplates.FindBlocksToMergeExpanded,
			base(map[string]any{"Key": compositeKey}), "WHEN t1.node_position = 0"},
		{"GetBlockWithCount/simple", SQLTemplates.GetBlockWithCount,
			base(map[string]any{"IsComposite": false, "Key": simpleKey}), "WHEN t1.node_position = 0"},
		{"GetBlockWithCount/composite", SQLTemplates.GetBlockWithCount,
			base(map[string]any{"IsComposite": true, "Key": compositeKey}), "WHEN t1.node_position = 0"},
		{"GetBlockWithCountExpanded", SQLTemplates.GetBlockWithCountExpanded,
			base(map[string]any{"Key": compositeKey}), "WHEN t1.node_position = 0"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := RenderSQL(tc.tmpl, tc.data)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if !strings.Contains(sql, tc.want) {
				t.Errorf("SQL does not open the first block (want %q):\n%s", tc.want, sql)
			}
		})
	}
}

// The first block has no lower bound, so CDC must not change its
// range_start: a key below it already belongs to the first block.
func TestUpdateMtreeCountersKeepsRangeStart(t *testing.T) {
	for _, composite := range []bool{false, true} {
		sql, err := RenderSQL(SQLTemplates.UpdateMtreeCounters, map[string]any{
			"MtreeTable":        `"ace"."ace_mtree_public_t"`,
			"IsComposite":       composite,
			"PkeyType":          "integer",
			"CompositeTypeName": `"ace"."public_t_key_type"`,
		})
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if strings.Contains(sql, "range_start =") || strings.Contains(sql, "MIN(") {
			t.Errorf("composite=%v: template still changes range_start:\n%s", composite, sql)
		}
	}
}

// Leaves with the same range_start must keep their old order when they are
// numbered again, or another leaf can take position 0 and lose its lower bound.
func TestResetPositionsBreaksTiesByPosition(t *testing.T) {
	for name, tmpl := range map[string]*template.Template{
		"ResetPositionsByStart":         SQLTemplates.ResetPositionsByStart,
		"ResetPositionsByStartFromTemp": SQLTemplates.ResetPositionsByStartFromTemp,
		"ResetPositionsByStartExpanded": SQLTemplates.ResetPositionsByStartExpanded,
	} {
		sql, err := RenderSQL(tmpl, map[string]any{"MtreeTable": `"ace"."ace_mtree_public_t"`})
		if err != nil {
			t.Fatalf("%s: render: %v", name, err)
		}
		if !strings.Contains(sql, "ORDER BY range_start, node_position") {
			t.Errorf("%s does not break ties by node_position:\n%s", name, sql)
		}
	}
}
