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
	"regexp"
	"strings"
	"testing"
	"text/template"
)

// The range_end of a leaf is the range_start of the next leaf, and
// BlockHashSQL does not include it in the leaf. Every template that maps
// rows or changed keys to leaves must follow the same rule. Otherwise a key
// on the bound goes to two leaves: CDC marks both dirty and counts the
// change twice, and the row counts used for merges count the row twice.
func TestLeafTemplatesExcludeRangeEnd(t *testing.T) {
	simpleKey := []string{`"id"`}
	compositeKey := []string{`"a"`, `"b"`}
	data := func(extra map[string]any) map[string]any {
		d := map[string]any{
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
			d[k] = v
		}
		return d
	}

	cases := []struct {
		name string
		tmpl *template.Template
		data map[string]any
	}{
		{"UpdateMtreeCounters/simple", SQLTemplates.UpdateMtreeCounters,
			data(map[string]any{"IsComposite": false})},
		{"UpdateMtreeCounters/composite", SQLTemplates.UpdateMtreeCounters,
			data(map[string]any{"IsComposite": true})},
		{"FindBlocksToMerge/simple", SQLTemplates.FindBlocksToMerge,
			data(map[string]any{"SimplePrimaryKey": true, "Key": simpleKey})},
		{"FindBlocksToMerge/composite", SQLTemplates.FindBlocksToMerge,
			data(map[string]any{"SimplePrimaryKey": false, "Key": compositeKey})},
		{"FindBlocksToMergeExpanded", SQLTemplates.FindBlocksToMergeExpanded,
			data(map[string]any{"Key": compositeKey})},
		{"GetBlockWithCount/simple", SQLTemplates.GetBlockWithCount,
			data(map[string]any{"IsComposite": false, "Key": simpleKey})},
		{"GetBlockWithCount/composite", SQLTemplates.GetBlockWithCount,
			data(map[string]any{"IsComposite": true, "Key": compositeKey})},
		{"GetBlockWithCountExpanded", SQLTemplates.GetBlockWithCountExpanded,
			data(map[string]any{"Key": compositeKey})},
	}

	inclusive := regexp.MustCompile(`<=\s*(mt|t1)\.range_end`)
	exclusive := regexp.MustCompile(`<\s*(mt|t1)\.range_end`)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := RenderSQL(tc.tmpl, tc.data)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if inclusive.MatchString(sql) {
				t.Errorf("SQL includes range_end in the leaf:\n%s", sql)
			}
			if !exclusive.MatchString(sql) {
				t.Errorf("SQL has no exclusive range_end bound:\n%s", strings.TrimSpace(sql))
			}
		})
	}
}
