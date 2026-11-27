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
)

// The CDC lookup reads the leaves in the order of the leaf index, so that
// the index gives the leaf of a key with one probe. The two must agree.
func TestLeafLookupFollowsLeafIndex(t *testing.T) {
	ddl, err := MtreeLeafIndexDDL(`"ace"."ace_mtree_public_t"`)
	if err != nil {
		t.Fatalf("render index: %v", err)
	}
	if want := `CREATE INDEX ON "ace"."ace_mtree_public_t" (range_start, node_position) WHERE node_level = 0`; ddl != want {
		t.Errorf("leaf index DDL:\n got %s\nwant %s", ddl, want)
	}

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
		for _, want := range []string{
			"WHERE mt.node_level = 0",
			"AND mt.range_start <= p.pkey::",
			"ORDER BY mt.range_start DESC, mt.node_position DESC",
		} {
			if !strings.Contains(sql, want) {
				t.Errorf("composite=%v: lookup does not contain %q:\n%s", composite, want, sql)
			}
		}
		// An OR with the key condition keeps the planner from using the key
		// as an index bound.
		for _, bad := range []string{"OR p.pkey", "node_position = 0 OR"} {
			if strings.Contains(sql, bad) {
				t.Errorf("composite=%v: the lookup has %q:\n%s", composite, bad, sql)
			}
		}
	}
}
