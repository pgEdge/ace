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
	"os"
	"path/filepath"
	"testing"

	"github.com/pgedge/ace/internal/consistency/repair/plan"
	"github.com/stretchr/testify/require"
)

// TestRowStatement checks the SQL text and the parameters of each kind of row
// operation: every value goes as text in a parameter of the type of its
// column without a type modifier, never through a cast from text, and the
// key of the row must also print as the key value.
func TestRowStatement(t *testing.T) {
	task := &TableRepairTask{}
	task.Schema = "public"
	task.Table = "test_table"
	task.Key = []string{"id"}
	task.Cols = []string{"id", "a", "ts_col", "n"}
	castTypes := map[string]string{
		"id":     "integer",
		"a":      "text",
		"ts_col": "timestamp without time zone",
		"n":      "numeric",
	}
	row := map[string]any{"id": "1", "a": "x", "ts_col": "2024-06-15 10:30:00", "n": nil}
	key := `"id" = $1::integer AND format('%s', "id") COLLATE "C" = format('%s', $1::integer)`

	sql, args, err := task.rowStatement(opInsert, row, castTypes)
	require.NoError(t, err)
	require.Equal(t, `INSERT INTO "public"."test_table" ("id", "a", "ts_col", "n") VALUES `+
		`($1::integer, $2::text, $3::timestamp without time zone, $4::numeric)`, sql)
	require.Equal(t, []any{"1", "x", "2024-06-15 10:30:00", nil}, args)

	sql, args, err = task.rowStatement(opUpdate, row, castTypes)
	require.NoError(t, err)
	require.Equal(t, `UPDATE "public"."test_table" SET "a" = $2::text, "ts_col" = $3::timestamp without time zone, `+
		`"n" = $4::numeric WHERE `+key, sql)
	require.Equal(t, []any{"1", "x", "2024-06-15 10:30:00", nil}, args)

	sql, args, err = task.rowStatement(opDelete, row, castTypes)
	require.NoError(t, err)
	require.Equal(t, `DELETE FROM "public"."test_table" WHERE `+key, sql)
	require.Equal(t, []any{"1"}, args)

	// A NULL fix sets only the columns given in the row, and only where they
	// are still NULL. An empty string is a value, not a NULL.
	fix := map[string]any{"id": "1", "a": "", "ts_col": "2024-06-15 10:30:00"}
	sql, args, err = task.rowStatement(opFixNulls, fix, castTypes)
	require.NoError(t, err)
	require.Equal(t, `UPDATE "public"."test_table" SET "a" = $2::text, "ts_col" = $3::timestamp without time zone `+
		`WHERE `+key+` AND "a" IS NULL AND "ts_col" IS NULL`, sql)
	require.Equal(t, []any{"1", "", "2024-06-15 10:30:00"}, args)

	// A key cannot be NULL, and a column needs a type.
	_, _, err = task.rowStatement(opDelete, map[string]any{"id": nil}, castTypes)
	require.ErrorContains(t, err, "is NULL")
	_, _, err = task.rowStatement(opInsert, row, map[string]string{"id": "integer"})
	require.ErrorContains(t, err, "no type for column")

	// A table with only key columns has nothing to update.
	task.Cols = []string{"id"}
	_, _, err = task.rowStatement(opUpdate, map[string]any{"id": "1"}, castTypes)
	require.Error(t, err)
}

func TestRepairSetsOneOperationPerKey(t *testing.T) {
	a := map[string]any{"id": 1, "v": "a"}
	b := map[string]any{"id": 1, "v": "b"}

	tests := []struct {
		name         string
		add          func(s *repairSets)
		wantInsert   bool
		wantUpdate   bool
		wantDelete   bool
		wantConflict string
	}{
		{
			name: "same update from two pairs",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.write("n2", "1", a, true)
				s.pair = "n2/n3"
				s.write("n2", "1", map[string]any{"id": 1, "v": "a"}, true)
			},
			wantUpdate: true,
		},
		{
			name: "deletes with other rows",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.remove("n2", "1", a)
				s.pair = "n2/n3"
				s.remove("n2", "1", b)
			},
			wantDelete: true,
		},
		{
			name: "update and insert",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.write("n2", "1", a, true)
				s.pair = "n2/n3"
				s.write("n2", "1", a, false)
			},
			wantConflict: "node pairs n1/n2 and n2/n3 give different row operations for this key: update and insert",
		},
		{
			name: "updates with other rows",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.write("n2", "1", a, true)
				s.pair = "n2/n3"
				s.write("n2", "1", b, true)
			},
			wantConflict: "node pairs n1/n2 and n2/n3 give different rows to update for this key",
		},
		{
			name: "delete and insert, then a third pair",
			add: func(s *repairSets) {
				s.pair = "n1/n2"
				s.remove("n2", "1", a)
				s.pair = "n2/n3"
				s.write("n2", "1", b, false)
				s.pair = "n2/n4"
				s.write("n2", "1", b, false)
			},
			wantConflict: "node pairs n1/n2 and n2/n3 give different row operations for this key: delete and insert",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newRepairSets()
			tc.add(s)
			_, ins := s.inserts["n2"]["1"]
			_, upd := s.updates["n2"]["1"]
			_, del := s.deletes["n2"]["1"]
			require.Equal(t, tc.wantInsert, ins, "insert")
			require.Equal(t, tc.wantUpdate, upd, "update")
			require.Equal(t, tc.wantDelete, del, "delete")
			c, ok := s.conflicts["n2"]["1"]
			require.Equal(t, tc.wantConflict != "", ok, "conflict")
			require.Equal(t, tc.wantConflict, c.reason)
			require.Equal(t, []string{"n2"}, s.nodes())
		})
	}

	task := &TableRepairTask{}
	task.Key = []string{"id"}
	s := newRepairSets()
	s.pair = "n1/n2"
	s.write("n2", "1", a, true)
	s.pair = "n2/n3"
	s.write("n2", "1", a, false)
	task.report = &RepairReport{Changes: map[string]any{}}
	task.recordConflicts(s)
	want := []FailedRowOp{{
		Node:      "n2",
		Operation: opConflict,
		Key:       map[string]any{"id": 1},
		Reason:    "node pairs n1/n2 and n2/n3 give different row operations for this key: update and insert",
	}}
	require.Equal(t, want, task.FailedOps)
	require.Equal(t, want, task.report.Changes["n2"].(map[string]any)["failed_rows"])
}

// TestPlanBidirectional checks that the bidirectional action of a plan
// inserts a row that one node has on the other node, and refuses a row that
// both nodes have.
func TestPlanBidirectional(t *testing.T) {
	a := map[string]any{"id": 1, "v": "a"}
	b := map[string]any{"id": 1, "v": "b"}
	action := &planner.RepairPlanAction{Type: planner.RepairActionBidirectional}

	mismatch := planDiffRow{pkStr: "1", node1Name: "n1", node2Name: "n2", n1Row: a, n2Row: b, diffType: "row_mismatch"}
	require.Error(t, validateActionCompatibility(action, mismatch))
	require.Error(t, ensureActionCompatible(mismatch, action))
	require.Error(t, applyPlanAction(mismatch, action, "r", newRepairSets(), nil, nil, nil))

	tests := []struct {
		row    planDiffRow
		target string
		want   map[string]any
	}{
		{planDiffRow{pkStr: "1", node1Name: "n1", node2Name: "n2", n1Row: a, diffType: "missing_on_n2"}, "n2", a},
		{planDiffRow{pkStr: "1", node1Name: "n1", node2Name: "n2", n2Row: b, diffType: "missing_on_n1"}, "n1", b},
	}
	for _, tc := range tests {
		t.Run(tc.row.diffType, func(t *testing.T) {
			require.NoError(t, validateActionCompatibility(action, tc.row))
			require.NoError(t, ensureActionCompatible(tc.row, action))
			s := newRepairSets()
			require.NoError(t, applyPlanAction(tc.row, action, "r", s, nil, nil, nil))
			require.Equal(t, map[string]map[string]map[string]any{tc.target: {"1": tc.want}}, s.inserts)
			require.Empty(t, s.updates)
			require.Empty(t, s.deletes)
		})
	}
}

// TestPlanBidirectionalOnMismatchRefused checks that a plan with a
// bidirectional rule for row_mismatch is refused when it is read.
func TestPlanBidirectionalOnMismatchRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`version: 1
tables:
  public.t:
    rules:
      - name: swap
        diff_type: [row_mismatch]
        action: { type: bidirectional }
`), 0o600))
	_, err := planner.LoadRepairPlanFile(path)
	require.ErrorContains(t, err, "bidirectional incompatible with diff_type row_mismatch")
}

// TestBuildCustomRowTemplates checks the templates of custom_row. A template
// gives the text of a column or of the spock metadata, and nil for a NULL;
// a YAML null is NULL. A template with a column that the table does not
// have, a node on which the row is missing, or another form is an error,
// never a NULL that would clear the column or keep the helpers from filling
// it.
func TestBuildCustomRowTemplates(t *testing.T) {
	row := planDiffRow{
		pkStr:     "1",
		node1Name: "node_a",
		node2Name: "node_b",
		diffType:  "missing_on_n2",
		pkMap:     map[string]any{"id": "1"},
		n1Row:     map[string]any{"id": "1", "email": "a@example.com", "note": nil},
		n1Meta:    map[string]any{"commit_ts": "2026-01-01 00:00:00+00"},
	}
	text := func(s string) planner.PlanValue { return planner.PlanValue{Text: s, Valid: true} }
	custom := func(cr map[string]planner.PlanValue, coalesce ...string) *planner.RepairPlanAction {
		a := &planner.RepairPlanAction{Type: planner.RepairActionCustom, CustomRow: cr}
		if len(coalesce) > 0 {
			a.CustomHelpers = &planner.CustomHelperSpec{CoalescePriority: coalesce}
		}
		return a
	}

	got, err := buildCustomRow(custom(map[string]planner.PlanValue{
		"email":  text("{{n1.email}}"),
		"note":   text("{{ N1.note }}"),
		"extra":  {},
		"plain":  text("x"),
		"ts":     text("{{n1.commit_ts}}"),
		"origin": text("{{n1.node_origin}}"),
	}), row, nil)
	require.NoError(t, err)
	require.Equal(t, map[string]any{
		"id": "1", "email": "a@example.com", "note": nil, "extra": nil, "plain": "x",
		"ts": "2026-01-01 00:00:00+00", "origin": nil,
	}, got)

	for tmpl, want := range map[string]string{
		"{{n1.emali}}": `custom_row.email: template {{n1.emali}} names column "emali", which the table does not have (pk 1)`,
		"{{n2.email}}": "custom_row.email: template {{n2.email}} reads node n2, but the row is missing on node_b (pk 1)",
		"{{email}}":    "custom_row.email: template {{email}} is not {{n1.column}} or {{n2.column}} (pk 1)",
		"{{n3.email}}": "custom_row.email: template {{n3.email}} is not {{n1.column}} or {{n2.column}} (pk 1)",
		" {{n1.a.b}} ": "custom_row.email: template {{n1.a.b}} is not {{n1.column}} or {{n2.column}} (pk 1)",
	} {
		_, err := buildCustomRow(custom(map[string]planner.PlanValue{"email": text(tmpl)}, "n1"), row, nil)
		require.EqualError(t, err, want, "template %q", tmpl)
	}

	// A template that gives NULL is a value: the helper does not replace it.
	got, err = buildCustomRow(custom(map[string]planner.PlanValue{"email": text("{{n1.note}}")}, "n1"), row, nil)
	require.NoError(t, err)
	require.Nil(t, got["email"])
	require.Equal(t, "1", got["id"])
}

// TestPickFreshestRowOnOneNode checks that pick_freshest takes the row that
// exists when the other node does not have it, whatever the tie side and
// even when the key of the row is NULL: the tie must not choose the missing
// row and give a row with only the key. With rows on both nodes and two NULL
// keys the tie side still wins.
func TestPickFreshestRowOnOneNode(t *testing.T) {
	action := func(key, tie string) *planner.RepairPlanAction {
		return &planner.RepairPlanAction{
			Type:          planner.RepairActionCustom,
			CustomHelpers: &planner.CustomHelperSpec{PickFreshest: &planner.PickFreshestSpec{Key: key, Tie: tie}},
		}
	}
	full := map[string]any{"id": "1", "email": "a@example.com", "updated_at": nil}
	only := func(n1, n2 map[string]any) planDiffRow {
		return planDiffRow{pkStr: "1", node1Name: "node_a", node2Name: "node_b",
			pkMap: map[string]any{"id": "1"}, n1Row: n1, n2Row: n2,
			n1Meta: map[string]any{}, n2Meta: map[string]any{}}
	}

	for _, key := range []string{"updated_at", "commit_ts"} {
		got, err := buildCustomRow(action(key, "n1"), only(nil, full), nil)
		require.NoError(t, err)
		require.Equal(t, full, got, "row on n2 only, key %s, tie n1", key)

		got, err = buildCustomRow(action(key, "n2"), only(full, nil), nil)
		require.NoError(t, err)
		require.Equal(t, full, got, "row on n1 only, key %s, tie n2", key)
	}

	other := map[string]any{"id": "1", "email": "b@example.com", "updated_at": nil}
	got, err := buildCustomRow(action("updated_at", "n2"), only(full, other), nil)
	require.NoError(t, err)
	require.Equal(t, other, got, "rows on both nodes, two NULL keys: the tie side wins")
}

// TestPlanCustomKey checks that a custom row keeps the key of the diff row:
// a key column of custom_row must have the key text, and any other value,
// NULL included, refuses the plan.
func TestPlanCustomKey(t *testing.T) {
	n1 := map[string]any{"id": "42", "v": "a"}
	n2 := map[string]any{"id": "42", "v": "b"}
	row := planDiffRow{pkStr: "42", pkMap: map[string]any{"id": "42"}, node1Name: "n1", node2Name: "n2", n1Row: n1, n2Row: n2, diffType: "row_mismatch"}
	val := func(s string) planner.PlanValue { return planner.PlanValue{Text: s, Valid: true} }
	custom := func(id planner.PlanValue) *planner.RepairPlanAction {
		return &planner.RepairPlanAction{Type: planner.RepairActionCustom, CustomRow: map[string]planner.PlanValue{"id": id, "v": val("c")}}
	}

	refused := []struct {
		name string
		id   planner.PlanValue
	}{
		{"other key", val("43")},
		{"other text of the same number", val("042")},
		{"NULL", planner.PlanValue{}},
	}
	for _, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			s := newRepairSets()
			err := applyPlanAction(row, custom(tc.id), "r", s, nil, nil, nil)
			require.ErrorContains(t, err, "rule r: custom row sets key column id")
			require.ErrorContains(t, err, "a custom row cannot change the key")
			require.True(t, s.empty())
		})
	}

	want := map[string]any{"id": "42", "v": "c"}
	accepted := []struct {
		name   string
		action *planner.RepairPlanAction
		want   map[string]any
	}{
		{"same key text", custom(val("42")), want},
		{"template", custom(val("{{n1.id}}")), want},
		{"no key column", &planner.RepairPlanAction{Type: planner.RepairActionCustom, CustomRow: map[string]planner.PlanValue{"v": val("c")}}, want},
		{"key from a helper", &planner.RepairPlanAction{Type: planner.RepairActionCustom, CustomHelpers: &planner.CustomHelperSpec{CoalescePriority: []string{"n2", "n1"}}}, n2},
	}
	for _, tc := range accepted {
		t.Run(tc.name, func(t *testing.T) {
			s := newRepairSets()
			require.NoError(t, applyPlanAction(row, tc.action, "r", s, nil, nil, nil))
			require.Equal(t, map[string]map[string]map[string]any{"n1": {"42": tc.want}, "n2": {"42": tc.want}}, s.updates)
			require.Empty(t, s.inserts)
			require.Empty(t, s.deletes)
		})
	}
}
