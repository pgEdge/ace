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

package planner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func loadPlan(t *testing.T, text string) *RepairPlanFile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(path, []byte(text), 0o600))
	plan, err := LoadRepairPlanFile(path)
	require.NoError(t, err)
	return plan
}

// The keys and the values of a plan keep the text that the file writes: the
// YAML parser must not turn 1.50 into 1.5, 007 into 7 or true into a
// boolean. A null scalar is NULL, a quoted "null" is text, and "" is the
// empty text, not NULL.
func TestPlanValuesKeepTheirText(t *testing.T) {
	plan := loadPlan(t, `
version: 1
tables:
  public.t:
    default_action: { type: skip }
    rules:
      - name: r
        pk_in:
          - equals: [1.50, 007, true, "x y", 2026-01-02 03:04:05+00]
          - equals: [[1.50, "a"], [2, b]]
          - range: { from: 1.0, to: 10 }
        action:
          type: custom
          custom_row: { a: null, b: ~, c: "null", d: 1.50, e: "{{n1.e}}", f: "" }
    row_overrides:
      - pk: { id: 007 }
        action: { type: keep_n1 }
`)
	tp := plan.Tables["public.t"]
	m := tp.Rules[0].PKIn
	var texts []string
	for _, k := range m[0].Equals {
		require.False(t, k.List)
		texts = append(texts, k.Values[0].Text)
	}
	require.Equal(t, []string{"1.50", "007", "true", "x y", "2026-01-02 03:04:05+00"}, texts)
	require.True(t, m[1].Equals[0].List)
	require.Equal(t, []PlanValue{{Text: "1.50", Valid: true}, {Text: "a", Valid: true}}, m[1].Equals[0].Values)
	require.Equal(t, "1.0", m[2].Range.From.Text)
	require.Equal(t, "10", m[2].Range.To.Text)

	row := tp.Rules[0].Action.CustomRow
	require.Equal(t, PlanValue{}, row["a"])
	require.Equal(t, PlanValue{}, row["b"])
	require.Equal(t, PlanValue{Text: "null", Valid: true}, row["c"])
	require.Equal(t, PlanValue{Text: "1.50", Valid: true}, row["d"])
	require.Equal(t, PlanValue{Text: "{{n1.e}}", Valid: true}, row["e"])
	require.Equal(t, PlanValue{Text: "", Valid: true}, row["f"])

	require.Equal(t, PlanValue{Text: "007", Valid: true}, tp.RowOverrides[0].PK["id"])
}

// A key of an override cannot be NULL.
func TestPlanRefusesNullKeys(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(path, []byte(`
version: 1
tables:
  public.t:
    default_action: { type: skip }
    row_overrides:
      - pk: { id: null }
        action: { type: keep_n1 }
`), 0o600))
	_, err := LoadRepairPlanFile(path)
	require.ErrorContains(t, err, "cannot be null")
}

// A JSON plan keeps the text of its numbers too.
func TestPlanValuesFromJSON(t *testing.T) {
	var v PlanValue
	require.NoError(t, v.UnmarshalJSON([]byte(`1.50`)))
	require.Equal(t, PlanValue{Text: "1.50", Valid: true}, v)
	require.NoError(t, v.UnmarshalJSON([]byte(`"007"`)))
	require.Equal(t, PlanValue{Text: "007", Valid: true}, v)
	require.NoError(t, v.UnmarshalJSON([]byte(`null`)))
	require.Equal(t, PlanValue{}, v)
	require.Error(t, v.UnmarshalJSON([]byte(`{"a":1}`)))

	var k PlanKey
	require.NoError(t, k.UnmarshalJSON([]byte(`[1.50, "a"]`)))
	require.Equal(t, PlanKey{List: true, Values: []PlanValue{{Text: "1.50", Valid: true}, {Text: "a", Valid: true}}}, k)
}
