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

package schema

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestWorstExitCode_EmptyIsIdentical(t *testing.T) {
	if code := WorstExitCode(nil); code != ExitIdentical {
		t.Fatalf("expected ExitIdentical for no divergences, got %d", code)
	}
}

func TestWorstExitCode_PicksTheMostSevereRank(t *testing.T) {
	divs := []Divergence{
		{Rank: RankCosmetic},
		{Rank: RankEquivalentDiffering},
		{Rank: RankNarrowed},
	}
	if code := WorstExitCode(divs); code != ExitNarrowed {
		t.Fatalf("expected ExitNarrowed (worst of the three), got %d", code)
	}
}

func TestWorstExitCode_AbsentAndIncompatibleShareTheWorstCode(t *testing.T) {
	if code := WorstExitCode([]Divergence{{Rank: RankAbsent}}); code != ExitIncompatible {
		t.Fatalf("expected RankAbsent to map to ExitIncompatible, got %d", code)
	}
	if code := WorstExitCode([]Divergence{{Rank: RankIncompatible}}); code != ExitIncompatible {
		t.Fatalf("expected RankIncompatible to map to ExitIncompatible, got %d", code)
	}
}

func TestFormatDivergences_EmptyIsEmptyString(t *testing.T) {
	if got := FormatDivergences(nil); got != "" {
		t.Fatalf("expected empty string for no divergences, got %q", got)
	}
}

func TestFormatDivergences_IncludesObjectPropertyAndBothValues(t *testing.T) {
	divs := []Divergence{
		{
			Object: "public.orders.amount", Kind: "column", Property: "type",
			NodeA: "n1", NodeB: "n2", ValueOnA: "int4", ValueOnB: "int8",
			Rank: RankNarrowed, NarrowSide: "n1",
		},
	}
	got := FormatDivergences(divs)
	for _, want := range []string{"public.orders.amount", "type", "n1", "n2", "int4", "int8", RankNarrowed} {
		if !strings.Contains(got, want) {
			t.Errorf("expected formatted output to contain %q, got: %s", want, got)
		}
	}
}

func TestFormatDivergences_AppendsNoteWhenPresent(t *testing.T) {
	divs := []Divergence{
		{
			Object: "public.orders", Kind: "key", Property: "replica_identity",
			NodeA: "n1", NodeB: "n2", ValueOnA: "d", ValueOnB: "f",
			Rank: RankIncompatible, Note: "explains itself",
		},
	}
	got := FormatDivergences(divs)
	if !strings.Contains(got, "explains itself") {
		t.Errorf("expected Note to appear in output, got: %s", got)
	}
}

// TestReportRendersListSeparatorReadably checks that a value joined for
// comparison with a unit separator is not printed with it: a key of two
// columns must read as "a, b", not "a\x1fb".
func TestReportRendersListSeparatorReadably(t *testing.T) {
	out := FormatDivergences([]Divergence{{
		Object: "public.t", Kind: "key", Property: "key_columns",
		NodeA: "n1", NodeB: "n2",
		ValueOnA: joinList([]string{"a", "b"}),
		ValueOnB: joinList([]string{"b", "a"}),
		Rank:     RankIncompatible,
	}})

	if strings.Contains(out, "2:a2:b") || strings.Contains(out, "2:b2:a") {
		t.Fatalf("the packed list form reached the report: %s", out)
	}
	if !strings.Contains(out, `"a, b"`) || !strings.Contains(out, `"b, a"`) {
		t.Fatalf("want readable column lists, got: %s", out)
	}
}

// TestReportLeavesNonListPropertiesAlone checks that displayValue only
// decodes the properties joinList actually built - a value that merely
// looks like a packed list (starts with digits and a colon) must not be
// misread as one just because its own Property is not among those.
func TestReportLeavesNonListPropertiesAlone(t *testing.T) {
	out := FormatDivergences([]Divergence{{
		Object: "public.t", Kind: "domain", Property: "check",
		NodeA: "n1", NodeB: "n2",
		ValueOnA: "CHECK (5:00:00 < start_time)", ValueOnB: "(absent)",
		Rank: RankNarrowed,
	}})

	if !strings.Contains(out, "CHECK (5:00:00 < start_time)") {
		t.Fatalf("want the CHECK text printed verbatim, got: %s", out)
	}
}

// TestDivergenceJSON_DecodesPackedListValues: the structured report must
// not leak the packed form - the same enum drift once read "sad, ok, happy"
// in text and "3:sad2:ok5:happy" in JSON.
func TestDivergenceJSON_DecodesPackedListValues(t *testing.T) {
	d := Divergence{
		Object: "public.mood", Kind: "enum", Property: "labels",
		NodeA: "n1", NodeB: "n2",
		ValueOnA: joinList([]string{"sad", "ok", "happy"}),
		ValueOnB: joinList([]string{"sad", "happy"}),
		Rank:     RankIncompatible,
	}

	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := decoded["value_on_a"]; got != "sad, ok, happy" {
		t.Errorf("value_on_a = %q, want the decoded members", got)
	}
	if got := decoded["value_on_b"]; got != "sad, happy" {
		t.Errorf("value_on_b = %q, want the decoded members", got)
	}
	if strings.Contains(string(encoded), "3:sad") {
		t.Errorf("the packed form escaped into the report: %s", encoded)
	}
}

// TestDivergenceJSON_KeyColumnsAreDecoded covers the other packed
// properties, the ones a primary-key mismatch reports.
func TestDivergenceJSON_KeyColumnsAreDecoded(t *testing.T) {
	for _, property := range []string{"key_columns", "key_opclasses"} {
		d := Divergence{
			Object: "public.orders", Kind: "key", Property: property,
			NodeA: "n1", NodeB: "n2",
			ValueOnA: joinList([]string{"id"}),
			ValueOnB: joinList([]string{"id", "tenant"}),
			Rank:     RankIncompatible,
		}
		encoded, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("%s: marshal: %v", property, err)
		}
		var decoded map[string]any
		if err := json.Unmarshal(encoded, &decoded); err != nil {
			t.Fatalf("%s: unmarshal: %v", property, err)
		}
		if got := decoded["value_on_b"]; got != "id, tenant" {
			t.Errorf("%s: value_on_b = %q, want %q", property, got, "id, tenant")
		}
	}
}

// TestDivergenceJSON_LeavesNonListPropertiesAlone: a default expression is
// not a packed list, even when it starts with a digit and a colon.
func TestDivergenceJSON_LeavesNonListPropertiesAlone(t *testing.T) {
	d := Divergence{
		Object: "public.orders.note", Kind: "column", Property: "default",
		NodeA: "n1", NodeB: "n2",
		ValueOnA: "2:00", ValueOnB: "'x'::text",
		Rank: RankEquivalentDiffering,
	}

	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := decoded["value_on_a"]; got != "2:00" {
		t.Errorf("value_on_a = %q, want it left alone", got)
	}
	if got := decoded["value_on_b"]; got != "'x'::text" {
		t.Errorf("value_on_b = %q, want it left alone", got)
	}
}

// TestDivergenceJSON_AgreesWithTheTextReport is the invariant the others
// are instances of: one run, one description per finding.
func TestDivergenceJSON_AgreesWithTheTextReport(t *testing.T) {
	d := Divergence{
		Object: "public.orders", Kind: "key", Property: "key_columns",
		NodeA: "n1", NodeB: "n2",
		ValueOnA: joinList([]string{"id"}),
		ValueOnB: joinList([]string{"id", "tenant"}),
		Rank:     RankIncompatible,
	}

	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	text := FormatDivergences([]Divergence{d})
	for _, field := range []string{"value_on_a", "value_on_b"} {
		value, _ := decoded[field].(string)
		if !strings.Contains(text, value) {
			t.Errorf("json %s = %q does not appear in the text report:\n%s", field, value, text)
		}
	}
}

// TestDivergenceJSON_KeepsTheOtherFields checks the marshaller did not drop
// anything while swapping the two values out.
func TestDivergenceJSON_KeepsTheOtherFields(t *testing.T) {
	d := Divergence{
		Object: "public.orders.qty", Kind: "column", Property: "type",
		NodeA: "n1", NodeB: "n2", ValueOnA: "integer", ValueOnB: "bigint",
		Rank: RankNarrowed, NarrowSide: "n1", Note: "a note",
	}

	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	want := map[string]string{
		"object": "public.orders.qty", "kind": "column", "property": "type",
		"node_a": "n1", "node_b": "n2", "value_on_a": "integer", "value_on_b": "bigint",
		"rank": RankNarrowed, "narrow_side": "n1", "note": "a note",
	}
	for field, expected := range want {
		if got := decoded[field]; got != expected {
			t.Errorf("%s = %v, want %q", field, got, expected)
		}
	}
}
