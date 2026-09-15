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
