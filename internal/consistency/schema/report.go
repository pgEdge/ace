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
	"fmt"
	"strings"
)

// The five exit codes for the five Ranks, most severe last. A caller
// (schema-diff --compare=structure today) picks the single worst code
// across every Divergence found and exits with that, so a script wrapping
// ace can act on "how bad" without parsing the text report. RankAbsent
// shares RankIncompatible's code: both mean replication would break, either
// because a column vanished or because two types cannot hold each other's
// values.
const (
	ExitIdentical           = 0
	ExitCosmetic            = 16
	ExitEquivalentDiffering = 32
	ExitNarrowed            = 48
	ExitIncompatible        = 64
)

var rankExitCode = map[string]int{
	RankCosmetic:            ExitCosmetic,
	RankEquivalentDiffering: ExitEquivalentDiffering,
	RankNarrowed:            ExitNarrowed,
	RankIncompatible:        ExitIncompatible,
	RankAbsent:              ExitIncompatible,
}

// WorstExitCode returns the exit code for the single most severe Rank
// present in divs, or ExitIdentical if divs is empty. An unrecognised Rank
// is treated as ExitIncompatible, so a bug that invents a new Rank string
// fails loudly.
func WorstExitCode(divs []Divergence) int {
	worst := ExitIdentical
	for _, d := range divs {
		code, ok := rankExitCode[d.Rank]
		if !ok {
			code = ExitIncompatible
		}
		if code > worst {
			worst = code
		}
	}
	return worst
}

// FormatDivergences renders divs as a person-facing list of findings, one
// line per Divergence (plus an optional indented Note line), in whatever
// order divs was given - callers that want a stable order should sort
// first. It does not print anything when divs is empty; callers own the
// "no differences" message, since what that should say varies (a node
// pair's own report vs. a single mismatched table's error).
func FormatDivergences(divs []Divergence) string {
	var b strings.Builder
	for _, d := range divs {
		valueOnA, valueOnB := displayValue(d.Property, d.ValueOnA), displayValue(d.Property, d.ValueOnB)
		if d.Property != "" {
			fmt.Fprintf(&b, "  - %s.%s: on %s = %q, on %s = %q [%s]\n",
				d.Object, d.Property, d.NodeA, valueOnA, d.NodeB, valueOnB, rankText(d))
		} else {
			fmt.Fprintf(&b, "  - %s (%s): on %s = %q, on %s = %q [%s]\n",
				d.Object, d.Kind, d.NodeA, valueOnA, d.NodeB, valueOnB, rankText(d))
		}
		if d.Note != "" {
			fmt.Fprintf(&b, "    (%s)\n", d.Note)
		}
	}
	return strings.TrimRight(b.String(), "\n")
}

// listProperties are the Property names whose ValueOnA/ValueOnB were built
// by joinList (see collect.go), so FormatDivergences must decode them back
// into their members before printing — %q on the packed form would print a
// key of columns (a, b) as "2:a2:b", not "a, b". Restricted to these known
// properties rather than decoding whatever value happens to parse: a
// property that owes its shape to something else (a CHECK definition, a
// default expression) is never run through this decoding, so it cannot be
// misread as a packed list just because it happens to contain a colon and
// a leading digit.
var listProperties = map[string]bool{
	"labels":        true,
	"key_columns":   true,
	"key_opclasses": true,
}

// displayValue renders one compared value for a person, decoding it first
// when Property says it is one joinList packed. Comparison itself never
// goes through this decoding — it works on the packed form directly.
func displayValue(property, value string) string {
	if !listProperties[property] {
		return value
	}
	values, ok := splitList(value)
	if !ok {
		return value
	}
	return strings.Join(values, ", ")
}

// rankText renders a Divergence's Rank, and for RankNarrowed names the node
// whose side is the narrow one (Divergence.NarrowSide) — the useful half of
// the finding, since "narrowed" alone does not say which node is about to
// reject the other's rows.
func rankText(d Divergence) string {
	if d.Rank == RankNarrowed && d.NarrowSide != "" {
		return fmt.Sprintf("%s, narrower on %s", d.Rank, d.NarrowSide)
	}
	return d.Rank
}
