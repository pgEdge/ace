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
	"testing"
)

// snapshotBuilder assembles a Snapshot by hand, without a database, so
// these tests can exercise Compare/classifyType directly.
type snapshotBuilder struct {
	snap Snapshot
}

func newSnapshot(node string) *snapshotBuilder {
	return &snapshotBuilder{snap: Snapshot{
		Node:         node,
		Objects:      make(map[ObjectID]Object),
		TableColumns: make(map[ObjectID][]string),
	}}
}

// splitQualified is test-only sugar: it lets these tests write
// "public.orders" instead of a schema and a table argument each time, by
// cutting at the first dot. Tests that care about dotted identifiers spell
// the schema and name out through the ObjectID constructors directly.
func splitQualified(qualified string) (schema, name string) {
	schema, name, found := strings.Cut(qualified, ".")
	if !found {
		return "", qualified
	}
	return schema, name
}

func (s *snapshotBuilder) table(qualified string, props ...Property) *snapshotBuilder {
	sortProperties(props)
	schema, table := splitQualified(qualified)
	s.snap.Objects[TableID(schema, table)] = Object{Kind: "table", Name: qualified, Properties: props}
	return s
}

func (s *snapshotBuilder) column(qualified, name string, props ...Property) *snapshotBuilder {
	sortProperties(props)
	schema, table := splitQualified(qualified)
	colQualified := qualified + "." + name
	s.snap.Objects[ColumnID(schema, table, name)] = Object{Kind: "column", Name: colQualified, Properties: props}
	s.snap.TableColumns[TableID(schema, table)] = append(s.snap.TableColumns[TableID(schema, table)], name)
	return s
}

func (s *snapshotBuilder) key(qualified string, props ...Property) *snapshotBuilder {
	sortProperties(props)
	schema, table := splitQualified(qualified)
	s.snap.Objects[KeyID(schema, table)] = Object{Kind: "key", Name: qualified, Properties: props}
	return s
}

func (s *snapshotBuilder) constraints(qualified string, defs ...string) *snapshotBuilder {
	var props []Property
	for _, d := range defs {
		props = append(props, Property{Name: "constraint", Value: d})
	}
	sortProperties(props)
	schema, table := splitQualified(qualified)
	s.snap.Objects[ConstraintID(schema, table)] = Object{Kind: "constraint", Name: qualified, Properties: props}
	return s
}

// domain/rng/composite/enum mirror the corresponding Kind collectReferencedTypes
// builds from a live catalog, for tests exercising a referenced type's own
// definition rather than a column's declared type.
func (s *snapshotBuilder) domain(qualified string, props ...Property) *snapshotBuilder {
	sortProperties(props)
	schema, name := splitQualified(qualified)
	s.snap.Objects[TypeID("domain", schema, name)] = Object{Kind: "domain", Name: qualified, Properties: props}
	return s
}

func (s *snapshotBuilder) rng(qualified string, props ...Property) *snapshotBuilder {
	sortProperties(props)
	schema, name := splitQualified(qualified)
	s.snap.Objects[TypeID("range", schema, name)] = Object{Kind: "range", Name: qualified, Properties: props}
	return s
}

func (s *snapshotBuilder) composite(qualified string, props ...Property) *snapshotBuilder {
	// Not sorted: attribute order is part of a composite type's identity,
	// and callers pass props already in the order they want compared.
	schema, name := splitQualified(qualified)
	s.snap.Objects[TypeID("composite", schema, name)] = Object{Kind: "composite", Name: qualified, Properties: props}
	return s
}

func (s *snapshotBuilder) enum(qualified, labels string) *snapshotBuilder {
	schema, name := splitQualified(qualified)
	s.snap.Objects[TypeID("enum", schema, name)] = Object{
		Kind: "enum", Name: qualified,
		Properties: []Property{{Name: "labels", Value: labels}},
	}
	return s
}

// locale mirrors the database-wide Object CollectSnapshot builds from
// pg_database, for tests about the collation two nodes inherited rather
// than about anything in the compared schema.
func (s *snapshotBuilder) locale(name, collate, ctype, provider, locale string) *snapshotBuilder {
	props := []Property{
		{Name: "lc_collate", Value: collate},
		{Name: "lc_ctype", Value: ctype},
		{Name: "locale_provider", Value: provider},
		{Name: "locale", Value: locale},
	}
	sortProperties(props)
	s.snap.Objects[DatabaseID()] = Object{Kind: "database", Name: name, Properties: props}
	return s
}

func (s *snapshotBuilder) build() Snapshot { return s.snap }

// userTypeColumnProps is baseColumnProps for a column whose declared type is
// some referenced type object (domain/range/composite/enum), not a plain
// base type — type_namespace/type_name naming that type itself rather than
// pg_catalog.int4, and kind carrying pg_type.typtype ('d'/'r'/'c'/'e').
// These tests are about the referenced type's own definition, exercised by
// compareReferencedTypes, not about the column, so this only needs to make
// both sides agree at the column level.
func userTypeColumnProps(namespace, name, kind string) []Property {
	return []Property{
		{Name: "type", Value: name},
		{Name: "type_namespace", Value: namespace},
		{Name: "type_name", Value: name},
		{Name: "type_kind", Value: kind},
		{Name: "type_mod", Value: "-1"},
		{Name: "notnull", Value: "false"},
		{Name: "identity", Value: ""},
		{Name: "generated", Value: ""},
		{Name: "options", Value: ""},
		{Name: "collation", Value: ""},
		{Name: "default", Value: ""},
	}
}

// baseColumnProps returns a plausible, complete property set for a column so
// tests only need to override the one property under test.
func baseColumnProps(overrides ...Property) []Property {
	props := []Property{
		{Name: "type", Value: "integer"},
		{Name: "type_namespace", Value: "pg_catalog"},
		{Name: "type_name", Value: "int4"},
		{Name: "type_kind", Value: "b"},
		{Name: "type_mod", Value: "-1"},
		{Name: "notnull", Value: "false"},
		{Name: "identity", Value: ""},
		{Name: "generated", Value: ""},
		{Name: "options", Value: ""},
		{Name: "collation", Value: ""},
		{Name: "default", Value: ""},
	}
	for _, o := range overrides {
		for i := range props {
			if props[i].Name == o.Name {
				props[i].Value = o.Value
			}
		}
	}
	return props
}

func findDivergence(t *testing.T, divs []Divergence, object, property string) Divergence {
	t.Helper()
	for _, d := range divs {
		if d.Object == object && d.Property == property {
			return d
		}
	}
	t.Fatalf("no divergence found for object=%q property=%q in %+v", object, property, divs)
	return Divergence{}
}

// TestCompare_MissingColumnIsAbsent: a column dropped on one node only.
func TestCompare_MissingColumnIsAbsent(t *testing.T) {
	table := "public.orders"
	a := newSnapshot("n1").table(table).
		column(table, "id", baseColumnProps()...).
		column(table, "discount", baseColumnProps()...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "id", baseColumnProps()...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"orders"}, a, b)

	d := findDivergence(t, divs, table+".discount", "")
	if d.Rank != RankAbsent {
		t.Fatalf("want RankAbsent, got %q", d.Rank)
	}
	if d.ValueOnA != "present" || d.ValueOnB != "absent" {
		t.Fatalf("want presence present/absent, got %q/%q", d.ValueOnA, d.ValueOnB)
	}
}

// TestCompare_IntWideningIsNarrowedOnTheSmallerSide: int8 on n1, int4 on n2
// — n2 is the narrow side.
func TestCompare_IntWideningIsNarrowedOnTheSmallerSide(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).
		column(table, "id", baseColumnProps(Property{Name: "type", Value: "bigint"}, Property{Name: "type_name", Value: "int8"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "id", baseColumnProps(Property{Name: "type", Value: "integer"}, Property{Name: "type_name", Value: "int4"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"customers"}, a, b)

	d := findDivergence(t, divs, table+".id", "type")
	if d.Rank != RankNarrowed {
		t.Fatalf("want RankNarrowed, got %q", d.Rank)
	}
	if d.NarrowSide != "n2" {
		t.Fatalf("want narrow side n2 (int4), got %q", d.NarrowSide)
	}
}

// TestCompare_TextVsIntegerIsIncompatible: no widening relation applies.
func TestCompare_TextVsIntegerIsIncompatible(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		column(table, "x", baseColumnProps(Property{Name: "type", Value: "text"}, Property{Name: "type_name", Value: "text"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "x", baseColumnProps(Property{Name: "type", Value: "integer"}, Property{Name: "type_name", Value: "int4"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, table+".x", "type")
	if d.Rank != RankIncompatible {
		t.Fatalf("want RankIncompatible, got %q", d.Rank)
	}
}

// TestCompare_BpcharVsVarcharSameLengthIsEquivalentDiffering: char(n) and
// varchar(n) accept the same strings at the same declared length, but
// bpchar blank-pads a shorter value out to n characters and varchar does
// not, so the two nodes do not actually store or compare the same values -
// this is RankEquivalentDiffering, not "no difference".
func TestCompare_BpcharVsVarcharSameLengthIsEquivalentDiffering(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		// char(5): PostgreSQL stores the declared length as atttypmod-4.
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "bpchar"}, Property{Name: "type_mod", Value: "9"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "varchar"}, Property{Name: "type_mod", Value: "9"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, table+".x", "type")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_BpcharVsUnboundedTextIsEquivalentDiffering: the same holds
// even when neither side is length-limited - bare "char" (unbounded, like
// text) against text itself. It is bpchar's padding behaviour that
// differs, not a length mismatch, so the unbounded case is not exempt.
func TestCompare_BpcharVsUnboundedTextIsEquivalentDiffering(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		// type_mod stays at baseColumnProps' default of -1: unbounded.
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "bpchar"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "text"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, table+".x", "type")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_BpcharShorterThanVarcharIsNarrowed: char(5) accepts strictly
// fewer strings than varchar(10), so the difference in declared length must
// still be reported as a narrowing, not folded into the padding-only
// RankEquivalentDiffering case that applies when the declared lengths match.
func TestCompare_BpcharShorterThanVarcharIsNarrowed(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		// char(5): PostgreSQL stores the declared length as atttypmod-4.
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "bpchar"}, Property{Name: "type_mod", Value: "9"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		// varchar(10).
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "varchar"}, Property{Name: "type_mod", Value: "14"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, table+".x", "type")
	if d.Rank != RankNarrowed {
		t.Fatalf("want RankNarrowed, got %q", d.Rank)
	}
	if d.NarrowSide != "n1" {
		t.Fatalf("want n1 (char(5)) as the narrow side, got %q", d.NarrowSide)
	}
}

// TestCompare_BpcharBoundedVsUnboundedTextIsNarrowed: char(5) accepts
// strictly fewer strings than unbounded text, so a bounded bpchar against an
// unbounded text-like column is a narrowing, not the padding-only
// equivalence that applies when both sides are unbounded.
func TestCompare_BpcharBoundedVsUnboundedTextIsNarrowed(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		// char(5): PostgreSQL stores the declared length as atttypmod-4.
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "bpchar"}, Property{Name: "type_mod", Value: "9"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		// type_mod stays at baseColumnProps' default of -1: unbounded.
		column(table, "x", baseColumnProps(Property{Name: "type_name", Value: "text"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, table+".x", "type")
	if d.Rank != RankNarrowed {
		t.Fatalf("want RankNarrowed, got %q", d.Rank)
	}
	if d.NarrowSide != "n1" {
		t.Fatalf("want n1 (char(5)) as the narrow side, got %q", d.NarrowSide)
	}
}

// TestCompare_TimestampVsTimestamptzIsEquivalentDiffering: same values fit,
// meaning differs.
func TestCompare_TimestampVsTimestamptzIsEquivalentDiffering(t *testing.T) {
	table := "public.events"
	a := newSnapshot("n1").table(table).
		column(table, "at", baseColumnProps(Property{Name: "type", Value: "timestamp with time zone"}, Property{Name: "type_name", Value: "timestamptz"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "at", baseColumnProps(Property{Name: "type", Value: "timestamp without time zone"}, Property{Name: "type_name", Value: "timestamp"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"events"}, a, b)

	d := findDivergence(t, divs, table+".at", "type")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_TimestampPairWithEqualPrecisionIsEquivalentDiffering: the pair
// above, but with a precision named on both sides. Equal precision means the
// same values still fit, so only the time zone meaning differs.
func TestCompare_TimestampPairWithEqualPrecisionIsEquivalentDiffering(t *testing.T) {
	table := "public.events"
	a := newSnapshot("n1").table(table).
		column(table, "at", baseColumnProps(
			Property{Name: "type_name", Value: "timestamptz"},
			Property{Name: "type_mod", Value: "3"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "at", baseColumnProps(
			Property{Name: "type_name", Value: "timestamp"},
			Property{Name: "type_mod", Value: "3"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"events"}, a, b)

	d := findDivergence(t, divs, table+".at", "type")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_TimestampPairWithDifferentPrecisionIsIncompatible: for these
// types atttypmod is the fractional-second precision itself, and a lower
// precision rounds the value away - timestamp(3) stores .123456 as .123. So
// the two sides do not hold the same values, which is what
// equivalent-differing claims, and the pair must not be ranked as if they
// did. Two columns of the same name and different precision already fall
// through to incompatible; this keeps the cross-name pair consistent with
// that, instead of giving the pair with more differences the milder rank.
func TestCompare_TimestampPairWithDifferentPrecisionIsIncompatible(t *testing.T) {
	table := "public.events"
	a := newSnapshot("n1").table(table).
		column(table, "at", baseColumnProps(
			Property{Name: "type_name", Value: "timestamptz"},
			Property{Name: "type_mod", Value: "3"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "at", baseColumnProps(
			Property{Name: "type_name", Value: "timestamp"},
			Property{Name: "type_mod", Value: "6"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"events"}, a, b)

	d := findDivergence(t, divs, table+".at", "type")
	if d.Rank != RankIncompatible {
		t.Fatalf("want RankIncompatible, got %q", d.Rank)
	}
}

// TestCompare_CollationVersionDifferenceIsEquivalentDiffering.
func TestCompare_CollationVersionDifferenceIsEquivalentDiffering(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).
		column(table, "email", baseColumnProps(
			Property{Name: "type", Value: "text"}, Property{Name: "type_name", Value: "text"},
			Property{Name: "collation", Value: "en_US.utf8/c/2.36"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "email", baseColumnProps(
			Property{Name: "type", Value: "text"}, Property{Name: "type_name", Value: "text"},
			Property{Name: "collation", Value: "en_US.utf8/c/2.31"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"customers"}, a, b)

	d := findDivergence(t, divs, table+".email", "collation")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_DeltaApplyOptionDifferenceIsEquivalentDiffering: a Spock
// delta-apply column on one node, an ordinary column on the other. This
// package does not know what "delta-apply" means; it only compares the raw
// attoptions text.
func TestCompare_DeltaApplyOptionDifferenceIsEquivalentDiffering(t *testing.T) {
	table := "public.accounts"
	a := newSnapshot("n1").table(table).
		column(table, "balance", baseColumnProps(
			Property{Name: "type", Value: "numeric"}, Property{Name: "type_name", Value: "numeric"},
			Property{Name: "options", Value: "{log_old_value=true,delta_apply_function=spock.delta_apply}"})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "balance", baseColumnProps(
			Property{Name: "type", Value: "numeric"}, Property{Name: "type_name", Value: "numeric"})...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"accounts"}, a, b)

	d := findDivergence(t, divs, table+".balance", "options")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_KeyOpclassDifferenceIsEquivalentDiffering.
func TestCompare_KeyOpclassDifferenceIsEquivalentDiffering(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).
		key(table, Property{Name: "replica_identity", Value: "i"},
			Property{Name: "key_columns", Value: "email"},
			Property{Name: "key_opclasses", Value: "text_ops"}).
		constraints(table).build()
	b := newSnapshot("n2").table(table).
		key(table, Property{Name: "replica_identity", Value: "i"},
			Property{Name: "key_columns", Value: "email"},
			Property{Name: "key_opclasses", Value: "text_pattern_ops"}).
		constraints(table).build()

	divs := Compare("public", []string{"customers"}, a, b)

	d := findDivergence(t, divs, table, "key_opclasses")
	if d.Rank != RankEquivalentDiffering {
		t.Fatalf("want RankEquivalentDiffering, got %q", d.Rank)
	}
}

// TestCompare_ReplicaIdentityMismatchIsIncompatible: PRIMARY KEY vs FULL,
// where conflicts would resolve differently on each side.
func TestCompare_ReplicaIdentityMismatchIsIncompatible(t *testing.T) {
	table := "public.orders"
	a := newSnapshot("n1").table(table).
		key(table, Property{Name: "replica_identity", Value: "d"}).
		constraints(table).build()
	b := newSnapshot("n2").table(table).
		key(table, Property{Name: "replica_identity", Value: "f"}).
		constraints(table).build()

	divs := Compare("public", []string{"orders"}, a, b)

	d := findDivergence(t, divs, table, "replica_identity")
	if d.Rank != RankIncompatible {
		t.Fatalf("want RankIncompatible, got %q", d.Rank)
	}
}

// TestCompare_ConstraintNamesDifferDefinitionsMatch_NoDivergence checks that
// matching two constraints goes by definition text, never by PostgreSQL's
// invented names, or every pair of independently-created databases would
// report false constraint differences.
func TestCompare_ConstraintNamesDifferDefinitionsMatch_NoDivergence(t *testing.T) {
	table := "public.customers"
	// Same constraint, different auto-generated names — GetConstraintDescriptors
	// never reads conname, so both snapshots record only the definition text.
	a := newSnapshot("n1").table(table).key(table).
		constraints(table, "u|deferrable=false|validated=true|UNIQUE (email)").build()
	b := newSnapshot("n2").table(table).key(table).
		constraints(table, "u|deferrable=false|validated=true|UNIQUE (email)").build()

	divs := Compare("public", []string{"customers"}, a, b)
	if len(divs) != 0 {
		t.Fatalf("want no divergences for identical constraint definitions, got %+v", divs)
	}
}

// TestCompare_ExtraCheckConstraintIsNarrowedNotAbsent checks that a CHECK
// only one node has is ranked as a narrowing of that node, the same way
// compareDomainChecks ranks a domain's one-sided CHECK: the node holding it
// accepts strictly fewer rows. Ranking it RankAbsent instead would give the
// same change two different exit codes depending on whether the CHECK sat
// on a table or on a domain.
func TestCompare_ExtraCheckConstraintIsNarrowedNotAbsent(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).key(table).
		constraints(table, "c|deferrable=false|validated=true|CHECK (age >= 0)").build()
	b := newSnapshot("n2").table(table).key(table).constraints(table).build()

	divs := Compare("public", []string{"customers"}, a, b)
	if len(divs) != 1 {
		t.Fatalf("want exactly one divergence, got %d: %+v", len(divs), divs)
	}
	d := divs[0]
	if d.Kind != "constraint" || d.ValueOnB != "(absent)" {
		t.Fatalf("expected the extra constraint on n1 to be reported, got %+v", d)
	}
	if d.Rank != RankNarrowed || d.NarrowSide != "n1" {
		t.Fatalf("want narrowed on n1, got %q on %q", d.Rank, d.NarrowSide)
	}
}

// TestCompare_ExtraNonCheckConstraintStaysAbsent checks that the narrowing
// reasoning above is confined to CHECKs. A UNIQUE or FOREIGN KEY one node
// lacks does not make the other node's accepted values a subset of it —
// the two disagree about a constraint, which is RankAbsent.
func TestCompare_ExtraNonCheckConstraintStaysAbsent(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).key(table).
		constraints(table, "u|deferrable=false|validated=true|UNIQUE (email)").build()
	b := newSnapshot("n2").table(table).key(table).constraints(table).build()

	divs := Compare("public", []string{"customers"}, a, b)
	if len(divs) != 1 {
		t.Fatalf("want exactly one divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Rank != RankAbsent {
		t.Fatalf("want absent for a one-sided UNIQUE, got %q", divs[0].Rank)
	}
}

// TestCompare_CheckOnEachSideIsNotNarrowed checks that when each node has a
// CHECK the other lacks, neither is called the narrow side: neither value
// set contains the other, so both findings stay RankAbsent.
func TestCompare_CheckOnEachSideIsNotNarrowed(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).key(table).
		constraints(table, "c|deferrable=false|validated=true|CHECK (age >= 0)").build()
	b := newSnapshot("n2").table(table).key(table).
		constraints(table, "c|deferrable=false|validated=true|CHECK (age >= 18)").build()

	divs := Compare("public", []string{"customers"}, a, b)
	if len(divs) != 2 {
		t.Fatalf("want two divergences, got %d: %+v", len(divs), divs)
	}
	for _, d := range divs {
		if d.Rank != RankAbsent {
			t.Fatalf("want absent when each side has its own CHECK, got %q: %+v", d.Rank, d)
		}
	}
}

// TestCompare_ExtraCheckAgainstExtraNonCheckIsNotNarrowed checks the case
// between the two above: one node has an extra CHECK and the other an extra
// FOREIGN KEY. The CHECK alone would make n1 the narrow side, but n2 holds a
// constraint of its own that n1 does not, so n2 rejects rows n1 accepts.
// Neither set of accepted rows contains the other, and neither finding may
// be called a narrowing.
func TestCompare_ExtraCheckAgainstExtraNonCheckIsNotNarrowed(t *testing.T) {
	table := "public.customers"
	a := newSnapshot("n1").table(table).key(table).
		constraints(table, "c|deferrable=false|validated=true|CHECK (age >= 0)").build()
	b := newSnapshot("n2").table(table).key(table).
		constraints(table, "f|deferrable=false|validated=true|FOREIGN KEY (org_id) REFERENCES orgs(id)").build()

	divs := Compare("public", []string{"customers"}, a, b)
	if len(divs) != 2 {
		t.Fatalf("want two divergences, got %d: %+v", len(divs), divs)
	}
	for _, d := range divs {
		if d.Rank != RankAbsent {
			t.Fatalf("want absent when each side has an extra constraint, got %q: %+v", d.Rank, d)
		}
		if d.NarrowSide != "" {
			t.Fatalf("no side may be called narrow here, got %q: %+v", d.NarrowSide, d)
		}
	}
}

// TestCompare_DateAgainstTimestampIsIncompatible checks that date is not
// treated as a narrower timestamp. It looks like one, but neither value set
// contains the other: a timestamp carries a time of day that a date cannot
// hold, and date reaches 5874897 AD while the timestamp types stop at
// 294276 AD - PostgreSQL itself refuses '5874897-12-31'::date::timestamp
// with "date out of range for timestamp".
func TestCompare_DateAgainstTimestampIsIncompatible(t *testing.T) {
	for _, other := range []string{"timestamp", "timestamptz"} {
		table := "public.events"
		a := newSnapshot("n1").table(table).
			column(table, "at", baseColumnProps(Property{Name: "type_name", Value: "date"})...).
			key(table).constraints(table).build()
		b := newSnapshot("n2").table(table).
			column(table, "at", baseColumnProps(Property{Name: "type_name", Value: other})...).
			key(table).constraints(table).build()

		divs := Compare("public", []string{"events"}, a, b)

		d := findDivergence(t, divs, table+".at", "type")
		if d.Rank != RankIncompatible {
			t.Fatalf("date against %s: want RankIncompatible, got %q", other, d.Rank)
		}
		if d.NarrowSide != "" {
			t.Fatalf("date against %s: no side is the narrow one, got %q", other, d.NarrowSide)
		}
	}
}

// timestampColumn builds the two snapshots the timestamp precision tests
// need: one column of the given type name and modifier on each node.
func timestampColumn(t *testing.T, aName, aMod, bName, bMod string) []Divergence {
	t.Helper()
	table := "public.events"
	a := newSnapshot("n1").table(table).
		column(table, "at", baseColumnProps(
			Property{Name: "type_name", Value: aName},
			Property{Name: "type_mod", Value: aMod})...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "at", baseColumnProps(
			Property{Name: "type_name", Value: bName},
			Property{Name: "type_mod", Value: bMod})...).
		key(table).constraints(table).build()
	return Compare("public", []string{"events"}, a, b)
}

// TestCompare_ImplicitAndExplicitTimestampPrecisionAreOneType: an
// unspecified modifier is stored as -1 and means the type's full precision,
// which for both timestamp types is 6. So "timestamp" and "timestamp(6)" are
// one type written two ways, and there is nothing to report.
func TestCompare_ImplicitAndExplicitTimestampPrecisionAreOneType(t *testing.T) {
	if divs := timestampColumn(t, "timestamp", "-1", "timestamp", "6"); len(divs) != 0 {
		t.Fatalf("timestamp against timestamp(6) is the same type, got %+v", divs)
	}
	if divs := timestampColumn(t, "timestamptz", "6", "timestamptz", "-1"); len(divs) != 0 {
		t.Fatalf("timestamptz(6) against timestamptz is the same type, got %+v", divs)
	}
}

// TestCompare_LowerTimestampPrecisionIsNarrowed: at the same type name, the
// side with fewer fractional digits holds strictly fewer values - every
// timestamp(3) value is exactly representable as timestamp(6), and not the
// other way round. That is the definition of RankNarrowed, so reporting it
// as incompatible would claim the two sets are unrelated when one contains
// the other.
func TestCompare_LowerTimestampPrecisionIsNarrowed(t *testing.T) {
	cases := []struct {
		name           string
		aName, aMod    string
		bName, bMod    string
		wantNarrowSide string
	}{
		{"explicit against explicit", "timestamp", "3", "timestamp", "6", "n1"},
		{"the narrow side on n2", "timestamp", "6", "timestamp", "3", "n2"},
		{"explicit against implicit 6", "timestamp", "3", "timestamp", "-1", "n1"},
		{"timestamptz keeps its own rule", "timestamptz", "-1", "timestamptz", "0", "n2"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			divs := timestampColumn(t, c.aName, c.aMod, c.bName, c.bMod)
			d := findDivergence(t, divs, "public.events.at", "type")
			if d.Rank != RankNarrowed {
				t.Fatalf("want RankNarrowed, got %q", d.Rank)
			}
			if d.NarrowSide != c.wantNarrowSide {
				t.Fatalf("want the narrow side on %s, got %q", c.wantNarrowSide, d.NarrowSide)
			}
		})
	}
}

// TestCompare_TimestampPairAcrossNamesAtOnePrecision: the time zone
// difference on its own is still equivalent-differing, including when one
// side spells out the precision the other leaves implicit.
func TestCompare_TimestampPairAcrossNamesAtOnePrecision(t *testing.T) {
	for _, c := range [][4]string{
		{"timestamptz", "-1", "timestamp", "6"},
		{"timestamptz", "3", "timestamp", "3"},
	} {
		divs := timestampColumn(t, c[0], c[1], c[2], c[3])
		d := findDivergence(t, divs, "public.events.at", "type")
		if d.Rank != RankEquivalentDiffering {
			t.Fatalf("%s(%s) against %s(%s): want RankEquivalentDiffering, got %q",
				c[0], c[1], c[2], c[3], d.Rank)
		}
	}
}

// TestCompare_ColumnOrderDoesNotMatter checks that building two snapshots
// with columns added in different orders produces no divergence, since
// attnum is never collected.
func TestCompare_ColumnOrderDoesNotMatter(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		column(table, "a", baseColumnProps()...).
		column(table, "b", baseColumnProps()...).
		key(table).constraints(table).build()
	b := newSnapshot("n2").table(table).
		column(table, "b", baseColumnProps()...).
		column(table, "a", baseColumnProps()...).
		key(table).constraints(table).build()

	divs := Compare("public", []string{"t"}, a, b)
	if len(divs) != 0 {
		t.Fatalf("want no divergences when only column insertion order differs, got %+v", divs)
	}
}

// TestCompare_SameDomainDifferentLocalDetailsOnlyNoDivergence is a
// regression test: two nodes' own OIDs for a domain never reach this
// package (ColumnDescriptor.TypeOID stays node-local — see collect.go), so
// two byte-identical domains must compare as identical regardless of what
// OID a pg_dump/restore or an independent CREATE DOMAIN happened to assign
// on each side.
func TestCompare_SameDomainDifferentLocalDetailsOnlyNoDivergence(t *testing.T) {
	table := "public.city"
	domainProps := func() []Property {
		return []Property{
			{Name: "basetype_namespace", Value: "pg_catalog"},
			{Name: "basetype_name", Value: "numeric"},
			{Name: "basetypmod", Value: "-1"},
			{Name: "notnull", Value: "false"},
			{Name: "default", Value: ""},
		}
	}
	a := newSnapshot("n1").table(table).
		column(table, "budget", userTypeColumnProps("public", "city_budget", "d")...).
		key(table).constraints(table).
		domain("public.city_budget", domainProps()...).build()
	b := newSnapshot("n2").table(table).
		column(table, "budget", userTypeColumnProps("public", "city_budget", "d")...).
		key(table).constraints(table).
		domain("public.city_budget", domainProps()...).build()

	divs := Compare("public", []string{"city"}, a, b)
	if len(divs) != 0 {
		t.Fatalf("want no divergences for an identical domain reached via two different OIDs, got %+v", divs)
	}
}

// TestCompare_DomainCheckDroppedOnOneSideIsNarrowed covers two freshly
// built nodes that happen to assign a domain the same OID, then
// `ALTER DOMAIN pos DROP CONSTRAINT pos_check` runs on only one of them.
// This must be caught by descending into the domain's own CHECK set the
// same way table constraints are compared.
func TestCompare_DomainCheckDroppedOnOneSideIsNarrowed(t *testing.T) {
	table := "public.readings"
	base := func() []Property {
		return []Property{
			{Name: "basetype_namespace", Value: "pg_catalog"},
			{Name: "basetype_name", Value: "int4"},
			{Name: "basetypmod", Value: "-1"},
			{Name: "notnull", Value: "false"},
			{Name: "default", Value: ""},
		}
	}
	a := newSnapshot("n1").table(table).
		column(table, "pos", userTypeColumnProps("public", "pos", "d")...).
		key(table).constraints(table).
		domain("public.pos", append(base(), Property{Name: "check", Value: "CHECK (VALUE >= 0)"})...).build()
	b := newSnapshot("n2").table(table).
		column(table, "pos", userTypeColumnProps("public", "pos", "d")...).
		key(table).constraints(table).
		domain("public.pos", base()...).build()

	divs := Compare("public", []string{"readings"}, a, b)

	d := findDivergence(t, divs, "public.pos", "check")
	if d.Rank != RankNarrowed {
		t.Fatalf("want RankNarrowed, got %q", d.Rank)
	}
	if d.NarrowSide != "n1" {
		t.Fatalf("want n1 (the side that kept the stricter CHECK) as the narrow side, got %q", d.NarrowSide)
	}
}

// TestCompare_DomainBaseTypeNarrowedUsesClassifyType: a domain over
// varchar(20) on one side, the same domain name over varchar(10) on the
// other — the base-type change is classified with classifyType, the same
// as a column's declared type, not just flagged incompatible.
func TestCompare_DomainBaseTypeNarrowedUsesClassifyType(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		column(table, "code", userTypeColumnProps("public", "code_t", "d")...).
		key(table).constraints(table).
		domain("public.code_t",
			Property{Name: "basetype_namespace", Value: "pg_catalog"},
			Property{Name: "basetype_name", Value: "varchar"},
			Property{Name: "basetypmod", Value: "24"}, // varchar(20): atttypmod = declared length + 4
			Property{Name: "notnull", Value: "false"}, Property{Name: "default", Value: ""}).build()
	b := newSnapshot("n2").table(table).
		column(table, "code", userTypeColumnProps("public", "code_t", "d")...).
		key(table).constraints(table).
		domain("public.code_t",
			Property{Name: "basetype_namespace", Value: "pg_catalog"},
			Property{Name: "basetype_name", Value: "varchar"},
			Property{Name: "basetypmod", Value: "14"}, // varchar(10)
			Property{Name: "notnull", Value: "false"}, Property{Name: "default", Value: ""}).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, "public.code_t", "basetype")
	if d.Rank != RankNarrowed {
		t.Fatalf("want RankNarrowed, got %q", d.Rank)
	}
	if d.NarrowSide != "n2" {
		t.Fatalf("want n2 (varchar(10), the shorter side) as the narrow side, got %q", d.NarrowSide)
	}
}

// TestCompare_EnumLabelAddedIsIncompatible covers `ALTER TYPE mood ADD
// VALUE 'furious'` run on only one node.
func TestCompare_EnumLabelAddedIsIncompatible(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		column(table, "m", userTypeColumnProps("public", "mood", "e")...).
		key(table).constraints(table).
		enum("public.mood", "happy,sad").build()
	b := newSnapshot("n2").table(table).
		column(table, "m", userTypeColumnProps("public", "mood", "e")...).
		key(table).constraints(table).
		enum("public.mood", "happy,sad,furious").build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, "public.mood", "labels")
	if d.Rank != RankIncompatible {
		t.Fatalf("want RankIncompatible, got %q", d.Rank)
	}
}

// TestCompare_CompositeAttributeTypeChangedIsReported: a composite type's
// own attribute changed type on one side. Attribute order is what
// Property.Name's "attr:%04d:name" index encodes, so two attributes
// swapping declaration order is itself a real difference for a composite
// type.
func TestCompare_CompositeAttributeTypeChangedIsReported(t *testing.T) {
	table := "public.t"
	// Built through packAttr, the same as collect.go itself does, instead
	// of a hand-written string: a value that does not come from packAttr
	// fails unpackAttr and only exercises classifyAttr's "cannot be
	// unpacked" fallback, never the real int4-vs-int8 narrowing path this
	// test means to cover. f1 and f2 get distinct ordinals (1 and 2, as
	// two attributes of one composite type would), even though only f1
	// changes here.
	aInt4 := packAttr("pg_catalog", "int4", "b", -1, "")
	bInt8 := packAttr("pg_catalog", "int8", "b", -1, "")
	text := packAttr("pg_catalog", "text", "b", -1, "")
	a := newSnapshot("n1").table(table).
		column(table, "thing", userTypeColumnProps("public", "things", "c")...).
		key(table).constraints(table).
		composite("public.things",
			Property{Name: "attr:0001:f1", Value: aInt4},
			Property{Name: "attr:0002:f2", Value: text}).build()
	b := newSnapshot("n2").table(table).
		column(table, "thing", userTypeColumnProps("public", "things", "c")...).
		key(table).constraints(table).
		composite("public.things",
			Property{Name: "attr:0001:f1", Value: bInt8},
			Property{Name: "attr:0002:f2", Value: text}).build()

	divs := Compare("public", []string{"t"}, a, b)

	// The property is named for the reader — "attribute 1 (f1)", carrying
	// the attribute's own attnum — while the
	// index in the underlying "attr:%04d:name" key is what kept the
	// comparison in declaration order.
	d := findDivergence(t, divs, "public.things", "attribute 1 (f1)")
	// int4 -> int8 narrows exactly as it does for a table column (see
	// classifyAttr's own doc comment): n1's int4 accepts a strict subset
	// of what n2's int8 does.
	if d.Rank != RankNarrowed {
		t.Fatalf("want RankNarrowed, got %q", d.Rank)
	}
	if d.NarrowSide != "n1" {
		t.Fatalf("want n1 as the narrow side, got %q", d.NarrowSide)
	}
	if d.ValueOnA != aInt4 || d.ValueOnB != bInt8 {
		t.Fatalf("expected the raw packed attribute values to be shown when no display text was collected, got %q / %q",
			d.ValueOnA, d.ValueOnB)
	}
}

// TestPackAttrRoundTripsArbitraryCollation checks that packAttr/unpackAttr
// round-trip regardless of what the collation field contains. A collation
// name is a quoted identifier here (see collect.go's own collation string,
// "namespace.name/provider/version"), so it can contain punctuation a
// delimiter-based pack would need to reserve for itself.
func TestPackAttrRoundTripsArbitraryCollation(t *testing.T) {
	cases := []string{
		"",
		"public.\"und-x-icu\"/i/153.94",
		"3:not-a-length-prefix",
		"trailing\x1fseparator",
	}
	for _, collation := range cases {
		packed := packAttr("pg_catalog", "text", "b", 20, collation)
		ns, name, kind, mod, coll, ok := unpackAttr(packed)
		if !ok {
			t.Errorf("unpackAttr could not decode packAttr(..., %q)", collation)
			continue
		}
		if ns != "pg_catalog" || name != "text" || kind != "b" || mod != 20 || coll != collation {
			t.Errorf("packAttr(..., %q) round-tripped to (%q, %q, %q, %d, %q)",
				collation, ns, name, kind, mod, coll)
		}
	}
}

// TestCompare_RangeSubtypeChangedIsIncompatible: a range type's subtype
// itself differs between nodes.
func TestCompare_RangeSubtypeChangedIsIncompatible(t *testing.T) {
	table := "public.t"
	a := newSnapshot("n1").table(table).
		column(table, "r", userTypeColumnProps("public", "myrange", "r")...).
		key(table).constraints(table).
		rng("public.myrange",
			Property{Name: "subtype", Value: "pg_catalog.text"},
			Property{Name: "collation", Value: "default/c/2.36"},
			Property{Name: "opclass", Value: "text_ops"},
			Property{Name: "canonical", Value: ""}, Property{Name: "subtype_diff", Value: ""}).build()
	b := newSnapshot("n2").table(table).
		column(table, "r", userTypeColumnProps("public", "myrange", "r")...).
		key(table).constraints(table).
		rng("public.myrange",
			Property{Name: "subtype", Value: "pg_catalog.varchar"},
			Property{Name: "collation", Value: "default/c/2.36"},
			Property{Name: "opclass", Value: "text_ops"},
			Property{Name: "canonical", Value: ""}, Property{Name: "subtype_diff", Value: ""}).build()

	divs := Compare("public", []string{"t"}, a, b)

	d := findDivergence(t, divs, "public.myrange", "subtype")
	if d.Rank != RankIncompatible {
		t.Fatalf("want RankIncompatible, got %q", d.Rank)
	}
}

// TestDottedIdentifiersDoNotCollide is the regression test for the key
// ambiguity ObjectID removed: table "a.b" with a column "c", and table "a"
// with a column "b.c", must not collide on the same key.
func TestDottedIdentifiersDoNotCollide(t *testing.T) {
	// typed spells out a column of the given built-in type, printing the
	// type name as its own display text so the assertions below can tell
	// the two colliding candidates apart by value.
	typed := func(typeName string) []Property {
		return baseColumnProps(
			Property{Name: "type", Value: typeName},
			Property{Name: "type_name", Value: typeName},
		)
	}
	build := func(dottedTableType, dottedColumnType string) Snapshot {
		return newSnapshot("n1").
			table("public.a.b").
			column("public.a.b", "c", typed(dottedTableType)...).
			table("public.a").
			column("public.a", "b.c", typed(dottedColumnType)...).
			build()
	}

	a := build("int4", "int4")
	b := build("int4", "int8")
	b.Node = "n2"

	tables := []string{"a", "a.b"}
	divs := Compare("public", tables, a, b)

	if len(divs) != 1 {
		t.Fatalf("want exactly one divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Object != "public.a.b.c" || divs[0].Property != "type" {
		t.Fatalf("unexpected object/property: %q / %q", divs[0].Object, divs[0].Property)
	}
	// Both candidate objects print as "public.a.b.c" — the display name is
	// ambiguous by nature. What must not be ambiguous is which one was
	// compared, so check the values: only table "a"'s column changed.
	if divs[0].ValueOnA != "int4" || divs[0].ValueOnB != "int8" {
		t.Fatalf("the wrong object was compared: %q vs %q", divs[0].ValueOnA, divs[0].ValueOnB)
	}
	if divs[0].Rank != RankNarrowed || divs[0].NarrowSide != "n1" {
		t.Fatalf("want narrowed on n1, got %q on %q", divs[0].Rank, divs[0].NarrowSide)
	}

	// And the reverse pairing must be seen just as well.
	c := build("int8", "int4")
	c.Node = "n2"
	divs = Compare("public", tables, a, c)
	if len(divs) != 1 || divs[0].ValueOnB != "int8" {
		t.Fatalf("want one divergence on table %q, got %+v", "a.b", divs)
	}
}

// TestCompositeAttributeRankMatchesColumnRank checks that int4 -> int8 is
// RankNarrowed for a composite attribute, matching the rank a table column
// gets for the same change.
func TestCompositeAttributeRankMatchesColumnRank(t *testing.T) {
	attr := func(typeName string) Property {
		return Property{
			Name:  "attr:0001:x",
			Value: packAttr("pg_catalog", typeName, "b", -1, ""),
		}
	}
	text := func(typeText string) Property {
		return Property{Name: "attrtext:0001:x", Value: typeText}
	}

	a := newSnapshot("n1").composite("public.pair", attr("int4"), text("integer")).build()
	b := newSnapshot("n2").composite("public.pair", attr("int8"), text("bigint")).build()

	divs := Compare("public", nil, a, b)
	if len(divs) != 1 {
		t.Fatalf("want one divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Rank != RankNarrowed || divs[0].NarrowSide != "n1" {
		t.Fatalf("want narrowed on n1 (int4 is the narrow side), got %q on %q", divs[0].Rank, divs[0].NarrowSide)
	}
	if divs[0].ValueOnA != "integer" || divs[0].ValueOnB != "bigint" {
		t.Fatalf("want the printable types, got %q vs %q", divs[0].ValueOnA, divs[0].ValueOnB)
	}
}

// TestCompareOrderIsDeterministic checks that findings come out in a
// stable order across runs, even though referenced types come out of a map
// and Go randomises map iteration.
func TestCompareOrderIsDeterministic(t *testing.T) {
	build := func(node string, labels string) Snapshot {
		return newSnapshot(node).
			enum("public.zulu", labels).
			enum("public.alpha", labels).
			enum("public.mike", labels).
			build()
	}
	a := build("n1", "x,y")
	b := build("n2", "x,y,z")

	want := []string{"public.alpha", "public.mike", "public.zulu"}
	for run := 0; run < 20; run++ {
		divs := Compare("public", nil, a, b)
		if len(divs) != len(want) {
			t.Fatalf("run %d: want %d divergences, got %d", run, len(want), len(divs))
		}
		for i, name := range want {
			if divs[i].Object != name {
				t.Fatalf("run %d: position %d is %q, want %q", run, i, divs[i].Object, name)
			}
		}
	}
}

// TestColumnTypeKindChangeIsIncompatible checks the case a comparison
// keyed only on (namespace, name, typmod) cannot see: an enum dropped on
// one node and recreated under the same name as a domain. Nothing about
// the name or the printed type moves, and the two type Objects no longer
// share an ObjectID, so before type_kind joined the identity this came out
// as a clean match.
func TestColumnTypeKindChangeIsIncompatible(t *testing.T) {
	table := "public.orders"
	build := func(node, kind string) Snapshot {
		s := newSnapshot(node).table(table).key(table).constraints(table).
			column(table, "status", userTypeColumnProps("public", "status", kind)...)
		if kind == "e" {
			s = s.enum("public.status", joinList([]string{"new", "done"}))
		} else {
			s = s.domain("public.status",
				Property{Name: "basetype_namespace", Value: "pg_catalog"},
				Property{Name: "basetype_name", Value: "text"},
				Property{Name: "basetype_kind", Value: "b"},
				Property{Name: "basetypmod", Value: "-1"},
				Property{Name: "basetype_text", Value: "text"},
				Property{Name: "notnull", Value: "false"},
				Property{Name: "default", Value: ""},
			)
		}
		return s.build()
	}

	divs := Compare("public", []string{"orders"}, build("n1", "e"), build("n2", "d"))

	d := findDivergence(t, divs, "public.orders.status", "type")
	if d.Rank != RankIncompatible {
		t.Fatalf("want incompatible for enum replaced by a same-named domain, got %q", d.Rank)
	}
	// The two sides must not print identically, or the report reads as a
	// tool malfunction: same name, same type text, "differs".
	if d.ValueOnA == d.ValueOnB {
		t.Fatalf("both sides print as %q; the kind difference is invisible", d.ValueOnA)
	}
}

// TestCompositeAttributeTypeKindChangeIsIncompatible is the same substitution
// one level down, inside a composite's attribute, where the packed attribute
// value is what carries type identity.
func TestCompositeAttributeTypeKindChangeIsIncompatible(t *testing.T) {
	attr := func(kind string) Property {
		return Property{
			Name:  "attr:0001:status",
			Value: packAttr("public", "status", kind, -1, ""),
		}
	}

	a := newSnapshot("n1").composite("public.row_t", attr("e")).build()
	b := newSnapshot("n2").composite("public.row_t", attr("d")).build()

	divs := Compare("public", nil, a, b)
	if len(divs) != 1 {
		t.Fatalf("want one divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Rank != RankIncompatible {
		t.Fatalf("want incompatible for an attribute's type changing kind, got %q", divs[0].Rank)
	}
}

// TestDomainBaseTypeKindChangeIsIncompatible is the same substitution under
// a domain's base type.
func TestDomainBaseTypeKindChangeIsIncompatible(t *testing.T) {
	build := func(node, kind string) Snapshot {
		return newSnapshot(node).domain("public.code_t",
			Property{Name: "basetype_namespace", Value: "public"},
			Property{Name: "basetype_name", Value: "code"},
			Property{Name: "basetype_kind", Value: kind},
			Property{Name: "basetypmod", Value: "-1"},
			Property{Name: "basetype_text", Value: "code"},
			Property{Name: "notnull", Value: "false"},
			Property{Name: "default", Value: ""},
		).build()
	}

	divs := Compare("public", nil, build("n1", "e"), build("n2", "d"))
	if len(divs) != 1 {
		t.Fatalf("want one divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Rank != RankIncompatible {
		t.Fatalf("want incompatible for a base type changing kind, got %q", divs[0].Rank)
	}
	if divs[0].ValueOnA == divs[0].ValueOnB {
		t.Fatalf("both sides print as %q; the kind difference is invisible", divs[0].ValueOnA)
	}
}

// TestRangeSubtypeKindChangeIsReported is the same substitution under a
// range's subtype. The subtype name is unchanged, so a comparison that
// short-circuits on "the subtype text matches" reports nothing at all.
func TestRangeSubtypeKindChangeIsReported(t *testing.T) {
	build := func(node, kind string) Snapshot {
		return newSnapshot(node).rng("public.myrange",
			Property{Name: "subtype", Value: "public.stamp"},
			Property{Name: "subtype_kind", Value: kind},
			Property{Name: "collation", Value: ""},
			Property{Name: "opclass", Value: "pg_catalog.datetime_ops"},
			Property{Name: "canonical", Value: ""},
			Property{Name: "subtype_diff", Value: ""},
		).build()
	}

	divs := Compare("public", nil, build("n1", "d"), build("n2", "b"))
	if len(divs) != 1 {
		t.Fatalf("want one divergence, got %d: %+v", len(divs), divs)
	}
	if divs[0].Property != "subtype" || divs[0].Rank != RankIncompatible {
		t.Fatalf("want an incompatible subtype finding, got %+v", divs[0])
	}
}

// TestJoinListKeepsDistinctListsDistinct pins the property that makes it
// safe to compare a list of values as one string. Enum labels and key
// column names are compared that way, and both may contain a comma, so a
// comma-joined ("a,b", "c") and ("a", "b,c") would compare equal — the
// same collision ObjectID exists to avoid, one level down in the values.
func TestJoinListKeepsDistinctListsDistinct(t *testing.T) {
	collide := [][2][]string{
		{{"a,b", "c"}, {"a", "b,c"}},
		{{"x"}, {"x", ""}},
		{{"a.b", "c"}, {"a", "b.c"}},
	}
	for _, pair := range collide {
		if got, want := joinList(pair[0]), joinList(pair[1]); got == want {
			t.Errorf("%q and %q both join to %q", pair[0], pair[1], got)
		}
	}

	// And the joined form must still change when the list does, including
	// when only the order changes: an enum's label order is part of its
	// identity.
	if joinList([]string{"a", "b"}) == joinList([]string{"b", "a"}) {
		t.Error("reordered labels join to the same string")
	}
}

// TestJoinListRoundTripsArbitraryContent checks that joinList/splitList
// round-trip a value regardless of what it contains - an enum label is a
// string literal, not an identifier, so nothing stops a user from writing
// one that contains exactly what a delimiter-based join would have picked
// to keep values apart, or one that happens to look like joinList's own
// packed form.
func TestJoinListRoundTripsArbitraryContent(t *testing.T) {
	cases := [][]string{
		{"new\x1fdone"},                  // a former delimiter, embedded in one label
		{"3:foo", "bar"},                 // looks like another packed field
		{""},                             // an empty label
		{"a", "", "b"},                   // an empty label in the middle of the list
		{"5:00:00", "colons:everywhere"}, // digits and colons throughout
	}
	for _, values := range cases {
		got, ok := splitList(joinList(values))
		if !ok {
			t.Errorf("splitList could not decode joinList(%q)", values)
			continue
		}
		if len(got) != len(values) {
			t.Errorf("joinList(%q) round-tripped to %q", values, got)
			continue
		}
		for i := range values {
			if got[i] != values[i] {
				t.Errorf("joinList(%q) round-tripped to %q", values, got)
				break
			}
		}
	}
}

// TestEnumLabelsWithCommasDoNotCollide checks the same thing end to end:
// two label lists that a comma would flatten into the same text are
// reported as differing.
func TestEnumLabelsWithCommasDoNotCollide(t *testing.T) {
	a := newSnapshot("n1").enum("public.tricky", joinList([]string{"a,b", "c"})).build()
	b := newSnapshot("n2").enum("public.tricky", joinList([]string{"a", "b,c"})).build()

	divs := Compare("public", nil, a, b)
	if len(divs) != 1 {
		t.Fatalf("want the differing label lists reported, got %d: %+v", len(divs), divs)
	}
	if divs[0].Property != "labels" {
		t.Fatalf("want a labels finding, got %+v", divs[0])
	}
}

// TestDatabaseLocaleDifferenceIsReported checks that two nodes whose
// databases were created with different collations are reported even when
// every column matches — the case no per-column check can see, since an
// uncollated column records only "the default" on both nodes.
func TestDatabaseLocaleDifferenceIsReported(t *testing.T) {
	table := "public.orders"
	build := func(node, collate string) Snapshot {
		return newSnapshot(node).table(table).key(table).constraints(table).
			column(table, "name", baseColumnProps(
				Property{Name: "type", Value: "text"},
				Property{Name: "type_name", Value: "text"},
			)...).
			locale("appdb", collate, collate, "c", "").
			build()
	}

	divs := Compare("public", []string{"orders"},
		build("n1", "en_US.UTF-8"), build("n2", "C"))

	d := findDivergence(t, divs, "appdb", "lc_collate")
	if d.Rank != RankIncompatible {
		t.Fatalf("want incompatible for differing database collations, got %q", d.Rank)
	}
	if d.ValueOnA != "en_US.UTF-8" || d.ValueOnB != "C" {
		t.Fatalf("unexpected values: %q vs %q", d.ValueOnA, d.ValueOnB)
	}
}

// TestDatabaseLocaleMatchingIsSilent guards the other direction: the new
// database Object must not produce a finding on nodes that agree.
func TestDatabaseLocaleMatchingIsSilent(t *testing.T) {
	build := func(node string) Snapshot {
		return newSnapshot(node).locale("appdb", "C", "C", "c", "").build()
	}
	if divs := Compare("public", nil, build("n1"), build("n2")); len(divs) != 0 {
		t.Fatalf("want no divergences for matching locales, got %+v", divs)
	}
}

// TestCompositeAttributeOrdinalsSurviveADrop checks that dropping one
// attribute does not make the attributes after it look changed too. The
// property key carries each attribute's own attnum, so a gap stays a gap
// instead of renumbering everything below it.
func TestCompositeAttributeOrdinalsSurviveADrop(t *testing.T) {
	attr := func(attnum int, name, typeName string) []Property {
		return []Property{
			{Name: fmt.Sprintf("attr:%04d:%s", attnum, name),
				Value: packAttr("pg_catalog", typeName, "b", -1, "")},
			{Name: fmt.Sprintf("attrtext:%04d:%s", attnum, name), Value: typeName},
		}
	}
	compose := func(node string, attrs ...[]Property) Snapshot {
		var props []Property
		for _, a := range attrs {
			props = append(props, a...)
		}
		return newSnapshot(node).composite("public.addr", props...).build()
	}

	// n2 dropped attribute 2 ("zip"). The attributes that follow keep their
	// attnums, so only the drop should be reported.
	a := compose("n1", attr(1, "street", "text"), attr(2, "zip", "int4"), attr(3, "city", "text"))
	b := compose("n2", attr(1, "street", "text"), attr(3, "city", "text"))

	divs := Compare("public", nil, a, b)
	if len(divs) != 1 {
		t.Fatalf("want only the dropped attribute reported, got %d: %+v", len(divs), divs)
	}
	if divs[0].Rank != RankAbsent {
		t.Fatalf("want absent for the dropped attribute, got %q", divs[0].Rank)
	}
	if !strings.Contains(divs[0].Property, "zip") {
		t.Fatalf("want the finding to name the dropped attribute, got %q", divs[0].Property)
	}
	// The ordinal printed is the attribute's own attnum, so a reader can
	// line the finding up against \d+ on the type.
	if !strings.Contains(divs[0].Property, "2") {
		t.Fatalf("want the finding to carry attnum 2, got %q", divs[0].Property)
	}
}
