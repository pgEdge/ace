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
	"sort"
	"strconv"
	"strings"
)

// Divergence is one difference found between two nodes' structure.
// NodeA/NodeB are the two snapshots Compare was given, in the order given,
// and ValueOnA/ValueOnB follow that same order; comparison is symmetric.
// Turning this into directional "n1 -> n2" reporting, once a replication
// topology is known, is internal/consistency/drift's job.
type Divergence struct {
	Object   string `json:"object"`             // "public.orders" or "public.orders.discount"
	Kind     string `json:"kind"`               // "table" | "column" | "key" | "constraint"
	Property string `json:"property,omitempty"` // "type", "notnull", ...; empty when the whole object is what differs

	NodeA    string `json:"node_a"`
	NodeB    string `json:"node_b"`
	ValueOnA string `json:"value_on_a"`
	ValueOnB string `json:"value_on_b"`

	Rank string `json:"rank"` // one of the Rank* constants in rank.go

	// NarrowSide names the node whose value set is the narrower one. Only
	// set when Rank == RankNarrowed.
	NarrowSide string `json:"narrow_side,omitempty"`

	// Note carries a short, human-facing explanation for a Divergence whose
	// Rank alone would not make the reason obvious. Empty when the
	// Object/Kind/Property/Rank already say enough.
	Note string `json:"note,omitempty"`
}

// FindingKey identifies one finding regardless of which side of a node
// pair its values landed on, so a caller can count distinct findings across
// every pair without counting one drift once per pair that sees it.
//
// Object+Kind+Property is enough for all but constraints, which
// compareConstraints reports against the table with no Property: without
// the definition text a table missing five would count as one. Their
// values are sorted into the key because the odd node out is NodeA in one
// pair and NodeB in the next.
func (d Divergence) FindingKey() string {
	key := d.Object + "\x00" + d.Kind + "\x00" + d.Property
	if d.Kind != "constraint" {
		return key
	}
	lo, hi := d.ValueOnA, d.ValueOnB
	if hi < lo {
		lo, hi = hi, lo
	}
	return key + "\x00" + lo + "\x00" + hi
}

// Compare finds every structural difference between two Snapshots, for the
// given tables in schemaName.
//
// Both snapshots must have been collected with the same schemaName and
// tables. Reconciling a scope that resolved differently on the two nodes —
// a table present on one node but not the other — is the caller's job: pass
// Compare only the tables both sides agree are in scope, and report a scope
// mismatch as a separate message, not as a Divergence. A table that
// disappears between scope resolution and collection (dropped concurrently)
// is different — Compare does catch that, as RankAbsent on the table
// itself.
func Compare(schemaName string, tables []string, a, b Snapshot) []Divergence {
	var out []Divergence

	for _, table := range tables {
		tableID := TableID(schemaName, table)
		aTable, aOK := a.Objects[tableID]
		bTable, bOK := b.Objects[tableID]

		if !aOK || !bOK {
			// This package never opens a connection of its own — it only
			// prints an identifier a Snapshot already collected and quoted.
			// Whichever side has the table names it; if neither does, the
			// raw, unquoted schema.table is the only thing left to print.
			qualified := aTable.Name
			if qualified == "" {
				qualified = bTable.Name
			}
			if qualified == "" {
				qualified = schemaName + "." + table
			}
			out = append(out, Divergence{
				Object: qualified, Kind: "table",
				NodeA: a.Node, NodeB: b.Node,
				ValueOnA: presence(aOK), ValueOnB: presence(bOK),
				Rank: RankAbsent,
				Note: "the table is missing from one node's snapshot",
			})
			continue
		}
		qualified := aTable.Name

		out = append(out, compareTableProperties(schemaName, table, qualified, a, b)...)
		out = append(out, compareColumns(schemaName, table, a, b)...)
		out = append(out, compareKey(schemaName, table, qualified, a, b)...)
		out = append(out, compareConstraints(schemaName, table, qualified, a, b)...)
	}

	out = append(out, compareReferencedTypes(a, b)...)
	out = append(out, compareDatabaseLocale(a, b)...)

	return out
}

// compareDatabaseLocale diffs the two databases' own collation settings.
//
// This is reported once per comparison rather than per column, because it
// is what an uncollated column inherits and what no column can show: both
// nodes record the "default" collation in pg_attribute whatever their
// database was created with. Two nodes that disagree here hold the same
// values but sort them differently, which means a unique index can disagree
// about which rows are duplicates — so this is RankIncompatible, not a
// cosmetic difference, even though no DDL on either node caused it.
func compareDatabaseLocale(a, b Snapshot) []Divergence {
	aObj, aOK := a.Objects[DatabaseID()]
	bObj, bOK := b.Objects[DatabaseID()]
	if !aOK || !bOK {
		// One side collected no locale, which happens when its scope held
		// no tables. Nothing was compared there, so there is nothing to say.
		return nil
	}

	var out []Divergence
	for _, prop := range []string{"lc_collate", "lc_ctype", "locale_provider", "locale"} {
		av, bv := getProp(aObj.Properties, prop), getProp(bObj.Properties, prop)
		if av == bv {
			continue
		}
		out = append(out, Divergence{
			Object: databaseObjectName(aObj, bObj), Kind: "database", Property: prop,
			NodeA: a.Node, NodeB: b.Node, ValueOnA: av, ValueOnB: bv,
			Rank: RankIncompatible,
			Note: "the databases were created with different collation settings, so text sorts and compares differently on the two nodes even where every column matches",
		})
	}
	return out
}

// databaseObjectName names the database a locale finding is about, allowing
// for the two nodes having named theirs differently.
func databaseObjectName(aObj, bObj Object) string {
	switch {
	case aObj.Name == bObj.Name:
		return aObj.Name
	case aObj.Name == "":
		return bObj.Name
	case bObj.Name == "":
		return aObj.Name
	default:
		return aObj.Name + "/" + bObj.Name
	}
}

// compareReferencedTypes diffs every domain/range/composite/enum type both
// snapshots collected. A qualified type name present in only one snapshot
// is not reported here: some column already carries that mismatch as its
// own "type" Divergence, so a second finding would only be noise. What this
// pass catches is what column-level comparison cannot: two columns that
// agree on a type's name but disagree about what it actually constrains.
//
// Objects is a map, so the keys are gathered and sorted before anything is
// compared: two runs over an unchanged pair of nodes must produce the same
// report in the same order.
func compareReferencedTypes(a, b Snapshot) []Divergence {
	var out []Divergence
	for _, kind := range []string{"domain", "range", "composite", "enum"} {
		ids := make([]ObjectID, 0, len(a.Objects))
		for id := range a.Objects {
			if id.Kind == kind {
				ids = append(ids, id)
			}
		}
		sort.Slice(ids, func(i, j int) bool {
			if ids[i].Schema != ids[j].Schema {
				return ids[i].Schema < ids[j].Schema
			}
			return ids[i].Name < ids[j].Name
		})

		for _, id := range ids {
			aObj := a.Objects[id]
			bObj, ok := b.Objects[id]
			if !ok {
				continue
			}
			switch kind {
			case "domain":
				out = append(out, compareDomainObject(aObj.Name, a.Node, b.Node, aObj.Properties, bObj.Properties)...)
			case "range":
				out = append(out, compareRangeObject(aObj.Name, a.Node, b.Node, aObj.Properties, bObj.Properties)...)
			case "composite":
				out = append(out, compareCompositeObject(aObj.Name, a.Node, b.Node, aObj.Properties, bObj.Properties)...)
			case "enum":
				out = append(out, compareEnumObject(aObj.Name, a.Node, b.Node, aObj.Properties, bObj.Properties)...)
			}
		}
	}
	return out
}

// compareDomainObject diffs one domain's constraints between two nodes. A
// changed base type is classified with classifyType, same as a column's own
// type (e.g. varchar(20) to varchar(10) is RankNarrowed); NOT NULL added on
// a domain excludes NULL, so it is RankNarrowed too; a changed default is
// representation, not a value-set change, so RankEquivalentDiffering; CHECK
// constraints are compared as a set, same as table constraints.
func compareDomainObject(name, nodeA, nodeB string, aProps, bProps []Property) []Divergence {
	var out []Divergence

	aNS, bNS := getProp(aProps, "basetype_namespace"), getProp(bProps, "basetype_namespace")
	aName, bName := getProp(aProps, "basetype_name"), getProp(bProps, "basetype_name")
	aKind, bKind := getProp(aProps, "basetype_kind"), getProp(bProps, "basetype_kind")
	aMod64, _ := strconv.ParseInt(getProp(aProps, "basetypmod"), 10, 32)
	bMod64, _ := strconv.ParseInt(getProp(bProps, "basetypmod"), 10, 32)
	switch {
	case aKind != bKind:
		// See the same case in comparePropertiesForColumn: two kinds of
		// type are never interchangeable, however alike their names.
		out = append(out, Divergence{
			Object: name, Kind: "domain", Property: "basetype",
			NodeA: nodeA, NodeB: nodeB,
			ValueOnA: describeKind(getProp(aProps, "basetype_text"), aKind),
			ValueOnB: describeKind(getProp(bProps, "basetype_text"), bKind),
			Rank:     RankIncompatible,
			Note:     "the domain's base type has the same name on both nodes but is a different kind of type",
		})

	case aNS != bNS || aName != bName || aMod64 != bMod64:
		rank, side := classifyType(aNS, aName, int32(aMod64), bNS, bName, int32(bMod64), nodeA, nodeB)
		if rank != "" {
			// Printed from basetype_text, not from the portable
			// (namespace, name) pair the comparison keys on, since that
			// text is what shows a varchar(20) vs varchar(10) difference.
			out = append(out, Divergence{
				Object: name, Kind: "domain", Property: "basetype",
				NodeA: nodeA, NodeB: nodeB,
				ValueOnA: getProp(aProps, "basetype_text"), ValueOnB: getProp(bProps, "basetype_text"),
				Rank: rank, NarrowSide: side,
			})
		}
	}

	if av, bv := getProp(aProps, "notnull"), getProp(bProps, "notnull"); av != bv {
		side := nodeA
		if bv == "true" {
			side = nodeB
		}
		out = append(out, Divergence{
			Object: name, Kind: "domain", Property: "notnull",
			NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
			Rank: RankNarrowed, NarrowSide: side,
		})
	}

	if av, bv := getProp(aProps, "default"), getProp(bProps, "default"); av != bv {
		out = append(out, Divergence{
			Object: name, Kind: "domain", Property: "default",
			NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
			Rank: RankEquivalentDiffering,
		})
	}

	out = append(out, compareDomainChecks(name, nodeA, nodeB, aProps, bProps)...)

	return out
}

// compareDomainChecks diffs one domain's CHECK set.
//
// "One side has a CHECK the other lacks" is a narrowing: that side accepts
// strictly fewer values. When each side has a CHECK the other lacks,
// neither value set contains the other, so that case is reported as one
// RankIncompatible finding listing both sides' extra CHECKs. Whether the
// two conditions happen to imply one another is not decided from the
// expression text.
func compareDomainChecks(name, nodeA, nodeB string, aProps, bProps []Property) []Divergence {
	aChecks, bChecks := propertySet(aProps, "check"), propertySet(bProps, "check")

	aOnly := sortedMissing(aChecks, bChecks)
	bOnly := sortedMissing(bChecks, aChecks)

	switch {
	case len(aOnly) == 0 && len(bOnly) == 0:
		return nil

	case len(aOnly) > 0 && len(bOnly) > 0:
		return []Divergence{{
			Object: name, Kind: "domain", Property: "check",
			NodeA: nodeA, NodeB: nodeB,
			ValueOnA: strings.Join(aOnly, "; "), ValueOnB: strings.Join(bOnly, "; "),
			Rank: RankIncompatible,
			Note: "each side has a CHECK the other lacks, so neither value set is contained in the other",
		}}
	}

	only, side, narrowIsA := aOnly, nodeA, true
	if len(bOnly) > 0 {
		only, side, narrowIsA = bOnly, nodeB, false
	}

	out := make([]Divergence, 0, len(only))
	for _, def := range only {
		d := Divergence{
			Object: name, Kind: "domain", Property: "check",
			NodeA: nodeA, NodeB: nodeB,
			Rank: RankNarrowed, NarrowSide: side,
			Note: "the CHECK exists on one side only, so that side narrows the domain further",
		}
		if narrowIsA {
			d.ValueOnA, d.ValueOnB = def, "(absent)"
		} else {
			d.ValueOnA, d.ValueOnB = "(absent)", def
		}
		out = append(out, d)
	}
	return out
}

// sortedMissing returns the members of have that want does not contain, in
// sorted order — sorted because the sets come from maps and a report's line
// order must not depend on Go's map iteration.
func sortedMissing(have, want map[string]bool) []string {
	var out []string
	for def := range have {
		if !want[def] {
			out = append(out, def)
		}
	}
	sort.Strings(out)
	return out
}

// compareRangeObject diffs a range type's subtype, collation, opclass and
// canonical/subtype_diff functions. None of these have a narrowing story
// this package can reason about — any difference changes what values sort
// where or how they canonicalise, so all are RankIncompatible.
//
// When the subtype itself differs, only that is reported: the opclass and
// collation belong to the subtype, so one finding covers what would
// otherwise be three (subtype, opclass, possibly collation) for a single
// change.
func compareRangeObject(name, nodeA, nodeB string, aProps, bProps []Property) []Divergence {
	// The subtype's identity is its name and its kind together, so a kind
	// changed under an unchanged name is still a changed subtype. Both are
	// folded into one finding, printed so that the two sides differ visibly
	// even when only the kind moved.
	aSubtype, bSubtype := getProp(aProps, "subtype"), getProp(bProps, "subtype")
	aKind, bKind := getProp(aProps, "subtype_kind"), getProp(bProps, "subtype_kind")

	if aSubtype != bSubtype || aKind != bKind {
		// Only the subtype is reported: the operator class, collation and
		// canonical function all belong to it, so one change would
		// otherwise produce three findings.
		return []Divergence{{
			Object: name, Kind: "range", Property: "subtype",
			NodeA: nodeA, NodeB: nodeB,
			ValueOnA: describeKind(aSubtype, aKind),
			ValueOnB: describeKind(bSubtype, bKind),
			Rank:     RankIncompatible,
			Note:     "the range's subtype differs; its operator class, collation and canonical function follow from it and are not reported separately",
		}}
	}

	var out []Divergence
	for _, prop := range []string{"collation", "opclass", "canonical", "subtype_diff"} {
		av, bv := getProp(aProps, prop), getProp(bProps, prop)
		if av == bv {
			continue
		}
		out = append(out, Divergence{
			Object: name, Kind: "range", Property: prop,
			NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
			Rank: RankIncompatible,
		})
	}
	return out
}

// compareCompositeObject diffs a composite type's attributes. Property
// Names already carry the attnum-order index, so a union-by-Name comparison
// is naturally in declaration order and reports an added/removed/retyped/
// recollated attribute as one line: a missing attribute on one side simply
// has "" there, matching the absent-value convention used elsewhere.
func compareCompositeObject(name, nodeA, nodeB string, aProps, bProps []Property) []Divergence {
	var out []Divergence
	names := unionSorted(propNames(aProps, "attr:"), propNames(bProps, "attr:"))
	for _, key := range names {
		av, bv := getProp(aProps, key), getProp(bProps, key)
		if av == bv {
			continue
		}
		rank, side := classifyAttr(av, bv, nodeA, nodeB)

		// The property key is "attr:0007:city" and its value is a packed
		// "ns.name/typmod/collation" string — neither fit for display. The
		// paired attrtext property carries the same attribute as PostgreSQL
		// prints it.
		ord, attr := splitAttrKey(key)
		suffix := strings.TrimPrefix(key, "attr:")
		out = append(out, Divergence{
			Object: name, Kind: "composite",
			Property: fmt.Sprintf("attribute %d (%s)", ord, attr),
			NodeA:    nodeA, NodeB: nodeB,
			ValueOnA: displayOrRaw(getProp(aProps, "attrtext:"+suffix), av),
			ValueOnB: displayOrRaw(getProp(bProps, "attrtext:"+suffix), bv),
			Rank:     rank, NarrowSide: side,
		})
	}
	return out
}

// classifyAttr ranks a difference between two composite attributes.
//
// An attribute missing on one side is RankAbsent. Otherwise the packed
// values are split back into (type namespace, type name, typmod, collation)
// and put through classifyType, the same reasoning a table column's own
// type gets, so widening int4 -> int8 is RankNarrowed for both a column and
// a composite attribute. A pair that cannot be unpacked, or that differs
// only in collation, falls back to what can still be said for certain: the
// two are not the same.
func classifyAttr(packedA, packedB, nodeA, nodeB string) (rank, narrowSide string) {
	if packedA == "" || packedB == "" {
		return RankAbsent, ""
	}

	aNS, aName, aKind, aMod, aColl, aOK := unpackAttr(packedA)
	bNS, bName, bKind, bMod, bColl, bOK := unpackAttr(packedB)
	if !aOK || !bOK {
		return RankIncompatible, ""
	}

	// Kind is part of the attribute type's identity, as it is for a column
	// (see comparePropertiesForColumn); an enum replaced by a same-named
	// domain is not a narrowing of anything.
	if aKind != bKind {
		return RankIncompatible, ""
	}

	if aNS == bNS && aName == bName && aMod == bMod {
		// Same type, so what differs is the collation: the attribute holds
		// the same values on both nodes but compares and sorts them
		// differently.
		if aColl != bColl {
			return RankEquivalentDiffering, ""
		}
		// Unreachable: identical fields pack to identical strings, and the
		// caller only gets here for strings that differ. Kept loud rather
		// than plausible, so a future change to packAttr's format shows up
		// as an obviously wrong rank instead of a quiet one.
		return RankIncompatible, ""
	}

	rank, side := classifyType(aNS, aName, aMod, bNS, bName, bMod, nodeA, nodeB)
	if rank == "" {
		return RankIncompatible, ""
	}
	return rank, side
}

// splitAttrKey turns collect.go's "attr:0007:city" back into (7, "city").
// The number is the attribute's own attnum, so it is printed as it stands —
// it matches what \d+ on the type shows, gaps from dropped attributes
// included.
func splitAttrKey(key string) (int, string) {
	rest := strings.TrimPrefix(key, "attr:")
	idx, attr, found := strings.Cut(rest, ":")
	if !found {
		return 0, rest
	}
	ord, err := strconv.Atoi(idx)
	if err != nil {
		return 0, attr
	}
	return ord, attr
}

// displayOrRaw prefers the display text collect.go paired with an
// attribute, falls back to the raw portable value when there is none, and
// says "absent" when the attribute itself is missing on that side.
func displayOrRaw(display, raw string) string {
	switch {
	case display != "":
		return display
	case raw != "":
		return raw
	default:
		return "(absent)"
	}
}

// compareEnumObject diffs an enum's label list as one whole-list property:
// order is part of an enum's identity, so a single added/removed/reordered
// label is reported as one line naming the whole list.
func compareEnumObject(name, nodeA, nodeB string, aProps, bProps []Property) []Divergence {
	av, bv := getProp(aProps, "labels"), getProp(bProps, "labels")
	if av == bv {
		return nil
	}
	return []Divergence{{
		Object: name, Kind: "enum", Property: "labels",
		NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
		Rank: RankIncompatible,
		Note: "an enum's set of labels and their order both drive comparison and casting",
	}}
}

// propNames returns the names of every property whose name starts with
// prefix (pass "" for all of them).
func propNames(props []Property, prefix string) []string {
	names := make([]string, 0, len(props))
	for _, p := range props {
		if strings.HasPrefix(p.Name, prefix) {
			names = append(names, p.Name)
		}
	}
	return names
}

// propertySet gathers the values of every property called name into a set.
// It serves the kinds that are compared as a set rather than slot by slot -
// a domain's CHECKs, a table's constraints - where the question is only
// whether both nodes hold the same definitions, in any order.
func propertySet(props []Property, name string) map[string]bool {
	set := make(map[string]bool)
	for _, p := range props {
		if p.Name == name {
			set[p.Value] = true
		}
	}
	return set
}

// presence spells "the object exists here" as a value, so that a finding
// about a missing table or column still shows something on both sides of the
// report instead of an empty string on one of them.
func presence(ok bool) string {
	if ok {
		return "present"
	}
	return "absent"
}

// typeKindNames spells out pg_type.typtype for a report. An unrecognised
// code is printed as-is rather than guessed at.
var typeKindNames = map[string]string{
	"b": "base type",
	"c": "composite type",
	"d": "domain",
	"e": "enum",
	"m": "multirange",
	"p": "pseudo-type",
	"r": "range",
}

// describeKind names the kind alongside the type, for the one finding where
// the type names match and only the kind differs — printing "public.status"
// on both sides would otherwise read as a tool malfunction.
func describeKind(typeText, kind string) string {
	name, ok := typeKindNames[kind]
	if !ok {
		name = fmt.Sprintf("typtype %q", kind)
	}
	if typeText == "" {
		return name
	}
	return fmt.Sprintf("%s (%s)", typeText, name)
}

// parseTypmod reads a type modifier a Snapshot recorded. A modifier that
// will not parse is an error rather than a zero: 0 is a legal typmod, so
// defaulting to it would turn a broken snapshot into a plausible-looking
// comparison.
func parseTypmod(value string) (int64, error) {
	mod, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("type modifier %q: %w", value, err)
	}
	return mod, nil
}

// getProp returns the value of the property called name, or "" when there is
// no such property. A property that is absent and one that holds "" are
// deliberately not told apart: every caller here asks whether two nodes
// agree, and "neither node recorded anything" is agreement.
func getProp(props []Property, name string) string {
	for _, p := range props {
		if p.Name == name {
			return p.Value
		}
	}
	return ""
}

// compareTableProperties diffs what is recorded about the table itself,
// rather than about its columns, key or constraints. Today that is only how
// the table is partitioned, which is why a table with no partitioning at all
// yields nothing here.
func compareTableProperties(schemaName, table, qualified string, a, b Snapshot) []Divergence {
	aTable := a.Objects[TableID(schemaName, table)]
	bTable := b.Objects[TableID(schemaName, table)]

	var out []Divergence
	for _, name := range []string{"partition_bound", "partition_key"} {
		av, bv := getProp(aTable.Properties, name), getProp(bTable.Properties, name)
		if av == bv {
			continue
		}
		out = append(out, Divergence{
			Object: qualified, Kind: "table", Property: name,
			NodeA: a.Node, NodeB: b.Node, ValueOnA: av, ValueOnB: bv,
			Rank: RankIncompatible,
			Note: "the partitioning differs; whether one range contains the other is not checked",
		})
	}
	return out
}

// compareColumns diffs every column either node holds, walking the union of
// the two column lists rather than one node's. That is what lets a column
// present on one side only be reported as absent instead of quietly skipped,
// and it keeps the result the same whichever snapshot was passed first.
func compareColumns(schemaName, table string, a, b Snapshot) []Divergence {
	tableID := TableID(schemaName, table)
	names := unionSorted(a.TableColumns[tableID], b.TableColumns[tableID])

	var out []Divergence
	for _, name := range names {
		aCol, aOK := a.Objects[ColumnID(schemaName, table, name)]
		bCol, bOK := b.Objects[ColumnID(schemaName, table, name)]

		// Whichever side has the column already carries its own quoted
		// display name (built by qualify() when the Snapshot was
		// collected) — see the note in Compare's own missing-table branch
		// for why this package prints only names a Snapshot already
		// quoted, never one it builds itself.
		colQualified := aCol.Name
		if colQualified == "" {
			colQualified = bCol.Name
		}
		if colQualified == "" {
			colQualified = schemaName + "." + table + "." + name
		}

		if !aOK || !bOK {
			out = append(out, Divergence{
				Object: colQualified, Kind: "column",
				NodeA: a.Node, NodeB: b.Node,
				ValueOnA: presence(aOK), ValueOnB: presence(bOK),
				Rank: RankAbsent,
			})
			continue
		}

		out = append(out, comparePropertiesForColumn(colQualified, a.Node, b.Node, aCol.Properties, bCol.Properties)...)
	}
	return out
}

// comparePropertiesForColumn diffs one column that both nodes have, property
// by property.
//
// The type is handled first and on its own, because it decides more than its
// own finding: an unreadable type modifier or a changed kind of type makes
// any narrowing reasoning meaningless, and a type that changed outright
// makes the column's collation a consequence of that change rather than a
// separate difference worth reporting.
func comparePropertiesForColumn(colQualified, nodeA, nodeB string, aProps, bProps []Property) []Divergence {
	var out []Divergence

	aNS, bNS := getProp(aProps, "type_namespace"), getProp(bProps, "type_namespace")
	aName, bName := getProp(aProps, "type_name"), getProp(bProps, "type_name")
	aKind, bKind := getProp(aProps, "type_kind"), getProp(bProps, "type_kind")
	aTypeMod64, aModErr := parseTypmod(getProp(aProps, "type_mod"))
	bTypeMod64, bModErr := parseTypmod(getProp(bProps, "type_mod"))

	typeIsIncompatible := false
	switch {
	case aModErr != nil || bModErr != nil:
		// Neither side's modifier can be trusted, so say that rather than
		// silently comparing against a typmod of 0, which is a legal value
		// for some types and would make a broken snapshot look like a real
		// difference — or worse, like a match.
		typeIsIncompatible = true
		out = append(out, Divergence{
			Object: colQualified, Kind: "column", Property: "type",
			NodeA: nodeA, NodeB: nodeB,
			ValueOnA: getProp(aProps, "type"), ValueOnB: getProp(bProps, "type"),
			Rank: RankIncompatible,
			Note: "the snapshot's type modifier could not be read, so the two types cannot be compared",
		})

	case aKind != bKind:
		// The kind is part of a type's identity. Dropping an enum and
		// recreating the name as a domain leaves (namespace, name) and even
		// the printed type untouched, so without this the substitution is
		// invisible — and no narrowing reasoning applies between kinds.
		typeIsIncompatible = true
		out = append(out, Divergence{
			Object: colQualified, Kind: "column", Property: "type",
			NodeA: nodeA, NodeB: nodeB,
			ValueOnA: describeKind(getProp(aProps, "type"), aKind),
			ValueOnB: describeKind(getProp(bProps, "type"), bKind),
			Rank:     RankIncompatible,
			Note:     "same type name on both nodes, but one node's is a different kind of type",
		})

	case aNS != bNS || aName != bName || aTypeMod64 != bTypeMod64:
		rank, side := classifyType(aNS, aName, int32(aTypeMod64), bNS, bName, int32(bTypeMod64), nodeA, nodeB)
		if rank != "" {
			typeIsIncompatible = rank == RankIncompatible
			out = append(out, Divergence{
				Object: colQualified, Kind: "column", Property: "type",
				NodeA: nodeA, NodeB: nodeB,
				ValueOnA: getProp(aProps, "type"), ValueOnB: getProp(bProps, "type"),
				Rank: rank, NarrowSide: side,
			})
		}
	}

	// NOT NULL narrows the column's set of acceptable values (it excludes
	// NULL), fitting the same "strict subset" reasoning as type width.
	if av, bv := getProp(aProps, "notnull"), getProp(bProps, "notnull"); av != bv {
		side := nodeA
		if bv == "true" {
			side = nodeB
		}
		out = append(out, Divergence{
			Object: colQualified, Kind: "column", Property: "notnull",
			NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
			Rank: RankNarrowed, NarrowSide: side,
		})
	}

	// None of these restrict which values fit; they change representation
	// or behaviour for values that fit equally well on both sides.
	for _, name := range []string{"identity", "generated", "options", "default"} {
		av, bv := getProp(aProps, name), getProp(bProps, name)
		if av == bv {
			continue
		}
		out = append(out, Divergence{
			Object: colQualified, Kind: "column", Property: name,
			NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
			Rank: RankEquivalentDiffering,
		})
	}

	// Collation carries the collation's version as well as its name, so a
	// version difference (a different glibc/ICU release, not a schema edit)
	// needs the Note to say so. It is skipped once the type itself is
	// incompatible, since a column retyped integer -> text gains a
	// collation only as a consequence of that one change.
	if av, bv := getProp(aProps, "collation"), getProp(bProps, "collation"); av != bv && !typeIsIncompatible {
		out = append(out, Divergence{
			Object: colQualified, Kind: "column", Property: "collation",
			NodeA: nodeA, NodeB: nodeB, ValueOnA: av, ValueOnB: bv,
			Rank: RankEquivalentDiffering,
			Note: "collation name/provider/version, in that order; a differing version means the nodes' collation libraries differ, so the same text can sort differently and a unique index can disagree about duplicates",
		})
	}

	return out
}

// compareKey diffs the row identity the two nodes replicate by: the replica
// identity mode, the key's columns in index order, and the operator classes
// those columns use.
//
// The mode and the columns are RankIncompatible, since two nodes that do not
// agree on which rows are the same row cannot converge by exchanging them.
// The operator classes are ranked lower: the same rows are still identified,
// but a different class means a different idea of which values are equal and
// how they sort.
func compareKey(schemaName, table, qualified string, a, b Snapshot) []Divergence {
	aKey := a.Objects[KeyID(schemaName, table)]
	bKey := b.Objects[KeyID(schemaName, table)]

	var out []Divergence

	if av, bv := getProp(aKey.Properties, "replica_identity"), getProp(bKey.Properties, "replica_identity"); av != bv {
		out = append(out, Divergence{
			Object: qualified, Kind: "key", Property: "replica_identity",
			NodeA: a.Node, NodeB: b.Node, ValueOnA: av, ValueOnB: bv,
			Rank: RankIncompatible,
			Note: "different row identity mode: each node would resolve conflicts differently",
		})
	}
	if av, bv := getProp(aKey.Properties, "key_columns"), getProp(bKey.Properties, "key_columns"); av != bv {
		out = append(out, Divergence{
			Object: qualified, Kind: "key", Property: "key_columns",
			NodeA: a.Node, NodeB: b.Node, ValueOnA: av, ValueOnB: bv,
			Rank: RankIncompatible,
		})
	}
	if av, bv := getProp(aKey.Properties, "key_opclasses"), getProp(bKey.Properties, "key_opclasses"); av != bv {
		out = append(out, Divergence{
			Object: qualified, Kind: "key", Property: "key_opclasses",
			NodeA: a.Node, NodeB: b.Node, ValueOnA: av, ValueOnB: bv,
			Rank: RankEquivalentDiffering,
			Note: "same type, different operator class: a different notion of equality and ordering for this column",
		})
	}
	return out
}

// compareConstraints diffs two tables' constraint sets. This is a set
// comparison, not a property comparison: PostgreSQL invents names for
// unnamed constraints, so there is no stable per-name slot to line up
// across nodes — only "does this exact definition exist on both sides".
func compareConstraints(schemaName, table, qualified string, a, b Snapshot) []Divergence {
	aSet := constraintSet(a.Objects[ConstraintID(schemaName, table)])
	bSet := constraintSet(b.Objects[ConstraintID(schemaName, table)])

	aOnly, bOnly := sortedMissing(aSet, bSet), sortedMissing(bSet, aSet)

	// A CHECK one node has and the other lacks makes that node's accepted
	// values a strict subset, exactly as it does on a domain, so it is
	// ranked the same way here: RankNarrowed, naming the narrow side. The
	// same reasoning does not extend to the other constraint types — a
	// missing FOREIGN KEY or UNIQUE is not a narrowing of a value set the
	// two nodes otherwise share. See compareDomainChecks, which this
	// deliberately mirrors.
	//
	// Both conditions below are needed. One side's extra constraints must
	// be CHECKs and nothing else, and the other side must have no extra
	// constraint at all: a CHECK on one node against a FOREIGN KEY on the
	// other is not a narrowing either way, since each node then rejects
	// rows the other accepts and neither set contains the other.
	checkNarrows := (onlyChecks(aOnly) && len(bOnly) == 0) ||
		(onlyChecks(bOnly) && len(aOnly) == 0)

	var out []Divergence
	for _, def := range aOnly {
		d := Divergence{
			Object: qualified, Kind: "constraint",
			NodeA: a.Node, NodeB: b.Node, ValueOnA: def, ValueOnB: "(absent)",
			Rank: RankAbsent,
		}
		if checkNarrows && isCheck(def) {
			d.Rank, d.NarrowSide = RankNarrowed, a.Node
			d.Note = "the CHECK exists on one side only, so that side accepts strictly fewer rows"
		}
		out = append(out, d)
	}
	for _, def := range bOnly {
		d := Divergence{
			Object: qualified, Kind: "constraint",
			NodeA: a.Node, NodeB: b.Node, ValueOnA: "(absent)", ValueOnB: def,
			Rank: RankAbsent,
		}
		if checkNarrows && isCheck(def) {
			d.Rank, d.NarrowSide = RankNarrowed, b.Node
			d.Note = "the CHECK exists on one side only, so that side accepts strictly fewer rows"
		}
		out = append(out, d)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].ValueOnA+"\x00"+out[i].ValueOnB < out[j].ValueOnA+"\x00"+out[j].ValueOnB
	})
	return out
}

// isCheck reports whether a packed constraint value is a CHECK. collect.go
// writes contype first, so the prefix is the whole test.
func isCheck(def string) bool {
	return strings.HasPrefix(def, "c|")
}

// onlyChecks reports whether defs is non-empty and holds nothing but CHECK
// constraints — the condition under which one side can be called the
// narrower one.
func onlyChecks(defs []string) bool {
	if len(defs) == 0 {
		return false
	}
	for _, def := range defs {
		if !isCheck(def) {
			return false
		}
	}
	return true
}

// constraintSet gathers one table's constraint definitions into a set. It
// takes the whole Object rather than its properties, so that a table for
// which no constraint Object was collected at all comes back as an empty
// set: a table with no constraints and a table nothing was read from compare
// the same way, which is what lets compareConstraints stay a set difference.
func constraintSet(obj Object) map[string]bool {
	set := make(map[string]bool, len(obj.Properties))
	for _, p := range obj.Properties {
		if p.Name == "constraint" {
			set[p.Value] = true
		}
	}
	return set
}

// unionSorted returns every name held by a or b, once each, in sorted order.
// The union is built through a map, so it has to be sorted before it is
// returned: a report's line order must not follow Go's map iteration, which
// differs from run to run.
func unionSorted(a, b []string) []string {
	set := make(map[string]bool, len(a)+len(b))
	for _, s := range a {
		set[s] = true
	}
	for _, s := range b {
		set[s] = true
	}
	out := make([]string, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}
