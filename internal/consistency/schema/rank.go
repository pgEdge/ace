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

// Rank names a kind of structural difference. Each one must be derivable
// from the two definitions alone — never from a guess about behaviour,
// data, or replication. These are plain strings, not an enum with severity
// baked in: how alarming each Rank is belongs in configuration, not here.
const (
	// RankAbsent: the object (or property) exists on one node and not the
	// other.
	RankAbsent = "absent"
	// RankIncompatible: both sides have something, and neither's set of
	// possible values contains the other's.
	RankIncompatible = "incompatible"
	// RankNarrowed: one side's set of possible values is a strict subset of
	// the other's. NarrowSide names the narrow side.
	RankNarrowed = "narrowed"
	// RankEquivalentDiffering: the same values fit on both sides, but they
	// are represented or handled differently (a different collation on an
	// identical type, a different operator class, Spock's delta-apply
	// option present on one side and not the other).
	RankEquivalentDiffering = "equivalent-differing"
	// RankCosmetic: does not affect the shape of the data. Nothing
	// collect.go reads today actually produces this rank, since the
	// cosmetic-only properties (column order, constraint names, non-unique
	// indexes, storage parameters, ownership, comments) are either not
	// collected yet or excluded by construction.
	RankCosmetic = "cosmetic"
)

// Well-known built-in type names this package knows how to reason about
// narrowing for, identified by their portable (namespace, typname) pair —
// never by OID. An OID is only stable within one already-running cluster:
// two independently initdb'd instances hand out different OIDs to every
// object created after initdb, so a type's OID differing between nodes says
// nothing about the schema. Type names in pg_catalog do not have this
// problem: PostgreSQL has never renamed int4, varchar, or timestamptz
// between major versions.
//
// This list is deliberately small; a type mismatch not covered here is
// reported as RankIncompatible.
const pgCatalog = "pg_catalog"

const (
	nameInt2       = "int2"
	nameInt4       = "int4"
	nameInt8       = "int8"
	nameFloat4     = "float4"
	nameFloat8     = "float8"
	nameVarchar    = "varchar"
	nameBpchar     = "bpchar"
	nameText       = "text"
	nameDate       = "date"
	nameTimestamp  = "timestamp"
	nameTimestampz = "timestamptz"
)

// qname builds the (namespace, name) pair every type-identity comparison in
// this package keys on.
type qname struct{ namespace, name string }

// pgqname builds the qname of a built-in type, which is every type this file
// knows how to reason about: a user-defined type is compared by its own
// definition (see compareReferencedTypes), not by the narrowing tables here.
func pgqname(name string) qname { return qname{pgCatalog, name} }

var integerWidth = map[qname]int{pgqname(nameInt2): 2, pgqname(nameInt4): 4, pgqname(nameInt8): 8}
var floatWidth = map[qname]int{pgqname(nameFloat4): 4, pgqname(nameFloat8): 8}

// maxTimestampPrecision is the fractional-second precision both timestamp
// types carry when none is declared. PostgreSQL never stores more than this:
// a larger declaration is clamped down to it, with a warning.
const maxTimestampPrecision int32 = 6

// timestampPrecision reports q's effective fractional-second precision, and
// whether q is one of the two timestamp types at all.
//
// An unspecified modifier is stored as -1 and stands for the type's full
// precision, which is maxTimestampPrecision. Normalising it here is what
// keeps "timestamp" and "timestamp(6)" from being read as two different
// types: they are one type written two ways, and PostgreSQL stores the same
// value for either.
func timestampPrecision(q qname, mod int32) (precision int32, ok bool) {
	if q != pgqname(nameTimestamp) && q != pgqname(nameTimestampz) {
		return 0, false
	}
	if mod < 0 {
		return maxTimestampPrecision, true
	}
	return mod, true
}

// classifyTimestamps ranks a difference between two timestamp columns. They
// can differ in the time zone half of the type, in the precision, or in
// both, and the three cases do not get the same answer.
//
// Precision alone is a narrowing. A lower precision holds strictly fewer
// values — every timestamp(3) is exactly representable as timestamp(6) —
// and PostgreSQL rounds the other direction away rather than refusing it, so
// the side with fewer digits is the narrow one.
//
// The time zone half alone is not a narrowing. Both sides hold the same
// instants, but a timestamp with no zone attached does not mean the same
// thing as a timestamptz once a DST boundary is crossed, which is what
// RankEquivalentDiffering is for.
//
// When both halves differ, no single rank states it: the value sets differ
// and the meaning differs. The pair is then reported as incompatible rather
// than as whichever half sounds milder.
func classifyTimestamps(aq qname, aPrec int32, bq qname, bPrec int32, nodeA, nodeB string) (rank, narrowSide string) {
	switch {
	case aPrec == bPrec && aq == bq:
		return "", ""
	case aPrec == bPrec:
		return RankEquivalentDiffering, ""
	case aq != bq:
		return RankIncompatible, ""
	case aPrec < bPrec:
		return RankNarrowed, nodeA
	default:
		return RankNarrowed, nodeB
	}
}

// date against timestamp or timestamptz is deliberately NOT a narrowing,
// although it looks like one. RankNarrowed means one value set is a strict
// subset of the other, and here neither is:
//
//   - a timestamp carries a time of day, which a date cannot represent, so
//     the timestamp side is not contained in the date side;
//   - date reaches 5874897 AD while both timestamp types stop at 294276 AD,
//     so the date side is not contained in the timestamp side either.
//     PostgreSQL refuses '5874897-12-31'::date::timestamp outright, with
//     "date out of range for timestamp".
//
// Two sets that each hold values the other cannot is the definition of
// RankIncompatible, which classifyType reaches by falling through to its
// last line. There is no table here on purpose: an earlier version had one,
// and a table named "dateNarrowsInto" invites the next reader to add a type
// to it without asking whether the subset relation actually holds.

// isTextLike reports whether q is one of the three character types whose
// differences classifyType ranks by declared length, rather than treating
// any change of name as a plain type change.
func isTextLike(q qname) bool {
	return q == pgqname(nameVarchar) || q == pgqname(nameBpchar) || q == pgqname(nameText)
}

// isBpchar reports whether q is bpchar, the one character type that pads a
// value out to its declared length. That padding is why two character types
// of the same length are still not the same thing - see classifyType.
func isBpchar(q qname) bool {
	return q == pgqname(nameBpchar)
}

// textLength returns a text-like type's declared length, and whether it is
// effectively unbounded: bare text, or varchar/char with no length
// modifier recorded (atttypmod < 0). PostgreSQL stores the declared length
// as atttypmod-4 for varchar/bpchar.
func textLength(q qname, mod int32) (length int, unbounded bool) {
	if q == pgqname(nameText) || mod < 4 {
		return 0, true
	}
	return int(mod) - 4, false
}

// classifyType compares a column's declared type between two nodes and
// returns the Rank of the difference, and — for RankNarrowed — which
// node's side is the narrow one. Types are identified purely by their
// portable (namespace, name) pair plus typmod. An empty rank means "no
// meaningful difference"; a caller need not check equality before calling
// this.
func classifyType(aNamespace, aName string, aMod int32, bNamespace, bName string, bMod int32, nodeA, nodeB string) (rank, narrowSide string) {
	aq := qname{aNamespace, aName}
	bq := qname{bNamespace, bName}

	if aq == bq && aMod == bMod {
		return "", ""
	}

	if aPrec, aIsTimestamp := timestampPrecision(aq, aMod); aIsTimestamp {
		if bPrec, bIsTimestamp := timestampPrecision(bq, bMod); bIsTimestamp {
			return classifyTimestamps(aq, aPrec, bq, bPrec, nodeA, nodeB)
		}
	}

	if aw, aok := integerWidth[aq]; aok {
		if bw, bok := integerWidth[bq]; bok {
			return narrowByWidth(aw, bw, nodeA, nodeB)
		}
	}
	if aw, aok := floatWidth[aq]; aok {
		if bw, bok := floatWidth[bq]; bok {
			return narrowByWidth(aw, bw, nodeA, nodeB)
		}
	}
	if isTextLike(aq) && isTextLike(bq) {
		// bpchar (CHAR(n)) blank-pads to its declared length; varchar and
		// text do not. Length is checked first, the same way as for two
		// non-bpchar text-like columns: a shorter declared length (or a
		// bounded length against an unbounded side) still accepts strictly
		// fewer strings, so it is RankNarrowed regardless of which side is
		// bpchar. Only once both sides accept the same strings (equal
		// declared length, or both unbounded) does the bpchar-vs-non-bpchar
		// difference matter on its own: char(5) and varchar(5) accept the
		// same strings but do not store or compare them the same way (a
		// value shorter than 5 gets trailing spaces on the bpchar side
		// only), so that case is RankEquivalentDiffering, not "no
		// difference".
		differsInBpchar := isBpchar(aq) != isBpchar(bq)
		aLen, aUnb := textLength(aq, aMod)
		bLen, bUnb := textLength(bq, bMod)
		switch {
		case aUnb && bUnb:
			if differsInBpchar {
				return RankEquivalentDiffering, ""
			}
			return "", ""
		case aUnb:
			return RankNarrowed, nodeB
		case bUnb:
			return RankNarrowed, nodeA
		case aLen == bLen:
			if differsInBpchar {
				return RankEquivalentDiffering, ""
			}
			return "", ""
		case aLen < bLen:
			return RankNarrowed, nodeA
		default:
			return RankNarrowed, nodeB
		}
	}
	return RankIncompatible, ""
}

// narrowByWidth ranks a difference between two types of one family that
// differ only in how many bytes they hold: every value of the narrower type
// fits the wider one, so the narrow side is named and the rank is
// RankNarrowed. Equal widths mean the two are the same type under two names,
// which is reported as no difference at all.
func narrowByWidth(aWidth, bWidth int, nodeA, nodeB string) (string, string) {
	switch {
	case aWidth == bWidth:
		return "", ""
	case aWidth < bWidth:
		return RankNarrowed, nodeA
	default:
		return RankNarrowed, nodeB
	}
}
