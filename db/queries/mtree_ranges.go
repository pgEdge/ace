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
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/pkg/types"
)

// GetCollations returns, for each column of schema.table that has a
// collation, the qualified and quoted name of that collation. A column of a
// type without collations is not in the result.
func GetCollations(ctx context.Context, db DBQuerier, schema, table string) (map[string]string, error) {
	defs, err := queryCollations(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}
	names := make(map[string]string, len(defs))
	for col, d := range defs {
		names[col] = d.name
	}
	return names, nil
}

// GetCollationOrders returns, for each column of schema.table that has a
// collation, a text that tells collation orders apart across nodes. Two
// collations that order values in the same way give the same text, also when
// the catalog spells their locale in another way.
//
// XXX: The version of the collation library is not compared. Another ICU or
// glibc version on another node can order some values in another way; such a
// difference is not seen.
func GetCollationOrders(ctx context.Context, db DBQuerier, schema, table string) (map[string]string, error) {
	defs, err := queryCollations(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}
	orders := make(map[string]string, len(defs))
	for col, d := range defs {
		orders[col] = d.orderText()
	}
	return orders, nil
}

// collationDef is a collation as the catalog defines it. For the default
// collation provider, collate, locale and rules are these of the database.
type collationDef struct {
	name          string
	isDefault     bool
	provider      string
	collate       string
	locale        string
	rules         string
	deterministic bool
}

// orderText returns the name of the collation and only the settings that
// decide the order of values. A libc collation orders by its LC_COLLATE
// locale. ICU and the builtin provider order by their own locale; the
// LC_COLLATE setting that a database with these providers still has does
// not change the order of text values. A collation other than the default
// one also keeps its qualified name, so that nodes use the same name in SQL.
func (d collationDef) orderText() string {
	name := d.name
	if d.isDefault {
		name = "default"
	}
	var parts []string
	switch d.provider {
	case "c":
		parts = append(parts, "libc "+canonicalLibcLocale(d.collate))
	case "i":
		parts = append(parts, "icu "+d.locale)
		if d.rules != "" {
			parts = append(parts, "rules "+d.rules)
		}
	case "b":
		parts = append(parts, "builtin "+d.locale)
	default:
		parts = append(parts, "provider "+d.provider, "collate "+d.collate, "locale "+d.locale)
		if d.rules != "" {
			parts = append(parts, "rules "+d.rules)
		}
	}
	if !d.deterministic {
		parts = append(parts, "nondeterministic")
	}
	return name + " (" + strings.Join(parts, ", ") + ")"
}

// canonicalLibcLocale writes a libc locale name in one form. The C library
// reads the code set of a name in a normalised form, so en_US.UTF-8,
// en_US.utf8 and en_US.UTF8 are one locale: here the code set keeps only
// letters and digits, in lower case, and a code set of digits only gets the
// prefix "iso", as the C library does. POSIX is another name of C.
func canonicalLibcLocale(locale string) string {
	if locale == "POSIX" {
		return "C"
	}
	name, modifier := locale, ""
	if i := strings.IndexByte(name, '@'); i >= 0 {
		name, modifier = name[:i], name[i:]
	}
	i := strings.IndexByte(name, '.')
	if i < 0 {
		return locale
	}
	var cs strings.Builder
	digitsOnly := true
	for _, r := range name[i+1:] {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			cs.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			cs.WriteRune(r - 'A' + 'a')
		default:
			continue
		}
		if r < '0' || r > '9' {
			digitsOnly = false
		}
	}
	codeset := cs.String()
	if digitsOnly && codeset != "" {
		codeset = "iso" + codeset
	}
	return name[:i] + "." + codeset + modifier
}

// queryCollations returns the collation of each column of schema.table that
// has one.
func queryCollations(ctx context.Context, db DBQuerier, schema, table string) (map[string]collationDef, error) {
	sql, err := RenderSQL(SQLTemplates.GetCollations, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetCollations SQL: %w", err)
	}
	rows, err := db.Query(ctx, sql, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query to get collations failed for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	defs := make(map[string]collationDef)
	for rows.Next() {
		var col string
		var d collationDef
		if err := rows.Scan(&col, &d.name, &d.isDefault, &d.provider, &d.collate,
			&d.locale, &d.rules, &d.deterministic); err != nil {
			return nil, fmt.Errorf("failed to scan collation: %w", err)
		}
		defs[col] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over collations: %w", err)
	}
	return defs, nil
}

// KeyCollationMismatch returns the first key column with another collation in
// a than in b, and both collations; ok is false if there is none. a and b map
// a column to the text that tells collation orders apart, a column without a
// collation to "". With another collation a node orders the key in another
// way, and the same key range holds other rows there.
func KeyCollationMismatch(key []string, a, b map[string]string) (col, collA, collB string, ok bool) {
	for _, k := range key {
		if a[k] != b[k] {
			return k, a[k], b[k], true
		}
	}
	return "", "", "", false
}

// CollationText returns a collation name for a message: the name, or "no
// collation".
func CollationText(coll string) string {
	if coll == "" {
		return "no collation"
	}
	return "collation " + coll
}

// diffRangesSQL returns the query that builds the key ranges to compare from
// the bounds of the leaves that differ on two nodes. Its parameters are one
// text[] for each key column with the lower bounds of the leaves, then one
// text[] for each key column with the upper bounds. Element i of every array
// belongs to leaf i. A NULL bound is an open side of the leaf.
//
// All comparisons run on the server, with the type of each key column. No
// column of the table takes part in them, so a key column with a collation
// gets its own collation by an explicit COLLATE; without it the comparisons
// would use the default collation of the database.
//
// Each row of the result is one range: the lower bound, the upper bound
// (NULL for an open side) and a flag that the rows of the lower bound are
// already in the range before it.
func diffRangesSQL(key []string, castTypes, collations map[string]string) (string, error) {
	n := len(key)
	if n == 0 {
		return "", fmt.Errorf("the key has no columns")
	}
	names := func(prefix string, alias string) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("%s%d", prefix, i+1)
			if alias != "" {
				out[i] = alias + "." + out[i]
			}
		}
		return out
	}
	row := func(cols []string) string {
		if len(cols) == 1 {
			return cols[0]
		}
		return "ROW(" + strings.Join(cols, ", ") + ")"
	}
	nulls := make([]string, n)
	for i := range nulls {
		nulls[i] = "NULL"
	}

	casts := make([]string, 0, 2*n)
	params := make([]string, 0, 2*n)
	for _, side := range []string{"s", "e"} {
		for i, k := range key {
			typ, ok := castTypes[k]
			if !ok || strings.TrimSpace(typ) == "" {
				return "", fmt.Errorf("no cast type for key column %s", k)
			}
			expr := fmt.Sprintf("%s%d::%s", side, i+1, typ)
			if coll, ok := collations[k]; ok {
				expr += " COLLATE " + coll
			}
			casts = append(casts, fmt.Sprintf("%s AS %s%d", expr, side, i+1))
			params = append(params, fmt.Sprintf("$%d::text[]", len(params)+1))
		}
	}
	s, e := names("s", ""), names("e", "")
	rs, re := names("s", "r"), names("e", "r")
	v := names("v", "")
	lo, hi := names("lo", ""), names("hi", "")
	plo, phi := names("lo", "p"), names("hi", "p")

	leads := make([]string, n)
	for i := range leads {
		leads[i] = fmt.Sprintf("lead(%s) OVER w AS %s", v[i], hi[i])
	}
	los := make([]string, n)
	for i := range los {
		los[i] = fmt.Sprintf("%s AS %s", v[i], lo[i])
	}

	// kind is 0 for an open lower bound, 1 for a key and 2 for an open upper
	// bound. An open bound is NULL in the text array, and it is tested there:
	// a value of a composite type with a NULL field is neither NULL nor NOT
	// NULL after the cast. UNION removes keys that are equal for the type,
	// such as 1.0 and 1.00 of a numeric key on two nodes. Two neighbour
	// bounds make one candidate range, and a range stays only if it overlaps
	// a leaf of either node. Keep the overlap test strict: a candidate that
	// only touches a leaf at a bound holds no row of it, and a leaf [s, s)
	// gives no range. The lower bound of a range is in the range before it
	// when that range ends at the same bound.
	return fmt.Sprintf(`
WITH r AS (
	SELECT %[1]s, s1 IS NULL AS s_open, e1 IS NULL AS e_open
	FROM unnest(%[2]s) AS r(%[3]s, %[4]s)
), b(kind, %[8]s) AS (
	SELECT 1, %[3]s FROM r WHERE NOT s_open
	UNION
	SELECT 1, %[4]s FROM r WHERE NOT e_open
	UNION
	SELECT 0, %[5]s WHERE EXISTS (SELECT FROM r WHERE s_open)
	UNION
	SELECT 2, %[5]s WHERE EXISTS (SELECT FROM r WHERE e_open)
), p AS (
	SELECT kind AS lo_kind, %[6]s,
		lead(kind) OVER w AS hi_kind, %[7]s,
		row_number() OVER w AS n
	FROM b
	WINDOW w AS (ORDER BY kind, %[8]s)
), k AS (
	SELECT * FROM p
	WHERE hi_kind IS NOT NULL
		AND EXISTS (SELECT FROM r
			WHERE (r.s_open OR p.hi_kind = 2 OR %[9]s > %[10]s)
				AND (r.e_open OR p.lo_kind = 0 OR %[11]s < %[12]s))
)
SELECT %[13]s, %[14]s,
	coalesce(lo_kind = 1 AND lag(n) OVER (ORDER BY n) = n - 1, false)
FROM k
ORDER BY n`,
		strings.Join(casts, ", "),
		strings.Join(params, ", "),
		strings.Join(s, ", "),
		strings.Join(e, ", "),
		strings.Join(nulls, ", "),
		strings.Join(los, ", "),
		strings.Join(leads, ", "),
		strings.Join(v, ", "),
		row(phi), row(rs),
		row(plo), row(re),
		strings.Join(lo, ", "),
		strings.Join(hi, ", "),
	), nil
}

// MtreeDiffRanges returns the key ranges to compare for the leaves that
// differ on two nodes, in key order. leaves holds the leaves of both nodes;
// their bounds are the texts of the key columns. The query runs on db, with
// castTypes and collations of the key columns on that node.
//
// The ranges cover every key of every leaf. Each range holds the rows of its
// upper bound, and a range that starts where the one before it ends excludes
// the rows of its lower bound, so no row is compared twice.
func MtreeDiffRanges(ctx context.Context, db DBQuerier, key []string, castTypes, collations map[string]string, leaves []types.LeafRange) ([]KeyRange, error) {
	if len(leaves) == 0 {
		return nil, nil
	}
	sql, err := diffRangesSQL(key, castTypes, collations)
	if err != nil {
		return nil, err
	}
	n := len(key)
	args := make([]any, 0, 1+2*n)
	args = append(args, pgx.QueryResultFormats{pgx.TextFormatCode})
	for _, side := range []func(types.LeafRange) []string{
		func(l types.LeafRange) []string { return l.RangeStart },
		func(l types.LeafRange) []string { return l.RangeEnd },
	} {
		for i := 0; i < n; i++ {
			col := make([]*string, len(leaves))
			for j, l := range leaves {
				b := side(l)
				if b == nil {
					continue
				}
				if len(b) != n {
					return nil, fmt.Errorf("a leaf bound has %d values for %d key columns", len(b), n)
				}
				col[j] = &b[i]
			}
			args = append(args, col)
		}
	}

	rows, err := db.Query(ctx, sql, args...) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to build the ranges to compare failed: %w", err)
	}
	defer rows.Close()

	var out []KeyRange
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != 2*n+1 {
			return nil, fmt.Errorf("expected %d columns, got %d", 2*n+1, len(raw))
		}
		r := KeyRange{
			Lower:       textBound(raw[:n]),
			Upper:       textBound(raw[n : 2*n]),
			LowerOpen:   string(raw[2*n]) == "t",
			UpperClosed: true,
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over the ranges to compare: %w", err)
	}
	return out, nil
}

// textBound returns the key texts of one bound, or nil if the bound is open:
// a key column is never NULL, so NULL in the first column means no bound. An
// open bound of a composite key in a leaf can be NULL or a row of NULLs.
func textBound(raw [][]byte) []string {
	if raw[0] == nil {
		return nil
	}
	out := make([]string, len(raw))
	for i, v := range raw {
		out[i] = string(v)
	}
	return out
}
