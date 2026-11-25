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
	"fmt"
	"strings"
	"testing"
	"time"
)

type mockRow struct {
	scanArgs []any
	scanErr  error
}

func (m *mockRow) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if len(dest) > 0 && len(m.scanArgs) > 0 {
		/*
		 * TODO: This is a little too simple right now, and it only works for
		 * AvgColumnSize. Need to make it more generic for other functions.
		 */
		if ptr, ok := dest[0].(*int64); ok {
			if val, okVal := m.scanArgs[0].(int64); okVal {
				*ptr = val
			}
		}
	}
	return nil
}

func TestSanitiseIdentifier(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "valid identifier",
			input:   "valid_identifier",
			wantErr: false,
		},
		{
			name:    "valid identifier with numbers",
			input:   "valid_identifier_123",
			wantErr: false,
		},
		{
			name:    "identifier starting with underscore",
			input:   "_valid_identifier",
			wantErr: false,
		},
		{
			name:    "invalid identifier - starts with number",
			input:   "1invalid",
			wantErr: true,
		},
		{
			name:    "invalid identifier - contains special character",
			input:   "invalid-char",
			wantErr: true,
		},
		{
			name:    "invalid identifier - contains space",
			input:   "invalid space",
			wantErr: true,
		},
		{
			name:    "invalid identifier - SQL keyword (lowercase)",
			input:   "select",
			wantErr: false, // Assuming keywords are allowed if they match the regex
		},
		{
			name:    "invalid identifier - SQL keyword (uppercase)",
			input:   "TABLE",
			wantErr: false, // Assuming keywords are allowed if they match the regex
		},
		{
			name:    "empty string",
			input:   "",
			wantErr: true,
		},
		{
			name:    "identifier with only numbers",
			input:   "123",
			wantErr: true,
		},
		{
			name:    "identifier with special char at end",
			input:   "id$",
			wantErr: true,
		},
		{
			name:    "sql injection attempt 1",
			input:   "id; DROP TABLE users;",
			wantErr: true,
		},
		{
			name:    "sql injection attempt 2",
			input:   "id OR '1'='1';",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SanitiseIdentifier(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("SanitiseIdentifier(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
		})
	}
}

func TestCommitTimestampFilter(t *testing.T) {
	t.Run("nil returns empty string", func(t *testing.T) {
		got := CommitTimestampFilter(nil)
		if got != "" {
			t.Errorf("expected empty string, got %q", got)
		}
	})

	t.Run("non-nil returns predicate including frozen row handling", func(t *testing.T) {
		ts := time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC)
		got := CommitTimestampFilter(&ts)
		if !strings.Contains(got, "IS NULL") {
			t.Errorf("expected frozen-row NULL check, got %q", got)
		}
		if !strings.Contains(got, "2026-04-15T12:00:00Z") {
			t.Errorf("expected formatted timestamp, got %q", got)
		}
		if !strings.Contains(got, "pg_xact_commit_timestamp(xmin) <=") {
			t.Errorf("expected upper-bound comparison, got %q", got)
		}
	})
}

func normalizeSQLWhitespace(s string) string {
	s = strings.Join(strings.Fields(s), " ")
	s = strings.ReplaceAll(s, "( ", "(")
	s = strings.ReplaceAll(s, " )", ")")
	return s
}

// blockHashSQL builds the block hash query of a block with or without each
// bound, in the same way as table-diff builds it. Every key column has the
// cast type bigint.
func blockHashSQL(schema, table string, key []string, mode string, lower, upper bool, filter string, cols []string) (string, error) {
	types := make(map[string]string, len(key))
	bound := make([]string, len(key))
	for i, k := range key {
		types[k] = "bigint"
		bound[i] = "1"
	}
	var r KeyRange
	if lower {
		r.Lower = bound
	}
	if upper {
		r.Upper = bound
	}
	where, _, err := BlockWhere(key, types, r, filter)
	if err != nil {
		return "", err
	}
	return BlockHashSQL(schema, table, mode, where, cols, HashFormBytes)
}

func TestBlockHashSQL(t *testing.T) {
	tests := []struct {
		name              string
		schema            string
		table             string
		primaryKeyCols    []string
		allCols           []string
		includeLower      bool
		includeUpper      bool
		filter            string
		wantQueryContains []string
		wantErr           bool
	}{
		{
			name:           "nil cols is an error",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			allCols:        nil,
			includeLower:   true,
			includeUpper:   true,
			wantErr:        true,
		},
		{
			name:           "with columns",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			allCols:        []string{"event_id", "name", "amount"},
			includeLower:   true,
			includeUpper:   true,
			wantQueryContains: []string{
				`FROM "public"."events" AS _tbl_`,
				`WHERE "event_id" >= $1::bigint AND "event_id" < $2::bigint`,
				`SELECT encode(CASE WHEN count(*) = 0 THEN decode(repeat('00', 32), 'hex') ELSE sha256(convert_to(count(*)::text || ',' || sum(hashint8extended(`,
				`hashtextextended(_tbl_."amount"::text COLLATE "C", 1)`,
				`hashtextextended(_tbl_."event_id"::text COLLATE "C", 2)`,
				`hashtextextended(_tbl_."name"::text COLLATE "C", 3)`,
				`, 'hex'), count(*)`,
			},
		},
		{
			name:           "composite primary key with columns",
			schema:         "commerce",
			table:          "line_items",
			primaryKeyCols: []string{"order_id", "item_seq"},
			allCols:        []string{"order_id", "item_seq", "price"},
			includeLower:   true,
			includeUpper:   true,
			wantQueryContains: []string{
				`FROM "commerce"."line_items" AS _tbl_`,
				`WHERE ROW("order_id", "item_seq") >= ROW($1::bigint, $2::bigint) AND ROW("order_id", "item_seq") < ROW($3::bigint, $4::bigint)`,
				`hashtextextended(_tbl_."item_seq"::text COLLATE "C", 1)`,
			},
		},
		{
			name:           "invalid schema identifier",
			schema:         "bad-schema!",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			wantErr:        true,
		},
		{
			name:           "invalid table identifier",
			schema:         "public",
			table:          "events 123",
			primaryKeyCols: []string{"event_id"},
			wantErr:        true,
		},
		{
			name:           "empty primary key columns",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{},
			wantErr:        true,
		},
		{
			name:           "valid inputs - with filter",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			allCols:        []string{"event_id", "status"},
			filter:         "status = 'live'",
			wantQueryContains: []string{
				`WHERE (status = 'live')`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := blockHashSQL(tt.schema, tt.table, tt.primaryKeyCols, "TD_BLOCK_HASH" /* mode */, tt.includeLower, tt.includeUpper, tt.filter, tt.allCols)

			if (err != nil) != tt.wantErr {
				t.Errorf("BlockHashSQL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				for _, substr := range tt.wantQueryContains {
					if !strings.Contains(query, substr) {
						t.Errorf("BlockHashSQL() query = %q, want to contain %q", query, substr)
					}
				}
				// The block hash does not depend on row order, so the query
				// needs no ORDER BY.
				if strings.Contains(query, "ORDER BY") {
					t.Errorf("BlockHashSQL() query = %q, must not sort", query)
				}
			}
		})
	}
}

func TestBlockHashSQLLeafMode(t *testing.T) {
	query, err := blockHashSQL("public", "events", []string{"event_id"}, "MTREE_LEAF_HASH",
		true, false, "", []string{"event_id", "name"})
	if err != nil {
		t.Fatalf("BlockHashSQL() error = %v", err)
	}
	// The leaf hash is stored as raw bytes, so it is not hex-encoded.
	if !strings.HasPrefix(strings.TrimSpace(query), "SELECT CASE WHEN count(*) = 0") {
		t.Errorf("leaf hash must be the raw aggregate: %q", query)
	}
	if !strings.Contains(query, `WHERE "event_id" >= $1::bigint`) || strings.Contains(query, "<") {
		t.Errorf("expected only a lower bound: %q", query)
	}
}

// TestBlockHashSQLHasNoWholeBlockString: the block hash must not build one
// value out of all rows of the block, because one value is limited to 1 GB.
// The aggregate state is one count and one sum, and it needs no row order.
// The sum is not cast back to bigint, so it does not overflow.
func TestBlockHashSQLHasNoWholeBlockString(t *testing.T) {
	cols := []string{"id", "name", "amount"}
	for _, mode := range []string{"TD_BLOCK_HASH", "MTREE_LEAF_HASH"} {
		for _, bounds := range []struct{ lower, upper bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
			for _, pk := range [][]string{{"id"}, {"id", "name"}} {
				name := fmt.Sprintf("%s/lower=%v/upper=%v/pk=%d", mode, bounds.lower, bounds.upper, len(pk))
				t.Run(name, func(t *testing.T) {
					query, err := blockHashSQL("public", "t", pk, mode, bounds.lower, bounds.upper, `"amount" > 0`, cols)
					if err != nil {
						t.Fatalf("BlockHashSQL() error = %v", err)
					}
					for _, bad := range []string{"string_agg", "array_agg", "ORDER BY", "digest(", "bit(256)", "bit_xor", ")::bigint)"} {
						if strings.Contains(query, bad) {
							t.Errorf("query must not contain %q: %q", bad, query)
						}
					}
					if !strings.Contains(query, "sum(hashint8extended(") {
						t.Errorf("query = %q, want the sum of the row hashes", query)
					}
				})
			}
		}
	}
}

func TestNullMark(t *testing.T) {
	// The constants are a part of the hash version: they must not change.
	if got := nullMark(1); got != -2152535657050944081 {
		t.Errorf("nullMark(1) = %d", got)
	}
	if got := nullMark(2); got != 7960286522194355700 {
		t.Errorf("nullMark(2) = %d", got)
	}
}

func TestRowHashExpr(t *testing.T) {
	mustExpr := func(t *testing.T, alias string, cols []string, form HashForm) string {
		t.Helper()
		got, err := RowHashExpr(alias, cols, form)
		if err != nil {
			t.Fatalf("RowHashExpr() error = %v", err)
		}
		return got
	}

	t.Run("bytes form, columns by name", func(t *testing.T) {
		got := mustExpr(t, "", []string{"id", "debit"}, HashFormBytes)
		want := fmt.Sprintf(`hashint8extended(%d, COALESCE(hashtextextended("debit"::text COLLATE "C", 1), %d) # `+
			`COALESCE(hashtextextended("id"::text COLLATE "C", 2), %d))`, CurrentHashVersion, nullMark(1), nullMark(2))
		if got != want {
			t.Errorf("RowHashExpr() = %q, want %q", got, want)
		}
	})

	t.Run("UTF8 form", func(t *testing.T) {
		got := mustExpr(t, "_tbl_", []string{"a"}, HashFormUTF8)
		want := fmt.Sprintf(`hashint8extended(%d, COALESCE(CASE WHEN _tbl_."a"::text IS NOT NULL THEN `+
			`hash_record_extended(ROW(convert_to(_tbl_."a"::text, 'UTF8')), 1) END, %d))`, CurrentHashVersion, nullMark(1))
		if got != want {
			t.Errorf("RowHashExpr() = %q, want %q", got, want)
		}
	})

	t.Run("column order does not matter", func(t *testing.T) {
		a := mustExpr(t, "_tbl_", []string{"x", "b", "Name"}, HashFormBytes)
		b := mustExpr(t, "_tbl_", []string{"Name", "x", "b"}, HashFormBytes)
		if a != b {
			t.Errorf("RowHashExpr() depends on the column order: %q vs %q", a, b)
		}
	})

	t.Run("no columns is an error", func(t *testing.T) {
		if got, err := RowHashExpr("_tbl_", nil, HashFormBytes); err == nil {
			t.Errorf("RowHashExpr() = %q, want an error", got)
		}
	})

	t.Run("unknown form is an error", func(t *testing.T) {
		if got, err := RowHashExpr("_tbl_", []string{"a"}, HashForm(7)); err == nil {
			t.Errorf("RowHashExpr() = %q, want an error", got)
		}
	})

	t.Run("wide table", func(t *testing.T) {
		// The columns are joined by an operator, not by function
		// arguments, so the limit of 100 arguments does not apply.
		cols := make([]string, 250)
		for i := range cols {
			cols[i] = fmt.Sprintf("col%03d", i)
		}
		got := mustExpr(t, "_tbl_", cols, HashFormBytes)
		if strings.Count(got, "hashtextextended(") != 250 {
			t.Errorf("expected 250 column hashes, got: %s", got)
		}
	})
}

func TestHashBasis(t *testing.T) {
	tests := []struct {
		encodings []string
		want      string
	}{
		{[]string{"UTF8", "UTF8"}, "UTF8"},
		{[]string{"LATIN1", "LATIN1"}, "LATIN1"},
		{[]string{"LATIN1", "UTF8"}, "UTF8"},
		{[]string{"EUC_JP", "LATIN1"}, "UTF8"},
		{[]string{"SQL_ASCII", "SQL_ASCII"}, "SQL_ASCII"},
		{[]string{"SQL_ASCII", "LATIN1"}, "LATIN1"},
		{[]string{"SQL_ASCII", "UTF8", "LATIN1"}, "UTF8"},
		{[]string{"utf8"}, "UTF8"},
	}
	for _, tt := range tests {
		if got := HashBasis(tt.encodings); got != tt.want {
			t.Errorf("HashBasis(%v) = %q, want %q", tt.encodings, got, tt.want)
		}
	}
}

func TestHashForms(t *testing.T) {
	basis, forms, warnings, err := HashForms(map[string]string{"n1": "UTF8", "n2": "EUC_JP", "n3": "SQL_ASCII"})
	if err != nil {
		t.Fatal(err)
	}
	if basis != "UTF8" {
		t.Errorf("basis = %q", basis)
	}
	want := map[string]HashForm{"n1": HashFormBytes, "n2": HashFormUTF8, "n3": HashFormBytes}
	for n, f := range want {
		if got, ok := forms[n]; !ok || got != f {
			t.Errorf("form of %s = %d (present %v), want %d", n, got, ok, f)
		}
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "node n2") || !strings.Contains(warnings[0], "EUC_JP") {
		t.Errorf("warnings = %q", warnings)
	}
	if _, err := HashFormFor("UTF8", "LATIN1"); err == nil {
		t.Error("HashFormFor(UTF8, LATIN1) must fail")
	}
}

func TestColExpr(t *testing.T) {
	tests := []struct {
		alias, col string
		want       string
	}{
		{"", "id", `"id"`},
		{"_tbl_", "id", `_tbl_."id"`},
		{"", "Name", `"Name"`},
		{"", `we"ird`, `"we""ird"`},
		{"", "price", `"price"`},
	}
	for _, tt := range tests {
		t.Run(tt.alias+"."+tt.col, func(t *testing.T) {
			if got := ColExpr(tt.alias, tt.col); got != tt.want {
				t.Errorf("ColExpr(%q, %q) = %q, want %q", tt.alias, tt.col, got, tt.want)
			}
		})
	}
}

func TestTextExprAndKeyMatchCond(t *testing.T) {
	if got := TextExpr("t", "id"); got != `t."id"::text` {
		t.Errorf("TextExpr() = %q", got)
	}
	if got := KeyMatchCond("", "id", "$1::integer"); got != `"id"::text COLLATE "C" = ($1::integer)::text` {
		t.Errorf("KeyMatchCond() = %q", got)
	}
}

// The row hash takes each column as its value::text, the text by which
// rows are compared and matched.
func TestRowHashExprUsesTextExpr(t *testing.T) {
	cols := []string{"id", "price", "prices", "Name"}
	got, err := RowHashExpr("_tbl_", cols, HashFormBytes)
	if err != nil {
		t.Fatalf("RowHashExpr() error = %v", err)
	}
	for _, c := range cols {
		if !strings.Contains(got, TextExpr("_tbl_", c)+` COLLATE "C"`) {
			t.Errorf("RowHashExpr() = %q, want %s", got, TextExpr("_tbl_", c))
		}
	}
}

// TestSortColumns: the order of columns is the byte order of the UTF8
// names, the same on every node whatever its collation, and the input is
// not changed.
func TestSortColumns(t *testing.T) {
	in := []string{"id", "Zeta", "é", "a b", "_x", "b"}
	got := SortColumns(in)
	want := []string{"Zeta", "_x", "a b", "b", "id", "é"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("SortColumns(%q) = %q, want %q", in, got, want)
	}
	if in[0] != "id" {
		t.Fatalf("SortColumns changed its input: %q", in)
	}
}
