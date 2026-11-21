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
	return BlockHashSQL(schema, table, mode, where, cols)
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
			// The whole-row value would be a second row encoding.
			name:           "nil cols is an error",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			allCols:        nil,
			includeLower:   true,
			includeUpper:   true,
			filter:         "",
			wantErr:        true,
		},
		{
			name:           "with columns - row constructor",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			allCols:        []string{"event_id", "name", "amount"},
			includeLower:   true,
			includeUpper:   true,
			filter:         "",
			wantQueryContains: []string{
				`FROM "public"."events" AS _tbl_`,
				`WHERE "event_id" >= $1::bigint AND "event_id" < $2::bigint`,
				`SELECT encode(` + BlockHashAggExpr + `, 'hex')`,
				`SELECT ('x' || encode(sha256(convert_to(ROW(_tbl_."event_id", _tbl_."name", _tbl_."amount")::text, 'UTF8')), 'hex'))::bit(256) AS _rh`,
				`OFFSET 0`,
			},
			wantErr: false,
		},
		{
			name:           "composite primary key with columns",
			schema:         "commerce",
			table:          "line_items",
			primaryKeyCols: []string{"order_id", "item_seq"},
			allCols:        []string{"order_id", "item_seq", "price"},
			includeLower:   true,
			includeUpper:   true,
			filter:         "",
			wantQueryContains: []string{
				`FROM "commerce"."line_items" AS _tbl_`,
				`WHERE ROW("order_id", "item_seq") >= ROW($1::bigint, $2::bigint) AND ROW("order_id", "item_seq") < ROW($3::bigint, $4::bigint)`,
				`('x' || encode(sha256(convert_to(ROW(_tbl_."order_id", _tbl_."item_seq", _tbl_."price")::text, 'UTF8')), 'hex'))::bit(256)`,
			},
			wantErr: false,
		},
		{
			name:           "invalid schema identifier",
			schema:         "bad-schema!",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			filter:         "",
			wantErr:        true,
		},
		{
			name:           "invalid table identifier",
			schema:         "public",
			table:          "events 123",
			primaryKeyCols: []string{"event_id"},
			filter:         "",
			wantErr:        true,
		},
		{
			name:           "empty primary key columns",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{},
			filter:         "",
			wantErr:        true, // We need this to error out here
		},
		{
			name:           "valid inputs - with filter",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			allCols:        []string{"event_id", "status"},
			includeLower:   false,
			includeUpper:   false,
			filter:         "status = 'live'",
			wantQueryContains: []string{
				`WHERE (status = 'live')`,
			},
			wantErr: false,
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
				if query == "" {
					t.Errorf("BlockHashSQL() returned an empty query string, expected a query")
				}
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
	for _, want := range []string{
		`SELECT ` + BlockHashAggExpr + "\n",
		`SELECT ('x' || encode(sha256(convert_to(ROW(_tbl_."event_id", _tbl_."name")::text, 'UTF8')), 'hex'))::bit(256) AS _rh`,
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query = %q, want to contain %q", query, want)
		}
	}
	if strings.HasPrefix(strings.TrimSpace(query), "SELECT encode(") {
		t.Errorf("leaf hash must not be hex-encoded: %q", query)
	}
	if !strings.Contains(query, `WHERE "event_id" >= $1::bigint`) || strings.Contains(query, "<") {
		t.Errorf("expected only a lower bound: %q", query)
	}
}

// TestBlockHashSQLHasNoWholeBlockString guards the fix for "out of memory"
// (SQLSTATE 54000) on large blocks: the block hash must not build one value
// out of all rows of the block. string_agg did that, and its result is
// limited to 1 GB. The aggregate state of the block hash is one count and
// four sums, and it needs no row order.
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
					for _, bad := range []string{"string_agg", "array_agg", "ORDER BY", "digest(", "EMPTY_BLOCK"} {
						if strings.Contains(query, bad) {
							t.Errorf("query must not contain %q: %q", bad, query)
						}
					}
					if !strings.Contains(query, BlockHashAggExpr) {
						t.Errorf("query = %q, want the aggregate BlockHashAggExpr", query)
					}
				})
			}
		}
	}
}

func TestRowHashExpr(t *testing.T) {
	mustExpr := func(t *testing.T, alias string, cols []string) string {
		t.Helper()
		got, err := RowHashExpr(alias, cols)
		if err != nil {
			t.Fatalf("RowHashExpr() error = %v", err)
		}
		return got
	}

	t.Run("qualified columns, numeric as it is", func(t *testing.T) {
		got := mustExpr(t, "_tbl_", []string{"id", "Name", "price"})
		want := `sha256(convert_to(ROW(_tbl_."id", _tbl_."Name", _tbl_."price")::text, 'UTF8'))`
		if got != want {
			t.Errorf("RowHashExpr() = %q, want %q", got, want)
		}
	})

	t.Run("empty alias gives unqualified columns", func(t *testing.T) {
		got := mustExpr(t, "", []string{"id", "debit"})
		want := `sha256(convert_to(ROW("id", "debit")::text, 'UTF8'))`
		if got != want {
			t.Errorf("RowHashExpr() = %q, want %q", got, want)
		}
	})

	t.Run("no columns is an error", func(t *testing.T) {
		// The whole-row value would list the columns in the physical order
		// instead of the order of the column list.
		if got, err := RowHashExpr("_tbl_", nil); err == nil {
			t.Errorf("RowHashExpr() = %q, want an error", got)
		}
	})

	t.Run("hash does not depend on encoding or byte order", func(t *testing.T) {
		// convert_to makes the bytes UTF8 on every node; sha256 is defined
		// on bytes, not on machine words.
		got := mustExpr(t, "_tbl_", []string{"a"})
		for _, need := range []string{"convert_to(", "'UTF8'", "sha256("} {
			if !strings.Contains(got, need) {
				t.Errorf("RowHashExpr() = %q, want to contain %q", got, need)
			}
		}
		if strings.Contains(got, "hashtext") {
			t.Errorf("RowHashExpr() = %q, must not use a byte-order dependent hash", got)
		}
	})

	t.Run("no NULL or separator handling in the encoding", func(t *testing.T) {
		// ROW()::text keeps NULL and '' apart and quotes delimiters itself.
		// COALESCE or a hand-made separator would bring the collisions back.
		got := mustExpr(t, "_tbl_", []string{"a", "b"})
		for _, bad := range []string{"COALESCE", "concat_ws", "'|'"} {
			if strings.Contains(got, bad) {
				t.Errorf("RowHashExpr() = %q, must not contain %q", got, bad)
			}
		}
	})

	t.Run("wide table is one row constructor", func(t *testing.T) {
		// ROW() is not a function call, so the limit of 100 arguments does
		// not apply to it.
		cols := make([]string, 250)
		for i := range cols {
			cols[i] = fmt.Sprintf("col%d", i)
		}
		got := mustExpr(t, "_tbl_", cols)
		if strings.Count(got, "ROW(") != 1 {
			t.Errorf("expected one ROW() for 250 columns, got: %s", got)
		}
		for _, c := range cols {
			if !strings.Contains(got, `_tbl_."`+c+`"`) {
				t.Errorf("missing column %s in %s", c, got)
			}
		}
	})
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
		// A numeric column is not wrapped in trim_scale(): 1.5 and 1.50
		// are different values.
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

// The row hash must take each column in the same form as the row fetch.
func TestRowHashExprUsesColExpr(t *testing.T) {
	cols := []string{"id", "price", "prices", "Name"}
	got, err := RowHashExpr("_tbl_", cols)
	if err != nil {
		t.Fatalf("RowHashExpr() error = %v", err)
	}
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = ColExpr("_tbl_", c)
	}
	want := "sha256(convert_to(ROW(" + strings.Join(exprs, ", ") + ")::text, 'UTF8'))"
	if got != want {
		t.Errorf("RowHashExpr() = %q, want %q", got, want)
	}
}
