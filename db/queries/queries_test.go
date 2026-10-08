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

func TestGeneratePkeyOffsetsQuery(t *testing.T) {
	tests := []struct {
		name              string
		schema            string
		table             string
		keyColumns        []string
		tableSampleMethod string
		samplePercent     float64
		ntileCount        int
		filter            string
		wantQueryContains []string
		wantErr           bool
	}{
		{
			name:              "valid inputs - single key column",
			schema:            "public",
			table:             "users",
			keyColumns:        []string{"id"},
			tableSampleMethod: "BERNOULLI",
			samplePercent:     10,
			ntileCount:        100,
			filter:            "",
			wantQueryContains: []string{
				`FROM "public"."users"`,
				`TABLESAMPLE BERNOULLI(10)`,
				`ntile(100) OVER (ORDER BY "id")`,
				`"id" AS "range_start_id"`,
				`LEAD("id") OVER (ORDER BY seq, "id") AS "range_end_id"`,
			},
			wantErr: false,
		},
		{
			name:              "valid inputs - composite key columns",
			schema:            "myschema",
			table:             "orders",
			keyColumns:        []string{"customer_id", "order_date"},
			tableSampleMethod: "SYSTEM",
			samplePercent:     5.5,
			ntileCount:        50,
			filter:            "",
			wantQueryContains: []string{
				`FROM "myschema"."orders"`,
				`TABLESAMPLE SYSTEM(5.5)`,
				`ntile(50) OVER (ORDER BY "customer_id", "order_date")`,
				`"customer_id" AS "range_start_customer_id"`,
				`"order_date" AS "range_start_order_date"`,
				`LEAD("customer_id") OVER (ORDER BY seq, "customer_id", "order_date") AS "range_end_customer_id"`,
				`LEAD("order_date") OVER (ORDER BY seq, "customer_id", "order_date") AS "range_end_order_date"`,
			},
			wantErr: false,
		},
		{
			name:              "invalid schema identifier",
			schema:            "invalid-schema",
			table:             "users",
			keyColumns:        []string{"id"},
			tableSampleMethod: "BERNOULLI",
			samplePercent:     10,
			ntileCount:        100,
			wantErr:           true,
		},
		{
			name:              "invalid table identifier",
			schema:            "public",
			table:             "invalid table",
			keyColumns:        []string{"id"},
			tableSampleMethod: "BERNOULLI",
			samplePercent:     10,
			ntileCount:        100,
			wantErr:           true,
		},
		{
			name:              "invalid key column identifier",
			schema:            "public",
			table:             "users",
			keyColumns:        []string{"id;"},
			tableSampleMethod: "BERNOULLI",
			samplePercent:     10,
			ntileCount:        100,
			wantErr:           true,
		},
		{
			name:              "empty key columns",
			schema:            "public",
			table:             "users",
			keyColumns:        []string{},
			tableSampleMethod: "BERNOULLI",
			samplePercent:     10,
			ntileCount:        100,
			filter:            "",
			wantErr:           true, // Assuming empty key columns is an invalid input causing SanitiseIdentifier to err
		},
		{
			name:              "valid inputs - with filter clause",
			schema:            "public",
			table:             "users",
			keyColumns:        []string{"id"},
			tableSampleMethod: "SYSTEM_ROWS",
			samplePercent:     1000,
			ntileCount:        10,
			filter:            `status = 'active'`,
			wantQueryContains: []string{
				`FROM "public"."users"`,
				`WHERE status = 'active'`,
				`TABLESAMPLE SYSTEM_ROWS(1000)`,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := GeneratePkeyOffsetsQuery(
				tt.schema,
				tt.table,
				tt.keyColumns,
				tt.tableSampleMethod,
				tt.samplePercent,
				tt.ntileCount,
				tt.filter,
			)

			if (err != nil) != tt.wantErr {
				t.Errorf("GeneratePkeyOffsetsQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if query == "" {
					t.Errorf("GeneratePkeyOffsetsQuery() returned an empty query string, expected a query")
				}
				normalizedQuery := normalizeSQLWhitespace(query)
				for _, substr := range tt.wantQueryContains {
					normalizedNeedle := normalizeSQLWhitespace(substr)
					if !strings.Contains(normalizedQuery, normalizedNeedle) {
						t.Errorf("GeneratePkeyOffsetsQuery() query = %q, want to contain %q", query, substr)
					}
				}
			}
		})
	}
}

func TestBlockHashSQL(t *testing.T) {
	tests := []struct {
		name              string
		schema            string
		table             string
		primaryKeyCols    []string
		allCols           []string
		colTypes          map[string]string
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
			colTypes:       nil,
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
			colTypes:       map[string]string{"event_id": "integer", "name": "text", "amount": "numeric(10,2)"},
			includeLower:   true,
			includeUpper:   true,
			filter:         "",
			wantQueryContains: []string{
				`FROM "public"."events" AS _tbl_`,
				`WHERE "event_id" >= $1 AND "event_id" < $2`,
				`SELECT encode(` + BlockHashAggExpr + `, 'hex')`,
				`SELECT ('x' || encode(sha256(convert_to(ROW(_tbl_."event_id", _tbl_."name", trim_scale(_tbl_."amount"))::text, 'UTF8')), 'hex'))::bit(256) AS _rh`,
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
			colTypes:       map[string]string{"order_id": "integer", "item_seq": "integer", "price": "decimal"},
			includeLower:   true,
			includeUpper:   true,
			filter:         "",
			wantQueryContains: []string{
				`FROM "commerce"."line_items" AS _tbl_`,
				`WHERE ROW("order_id", "item_seq") >= ROW($1, $2) AND ROW("order_id", "item_seq") < ROW($3, $4)`,
				`('x' || encode(sha256(convert_to(ROW(_tbl_."order_id", _tbl_."item_seq", trim_scale(_tbl_."price"))::text, 'UTF8')), 'hex'))::bit(256)`,
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
			name:           "invalid primary key column identifier",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event-id"},
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
			query, err := BlockHashSQL(tt.schema, tt.table, tt.primaryKeyCols, "TD_BLOCK_HASH" /* mode */, tt.includeLower, tt.includeUpper, tt.filter, tt.allCols, tt.colTypes)

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
	query, err := BlockHashSQL("public", "events", []string{"event_id"}, "MTREE_LEAF_HASH",
		true, false, "", []string{"event_id", "name"}, nil)
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
	if !strings.Contains(query, `WHERE "event_id" >= $1`) || strings.Contains(query, "<") {
		t.Errorf("expected only a lower bound: %q", query)
	}
}

func TestRowHashExpr(t *testing.T) {
	mustExpr := func(t *testing.T, alias string, cols []string, colTypes map[string]string) string {
		t.Helper()
		got, err := RowHashExpr(alias, cols, colTypes)
		if err != nil {
			t.Fatalf("RowHashExpr() error = %v", err)
		}
		return got
	}

	t.Run("qualified columns and trim_scale", func(t *testing.T) {
		got := mustExpr(t, "_tbl_", []string{"id", "Name", "price"},
			map[string]string{"id": "integer", "Name": "text", "price": "numeric(10,2)"})
		want := `sha256(convert_to(ROW(_tbl_."id", _tbl_."Name", trim_scale(_tbl_."price"))::text, 'UTF8'))`
		if got != want {
			t.Errorf("RowHashExpr() = %q, want %q", got, want)
		}
	})

	t.Run("empty alias gives unqualified columns", func(t *testing.T) {
		got := mustExpr(t, "", []string{"id", "debit"}, map[string]string{"debit": "DECIMAL"})
		want := `sha256(convert_to(ROW("id", trim_scale("debit"))::text, 'UTF8'))`
		if got != want {
			t.Errorf("RowHashExpr() = %q, want %q", got, want)
		}
	})

	t.Run("no columns is an error", func(t *testing.T) {
		// The whole-row value would be a second encoding: no trim_scale,
		// and the physical column order instead of the column list.
		if got, err := RowHashExpr("_tbl_", nil, nil); err == nil {
			t.Errorf("RowHashExpr() = %q, want an error", got)
		}
	})

	t.Run("hash does not depend on encoding or byte order", func(t *testing.T) {
		// convert_to makes the bytes UTF8 on every node; sha256 is defined
		// on bytes, not on machine words.
		got := mustExpr(t, "_tbl_", []string{"a"}, nil)
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
		got := mustExpr(t, "_tbl_", []string{"a", "b"}, nil)
		for _, bad := range []string{"COALESCE", "concat_ws", "'|'"} {
			if strings.Contains(got, bad) {
				t.Errorf("RowHashExpr() = %q, must not contain %q", got, bad)
			}
		}
	})

	t.Run("numeric array is not trimmed", func(t *testing.T) {
		// trim_scale(numeric[]) does not exist.
		got := mustExpr(t, "", []string{"id", "vals"}, map[string]string{"vals": "numeric(10,2)[]"})
		if strings.Contains(got, "trim_scale") {
			t.Errorf("RowHashExpr() = %q, must not trim an array", got)
		}
	})

	t.Run("wide table is one row constructor", func(t *testing.T) {
		// ROW() is not a function call, so the limit of 100 arguments does
		// not apply to it.
		cols := make([]string, 250)
		for i := range cols {
			cols[i] = fmt.Sprintf("col%d", i)
		}
		got := mustExpr(t, "_tbl_", cols, nil)
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

func TestIsNumericType(t *testing.T) {
	tests := []struct {
		colType string
		want    bool
	}{
		{"numeric", true},
		{"numeric(10,2)", true},
		{"NUMERIC", true},
		{"decimal", true},
		{"decimal(18,4)", true},
		{"DECIMAL", true},
		{"integer", false},
		{"bigint", false},
		{"text", false},
		{"double precision", false},
		{"real", false},
		{"", false},
		// trim_scale has no array variant.
		{"numeric[]", false},
		{"numeric(10,2)[]", false},
		{"DECIMAL[]", false},
	}

	for _, tt := range tests {
		t.Run(tt.colType, func(t *testing.T) {
			if got := isNumericType(tt.colType); got != tt.want {
				t.Errorf("isNumericType(%q) = %v, want %v", tt.colType, got, tt.want)
			}
		})
	}
}
