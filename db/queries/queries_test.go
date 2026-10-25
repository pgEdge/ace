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
	"text/template"
	"time"

	"github.com/stretchr/testify/require"
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

// TestBlockHashSQLHasNoWholeBlockString guards the fix for "out of memory"
// (SQLSTATE 54000) on large blocks: the block hash must not build one value
// out of all rows of the block. string_agg did that, and its result is
// limited to 1 GB. The aggregate state of the block hash is one count and
// four sums, and it needs no row order.
func TestBlockHashSQLHasNoWholeBlockString(t *testing.T) {
	cols := []string{"id", "name", "amount"}
	colTypes := map[string]string{"id": "integer", "name": "text", "amount": "numeric"}
	for _, mode := range []string{"TD_BLOCK_HASH", "MTREE_LEAF_HASH"} {
		for _, bounds := range []struct{ lower, upper bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
			for _, pk := range [][]string{{"id"}, {"id", "name"}} {
				name := fmt.Sprintf("%s/lower=%v/upper=%v/pk=%d", mode, bounds.lower, bounds.upper, len(pk))
				t.Run(name, func(t *testing.T) {
					query, err := BlockHashSQL("public", "t", pk, mode, bounds.lower, bounds.upper, `"amount" > 0`, cols, colTypes)
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

func TestColExpr(t *testing.T) {
	tests := []struct {
		alias, col, colType string
		want                string
	}{
		{"", "id", "integer", `"id"`},
		{"_tbl_", "id", "integer", `_tbl_."id"`},
		{"", "Name", "text", `"Name"`},
		{"", `we"ird`, "text", `"we""ird"`},
		{"", "price", "numeric(10,2)", `trim_scale("price")`},
		{"_tbl_", "price", "DECIMAL", `trim_scale(_tbl_."price")`},
		// trim_scale has no array variant.
		{"", "prices", "numeric[]", `"prices"`},
		// Unknown type: the plain column.
		{"", "c", "", `"c"`},
	}
	for _, tt := range tests {
		t.Run(tt.alias+"."+tt.col+":"+tt.colType, func(t *testing.T) {
			if got := ColExpr(tt.alias, tt.col, tt.colType); got != tt.want {
				t.Errorf("ColExpr(%q, %q, %q) = %q, want %q", tt.alias, tt.col, tt.colType, got, tt.want)
			}
		})
	}
}

// The row hash must take each column in the same form as the row fetch.
func TestRowHashExprUsesColExpr(t *testing.T) {
	cols := []string{"id", "price", "prices", "Name"}
	colTypes := map[string]string{"id": "integer", "price": "numeric", "prices": "numeric[]", "Name": "text"}
	got, err := RowHashExpr("_tbl_", cols, colTypes)
	if err != nil {
		t.Fatalf("RowHashExpr() error = %v", err)
	}
	exprs := make([]string, len(cols))
	for i, c := range cols {
		exprs[i] = ColExpr("_tbl_", c, colTypes[c])
	}
	want := "sha256(convert_to(ROW(" + strings.Join(exprs, ", ") + ")::text, 'UTF8'))"
	if got != want {
		t.Errorf("RowHashExpr() = %q, want %q", got, want)
	}
}

// Complex and unknown types are fetched as ::TEXT so their Postgres text form
// round-trips through the diff report; known scalars stay native. A scalar
// numeric is wrapped in trim_scale, as in the row hash.
func TestSelectColExpr(t *testing.T) {
	tests := []struct {
		colType string
		want    string
	}{
		{"integer", `"c"`},
		{"timestamp without time zone", `"c"`},
		{"timestamp(3) with time zone", `"c"`},
		{"numeric(10,2)", `trim_scale("c") AS "c"`},
		{"numeric", `trim_scale("c") AS "c"`},
		{"numeric(10,2)[]", `"c"::TEXT AS "c"`},
		{"date", `"c"`},
		{"uuid", `"c"`},
		{"integer[]", `"c"::TEXT AS "c"`},
		{"jsonb", `"c"::TEXT AS "c"`},
		{"bytea", `"c"::TEXT AS "c"`},
		{"point", `"c"::TEXT AS "c"`},
		{"int4range", `"c"::TEXT AS "c"`},
		// "daterange" must not be mistaken for "date" (nor "datemultirange",
		// nor a UDT that happens to share a scalar's name as a prefix).
		{"daterange", `"c"::TEXT AS "c"`},
		{"datemultirange", `"c"::TEXT AS "c"`},
		{"date_status_enum", `"c"::TEXT AS "c"`},
		{"mood_enum", `"c"::TEXT AS "c"`},
		{"xml", `"c"::TEXT AS "c"`},
		{"bit(8)", `"c"::TEXT AS "c"`},
		{"inet", `"c"::TEXT AS "c"`},
		{"time with time zone", `"c"::TEXT AS "c"`},
	}
	for _, tt := range tests {
		t.Run(tt.colType, func(t *testing.T) {
			require.Equal(t, tt.want, SelectColExpr("", "c", tt.colType))
		})
	}

	// A qualified column must still give a result column with the name of the
	// column, and the name must be quoted.
	qualified := []struct {
		colType string
		want    string
	}{
		{"integer", `t."Name"`},
		{"numeric", `trim_scale(t."Name") AS "Name"`},
		{"jsonb", `t."Name"::TEXT AS "Name"`},
	}
	for _, tt := range qualified {
		t.Run("qualified/"+tt.colType, func(t *testing.T) {
			require.Equal(t, tt.want, SelectColExpr("t", "Name", tt.colType))
		})
	}
}

// The fetch must start from the expression of the row hash: a fetched value
// is then the value that was hashed.
func TestFetchColExprStartsFromColExpr(t *testing.T) {
	for _, colType := range []string{"integer", "text", "numeric", "numeric(12,4)", "DECIMAL", "numeric[]", "jsonb", "inet", ""} {
		t.Run(colType, func(t *testing.T) {
			hashExpr := ColExpr("_tbl_", "c", colType)
			require.True(t, strings.HasPrefix(FetchColExpr("_tbl_", "c", colType), hashExpr),
				"FetchColExpr must start with %q", hashExpr)
		})
	}
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
		// A type whose name only starts like numeric, such as an enum.
		{"numeric_level", false},
		{"decimal_kind", false},
		{"numericx", false},
	}

	for _, tt := range tests {
		t.Run(tt.colType, func(t *testing.T) {
			if got := isNumericType(tt.colType); got != tt.want {
				t.Errorf("isNumericType(%q) = %v, want %v", tt.colType, got, tt.want)
			}
		})
	}
}

func TestNumericColumns(t *testing.T) {
	got := NumericColumns(map[string]string{
		"id":     "integer",
		"price":  "numeric(10,2)",
		"amount": "DECIMAL",
		"prices": "numeric[]",
		"note":   "text",
	})
	want := map[string]bool{"price": true, "amount": true}
	if len(got) != len(want) {
		t.Fatalf("NumericColumns() = %v, want %v", got, want)
	}
	for col := range want {
		if !got[col] {
			t.Errorf("NumericColumns() = %v, want %v", got, want)
		}
	}
	if got := NumericColumns(nil); got != nil {
		t.Errorf("NumericColumns(nil) = %v, want nil", got)
	}
}

// A block with no lower bound reaches the max-value queries with no start key.
// They must then look at the whole table, not compare the key with NULL.
func TestGetMaxValOpenStart(t *testing.T) {
	cases := []struct {
		name     string
		tmpl     *template.Template
		data     map[string]any
		hasWhere bool
	}{
		{"simple/open", SQLTemplates.GetMaxValSimple,
			map[string]any{"SchemaIdent": `"public"`, "TableIdent": `"t"`, "Key": `"id"`, "HasStart": false}, false},
		{"simple/bounded", SQLTemplates.GetMaxValSimple,
			map[string]any{"SchemaIdent": `"public"`, "TableIdent": `"t"`, "Key": `"id"`, "HasStart": true}, true},
		{"composite/open", SQLTemplates.GetMaxValComposite,
			map[string]any{"SchemaIdent": `"public"`, "TableIdent": `"t"`, "PkeyCols": `"a", "b"`, "PkeyValues": "ROW()", "HasStart": false}, false},
		{"composite/bounded", SQLTemplates.GetMaxValComposite,
			map[string]any{"SchemaIdent": `"public"`, "TableIdent": `"t"`, "PkeyCols": `"a", "b"`, "PkeyValues": "ROW($1, $2)", "HasStart": true}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := RenderSQL(tc.tmpl, tc.data)
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			if got := strings.Contains(sql, "WHERE"); got != tc.hasWhere {
				t.Errorf("WHERE present = %v, want %v:\n%s", got, tc.hasWhere, sql)
			}
		})
	}
}
