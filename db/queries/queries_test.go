// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package queries

import (
	"strings"
	"testing"
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
			wantErr:           true, // Assuming empty key columns is an invalid input causing SanitiseIdentifier to err
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
		includeLower      bool
		includeUpper      bool
		wantQueryContains []string
		wantErr           bool
	}{
		{
			name:           "valid inputs - single primary key",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event_id"},
			includeLower:   true,
			includeUpper:   true,
			wantQueryContains: []string{
				`FROM "public"."events" AS _tbl_`,
				`ORDER BY "event_id"`,
				`WHERE "event_id" >= $1 AND "event_id" < $2`,
				`encode(digest(COALESCE(string_agg(_tbl_::text, '|' ORDER BY "event_id"), 'EMPTY_BLOCK'), 'sha256'), 'hex')`,
			},
			wantErr: false,
		},
		{
			name:           "valid inputs - composite primary key",
			schema:         "commerce",
			table:          "line_items",
			primaryKeyCols: []string{"order_id", "item_seq"},
			includeLower:   true,
			includeUpper:   true,
			wantQueryContains: []string{
				`FROM "commerce"."line_items" AS _tbl_`,
				`ORDER BY "order_id", "item_seq"`,
				`WHERE ROW("order_id", "item_seq") >= ROW($1, $2) AND ROW("order_id", "item_seq") < ROW($3, $4)`,
				`encode(digest(COALESCE(string_agg(_tbl_::text, '|' ORDER BY "order_id", "item_seq"), 'EMPTY_BLOCK'), 'sha256'), 'hex')`,
			},
			wantErr: false,
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
			name:           "invalid primary key column identifier",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{"event-id"},
			wantErr:        true,
		},
		{
			name:           "empty primary key columns",
			schema:         "public",
			table:          "events",
			primaryKeyCols: []string{},
			wantErr:        true, // We need this to error out here
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := BlockHashSQL(tt.schema, tt.table, tt.primaryKeyCols, "TD_BLOCK_HASH" /* mode */, tt.includeLower, tt.includeUpper)

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
			}
		})
	}
}
