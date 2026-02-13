package mtree

import (
	"strings"
	"testing"
)

func TestIsNumericColType(t *testing.T) {
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
	}

	for _, tt := range tests {
		t.Run(tt.colType, func(t *testing.T) {
			got := isNumericColType(tt.colType)
			if got != tt.want {
				t.Errorf("isNumericColType(%q) = %v, want %v", tt.colType, got, tt.want)
			}
		})
	}
}

func TestBuildRowHashQuery(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		key           []string
		cols          []string
		whereClause   string
		colTypes      map[string]string
		wantContains  []string
		wantNotContain []string
		wantOrderBy   string
	}{
		{
			name:        "nil colTypes - no trim_scale",
			tableName:   `"public"."orders"`,
			key:         []string{"id"},
			cols:        []string{"id", "name", "amount"},
			whereClause: "TRUE",
			colTypes:    nil,
			wantContains: []string{
				`SELECT "id", encode(digest(concat_ws('|',`,
				`COALESCE("id"::text, '')`,
				`COALESCE("name"::text, '')`,
				`COALESCE("amount"::text, '')`,
				`,'sha256'),'hex') as row_hash`,
				`FROM "public"."orders"`,
				`WHERE TRUE`,
				`ORDER BY "id"`,
			},
			wantNotContain: []string{
				`trim_scale`,
			},
			wantOrderBy: `"id"`,
		},
		{
			name:        "numeric column gets trim_scale",
			tableName:   `"public"."orders"`,
			key:         []string{"id"},
			cols:        []string{"id", "name", "price"},
			whereClause: "TRUE",
			colTypes:    map[string]string{"id": "integer", "name": "text", "price": "numeric(10,2)"},
			wantContains: []string{
				`COALESCE("id"::text, '')`,
				`COALESCE("name"::text, '')`,
				`COALESCE(trim_scale("price")::text, '')`,
				`encode(digest(concat_ws('|',`,
			},
			wantOrderBy: `"id"`,
		},
		{
			name:        "decimal column gets trim_scale",
			tableName:   `"public"."ledger"`,
			key:         []string{"txn_id"},
			cols:        []string{"txn_id", "debit", "credit"},
			whereClause: `"txn_id" >= $1`,
			colTypes:    map[string]string{"txn_id": "bigint", "debit": "decimal(18,4)", "credit": "DECIMAL"},
			wantContains: []string{
				`COALESCE("txn_id"::text, '')`,
				`COALESCE(trim_scale("debit")::text, '')`,
				`COALESCE(trim_scale("credit")::text, '')`,
				`WHERE "txn_id" >= $1`,
			},
			wantOrderBy: `"txn_id"`,
		},
		{
			name:        "composite primary key",
			tableName:   `"sales"."line_items"`,
			key:         []string{"order_id", "line_num"},
			cols:        []string{"order_id", "line_num", "qty", "unit_price"},
			whereClause: "TRUE",
			colTypes:    map[string]string{"order_id": "integer", "line_num": "integer", "qty": "integer", "unit_price": "numeric"},
			wantContains: []string{
				`SELECT "order_id", "line_num", encode(digest(`,
				`COALESCE(trim_scale("unit_price")::text, '')`,
				`ORDER BY "order_id", "line_num"`,
			},
			wantOrderBy: `"order_id", "line_num"`,
		},
		{
			name:        "no numeric columns - no trim_scale even with colTypes",
			tableName:   `"public"."users"`,
			key:         []string{"user_id"},
			cols:        []string{"user_id", "email", "created_at"},
			whereClause: "TRUE",
			colTypes:    map[string]string{"user_id": "integer", "email": "text", "created_at": "timestamp"},
			wantContains: []string{
				`COALESCE("user_id"::text, '')`,
				`COALESCE("email"::text, '')`,
				`COALESCE("created_at"::text, '')`,
			},
			wantNotContain: []string{
				`trim_scale`,
			},
			wantOrderBy: `"user_id"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, orderBy := buildRowHashQuery(tt.tableName, tt.key, tt.cols, tt.whereClause, tt.colTypes)

			if orderBy != tt.wantOrderBy {
				t.Errorf("orderBy = %q, want %q", orderBy, tt.wantOrderBy)
			}

			for _, substr := range tt.wantContains {
				if !strings.Contains(query, substr) {
					t.Errorf("query missing expected substring %q\nquery: %s", substr, query)
				}
			}
			for _, substr := range tt.wantNotContain {
				if strings.Contains(query, substr) {
					t.Errorf("query should NOT contain %q\nquery: %s", substr, query)
				}
			}
		})
	}
}
