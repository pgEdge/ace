package mtree

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pgedge/ace/pkg/types"
)

// mtreeTaskWithDiffState returns a task wired just enough to exercise the diff
// accumulator (appendDiffs) without a database.
func mtreeTaskWithDiffState(maxDiffRows int64) *MerkleTreeTask {
	m := &MerkleTreeTask{MaxDiffRows: maxDiffRows}
	m.Key = []string{"id"}
	m.Cols = []string{"id", "val"}
	m.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary:   types.DiffSummary{DiffRowsCount: make(map[string]int)},
	}
	m.diffRowKeySets = make(map[string]map[string]map[string]struct{})
	m.diffRowCounts = make(map[string]int64)
	return m
}

// node1OnlyRows builds n rows with unique primary keys, all absent from the peer.
func node1OnlyRows(n int) []types.OrderedMap {
	rows := make([]types.OrderedMap, n)
	for i := range rows {
		rows[i] = types.OrderedMap{{Key: "id", Value: fmt.Sprintf("%d", i)}, {Key: "val", Value: "x"}}
	}
	return rows
}

// A diverged table stops collecting at max_diff_rows and marks the report truncated.
func TestMtreeDiffEnforcesMaxDiffRows(t *testing.T) {
	const cap = 5
	m := mtreeTaskWithDiffState(cap)
	work := CompareRangesWorkItem{
		Node1: map[string]any{"Name": "n1"},
		Node2: map[string]any{"Name": "n2"},
	}

	if err := m.appendDiffs("n1/n2", work, node1OnlyRows(20), nil); err != nil {
		t.Fatalf("appendDiffs returned error: %v", err)
	}

	if got := len(m.DiffResult.NodeDiffs["n1/n2"].Rows["n1"]); got != cap {
		t.Errorf("collected %d rows for n1, want the cap of %d", got, cap)
	}
	if !m.DiffResult.Summary.DiffRowLimitReached {
		t.Errorf("expected DiffRowLimitReached=true after exceeding the cap")
	}
}

// A pair with exactly max_diff_rows diffs collects them all and IS marked
// truncated, matching table-diff: reaching the cap is a report-size bound, so we
// warn that additional differences may exist even at the exact boundary.
func TestMtreeDiffExactCapMarksTruncated(t *testing.T) {
	const cap = 5
	m := mtreeTaskWithDiffState(cap)
	work := CompareRangesWorkItem{
		Node1: map[string]any{"Name": "n1"},
		Node2: map[string]any{"Name": "n2"},
	}

	if err := m.appendDiffs("n1/n2", work, node1OnlyRows(cap), nil); err != nil {
		t.Fatalf("appendDiffs returned error: %v", err)
	}

	if got := len(m.DiffResult.NodeDiffs["n1/n2"].Rows["n1"]); got != cap {
		t.Errorf("collected %d rows for n1, want all %d", got, cap)
	}
	if !m.DiffResult.Summary.DiffRowLimitReached {
		t.Errorf("expected DiffRowLimitReached=true when diffs exactly equal the cap")
	}
}

// With no cap configured, every differing row is collected and nothing is flagged truncated.
func TestMtreeDiffNoLimitCollectsAll(t *testing.T) {
	m := mtreeTaskWithDiffState(0)
	work := CompareRangesWorkItem{
		Node1: map[string]any{"Name": "n1"},
		Node2: map[string]any{"Name": "n2"},
	}

	if err := m.appendDiffs("n1/n2", work, node1OnlyRows(20), nil); err != nil {
		t.Fatalf("appendDiffs returned error: %v", err)
	}

	if got := len(m.DiffResult.NodeDiffs["n1/n2"].Rows["n1"]); got != 20 {
		t.Errorf("collected %d rows for n1, want all 20", got)
	}
	if m.DiffResult.Summary.DiffRowLimitReached {
		t.Errorf("did not expect DiffRowLimitReached with no cap configured")
	}
}

// A negative max_diff_rows is rejected before any work runs.
func TestMtreeDiffRejectsNegativeMaxDiffRows(t *testing.T) {
	m := &MerkleTreeTask{MaxDiffRows: -1, SkipDBUpdate: true}
	m.Ctx = context.Background()

	err := m.DiffMtree()
	if err == nil || !strings.Contains(err.Error(), "max_diff_rows must be >= 0") {
		t.Fatalf("expected max_diff_rows validation error, got %v", err)
	}
}

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
		name           string
		schema         string
		table          string
		key            []string
		cols           []string
		whereClause    string
		colTypes       map[string]string
		wantContains   []string
		wantNotContain []string
		wantOrderBy    string
	}{
		{
			name:        "nil colTypes - no trim_scale",
			schema:      "public",
			table:       "orders",
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
			schema:      "public",
			table:       "orders",
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
			schema:      "public",
			table:       "ledger",
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
			schema:      "sales",
			table:       "line_items",
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
			schema:      "public",
			table:       "users",
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
			query, orderBy := buildRowHashQuery(tt.schema, tt.table, tt.key, tt.cols, tt.whereClause, tt.colTypes)

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
