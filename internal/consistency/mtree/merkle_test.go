package mtree

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgedge/ace/pkg/types"
)

// fakeRows is the smallest pgx.Rows that readRowHashes needs. Scan copies
// values into *any destinations, which is how an untyped scan buffer receives
// the driver's own Go values: a uuid arrives as [16]byte, not as text.
type fakeRows struct {
	rows [][]any
	pos  int
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Next() bool                                   { r.pos++; return r.pos <= len(r.rows) }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

// current returns the row that Next stopped on. A real pgx.Rows returns an
// error when it is read past the last row instead of panicking. Do the same
// here, so that a later test of the error path fails with a message rather
// than an index panic.
func (r *fakeRows) current() ([]any, error) {
	if r.pos < 1 || r.pos > len(r.rows) {
		return nil, fmt.Errorf("read at position %d, outside the %d rows", r.pos, len(r.rows))
	}
	return r.rows[r.pos-1], nil
}

func (r *fakeRows) Values() ([]any, error) { return r.current() }

func (r *fakeRows) Scan(dest ...any) error {
	row, err := r.current()
	if err != nil {
		return err
	}
	if len(dest) != len(row) {
		return fmt.Errorf("scanning into %d destinations, row has %d values", len(dest), len(row))
	}
	for i := range dest {
		p, ok := dest[i].(*any)
		if !ok {
			return fmt.Errorf("dest[%d] is %T, want *any", i, dest[i])
		}
		*p = row[i]
	}
	return nil
}

func uuidBytes(b byte) [16]byte {
	var out [16]byte
	for i := range out {
		out[i] = b
	}
	return out
}

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

// The pkey values that reach the row-fetch query must be the ones pgx decoded,
// not the map key. For a uuid the key prints as "[17 17 ...]", which the server
// rejects as invalid uuid input.
func TestReadRowHashesKeepsScannedPkey(t *testing.T) {
	id := uuidBytes(0x11)
	rows := &fakeRows{rows: [][]any{{id, "hash-a"}}}

	got, err := readRowHashes(rows, 1)
	if err != nil {
		t.Fatalf("readRowHashes returned error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("read %d entries, want 1", len(got))
	}
	for _, e := range got {
		if e.hash != "hash-a" {
			t.Errorf("hash = %q, want %q", e.hash, "hash-a")
		}
		if len(e.pkey) != 1 {
			t.Fatalf("pkey has %d values, want 1", len(e.pkey))
		}
		if e.pkey[0] != id {
			t.Errorf("pkey[0] = %#v (%T), want the scanned [16]byte", e.pkey[0], e.pkey[0])
		}
	}
}

// Composite keys must not collide. Without quotes, ("a|b","c") and
// ("a","b|c") join into the same string, and one of the two rows drops out of
// the comparison.
func TestReadRowHashesKeysDoNotCollide(t *testing.T) {
	rows := &fakeRows{rows: [][]any{
		{"a|b", "c", "hash-1"},
		{"a", "b|c", "hash-2"},
	}}

	got, err := readRowHashes(rows, 2)
	if err != nil {
		t.Fatalf("readRowHashes returned error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("read %d entries, want 2 distinct keys", len(got))
	}
}

// pkey points into the scan buffer, so that buffer has to stay inside the
// loop. Moving it out would leave every entry pointing at the last row.
func TestReadRowHashesRowsDoNotAlias(t *testing.T) {
	rows := &fakeRows{rows: [][]any{
		{uuidBytes(0x01), "hash-1"},
		{uuidBytes(0x02), "hash-2"},
	}}

	got, err := readRowHashes(rows, 1)
	if err != nil {
		t.Fatalf("readRowHashes returned error: %v", err)
	}
	seen := make(map[[16]byte]bool, len(got))
	for _, e := range got {
		seen[e.pkey[0].([16]byte)] = true
	}
	if len(seen) != 2 {
		t.Errorf("entries share %d distinct pkeys, want 2", len(seen))
	}
}

// A lost work item means the pair has no result. The summary has to say so,
// otherwise a diff count of zero reads as a match.
func TestIncompletePairsReportsFailedWorkItems(t *testing.T) {
	m := &MerkleTreeTask{}

	if got := m.incompletePairs(); got != nil {
		t.Errorf("expected no incomplete pairs on a clean task, got %v", got)
	}

	m.recordPairCompareErr("n2/n3")
	m.recordPairCompareErr("n1/n2")
	m.recordPairCompareErr("n1/n2")

	got := m.incompletePairs()
	want := []string{"n1/n2", "n2/n3"}
	if len(got) != len(want) {
		t.Fatalf("incompletePairs() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("incompletePairs() = %v, want %v (stable, sorted)", got, want)
		}
	}
}

// uuid and bytea bounds must be sorted like uuid_cmp() and byteacmp(), not by
// their printed Go form. 0x02 against 0x11 is the pair the fmt fallback got
// backwards: as text, "[17 ..." comes before "[2 ...".
func TestCompareBoundariesOrdersUUIDs(t *testing.T) {
	m := &MerkleTreeTask{}
	m.Key = []string{"id"}

	if got := m.compareBoundaries([]byte{0x02}, []byte{0x11}); got != -1 {
		t.Errorf("compareBoundaries(bytea 0x02, 0x11) = %d, want -1", got)
	}
	if got := m.compareBoundaries(false, true); got != -1 {
		t.Errorf("compareBoundaries(false, true) = %d, want -1", got)
	}
	if got := m.compareBoundaries(true, false); got != 1 {
		t.Errorf("compareBoundaries(true, false) = %d, want 1", got)
	}

	small, large := uuidBytes(0x02), uuidBytes(0x11)

	if got := m.compareBoundaries(small, large); got != -1 {
		t.Errorf("compareBoundaries(0x02.., 0x11..) = %d, want -1", got)
	}
	if got := m.compareBoundaries(large, small); got != 1 {
		t.Errorf("compareBoundaries(0x11.., 0x02..) = %d, want 1", got)
	}
	if got := m.compareBoundaries(large, large); got != 0 {
		t.Errorf("compareBoundaries of equal bounds = %d, want 0", got)
	}
}

// NaN is a legal float primary key, and Postgres sorts it above every number
// while Go's cmp.Compare puts it below. Getting this backwards inverts the
// bounds around it.
func TestComparePkeyValuesNaNSortsLast(t *testing.T) {
	nan := math.NaN()
	if got, ok := comparePkeyValues(nan, 1.0); !ok || got != 1 {
		t.Errorf("comparePkeyValues(NaN, 1.0) = %d, %v; want 1, true", got, ok)
	}
	if got, ok := comparePkeyValues(1.0, nan); !ok || got != -1 {
		t.Errorf("comparePkeyValues(1.0, NaN) = %d, %v; want -1, true", got, ok)
	}
	if got, ok := comparePkeyValues(nan, nan); !ok || got != 0 {
		t.Errorf("comparePkeyValues(NaN, NaN) = %d, %v; want 0, true", got, ok)
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

// Postgres holds -0.0 = 0.0, so both must get one row identity. Two identities
// would split a single row into a phantom pair of differences.
func TestPkeyIdentityNegativeZero(t *testing.T) {
	pos, ok := pkeyIdentity(0.0)
	if !ok {
		t.Fatalf("pkeyIdentity(0.0) refused")
	}
	neg, ok := pkeyIdentity(math.Copysign(0, -1))
	if !ok {
		t.Fatalf("pkeyIdentity(-0.0) refused")
	}
	if pos != neg {
		t.Errorf("0.0 renders as %q and -0.0 as %q; Postgres treats them as equal", pos, neg)
	}
}
