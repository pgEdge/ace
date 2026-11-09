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

package keydiff

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// fakeRows is a pgx.Rows over fixed rows. For the key query it serves
// RawValues; for the row read it serves Values. Scan fails, so that a test
// fails if the row read does not use the common way to read a row. It
// records whether the context of its query was cancelled when Close was
// called.
type fakeRows struct {
	ctx              context.Context
	raw              [][][]byte
	vals             [][]any
	fields           []pgconn.FieldDescription
	i                int
	closed           bool
	cancelledAtClose bool
}

// Close marks the rows closed and records whether the context of the query
// was cancelled at that moment.
func (r *fakeRows) Close() {
	if !r.closed {
		r.closed = true
		r.cancelledAtClose = r.ctx.Err() != nil
	}
}
func (r *fakeRows) Err() error                                   { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

// n returns the number of rows: raw rows for the key query, values for the
// row read.
func (r *fakeRows) n() int {
	if r.raw != nil {
		return len(r.raw)
	}
	return len(r.vals)
}

// Next moves to the next row and closes the rows after the last one, as pgx
// does.
func (r *fakeRows) Next() bool {
	if r.i >= r.n() {
		r.Close()
		return false
	}
	r.i++
	return true
}
func (r *fakeRows) RawValues() [][]byte    { return r.raw[r.i-1] }
func (r *fakeRows) Values() ([]any, error) { return r.vals[r.i-1], nil }
func (r *fakeRows) Scan(dest ...any) error {
	return fmt.Errorf("fakeRows: Scan is not supported, use Values")
}

// fakeNode answers the key query with keys and the row read with one row for
// every requested key that it holds.
type fakeNode struct {
	name   string
	keys   [][][]byte           // raw values of the key query: key text, row hash
	stored map[string][]fakeRow // key text -> rows
	log    *[]string
	opened []*fakeRows
}

type fakeRow struct {
	hash byte
	tid  string
	v    string
}

func hash8(b byte) []byte {
	h := make([]byte, rowHashSize)
	h[0] = b
	return h
}

// Query answers the row read with the stored rows of the requested keys, and
// any other query with the keys of the node. It logs each query and keeps
// the rows it returns.
func (n *fakeNode) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.HasPrefix(sql, "SELECT _ace_keys.") {
		*n.log = append(*n.log, n.name+":read")
		texts := args[0].([]string)
		r := &fakeRows{ctx: ctx, fields: []pgconn.FieldDescription{
			{Name: posCol}, {Name: rowHashCol}, {Name: relCol}, {Name: tidCol},
			{Name: "commit_ts"}, {Name: "node_origin"}, {Name: "id"}, {Name: "v"}}}
		for i, k := range texts {
			for _, fr := range n.stored[k] {
				r.vals = append(r.vals, []any{int64(i + 1), hash8(fr.hash), int64(1), fr.tid, nil, nil, k, fr.v})
			}
		}
		n.opened = append(n.opened, r)
		return r, nil
	}
	*n.log = append(*n.log, n.name+":keys")
	r := &fakeRows{ctx: ctx, raw: n.keys}
	if r.raw == nil {
		r.raw = [][][]byte{}
	}
	n.opened = append(n.opened, r)
	return r, nil
}

func keyRaw(k string, b byte) [][]byte { return [][]byte{[]byte(k), hash8(b)} }

var fakeTable = Table{Schema: "public", Name: "t", Key: []string{"id"}, Cols: []string{"id", "v"},
	HashTypes: map[string]string{"id": "text", "v": "text"}}
var fakeTypes = map[string]string{"id": "text", "v": "text"}

func twoNodes() (*fakeNode, *fakeNode, *[]string) {
	log := &[]string{}
	n1 := &fakeNode{name: "n1", log: log, stored: map[string][]fakeRow{}}
	n2 := &fakeNode{name: "n2", log: log, stored: map[string][]fakeRow{}}
	return n1, n2, log
}

func side(n *fakeNode, rows int64, known bool) Side {
	return Side{Name: n.name, Conn: n, ColTypes: fakeTypes, Rows: rows, RowsKnown: known}
}

// collected holds the entries that a comparison gave to its sink.
type collected struct {
	entries []Entry
	calls   int
}

// sink returns a Sink that keeps a copy of every entry.
func (c *collected) sink() Sink {
	return func(entries []Entry) error {
		c.calls++
		c.entries = append(c.entries, entries...)
		return nil
	}
}

// of returns the entries of one kind.
func (c *collected) of(k Kind) []Entry {
	var out []Entry
	for _, e := range c.entries {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

// discard is a Sink that drops the entries.
func discard([]Entry) error { return nil }

// TestCompareBuildSideAndResult checks that the node with fewer rows is the
// build side and is read first, that a changed row and a row on each node
// only are found, that the rows are read in batches, and that no query read
// to its end is cancelled.
func TestCompareBuildSideAndResult(t *testing.T) {
	n1, n2, log := twoNodes()
	// n1: a, b (changed), c (only n1); n2: a, b, d (only n2)
	n1.keys = [][][]byte{keyRaw("a", 1), keyRaw("b", 2), keyRaw("c", 3)}
	n2.keys = [][][]byte{keyRaw("a", 1), keyRaw("b", 9), keyRaw("d", 4)}
	n1.stored = map[string][]fakeRow{"b": {{2, "(0,2)", "b1"}}, "c": {{3, "(0,3)", "c1"}}}
	n2.stored = map[string][]fakeRow{"b": {{9, "(0,2)", "b2"}}, "d": {{4, "(0,4)", "d2"}}}

	// n2 has fewer rows, so its keys go into the map: its key query is first.
	var got collected
	res, err := Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 3, true), side(n2, 2, true), Options{BatchSize: 1}, got.sink())
	if err != nil {
		t.Fatal(err)
	}
	if (*log)[0] != "n2:keys" || (*log)[1] != "n1:keys" {
		t.Fatalf("query order %q, want the build side n2 first", *log)
	}
	if res.BuildNode != "n2" || res.DiffKeys != 3 {
		t.Fatalf("BuildNode=%s DiffKeys=%d", res.BuildNode, res.DiffKeys)
	}
	if res.Only1 != 1 || res.Only2 != 1 || res.Changed != 1 {
		t.Fatalf("Only1=%d Only2=%d Changed=%d", res.Only1, res.Only2, res.Changed)
	}
	if len(got.of(KindOnly1)) != 1 || len(got.of(KindOnly2)) != 1 || len(got.of(KindChanged)) != 1 {
		t.Fatalf("the sink got %d entries: %+v", len(got.entries), got.entries)
	}
	changed := got.of(KindChanged)[0]
	if v, _ := changed.Row1.Get("v"); v != "b1" {
		t.Fatalf("Row1 of the changed row comes from n2: %v", v)
	}
	if changed.Hash1 != RowHash(h(2)) || changed.Hash2 != RowHash(h(9)) {
		t.Fatalf("hashes of the changed row: %x %x", changed.Hash1, changed.Hash2)
	}
	// Each batch of one key gives one entry, and the sink gets each batch.
	if got.calls != 3 {
		t.Fatalf("the sink was called %d times, want 3", got.calls)
	}
	// Three keys in batches of one: three row reads on each node.
	reads := 0
	for _, l := range *log {
		if strings.HasSuffix(l, ":read") {
			reads++
		}
	}
	if reads != 6 {
		t.Fatalf("%d row reads, want 6: %q", reads, *log)
	}
	for _, r := range append(n1.opened, n2.opened...) {
		if r.cancelledAtClose {
			t.Fatal("a query that was read to the end was cancelled")
		}
	}
}

// TestCompareEmptyBuildSideIsNotQueried checks that a build side with no rows
// in the block is not queried, and that the rows of the other side are
// reported.
func TestCompareEmptyBuildSideIsNotQueried(t *testing.T) {
	n1, n2, log := twoNodes()
	n1.keys = [][][]byte{keyRaw("a", 1)}
	n1.stored = map[string][]fakeRow{"a": {{1, "(0,1)", "a"}}}
	var got collected
	res, err := Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 1, true), side(n2, 0, true), Options{}, got.sink())
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range *log {
		if l == "n2:keys" {
			t.Fatalf("the empty build side was queried: %q", *log)
		}
	}
	if res.Only1 != 1 || len(got.of(KindOnly1)) != 1 {
		t.Fatalf("Only1=%d, sink %d, want 1", res.Only1, len(got.of(KindOnly1)))
	}
}

// TestCompareMemoryLimitError checks the error of the memory limit during the
// build and during the probe, the nodes that it names, and that the build
// query stopped by the limit is cancelled.
func TestCompareMemoryLimitError(t *testing.T) {
	n1, n2, _ := twoNodes()
	n1.keys = [][][]byte{keyRaw("a", 1), keyRaw("b", 2)}
	n2.keys = [][][]byte{keyRaw("c", 1), keyRaw("d", 2), keyRaw("e", 3)}
	idA, _ := encodeKeyID([][]byte{[]byte("a")})
	_, err := Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 2, true), side(n2, 3, true),
		Options{MaxMemory: 2*keyCost(idA) + 1}, discard)
	var me *MemoryLimitError
	if !errors.As(err, &me) {
		t.Fatalf("got %v, want a MemoryLimitError", err)
	}
	if me.BuildNode != "n1" || me.ProbeNode != "n2" || me.BuildRows != 2 || me.Estimated {
		t.Fatalf("got %+v", me)
	}
	// The build side alone passes the limit: no probe node in the error.
	_, err = Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 2, true), side(n2, 3, true),
		Options{MaxMemory: keyCost(idA)}, discard)
	if !errors.As(err, &me) || me.ProbeNode != "" {
		t.Fatalf("got %v, want a build-side MemoryLimitError", err)
	}
	if !n1.opened[len(n1.opened)-1].cancelledAtClose {
		t.Fatal("the build query stopped by the limit must be cancelled before Close")
	}
}

// TestCompareBadKeyRows checks that a key row with a NULL key, a short row
// hash or a wrong number of columns stops the comparison with an error, and
// that the key query is cancelled.
func TestCompareBadKeyRows(t *testing.T) {
	tests := []struct {
		name string
		row  [][]byte
		want string
	}{
		{"null key", [][]byte{nil, hash8(1)}, "NULL"},
		{"short hash", [][]byte{[]byte("a"), []byte{1, 2}}, "row hash of 2 bytes"},
		{"columns", [][]byte{[]byte("a")}, "returned 1 columns"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n1, n2, _ := twoNodes()
			n1.keys = [][][]byte{tt.row, keyRaw("b", 2)}
			_, err := Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 2, true), side(n2, 2, true), Options{}, discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want an error with %q", err, tt.want)
			}
			if !n1.opened[0].cancelledAtClose {
				t.Fatal("a key query stopped by an error must be cancelled before Close")
			}
		})
	}
}

// TestCompareSameRowForTwoKeys checks that one row found through two key
// texts in different batches is reported once, as a changed row.
func TestCompareSameRowForTwoKeys(t *testing.T) {
	// "-0" and "0" are two key texts for one row: the server finds the same
	// row (same ctid) for both, in different batches.
	n1, n2, _ := twoNodes()
	n1.keys = [][][]byte{keyRaw("-0", 1)}
	n2.keys = [][][]byte{keyRaw("0", 2)}
	n1.stored = map[string][]fakeRow{"-0": {{1, "(0,1)", "x"}}, "0": {{1, "(0,1)", "x"}}}
	n2.stored = map[string][]fakeRow{"-0": {{2, "(0,1)", "x"}}, "0": {{2, "(0,1)", "x"}}}
	res, err := Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 1, true), side(n2, 1, true), Options{BatchSize: 1}, discard)
	if err != nil {
		t.Fatal(err)
	}
	if res.DiffKeys != 2 || res.Changed != 1 || res.Only1+res.Only2 != 0 {
		t.Fatalf("DiffKeys=%d Changed=%d Only1=%d Only2=%d, want one changed row", res.DiffKeys, res.Changed, res.Only1, res.Only2)
	}
}

// TestCompareSinkError checks that an error of the sink stops the comparison
// after the first batch and is returned.
func TestCompareSinkError(t *testing.T) {
	n1, n2, log := twoNodes()
	n1.keys = [][][]byte{keyRaw("a", 1), keyRaw("b", 2)}
	n1.stored = map[string][]fakeRow{"a": {{1, "(0,1)", "a"}}, "b": {{2, "(0,2)", "b"}}}
	stop := errors.New("disk full")
	calls := 0
	_, err := Compare(context.Background(), fakeTable, Block{Where: "TRUE"}, side(n1, 2, true), side(n2, 0, true), Options{BatchSize: 1},
		func([]Entry) error {
			calls++
			return stop
		})
	if !errors.Is(err, stop) {
		t.Fatalf("got %v, want the error of the sink", err)
	}
	reads := 0
	for _, l := range *log {
		if strings.HasSuffix(l, ":read") {
			reads++
		}
	}
	if calls != 1 || reads != 2 {
		t.Fatalf("sink calls %d, row reads %d; want 1 and 2: the comparison must stop after the first batch", calls, reads)
	}
}
