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
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/pgedge/ace/pkg/types"
)

// TestKeyIDRoundTrip checks that the key IDs of many key texts decode back to
// the same texts.
func TestKeyIDRoundTrip(t *testing.T) {
	cases := [][]string{
		{"1"},
		{""},
		{"", ""},
		{"a,b", "(c)"},
		{`"quoted"`, `back\slash`},
		{"Привет", "日本"},
		{strings.Repeat("x", 300), "y"},
	}
	for _, c := range cases {
		texts := make([][]byte, len(c))
		for i, s := range c {
			texts[i] = []byte(s)
		}
		id, err := encodeKeyID(texts)
		if err != nil {
			t.Fatalf("encode %q: %v", c, err)
		}
		got, err := decodeKeyID(id, len(c))
		if err != nil {
			t.Fatalf("decode %q: %v", c, err)
		}
		if !reflect.DeepEqual(got, c) {
			t.Fatalf("round trip of %q gave %q", c, got)
		}
	}
}

// TestKeyIDHasNoAmbiguity checks that the same bytes split between the key
// columns in two ways give two key IDs.
func TestKeyIDHasNoAmbiguity(t *testing.T) {
	a, _ := encodeKeyID([][]byte{[]byte("ab"), []byte("c")})
	b, _ := encodeKeyID([][]byte{[]byte("a"), []byte("bc")})
	if a == b {
		t.Fatal("two different keys gave the same key ID")
	}
}

// TestKeyIDNull checks that a NULL key column gives an error.
func TestKeyIDNull(t *testing.T) {
	if _, err := encodeKeyID([][]byte{[]byte("1"), nil}); err == nil {
		t.Fatal("a NULL key column must give an error")
	}
}

// TestKeyIDDecodeErrors checks that a key ID that does not fit the number of
// key columns, or that is cut short, gives an error.
func TestKeyIDDecodeErrors(t *testing.T) {
	id, _ := encodeKeyID([][]byte{[]byte("abc")})
	if _, err := decodeKeyID(id, 2); err == nil {
		t.Fatal("wrong number of columns must give an error")
	}
	if _, err := decodeKeyID(id[:len(id)-1], 1); err == nil {
		t.Fatal("a cut key ID must give an error")
	}
}

type fakeSource struct {
	rows []keyRow
	i    int
	err  error
}

// Next moves to the next row, or returns false after the last one.
func (f *fakeSource) Next() bool {
	if f.i >= len(f.rows) {
		return false
	}
	f.i++
	return true
}
func (f *fakeSource) Row() keyRow { return f.rows[f.i-1] }
func (f *fakeSource) Err() error  { return f.err }

func h(b byte) rowHash {
	var x rowHash
	x[0] = b
	return x
}

func src(rows ...keyRow) *fakeSource { return &fakeSource{rows: rows} }

func kr(id string, b byte) keyRow { return keyRow{id: id, hash: h(b)} }

func sorted(ids []string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

// TestFindDiffKeys checks the keys that the search returns for equal, changed
// and missing rows, for empty sides, and for keys with more than one row.
func TestFindDiffKeys(t *testing.T) {
	tests := []struct {
		name  string
		build *fakeSource
		probe *fakeSource
		want  []string
	}{
		{"all equal", src(kr("1", 1), kr("2", 2)), src(kr("2", 2), kr("1", 1)), nil},
		{"changed", src(kr("1", 1), kr("2", 2)), src(kr("1", 1), kr("2", 9)), []string{"2"}},
		{"probe only", src(kr("1", 1)), src(kr("1", 1), kr("3", 3)), []string{"3"}},
		{"build only", src(kr("1", 1), kr("4", 4)), src(kr("1", 1)), []string{"4"}},
		{"all kinds", src(kr("1", 1), kr("2", 2), kr("4", 4)), src(kr("1", 1), kr("2", 9), kr("3", 3)), []string{"2", "3", "4"}},
		{"empty build", nil, src(kr("1", 1), kr("2", 2)), []string{"1", "2"}},
		{"empty probe", src(kr("1", 1), kr("2", 2)), src(), []string{"1", "2"}},
		{"both empty", src(), src(), nil},
		{"duplicate key equal", src(kr("1", 1), kr("1", 2)), src(kr("1", 2), kr("1", 1)), nil},
		{"duplicate key one missing", src(kr("1", 1), kr("1", 2)), src(kr("1", 2)), []string{"1"}},
		{"duplicate key on probe", src(kr("1", 1)), src(kr("1", 1), kr("1", 1)), []string{"1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b keySource
			if tt.build != nil {
				b = tt.build
			}
			mem := &memCounter{}
			got, truncated, err := findDiffKeys(b, tt.probe, 0, mem)
			if err != nil {
				t.Fatal(err)
			}
			if truncated {
				t.Fatal("must not be truncated")
			}
			if !reflect.DeepEqual(sorted(got), sorted(tt.want)) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFindDiffKeysOrder checks the order of the keys: the probe rows in the
// order they were read, then the keys left in the map in byte order.
func TestFindDiffKeysOrder(t *testing.T) {
	// Probe rows in the order read, then the keys left in the map in byte
	// order, so that the result does not depend on the order of a Go map.
	got, _, err := findDiffKeys(src(kr("z", 1), kr("b", 2), kr("m", 3)), src(kr("q", 1), kr("b", 9), kr("a", 1)), 0, &memCounter{})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"q", "b", "a", "m", "z"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestFindDiffKeysMemoryKeepsPeak checks that rows that match do not lower
// the memory count.
func TestFindDiffKeysMemoryKeepsPeak(t *testing.T) {
	mem := &memCounter{}
	got, _, err := findDiffKeys(src(kr("1", 1), kr("2", 2), kr("2", 3)), src(kr("2", 3), kr("2", 2), kr("1", 1)), 0, mem)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("got %q, want no keys", got)
	}
	// A Go map does not shrink, so matched rows do not give memory back.
	want := keyCost("1") + keyCost("2") + rowHashSize + entryOverhead
	if mem.used != want {
		t.Fatalf("memory after all rows matched is %d, want the peak %d", mem.used, want)
	}
}

// TestFindDiffKeysMaxKeys checks the stop after a number of differing keys,
// on the probe side and among the keys left in the map, and that a result of
// exactly that number is not a stop.
func TestFindDiffKeysMaxKeys(t *testing.T) {
	probe := src(kr("1", 1), kr("2", 2), kr("3", 3))
	got, truncated, err := findDiffKeys(nil, probe, 2, &memCounter{})
	if err != nil {
		t.Fatal(err)
	}
	if !truncated || len(got) != 2 {
		t.Fatalf("got %q truncated=%v, want 2 keys and truncated", got, truncated)
	}
	if probe.i != 3 {
		t.Fatalf("the probe read %d rows, want 3: it must stop at the first key over the limit", probe.i)
	}

	// Exactly as many keys as the limit: not truncated.
	got, truncated, err = findDiffKeys(nil, src(kr("1", 1), kr("2", 2)), 2, &memCounter{})
	if err != nil || truncated || len(got) != 2 {
		t.Fatalf("got %q truncated=%v err=%v, want 2 keys, not truncated", got, truncated, err)
	}

	// The limit is reached by the keys left in the map.
	got, truncated, err = findDiffKeys(src(kr("1", 1), kr("2", 2), kr("3", 3)), src(), 1, &memCounter{})
	if err != nil || !truncated || !reflect.DeepEqual(got, []string{"1"}) {
		t.Fatalf("got %q truncated=%v err=%v, want [1] and truncated", got, truncated, err)
	}
}

// TestFindDiffKeysMemoryLimit checks the memory limit on the build side and
// on the probe side, and the memory counted for a changed key.
func TestFindDiffKeysMemoryLimit(t *testing.T) {
	// Build side over the limit.
	mem := &memCounter{limit: 2 * keyCost("1")}
	_, _, err := findDiffKeys(src(kr("1", 1), kr("2", 2), kr("3", 3)), src(), 0, mem)
	var me *errMemory
	if !errors.As(err, &me) || !me.build {
		t.Fatalf("got %v, want a memory error on the build side", err)
	}

	// Empty build side, all probe rows differ.
	mem = &memCounter{limit: 2 * keyCost("1")}
	_, _, err = findDiffKeys(nil, src(kr("1", 1), kr("2", 2), kr("3", 3)), 0, mem)
	if !errors.As(err, &me) || me.build {
		t.Fatalf("got %v, want a memory error on the probe side", err)
	}

	// Rows that match do not make room for new ones.
	mem = &memCounter{limit: 2 * keyCost("1")}
	_, _, err = findDiffKeys(src(kr("1", 1), kr("2", 2)), src(kr("1", 1), kr("2", 2), kr("3", 3)), 0, mem)
	if !errors.As(err, &me) || me.build {
		t.Fatalf("got %v, want a memory error on the probe side", err)
	}

	// A changed key read from the probe side is a string of its own, so the
	// list counts its full cost, although the map holds the same key.
	mem = &memCounter{limit: 3 * keyCost("1")}
	got, _, err := findDiffKeys(src(kr("1", 1), kr("2", 2)), src(kr("1", 1), kr("2", 9)), 0, mem)
	if err != nil || !reflect.DeepEqual(got, []string{"2"}) {
		t.Fatalf("got %q err=%v, want [2]", got, err)
	}
	mem = &memCounter{limit: 3*keyCost("1") - 1}
	_, _, err = findDiffKeys(src(kr("1", 1), kr("2", 2)), src(kr("1", 1), kr("2", 9)), 0, mem)
	if !errors.As(err, &me) || me.build {
		t.Fatalf("got %v, want a memory error on the probe side", err)
	}
}

// TestFindDiffKeysSourceError checks that an error of the build stream or of
// the probe stream is returned.
func TestFindDiffKeysSourceError(t *testing.T) {
	boom := errors.New("boom")
	_, _, err := findDiffKeys(&fakeSource{err: boom}, src(), 0, &memCounter{})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the build error", err)
	}
	_, _, err = findDiffKeys(src(), &fakeSource{err: boom}, 0, &memCounter{})
	if !errors.Is(err, boom) {
		t.Fatalf("got %v, want the probe error", err)
	}
}

func row(id string) types.OrderedMap {
	return types.OrderedMap{{Key: "id", Value: id}}
}

func rr(id string, b byte) readRow { return readRow{hash: h(b), row: row(id)} }

// TestClassify checks the final result for the rows of one key: on one node
// only, changed, changed back, deleted on both nodes, and keys with more than
// one row.
func TestClassify(t *testing.T) {
	rows1 := [][]readRow{
		{rr("only1", 1)},         // only on node 1
		nil,                      // only on node 2
		{rr("chg", 1)},           // changed
		{rr("same", 5)},          // changed back during the run
		nil,                      // deleted on both nodes
		{rr("d", 1), rr("d", 2)}, // two rows, one matches
		{rr("e", 1), rr("e", 2)}, // two rows, both changed
		{rr("f", 1), rr("f", 2)}, // two rows on node 1, one on node 2
	}
	rows2 := [][]readRow{
		nil,
		{rr("only2", 1)},
		{rr("chg", 2)},
		{rr("same", 5)},
		nil,
		{rr("d", 2), rr("d", 3)},
		{rr("e", 3), rr("e", 4)},
		{rr("f", 2)},
	}
	var res Result
	classify(len(rows1), rows1, rows2, &res)

	ids := func(rows []types.OrderedMap) []string {
		var out []string
		for _, r := range rows {
			v, _ := r.Get("id")
			out = append(out, v.(string))
		}
		return out
	}
	if got := ids(res.Only1); !reflect.DeepEqual(got, []string{"only1", "f"}) {
		t.Fatalf("Only1 = %q", got)
	}
	if got := ids(res.Only2); !reflect.DeepEqual(got, []string{"only2"}) {
		t.Fatalf("Only2 = %q", got)
	}
	var changed []string
	for _, c := range res.Changed {
		v, _ := c.Row1.Get("id")
		changed = append(changed, v.(string))
	}
	if !reflect.DeepEqual(changed, []string{"chg", "d", "e", "e"}) {
		t.Fatalf("Changed = %q", changed)
	}
	if res.Count() != 7 {
		t.Fatalf("Count = %d, want 7", res.Count())
	}
}

// TestCheckEstimate checks the memory estimate before the build: a block under
// and over the limit, and no check without statistics, without a row count
// or without a limit.
func TestCheckEstimate(t *testing.T) {
	tbl := Table{Key: []string{"id"}}
	o := Options{MaxMemory: 1 << 20}
	per := int64(8 + 1 + rowHashSize + entryOverhead)

	if err := checkEstimate(tbl, Side{Name: "n1", Rows: (1 << 20) / per, RowsKnown: true, KeyWidth: 8}, o); err != nil {
		t.Fatalf("under the limit: %v", err)
	}
	err := checkEstimate(tbl, Side{Name: "n1", Rows: (1<<20)/per + 1, RowsKnown: true, KeyWidth: 8}, o)
	var me *MemoryLimitError
	if !errors.As(err, &me) || !me.Estimated || me.BuildNode != "n1" {
		t.Fatalf("over the limit: got %v", err)
	}
	if !strings.Contains(err.Error(), "n1") || !strings.Contains(err.Error(), "are estimated to need") {
		t.Fatalf("message does not name the node: %s", err)
	}
	if err := checkEstimate(tbl, Side{Rows: 1 << 30, RowsKnown: true}, o); err != nil {
		t.Fatalf("no statistics must skip the check: %v", err)
	}
	if err := checkEstimate(tbl, Side{Rows: 1 << 30, KeyWidth: 8}, o); err != nil {
		t.Fatalf("an unknown row count must skip the check: %v", err)
	}
	if err := checkEstimate(tbl, Side{Rows: 1 << 30, RowsKnown: true, KeyWidth: 8}, Options{}); err != nil {
		t.Fatalf("no limit must skip the check: %v", err)
	}
}

// TestKeyQuery checks the key query: the key columns and the short row hash in
// the select list, the range in the WHERE clause, no ORDER BY, and the
// result formats.
func TestKeyQuery(t *testing.T) {
	tbl := Table{
		Schema:    "public",
		Name:      "t",
		Key:       []string{"id", "amount"},
		Cols:      []string{"id", "amount", "note"},
		HashTypes: map[string]string{"id": "integer", "amount": "numeric(10,2)", "note": "text"},
	}
	q, err := tbl.keyQuery(`"id" >= $1`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`SELECT "id", trim_scale("amount"), substring(`, `FROM 1 FOR 8)`, `FROM "public"."t" WHERE "id" >= $1`} {
		if !strings.Contains(q, want) {
			t.Fatalf("key query %q does not contain %q", q, want)
		}
	}
	if strings.Contains(strings.ToUpper(q), "ORDER BY") {
		t.Fatalf("key query must not sort: %q", q)
	}
	if f := tbl.keyQueryFormats(); len(f) != 3 || f[0] != 0 || f[1] != 0 || f[2] != 1 {
		t.Fatalf("result formats = %v", f)
	}
}

// TestReadQuery checks the row read: one lookup per key with the key texts
// cast to the column types, the text cast of a jsonb column, the row hash and
// the row address, the filter, the plain join for a table with inheritance
// children, and the error for a key column without a type.
func TestReadQuery(t *testing.T) {
	tbl := Table{
		Schema:    "public",
		Name:      "t",
		Key:       []string{"id", "name"},
		Cols:      []string{"id", "name", "data"},
		HashTypes: map[string]string{"id": "integer", "name": "text", "data": "jsonb"},
	}
	colTypes := map[string]string{"id": "integer", "name": "character varying(20)", "data": "jsonb"}
	q, err := tbl.ReadQuery(colTypes, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"unnest($1::text[], $2::text[]) WITH ORDINALITY AS _ace_keys(_ace_k1, _ace_k2, _ace_pos)",
		"CROSS JOIN LATERAL (",
		`"id" = _ace_keys._ace_k1::integer AND "name" = _ace_keys._ace_k2::character varying(20)`,
		"OFFSET 0",
		`"data"::TEXT AS "data"`,
		"AS _ace_row_hash",
		"_ace_t.tableoid::bigint AS _ace_rel, _ace_t.ctid::text AS _ace_tid",
		`FROM "public"."t" AS _ace_t WHERE`,
	} {
		if !strings.Contains(q, want) {
			t.Fatalf("read query %q does not contain %q", q, want)
		}
	}

	q, err = tbl.ReadQuery(colTypes, `"id" > 0`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q, `AND ("id" > 0) OFFSET 0`) {
		t.Fatalf("read query does not add the filter: %q", q)
	}

	tbl.ScanChildren = true
	q, err = tbl.ReadQuery(colTypes, "")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(q, "LATERAL") || strings.Contains(q, "OFFSET 0") ||
		!strings.Contains(q, `JOIN "public"."t" AS _ace_t ON "id" = _ace_keys._ace_k1::integer`) {
		t.Fatalf("read query with children is not a plain join: %q", q)
	}
	tbl.ScanChildren = false

	if _, err := tbl.ReadQuery(map[string]string{"id": "integer"}, ""); err == nil {
		t.Fatal("a key column without a type must give an error")
	}
}

// TestMemoryLimitErrorMessage checks that the message names both nodes, says
// that the row count is unknown, and gives the memory and the limit in
// megabytes.
func TestMemoryLimitErrorMessage(t *testing.T) {
	e := &MemoryLimitError{BuildNode: "n1", ProbeNode: "n2", BuildRows: -1, Used: 600 << 10, Limit: 512 << 10}
	msg := e.Error()
	for _, want := range []string{"n1 (unknown rows)", "differing keys of node n2", "0.5 MB", "0.6 MB"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q does not contain %q", msg, want)
		}
	}
	e = &MemoryLimitError{BuildNode: "n1", BuildRows: 10, Used: 3 << 20, Limit: 1 << 20}
	if msg := e.Error(); strings.Contains(msg, "differing keys") || !strings.Contains(msg, "(10 rows)") {
		t.Fatalf("message of a build-side stop: %q", msg)
	}
}

// FuzzKeyID checks with random texts that a key ID of two columns decodes back
// to the same texts, and that another split of the same bytes gives another
// key ID.
func FuzzKeyID(f *testing.F) {
	f.Add("1", "a,b")
	f.Add("", "")
	f.Add("\x00", "日本")
	f.Fuzz(func(t *testing.T, a, b string) {
		id, err := encodeKeyID([][]byte{[]byte(a), []byte(b)})
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeKeyID(id, 2)
		if err != nil {
			t.Fatal(err)
		}
		if got[0] != a || got[1] != b {
			t.Fatalf("round trip of %q, %q gave %q", a, b, got)
		}
		// A different split of the same bytes must give a different ID.
		if len(a) > 0 {
			other, _ := encodeKeyID([][]byte{[]byte(a[:len(a)-1]), []byte(a[len(a)-1:] + b)})
			if other == id {
				t.Fatalf("two splits of %q%q gave the same ID", a, b)
			}
		}
	})
}
