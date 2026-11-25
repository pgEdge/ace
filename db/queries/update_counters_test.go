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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// execRecorder is a DBQuerier that keeps the statements given to Exec.
type execRecorder struct {
	sqls []string
	args [][]any
}

func (r *execRecorder) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sqls = append(r.sqls, sql)
	r.args = append(r.args, args)
	return pgconn.CommandTag{}, nil
}

func (r *execRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic("not used")
}

func (r *execRecorder) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("not used")
}

func (r *execRecorder) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("not used")
}

// Key texts that a record literal cannot carry must reach the server as
// parameters, as they are, and the server must build the key.
func TestUpdateMtreeCountersSendsKeyTexts(t *testing.T) {
	texts := []string{`a,b`, `a"b`, `(x)`, `a\b`, ``, `NULL`}
	changes := make([]MtreeKeyChange, len(texts))
	for i, s := range texts {
		changes[i] = MtreeKeyChange{Op: MtreeOpInsert, Key: []string{fmt.Sprint(i), s}}
	}
	var r execRecorder
	err := UpdateMtreeCounters(context.Background(), &r, `"ace"."ace_mtree_public_t"`, `"ace"."public_t_key_type"`,
		[]string{"integer", "text"}, changes)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.sqls) != 1 {
		t.Fatalf("got %d statements, want 1", len(r.sqls))
	}
	sql := r.sqls[0]
	for _, want := range []string{
		`ROW(k.k1, k.k2)::"ace"."public_t_key_type" AS pkey`,
		"AS k(op, k1, k2)",
		"('insert', $1::integer, $2::text)",
		"('insert', $11::integer, $12::text)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL has no %q:\n%s", want, sql)
		}
	}
	for _, s := range texts[:4] {
		if strings.Contains(sql, s) {
			t.Errorf("SQL has the key text %q:\n%s", s, sql)
		}
	}
	args := r.args[0]
	if len(args) != 2*len(texts) {
		t.Fatalf("got %d arguments, want %d", len(args), 2*len(texts))
	}
	for i, s := range texts {
		if args[2*i+1] != s {
			t.Errorf("argument %d is %q, want %q", 2*i+2, args[2*i+1], s)
		}
	}
}

// A simple key needs no key type: the key is the one key column, read with
// the input function of its schema-qualified type.
func TestUpdateMtreeCountersSimpleKey(t *testing.T) {
	var r execRecorder
	err := UpdateMtreeCounters(context.Background(), &r, `"ace"."ace_mtree_public_t"`, "",
		[]string{`"s"."t"`}, []MtreeKeyChange{{Op: MtreeOpDelete, Key: []string{"x"}}})
	if err != nil {
		t.Fatal(err)
	}
	sql := r.sqls[0]
	for _, want := range []string{"SELECT k.op, k.k1 AS pkey", `('delete', $1::"s"."t")`} {
		if !strings.Contains(sql, want) {
			t.Errorf("SQL has no %q:\n%s", want, sql)
		}
	}
}

// A query can have at most 65535 parameters, so many changes go in several
// statements.
func TestUpdateMtreeCountersBatches(t *testing.T) {
	changes := make([]MtreeKeyChange, 2500)
	for i := range changes {
		changes[i] = MtreeKeyChange{Op: MtreeOpUpdate, Key: []string{fmt.Sprint(i)}}
	}
	var r execRecorder
	if err := UpdateMtreeCounters(context.Background(), &r, "t", "", []string{"integer"}, changes); err != nil {
		t.Fatal(err)
	}
	if len(r.sqls) != 3 {
		t.Fatalf("got %d statements, want 3", len(r.sqls))
	}
	n := 0
	for _, a := range r.args {
		n += len(a)
	}
	if n != len(changes) {
		t.Errorf("got %d arguments, want %d", n, len(changes))
	}
}

// Wrong input is an error before any statement runs, so an operation that
// is not one of the known ones never reaches the SQL text.
func TestUpdateMtreeCountersErrors(t *testing.T) {
	cases := []struct {
		name      string
		keyType   string
		castTypes []string
		changes   []MtreeKeyChange
	}{
		{"no key columns", "", nil, []MtreeKeyChange{{Op: MtreeOpInsert}}},
		{"no key type", "", []string{"integer", "text"}, []MtreeKeyChange{{Op: MtreeOpInsert, Key: []string{"1", "a"}}}},
		{"bad operation", "", []string{"integer"}, []MtreeKeyChange{{Op: "insert'); DROP TABLE t; --", Key: []string{"1"}}}},
		{"short key", "kt", []string{"integer", "text"}, []MtreeKeyChange{{Op: MtreeOpInsert, Key: []string{"1"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var r execRecorder
			if err := UpdateMtreeCounters(context.Background(), &r, "t", tc.keyType, tc.castTypes, tc.changes); err == nil {
				t.Error("no error")
			}
			if len(r.sqls) != 0 {
				t.Errorf("ran %d statements", len(r.sqls))
			}
		})
	}
}
