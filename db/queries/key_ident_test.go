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
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// sqlRecorder is a DBQuerier that keeps the SQL of the last QueryRow call and
// answers it with no rows.
type sqlRecorder struct {
	sql string
}

func (r *sqlRecorder) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("sqlRecorder: Exec is not supported")
}

func (r *sqlRecorder) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("sqlRecorder: Query is not supported")
}

func (r *sqlRecorder) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.sql = sql
	return &mockRow{scanErr: pgx.ErrNoRows}
}

func (r *sqlRecorder) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	return nil
}

// The split code passes key column names as they are in the catalog. The
// query functions must quote them, or a key like "UserId" is folded to
// userid and the query fails.
func TestSplitQueriesQuoteKeyColumns(t *testing.T) {
	ctx := context.Background()
	simple := []string{"UserId"}
	composite := []string{"UserId", "Region"}

	cases := []struct {
		name string
		run  func(db DBQuerier)
		cols []string
	}{
		{"GetMaxValSimple/open", func(db DBQuerier) {
			_, _ = GetMaxValSimple(ctx, db, "public", "t", simple[0], nil)
		}, simple},
		{"GetMaxValSimple/bounded", func(db DBQuerier) {
			_, _ = GetMaxValSimple(ctx, db, "public", "t", simple[0], 1)
		}, simple},
		{"GetMinValSimple", func(db DBQuerier) {
			_, _ = GetMinValSimple(ctx, db, "public", "t", simple[0])
		}, simple},
		{"GetBlockRowCount/simple", func(db DBQuerier) {
			_, _ = GetBlockRowCount(ctx, db, "public", "t", simple, false, []any{1}, []any{9})
		}, simple},
		{"GetBlockRowCount/composite", func(db DBQuerier) {
			_, _ = GetBlockRowCount(ctx, db, "public", "t", composite, true, []any{1, "a"}, []any{9, "z"})
		}, composite},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &sqlRecorder{}
			tc.run(rec)
			if rec.sql == "" {
				t.Fatal("no query was sent")
			}
			for _, c := range tc.cols {
				quoted := `"` + c + `"`
				if !strings.Contains(rec.sql, quoted) {
					t.Errorf("column %s is missing:\n%s", quoted, rec.sql)
				}
				if strings.Count(rec.sql, c) != strings.Count(rec.sql, quoted) {
					t.Errorf("column %s is not quoted everywhere:\n%s", c, rec.sql)
				}
			}
		})
	}
}

// A nil bound and an all-NULL bound are both open: GetBlockRowCount puts no
// condition on that side.
func TestGetBlockRowCountOpenBounds(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name      string
		composite bool
		key       []string
		start     []any
		end       []any
		want      []string
		notWant   []string
	}{
		{"simple/nil start", false, []string{"id"}, nil, []any{9},
			[]string{`"id" <= $1`}, []string{">="}},
		{"simple/NULL start", false, []string{"id"}, []any{nil}, []any{9},
			[]string{`"id" <= $1`}, []string{">="}},
		{"simple/NULL end", false, []string{"id"}, []any{1}, []any{nil},
			[]string{`"id" >= $1`}, []string{"<="}},
		{"simple/both open", false, []string{"id"}, []any{nil}, nil,
			[]string{"TRUE"}, []string{">=", "<="}},
		{"composite/NULL start", true, []string{"a", "b"}, []any{nil, nil}, []any{9, 9},
			[]string{`ROW("a", "b") <= ROW($1, $2)`}, []string{">="}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &sqlRecorder{}
			_, _ = GetBlockRowCount(ctx, rec, "public", "t", tc.key, tc.composite, tc.start, tc.end)
			for _, w := range tc.want {
				if !strings.Contains(rec.sql, w) {
					t.Errorf("want %q in:\n%s", w, rec.sql)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(rec.sql, w) {
					t.Errorf("do not want %q in:\n%s", w, rec.sql)
				}
			}
		})
	}
}
