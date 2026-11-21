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

// sqlRecorder is a DBQuerier that keeps the SQL of the last Query or
// QueryRow call and answers it with no rows.
type sqlRecorder struct {
	sql string
}

func (r *sqlRecorder) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("sqlRecorder: Exec is not supported")
}

func (r *sqlRecorder) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	r.sql = sql
	return nil, pgx.ErrNoRows
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
	types := map[string]string{"UserId": "integer", "Region": "text"}

	cases := []struct {
		name string
		run  func(db DBQuerier) error
		cols []string
	}{
		{"GetMaxKey/open", func(db DBQuerier) error {
			_, err := GetMaxKey(ctx, db, "public", "t", simple, types, nil)
			return err
		}, simple},
		{"GetMaxKey/bounded", func(db DBQuerier) error {
			_, err := GetMaxKey(ctx, db, "public", "t", composite, types, []string{"1", "a"})
			return err
		}, composite},
		{"GetMinKey", func(db DBQuerier) error {
			_, err := GetMinKey(ctx, db, "public", "t", simple)
			return err
		}, simple},
		{"GetBlockRowCount/simple", func(db DBQuerier) error {
			_, err := GetBlockRowCount(ctx, db, "public", "t", simple, types, KeyRange{Lower: []string{"1"}, Upper: []string{"9"}})
			return err
		}, simple},
		{"GetBlockRowCount/composite", func(db DBQuerier) error {
			_, err := GetBlockRowCount(ctx, db, "public", "t", composite, types, KeyRange{Lower: []string{"1", "a"}, Upper: []string{"9", "z"}})
			return err
		}, composite},
		{"GetBulkSplitPoints/composite", func(db DBQuerier) error {
			_, err := GetBulkSplitPoints(ctx, db, "public", "t", composite, types, KeyRange{Lower: []string{"1", "a"}}, 100)
			return err
		}, composite},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &sqlRecorder{}
			// The recorder answers with no rows; any other error is a failure.
			if err := tc.run(rec); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("unexpected error: %v", err)
			}
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

// A nil bound is open: the row count of a range puts no condition on that
// side.
func TestGetBlockRowCountOpenBounds(t *testing.T) {
	ctx := context.Background()
	types := map[string]string{"id": "bigint", "a": "integer", "b": "text"}
	cases := []struct {
		name    string
		key     []string
		start   []string
		end     []string
		want    []string
		notWant []string
	}{
		{"simple/nil start", []string{"id"}, nil, []string{"9"},
			[]string{`"id" <= $1::bigint`}, []string{">="}},
		{"simple/nil end", []string{"id"}, []string{"1"}, nil,
			[]string{`"id" >= $1::bigint`}, []string{"<="}},
		{"simple/both open", []string{"id"}, nil, nil,
			[]string{"TRUE"}, []string{">=", "<="}},
		{"composite/nil start", []string{"a", "b"}, nil, []string{"9", "z"},
			[]string{`ROW("a", "b") <= ROW($1::integer, $2::text)`}, []string{">="}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &sqlRecorder{}
			if _, err := GetBlockRowCount(ctx, rec, "public", "t", tc.key, types, KeyRange{Lower: tc.start, Upper: tc.end, UpperClosed: true}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("unexpected error: %v", err)
			}
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

// The count of a leaf joins the tree table with the table. The table can have
// a column named range_start or range_end, so the bounds of a composite key
// must be read through the alias of the tree table.
func TestGetBlockWithCountQualifiesBounds(t *testing.T) {
	rec := &sqlRecorder{}
	if _, err := GetBlockWithCount(context.Background(), rec, `"pgedge_ace"."ace_mtree_public_t"`, "public", "t",
		[]string{"a", "b"}, true, 1); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unexpected error: %v", err)
	}
	for _, want := range []string{`(t1.range_start)."a"`, `(t1.range_start)."b"`, `(t1.range_end)."a"`, `(t1.range_end)."b"`} {
		if !strings.Contains(rec.sql, want) {
			t.Errorf("want %s in:\n%s", want, rec.sql)
		}
	}
	if strings.Contains(rec.sql, "(range_start).") || strings.Contains(rec.sql, "(range_end).") {
		t.Errorf("an unqualified bound in:\n%s", rec.sql)
	}
}
