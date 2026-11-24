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
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
)

// TestEnsureTreeStateColumnsQuotedSchema adds the tree state columns to a
// metadata table of an earlier ACE in a schema whose name has an apostrophe
// and a space, and then runs again on the complete table.
func TestEnsureTreeStateColumnsQuotedSchema(t *testing.T) {
	dsn := os.Getenv("ACE_TEST_DSN")
	if dsn == "" {
		t.Skip("ACE_TEST_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	const schema = "ace's state test"
	qs := pgx.Identifier{schema}.Sanitize()
	old := config.Get()
	cfg := &config.Config{}
	if old != nil {
		c := *old
		cfg = &c
	}
	cfg.MTree.Schema = schema
	config.Set(cfg)
	t.Cleanup(func() { config.Set(old) })

	for _, sql := range []string{
		`DROP SCHEMA IF EXISTS ` + qs + ` CASCADE`,
		`CREATE SCHEMA ` + qs,
		`CREATE TABLE ` + qs + `.ace_mtree_metadata (
			schema_name text,
			table_name text,
			hash_version int NOT NULL,
			PRIMARY KEY (schema_name, table_name))`,
	} {
		if _, err := pool.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+qs+` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", qs, err)
		}
	})

	for i := 0; i < 2; i++ {
		if err := EnsureTreeStateColumns(ctx, pool); err != nil {
			t.Fatalf("run %d: %v", i+1, err)
		}
	}
	var n int
	err = pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_catalog.pg_attribute
		WHERE attrelid = $1::regclass
			AND attname IN ('table_shape', 'valid_from_lsn', 'state', 'invalid_reason')
			AND NOT attisdropped`,
		qs+`.ace_mtree_metadata`).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 {
		t.Fatalf("got %d tree state columns, want 4", n)
	}
}
