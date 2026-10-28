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

package auth

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestKeepOpenPoolSettings(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("host=localhost")
	if err != nil {
		t.Fatal(err)
	}
	applyConnectionOptions(cfg, ConnectionOptions{PoolSize: 3, KeepOpen: true})
	if cfg.MaxConns != 3 {
		t.Errorf("MaxConns = %d, want 3", cfg.MaxConns)
	}
	if cfg.MaxConnLifetime < 24*time.Hour || cfg.MaxConnIdleTime < 24*time.Hour {
		t.Errorf("KeepOpen pool: lifetime %v, idle time %v; want at least a day",
			cfg.MaxConnLifetime, cfg.MaxConnIdleTime)
	}
}

// TestOpenConnsRoleLimit needs a PostgreSQL server where the test may create
// a role. Set ACE_TEST_DSN to a superuser connection string.
func TestOpenConnsRoleLimit(t *testing.T) {
	dsn := os.Getenv("ACE_TEST_DSN")
	if dsn == "" {
		t.Skip("ACE_TEST_DSN is not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)

	const role = "ace_open_conns_test"
	if _, err := admin.Exec(ctx, "DROP ROLE IF EXISTS "+role); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN CONNECTION LIMIT 2"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(context.Background(), "DROP ROLE IF EXISTS "+role) })

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = role
	applyConnectionOptions(cfg, ConnectionOptions{PoolSize: 5, KeepOpen: true})
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	opened, refusal, err := OpenConns(ctx, pool, 4)
	if err != nil {
		t.Fatalf("OpenConns: %v", err)
	}
	if opened != 2 || refusal == nil {
		t.Fatalf("OpenConns = %d, %v; want 2 and a refusal", opened, refusal)
	}
	if st := pool.Stat(); st.IdleConns() != 2 || st.AcquiredConns() != 0 {
		t.Fatalf("pool after OpenConns: %d idle, %d acquired; want 2 idle", st.IdleConns(), st.AcquiredConns())
	}

	// The two open connections serve two callers with no new connection.
	before := pool.Stat().NewConnsCount()
	conns, err := AcquireConns(ctx, pool, 2)
	if err != nil {
		t.Fatalf("AcquireConns: %v", err)
	}
	ReleaseConns(conns)
	if after := pool.Stat().NewConnsCount(); after != before {
		t.Fatalf("the pool tried %d new connections for two callers, want none", after-before)
	}
}
