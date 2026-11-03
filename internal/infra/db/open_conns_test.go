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

	// Many more connections than the role may have: after the first refusal
	// OpenConns starts no new attempts, so it makes far fewer than n.
	cfg.MaxConns = 64
	many, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer many.Close()
	pool.Reset()
	for deadline := time.Now().Add(10 * time.Second); ; {
		var n int
		if err := admin.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE usename = $1", role).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d backends of %s are still open", n, role)
		}
		time.Sleep(10 * time.Millisecond)
	}
	opened, refusal, err = OpenConns(ctx, many, 64)
	if err != nil {
		t.Fatalf("OpenConns of 64: %v", err)
	}
	if opened != 2 || refusal == nil {
		t.Fatalf("OpenConns of 64 = %d, %v; want 2 and a refusal", opened, refusal)
	}
	// Every attempt that started before the first refusal came back either
	// succeeded or was one of at most openConnsAtOnce that ran at that time.
	if tries, most := many.Stat().NewConnsCount(), int64(opened+openConnsAtOnce); tries > most {
		t.Fatalf("OpenConns of 64 tried %d connections, want at most %d", tries, most)
	}
	many.Reset()

	// A role with no free slot at all: the first connection is refused, and
	// OpenConns makes no other attempt.
	if _, err := admin.Exec(ctx, "ALTER ROLE "+role+" CONNECTION LIMIT 0"); err != nil {
		t.Fatal(err)
	}
	empty, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	opened, refusal, err = OpenConns(ctx, empty, 4)
	if err != nil {
		t.Fatalf("OpenConns with no free slot: %v", err)
	}
	if opened != 0 || refusal == nil {
		t.Fatalf("OpenConns with no free slot = %d, %v; want 0 and a refusal", opened, refusal)
	}
	if tries := empty.Stat().NewConnsCount(); tries != 1 {
		t.Fatalf("OpenConns with no free slot tried %d connections, want 1", tries)
	}
}

// TestOpenConnsAll needs a PostgreSQL server. Set ACE_TEST_DSN to a
// connection string.
func TestOpenConnsAll(t *testing.T) {
	dsn := os.Getenv("ACE_TEST_DSN")
	if dsn == "" {
		t.Skip("ACE_TEST_DSN is not set")
	}
	ctx := context.Background()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	applyConnectionOptions(cfg, ConnectionOptions{PoolSize: 8, KeepOpen: true})
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	opened, refusal, err := OpenConns(ctx, pool, 8)
	if err != nil || refusal != nil || opened != 8 {
		t.Fatalf("OpenConns = %d, %v, %v; want 8 and no refusal", opened, refusal, err)
	}
	if st := pool.Stat(); st.IdleConns() != 8 || st.AcquiredConns() != 0 {
		t.Fatalf("pool after OpenConns: %d idle, %d acquired; want 8 idle", st.IdleConns(), st.AcquiredConns())
	}
	if opened, _, err := OpenConns(ctx, pool, 0); opened != 0 || err != nil {
		t.Fatalf("OpenConns(0) = %d, %v; want 0 and no error", opened, err)
	}
}
