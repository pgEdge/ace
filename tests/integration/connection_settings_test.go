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

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
	auth "github.com/pgedge/ace/internal/infra/db"
	"github.com/stretchr/testify/require"
)

// The row hash is computed over the text of each value, so this text must be
// the same on every node. Each value here prints differently under at least
// one other setting: TimeZone, IntervalStyle, bytea_output,
// extra_float_digits (on servers before PostgreSQL 12) and lc_monetary.
const (
	rowTextQuery = `SELECT ROW('2026-01-02 03:04:05+00'::timestamptz,
	                           '1 day'::interval,
	                           '\x01'::bytea,
	                           0.1::float8,
	                           1.5::money)::text`
	rowTextWant = `("2026-01-02 03:04:05+00","1 day","\\x01",0.1,$1.50)`
)

// TestACEConnPinsOutputSettings checks that a connection opened by ACE uses
// the pinned output settings even when the role has other defaults. A
// startup-packet value has priority over ALTER ROLE ... SET, and this test
// proves it on a real server.
func TestACEConnPinsOutputSettings(t *testing.T) {
	ctx := context.Background()
	node := pgCluster.ClusterNodes[0]

	admin, err := auth.GetClusterNodeConnection(ctx, node, auth.ConnectionOptions{})
	require.NoError(t, err)

	// Register the cleanup before the first ALTER ROLE, so that the role
	// defaults are reset even if one of the statements below fails. RESET
	// of a setting that was never set is not an error.
	roleDefaults := map[string]string{
		"TimeZone":     "Asia/Tokyo",
		"DateStyle":    "SQL, DMY",
		"bytea_output": "escape",
	}
	t.Cleanup(func() {
		for name := range roleDefaults {
			_, err := admin.Exec(context.Background(), "ALTER ROLE CURRENT_USER RESET "+name)
			require.NoError(t, err)
		}
		admin.Close()
	})

	// Role defaults apply only to new sessions, so set them before the
	// pool under test opens its first connection.
	for name, value := range roleDefaults {
		_, err = admin.Exec(ctx, "ALTER ROLE CURRENT_USER SET "+name+" = '"+value+"'")
		require.NoError(t, err, "ALTER ROLE ... SET %s", name)
	}

	pool, err := auth.GetClusterNodeConnection(ctx, node, auth.ConnectionOptions{})
	require.NoError(t, err)
	defer pool.Close()

	for name, want := range auth.OutputSettings() {
		var got string
		require.NoError(t, pool.QueryRow(ctx, "SHOW "+name).Scan(&got))
		require.Equal(t, want, got, "SHOW %s on an ACE pool connection", name)
	}

	var rowText string
	require.NoError(t, pool.QueryRow(ctx, rowTextQuery).Scan(&rowText))
	require.Equal(t, rowTextWant, rowText)

	// The replication connection is opened by pgconn directly. Its
	// walsender prints the pgoutput tuple values, so it needs the same
	// settings. A walsender in database mode accepts SHOW.
	repl, err := auth.GetReplModeConnection(node)
	require.NoError(t, err)
	defer repl.Close(ctx)

	for name, want := range auth.OutputSettings() {
		results, err := repl.Exec(ctx, "SHOW "+name).ReadAll()
		require.NoError(t, err, "SHOW %s on the replication connection", name)
		require.Len(t, results, 1)
		require.Len(t, results[0].Rows, 1)
		require.Equal(t, want, string(results[0].Rows[0][0]),
			"SHOW %s on the replication connection", name)
	}
}

// TestBackendStopsAfterLostClient checks that a backend of an ACE
// connection stops a long query when the client is gone without a word,
// as when the kernel kills ACE. The test sends a query and closes the
// socket, with no cancel request and no Terminate message. Without
// client_connection_check_interval, the backend would run the query to the
// end.
func TestBackendStopsAfterLostClient(t *testing.T) {
	ctx := context.Background()
	node := pgCluster.ClusterNodes[0]

	pool, err := auth.GetClusterNodeConnection(ctx, node, auth.ConnectionOptions{})
	require.NoError(t, err)
	defer pool.Close()

	var interval string
	require.NoError(t, pool.QueryRow(ctx, "SHOW client_connection_check_interval").Scan(&interval))
	require.Equal(t, "5s", interval)

	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	pgxConn := conn.Hijack()
	pid := pgxConn.PgConn().PID()
	hijacked, err := pgxConn.PgConn().Hijack()
	require.NoError(t, err)

	hijacked.Frontend.Send(&pgproto3.Query{String: "SELECT pg_sleep(60)"})
	require.NoError(t, hijacked.Frontend.Flush())

	const activeQuery = `SELECT count(*) FROM pg_stat_activity
WHERE pid = $1 AND state = 'active' AND query LIKE '%pg_sleep(60)%'`
	waitFor := func(want int, limit time.Duration) time.Duration {
		start := time.Now()
		for {
			var n int
			require.NoError(t, pool.QueryRow(ctx, activeQuery, pid).Scan(&n))
			if n == want {
				return time.Since(start)
			}
			if time.Since(start) > limit {
				t.Fatalf("backend %d: active queries = %d, want %d after %s", pid, n, want, limit)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	waitFor(1, 5*time.Second)

	require.NoError(t, hijacked.Conn.Close())
	took := waitFor(0, 20*time.Second)
	t.Logf("backend %d stopped the query %s after the client was gone", pid, took)
}
