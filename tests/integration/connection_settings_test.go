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
