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
	"sort"
	"testing"

	"github.com/pgedge/ace/db/queries"
	"github.com/stretchr/testify/require"
)

// TestCatalogQueriesWithSelectOnlyRole runs the table metadata queries as a
// role that has only SELECT on the table. information_schema hides
// constraints from such a role (table_constraints needs a privilege other
// than SELECT), so the primary key came back empty and table-diff failed
// with "no primary key found". The queries read pg_catalog, which shows
// every object regardless of privileges.
func TestCatalogQueriesWithSelectOnlyRole(t *testing.T) {
	ctx := context.Background()
	const (
		role     = "ace_select_only"
		password = "password"
		schema   = "ace_catalog_privs"
	)
	admin := pgCluster.Node1Pool

	exec := func(sql string) {
		t.Helper()
		_, err := admin.Exec(ctx, sql)
		require.NoError(t, err, sql)
	}
	cleanup := func() {
		exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		exec("DROP ROLE IF EXISTS " + role)
	}
	cleanup()
	t.Cleanup(cleanup)

	exec("CREATE ROLE " + role + " LOGIN PASSWORD '" + password + "'")
	exec("CREATE SCHEMA " + schema)
	exec("CREATE TABLE " + schema + ".readable (a int, b int, v text, PRIMARY KEY (b, a))")
	exec("CREATE TABLE " + schema + ".hidden (id int PRIMARY KEY)")
	exec("GRANT USAGE ON SCHEMA " + schema + " TO " + role)
	exec("GRANT SELECT ON " + schema + ".readable TO " + role)

	pool, err := connectToNode(pgCluster.Node1Host, pgCluster.Node1Port, role, password, dbName)
	require.NoError(t, err)
	defer pool.Close()

	key, err := queries.GetPrimaryKey(ctx, pool, schema, "readable")
	require.NoError(t, err)
	require.Equal(t, []string{"b", "a"}, key)

	cols, err := queries.GetColumns(ctx, pool, schema, "readable")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "v"}, cols)

	// A table the role cannot read is still listed, so schema-diff reports
	// it as failed instead of leaving it out of the run.
	tables, err := queries.GetTablesInSchema(ctx, pool, schema)
	require.NoError(t, err)
	sort.Strings(tables)
	require.Equal(t, []string{"hidden", "readable"}, tables)

	privs, err := queries.CheckUserPrivileges(ctx, pool, role, schema, "hidden")
	require.NoError(t, err)
	require.False(t, privs.TableSelect)
}
