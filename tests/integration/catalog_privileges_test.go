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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	selectOnlyRole     = "ace_select_only"
	selectOnlyPassword = "password"
	catalogPrivsSchema = "ace_catalog_privs"
	// No USAGE is granted on this schema.
	catalogNoUsageSchema = "ace_catalog_no_usage"
	// USAGE is granted, but none of its tables can be read.
	catalogUnreadableSchema = "ace_catalog_unreadable"
	catalogPrivsRepset      = "ace_catalog_privs_rs"
	// Its own server, so cleanup can drop it without touching the one
	// the foreign-table tests share.
	catalogPrivsServer = "ace_catalog_privs_csv"
)

// setupSelectOnlyRole creates, on n1 and n2, a role that can read some
// tables of catalogPrivsSchema and not others:
//   - readable:    SELECT on the table, key (b, a) out of column order
//   - hidden:      no grant
//   - nopk_hidden: no grant and no primary key
//   - colgrant:    SELECT granted per column, on every column
//   - n1_only:     SELECT on n1 only, as when grants drift between nodes
//
// plus a view, a sequence and a foreign table in the same schema.
func setupSelectOnlyRole(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	exec := func(p *pgxpool.Pool, sql string) {
		t.Helper()
		_, err := p.Exec(ctx, sql)
		require.NoError(t, err, sql)
	}
	// cleanup runs every drop on every node. Before the test it fails on
	// the first error, since the setup would fail anyway; after the test
	// it logs errors and goes on, so one failed drop doesn't leave the
	// rest behind.
	cleanup := func(run func(*pgxpool.Pool, string)) {
		for _, p := range pools {
			run(p, "DROP SCHEMA IF EXISTS "+catalogPrivsSchema+" CASCADE")
			run(p, "DROP SCHEMA IF EXISTS "+catalogNoUsageSchema+" CASCADE")
			run(p, "DROP SCHEMA IF EXISTS "+catalogUnreadableSchema+" CASCADE")
			run(p, "DROP SERVER IF EXISTS "+catalogPrivsServer+" CASCADE")
			// Revokes grants on objects outside the dropped schemas, such
			// as spock's tables, which would block DROP ROLE.
			run(p, "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '"+selectOnlyRole+"') THEN DROP OWNED BY "+selectOnlyRole+"; END IF; END $$")
			run(p, "DROP ROLE IF EXISTS "+selectOnlyRole)
		}
	}
	cleanup(exec)
	t.Cleanup(func() {
		cleanup(func(p *pgxpool.Pool, sql string) {
			if _, err := p.Exec(ctx, sql); err != nil {
				t.Logf("cleanup: %s: %v", sql, err)
			}
		})
	})

	s := catalogPrivsSchema
	for _, p := range pools {
		exec(p, "CREATE ROLE "+selectOnlyRole+" LOGIN PASSWORD '"+selectOnlyPassword+"'")
		exec(p, "CREATE SCHEMA "+s)
		exec(p, "CREATE TABLE "+s+".readable (a int, b int, v text, PRIMARY KEY (b, a))")
		exec(p, "INSERT INTO "+s+".readable VALUES (1, 1, 'x'), (2, 1, 'y')")
		exec(p, "CREATE TABLE "+s+".hidden (id int PRIMARY KEY)")
		exec(p, "CREATE TABLE "+s+".nopk_hidden (id int)")
		exec(p, "CREATE TABLE "+s+".n1_only (id int PRIMARY KEY)")
		exec(p, "CREATE TABLE "+s+".colgrant (id int PRIMARY KEY, v text)")
		exec(p, "CREATE VIEW "+s+".a_view AS SELECT a, b FROM "+s+".readable")
		exec(p, "CREATE SEQUENCE "+s+".a_seq")
		exec(p, "CREATE EXTENSION IF NOT EXISTS file_fdw")
		exec(p, "CREATE SERVER "+catalogPrivsServer+" FOREIGN DATA WRAPPER file_fdw")
		exec(p, "CREATE FOREIGN TABLE "+s+".a_foreign (id int) SERVER "+catalogPrivsServer+" OPTIONS (filename '/tmp/ace_fdw_rows.csv', format 'csv')")
		exec(p, "GRANT USAGE ON SCHEMA "+s+" TO "+selectOnlyRole)
		exec(p, "GRANT SELECT ON "+s+".readable TO "+selectOnlyRole)
		exec(p, "GRANT SELECT (id, v) ON "+s+".colgrant TO "+selectOnlyRole)
		exec(p, "CREATE SCHEMA "+catalogNoUsageSchema)
		exec(p, "CREATE TABLE "+catalogNoUsageSchema+".t (id int PRIMARY KEY)")
		exec(p, "GRANT SELECT ON "+catalogNoUsageSchema+".t TO "+selectOnlyRole)
		exec(p, "CREATE SCHEMA "+catalogUnreadableSchema)
		exec(p, "CREATE TABLE "+catalogUnreadableSchema+".t (id int PRIMARY KEY)")
		exec(p, "GRANT USAGE ON SCHEMA "+catalogUnreadableSchema+" TO "+selectOnlyRole)
	}
	exec(pgCluster.Node1Pool, "GRANT SELECT ON "+s+".n1_only TO "+selectOnlyRole)
}

// TestCatalogQueriesWithSelectOnlyRole runs the table metadata queries as a
// role that has only SELECT on the table. information_schema hides
// constraints from such a role (table_constraints needs a privilege other
// than SELECT), so the primary key came back empty and table-diff failed
// with "no primary key found". The queries read pg_catalog, which shows
// every object regardless of privileges.
func TestCatalogQueriesWithSelectOnlyRole(t *testing.T) {
	ctx := context.Background()
	setupSelectOnlyRole(t)
	s := catalogPrivsSchema

	pool, err := connectToNode(pgCluster.Node1Host, pgCluster.Node1Port, selectOnlyRole, selectOnlyPassword, dbName)
	require.NoError(t, err)
	defer pool.Close()

	key, err := queries.GetPrimaryKey(ctx, pool, s, "readable")
	require.NoError(t, err)
	require.Equal(t, []string{"b", "a"}, key)

	cols, colTypes, err := queries.GetColumns(ctx, pool, s, "readable")
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b", "v"}, cols)
	require.Equal(t, map[string]string{"a": "integer", "b": "integer", "v": "text"}, colTypes)

	// A sequence has attributes in pg_attribute but is not a table.
	cols, _, err = queries.GetColumns(ctx, pool, s, "a_seq")
	require.NoError(t, err)
	require.Empty(t, cols)

	// Tables the role cannot read are listed too; schema-diff skips them
	// with a reason instead of leaving them out without a word.
	tables, err := queries.GetTablesInSchema(ctx, pool, s)
	require.NoError(t, err)
	require.Equal(t, []string{"colgrant", "hidden", "n1_only", "nopk_hidden", "readable"}, tables)

	foreign, err := queries.GetForeignTablesInSchema(ctx, pool, s)
	require.NoError(t, err)
	require.Equal(t, []string{"a_foreign"}, foreign)

	views, err := queries.GetViewsInSchema(ctx, pool, s)
	require.NoError(t, err)
	require.Equal(t, []string{"a_view"}, views)

	privs, err := queries.CheckUserPrivileges(ctx, pool, selectOnlyRole, s, "readable")
	require.NoError(t, err)
	require.True(t, privs.SchemaUsage)
	require.True(t, privs.TableSelect)
	require.Empty(t, privs.SelectHint())

	privs, err = queries.CheckUserPrivileges(ctx, pool, selectOnlyRole, s, "hidden")
	require.NoError(t, err)
	require.False(t, privs.TableSelect)
	require.False(t, privs.ColumnSelect)
	require.Empty(t, privs.SelectHint())

	// SELECT on every column is still not SELECT on the table.
	privs, err = queries.CheckUserPrivileges(ctx, pool, selectOnlyRole, s, "colgrant")
	require.NoError(t, err)
	require.False(t, privs.TableSelect)
	require.True(t, privs.ColumnSelect)
	require.Contains(t, privs.SelectHint(), "individual columns")

	// has_table_privilege ignores schema USAGE, so the table check alone
	// would pass here.
	privs, err = queries.CheckUserPrivileges(ctx, pool, selectOnlyRole, catalogNoUsageSchema, "t")
	require.NoError(t, err)
	require.True(t, privs.TableSelect)
	require.False(t, privs.SchemaUsage)
}

// TestSchemaDiffSkipsUnreadableTables runs schema-diff as the SELECT-only
// role. The tables it cannot read are reported as skipped with the reason,
// even one without a primary key, the readable table is compared, and the
// run succeeds.
func TestSchemaDiffSkipsUnreadableTables(t *testing.T) {
	setupSelectOnlyRole(t)
	clusterName := writeRoleClusterConfig(t, selectOnlyRole, selectOnlyPassword)

	task := newTestSchemaDiffTask(catalogPrivsSchema, fmt.Sprintf("%s,%s", serviceN1, serviceN2))
	task.ClusterName = clusterName
	var diffErr error
	logOutput := captureLog(t, func() { diffErr = task.SchemaTableDiff() })

	require.NoError(t, diffErr)
	assert.NotEqual(t, taskstore.StatusFailed, task.Task.TaskStatus)
	summary := extractSummarySection(t, logOutput)

	identical := extractBetween(summary, "table(s) are identical:", "table(s)")
	assert.Contains(t, identical, catalogPrivsSchema+".readable")

	skipped := extractBetween(summary, "table(s) were skipped:", "table(s)")
	for _, table := range []string{"hidden", "nopk_hidden", "colgrant"} {
		assert.Contains(t, skipped,
			fmt.Sprintf("%s.%s (no SELECT privilege on %s)", catalogPrivsSchema, table, serviceN1))
	}
	// Readable on n1, so the label must name n2.
	assert.Contains(t, skipped,
		fmt.Sprintf("%s.n1_only (no SELECT privilege on %s)", catalogPrivsSchema, serviceN2))
}

// TestSchemaDiffFailsWithoutSchemaUsage: a table the role has SELECT on,
// in a schema it has no USAGE on, cannot be read either. That is a setup
// problem rather than a table to leave out, so the table fails instead of
// being skipped.
func TestSchemaDiffFailsWithoutSchemaUsage(t *testing.T) {
	setupSelectOnlyRole(t)
	clusterName := writeRoleClusterConfig(t, selectOnlyRole, selectOnlyPassword)

	task := newTestSchemaDiffTask(catalogNoUsageSchema, fmt.Sprintf("%s,%s", serviceN1, serviceN2))
	task.ClusterName = clusterName
	var diffErr error
	logOutput := captureLog(t, func() { diffErr = task.SchemaTableDiff() })

	require.Error(t, diffErr)
	assert.Contains(t, diffErr.Error(), "failed during Schema diff")
	summary := extractSummarySection(t, logOutput)
	failed := extractBetween(summary, "encountered errors and could not be compared:", "")
	assert.Contains(t, failed, catalogNoUsageSchema+".t")
	assert.Contains(t, failed, "does not have USAGE on schema")
	assert.NotContains(t, summary, "were skipped")
}

// TestSchemaDiffFailsWhenNothingIsReadable: skipping unreadable tables must
// not let a run that compared nothing succeed.
func TestSchemaDiffFailsWhenNothingIsReadable(t *testing.T) {
	setupSelectOnlyRole(t)
	clusterName := writeRoleClusterConfig(t, selectOnlyRole, selectOnlyPassword)

	task := newTestSchemaDiffTask(catalogUnreadableSchema, fmt.Sprintf("%s,%s", serviceN1, serviceN2))
	task.ClusterName = clusterName
	err := task.SchemaTableDiff()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "compared no tables")
}

// TestRepsetDiffSkipsUnreadableTables is the repset-diff side of
// TestSchemaDiffSkipsUnreadableTables.
func TestRepsetDiffSkipsUnreadableTables(t *testing.T) {
	ctx := context.Background()
	setupSelectOnlyRole(t)
	clusterName := writeRoleClusterConfig(t, selectOnlyRole, selectOnlyPassword)

	for _, p := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		// repset-diff reads the set's tables from spock's catalog.
		_, err := p.Exec(ctx, "GRANT USAGE ON SCHEMA spock TO "+selectOnlyRole)
		require.NoError(t, err)
		_, err = p.Exec(ctx, "GRANT SELECT ON ALL TABLES IN SCHEMA spock TO "+selectOnlyRole)
		require.NoError(t, err)
		_, err = p.Exec(ctx, "SELECT spock.repset_create($1)", catalogPrivsRepset)
		require.NoError(t, err)
		for _, table := range []string{"readable", "hidden"} {
			_, err = p.Exec(ctx, "SELECT spock.repset_add_table($1, $2)", catalogPrivsRepset, catalogPrivsSchema+"."+table)
			require.NoError(t, err)
		}
	}
	// Registered after setupSelectOnlyRole's cleanup, so it runs first and
	// the set is gone before its tables are dropped.
	t.Cleanup(func() {
		for _, p := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			p.Exec(ctx, "SELECT spock.repset_drop($1)", catalogPrivsRepset) //nolint:errcheck
		}
	})

	task := newTestRepsetDiffTask(catalogPrivsRepset)
	task.ClusterName = clusterName
	var diffErr error
	logOutput := captureLog(t, func() { diffErr = diff.RepsetDiff(task) })

	require.NoError(t, diffErr)
	summary := extractSummarySection(t, logOutput)
	identical := extractBetween(summary, "table(s) are identical:", "table(s)")
	assert.Contains(t, identical, catalogPrivsSchema+".readable")
	skipped := extractBetween(summary, "table(s) were skipped:", "table(s)")
	assert.Contains(t, skipped,
		fmt.Sprintf("%s.hidden (no SELECT privilege on %s)", catalogPrivsSchema, serviceN1))
}

// captureLog runs fn with the logger writing to a pipe and returns what it
// logged.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	logger.SetOutput(w)
	defer logger.SetOutput(os.Stderr)

	fn()

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	t.Logf("Captured log output:\n%s", buf.String())
	return buf.String()
}

// writeRoleClusterConfig copies the test cluster config with user and
// password as the database credentials, and returns the new cluster name.
func writeRoleClusterConfig(t *testing.T, user, password string) string {
	t.Helper()
	data, err := os.ReadFile(pgCluster.ClusterName + ".json")
	require.NoError(t, err)
	var cfg types.ClusterConfig
	require.NoError(t, json.Unmarshal(data, &cfg))

	cfg.ClusterName = pgCluster.ClusterName + "_" + user
	for i := range cfg.PGEdge.Databases {
		cfg.PGEdge.Databases[i].DBUser = user
		cfg.PGEdge.Databases[i].DBPassword = password
	}

	data, err = json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)
	path := cfg.ClusterName + ".json"
	require.NoError(t, os.WriteFile(path, data, 0644))
	t.Cleanup(func() { os.Remove(path) })
	return cfg.ClusterName
}
