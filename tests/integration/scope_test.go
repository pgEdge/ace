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
	"fmt"
	"testing"

	"github.com/pgedge/ace/internal/consistency/scope"
	"github.com/stretchr/testify/require"
)

// TestSchemaProvider_ResolvesBaseTablesOnly verifies that scope.SchemaProvider
// returns every base table in a schema and excludes views, since views are
// DDL-only and schema-diff does not compare their data.
func TestSchemaProvider_ResolvesBaseTablesOnly(t *testing.T) {
	ctx := context.Background()
	tableName := "scope_provider_table"
	viewName := "scope_provider_view"

	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s.%s (id INT PRIMARY KEY)`, testSchema, tableName))
	require.NoError(t, err, "create table")
	_, err = pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`CREATE OR REPLACE VIEW %s.%s AS SELECT id FROM %s.%s`, testSchema, viewName, testSchema, tableName))
	require.NoError(t, err, "create view")

	t.Cleanup(func() {
		pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(`DROP VIEW IF EXISTS %s.%s`, testSchema, viewName))           //nolint:errcheck
		pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s.%s CASCADE`, testSchema, tableName)) //nolint:errcheck
	})

	provider := scope.SchemaProvider{SchemaName: testSchema}
	resolved, err := provider.Resolve(ctx, pgCluster.Node1Pool)
	require.NoError(t, err)

	var sawTable, sawView bool
	for _, q := range resolved.Tables {
		require.Equal(t, testSchema, q.Schema, "every entry must belong to the requested schema")
		switch q.Table {
		case tableName:
			sawTable = true
		case viewName:
			sawView = true
		}
	}
	require.True(t, sawTable, "base table must be in scope")
	require.False(t, sawView, "view must not be in scope: schema-diff treats views as DDL-only")
	require.Equal(t, provider.Describe(), resolved.Source)
}

// TestSchemaProvider_UnknownSchemaIsEmptyNotError checks that Resolve leaves
// schema-existence checks to the caller: an unknown schema resolves to zero
// tables, not an error.
func TestSchemaProvider_UnknownSchemaIsEmptyNotError(t *testing.T) {
	ctx := context.Background()
	provider := scope.SchemaProvider{SchemaName: "schema_that_does_not_exist_anywhere"}

	resolved, err := provider.Resolve(ctx, pgCluster.Node1Pool)
	require.NoError(t, err)
	require.Empty(t, resolved.Tables)
}
