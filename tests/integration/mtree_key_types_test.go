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

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestMtreeRefusesDifferentKeyTypes checks that mtree stops when a primary
// key column has another type on a node. With varchar(3) on one node and
// text on the other, a key of four characters would be cut to three when
// the row is looked up, and the row would be lost from the diff.
func TestMtreeRefusesDifferentKeyTypes(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_key_types"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	keydiffExec(t, ctx, env, env.N1Pool, fmt.Sprintf("CREATE TABLE %s (id varchar(3) PRIMARY KEY, v text)", ident))
	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("CREATE TABLE %s (id text PRIMARY KEY, v text)", ident))

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	err := task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mtree needs the same key types on every node")
	require.Contains(t, err.Error(), `"id" has type character varying(3)`)
}

// TestMtreeNonKeyTypesMayDiffer checks that columns outside the key may have
// other types on the nodes: the diff reads the rows of each node with the
// types of that node and finds the changed row.
func TestMtreeNonKeyTypesMayDiffer(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	table := "mtree_nonkey_types"
	qualified := testSchema + "." + table
	ident := pgx.Identifier{testSchema, table}.Sanitize()
	dropOnBoth(t, ctx, env, "TABLE IF EXISTS "+ident)
	keydiffExec(t, ctx, env, env.N1Pool,
		fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, n int4, v varchar(20))", ident),
		fmt.Sprintf("INSERT INTO %s SELECT g, g, 'v' || g FROM generate_series(1, 100) g", ident))
	keydiffExec(t, ctx, env, env.N2Pool,
		fmt.Sprintf("CREATE TABLE %s (id int PRIMARY KEY, n int8, v text)", ident),
		fmt.Sprintf("INSERT INTO %s SELECT g, g, 'v' || g FROM generate_series(1, 100) g", ident))

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { _ = task.MtreeTeardown() })
	require.NoError(t, task.BuildMtree())

	keydiffExec(t, ctx, env, env.N2Pool, fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE id = 42", ident))
	require.NoError(t, task.DiffMtree())

	pair, ok := diffPairs(t, task)[env.pairKey()]
	require.True(t, ok, "no diff for %s", env.pairKey())
	require.Len(t, pair.Rows[env.ServiceN1], 1)
	require.Len(t, pair.Rows[env.ServiceN2], 1)
	id, _ := pair.Rows[env.ServiceN2][0].Get("id")
	v, _ := pair.Rows[env.ServiceN2][0].Get("v")
	require.Equal(t, "42", fmt.Sprint(id))
	require.Equal(t, "changed", v)
}
