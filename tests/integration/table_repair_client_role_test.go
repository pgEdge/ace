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

	"github.com/stretchr/testify/require"
)

// TestTableRepairClientRole runs repair as a client role without superuser
// rights. The target gets an insert and an update: two commands on one
// connection. The client role must end with the transaction of each command;
// if it stayed in the session, the second command could not set
// session_replication_role. The role gets USAGE on schema spock, because
// spock.repair_mode(false) runs at the end of each command as this role.
func TestTableRepairClientRole(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	const role = "ace_repair_client"
	table := typedKeyTable(t, env, "ace_client_role", "id int PRIMARY KEY, v text", "id, v",
		[]string{"(1, 'a')", "(2, 'b')"},
		[]string{"(2, 'x')"})
	for _, pool := range env.pools() {
		for _, sql := range []string{
			"DO $$ BEGIN CREATE ROLE " + role + " NOLOGIN; EXCEPTION WHEN duplicate_object THEN NULL; END $$",
			"GRANT USAGE ON SCHEMA " + testSchema + ", spock TO " + role,
			"GRANT SELECT, INSERT, UPDATE, DELETE ON " + table + " TO " + role,
		} {
			_, err := pool.Exec(ctx, sql) // nosemgrep
			require.NoError(t, err)
		}
	}

	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	task := env.newTableRepairTask(env.ServiceN1, table, diffFile)
	task.ClientRole = role
	require.NoError(t, task.Run(false), "failed row operations: %+v", task.FailedOps)
	require.Empty(t, task.FailedOps)
	env.assertNoTableDiff(t, table)
}
