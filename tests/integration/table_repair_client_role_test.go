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
	"os"
	"path/filepath"
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

// TestTableRepairClientRoleComparisons checks that the comparisons of a
// repair plan run with the rights of the client role. The enum of the table
// is in a schema on which the role first has no USAGE, so the comparison of
// the when rule fails there, before any row operation. With USAGE the plan
// runs, and the rule picks the row of n2 for 'ok'.
func TestTableRepairClientRoleComparisons(t *testing.T) {
	env := newSpockEnv()
	ctx := context.Background()
	const role = "ace_repair_client"
	createTypesOnBoth(t, env, []string{
		`CREATE SCHEMA "ace cmp s"`,
		`CREATE TYPE "ace cmp s".mood AS ENUM ('ok', 'bad')`,
	}, []string{
		`DROP SCHEMA IF EXISTS "ace cmp s" CASCADE`,
	})
	table := typedKeyTable(t, env, "ace_client_role_cmp", `id int PRIMARY KEY, m "ace cmp s".mood, v text`, "id, m, v",
		[]string{"(1, 'ok', 'n1')", "(2, 'bad', 'n1')"},
		[]string{"(1, 'ok', 'n2')", "(2, 'bad', 'n2')"})
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

	plan := fmt.Sprintf(`
version: 1
tables:
  %s:
    default_action: { type: keep_n1 }
    rules:
      - name: ok_rows
        when: "n1.m = 'ok'"
        action: { type: keep_n2 }
`, table)
	planPath := filepath.Join(t.TempDir(), "plan.yaml")
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o600))
	diffFile := env.runTableDiff(t, table, []string{env.ServiceN1, env.ServiceN2})
	run := func() error {
		task := env.newTableRepairTask("", table, diffFile)
		task.RepairPlanPath = planPath
		task.ClientRole = role
		return task.Run(false)
	}

	err := run()
	require.ErrorContains(t, err, "compare values on the server")
	require.ErrorContains(t, err, "permission denied for schema")

	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, `GRANT USAGE ON SCHEMA "ace cmp s" TO `+role) // nosemgrep
		require.NoError(t, err)
	}
	require.NoError(t, run())
	for _, pool := range env.pools() {
		require.Equal(t, []string{"(1,ok,n2)", "(2,bad,n1)"}, rowTexts(t, pool, table, "id"))
	}
}
