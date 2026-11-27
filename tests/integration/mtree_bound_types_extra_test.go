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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// boundTypeCase is a primary key type whose block bounds PR #224 does not test:
// citext (a dynamic OID the driver does not know, compared case-insensitively),
// interval, a domain over a base type, and numeric with a NaN value (NaN sorts
// after every number, so it ends up in the last block's bound). setup runs on
// both nodes before the table is made.
type boundTypeCase struct {
	name      string
	pkType    string
	setup     []string
	drop      []string
	valuesSQL string // exactly pkeyTypeRowCount distinct values of pkType
}

func boundTypeCases() []boundTypeCase {
	g := fmt.Sprintf("generate_series(1, %d) g", pkeyTypeRowCount)
	return []boundTypeCase{
		{
			name:      "citext",
			pkType:    "CITEXT",
			setup:     []string{"CREATE EXTENSION IF NOT EXISTS citext"},
			valuesSQL: "SELECT md5(g::text)::citext FROM " + g,
		},
		{
			name:      "interval",
			pkType:    "INTERVAL",
			valuesSQL: "SELECT (g * 977 || ' seconds')::interval FROM " + g,
		},
		{
			name:      "domain_over_int",
			pkType:    testSchema + ".bound_dom",
			setup:     []string{"CREATE DOMAIN " + testSchema + ".bound_dom AS integer"},
			drop:      []string{"DROP DOMAIN IF EXISTS " + testSchema + ".bound_dom CASCADE"},
			valuesSQL: "SELECT (g * 7919)::integer FROM " + g,
		},
		{
			name:   "numeric_with_nan",
			pkType: "NUMERIC",
			// NaN is the largest numeric, so it is the bound of the last block.
			valuesSQL: "SELECT CASE WHEN g = " + fmt.Sprintf("%d", pkeyTypeRowCount) +
				" THEN 'NaN'::numeric ELSE (g * 7919)::numeric / 97 END FROM " + g,
		},
	}
}

// TestMtreeBoundTypesExtra cuts each table into several blocks and diverges
// rows in the interior, so the block bounds of these key types really are cut,
// carried as text, sorted and compared. A bound that did not round-trip, or a
// comparison done on the wrong text, would move a boundary and the diff would
// not find exactly the diverged rows.
func TestMtreeBoundTypesExtra(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, tc := range boundTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			tableName := "mtree_bound_" + tc.name
			qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
			safe := pgx.Identifier{testSchema, tableName}.Sanitize()

			for _, pool := range pools {
				for _, s := range tc.setup {
					_, err := pool.Exec(ctx, s) // nosemgrep
					require.NoError(t, err)
				}
				_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
					"CREATE TABLE IF NOT EXISTS %s (id %s PRIMARY KEY, val TEXT)", safe, tc.pkType))
				require.NoError(t, err)
			}
			t.Cleanup(func() {
				for _, pool := range pools {
					execOK(t, ctx, pool, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
					for _, d := range tc.drop {
						execOK(t, ctx, pool, d) // nosemgrep
					}
				}
			})

			for _, pool := range pools {
				_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
					"INSERT INTO %s (id, val) SELECT v, 'same' FROM (%s) s(v)", safe, tc.valuesSQL))
				require.NoError(t, err)
				_, err = pool.Exec(ctx, "ANALYZE "+safe) // nosemgrep
				require.NoError(t, err)
			}
			t.Cleanup(func() { removeDiffResults(t, qualified) })

			task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
			task.BlockSize = 100
			task.OverrideBlockSize = true
			require.NoError(t, task.RunChecks(false))
			require.NoError(t, task.MtreeInit())
			t.Cleanup(func() { assert.NoError(t, task.MtreeTeardown()) })
			require.NoError(t, task.BuildMtree())

			tag, err := env.N1Pool.Exec(ctx, fmt.Sprintf( // nosemgrep
				`WITH ranked AS (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM %s)
				 UPDATE %s SET val = 'diverged'
				 WHERE id IN (SELECT id FROM ranked WHERE rn BETWEEN %d AND %d AND rn %% %d = 0)`,
				safe, safe, pkeyTypeDivergeFrom, pkeyTypeDivergeTo, pkeyTypeDivergeEvery))
			require.NoError(t, err)
			require.EqualValues(t, pkeyTypeDivergedRows, tag.RowsAffected(),
				"the divergence must land in the interior blocks")

			var reached int
			require.NoError(t, env.N2Pool.QueryRow(ctx, // nosemgrep
				"SELECT count(*) FROM "+safe+" WHERE val = 'diverged'").Scan(&reached))
			require.Zero(t, reached, "the change must not have reached the peer node")

			require.NoError(t, task.DiffMtree())
			total := 0
			for _, c := range task.DiffResult.Summary.DiffRowsCount {
				total += c
			}
			require.Equal(t, pkeyTypeDivergedRows, total,
				"%d rows differ, so the diff must report exactly that many", pkeyTypeDivergedRows)
		})
	}
}
