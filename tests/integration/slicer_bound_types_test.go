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
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/stretchr/testify/require"
)

// slicerBoundRows is enough rows that the sample method keeps several pages
// per block and does not fall back to the cutter on a tiny table.
const slicerBoundRows = 20000

// slicerBoundCase is a primary key of a hard type, built from an integer g.
// n carries g in a plain column, so the divergence does not depend on the key
// type. PR #224 tests these types only through the histogram path (mtree); the
// forced cutter and sample paths are tested here, with the same key held as
// text and cut into block bounds.
type slicerBoundCase struct {
	name    string
	setup   []string
	drop    []string
	columns string
	keyExpr string // SQL for the key, over generate_series value g
}

func slicerBoundCases() []slicerBoundCase {
	return []slicerBoundCase{
		{
			name:    "interval",
			columns: "id interval PRIMARY KEY, n int, v text",
			keyExpr: "(g * 977 || ' seconds')::interval",
		},
		{
			name:    "citext",
			setup:   []string{"CREATE EXTENSION IF NOT EXISTS citext"},
			columns: "id citext PRIMARY KEY, n int, v text",
			keyExpr: "md5(g::text)::citext",
		},
		{
			name:    "numeric_scale",
			columns: "id numeric PRIMARY KEY, n int, v text",
			keyExpr: "(g::numeric / 8)",
		},
		{
			name:    "bytea",
			columns: "id bytea PRIMARY KEY, n int, v text",
			keyExpr: "decode(md5(g::text), 'hex')",
		},
		{
			name:    "timestamp",
			columns: "id timestamp PRIMARY KEY, n int, v text",
			keyExpr: "'2020-01-01 00:00:00'::timestamp + (g || ' seconds')::interval",
		},
	}
}

// TestSlicerBoundTypesCutterAndSample forces the cutter and the sample for a
// hard-typed key, with a spread-out interior divergence. A bound that did not
// round-trip through text, or a comparison done on the wrong text, would move
// a block boundary and the diff would not report exactly the changed rows.
func TestSlicerBoundTypesCutterAndSample(t *testing.T) {
	ctx := context.Background()
	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	for _, tc := range slicerBoundCases() {
		for _, method := range []string{slicer.MethodCutter, slicer.MethodSample} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				table := "slicer_bound_" + tc.name
				qualified := testSchema + "." + table
				ident := pgx.Identifier{testSchema, table}.Sanitize()

				for _, p := range pools {
					for _, s := range tc.setup {
						slicingExec(t, ctx, p, s)
					}
					slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident+" CASCADE")
					slicingExec(t, ctx, p, fmt.Sprintf("CREATE TABLE %s (%s) WITH (autovacuum_enabled = false)", ident, tc.columns))
				}
				t.Cleanup(func() {
					for _, p := range pools {
						slicingExec(t, ctx, p, "DROP TABLE IF EXISTS "+ident+" CASCADE")
						for _, d := range tc.drop {
							slicingExec(t, ctx, p, d)
						}
					}
				})

				insert := fmt.Sprintf(
					"INSERT INTO %s SELECT %s, g, md5(g::text) FROM generate_series(1, %d) g",
					ident, tc.keyExpr, slicerBoundRows)
				for _, p := range pools {
					slicingExec(t, ctx, p, insert)
					slicingExec(t, ctx, p, "ANALYZE "+ident)
				}

				// Diverge n = 2000, 4000, ... 18000 on n2 only: nine rows,
				// spread over the interior so every method has bounds between
				// them.
				tag, err := pgCluster.Node2Pool.Exec(ctx, // nosemgrep
					fmt.Sprintf("UPDATE %s SET v = 'changed' WHERE n %% 2000 = 0 AND n BETWEEN 2000 AND 18000", ident))
				require.NoError(t, err)
				require.EqualValues(t, 9, tag.RowsAffected(), "the divergence must be the nine interior rows")

				diffs, summary := slicingDiffWith(t, qualified, method)
				t.Logf("%s/%s: method actually run = %s (%s), blocks diffed = %d",
					tc.name, method, summary.Slicer.Method, summary.Slicer.Reason, diffs)
				require.Equal(t, method, summary.Slicer.Method,
					"the forced method must be the one that ran, so the key really is cut this way")
				require.Equal(t, 9, diffs,
					"forced %s on a %s key must find exactly the nine changed rows", method, tc.name)
			})
		}
	}
}
