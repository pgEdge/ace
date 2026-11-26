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

// exoticTypesDDL covers the built-in types that the type tests of #215 and the
// mtree typed-repair test leave out: the other ranges and a multirange, bit
// varying, the geometric types beyond point, the rest of the network types,
// full text, and pg_lsn. Every value goes to the target as the text output of
// its type, so each column checks that input(output(v)) = v under the pinned
// session settings.
const exoticTypesDDL = ` (id BIGINT PRIMARY KEY,
	col_tstzrange   tstzrange,
	col_numrange    numrange,
	col_int8range   int8range,
	col_int4mr      int4multirange,
	col_varbit      bit varying(16),
	col_line        line,
	col_lseg        lseg,
	col_box         box,
	col_path        path,
	col_circle      circle,
	col_polygon     polygon,
	col_macaddr     macaddr,
	col_macaddr8    macaddr8,
	col_cidr        cidr,
	col_tsvector    tsvector,
	col_tsquery     tsquery,
	col_pglsn       pg_lsn)`

const exoticTypesSeedSQL = ` (id, col_tstzrange, col_numrange, col_int8range, col_int4mr,
		col_varbit, col_line, col_lseg, col_box, col_path, col_circle, col_polygon,
		col_macaddr, col_macaddr8, col_cidr, col_tsvector, col_tsquery, col_pglsn)
	SELECT i,
	       tstzrange(TIMESTAMPTZ '2020-01-01 00:00:00+00' + (i || ' days')::interval,
	                 TIMESTAMPTZ '2020-01-01 00:00:00+00' + ((i + 30) || ' days')::interval),
	       numrange(i::numeric, (i + 10.5)::numeric),
	       int8range(i::bigint, (i + 1000)::bigint),
	       int4multirange(int4range(i, i + 10), int4range(i + 100, i + 110)),
	       (i % 65536)::bit(16)::varbit,
	       ('{1,' || (i + 1) || ',' || (i + 2) || '}')::line,
	       ('[(' || i || ',' || i || '),(' || (i + 1) || ',' || (i + 2) || ')]')::lseg,
	       ('((' || i || ',' || i || '),(' || (i + 2) || ',' || (i + 3) || '))')::box,
	       ('[(' || i || ',0),(' || (i + 1) || ',1),(' || (i + 2) || ',0)]')::path,
	       circle(point(i, i), i + 1),
	       ('((' || i || ',0),(' || (i + 1) || ',1),(' || (i + 2) || ',0))')::polygon,
	       ('08:00:2b:' || lpad(to_hex(i % 256), 2, '0') || ':' || lpad(to_hex((i + 1) % 256), 2, '0') || ':' || lpad(to_hex((i + 2) % 256), 2, '0'))::macaddr,
	       ('08:00:2b:01:' || lpad(to_hex(i % 256), 2, '0') || ':' || lpad(to_hex((i + 1) % 256), 2, '0') || ':02:03')::macaddr8,
	       ('10.' || (i % 256) || '.' || ((i + 1) % 256) || '.0/24')::cidr,
	       to_tsvector('simple', 'word' || i || ' shared term'),
	       to_tsquery('simple', 'word' || i),
	       ('0/' || to_hex(i * 1000 + 1))::pg_lsn
	FROM generate_series(1, 100) AS i`

func seedExoticTypesTable(t *testing.T, ctx context.Context, env *testEnv, safe string) {
	t.Helper()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}
	for _, pool := range pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+exoticTypesDDL) // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			execOK(t, ctx, pool, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
	})
	_, err := env.N1Pool.Exec(ctx, "INSERT INTO "+safe+exoticTypesSeedSQL) // nosemgrep
	require.NoError(t, err)
	for _, pool := range pools {
		_, err := pool.Exec(ctx, "ANALYZE "+safe) // nosemgrep
		require.NoError(t, err)
	}
}

// TestMtreeDiffRepairExoticTypes seeds 100 rows on n1 only, across the exotic
// built-in types, then diffs and repairs. The repair inserts every row on n2,
// so each column's text output must read back into the same value. The nodes
// must converge.
func TestMtreeDiffRepairExoticTypes(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "exotic_types_repair"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	seedExoticTypesTable(t, ctx, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	mtreeTask.BlockSize = 100
	mtreeTask.OverrideBlockSize = true
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() { assert.NoError(t, mtreeTask.MtreeTeardown()) })
	require.NoError(t, mtreeTask.BuildMtree())

	before := diffReportsFor(t, tableName)
	require.NoError(t, mtreeTask.DiffMtree())
	total := 0
	for _, c := range mtreeTask.DiffResult.Summary.DiffRowsCount {
		total += c
	}
	require.Equal(t, 100, total, "diff should report the 100 divergent rows")
	diffFile := newDiffReportSince(t, tableName, before)

	repairTask := env.newTableRepairTask(env.ServiceN1, qualified, diffFile)
	require.NoError(t, repairTask.Run(false), "table-repair must succeed on the exotic types")
	require.Empty(t, repairTask.FailedOps, "no row operation should fail: %+v", repairTask.FailedOps)

	require.Equal(t,
		typedRepairFingerprint(t, ctx, env.N1Pool, safe),
		typedRepairFingerprint(t, ctx, env.N2Pool, safe),
		"nodes must hold identical rows after repair")

	// A second diff must see no difference: a value that did not round-trip
	// would show up here even if the repair reported success.
	env.assertNoTableDiff(t, qualified)
}
