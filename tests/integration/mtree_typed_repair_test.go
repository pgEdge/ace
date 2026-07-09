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
	"path/filepath"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/pgedge/ace/internal/consistency/diff"
)

// typedRepairDDL covers the type classes repair must round-trip: temporal,
// money, uuid, interval, numeric, plus the complex ones the driver scans as
// opaque structs (geometric, range, bit, network, enum, xml).
const typedRepairDDL = ` (id BIGINT PRIMARY KEY, name VARCHAR(100), col_time TIME, col_timetz TIMETZ,
	col_money MONEY, col_uuid UUID, col_interval INTERVAL, col_num NUMERIC(12,4),
	col_point POINT, col_range INT4RANGE, col_daterange DATERANGE, col_bit BIT(8),
	col_inet INET, col_xml XML, col_mood typed_repair_mood)`

const typedRepairSeedSQL = ` (id, name, col_time, col_timetz, col_money, col_uuid, col_interval, col_num,
		col_point, col_range, col_daterange, col_bit, col_inet, col_xml, col_mood)
	SELECT i,
	       'name_' || i,
	       TIME '00:00:00' + (i * 137 || ' seconds')::interval,
	       TIMETZ '00:00:00+02' + (i * 91 || ' seconds')::interval,
	       (i * 13.37)::numeric::money,
	       ('00000000-0000-0000-0000-' || lpad(i::text, 12, '0'))::uuid,
	       (i || ' hours 30 minutes')::interval,
	       i * 1.5,
	       point(i, i * 2),
	       int4range(i, i + 100),
	       daterange(DATE '2020-01-01' + i, DATE '2020-01-01' + i + 30),
	       (i % 256)::bit(8),
	       ('10.0.' || (i % 256) || '.' || (i % 200 + 1))::inet,
	       ('<item id="' || i || '"><name>item_' || i || '</name></item>')::xml,
	       (ARRAY['happy','sad','neutral']::typed_repair_mood[])[(i % 3) + 1]
	FROM generate_series(1, 100) AS i`

// seedTypedRepairTable creates the typed-columns table on both nodes and seeds
// rows on n1 only -- the diverged state a disabled subscription leaves behind.
func seedTypedRepairTable(t *testing.T, ctx context.Context, env *testEnv, safe string) {
	t.Helper()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}
	for _, pool := range pools {
		_, err := pool.Exec(ctx, "DROP TYPE IF EXISTS typed_repair_mood CASCADE")
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "CREATE TYPE typed_repair_mood AS ENUM ('happy','sad','neutral')")
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+typedRepairDDL) // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
			_, _ = pool.Exec(ctx, "DROP TYPE IF EXISTS typed_repair_mood CASCADE")
		}
	})

	_, err := env.N1Pool.Exec(ctx, "INSERT INTO "+safe+typedRepairSeedSQL) // nosemgrep
	require.NoError(t, err)
	for _, pool := range pools {
		_, err := pool.Exec(ctx, "ANALYZE "+safe) // nosemgrep
		require.NoError(t, err)
	}
}

// diffReportsFor snapshots the diff report files currently on disk for a table.
func diffReportsFor(t *testing.T, tableName string) map[string]struct{} {
	t.Helper()
	matches, err := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
	require.NoError(t, err)
	seen := make(map[string]struct{}, len(matches))
	for _, m := range matches {
		seen[m] = struct{}{}
	}
	return seen
}

// newDiffReportSince returns the newest diff report written after the snapshot,
// so a repair never runs from a report left over by an earlier test.
func newDiffReportSince(t *testing.T, tableName string, seen map[string]struct{}) string {
	t.Helper()
	matches, err := filepath.Glob(fmt.Sprintf("%s_%s_diffs-*.json", testSchema, tableName))
	require.NoError(t, err)
	matches = slices.DeleteFunc(matches, func(m string) bool {
		_, ok := seen[m]
		return ok
	})
	require.NotEmpty(t, matches, "mtree diff should have written a diff report")
	slices.Sort(matches)
	return matches[len(matches)-1]
}

// typedRepairFingerprint hashes a table's full ordered contents on one node.
func typedRepairFingerprint(t *testing.T, ctx context.Context, pool *pgxpool.Pool, safe string) (fp string) {
	t.Helper()
	require.NoError(t, pool.QueryRow(ctx, "SELECT COALESCE(md5(string_agg(tr::text, ',' ORDER BY id)), 'empty') FROM "+safe+" tr").Scan(&fp)) // nosemgrep
	return
}

// A diff produced by the Merkle-tree engine repairs cleanly on a table
// spanning temporal, money, uuid, interval, numeric, geometric, range, bit,
// network, enum and xml columns, and the nodes converge.
func TestMtreeDiffRepairTypedColumns(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "typed_repair"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	seedTypedRepairTable(t, ctx, env, safe)

	mtreeTask := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	mtreeTask.BlockSize = 100
	mtreeTask.OverrideBlockSize = true
	require.NoError(t, mtreeTask.RunChecks(false))
	require.NoError(t, mtreeTask.MtreeInit())
	t.Cleanup(func() { _ = mtreeTask.MtreeTeardown() })
	require.NoError(t, mtreeTask.BuildMtree())

	// mtree table-diff must find the 100 n1-only rows and write a new report.
	before := diffReportsFor(t, tableName)
	require.NoError(t, mtreeTask.DiffMtree())
	total := 0
	for _, c := range mtreeTask.DiffResult.Summary.DiffRowsCount {
		total += c
	}
	require.Equal(t, 100, total, "mtree diff should report the 100 divergent rows")
	diffFile := newDiffReportSince(t, tableName, before)

	// The repair must complete -- previously the time column's serialized pgx
	// struct aborted every upsert -- and converge the nodes.
	repairTask := env.newTableRepairTask(env.ServiceN1, qualified, diffFile)
	require.NoError(t, repairTask.Run(false), "table-repair from an mtree diff must succeed on typed columns")

	require.Equal(t, typedRepairFingerprint(t, ctx, env.N1Pool, safe), typedRepairFingerprint(t, ctx, env.N2Pool, safe),
		"nodes must hold identical rows (all typed columns included) after repair")

	// table-rerun re-fetches the diffed rows by primary key -- a separate
	// SELECT that must apply the same ::TEXT cast policy, or typed columns
	// fail the scan (timetz) or re-compare as spurious diffs. On the now
	// converged nodes it must succeed and confirm every diff resolved.
	rerunTask := diff.NewTableDiffTask()
	rerunTask.Mode = "rerun"
	rerunTask.ClusterName = env.ClusterName
	rerunTask.DBName = env.DBName
	rerunTask.DiffFilePath = diffFile
	rerunTask.Ctx = ctx
	require.NoError(t, rerunTask.ExecuteTask(), "table-rerun must succeed on typed columns")

	rerunReports, err := filepath.Glob(fmt.Sprintf("%s_%s_rerun-diffs-*.json", testSchema, tableName))
	require.NoError(t, err)
	require.Empty(t, rerunReports, "rerun on converged nodes must not report persistent diffs")
}
