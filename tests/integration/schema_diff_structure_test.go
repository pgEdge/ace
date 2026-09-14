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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/schema"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/stretchr/testify/require"
)

// These tests cover schema-diff --compare=structure against a real two-node
// cluster, going through real catalogs on two independently initialised
// nodes so the query behind CollectSnapshot actually executes.
//
// The schema uses a domain, an enum and a composite type: PostgreSQL assigns
// each a different OID per node, so structural comparison must key on their
// names and definitions.

const structureSchema = "ace_structure_test"

const structureTable = "t"

// setupStructureSchema creates structureSchema with identical DDL on both
// nodes, but shifts node2's OID counter first so no user-defined type ends
// up with the same OID on both nodes.
func setupStructureSchema(t *testing.T) {
	t.Helper()
	ctx := context.Background()

	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}
	for _, pool := range pools {
		_, err := pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, structureSchema))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA %s`, structureSchema))
		require.NoError(t, err)
	}

	// Burn OIDs on node2 only.
	_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf(`
		DO $$
		BEGIN
			FOR i IN 1..40 LOOP
				EXECUTE format('CREATE DOMAIN %s.burn%%s AS int', i);
				EXECUTE format('DROP DOMAIN %s.burn%%s', i);
			END LOOP;
		END $$`, structureSchema, structureSchema))
	require.NoError(t, err)

	ddl := []string{
		fmt.Sprintf(`CREATE DOMAIN %s.short AS varchar(20)`, structureSchema),
		fmt.Sprintf(`CREATE TYPE %s.mood AS ENUM ('sad', 'ok', 'happy')`, structureSchema),
		fmt.Sprintf(`CREATE TYPE %s.addr AS (city text, zip int)`, structureSchema),
		fmt.Sprintf(`CREATE TABLE %s.%s (
			id   int PRIMARY KEY,
			code %s.short,
			m    %s.mood,
			a    %s.addr,
			n    int
		)`, structureSchema, structureTable, structureSchema, structureSchema, structureSchema),
	}
	for _, pool := range pools {
		for _, stmt := range ddl {
			_, err := pool.Exec(ctx, stmt)
			require.NoError(t, err, "ddl: %s", stmt)
		}
	}

	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Exec(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE`, structureSchema)) //nolint:errcheck // best-effort cleanup
		}
	})

	requireUserTypeOIDsDiffer(t)
}

// requireUserTypeOIDsDiffer pins the premise of these tests: the two nodes
// really do disagree about the OIDs of identically-defined types. Without
// this the "identical structure reports nothing" test below could pass for
// the wrong reason.
func requireUserTypeOIDsDiffer(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	const q = `SELECT t.oid::int FROM pg_type t
	           JOIN pg_namespace n ON n.oid = t.typnamespace
	           WHERE n.nspname = $1 AND t.typname = 'short'`

	var oid1, oid2 int
	require.NoError(t, pgCluster.Node1Pool.QueryRow(ctx, q, structureSchema).Scan(&oid1))
	require.NoError(t, pgCluster.Node2Pool.QueryRow(ctx, q, structureSchema).Scan(&oid2))
	require.NotEqual(t, oid1, oid2,
		"this test needs the two nodes to assign different OIDs to the same domain")
}

func structureTaskForTest() *diff.SchemaDiffCmd {
	task := newTestSchemaDiffTask(structureSchema, fmt.Sprintf("%s,%s", serviceN1, serviceN2))
	task.Compare = diff.CompareStructure
	return task
}

// compareStructureForTest collects both nodes' snapshots and compares them,
// the same way schemaStructureDiff does, so a test can assert on individual
// Divergences.
func compareStructureForTest(t *testing.T) []schema.Divergence {
	t.Helper()
	ctx := context.Background()
	tables := []string{structureTable}

	snapA, err := schema.CollectSnapshot(ctx, pgCluster.Node1Pool, serviceN1, structureSchema, tables)
	require.NoError(t, err)
	snapB, err := schema.CollectSnapshot(ctx, pgCluster.Node2Pool, serviceN2, structureSchema, tables)
	require.NoError(t, err)

	return schema.Compare(structureSchema, tables, snapA, snapB)
}

func findDivergenceFor(t *testing.T, divs []schema.Divergence, object, property string) schema.Divergence {
	t.Helper()
	for _, d := range divs {
		if d.Object == object && d.Property == property {
			return d
		}
	}
	t.Fatalf("no divergence for object %q property %q in %+v", object, property, divs)
	return schema.Divergence{}
}

// TestSchemaDiffStructure_IdenticalSchemaReportsNothing is the false-positive
// guard: the same DDL on both nodes, with every user-defined type carrying a
// different OID on each. Anything reported here is noise, and noise in this
// mode is expensive — its whole purpose is a severity-coded exit status a
// script can act on.
func TestSchemaDiffStructure_IdenticalSchemaReportsNothing(t *testing.T) {
	setupStructureSchema(t)

	divs := compareStructureForTest(t)
	require.Empty(t, divs, "identical structure must produce no divergences")

	require.NoError(t, structureTaskForTest().SchemaTableDiff(),
		"identical structure must not produce an exit-code error")
}

// TestSchemaDiffStructure_ColumnAndTypeDriftIsReported drifts one column, one
// enum and one composite attribute, and checks each is found with the
// expected rank, including which side of a narrowing is the narrow one.
func TestSchemaDiffStructure_ColumnAndTypeDriftIsReported(t *testing.T) {
	setupStructureSchema(t)
	ctx := context.Background()

	for _, stmt := range []string{
		// int4 -> int8: node1 is now the narrow side.
		fmt.Sprintf(`ALTER TABLE %s.%s ALTER COLUMN n TYPE bigint`, structureSchema, structureTable),
		// A column node1 does not have at all.
		fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN extra text`, structureSchema, structureTable),
		// A value node1's enum cannot hold. The type's OID does not change,
		// so nothing but its definition can reveal this.
		fmt.Sprintf(`ALTER TYPE %s.mood ADD VALUE 'furious'`, structureSchema),
	} {
		_, err := pgCluster.Node2Pool.Exec(ctx, stmt)
		require.NoError(t, err, "drift: %s", stmt)
	}

	// Drift a composite type's attribute too. ALTER TYPE ... ALTER ATTRIBUTE
	// refuses to run at all - CASCADE included - while any plain table
	// column uses the type directly (CASCADE there only reaches typed
	// tables, ones created with CREATE TABLE ... OF the type, which this
	// one is not). So the column is dropped first and re-added after the
	// type is rebuilt; column order is not part of what this mode compares.
	for _, stmt := range []string{
		fmt.Sprintf(`ALTER TABLE %s.%s DROP COLUMN a`, structureSchema, structureTable),
		fmt.Sprintf(`DROP TYPE %s.addr`, structureSchema),
		fmt.Sprintf(`CREATE TYPE %s.addr AS (city text, zip bigint)`, structureSchema),
		fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN a %s.addr`, structureSchema, structureTable, structureSchema),
	} {
		_, err := pgCluster.Node2Pool.Exec(ctx, stmt)
		require.NoError(t, err, "drift: %s", stmt)
	}

	divs := compareStructureForTest(t)

	qualified := structureSchema + "." + structureTable

	colType := findDivergenceFor(t, divs, qualified+".n", "type")
	require.Equal(t, schema.RankNarrowed, colType.Rank)
	require.Equal(t, serviceN1, colType.NarrowSide, "int4 is the narrow side")

	extra := findDivergenceFor(t, divs, qualified+".extra", "")
	require.Equal(t, schema.RankAbsent, extra.Rank)

	mood := findDivergenceFor(t, divs, structureSchema+".mood", "labels")
	require.Equal(t, schema.RankIncompatible, mood.Rank)
	require.Contains(t, mood.ValueOnB, "furious")
	require.NotContains(t, mood.ValueOnA, "furious")

	// The composite attribute is named for a reader, with its value shown
	// as PostgreSQL prints the type. int4 -> int8 is a narrowing here too,
	// the same as it is for a table column (see colType above).
	addr := findDivergenceFor(t, divs, structureSchema+".addr", "attribute 2 (zip)")
	require.Equal(t, schema.RankNarrowed, addr.Rank)
	require.Equal(t, serviceN1, addr.NarrowSide, "int4 is the narrow side")
	require.Equal(t, "integer", addr.ValueOnA)
	require.Equal(t, "bigint", addr.ValueOnB)

	// The exit code the mode exists for.
	err := structureTaskForTest().SchemaTableDiff()
	var exitErr *utils.ExitCodeError
	require.True(t, errors.As(err, &exitErr), "expected an ExitCodeError, got %v", err)
	require.Equal(t, schema.ExitIncompatible, exitErr.Code)
}

// TestSchemaDiffStructure_DomainDefinitionDrift covers two domain cases: a
// narrowed base type, and CHECK sets where each side has a constraint the
// other lacks, which must be reported as one incompatible finding.
func TestSchemaDiffStructure_DomainDefinitionDrift(t *testing.T) {
	setupStructureSchema(t)
	ctx := context.Background()

	_, err := pgCluster.Node1Pool.Exec(ctx,
		fmt.Sprintf(`ALTER DOMAIN %s.short ADD CONSTRAINT short_min CHECK (length(VALUE) > 2)`, structureSchema))
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx,
		fmt.Sprintf(`ALTER DOMAIN %s.short ADD CONSTRAINT short_max CHECK (length(VALUE) < 10)`, structureSchema))
	require.NoError(t, err)

	divs := compareStructureForTest(t)

	var checks []schema.Divergence
	for _, d := range divs {
		if d.Object == structureSchema+".short" && d.Property == "check" {
			checks = append(checks, d)
		}
	}
	require.Len(t, checks, 1,
		"mutually exclusive CHECK sets must be one finding, not one per side: %+v", checks)
	require.Equal(t, schema.RankIncompatible, checks[0].Rank)
	// Checked against the number only, not the full "length(VALUE) > 2"
	// text: PostgreSQL may print VALUE with an explicit ::text cast here
	// depending on version, and that cast is not what this test is about.
	require.Contains(t, checks[0].ValueOnA, "> 2")
	require.Contains(t, checks[0].ValueOnB, "< 10")
}

// TestSchemaDiffStructure_ReportOrderIsStable checks that two comparisons of
// the same unchanged pair of nodes return findings in the same order, since
// type-level findings are gathered from a map.
func TestSchemaDiffStructure_ReportOrderIsStable(t *testing.T) {
	setupStructureSchema(t)
	ctx := context.Background()

	for _, stmt := range []string{
		fmt.Sprintf(`CREATE DOMAIN %s.d1 AS int CHECK (VALUE > 1)`, structureSchema),
		fmt.Sprintf(`CREATE DOMAIN %s.d2 AS int CHECK (VALUE > 2)`, structureSchema),
		fmt.Sprintf(`CREATE DOMAIN %s.d3 AS int CHECK (VALUE > 3)`, structureSchema),
	} {
		_, err := pgCluster.Node1Pool.Exec(ctx, stmt)
		require.NoError(t, err)
		_, err = pgCluster.Node2Pool.Exec(ctx, replaceCheck(stmt))
		require.NoError(t, err)
	}
	for i, col := range []string{"c1", "c2", "c3"} {
		stmt := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN %s %s.d%d`,
			structureSchema, structureTable, col, structureSchema, i+1)
		_, err := pgCluster.Node1Pool.Exec(ctx, stmt)
		require.NoError(t, err)
		_, err = pgCluster.Node2Pool.Exec(ctx, stmt)
		require.NoError(t, err)
	}

	first := compareStructureForTest(t)
	require.NotEmpty(t, first, "the three domains differ, so there is something to order")

	for i := 0; i < 5; i++ {
		again := compareStructureForTest(t)
		require.Equal(t, first, again, "the same comparison produced a different report")
	}
}

// TestSchemaDiffStructure_SkipTablesExcludesTable checks that --skip-tables
// (SkipTables) excludes a table from --compare=structure the same way it
// already does from the default per-table data diff: a table named there
// must not contribute to the report or the exit code, even though it still
// exists and still genuinely differs.
func TestSchemaDiffStructure_SkipTablesExcludesTable(t *testing.T) {
	setupStructureSchema(t)
	ctx := context.Background()

	// A real divergence: node2 gets an extra column. Left unskipped, this
	// must produce an ExitCodeError - this is the control the skipped case
	// below is contrasted against.
	_, err := pgCluster.Node2Pool.Exec(ctx,
		fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN extra text`, structureSchema, structureTable))
	require.NoError(t, err)

	t.Run("NotSkippedStillReportsTheDivergence", func(t *testing.T) {
		err := structureTaskForTest().SchemaTableDiff()
		var exitErr *utils.ExitCodeError
		require.True(t, errors.As(err, &exitErr), "expected an ExitCodeError, got %v", err)
		require.Equal(t, schema.ExitIncompatible, exitErr.Code)
	})

	t.Run("SkippedTableProducesNoDivergence", func(t *testing.T) {
		task := structureTaskForTest()
		task.SkipTables = structureTable
		require.NoError(t, task.SchemaTableDiff(),
			"the only table in scope was named by --skip-tables, so nothing was left to compare")
	})
}

// TestSchemaDiffStructure_SkipTablesExcludesMissingTableFromReport checks
// that --skip-tables also keeps a table missing on some nodes out of the
// "tables missing on some nodes" section and out of the exit code, not only
// out of the per-table structural comparison (that part is covered by
// TestSchemaDiffStructure_SkipTablesExcludesTable above). Without this, a
// table the user explicitly excluded would still be reported as missing and
// would still force schema.ExitIncompatible.
func TestSchemaDiffStructure_SkipTablesExcludesMissingTableFromReport(t *testing.T) {
	setupStructureSchema(t)
	ctx := context.Background()

	const onlyOnNode1 = "only_on_node1"
	_, err := pgCluster.Node1Pool.Exec(ctx,
		fmt.Sprintf(`CREATE TABLE %s.%s (id int PRIMARY KEY)`, structureSchema, onlyOnNode1))
	require.NoError(t, err)

	t.Run("NotSkippedStillReportsMissingTable", func(t *testing.T) {
		err := structureTaskForTest().SchemaTableDiff()
		var exitErr *utils.ExitCodeError
		require.True(t, errors.As(err, &exitErr), "expected an ExitCodeError, got %v", err)
		require.Equal(t, schema.ExitIncompatible, exitErr.Code)
	})

	t.Run("SkippedMissingTableProducesNoFailure", func(t *testing.T) {
		task := structureTaskForTest()
		task.SkipTables = onlyOnNode1
		require.NoError(t, task.SchemaTableDiff(),
			"the only asymmetric table was named by --skip-tables, so it must not be reported as missing or force a failure")
	})
}

// captureStdout redirects os.Stdout for the duration of fn and returns
// everything written to it. schemaStructureDiff prints straight to
// os.Stdout rather than returning the report, so this is the only way to
// check what --output=json actually produced.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	orig := os.Stdout
	os.Stdout = w

	fn()

	require.NoError(t, w.Close())
	os.Stdout = orig
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out)
}

// TestSchemaDiffStructure_JSONOutput checks --output=json end-to-end: it
// must be valid JSON carrying the same divergence the text report shows,
// with the same exit code, and it must only replace the text report when
// --output was actually given (OutputExplicit) - see SchemaDiffCmd's doc
// comment on that field for why "json" being the flag's own default is not
// enough on its own.
func TestSchemaDiffStructure_JSONOutput(t *testing.T) {
	setupStructureSchema(t)
	ctx := context.Background()

	_, err := pgCluster.Node2Pool.Exec(ctx,
		fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN extra text`, structureSchema, structureTable))
	require.NoError(t, err)

	task := structureTaskForTest()
	task.Output = "json"
	task.OutputExplicit = true

	var runErr error
	stdout := captureStdout(t, func() {
		runErr = task.SchemaTableDiff()
	})

	var exitErr *utils.ExitCodeError
	require.True(t, errors.As(runErr, &exitErr), "expected an ExitCodeError, got %v", runErr)
	require.Equal(t, schema.ExitIncompatible, exitErr.Code)

	var report diff.StructureDiffReport
	require.NoError(t, json.Unmarshal([]byte(stdout), &report), "stdout must be valid JSON: %s", stdout)
	require.Equal(t, structureSchema, report.Schema)
	require.Equal(t, exitErr.Code, report.ExitCode)
	require.Len(t, report.Comparisons, 1)

	found := false
	for _, d := range report.Comparisons[0].Divergences {
		if d.Object == structureSchema+"."+structureTable+".extra" {
			found = true
			require.Equal(t, schema.RankAbsent, d.Rank)
		}
	}
	require.True(t, found, "expected the added column to appear in the JSON report: %+v", report)
}

// TestSchemaDiffStructure_TextOutputWithoutExplicitFlag checks the other
// half of the same guard: a run that never passes --output must keep
// printing the prose report, even though "json" is the flag's own default
// value once it reaches SchemaDiffCmd.Output.
func TestSchemaDiffStructure_TextOutputWithoutExplicitFlag(t *testing.T) {
	setupStructureSchema(t)

	task := structureTaskForTest()
	// task.Output and task.OutputExplicit both left at their zero values,
	// the same state a task never touched by the --output flag is in.

	stdout := captureStdout(t, func() {
		_ = task.SchemaTableDiff()
	})
	require.True(t, strings.HasPrefix(strings.TrimSpace(stdout), "==="),
		"default output must stay the prose report, not JSON: %s", stdout)
}

// replaceCheck strips the CHECK clause, so node2 gets the same domains
// without the constraint node1's carry.
func replaceCheck(stmt string) string {
	if idx := indexOfCheck(stmt); idx >= 0 {
		return stmt[:idx]
	}
	return stmt
}

func indexOfCheck(stmt string) int {
	const needle = " CHECK ("
	for i := 0; i+len(needle) <= len(stmt); i++ {
		if stmt[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
