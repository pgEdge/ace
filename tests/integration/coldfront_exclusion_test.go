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
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSchemaDiff_ColdFrontSchemaRejected verifies that schema-diff refuses
// to diff the ColdFront schema. The rejection happens in Validate(), so no
// setup is required.
func TestSchemaDiff_ColdFrontSchemaRejected(t *testing.T) {
	nodes := fmt.Sprintf("%s,%s", serviceN1, serviceN2)

	task := newTestSchemaDiffTask("coldfront", nodes)
	err := task.SchemaTableDiff()

	require.Error(t, err, "schema-diff must refuse to diff the coldfront schema")
	assert.Contains(t, err.Error(), "reserved for the pgEdge ColdFront extension")
}

// TestRepsetDiff_ColdFrontSchemaExcluded verifies that repset-diff skips
// tables in the ColdFront schema even when they are part of the repset.
// The exclusion keys off the schema name only, so the extension itself
// does not need to be installed.
func TestRepsetDiff_ColdFrontSchemaExcluded(t *testing.T) {
	ctx := context.Background()
	const repsetName = "default"
	const coldfrontTable = "claims_stub"
	qualifiedColdfront := fmt.Sprintf("coldfront.%s", coldfrontTable)

	pools := []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool}

	// Diverge the data between nodes: a broken exclusion would report it
	// as a difference.
	createSQL := fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS coldfront;
		CREATE TABLE IF NOT EXISTS %s (
			id  INT PRIMARY KEY,
			val TEXT
		)`, qualifiedColdfront)
	for _, pool := range pools {
		_, err := pool.Exec(ctx, createSQL)
		require.NoError(t, err)
	}
	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'from_n1') ON CONFLICT DO NOTHING`, qualifiedColdfront))
	require.NoError(t, err)
	_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf(
		`INSERT INTO %s (id, val) VALUES (1, 'from_n2') ON CONFLICT DO NOTHING`, qualifiedColdfront))
	require.NoError(t, err)

	for _, pool := range pools {
		_, err := pool.Exec(ctx,
			fmt.Sprintf(`SELECT spock.repset_add_table('%s', '%s');`, repsetName, qualifiedColdfront))
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			pool.Exec(ctx, fmt.Sprintf(
				`SELECT spock.repset_remove_table('%s', '%s');`, repsetName, qualifiedColdfront))
			pool.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS %s CASCADE`, qualifiedColdfront))
		}
	})

	// Control table: ordinary tables must still be diffed.
	controlQualified := createRepsetDiffTable(t, "cf_excl_control_tbl", repsetName, false)

	r, w, err := os.Pipe()
	require.NoError(t, err)
	logger.SetOutput(w)
	t.Cleanup(func() { logger.SetOutput(os.Stderr) })

	task := newTestRepsetDiffTask(repsetName)
	diffErr := diff.RepsetDiff(task)

	w.Close()
	var buf bytes.Buffer
	buf.ReadFrom(r)
	logOutput := buf.String()
	t.Logf("Captured log output:\n%s", logOutput)

	require.NoError(t, diffErr)

	summarySection := extractSummarySection(t, logOutput)

	skippedSection := extractBetween(summarySection, "table(s) were skipped:", "table(s)")
	assert.Contains(t, skippedSection, qualifiedColdfront,
		"the ColdFront table should be reported as skipped")
	assert.Contains(t, skippedSection, "pgEdge ColdFront schema",
		"the skipped entry should state why it was excluded")

	identicalSection := extractBetween(summarySection, "table(s) are identical:", "table(s)")
	assert.Contains(t, identicalSection, controlQualified,
		"the control table should still be diffed")

	diffSection := extractBetween(summarySection, "table(s) have differences:", "")
	assert.NotContains(t, diffSection, coldfrontTable,
		"the ColdFront table must not be reported as differing")
}
