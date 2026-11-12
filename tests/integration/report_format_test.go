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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// typedKeyTable creates a table with the given column definitions on both
// nodes, adds it to the default replication set, and loads rows on each node
// in repair mode, so that nothing is replicated. It returns the qualified
// table name.
func typedKeyTable(t *testing.T, env *testEnv, name, defs, cols string, n1Rows, n2Rows []string) string {
	t.Helper()
	ctx := context.Background()
	qualifiedTable := fmt.Sprintf("%s.%s", testSchema, name)
	for _, pool := range env.pools() {
		_, err := pool.Exec(ctx, fmt.Sprintf("CREATE TABLE %s (%s)", qualifiedTable, defs)) // nosemgrep
		require.NoError(t, err)
		if env.HasSpock {
			_, err = pool.Exec(ctx, "SELECT spock.repset_add_table('default', $1)", qualifiedTable)
			require.NoError(t, err)
		}
	}
	t.Cleanup(func() {
		for _, pool := range env.pools() {
			if env.HasSpock {
				_, _ = pool.Exec(context.Background(), "SELECT spock.repset_remove_table('default', $1)", qualifiedTable)
			}
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualifiedTable+" CASCADE") // nosemgrep
		}
	})
	for _, side := range []struct {
		pool *pgxpool.Pool
		rows []string
	}{{env.N1Pool, n1Rows}, {env.N2Pool, n2Rows}} {
		if len(side.rows) == 0 {
			continue
		}
		tx, err := side.pool.Begin(ctx)
		require.NoError(t, err)
		env.withRepairModeTx(t, ctx, tx, func() {
			_, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", qualifiedTable, cols, strings.Join(side.rows, ", "))) // nosemgrep
			require.NoError(t, err)
		})
		require.NoError(t, tx.Commit(ctx))
	}
	return qualifiedTable
}

// diffResultOf reads the JSON report of the last run of a table-diff or
// mtree table-diff task. A run that found no differences has no report;
// then the result has no pairs.
func diffResultOf(t *testing.T, task any) types.DiffOutput {
	t.Helper()
	var path string
	var summary types.DiffSummary
	switch tk := task.(type) {
	case *diff.TableDiffTask:
		path, summary = tk.DiffFilePath, tk.DiffResult.Summary
	case *mtree.MerkleTreeTask:
		path, summary = tk.DiffFilePath, tk.DiffResult.Summary
	default:
		t.Fatalf("diffResultOf: unsupported task %T", task)
	}
	if path == "" {
		require.Zero(t, summary.DiffPairs(), "the run counted differences but wrote no report")
		return types.DiffOutput{NodeDiffs: map[string]types.DiffByNodePair{}, Summary: summary}
	}
	return readDiffReport(t, path)
}

// intValue returns a whole number from a value of a diff row: a Go integer
// or a JSON number.
func intValue(v any) (int64, bool) {
	switch x := v.(type) {
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case int:
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	case float64:
		if x == float64(int64(x)) {
			return int64(x), true
		}
	}
	return 0, false
}

// diffPairs returns the differing rows of the last run of a task, by node
// pair, as the JSON report has them.
func diffPairs(t *testing.T, task any) map[string]types.DiffByNodePair {
	t.Helper()
	return diffResultOf(t, task).NodeDiffs
}

func readDiffReport(t *testing.T, path string) types.DiffOutput {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var out types.DiffOutput
	require.NoError(t, json.Unmarshal(data, &out))
	return out
}

// copyDiffResult copies the result directory of diffFile and the JSON report
// next to it into a new directory, applies edit to the manifest of the copy,
// and returns the path of the copied JSON report.
func copyDiffResult(t *testing.T, diffFile string, edit func(manifest map[string]any)) string {
	t.Helper()
	srcDir := strings.TrimSuffix(diffFile, filepath.Ext(diffFile))
	dstDir := filepath.Join(t.TempDir(), filepath.Base(srcDir))
	require.NoError(t, os.CopyFS(dstDir, os.DirFS(srcDir)))
	data, err := os.ReadFile(diffFile)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dstDir+".json", data, 0o600))

	path := filepath.Join(dstDir, "manifest.json")
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	// UseNumber keeps large integers such as max_diff_rows exact.
	var manifest map[string]any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&manifest))
	edit(manifest)
	data, err = json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return dstDir + ".json"
}

// TestDiffReportFormatVersionChecked checks that table-diff writes the
// report format version, and that repair and rerun refuse a report written
// in another format: by a newer ACE, or by an older one. Repair reads the
// row format version of the result directory, rerun the format version of
// the JSON report.
func TestDiffReportFormatVersionChecked(t *testing.T) {
	env := newSpockEnv()
	qualifiedTable := typedKeyTable(t, env, "ace_format_version", "id int PRIMARY KEY, v text", "id, v",
		[]string{"(1, 'a')"}, []string{"(1, 'b')"})
	nodes := []string{env.ServiceN1, env.ServiceN2}
	diffFile := env.runTableDiff(t, qualifiedTable, nodes)

	// UseNumber keeps large integers such as max_diff_rows exact.
	var raw map[string]any
	data, err := os.ReadFile(diffFile)
	require.NoError(t, err)
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&raw))
	require.Equal(t, json.Number(fmt.Sprint(types.DiffFormatVersion)), raw["format_version"])

	// A report without the field was written before the field existed, and
	// version 1 kept a numeric value without its trailing zeros. The keys
	// of both do not match the rows read now, so the diff must run again.
	for _, tc := range []struct {
		version int
		want    string
	}{
		{types.DiffFormatVersion + 1, "unsupported diff report format version"},
		{1, "run the diff again"},
		{0, "run the diff again"},
	} {
		result := copyDiffResult(t, diffFile, func(m map[string]any) {
			m["row_format_version"] = tc.version
		})
		err = env.newTableRepairTask(env.ServiceN1, qualifiedTable, result).ValidateAndPrepare()
		require.ErrorContains(t, err, tc.want, "repair of version %d", tc.version)

		if tc.version == 0 {
			delete(raw, "format_version")
		} else {
			raw["format_version"] = tc.version
		}
		report, err := json.Marshal(raw)
		require.NoError(t, err)
		reportFile := filepath.Join(t.TempDir(), fmt.Sprintf("v%d_diffs.json", tc.version))
		require.NoError(t, os.WriteFile(reportFile, report, 0o600))
		rerunTask := env.newTableDiffTask(t, qualifiedTable, nodes)
		rerunTask.DiffFilePath = reportFile
		err = rerunTask.ExecuteRerunTask()
		require.ErrorContains(t, err, tc.want, "rerun of version %d", tc.version)
	}
}
