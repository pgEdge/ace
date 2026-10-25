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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// reportRows renders the rows of one node of a node pair as sorted JSON
// lines, without the replication metadata, so that two reports can be
// compared.
func reportRows(t *testing.T, out types.DiffOutput, pairKey, node string) []string {
	t.Helper()
	var lines []string
	for _, row := range out.NodeDiffs[pairKey].Rows[node] {
		var kept types.OrderedMap
		for _, kv := range row {
			switch kv.Key {
			case "_spock_metadata_", "commit_ts", "node_origin":
				continue
			}
			kept = append(kept, kv)
		}
		b, err := json.Marshal(kept)
		require.NoError(t, err)
		lines = append(lines, string(b))
	}
	sort.Strings(lines)
	return lines
}

// TestTableDiffRerunKeepsRows runs table-diff, then a rerun of its report
// with nothing changed in between. Every difference is still there, so the
// rerun report must hold the same rows with the same values. The rerun reads
// the rows with its own query, and they must have the same form as in the
// diff report, or the rerun loses them or reports other values.
func TestTableDiffRerunKeepsRows(t *testing.T) {
	env := newSpockEnv()
	qualifiedTable := typedKeyTable(t, env, "ace_rerun_typed",
		"id uuid PRIMARY KEY, amount numeric, at time, span interval, tags text[]",
		"id, amount, at, span, tags",
		[]string{
			"('00000000-0000-0000-0000-000000000001', 1.50, '10:00', '1 day', '{a}')",
			"('00000000-0000-0000-0000-000000000002', 2, '11:00', '2 hours', '{b}')",
			"('00000000-0000-0000-0000-000000000003', 3, '12:00', '3 mins', '{c}')",
		},
		[]string{
			"('00000000-0000-0000-0000-000000000001', 1.50, '10:00', '1 day', '{a}')",
			"('00000000-0000-0000-0000-000000000002', 2.5, '11:30', '2 hours', '{b,x}')",
			"('00000000-0000-0000-0000-000000000004', 4, '13:00', '4 secs', '{d}')",
		})

	nodes := []string{env.ServiceN1, env.ServiceN2}
	pairKey := env.pairKey()
	diffFile := env.runTableDiff(t, qualifiedTable, nodes)
	original := readDiffReport(t, diffFile)
	require.Equal(t, types.DiffFormatVersion, original.FormatVersion, "table-diff must write the report format version")
	require.Equal(t, 3, original.Summary.DiffRowsCount[pairKey])

	rerunGlob := strings.ReplaceAll(qualifiedTable, ".", "_") + "_rerun-diffs-*.json"
	removeRerunFiles := func() {
		files, _ := filepath.Glob(rerunGlob)
		for _, f := range files {
			_ = os.Remove(f)
		}
	}
	removeRerunFiles()
	t.Cleanup(removeRerunFiles)

	task := env.newTableDiffTask(t, qualifiedTable, nodes)
	task.DiffFilePath = diffFile
	require.NoError(t, task.ExecuteRerunTask())

	files, err := filepath.Glob(rerunGlob)
	require.NoError(t, err)
	require.Len(t, files, 1, "the rerun must write a report: every difference is still there")
	rerun := readDiffReport(t, files[0])
	require.Equal(t, types.DiffFormatVersion, rerun.FormatVersion, "rerun must write the report format version")
	require.Equal(t, original.Summary.DiffRowsCount[pairKey], rerun.Summary.DiffRowsCount[pairKey])
	for _, node := range nodes {
		require.Equal(t, reportRows(t, original, pairKey, node), reportRows(t, rerun, pairKey, node),
			"rows of %s in the rerun report", node)
	}
}

// TestAdvancedRepairPlan_StaleCheckFindsKey checks the stale check of a
// repair plan for key types whose raw driver value prints in another form
// than the value in the diff report: numeric (1.50 against 1.5 on the other
// node) and uuid, as a simple key and together in a composite key. The
// check reads the current commit timestamp of each target row and matches it
// to the report by key. If the keys do not match, the check is skipped with
// no message, and repair overwrites a row that changed after the diff.
func TestAdvancedRepairPlan_StaleCheckFindsKey(t *testing.T) {
	cases := []struct {
		name     string
		defs     string // column definitions of the table
		cols     string // columns of the rows in n1 and n2
		key      string // the primary key, as SQL
		n1, n2   []string
		modified string // key of the modified row, as SQL
		extra    string // key of the row only on n2, as SQL
	}{
		{
			name:     "numeric",
			defs:     "id numeric PRIMARY KEY, v text",
			cols:     "id, v",
			key:      "id",
			n1:       []string{"(1.50, 'a')"},
			n2:       []string{"(1.5, 'b')", "(2.00, 'x')"},
			modified: "1.5",
			extra:    "2",
		},
		{
			name:     "uuid",
			defs:     "id uuid PRIMARY KEY, v text",
			cols:     "id, v",
			key:      "id",
			n1:       []string{"('00000000-0000-0000-0000-000000000001', 'a')"},
			n2:       []string{"('00000000-0000-0000-0000-000000000001', 'b')", "('00000000-0000-0000-0000-000000000002', 'x')"},
			modified: "'00000000-0000-0000-0000-000000000001'",
			extra:    "'00000000-0000-0000-0000-000000000002'",
		},
		{
			// A composite key is checked with another query.
			name:     "composite",
			defs:     "id numeric, u uuid, v text, PRIMARY KEY (id, u)",
			cols:     "id, u, v",
			key:      "(id, u)",
			n1:       []string{"(1.50, '00000000-0000-0000-0000-000000000001', 'a')"},
			n2:       []string{"(1.5, '00000000-0000-0000-0000-000000000001', 'b')", "(2.00, '00000000-0000-0000-0000-000000000002', 'x')"},
			modified: "(1.5, '00000000-0000-0000-0000-000000000001'::uuid)",
			extra:    "(2, '00000000-0000-0000-0000-000000000002'::uuid)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			env := newSpockEnv()
			qualifiedTable := typedKeyTable(t, env, "ace_stale_key_"+tc.name, tc.defs, tc.cols, tc.n1, tc.n2)
			diffFile := env.runTableDiff(t, qualifiedTable, []string{env.ServiceN1, env.ServiceN2})
			beforeLogs := listStaleSkipLogs(t)

			// Change both rows on n2 after the diff: their commit timestamps
			// are now newer than the ones in the report.
			time.Sleep(10 * time.Millisecond)
			tx, err := env.N2Pool.Begin(ctx)
			require.NoError(t, err)
			env.withRepairModeTx(t, ctx, tx, func() {
				_, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET v = 'stale' WHERE %s IN (%s, %s)", qualifiedTable, tc.key, tc.modified, tc.extra)) // nosemgrep
				require.NoError(t, err)
			})
			require.NoError(t, tx.Commit(ctx))

			plan := fmt.Sprintf(`
version: 1
tables:
  %s:
    default_action: { type: skip }
    rules:
      - name: keep_n1_if_not_stale
        diff_type: [row_mismatch]
        action: { type: keep_n1, allow_stale_repairs: false }
      - name: delete_extra_if_not_stale
        diff_type: [missing_on_n1]
        action: { type: delete, allow_stale_repairs: false }
`, qualifiedTable)
			planPath := filepath.Join(t.TempDir(), "repair.yaml")
			require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))

			task := env.newTableRepairTask("", qualifiedTable, diffFile)
			task.RepairPlanPath = planPath
			require.NoError(t, task.ValidateAndPrepare())
			require.NoError(t, task.Run(true))

			// Both rows changed after the diff, so repair must leave them.
			var v string
			require.NoError(t, env.N2Pool.QueryRow(ctx, fmt.Sprintf("SELECT v FROM %s WHERE %s = %s", qualifiedTable, tc.key, tc.modified)).Scan(&v)) // nosemgrep
			require.Equal(t, "stale", v, "the modified row changed after the diff and must not be overwritten")
			var n int
			require.NoError(t, env.N2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE %s = %s", qualifiedTable, tc.key, tc.extra)).Scan(&n)) // nosemgrep
			require.Equal(t, 1, n, "the extra row changed after the diff and must not be deleted")

			entries := readStaleSkipEntries(t, findNewStaleSkipLog(t, beforeLogs))
			ops := map[string]bool{}
			for _, e := range entries {
				if e["table"] == qualifiedTable {
					op, _ := e["operation"].(string)
					ops[op] = true
				}
			}
			require.Equal(t, map[string]bool{"upsert": true, "delete": true}, ops, "stale skip log entries: %v", entries)
		})
	}
}
