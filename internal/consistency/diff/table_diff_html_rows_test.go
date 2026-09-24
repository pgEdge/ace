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

package diff

import (
	"strings"
	"testing"

	"github.com/pgedge/ace/pkg/config"
)

// TestValidateMaxHTMLRows checks how Validate resolves max_html_rows: the
// task value wins, 0 falls back to table_diff.max_html_rows, and a negative
// value in either place is an error. Validate goes on to fail on other
// checks later (the task here is incomplete), so only the max_html_rows
// errors and the resolved value are checked.
func TestValidateMaxHTMLRows(t *testing.T) {
	for _, tc := range []struct {
		name     string
		task     int64
		cfg      int64
		want     int64
		wantErrs string
	}{
		{name: "task value wins", task: 7, cfg: 500, want: 7},
		{name: "config when task is 0", task: 0, cfg: 500, want: 500},
		{name: "both 0 leave the default to the writer", task: 0, cfg: 0, want: 0},
		{name: "negative task value", task: -1, cfg: 500, wantErrs: "max_html_rows must be >= 0"},
		{name: "negative config value", task: 0, cfg: -5, wantErrs: "table_diff.max_html_rows in the config must be >= 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.TableDiff.MinBlockSize = 1
			cfg.TableDiff.MaxBlockSize = 1000000
			cfg.TableDiff.MaxHTMLRows = tc.cfg
			config.Set(cfg)
			t.Cleanup(func() { config.Set(nil) })

			task := NewTableDiffTask()
			task.ClusterName = "c"
			task.QualifiedTableName = "public.t"
			task.BlockSize = 1000
			task.MaxHTMLRows = tc.task

			err := task.Validate()
			if tc.wantErrs != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrs) {
					t.Fatalf("Validate: got error %v, want one containing %q", err, tc.wantErrs)
				}
				return
			}
			if err != nil && strings.Contains(err.Error(), "max_html_rows") {
				t.Fatalf("Validate: unexpected max_html_rows error: %v", err)
			}
			if task.MaxHTMLRows != tc.want {
				t.Errorf("MaxHTMLRows after Validate: got %d, want %d", task.MaxHTMLRows, tc.want)
			}
		})
	}
}
