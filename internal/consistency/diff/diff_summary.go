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
	"fmt"
	"strings"

	"github.com/pgedge/ace/pkg/logger"
)

// MissingTableInfo records a table that was not found on every node.
type MissingTableInfo struct {
	Table       string   // schema-qualified table name
	PresentOn   []string // node names where the table exists
	MissingFrom []string // node names where the table does not exist
}

// FailedTableInfo records a table whose diff failed along with the reason.
type FailedTableInfo struct {
	Table string // schema-qualified table name
	Err   error  // the error that caused the failure
}

// DiffSummary collects per-table outcomes during a schema-diff or repset-diff
// run and prints a human-readable summary at the end.
type DiffSummary struct {
	MatchedTables  []string
	DifferedTables []string
	FailedTables   []FailedTableInfo
	MissingTables  []MissingTableInfo
	SkippedTables  []string
}

// PrintAndFinalize logs the summary and returns an error if any tables failed.
// The label identifies the scope (e.g. "schema public" or "repset default").
func (s *DiffSummary) PrintAndFinalize(commandName, label string) error {
	total := len(s.MatchedTables) + len(s.DifferedTables) + len(s.FailedTables) +
		len(s.SkippedTables) + len(s.MissingTables)
	logger.Info("%s summary: %d table(s) in %s", commandName, total, label)
	if len(s.MatchedTables) > 0 {
		logger.Info("  %d table(s) are identical:", len(s.MatchedTables))
		for _, t := range s.MatchedTables {
			logger.Info("    - %s", t)
		}
	}
	if len(s.SkippedTables) > 0 {
		logger.Info("  %d table(s) were skipped:", len(s.SkippedTables))
		for _, t := range s.SkippedTables {
			logger.Info("    - %s", t)
		}
	}
	if len(s.DifferedTables) > 0 {
		logger.Warn("  %d table(s) have differences:", len(s.DifferedTables))
		for _, t := range s.DifferedTables {
			logger.Warn("    - %s", t)
		}
	}
	if len(s.MissingTables) > 0 {
		logger.Warn("  %d table(s) not present on all nodes:", len(s.MissingTables))
		for _, mt := range s.MissingTables {
			logger.Warn("    - %s found on [%s] but missing from [%s]",
				mt.Table,
				strings.Join(mt.PresentOn, ", "),
				strings.Join(mt.MissingFrom, ", "))
		}
	}
	if len(s.FailedTables) > 0 {
		logger.Error("  %d table(s) encountered errors and could not be compared:", len(s.FailedTables))
		var names []string
		for _, ft := range s.FailedTables {
			logger.Error("    - %s: %v", ft.Table, ft.Err)
			names = append(names, ft.Table)
		}
		return fmt.Errorf("%d table(s) failed during %s: %v", len(s.FailedTables), commandName, names)
	}

	return nil
}
