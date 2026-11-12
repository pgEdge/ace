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

package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/pgedge/ace/db/queries"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
)

type staleRepairSkipEntry struct {
	Timestamp       string             `json:"timestamp"`
	Table           string             `json:"table"`
	Node            string             `json:"node"`
	Operation       string             `json:"operation"`
	Action          staleActionSummary `json:"action"`
	Rule            string             `json:"rule"`
	DiffType        string             `json:"diff_type"`
	Reason          string             `json:"reason"`
	DiffCommitTS    string             `json:"diff_commit_ts,omitempty"`
	DiffCommitTSRaw string             `json:"diff_commit_ts_raw,omitempty"`
	CurrentCommitTS string             `json:"current_commit_ts,omitempty"`
	PK              map[string]any     `json:"pk"`
	Row             map[string]any     `json:"row"`
}

type staleSkipLogger struct {
	file  *os.File
	path  string
	first bool
}

func (t *TableRepairTask) filterStaleRepairs(ctx context.Context, tx pgx.Tx, nodeName string, rows map[string]map[string]any, colTypes map[string]string, operation string) error {
	if t.RepairPlan == nil || len(rows) == 0 || t.planStaleChecks == nil {
		return nil
	}

	checks := t.planStaleChecks[nodeName]
	if len(checks) == 0 {
		return nil
	}

	candidates := make(map[string]map[string]any)
	for pk, row := range rows {
		if _, ok := checks[pk]; ok {
			candidates[pk] = row
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	currentCommitTS, err := fetchCommitTimestamps(ctx, tx, t, nodeName, colTypes, candidates)
	if err != nil {
		return err
	}

	for pk, row := range candidates {
		info, ok := checks[pk]
		if !ok {
			continue
		}
		currentTS, ok := currentCommitTS[pk]
		if !ok {
			continue
		}

		diffTS, diffErr := parseCommitTimestamp(info.DiffCommitRaw)
		reason := ""
		if diffErr != nil {
			reason = "invalid_diff_commit_ts"
		} else if diffTS == nil {
			reason = "missing_diff_commit_ts"
		} else if currentTS.After(*diffTS) {
			reason = "target_row_newer_than_diff"
		}
		if reason == "" {
			continue
		}

		diffTSRaw := ""
		if diffTS == nil && info.DiffCommitRaw != nil {
			diffTSRaw = fmt.Sprintf("%v", info.DiffCommitRaw)
		}
		pkMap, _ := extractPkMap(t.Key, row)
		entry := staleRepairSkipEntry{
			Timestamp:       time.Now().Format(time.RFC3339Nano),
			Table:           t.QualifiedTableName,
			Node:            nodeName,
			Operation:       operation,
			Action:          info.Action,
			Rule:            info.RuleName,
			DiffType:        info.DiffType,
			Reason:          reason,
			DiffCommitTS:    formatCommitTimestamp(diffTS),
			DiffCommitTSRaw: diffTSRaw,
			CurrentCommitTS: formatCommitTimestamp(&currentTS),
			PK:              pkMap,
			Row:             row,
		}
		t.logStaleSkip(entry)

		delete(rows, pk)
		delete(checks, pk)
		if matches, ok := t.planRuleMatches[nodeName]; ok && matches != nil {
			delete(matches, pk)
		}
	}

	return nil
}

// fetchCommitTimestamps reads the current commit timestamp of the rows on a
// node, by row key. A row is found by its key text, as in the diff: the key
// of the row must be equal to the key text and print as the key text, byte
// for byte. A row that is gone, or whose commit timestamp is not known, has
// no entry in the result.
func fetchCommitTimestamps(ctx context.Context, tx pgx.Tx, task *TableRepairTask, nodeName string, castTypes map[string]string, rows map[string]map[string]any) (map[string]time.Time, error) {
	results := make(map[string]time.Time)
	if len(rows) == 0 {
		return results, nil
	}
	n := len(task.Key)
	params := make([]string, n)
	keyCols := make([]string, n)
	conds := make([]string, 0, 2*n)
	for i, col := range task.Key {
		typ := castTypes[col]
		if typ == "" {
			return nil, fmt.Errorf("stale check on %s: no type for primary key column %s", nodeName, col)
		}
		params[i] = fmt.Sprintf("$%d::text[]", i+1)
		keyCols[i] = fmt.Sprintf("k%d", i+1)
		ident := pgx.Identifier{col}.Sanitize()
		conds = append(conds,
			fmt.Sprintf("t.%s = k.k%d::%s", ident, i+1, typ),
			fmt.Sprintf(`format('%%s', %s) COLLATE "C" = k.k%d`, queries.ColExpr("t", col), i+1))
	}
	query := fmt.Sprintf("SELECT k.pos, pg_xact_commit_timestamp(t.xmin) "+
		"FROM unnest(%s) WITH ORDINALITY AS k(%s, pos) JOIN %s AS t ON %s",
		strings.Join(params, ", "), strings.Join(keyCols, ", "),
		pgx.Identifier{task.Schema, task.Table}.Sanitize(), strings.Join(conds, " AND "))

	pks := make([]string, 0, len(rows))
	for pk := range rows {
		pks = append(pks, pk)
	}
	sort.Strings(pks)
	const batchSize = 1000
	for start := 0; start < len(pks); start += batchSize {
		batch := pks[start:min(start+batchSize, len(pks))]
		cols := make([][]string, n)
		for i := range cols {
			cols[i] = make([]string, len(batch))
		}
		for j, pk := range batch {
			for i, col := range task.Key {
				text, ok := valueText(rows[pk][col])
				if !ok {
					return nil, fmt.Errorf("stale check on %s: primary key column %s of row %s is NULL", nodeName, col, pk)
				}
				cols[i][j] = text
			}
		}
		args := make([]any, n)
		for i := range cols {
			args[i] = cols[i]
		}
		res, err := tx.Query(ctx, query, args...) // nosemgrep
		if err != nil {
			return nil, fmt.Errorf("query current commit timestamp on %s: %w", nodeName, err)
		}
		for res.Next() {
			var pos int64
			var ts pgtype.Timestamptz
			if err := res.Scan(&pos, &ts); err != nil {
				res.Close()
				return nil, fmt.Errorf("read commit timestamp result: %w", err)
			}
			if pos < 1 || pos > int64(len(batch)) {
				res.Close()
				return nil, fmt.Errorf("commit timestamp result has a bad key position %d", pos)
			}
			if ts.Valid && ts.InfinityModifier == pgtype.Finite {
				results[batch[pos-1]] = ts.Time
			}
		}
		res.Close()
		if err := res.Err(); err != nil {
			return nil, fmt.Errorf("iterate commit timestamp results: %w", err)
		}
	}
	return results, nil
}

func parseCommitTimestamp(raw any) (*time.Time, error) {
	if raw == nil {
		return nil, nil
	}
	switch v := raw.(type) {
	case time.Time:
		return &v, nil
	case string:
		if strings.TrimSpace(v) == "" {
			return nil, nil
		}
		if ts, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return &ts, nil
		}
		if ts, err := time.Parse(time.RFC3339, v); err == nil {
			return &ts, nil
		}
		return nil, fmt.Errorf("parse commit timestamp %q", v)
	case []byte:
		str := string(v)
		return parseCommitTimestamp(str)
	default:
		return nil, fmt.Errorf("unsupported commit timestamp type %T", raw)
	}
}

func formatCommitTimestamp(ts *time.Time) string {
	if ts == nil {
		return ""
	}
	return ts.Format(time.RFC3339Nano)
}

func (t *TableRepairTask) logStaleSkip(entry staleRepairSkipEntry) {
	if t.staleSkipLog == nil {
		t.staleSkipLog = &staleSkipLogger{first: true}
	}
	if err := t.staleSkipLog.write(entry); err != nil {
		logger.Warn("table-repair: failed to write stale skip log: %v", err)
	}
}

func (t *TableRepairTask) closeStaleSkipLog() {
	if t.staleSkipLog == nil {
		return
	}
	if err := t.staleSkipLog.close(); err != nil {
		logger.Warn("table-repair: failed to close stale skip log: %v", err)
	}
}

func (l *staleSkipLogger) write(entry staleRepairSkipEntry) error {
	if err := l.ensureOpen(); err != nil {
		return err
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal stale skip entry: %w", err)
	}
	if l.first {
		l.first = false
	} else {
		if _, err := l.file.WriteString(",\n"); err != nil {
			return fmt.Errorf("write stale skip separator: %w", err)
		}
	}
	if _, err := l.file.Write(data); err != nil {
		return fmt.Errorf("write stale skip entry: %w", err)
	}
	return nil
}

func (l *staleSkipLogger) ensureOpen() error {
	if l.file != nil {
		return nil
	}
	now := time.Now()
	reportDir := filepath.Join("reports", now.Format("2006-01-02"))
	if err := utils.MkdirAllSecure(reportDir); err != nil {
		return fmt.Errorf("create stale skip log directory %s: %w", reportDir, err)
	}
	fileName := fmt.Sprintf("stale_repair_skips_%s.json", now.Format("150405")+fmt.Sprintf(".%03d", now.Nanosecond()/1e6))
	path := filepath.Join(reportDir, fileName)
	file, err := utils.CreateFileSecure(path)
	if err != nil {
		return fmt.Errorf("create stale skip log file %s: %w", path, err)
	}
	if _, err := file.WriteString("[\n"); err != nil {
		file.Close()
		return fmt.Errorf("write stale skip log header: %w", err)
	}
	l.file = file
	l.path = path
	l.first = true
	return nil
}

func (l *staleSkipLogger) close() error {
	if l.file == nil {
		return nil
	}
	if _, err := l.file.WriteString("\n]\n"); err != nil {
		l.file.Close()
		return fmt.Errorf("write stale skip log footer: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close stale skip log file: %w", err)
	}
	logger.Info("Wrote stale repair skip log to %s", l.path)
	l.file = nil
	return nil
}
