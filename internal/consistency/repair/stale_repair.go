// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
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
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
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

func fetchCommitTimestamps(ctx context.Context, tx pgx.Tx, task *TableRepairTask, nodeName string, colTypes map[string]string, rows map[string]map[string]any) (map[string]time.Time, error) {
	results := make(map[string]time.Time)
	if len(rows) == 0 {
		return results, nil
	}

	if colTypes == nil {
		if derived, _, err := task.getColTypesForNode(nodeName); err == nil {
			colTypes = derived
		}
	}

	tableIdent := pgx.Identifier{task.Schema, task.Table}.Sanitize()
	batchSize := 1000

	if task.SimplePrimaryKey {
		pkCol := task.Key[0]
		pkIdent := pgx.Identifier{pkCol}.Sanitize()
		var pkType string
		if colTypes != nil {
			pkType = colTypes[pkCol]
		}

		keys := make([]any, 0, len(rows))
		for _, row := range rows {
			val, ok := row[pkCol]
			if !ok {
				return nil, fmt.Errorf("primary key column %s not found in row data", pkCol)
			}
			if pkType != "" {
				converted, err := convertValueForType(val, pkType)
				if err != nil {
					return nil, fmt.Errorf("convert primary key %s value: %w", pkCol, err)
				}
				val = converted
			}
			keys = append(keys, val)
		}

		for i := 0; i < len(keys); i += batchSize {
			end := i + batchSize
			if end > len(keys) {
				end = len(keys)
			}
			batchKeys := keys[i:end]

			var sb strings.Builder
			args := make([]any, 0, len(batchKeys))
			paramIdx := 1
			sb.WriteString("SELECT ")
			sb.WriteString(pkIdent)
			sb.WriteString(", pg_xact_commit_timestamp(xmin) as commit_ts FROM ")
			sb.WriteString(tableIdent)
			sb.WriteString(" WHERE ")
			sb.WriteString(pkIdent)
			sb.WriteString(" IN (")
			for j, key := range batchKeys {
				if j > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("$%d", paramIdx))
				args = append(args, key)
				paramIdx++
			}
			sb.WriteString(")")

			rows, err := tx.Query(ctx, sb.String(), args...)
			if err != nil {
				return nil, fmt.Errorf("query current commit timestamp on %s: %w", nodeName, err)
			}
			for rows.Next() {
				values, err := rows.Values()
				if err != nil {
					rows.Close()
					return nil, fmt.Errorf("read commit timestamp result: %w", err)
				}
				if len(values) < 2 {
					rows.Close()
					return nil, fmt.Errorf("unexpected commit timestamp result size: %d", len(values))
				}
				pkMap := map[string]any{pkCol: values[0]}
				pkStr, err := utils.StringifyKey(pkMap, task.Key)
				if err != nil {
					rows.Close()
					return nil, fmt.Errorf("stringify primary key: %w", err)
				}
				commitTS, err := parseCommitTimestamp(values[1])
				if err != nil {
					rows.Close()
					return nil, err
				}
				if commitTS != nil {
					results[pkStr] = *commitTS
				}
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return nil, fmt.Errorf("iterate commit timestamp results: %w", err)
			}
			rows.Close()
		}

		return results, nil
	}

	colIdents := make([]string, len(task.Key))
	for i, col := range task.Key {
		colIdents[i] = pgx.Identifier{col}.Sanitize()
	}
	pkSQL := strings.Join(colIdents, ", ")

	keys := make([][]any, 0, len(rows))
	for _, row := range rows {
		tuple := make([]any, len(task.Key))
		for i, col := range task.Key {
			val, ok := row[col]
			if !ok {
				return nil, fmt.Errorf("composite primary key column %s not found in row data", col)
			}
			if colTypes != nil {
				if pkType := colTypes[col]; pkType != "" {
					converted, err := convertValueForType(val, pkType)
					if err != nil {
						return nil, fmt.Errorf("convert primary key %s value: %w", col, err)
					}
					val = converted
				}
			}
			tuple[i] = val
		}
		keys = append(keys, tuple)
	}

	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batchKeys := keys[i:end]

		var sb strings.Builder
		args := make([]any, 0, len(batchKeys)*len(task.Key))
		paramIdx := 1
		sb.WriteString("SELECT ")
		sb.WriteString(pkSQL)
		sb.WriteString(", pg_xact_commit_timestamp(xmin) as commit_ts FROM ")
		sb.WriteString(tableIdent)
		sb.WriteString(" WHERE (")
		sb.WriteString(pkSQL)
		sb.WriteString(") IN (")
		for j, key := range batchKeys {
			if j > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(")
			for k, val := range key {
				if k > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString(fmt.Sprintf("$%d", paramIdx))
				args = append(args, val)
				paramIdx++
			}
			sb.WriteString(")")
		}
		sb.WriteString(")")

		rows, err := tx.Query(ctx, sb.String(), args...)
		if err != nil {
			return nil, fmt.Errorf("query current commit timestamp on %s: %w", nodeName, err)
		}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("read commit timestamp result: %w", err)
			}
			if len(values) < len(task.Key)+1 {
				rows.Close()
				return nil, fmt.Errorf("unexpected commit timestamp result size: %d", len(values))
			}
			pkMap := make(map[string]any, len(task.Key))
			for idx, col := range task.Key {
				pkMap[col] = values[idx]
			}
			pkStr, err := utils.StringifyKey(pkMap, task.Key)
			if err != nil {
				rows.Close()
				return nil, fmt.Errorf("stringify primary key: %w", err)
			}
			commitTS, err := parseCommitTimestamp(values[len(task.Key)])
			if err != nil {
				rows.Close()
				return nil, err
			}
			if commitTS != nil {
				results[pkStr] = *commitTS
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, fmt.Errorf("iterate commit timestamp results: %w", err)
		}
		rows.Close()
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
	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return fmt.Errorf("create stale skip log directory %s: %w", reportDir, err)
	}
	fileName := fmt.Sprintf("stale_repair_skips_%s.json", now.Format("150405")+fmt.Sprintf(".%03d", now.Nanosecond()/1e6))
	path := filepath.Join(reportDir, fileName)
	file, err := os.Create(path)
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
