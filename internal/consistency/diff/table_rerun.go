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
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/keydiff"
	auth "github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
)

// ExecuteRerunTask checks the differences of an earlier diff again. It
// reads the key texts of the entries from the result directory of the diff,
// reads the rows of these keys again on the nodes of each pair, compares
// them by row hash, and writes the rows that still differ as a new result
// directory in the same format. The rows are read with the filter that the
// manifest of the diff gives, so the rerun sees the rows that the diff saw:
// a row outside the filter on one node is still missing there. DiffFilePath is the result directory of the
// earlier diff, or the JSON report next to it; after the run it is the JSON
// report of the new result, or empty if no difference is left.
func (t *TableDiffTask) ExecuteRerunTask() error {
	startTime := time.Now()

	stored, err := CheckDiffFileFormat(t.DiffFilePath, t)
	if err != nil {
		return err
	}
	logger.Info("Successfully loaded and validated diff result: %s", stored.Dir)

	if err := utils.ReadClusterInfo(t); err != nil {
		return fmt.Errorf("error loading cluster information for rerun: %w", err)
	}

	var clusterNodes []map[string]any
	for _, nodeInfo := range t.ClusterNodes {
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeInfo)
		utils.ApplyDatabaseCredentials(combinedMap, t.Database)
		clusterNodes = append(clusterNodes, combinedMap)
	}
	t.ClusterNodes = clusterNodes

	keyOfDiff := slices.Clone(t.Key)
	if err := t.RunChecks(true); err != nil {
		return fmt.Errorf("pre-run checks failed for rerun: %w", err)
	}
	// The entries name rows by the key of the diff, so the table must still
	// have this key.
	if !slices.Equal(t.Key, keyOfDiff) {
		return fmt.Errorf("diff result %s was made with the primary key (%s), but %s has the primary key (%s)",
			stored.Dir, strings.Join(keyOfDiff, ", "), t.QualifiedTableName, strings.Join(t.Key, ", "))
	}

	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		if !utils.Contains(t.NodeList, name) {
			continue
		}
		pool, err := auth.GetClusterNodeConnection(ctx, nodeInfo, t.connOpts())
		if err != nil {
			for _, p := range pools {
				p.Close()
			}
			return fmt.Errorf("failed to connect to node %s: %w", name, err)
		}
		pools[name] = pool
	}
	t.Pools = pools
	defer func() {
		for _, p := range t.Pools {
			p.Close()
		}
	}()

	if err := t.loadNodeOriginNames(); err != nil {
		logger.Warn("table-diff rerun: unable to load node origin names; using raw node_origin values: %v", err)
	}

	tbl := keydiff.Table{Schema: t.Schema, Name: t.Table, Key: t.Key, Cols: t.Cols}
	for _, p := range pools {
		if tbl.ScanChildren, err = keydiff.HasInheritanceChildren(ctx, p, t.Schema, t.Table); err != nil {
			return err
		}
		break
	}

	summary := stored.Manifest.Summary
	summary.DiffRowsCount = make(map[string]int)
	summary.TotalRowsChecked = 0
	summary.MismatchedRangesCount = 0
	t.DiffResult = types.DiffOutput{FormatVersion: types.DiffFormatVersion, Summary: summary}
	if t.BaseTable == "" {
		t.BaseTable = t.Table
	}

	t.result = utils.NewDiffResultWriter(t.Schema, t.BaseTable, t.Key)
	defer t.result.Abort()

	checked := 0
	for _, mp := range stored.Manifest.Pairs {
		n, err := t.rerunPair(ctx, tbl, stored, mp)
		if err != nil {
			return err
		}
		checked += n
	}
	logger.Info("Checked %d rows with differences in the original result again.", checked)

	endTime := time.Now()
	t.DiffResult.Summary.EndTime = endTime.Format(time.RFC3339)
	t.DiffResult.Summary.TimeTaken = endTime.Sub(startTime).String()

	t.DiffFilePath = ""
	t.DiffResultDir = ""
	paths, err := t.result.Finish(ctx, t.resultManifest(), utils.DiffReportOptions{
		Format:             t.Output,
		MaxHTMLRows:        t.MaxHTMLRows,
		MaxHTMLValueLength: t.MaxHTMLValueLength,
	})
	if err != nil {
		return err
	}
	if paths.JSON == "" {
		logger.Info("%s All previously reported differences have been resolved.", utils.CheckMark)
		return nil
	}
	t.DiffFilePath = paths.JSON
	t.DiffResultDir = paths.Dir
	return nil
}

// rerunPair checks the entries of one pair again, one block file at a time,
// and writes the rows that still differ into a block file with the same
// number. It returns the number of entries it checked. The row read applies
// the effective filter of the manifest: the table filter, and the conditions
// on the origin and the commit timestamp of the rows.
func (t *TableDiffTask) rerunPair(ctx context.Context, tbl keydiff.Table, stored *utils.StoredDiff, mp utils.DiffResultPair) (int, error) {
	side := func(node string) (keydiff.Side, error) {
		pool, ok := t.Pools[node]
		if !ok {
			return keydiff.Side{}, fmt.Errorf("no connection to node %s of the pair %s", node, mp.Pair)
		}
		castTypes, err := t.nodeCastTypes(node)
		if err != nil {
			return keydiff.Side{}, err
		}
		return keydiff.Side{Name: node, Conn: pool, CastTypes: castTypes}, nil
	}
	s1, err := side(mp.Node1)
	if err != nil {
		return 0, err
	}
	s2, err := side(mp.Node2)
	if err != nil {
		return 0, err
	}

	files, err := stored.BlockFiles(mp)
	if err != nil {
		return 0, err
	}
	checked := 0
	for seq, file := range files {
		var keys [][]string
		// A key can have more than one entry: an inheritance tree can hold
		// a row with the same key in the parent and in a child. The row
		// read gives all rows of a key, so the key is read once. The
		// entries of one key are next to each other in a block file, so it
		// is enough to compare a key with the key before it.
		// XXX: Remove this machinery when the tables of an inheritance tree
		// are checked separately.
		var last []string
		err := utils.ReadDiffBlock(file, func(e *utils.DiffEntryLine) error {
			raw := e.Row1
			if len(raw) == 0 {
				raw = e.Row2
			}
			var row types.OrderedMap
			if err := row.UnmarshalJSON(raw); err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
			key := make([]string, len(t.Key))
			for i, col := range t.Key {
				v, _ := row.Get(col)
				s, ok := v.(string)
				if !ok {
					return fmt.Errorf("%s: the key column %s of an entry is not text", file, col)
				}
				key[i] = s
			}
			if slices.Equal(key, last) {
				return nil
			}
			last = key
			keys = append(keys, key)
			return nil
		})
		if err != nil {
			return 0, err
		}
		checked += len(keys)

		out := t.result.OpenBlock(mp.Pair, mp.Node1, mp.Node2, 0, seq)
		res, err := keydiff.CompareKeys(ctx, tbl, stored.Manifest.Summary.EffectiveFilter, s1, s2, keys, keydiff.Options{},
			keydiff.BlockSink(out, t.entryRow(mp.Node1), t.entryRow(mp.Node2)))
		if cerr := out.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return 0, fmt.Errorf("check the rows of %s again: %w", mp.Pair, err)
		}
		if n := res.Count(); n > 0 {
			t.DiffResult.Summary.DiffRowsCount[mp.Pair] += n
		}
	}
	return checked, nil
}

// CheckDiffFileFormat opens the result directory of an earlier diff and
// takes the table, the nodes, the key and the settings of the run from its
// manifest. It refuses a result of another row format version.
func CheckDiffFileFormat(filePath string, task *TableDiffTask) (*utils.StoredDiff, error) {
	if filePath == "" {
		return nil, fmt.Errorf("a diff file path must be provided for rerun mode via --rerun-from")
	}
	stored, err := utils.OpenDiffResult(filePath)
	if err != nil {
		return nil, err
	}
	m := stored.Manifest
	if err := types.CheckFormatVersion(m.RowFormatVersion); err != nil {
		return nil, fmt.Errorf("diff result %s: %w", stored.Dir, err)
	}

	summary := m.Summary
	if summary.Schema == "" || summary.Table == "" || len(summary.Nodes) == 0 || len(m.PrimaryKey) == 0 {
		return nil, fmt.Errorf("diff result %s is missing essential summary information (schema, table, nodes, primaryKey)", stored.Dir)
	}

	task.Schema = summary.Schema
	task.Table = summary.Table
	task.QualifiedTableName = fmt.Sprintf("%s.%s", task.Schema, task.Table)
	task.NodeList = summary.Nodes
	task.Key = m.PrimaryKey
	task.SimplePrimaryKey = len(task.Key) == 1

	// Inherit settings from the original run
	task.BlockSize = summary.BlockSize
	task.ConcurrencyFactor = summary.ConcurrencyFactor
	task.CompareUnitSize = summary.CompareUnitSize

	return stored, nil
}
