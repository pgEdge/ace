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

package mtree

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"runtime"
	"sync"

	"bytes"

	"encoding/json"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/cdc"
	"github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// aceSchema returns the PostgreSQL schema used for merkle-tree objects.
// The value is captured from config on first call so that a mid-task
// SIGHUP reload cannot cause mixed-schema references within one task.
func (m *MerkleTreeTask) aceSchema() string {
	if m.mtreeSchema == "" {
		m.mtreeSchema = config.Get().MTree.Schema
	}
	return m.mtreeSchema
}

const (
	tableAlreadyInPublicationError = "42710"
	TempOffset                     = 1000000
)

type MerkleTreeTask struct {
	types.Task
	types.DerivedFields

	QualifiedTableName string
	DBName             string
	Nodes              string
	ClientRole         string
	InvokeMethod       string

	Analyse           bool
	Rebalance         bool
	RecreateObjects   bool // TBD
	BlockSize         int
	MaxCpuRatio       float64
	BatchSize         int
	Output            string
	QuietMode         bool
	RangesFile        string
	WriteRanges       bool
	OverrideBlockSize bool
	Mode              string
	NoCDC             bool
	CDCTimeoutSec     int // per-invocation CDC drain budget; 0 = use config/default
	// CDCSkippedNodes lists nodes whose CDC drain was skipped because a running `mtree listen` held the replication slot (best-effort mode).
	CDCSkippedNodes []string
	SkipDBUpdate    bool
	Until           string

	untilTime *time.Time

	TaskStore     *taskstore.Store
	TaskStorePath string

	mtreeSchema string

	// MaxDiffRows bounds how many differing rows are collected per node pair.
	// 0 (the default) is resolved from config.MTree.Diff.MaxDiffRows in
	// DiffMtree; a non-positive value after that means unlimited. Without it a
	// massively diverged table would accumulate every differing row in memory
	// and OOM the process.
	MaxDiffRows int64

	DiffResult     types.DiffOutput
	diffMutex      sync.Mutex
	diffRowKeySets map[string]map[string]map[string]struct{}
	// diffRowCounts tracks differing rows collected per node pair for the
	// MaxDiffRows cap; diffLimitWarned makes the "limit reached" warning fire
	// once. Both are guarded by diffMutex.
	diffRowCounts   map[string]int64
	diffLimitWarned bool
	// pairCompareErrs records node pairs whose range comparison lost work items
	// to errors. A zero diff count for such a pair means "not fully compared",
	// not "no differences", so stale-block healing must not act on it. Guarded
	// by diffMutex.
	pairCompareErrs map[string]bool
	StartTime       time.Time
	NodeOriginNames map[string]map[string]string

	Ctx context.Context
}

type CompareRangesWorkItem struct {
	Node1  map[string]any
	Node2  map[string]any
	Ranges [][2][]any
}

type CompareRangesResult struct {
	Diffs     map[string]map[string][]map[string]any
	TotalDiff int
	Err       error
}

func (m *MerkleTreeTask) startLifecycle(taskType string, initialCtx map[string]any) (*taskstore.Recorder, time.Time) {
	start := time.Now()

	if strings.TrimSpace(m.TaskID) == "" {
		m.TaskID = uuid.NewString()
	}
	m.Task.TaskType = taskType
	m.Task.StartedAt = start
	m.Task.TaskStatus = taskstore.StatusRunning
	m.Task.ClusterName = m.ClusterName

	ctx := make(map[string]any)
	maps.Copy(ctx, initialCtx)
	if m.Mode != "" {
		ctx["mode"] = m.Mode
	}
	if m.QualifiedTableName != "" {
		ctx["qualified_table"] = m.QualifiedTableName
	}
	if m.Schema != "" {
		ctx["schema"] = m.Schema
	}
	if m.Table != "" {
		ctx["table"] = m.Table
	}
	if len(m.NodeList) > 0 {
		ctx["nodes"] = m.NodeList
	}

	var recorder *taskstore.Recorder
	if !m.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(m.TaskStore, m.TaskStorePath)
		if recErr != nil {
			logger.Warn("mtree: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if m.TaskStore == nil && rec.Store() != nil {
				m.TaskStore = rec.Store()
			}

			record := taskstore.Record{
				TaskID:      m.TaskID,
				TaskType:    taskType,
				Status:      taskstore.StatusRunning,
				ClusterName: m.ClusterName,
				SchemaName:  m.Schema,
				TableName:   m.Table,
				StartedAt:   start,
				TaskContext: ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("mtree: unable to write initial task status (%v)", err)
			}
		}
	}

	return recorder, start
}

func (m *MerkleTreeTask) finishLifecycle(recorder *taskstore.Recorder, start time.Time, statusErr error, resultCtx map[string]any) {
	finished := time.Now()
	m.Task.FinishedAt = finished
	m.Task.TimeTaken = finished.Sub(start).Seconds()

	status := taskstore.StatusFailed
	if statusErr == nil {
		status = taskstore.StatusCompleted
	}
	m.Task.TaskStatus = status

	if recorder != nil && recorder.Created() {
		ctx := make(map[string]any)
		for k, v := range resultCtx {
			ctx[k] = v
		}
		if statusErr != nil {
			ctx["error"] = statusErr.Error()
		}

		updateErr := recorder.Update(taskstore.Record{
			TaskID:      m.TaskID,
			Status:      status,
			FinishedAt:  finished,
			TimeTaken:   m.Task.TimeTaken,
			TaskContext: ctx,
		})
		if updateErr != nil {
			logger.Warn("mtree: unable to update task status (%v)", updateErr)
		}
	}

	if recorder != nil && recorder.OwnsStore() {
		storePtr := recorder.Store()
		if closeErr := recorder.Close(); closeErr != nil {
			logger.Warn("mtree: failed to close task store (%v)", closeErr)
		}
		if storePtr != nil && m.TaskStore == storePtr {
			m.TaskStore = nil
		}
	}
}

func (m *MerkleTreeTask) CompareRanges(workItems []CompareRangesWorkItem) {
	numWorkers := int(float64(runtime.NumCPU()) * m.MaxCpuRatio)
	if numWorkers < 1 {
		numWorkers = 1
	}
	jobs := make(chan CompareRangesWorkItem, len(workItems))

	p := mpb.New(mpb.WithOutput(os.Stderr))
	bar := p.AddBar(int64(len(workItems)),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Comparing ranges:"),
			decor.CountersNoUnit(" %d / %d"),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(" | "),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
		),
	)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go m.compareRangesWorker(&wg, jobs, bar)
	}

	for _, item := range workItems {
		jobs <- item
	}
	close(jobs)

	wg.Wait()
	p.Wait()
}

func (m *MerkleTreeTask) compareRangesWorker(wg *sync.WaitGroup, jobs <-chan CompareRangesWorkItem, bar *mpb.Bar) {
	defer wg.Done()
	pools := make(map[string]*pgxpool.Pool)
	defer func() {
		for _, pool := range pools {
			pool.Close()
		}
	}()

	for work := range jobs {
		nodePairKey := fmt.Sprintf("%s/%s", work.Node1["Name"], work.Node2["Name"])

		node1Name := work.Node1["Name"].(string)
		pool1, ok := pools[node1Name]
		if !ok {
			var err error
			pool1, err = auth.GetClusterNodeConnection(m.Ctx, work.Node1, m.connOpts())
			if err != nil {
				logger.Error("worker failed to connect to %s: %v", node1Name, err)
				m.recordPairCompareErr(nodePairKey)
				bar.Increment()
				continue
			}
			pools[node1Name] = pool1
		}

		node2Name := work.Node2["Name"].(string)
		pool2, ok := pools[node2Name]
		if !ok {
			var err error
			pool2, err = auth.GetClusterNodeConnection(m.Ctx, work.Node2, m.connOpts())
			if err != nil {
				logger.Error("worker failed to connect to %s: %v", node2Name, err)
				m.recordPairCompareErr(nodePairKey)
				bar.Increment()
				continue
			}
			pools[node2Name] = pool2
		}

		err := m.processWorkItem(work, pool1, pool2)
		if err != nil {
			logger.Error("failed to process work item for %s: %v", nodePairKey, err)
			m.recordPairCompareErr(nodePairKey)
		}
		bar.Increment()
	}
}

// recordPairCompareErr flags a node pair as incompletely compared: at least one
// of its range-comparison work items was lost to an error, so a zero diff
// count for the pair cannot be trusted.
func (m *MerkleTreeTask) recordPairCompareErr(pairKey string) {
	m.diffMutex.Lock()
	defer m.diffMutex.Unlock()
	if m.pairCompareErrs == nil {
		m.pairCompareErrs = make(map[string]bool)
	}
	m.pairCompareErrs[pairKey] = true
}

func (m *MerkleTreeTask) processWorkItem(work CompareRangesWorkItem, pool1, pool2 *pgxpool.Pool) error {
	logger.Debug("Processing work item with %d ranges for %s and %s", len(work.Ranges), work.Node1["Name"], work.Node2["Name"])
	var whereClause string
	var args []any
	paramIndex := 1
	var orClauses []string

	if m.SimplePrimaryKey {
		sanitisedKey := pgx.Identifier{m.Key[0]}.Sanitize()
		for _, r := range work.Ranges {
			var andClauses []string
			var startVal, endVal any
			if len(r[0]) > 0 {
				startVal = r[0][0]
			}
			if len(r[1]) > 0 {
				endVal = r[1][0]
			}

			if startVal != nil {
				andClauses = append(andClauses, fmt.Sprintf("%s >= $%d", sanitisedKey, paramIndex))
				args = append(args, startVal)
				paramIndex++
			}
			if endVal != nil {
				andClauses = append(andClauses, fmt.Sprintf("%s <= $%d", sanitisedKey, paramIndex))
				args = append(args, endVal)
				paramIndex++
			}
			if len(andClauses) > 0 {
				orClauses = append(orClauses, "("+strings.Join(andClauses, " AND ")+")")
			}
		}
	} else {
		sanitisedKeys := make([]string, len(m.Key))
		for i, k := range m.Key {
			sanitisedKeys[i] = pgx.Identifier{k}.Sanitize()
		}
		pkeyColsStr := strings.Join(sanitisedKeys, ", ")

		for _, r := range work.Ranges {
			startVals := r[0]
			endVals := r[1]

			var andClauses []string

			if len(startVals) > 0 && !allNil(startVals) {
				placeholders := make([]string, len(startVals))
				for i, v := range startVals {
					placeholders[i] = fmt.Sprintf("$%d", paramIndex)
					args = append(args, v)
					paramIndex++
				}
				andClauses = append(andClauses, fmt.Sprintf("ROW(%s) >= ROW(%s)", pkeyColsStr, strings.Join(placeholders, ", ")))
			}
			if len(endVals) > 0 && !allNil(endVals) {
				placeholders := make([]string, len(endVals))
				for i, v := range endVals {
					placeholders[i] = fmt.Sprintf("$%d", paramIndex)
					args = append(args, v)
					paramIndex++
				}
				andClauses = append(andClauses, fmt.Sprintf("ROW(%s) <= ROW(%s)", pkeyColsStr, strings.Join(placeholders, ", ")))
			}

			if len(andClauses) > 0 {
				orClauses = append(orClauses, "("+strings.Join(andClauses, " AND ")+")")
			}
		}
	}

	whereClause = strings.Join(orClauses, " OR ")
	if whereClause == "" {
		whereClause = "TRUE"
	}

	if f := queries.CommitTimestampFilter(m.untilTime); f != "" {
		whereClause = fmt.Sprintf("(%s) AND %s", whereClause, f)
	}

	rowHashQuery, orderByStr := buildRowHashQuery(m.Schema, m.Table, m.Key, m.Cols, whereClause, m.ColTypes["_ref"])
	logger.Debug("Row-hash Query: %s, Args: %v", rowHashQuery, args)

	rowsH1, err := pool1.Query(m.Ctx, rowHashQuery, args...) // nosemgrep
	if err != nil {
		return fmt.Errorf("worker failed to get row hashes from %s: %v", work.Node1["Name"], err)
	}
	node1Hashes, err := readRowHashes(rowsH1, len(m.Key))
	if err != nil {
		return fmt.Errorf("failed to read row hashes from %s: %v", work.Node1["Name"], err)
	}

	rowsH2, err := pool2.Query(m.Ctx, rowHashQuery, args...) // nosemgrep
	if err != nil {
		return fmt.Errorf("worker failed to get row hashes from %s: %v", work.Node2["Name"], err)
	}
	node2Hashes, err := readRowHashes(rowsH2, len(m.Key))
	if err != nil {
		return fmt.Errorf("failed to read row hashes from %s: %v", work.Node2["Name"], err)
	}

	mismatchedSimple := make([]any, 0)
	mismatchedComposite := make([][]any, 0)
	isComposite := !m.SimplePrimaryKey

	if isComposite {
		for k, h1 := range node1Hashes {
			if h2, ok := node2Hashes[k]; !ok || h1 != h2 {
				mismatchedComposite = append(mismatchedComposite, splitCompositeKey(k))
			}
		}
		for k, h2 := range node2Hashes {
			if h1, ok := node1Hashes[k]; !ok || h1 != h2 {
				mismatchedComposite = append(mismatchedComposite, splitCompositeKey(k))
			}
		}
	} else {
		for k, h1 := range node1Hashes {
			if h2, ok := node2Hashes[k]; !ok || h1 != h2 {
				mismatchedSimple = append(mismatchedSimple, k)
			}
		}
		for k, h2 := range node2Hashes {
			if h1, ok := node1Hashes[k]; !ok || h1 != h2 {
				mismatchedSimple = append(mismatchedSimple, k)
			}
		}
	}

	if (!isComposite && len(mismatchedSimple) == 0) || (isComposite && len(mismatchedComposite) == 0) {
		return nil
	}

	const fetchBatchSize = 2000
	// Stable per-pair key: getNodePairs emits each pair once in a fixed order,
	// so this is not canonicalised (unlike table_diff.pairKeyFor).
	nodePairKey := fmt.Sprintf("%s/%s", work.Node1["Name"], work.Node2["Name"])

	// Build the row-fetch SELECT list with the same cast policy as the classic
	// engine (SelectColExpr): complex/unknown types arrive as Postgres text
	// instead of opaque driver structs, so the diff report stays repairable.
	refTypes := m.ColTypes["_ref"]
	colExprs := make([]string, 0, len(m.Cols))
	for _, c := range m.Cols {
		colExprs = append(colExprs, utils.SelectColExpr(pgx.Identifier{c}.Sanitize(), refTypes[c]))
	}
	selectCols := "pg_xact_commit_timestamp(xmin) as commit_ts, " +
		"to_json(pg_xact_commit_timestamp_origin(xmin))->>'roident' as node_origin, " +
		strings.Join(colExprs, ", ")

	if isComposite {
		for i := 0; i < len(mismatchedComposite); i += fetchBatchSize {
			if m.pairCapReached(nodePairKey) {
				// The remaining mismatched keys are real diffs we are skipping.
				m.notePairLimitReached(nodePairKey)
				break
			}
			end := i + fetchBatchSize
			if end > len(mismatchedComposite) {
				end = len(mismatchedComposite)
			}
			batch := mismatchedComposite[i:end]

			q, qArgs := buildFetchRowsSQLComposite(m.Schema, m.Table, m.Key, selectCols, orderByStr, batch)

			r1, err := pool1.Query(m.Ctx, q, qArgs...) // nosemgrep
			if err != nil {
				return fmt.Errorf("failed to fetch rows (composite) from %s: %v", work.Node1["Name"], err)
			}
			pr1, err := processRows(r1)
			if err != nil {
				return fmt.Errorf("failed to process rows (composite) from %s: %v", work.Node1["Name"], err)
			}

			r2, err := pool2.Query(m.Ctx, q, qArgs...) // nosemgrep
			if err != nil {
				return fmt.Errorf("failed to fetch rows (composite) from %s: %v", work.Node2["Name"], err)
			}
			pr2, err := processRows(r2)
			if err != nil {
				return fmt.Errorf("failed to process rows (composite) from %s: %v", work.Node2["Name"], err)
			}

			if err := m.appendDiffs(nodePairKey, work, pr1, pr2); err != nil {
				return err
			}
		}
	} else {
		for i := 0; i < len(mismatchedSimple); i += fetchBatchSize {
			if m.pairCapReached(nodePairKey) {
				// The remaining mismatched keys are real diffs we are skipping.
				m.notePairLimitReached(nodePairKey)
				break
			}
			end := i + fetchBatchSize
			if end > len(mismatchedSimple) {
				end = len(mismatchedSimple)
			}
			batch := mismatchedSimple[i:end]

			q, qArgs := buildFetchRowsSQLSimple(m.Schema, m.Table, m.Key[0], selectCols, orderByStr, batch)

			r1, err := pool1.Query(m.Ctx, q, qArgs...) // nosemgrep
			if err != nil {
				return fmt.Errorf("failed to fetch rows from %s: %v", work.Node1["Name"], err)
			}
			pr1, err := processRows(r1)
			if err != nil {
				return fmt.Errorf("failed to process rows from %s: %v", work.Node1["Name"], err)
			}

			r2, err := pool2.Query(m.Ctx, q, qArgs...) // nosemgrep
			if err != nil {
				return fmt.Errorf("failed to fetch rows from %s: %v", work.Node2["Name"], err)
			}
			pr2, err := processRows(r2)
			if err != nil {
				return fmt.Errorf("failed to process rows from %s: %v", work.Node2["Name"], err)
			}

			if err := m.appendDiffs(nodePairKey, work, pr1, pr2); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m *MerkleTreeTask) loadNodeOriginNames() error {
	if m.NodeOriginNames != nil {
		return nil
	}

	m.NodeOriginNames = make(map[string]map[string]string)

	var lastErr error
	for _, nodeInfo := range m.ClusterNodes {
		nodeName, _ := nodeInfo["Name"].(string)
		if nodeName == "" {
			continue
		}
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
		if err != nil {
			lastErr = err
			continue
		}
		names, err := queries.GetNodeOriginNames(m.Ctx, pool)
		pool.Close()
		if err != nil {
			lastErr = err
			continue
		}
		m.NodeOriginNames[nodeName] = names
	}

	if len(m.NodeOriginNames) == 0 && lastErr != nil {
		return lastErr
	}
	return nil
}

// pairLimitReachedLocked reports whether the node pair has already collected
// MaxDiffRows differing rows. The caller must hold diffMutex.
func (m *MerkleTreeTask) pairLimitReachedLocked(pairKey string) bool {
	return m.MaxDiffRows > 0 && m.diffRowCounts[pairKey] >= m.MaxDiffRows
}

// pairCapReached is the lock-safe counterpart of pairLimitReachedLocked, for
// callers (e.g. the fetch loop) that do not already hold diffMutex. It lets a
// worker stop fetching further batches once the pair is full.
func (m *MerkleTreeTask) pairCapReached(pairKey string) bool {
	if m.MaxDiffRows <= 0 {
		return false
	}
	m.diffMutex.Lock()
	defer m.diffMutex.Unlock()
	return m.diffRowCounts[pairKey] >= m.MaxDiffRows
}

// recordPairRowLocked accounts for one collected differing row-pair. The caller
// must hold diffMutex. Reaching MaxDiffRows flags the report truncated, matching
// table-diff: the cap is a report-size bound, so once a pair holds max_diff_rows
// rows we treat any further divergence as possible-but-unenumerated ("additional
// differences may exist"), even when this pair happens to hold exactly the cap.
func (m *MerkleTreeTask) recordPairRowLocked(pairKey string) {
	if m.MaxDiffRows <= 0 {
		return
	}
	if m.diffRowCounts == nil {
		m.diffRowCounts = make(map[string]int64)
	}
	m.diffRowCounts[pairKey]++
	if m.diffRowCounts[pairKey] >= m.MaxDiffRows {
		m.notePairLimitReachedLocked(pairKey)
	}
}

// notePairLimitReachedLocked marks the report truncated and warns once. Call it
// when the pair has reached MaxDiffRows, whether on the row that fills the cap
// or on a later row being dropped. The caller must hold diffMutex.
func (m *MerkleTreeTask) notePairLimitReachedLocked(pairKey string) {
	m.DiffResult.Summary.DiffRowLimitReached = true
	if !m.diffLimitWarned {
		m.diffLimitWarned = true
		logger.Warn("mtree table-diff: %s reached max_diff_rows limit (%d); enumeration for this pair stops (other pairs continue)", pairKey, m.MaxDiffRows)
	}
}

// notePairLimitReached is the lock-safe counterpart of notePairLimitReachedLocked,
// for callers (e.g. the fetch loop) that do not already hold diffMutex.
func (m *MerkleTreeTask) notePairLimitReached(pairKey string) {
	m.diffMutex.Lock()
	defer m.diffMutex.Unlock()
	m.notePairLimitReachedLocked(pairKey)
}

// appendDiffs records the differences between two fetched row batches, stopping
// once the node pair reaches MaxDiffRows.
func (m *MerkleTreeTask) appendDiffs(nodePairKey string, work CompareRangesWorkItem, pr1, pr2 []types.OrderedMap) error {
	diffResult, err := utils.CompareRowSets(pr1, pr2, m.Key, m.Cols)
	if err != nil {
		return fmt.Errorf("worker failed to compare row sets: %v", err)
	}

	m.diffMutex.Lock()
	defer m.diffMutex.Unlock()

	if _, ok := m.DiffResult.NodeDiffs[nodePairKey]; !ok {
		m.DiffResult.NodeDiffs[nodePairKey] = types.DiffByNodePair{
			Rows: make(map[string][]types.OrderedMap),
		}
	}
	node1Name := work.Node1["Name"].(string)
	node2Name := work.Node2["Name"].(string)
	if _, ok := m.DiffResult.NodeDiffs[nodePairKey].Rows[node1Name]; !ok {
		m.DiffResult.NodeDiffs[nodePairKey].Rows[node1Name] = []types.OrderedMap{}
	}
	if _, ok := m.DiffResult.NodeDiffs[nodePairKey].Rows[node2Name]; !ok {
		m.DiffResult.NodeDiffs[nodePairKey].Rows[node2Name] = []types.OrderedMap{}
	}

	// Each loop stops at its top check the moment it is asked to record a row
	// while the pair is already full; recordPairRowLocked flags truncation as
	// soon as a pair reaches the cap, matching table-diff.
	var currentDiffRowsForPair int

	for _, row := range diffResult.Node1OnlyRows {
		if m.pairLimitReachedLocked(nodePairKey) {
			m.notePairLimitReachedLocked(nodePairKey)
			break
		}
		added, err := m.addRowToDiff(nodePairKey, node1Name, row)
		if err != nil {
			return err
		}
		if added {
			currentDiffRowsForPair++
			m.recordPairRowLocked(nodePairKey)
		}
	}
	for _, row := range diffResult.Node2OnlyRows {
		if m.pairLimitReachedLocked(nodePairKey) {
			m.notePairLimitReachedLocked(nodePairKey)
			break
		}
		added, err := m.addRowToDiff(nodePairKey, node2Name, row)
		if err != nil {
			return err
		}
		if added {
			currentDiffRowsForPair++
			m.recordPairRowLocked(nodePairKey)
		}
	}
	for _, modRow := range diffResult.ModifiedRows {
		if m.pairLimitReachedLocked(nodePairKey) {
			m.notePairLimitReachedLocked(nodePairKey)
			break
		}
		added1, err := m.addRowToDiff(nodePairKey, node1Name, modRow.Node1Data)
		if err != nil {
			return err
		}
		added2, err := m.addRowToDiff(nodePairKey, node2Name, modRow.Node2Data)
		if err != nil {
			return err
		}
		if added1 || added2 {
			currentDiffRowsForPair++
			m.recordPairRowLocked(nodePairKey)
		}
	}

	if m.DiffResult.Summary.DiffRowsCount == nil {
		m.DiffResult.Summary.DiffRowsCount = make(map[string]int)
	}
	m.DiffResult.Summary.DiffRowsCount[nodePairKey] += currentDiffRowsForPair
	return nil
}

func (m *MerkleTreeTask) addRowToDiff(nodePairKey, nodeName string, row types.OrderedMap) (bool, error) {
	if m.diffRowKeySets == nil {
		m.diffRowKeySets = make(map[string]map[string]map[string]struct{})
	}

	rowMap := utils.OrderedMapToMap(row)
	rowMap["node_origin"] = utils.TranslateNodeOrigin(rowMap["node_origin"], m.NodeOriginNames[nodeName])
	rowWithMeta := utils.AddSpockMetadata(rowMap)
	orderedRow := utils.MapToOrderedMap(rowWithMeta, m.Cols)

	pairSet, ok := m.diffRowKeySets[nodePairKey]
	if !ok {
		pairSet = make(map[string]map[string]struct{})
		m.diffRowKeySets[nodePairKey] = pairSet
	}

	nodeSet, ok := pairSet[nodeName]
	if !ok {
		nodeSet = make(map[string]struct{})
		pairSet[nodeName] = nodeSet
	}

	key, err := m.buildRowKey(orderedRow)
	if err != nil {
		return false, err
	}

	if _, exists := nodeSet[key]; exists {
		return false, nil
	}

	nodeSet[key] = struct{}{}
	m.DiffResult.NodeDiffs[nodePairKey].Rows[nodeName] = append(m.DiffResult.NodeDiffs[nodePairKey].Rows[nodeName], orderedRow)
	return true, nil
}

func (m *MerkleTreeTask) buildRowKey(row types.OrderedMap) (string, error) {
	values := make([]string, len(m.Key))
	for i, col := range m.Key {
		val, ok := row.Get(col)
		if !ok {
			return "", fmt.Errorf("primary key column %s not found in diff row", col)
		}
		values[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(values, "|"), nil
}

func isNumericColType(colType string) bool {
	lower := strings.ToLower(colType)
	return strings.HasPrefix(lower, "numeric") || strings.HasPrefix(lower, "decimal")
}

func buildRowHashQuery(schema, table string, key []string, cols []string, whereClause string, colTypes map[string]string) (string, string) {
	pkQuoted := make([]string, len(key))
	for i, k := range key {
		pkQuoted[i] = pgx.Identifier{k}.Sanitize()
	}
	colExprs := make([]string, len(cols))
	for i, c := range cols {
		quoted := pgx.Identifier{c}.Sanitize()
		if colTypes != nil && isNumericColType(colTypes[c]) {
			colExprs[i] = fmt.Sprintf("COALESCE(trim_scale(%s)::text, '')", quoted)
		} else {
			colExprs[i] = fmt.Sprintf("COALESCE(%s::text, '')", quoted)
		}
	}
	concatExpr := fmt.Sprintf("concat_ws('|', %s)", strings.Join(colExprs, ", "))

	qualifiedTable := fmt.Sprintf("%s.%s", pgx.Identifier{schema}.Sanitize(), pgx.Identifier{table}.Sanitize())
	orderBy := strings.Join(pkQuoted, ", ")
	selectList := strings.Join(pkQuoted, ", ") + ", encode(digest(" + concatExpr + ",'sha256'),'hex') as row_hash"

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s", selectList, qualifiedTable, whereClause, orderBy)
	return query, orderBy
}

func readRowHashes(rows pgx.Rows, numPK int) (map[string]string, error) {
	defer rows.Close()
	result := make(map[string]string)
	for rows.Next() {
		scan := make([]any, numPK+1)
		scanPtrs := make([]any, numPK+1)
		for i := range scan {
			scanPtrs[i] = &scan[i]
		}
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, err
		}
		parts := make([]string, numPK)
		for i := 0; i < numPK; i++ {
			parts[i] = fmt.Sprintf("%v", scan[i])
		}
		key := strings.Join(parts, "|")
		h, _ := scan[numPK].(string)
		result[key] = h
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func splitCompositeKey(k string) []any {
	if k == "" {
		return []any{}
	}
	parts := strings.Split(k, "|")
	res := make([]any, len(parts))
	for i := range parts {
		res[i] = parts[i]
	}
	return res
}

func buildFetchRowsSQLSimple(schema, table, pk, selectCols, orderBy string, keys []any) (string, []any) {
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys))
	for i := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = keys[i]
	}
	qualifiedTable := fmt.Sprintf("%s.%s", pgx.Identifier{schema}.Sanitize(), pgx.Identifier{table}.Sanitize())
	where := fmt.Sprintf("%s IN (%s)", pgx.Identifier{pk}.Sanitize(), strings.Join(placeholders, ","))
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s", selectCols, qualifiedTable, where, orderBy)
	return q, args
}

func buildFetchRowsSQLComposite(schema, table string, pk []string, selectCols, orderBy string, keys [][]any) (string, []any) {
	tupleCols := make([]string, len(pk))
	for i, k := range pk {
		tupleCols[i] = pgx.Identifier{k}.Sanitize()
	}
	var tuples []string
	var args []any
	argPos := 1
	for _, kv := range keys {
		ph := make([]string, len(kv))
		for j := range kv {
			ph[j] = fmt.Sprintf("$%d", argPos)
			args = append(args, kv[j])
			argPos++
		}
		tuples = append(tuples, fmt.Sprintf("(%s)", strings.Join(ph, ",")))
	}
	qualifiedTable := fmt.Sprintf("%s.%s", pgx.Identifier{schema}.Sanitize(), pgx.Identifier{table}.Sanitize())
	where := fmt.Sprintf("( %s ) IN ( %s )", strings.Join(tupleCols, ","), strings.Join(tuples, ","))
	q := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s", selectCols, qualifiedTable, where, orderBy)
	return q, args
}

func processRows(rows pgx.Rows) ([]types.OrderedMap, error) {
	var results []types.OrderedMap
	defer rows.Close()
	fields := rows.FieldDescriptions()
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}
		rowMap := make(types.OrderedMap, len(fields))
		for i, field := range fields {
			// Normalise driver structs (pgtype.Time, [16]byte UUIDs, ...) to
			// the canonical diff-report representation; raw pgx values would
			// serialise as JSON objects that table-repair cannot convert back.
			rowMap[i] = types.KVPair{Key: string(field.Name), Value: utils.NormalizeScannedValue(values[i])}
		}
		results = append(results, rowMap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func allNil(vals []any) bool {
	if len(vals) == 0 {
		return true
	}
	for _, v := range vals {
		if v != nil {
			return false
		}
	}
	return true
}

func valueOrNil(end []any) interface{} {
	if len(end) == 0 || end[0] == nil {
		return nil
	}
	return end[0]
}

func (m *MerkleTreeTask) MtreeInit() (err error) {
	recorder, start := m.startLifecycle(taskstore.TaskTypeMtreeInit, nil)
	defer func() {
		m.finishLifecycle(recorder, start, err, nil)
	}()

	if err = m.validateInit(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	cfg := config.Get().MTree.CDC

	for _, nodeInfo := range m.ClusterNodes {
		if err := m.initOneNode(nodeInfo, cfg.PublicationName, cfg.SlotName); err != nil {
			return err
		}
	}
	return nil
}

// initOneNode runs MtreeInit on a single node in three phases:
//
//  1. Phase A (tx 1): create schema, helpers, the CDC metadata table,
//     create the publication, and capture the publication's commit LSN.
//     Commit.
//  2. Phase B (no tx): create the logical replication slot via a fresh
//     replication-mode connection. Its consistent point is now
//     guaranteed to be > the publication's commit LSN, fixing the
//     "publication does not exist" replay error.
//  3. Phase C (tx 2): persist slot/start_lsn/pub_commit_lsn into
//     ace_cdc_metadata.
//
// Each iteration runs inside this function so deferred Close/Rollback
// calls fire per-node rather than at MtreeInit return.
func (m *MerkleTreeTask) initOneNode(nodeInfo map[string]any, publicationName, slotName string) error {
	logger.Info("Initialising Merkle tree objects on node: %s", nodeInfo["Name"])

	pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
	if err != nil {
		return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
	}
	defer pool.Close()

	if err := queries.CreateSchema(m.Ctx, pool, m.aceSchema()); err != nil {
		return fmt.Errorf("failed to create schema '%s' on node %s: %w", m.aceSchema(), nodeInfo["Name"], err)
	}

	var pubCommitLSN string
	if err := func() (err error) {
		tx, err := pool.Begin(m.Ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(m.Ctx)

		if err := queries.CreateXORFunction(m.Ctx, tx); err != nil {
			return fmt.Errorf("create xor function: %w", err)
		}
		if err := queries.CreateCDCMetadataTable(m.Ctx, tx); err != nil {
			return fmt.Errorf("create cdc metadata table: %w", err)
		}
		if err := cdc.SetupPublication(m.Ctx, tx, publicationName); err != nil {
			return fmt.Errorf("setup publication: %w", err)
		}
		// Captured mid-tx after CREATE PUBLICATION, so the value is
		// strictly less than this tx's commit LSN; Phase B's slot
		// consistent_point is >= that commit LSN. listen.go's guard
		// therefore catches any start LSN that predates the publication.
		pubCommitLSN, err = queries.CurrentWalInsertLSN(m.Ctx, tx)
		if err != nil {
			return fmt.Errorf("capture publication commit LSN: %w", err)
		}
		return tx.Commit(m.Ctx)
	}(); err != nil {
		return fmt.Errorf("phase A failed on node %s: %w", nodeInfo["Name"], err)
	}

	slotLSN, err := cdc.SetupReplicationSlot(m.Ctx, nodeInfo)
	if err != nil {
		return fmt.Errorf("phase B (replication slot) failed on node %s: %w", nodeInfo["Name"], err)
	}

	if err := func() (err error) {
		tx, err := pool.Begin(m.Ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(m.Ctx)

		if err := queries.InitCDCMetadata(m.Ctx, tx,
			publicationName, slotName,
			slotLSN.String(), pubCommitLSN, []string{}); err != nil {
			return fmt.Errorf("write cdc metadata: %w", err)
		}
		return tx.Commit(m.Ctx)
	}(); err != nil {
		// Slot leak mitigation: Phase B created a slot that we failed to
		// record. A subsequent SetupReplicationSlot will drop any prior
		// slot of the same name, so a re-run reaps it — but try a best
		// effort drop here too to keep the cluster tidy.
		if dropErr := queries.DropReplicationSlot(m.Ctx, pool, slotName); dropErr != nil {
			logger.Warn("failed to drop orphaned slot %s on node %s: %v", slotName, nodeInfo["Name"], dropErr)
		}
		return fmt.Errorf("phase C failed on node %s: %w", nodeInfo["Name"], err)
	}

	logger.Info("Merkle tree objects initialised on node: %s", nodeInfo["Name"])
	return nil
}

func (m *MerkleTreeTask) MtreeTeardown() (err error) {
	recorder, start := m.startLifecycle(taskstore.TaskTypeMtreeTeardown, nil)
	defer func() {
		m.finishLifecycle(recorder, start, err, nil)
	}()

	if err = m.validateInit(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		if err = queries.DropPublication(m.Ctx, pool, "ace_mtree_pub"); err != nil {
			return fmt.Errorf("failed to drop publication: %w", err)
		}
		logger.Info("Publication dropped on node: %s", nodeInfo["Name"])

		if err = queries.DropReplicationSlot(m.Ctx, pool, "ace_mtree_slot"); err != nil {
			return fmt.Errorf("failed to drop replication slot: %w", err)
		}
		logger.Info("Replication slot dropped on node: %s", nodeInfo["Name"])
		if err = queries.DropCDCMetadataTable(m.Ctx, pool); err != nil {
			return fmt.Errorf("failed to drop cdc metadata table: %w", err)
		}
		logger.Info("CDC metadata table dropped on node: %s", nodeInfo["Name"])
	}
	return nil
}

func (m *MerkleTreeTask) MtreeTeardownTable() (err error) {
	recorder, start := m.startLifecycle(taskstore.TaskTypeMtreeTeardownTable, nil)
	defer func() {
		m.finishLifecycle(recorder, start, err, nil)
	}()

	if err = m.validateTeardownTable(); err != nil {
		return fmt.Errorf("validation failed: %w", err)
	}

	cfg := config.Get().MTree.CDC

	for _, nodeInfo := range m.ClusterNodes {
		logger.Info("Tearing down Merkle tree objects for table '%s' on node: %s", m.QualifiedTableName, nodeInfo["Name"])

		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		isSimple, err := queries.GetSimplePrimaryKey(m.Ctx, pool, m.Schema, m.Table)
		if err != nil {
			logger.Warn("could not determine primary key type for table %s on node %s, assuming composite: %v", m.QualifiedTableName, nodeInfo["Name"], err)
		} else {
			m.SimplePrimaryKey = isSimple
		}

		err = queries.AlterPublicationDropTable(m.Ctx, pool, cfg.PublicationName, m.QualifiedTableName)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42704" { // undefined_object
				logger.Warn("Publication %s not found on node %s, continuing...", cfg.PublicationName, nodeInfo["Name"])
			} else {
				logger.Warn("Could not remove table %s from publication %s on node %s: %v", m.QualifiedTableName, cfg.PublicationName, nodeInfo["Name"], err)
			}
		} else {
			logger.Info("Table %s removed from publication %s on node %s", m.QualifiedTableName, cfg.PublicationName, nodeInfo["Name"])
		}

		tx, err := pool.Begin(m.Ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction on node %s: %w", nodeInfo["Name"], err)
		}
		defer tx.Rollback(m.Ctx)

		mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
		mtreeTableName := mtreeTableIdentifier.Sanitize()
		err = queries.DropMtreeTable(m.Ctx, tx, mtreeTableName)
		if err != nil {
			logger.Warn("Could not drop merkle tree table '%s' on node %s (may not exist): %v", mtreeTableName, nodeInfo["Name"], err)
		} else {
			logger.Info("Merkle tree table '%s' dropped on node %s", mtreeTableName, nodeInfo["Name"])
		}

		err = queries.RemoveTableFromCDCMetadata(m.Ctx, tx, m.QualifiedTableName, cfg.PublicationName)
		if err != nil {
			logger.Warn("Could not update CDC metadata on node %s, skipping update: %v", nodeInfo["Name"], err)
		} else {
			logger.Info("CDC metadata updated to remove table %s on node %s", m.QualifiedTableName, nodeInfo["Name"])
		}

		err = queries.DeleteMetadata(m.Ctx, tx, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("failed to delete metadata for table %s on node %s: %w", m.QualifiedTableName, nodeInfo["Name"], err)
		}
		logger.Info("Metadata for table %s deleted on node %s", m.QualifiedTableName, nodeInfo["Name"])

		if !m.SimplePrimaryKey {
			compositeTypeIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
			compositeTypeName := compositeTypeIdentifier.Sanitize()
			err = queries.DropCompositeType(m.Ctx, tx, compositeTypeName)
			if err != nil {
				logger.Warn("Could not drop composite type '%s' on node %s (may not exist): %v", compositeTypeName, nodeInfo["Name"], err)
			} else {
				logger.Info("Composite type '%s' dropped on node %s", compositeTypeName, nodeInfo["Name"])
			}
		}

		if err := tx.Commit(m.Ctx); err != nil {
			return fmt.Errorf("failed to commit transaction on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Merkle tree objects for table '%s' torn down on node: %s", m.QualifiedTableName, nodeInfo["Name"])
	}
	return nil
}

func (m *MerkleTreeTask) validateTeardownTable() error {
	if err := m.validateCommon(); err != nil {
		return err
	}
	if m.QualifiedTableName == "" {
		return fmt.Errorf("table_name is a required argument")
	}
	parts := strings.Split(m.QualifiedTableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("tableName %s must be of form 'schema.table_name'", m.QualifiedTableName)
	}
	schema, table := parts[0], parts[1]

	if err := queries.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := queries.SanitiseIdentifier(table); err != nil {
		return err
	}
	m.Schema = schema
	m.Table = table
	return nil
}

func (m *MerkleTreeTask) GetClusterName() string              { return m.ClusterName }
func (m *MerkleTreeTask) GetDBName() string                   { return m.DBName }
func (m *MerkleTreeTask) SetDBName(name string)               { m.DBName = name }
func (m *MerkleTreeTask) GetNodes() string                    { return m.Nodes }
func (m *MerkleTreeTask) GetNodeList() []string               { return m.NodeList }
func (m *MerkleTreeTask) SetNodeList(nl []string)             { m.NodeList = nl }
func (m *MerkleTreeTask) SetDatabase(db types.Database)       { m.Database = db }
func (m *MerkleTreeTask) GetClusterNodes() []map[string]any   { return m.ClusterNodes }
func (m *MerkleTreeTask) SetClusterNodes(cn []map[string]any) { m.ClusterNodes = cn }

func (m *MerkleTreeTask) GetNode(nodeName string) (map[string]interface{}, error) {
	for _, node := range m.ClusterNodes {
		if name, ok := node["Name"].(string); ok && name == nodeName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("node %s not found in cluster configuration", nodeName)
}

func NewMerkleTreeTask() *MerkleTreeTask {
	return &MerkleTreeTask{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskStatus: taskstore.StatusPending,
		},
		InvokeMethod: "cli",
		DerivedFields: types.DerivedFields{
			ColTypes:  make(map[string]map[string]string),
			PKeyTypes: make(map[string]string),
		},
		Ctx: context.Background(),
	}
}

func (m *MerkleTreeTask) connOpts() auth.ConnectionOptions {
	return auth.ConnectionOptions{}
}

func (m *MerkleTreeTask) userConnOpts() auth.ConnectionOptions {
	return auth.ConnectionOptions{
		Role: m.ClientRole,
	}
}
func (m *MerkleTreeTask) validateInit() error {
	return m.validateCommon()
}

func (m *MerkleTreeTask) validateCommon() error {
	if m.ClusterName == "" {
		return fmt.Errorf("cluster_name is a required argument")
	}

	nodeList, err := utils.ParseNodes(m.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. Error: %w", err)
	}
	m.SetNodeList(nodeList)

	if len(m.GetNodeList()) > 3 {
		return fmt.Errorf("mtree-diff currently supports up to a three-way table comparison")
	}

	err = utils.ReadClusterInfo(m)
	if err != nil {
		return fmt.Errorf("error loading cluster information: %w", err)
	}

	logger.Info("Cluster %s exists", m.ClusterName)

	var clusterNodes []map[string]any
	for _, nodeMap := range m.ClusterNodes {
		if len(nodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !utils.Contains(nodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeMap)
		utils.ApplyDatabaseCredentials(combinedMap, m.Database)
		combinedMap["Host"] = nodeMap["Host"]
		clusterNodes = append(clusterNodes, combinedMap)
	}

	if m.Nodes != "all" && len(nodeList) > 1 {
		for _, n := range nodeList {
			found := false
			for _, node := range clusterNodes {
				if name, ok := node["Name"].(string); ok && name == n {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("specified nodename %s not present in cluster", n)
			}
		}
	} else if len(nodeList) == 0 {
		m.NodeList = []string{}
		for _, node := range clusterNodes {
			m.NodeList = append(m.NodeList, node["Name"].(string))
		}
	}

	m.ClusterNodes = clusterNodes

	return nil
}

func (m *MerkleTreeTask) Validate() error {
	if err := m.validateCommon(); err != nil {
		return err
	}
	if m.Mode == "listen" {
		return nil
	}
	cfg := config.Get().MTree.Diff

	if m.BlockSize != 0 && !m.OverrideBlockSize {
		if m.BlockSize > cfg.MaxBlockSize {
			return fmt.Errorf("block size should be <= %d", cfg.MaxBlockSize)
		}
		if m.BlockSize < cfg.MinBlockSize {
			return fmt.Errorf("block size should be >= %d", cfg.MinBlockSize)
		}
	}

	if m.MaxCpuRatio > 1.0 || m.MaxCpuRatio < 0.0 {
		return fmt.Errorf("invalid value range for max_cpu_ratio")
	}

	if trimmed := strings.TrimSpace(m.Until); trimmed != "" {
		parsed, err := time.Parse(time.RFC3339Nano, trimmed)
		if err != nil {
			return fmt.Errorf("invalid value for --until (expected RFC3339 timestamp): %w", err)
		}
		m.untilTime = &parsed
	} else {
		m.untilTime = nil
	}

	if m.RangesFile != "" {
		if _, err := os.Stat(m.RangesFile); os.IsNotExist(err) {
			return fmt.Errorf("file %s does not exist", m.RangesFile)
		}
		// TODO: Add parsing and validation for ranges file content
	}

	parts := strings.Split(m.QualifiedTableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("tableName %s must be of form 'schema.table_name'", m.QualifiedTableName)
	}
	schema, table := parts[0], parts[1]

	// Sanitise inputs here
	if err := queries.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := queries.SanitiseIdentifier(table); err != nil {
		return err
	}
	m.Schema = schema
	m.Table = table

	// Fast fail if the caller role cannot read the target table on any node.
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.userConnOpts())
		if err != nil {
			return fmt.Errorf("failed to connect to node %s for privilege check: %w", nodeInfo["Name"], err)
		}

		dbUser, _ := nodeInfo["DBUser"].(string)
		if dbUser == "" {
			dbUser = m.Database.DBUser
		}
		privs, err := queries.CheckUserPrivileges(m.Ctx, pool, dbUser, m.Schema, m.Table)
		pool.Close()
		if err != nil {
			return fmt.Errorf("failed to check user privileges on node %s: %w", nodeInfo["Name"], err)
		}
		if !privs.TableSelect {
			return fmt.Errorf("user \"%s\" does not have SELECT privilege on table \"%s.%s\" on node \"%s\"",
				dbUser, m.Schema, m.Table, nodeInfo["Name"])
		}
	}

	return nil
}

func (m *MerkleTreeTask) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := m.Validate(); err != nil {
			return err
		}
	}
	var localCols, localKey []string

	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		if err := queries.EnsurePgcrypto(m.Ctx, pool); err != nil {
			return fmt.Errorf("failed to ensure pgcrypto is installed on %s: %w", nodeInfo["Name"], err)
		}
		tx, err := pool.Begin(m.Ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction for checks on node %s: %w", nodeInfo["Name"], err)
		}
		defer tx.Rollback(m.Ctx)

		currentColsSlice, err := queries.GetColumns(m.Ctx, tx, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("failed to get columns on node %s: %w", nodeInfo["Name"], err)
		}
		currentKeySlice, err := queries.GetPrimaryKey(m.Ctx, tx, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("failed to get primary key on node %s: %w", nodeInfo["Name"], err)
		}

		if len(currentColsSlice) == 0 {
			return fmt.Errorf("table '%s' not found on %s, or the current user does not have adequate privileges", m.QualifiedTableName, nodeInfo["Name"])
		}
		if len(currentKeySlice) == 0 {
			return fmt.Errorf("no primary key found for '%s'", m.QualifiedTableName)
		}

		if localCols == nil && localKey == nil {
			localCols, localKey = currentColsSlice, currentKeySlice
			pkeyTypes, err := queries.GetPkeyColumnTypes(m.Ctx, tx, m.Schema, m.Table, currentKeySlice)
			if err != nil {
				return fmt.Errorf("failed to get pkey column types on node %s: %w", nodeInfo["Name"], err)
			}
			m.PKeyTypes = pkeyTypes

			colTypes, err := queries.GetColumnTypes(m.Ctx, tx, m.Schema, m.Table)
			if err != nil {
				return fmt.Errorf("failed to get column types on node %s: %w", nodeInfo["Name"], err)
			}
			if m.ColTypes == nil {
				m.ColTypes = make(map[string]map[string]string)
			}
			m.ColTypes["_ref"] = colTypes
		}

		if strings.Join(currentColsSlice, ",") != strings.Join(localCols, ",") || strings.Join(currentKeySlice, ",") != strings.Join(localKey, ",") {
			return fmt.Errorf("table schemas or primary keys do not match between nodes")
		}
	}

	m.Cols = localCols
	m.Key = localKey
	m.SimplePrimaryKey = len(m.Key) == 1

	fmt.Println("Connections successful to all nodes in cluster.")
	fmt.Printf("Table %s is comparable across nodes.\n", m.QualifiedTableName)
	return nil
}

func (m *MerkleTreeTask) BuildMtree() (err error) {
	resultCtx := map[string]any{}
	initialCtx := map[string]any{
		"block_size":     m.BlockSize,
		"max_cpu_ratio":  m.MaxCpuRatio,
		"override_block": m.OverrideBlockSize,
		"write_ranges":   m.WriteRanges,
		"ranges_file":    m.RangesFile,
	}
	recorder, start := m.startLifecycle(taskstore.TaskTypeMtreeBuild, initialCtx)
	defer func() {
		m.finishLifecycle(recorder, start, err, resultCtx)
	}()

	var blockRanges []types.BlockRange
	var numBlocks int
	cfg := config.Get().MTree.CDC
	pools := make(map[string]*pgxpool.Pool, len(m.ClusterNodes))

	// Safety net for early returns. The row-estimate loop closes pools
	// itself on its error path, and the per-node build loop closes each
	// node's pool via its inner deferred close, but neither covers
	// pools belonging to nodes the build loop never reaches when an
	// earlier iteration errors out. pgxpool.Pool.Close is idempotent
	// (sync.Once internally), so the per-iteration closes still take
	// effect promptly and this defer is a no-op for pools already closed.
	defer func() {
		for _, p := range pools {
			if p != nil {
				p.Close()
			}
		}
	}()

	numWorkers := int(math.Ceil(float64(runtime.NumCPU()) * m.MaxCpuRatio * 2))
	if numWorkers < 1 {
		numWorkers = 1
	}
	poolSize := numWorkers + 1

	if m.RangesFile != "" {
		logger.Info("Reading block ranges from %s", m.RangesFile)
		data, err := os.ReadFile(m.RangesFile)
		if err != nil {
			return fmt.Errorf("failed to read ranges file: %w", err)
		}
		if err := json.Unmarshal(data, &blockRanges); err != nil {
			return fmt.Errorf("failed to unmarshal ranges file: %w", err)
		}
	}

	logger.Info("Getting row estimates from all nodes...")
	var maxRows int64
	var refNode map[string]any
	successfulEstimates := 0
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetSizedClusterNodeConnection(nodeInfo, auth.ConnectionOptions{PoolSize: poolSize})
		if err != nil {
			for _, p := range pools {
				p.Close()
			}
			return fmt.Errorf("could not connect to node %s to get row estimate: %w", nodeInfo["Name"], err)
		}
		pools[nodeInfo["Name"].(string)] = pool

		count, err := queries.GetRowCountEstimate(m.Ctx, pool, m.Schema, m.Table)
		if err != nil {
			logger.Warn("Warning: Could not get row estimate from node %s: %v", nodeInfo["Name"], err)
			continue
		}
		successfulEstimates++

		// Seed refNode on the first successful estimate so an all-empty table
		// (every count == 0) still resolves a reference node instead of being
		// mistaken for a total estimate failure below.
		if refNode == nil || count > maxRows {
			maxRows = count
			refNode = nodeInfo
		}
	}

	if successfulEstimates == 0 {
		for _, p := range pools {
			p.Close()
		}
		return fmt.Errorf("could not determine a reference node; failed to get row estimates from all nodes")
	}

	if maxRows == 0 {
		for _, p := range pools {
			p.Close()
		}
		return fmt.Errorf("table %s has 0 rows on all nodes; add data before building a Merkle tree", m.QualifiedTableName)
	}
	logger.Info("Using node %s as the reference for defining block ranges.", refNode["Name"])

	if len(blockRanges) == 0 {
		logger.Info("Calculating block ranges for ~%d rows...", maxRows)
		refPool, ok := pools[refNode["Name"].(string)]
		if !ok {
			return fmt.Errorf("could not find reference node %s in pools", refNode["Name"])
		}

		sampleMethod, samplePercent := computeSamplingParameters(maxRows)
		logger.Info("Using %s with sample percent %.2f", sampleMethod, samplePercent)

		numBlocks = int(math.Ceil(float64(maxRows) / float64(m.BlockSize)))
		if numBlocks == 0 && maxRows > 0 {
			numBlocks = 1
		}

		keyColumns := m.Key

		offsetsQuery, err := queries.GeneratePkeyOffsetsQuery(m.Schema, m.Table, keyColumns, sampleMethod, samplePercent, numBlocks, "")
		if err != nil {
			return fmt.Errorf("failed to generate pkey offsets query: %w", err)
		}

		rows, err := refPool.Query(m.Ctx, offsetsQuery)
		if err != nil {
			return fmt.Errorf("failed to execute pkey offsets query on node %s: %w", refNode["Name"], err)
		}
		defer rows.Close()

		numKeyCols := len(keyColumns)
		for i := 0; rows.Next(); i++ {
			dest := make([]any, 2*numKeyCols)
			destPtrs := make([]any, 2*numKeyCols)
			for j := range dest {
				destPtrs[j] = &dest[j]
			}

			if err := rows.Scan(destPtrs...); err != nil {
				return fmt.Errorf("failed to scan pkey offset row: %w", err)
			}

			startVals := make([]any, numKeyCols)
			endVals := make([]any, numKeyCols)

			for k := 0; k < numKeyCols; k++ {
				startVals[k] = dest[k]
				endVals[k] = dest[numKeyCols+k]
			}
			blockRanges = append(blockRanges, types.BlockRange{NodePosition: int64(i), RangeStart: startVals, RangeEnd: endVals})
		}
		if rows.Err() != nil {
			return fmt.Errorf("error iterating over pkey offset rows: %w", rows.Err())
		}
		rows.Close()

		if m.WriteRanges {
			now := time.Now().Format("20060102_150405")
			filename := fmt.Sprintf("%s_%s_%s_ranges.json", now, m.Schema, m.Table)
			data, err := json.MarshalIndent(blockRanges, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal block ranges: %w", err)
			}
			if err := os.WriteFile(filename, data, 0644); err != nil {
				return fmt.Errorf("failed to write block ranges to file: %w", err)
			}
			logger.Info("Block ranges written to %s", filename)
		}
	}

	// Ensure numBlocks matches actual block range count (covers both computed and RangesFile paths).
	if len(blockRanges) > 0 {
		numBlocks = len(blockRanges)
	}

	for _, nodeInfo := range m.ClusterNodes {
		// Per-iteration body in a closure so defer pool.Close and defer
		// tx.Rollback fire at end-of-iteration in the correct LIFO order.
		// Without this scoping, hitting any error inside the body called
		// pool.Close while the tx still held a pooled conn — pool.Close
		// blocks waiting for that conn to be released, but the deferred
		// tx.Rollback that would release it cannot run until the function
		// returns, which cannot happen while pool.Close is blocking. The
		// process then hangs and the underlying error never surfaces.
		if err := func() error {
			logger.Info("Processing node: %s", nodeInfo["Name"])
			pool, ok := pools[nodeInfo["Name"].(string)]
			if !ok {
				return fmt.Errorf("could not find node %s in pools", nodeInfo["Name"])
			}
			defer pool.Close()

			publicationName := cfg.PublicationName
			err := queries.AlterPublicationAddTable(m.Ctx, pool, publicationName, m.QualifiedTableName)
			if err != nil {
				var pgErr *pgconn.PgError
				if errors.As(err, &pgErr) && pgErr.Code == tableAlreadyInPublicationError {
					logger.Info("Table %s is already in publication %s on node %s", m.QualifiedTableName, publicationName, nodeInfo["Name"])
				} else {
					return fmt.Errorf("failed to add table to publication on node %s: %w", nodeInfo["Name"], err)
				}
			} else {
				logger.Info("Added table %s to publication %s on node %s", m.QualifiedTableName, publicationName, nodeInfo["Name"])
			}

			tx, err := pool.Begin(m.Ctx)
			if err != nil {
				return fmt.Errorf("failed to begin transaction on node %s: %w", nodeInfo["Name"], err)
			}
			defer tx.Rollback(m.Ctx)

			slotName, startLSN, tables, _, err := queries.GetCDCMetadata(m.Ctx, tx, publicationName)
			if err != nil {
				return fmt.Errorf("failed to get cdc metadata on node %s: %w", nodeInfo["Name"], err)
			}

			if !slices.Contains(tables, m.QualifiedTableName) {
				tables = append(tables, m.QualifiedTableName)
			}

			err = queries.UpdateCDCMetadata(m.Ctx, tx, publicationName, slotName, startLSN, tables)
			if err != nil {
				return fmt.Errorf("failed to update cdc metadata on node %s: %w", nodeInfo["Name"], err)
			}
			logger.Info("Updated CDC metadata for table %s on node %s", m.QualifiedTableName, nodeInfo["Name"])

			logger.Info("Creating Merkle Tree objects on %s...", nodeInfo["Name"])
			err = m.createMtreeObjects(tx, maxRows, numBlocks)
			if err != nil {
				return fmt.Errorf("failed to create mtree objects on node %s: %w", nodeInfo["Name"], err)
			}

			logger.Info("Inserting block ranges on %s...", nodeInfo["Name"])
			err = m.insertBlockRanges(tx, blockRanges)
			if err != nil {
				return fmt.Errorf("failed to insert block ranges on node %s: %w", nodeInfo["Name"], err)
			}

			logger.Info("Computing leaf hashes on %s...", nodeInfo["Name"])
			err = m.computeLeafHashes(pool, tx, blockRanges, numWorkers, "Computing leaf hashes:")
			if err != nil {
				return fmt.Errorf("failed to compute leaf hashes on node %s: %w", nodeInfo["Name"], err)
			}

			logger.Info("Building parent nodes on %s...", nodeInfo["Name"])
			err = m.buildParentNodes(tx)
			if err != nil {
				return fmt.Errorf("failed to build parent nodes on node %s: %w", nodeInfo["Name"], err)
			}

			logger.Info("Merkle tree built successfully on %s", nodeInfo["Name"])
			if err := tx.Commit(m.Ctx); err != nil {
				return fmt.Errorf("failed to commit transaction on node %s: %w", nodeInfo["Name"], err)
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	resultCtx["max_rows"] = maxRows
	resultCtx["num_blocks"] = numBlocks
	resultCtx["block_ranges_provided"] = len(blockRanges) > 0

	return nil
}

func (m *MerkleTreeTask) UpdateMtree(skipAllChecks bool) (err error) { //nolint:gocyclo // orchestrates per-node CDC drain + tree rehash/rebalance; cyclomatic complexity is inherent to the staged flow
	resultCtx := map[string]any{
		"rebalance":       m.Rebalance,
		"skip_all_checks": skipAllChecks,
	}

	if !skipAllChecks {
		if err = m.RunChecks(true); err != nil {
			return err
		}
	}

	var (
		recorder *taskstore.Recorder
		start    time.Time
	)
	if m.Mode == "update" || m.Mode == "" {
		initialCtx := map[string]any{
			"rebalance":       m.Rebalance,
			"skip_all_checks": skipAllChecks,
		}
		recorder, start = m.startLifecycle(taskstore.TaskTypeMtreeUpdate, initialCtx)
		defer func() {
			m.finishLifecycle(recorder, start, err, resultCtx)
		}()
	}

	// Pre-flight: read the tree's block size from metadata before any CDC
	// work. Block size is fixed at build time, so reading it first is safe —
	// and it doubles as an existence check: a table whose tree was never
	// built fails fast here with an actionable message instead of after a
	// full replication-stream drain.
	var blockSize int
	var foundBlockSize bool
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
		if err != nil {
			return fmt.Errorf("error getting connection pool for node %s: %w", nodeInfo["Name"], err)
		}

		// Ensure hash_version column exists (schema migration for upgrades).
		if err := queries.EnsureHashVersionColumn(m.Ctx, pool); err != nil {
			pool.Close()
			if isMissingTreeErr(err) {
				return m.missingTreeError(nodeInfo["Name"], err)
			}
			return fmt.Errorf("error migrating metadata schema on node %s: %w", nodeInfo["Name"], err)
		}

		blockSize, err = queries.GetBlockSizeFromMetadata(m.Ctx, pool, m.Schema, m.Table)
		if err != nil {
			pool.Close()
			if isMissingTreeErr(err) {
				return m.missingTreeError(nodeInfo["Name"], err)
			}
			return fmt.Errorf("error getting block size from metadata on node %s: %w", nodeInfo["Name"], err)
		}

		pool.Close()
		foundBlockSize = true
	}

	if !foundBlockSize {
		return fmt.Errorf("could not determine block size from any node")
	}
	m.BlockSize = blockSize

	if !m.NoCDC {
		cdcCfg := config.Get().MTree.CDC
		// Wall-clock budget for the whole CDC catch-up (all nodes drained
		// concurrently). Generous by default: a timeout now means "re-run or raise"
		// (progress is durable), not "rebuild", so we favour absorbing large
		// backlogs and busy-server slowdowns over failing fast. A per-invocation
		// --cdc-timeout flag (m.CDCTimeoutSec) overrides this, then config.
		timeout := 300 * time.Second
		if m.CDCTimeoutSec > 0 {
			timeout = time.Duration(m.CDCTimeoutSec) * time.Second
		} else if cdcCfg.CDCProcessingTimeout > 0 {
			timeout = time.Duration(cdcCfg.CDCProcessingTimeout) * time.Second
		}

		baseCtx := m.Ctx
		if baseCtx == nil {
			baseCtx = context.Background()
		}
		ctx, cancel := context.WithTimeout(baseCtx, timeout)
		defer cancel()

		// Drain each node's CDC stream concurrently. Each UpdateFromCDC targets
		// a distinct node over its own connection and writes only that node's
		// CDC metadata, so the calls are independent. Draining sequentially
		// made every mtree update/diff pay the per-node drain latency once per
		// node; running them together collapses that to roughly a single
		// drain's wall-clock. Errors are collected per node and the first
		// (in node order) is returned, preserving the prior semantics.
		var wg sync.WaitGroup
		cdcErrs := make([]error, len(m.ClusterNodes))
		skipped := make([]string, len(m.ClusterNodes))
		for i, nodeInfo := range m.ClusterNodes {
			wg.Add(1)
			go func(i int, nodeInfo map[string]any) {
				defer wg.Done()
				if err := cdc.UpdateFromCDC(ctx, nodeInfo); err != nil {
					name := fmt.Sprintf("%v", nodeInfo["Name"])
					// The slot is held by another active consumer -- typically a
					// running `mtree listen`, but possibly a concurrent bounded mtree
					// operation sharing this node's slot. Skip this node's CDC
					// catch-up (best-effort) and compare against the already-maintained
					// tree rather than failing the whole run.
					if errors.Is(err, cdc.ErrSlotBusy) {
						logger.Warn("node %s: replication slot is held by another consumer "+
							"(expected: 'mtree listen'); skipping this node's CDC drain and comparing "+
							"against the already-maintained tree (best-effort). Recent changes may be "+
							"omitted, so divergence can be under-reported. For a guaranteed-current "+
							"diff, ensure no 'mtree listen' or other mtree operation is holding the "+
							"slot, then re-run. (%v)", name, err)
						skipped[i] = name
						return
					}
					cdcErrs[i] = fmt.Errorf("CDC update failed for node %s: %w", name, err)
				}
			}(i, nodeInfo)
		}
		wg.Wait()
		for _, e := range cdcErrs {
			if e != nil {
				return e
			}
		}
		var skippedNodes []string
		for _, n := range skipped {
			if n != "" {
				skippedNodes = append(skippedNodes, n)
			}
		}
		if len(skippedNodes) > 0 {
			resultCtx["cdc_skipped_nodes"] = skippedNodes
			m.CDCSkippedNodes = skippedNodes
		}
	}

	for _, nodeInfo := range m.ClusterNodes {
		fmt.Printf("\nUpdating Merkle tree on node: %s\n", nodeInfo["Name"])
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, m.connOpts())
		if err != nil {
			return fmt.Errorf("error getting connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		conn, err := pool.Acquire(m.Ctx)
		if err != nil {
			return fmt.Errorf("failed to acquire connection: %w", err)
		}
		defer conn.Release()
		var compositeTypeName string

		if !m.SimplePrimaryKey {
			compositeTypeIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
			compositeTypeName = compositeTypeIdentifier.Sanitize()
			dt, err := conn.Conn().LoadType(m.Ctx, compositeTypeName)
			if err != nil {
				return fmt.Errorf("failed to load composite type %s: %w", compositeTypeName, err)
			}
			conn.Conn().TypeMap().RegisterType(dt)
		}

		tx, err := conn.Begin(m.Ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(m.Ctx)

		mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
		mtreeTableName := mtreeTableIdentifier.Sanitize()

		// Check if stored hashes use an older algorithm and need full recomputation.
		hashVersion, err := queries.GetHashVersion(m.Ctx, tx, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("error getting hash version on node %s: %w", nodeInfo["Name"], err)
		}
		hashVersionUpgraded := false
		if hashVersion < queries.CurrentHashVersion {
			marked, err := queries.MarkAllLeavesDirty(m.Ctx, tx, mtreeTableName)
			if err != nil {
				return fmt.Errorf("error marking all leaves dirty for hash upgrade on node %s: %w", nodeInfo["Name"], err)
			}
			fmt.Printf("Hash algorithm upgraded (v%d -> v%d): marked %d blocks for recomputation on %s\n",
				hashVersion, queries.CurrentHashVersion, marked, nodeInfo["Name"])
			hashVersionUpgraded = true
		}

		blocksToUpdate, err := queries.GetDirtyAndNewBlocks(m.Ctx, tx, mtreeTableName, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return fmt.Errorf("error getting dirty blocks on node %s: %w", nodeInfo["Name"], err)
		}

		if len(blocksToUpdate) == 0 {
			if hashVersionUpgraded {
				// No blocks exist yet, but still update the version marker.
				if err := queries.UpdateHashVersion(m.Ctx, tx, m.Schema, m.Table, queries.CurrentHashVersion); err != nil {
					return fmt.Errorf("error updating hash version on node %s: %w", nodeInfo["Name"], err)
				}
			}
			fmt.Printf("No updates needed for %s\n", nodeInfo["Name"])
			tx.Commit(m.Ctx)
			continue
		}

		splitThreshold := m.BlockSize / 2
		var blockPositionsToSplit []int64
		for _, b := range blocksToUpdate {
			blockPositionsToSplit = append(blockPositionsToSplit, b.NodePosition)
		}

		blocksToSplit, err := queries.FindBlocksToSplit(m.Ctx, tx, mtreeTableName, splitThreshold, blockPositionsToSplit, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTableName, err)
		}

		if len(blocksToSplit) > 0 {
			fmt.Printf("Found %d blocks that may need splitting\n", len(blocksToSplit))
			_, err := m.splitBlocks(tx, blocksToSplit)
			if err != nil {
				return err
			}
		}

		// We must re-sequence after splitting, because splits add blocks at the end,
		// breaking the node_position sequence required by merge logic.
		if len(blocksToSplit) > 0 {
			if err := queries.UpdateAllLeafNodePositionsToTemp(m.Ctx, tx, mtreeTableName, TempOffset); err != nil {
				return fmt.Errorf("failed to move leaf nodes to temporary positions after split: %w", err)
			}
			if err := queries.ResetPositionsByStartFromTemp(m.Ctx, tx, mtreeTableName, TempOffset); err != nil {
				return fmt.Errorf("failed to reset positions after split: %w", err)
			}
		}

		if m.Rebalance {
			if _, err := m.performMerges(tx); err != nil {
				return err
			}
		}

		blocksToUpdate, err = queries.GetDirtyAndNewBlocks(m.Ctx, tx, mtreeTableName, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return err
		}

		if len(blocksToUpdate) == 0 {
			fmt.Printf("No updates needed for %s after rebalancing\n", nodeInfo["Name"])
			tx.Commit(m.Ctx)
			continue
		}
		fmt.Printf("Found %d blocks to update\n", len(blocksToUpdate))

		var affectedPositions []int64
		for _, block := range blocksToUpdate {
			affectedPositions = append(affectedPositions, block.NodePosition)
		}

		if len(affectedPositions) > 0 {
			numWorkers := int(math.Ceil(float64(runtime.NumCPU()) * m.MaxCpuRatio * 2))
			if numWorkers < 1 {
				numWorkers = 1
			}

			if err := m.computeLeafHashes(pool, tx, blocksToUpdate, numWorkers, "Recomputing leaf hashes:"); err != nil {
				return fmt.Errorf("failed to recompute leaf hashes: %w", err)
			}

			fmt.Println("Rebuilding parent nodes")
			if err := m.buildParentNodes(tx); err != nil {
				return err
			}

			fmt.Println("Clearing dirty flags for affected blocks")
			err = queries.ClearDirtyFlags(m.Ctx, tx, mtreeTableName, affectedPositions)
			if err != nil {
				return err
			}
		}

		if hashVersionUpgraded {
			if err := queries.UpdateHashVersion(m.Ctx, tx, m.Schema, m.Table, queries.CurrentHashVersion); err != nil {
				return fmt.Errorf("error updating hash version on node %s: %w", nodeInfo["Name"], err)
			}
		}

		if err := tx.Commit(m.Ctx); err != nil {
			return fmt.Errorf("error committing transaction on node %s: %w", nodeInfo["Name"], err)
		}
		fmt.Printf("Successfully updated %d blocks on %s\n", len(affectedPositions), nodeInfo["Name"])
	}

	resultCtx["block_size"] = blockSize
	resultCtx["nodes_processed"] = len(m.ClusterNodes)

	return nil
}

func (m *MerkleTreeTask) splitBlocks(tx pgx.Tx, blocksToSplit []types.BlockRange) ([]int64, error) {
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()
	isComposite := !m.SimplePrimaryKey
	var modifiedPositions []int64

	compositeTypeIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
	compositeTypeName := compositeTypeIdentifier.Sanitize()

	currentBlocks := make([]types.BlockRange, len(blocksToSplit))
	copy(currentBlocks, blocksToSplit)

	if err := queries.DeleteParentNodes(m.Ctx, tx, mtreeTableName); err != nil {
		return nil, fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	for _, blk := range currentBlocks {
		pos := blk.NodePosition
		start := blk.RangeStart
		end := blk.RangeEnd
		originallyUnbounded := len(end) == 0 || allNil(end)

		if originallyUnbounded {
			var maxVal []any
			var err error
			if isComposite {
				maxVal, err = queries.GetMaxValComposite(m.Ctx, tx, m.Schema, m.Table, m.Key, start)
			} else {
				var simpleMaxVal any
				simpleMaxVal, err = queries.GetMaxValSimple(m.Ctx, tx, m.Schema, m.Table, m.Key[0], start[0])
				if err == nil && simpleMaxVal != nil {
					maxVal = []any{simpleMaxVal}
				}
			}
			if err == nil && maxVal != nil {
				end = maxVal
			}
		}

		count, err := queries.GetBlockRowCount(m.Ctx, tx, m.Schema, m.Table, m.Key, isComposite, start, end)
		if err != nil {
			return nil, fmt.Errorf("failed to get block row count for block %d: %w", pos, err)
		}

		if count <= int64(m.BlockSize) {
			continue
		}

		pkeyType, err := queries.GetPkeyType(m.Ctx, tx, m.Schema, m.Table, m.Key[0])
		if err != nil {
			return nil, fmt.Errorf("failed to get pkey type: %w", err)
		}
		splitPoints, err := queries.GetBulkSplitPoints(m.Ctx, tx, m.Schema, m.Table, m.Key, pkeyType, isComposite, start, end, m.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to get bulk split points for block %d: %w", pos, err)
		}

		if len(splitPoints) > 0 {
			lastSplitPoint := splitPoints[len(splitPoints)-1]
			sliverCount, err := queries.GetBlockRowCount(m.Ctx, tx, m.Schema, m.Table, m.Key, isComposite, lastSplitPoint, end)
			if err != nil {
				return nil, fmt.Errorf("failed to get row count for sliver block: %w", err)
			}

			if sliverCount < int64(float64(m.BlockSize)*0.25) && !originallyUnbounded {
				splitPoints = splitPoints[:len(splitPoints)-1]
			}
		}

		if len(splitPoints) == 0 {
			continue
		}

		for _, sp := range splitPoints {
			if isComposite {
				err = queries.UpdateBlockRangeEndComposite(m.Ctx, tx, mtreeTableName, compositeTypeName, sp, pos)
			} else {
				err = queries.UpdateBlockRangeEnd(m.Ctx, tx, mtreeTableName, sp[0], pos)
			}
			if err != nil {
				return nil, err
			}

			newPos, err := queries.GetMaxNodePosition(m.Ctx, tx, mtreeTableName)
			if err != nil {
				return nil, err
			}
			if isComposite {
				err = queries.InsertCompositeBlockRanges(m.Ctx, tx, mtreeTableName, newPos, sp, nil)
			} else {
				err = queries.InsertBlockRanges(m.Ctx, tx, mtreeTableName, newPos, sp[0], nil)
			}
			if err != nil {
				return nil, err
			}
			modifiedPositions = append(modifiedPositions, newPos)
			pos = newPos
		}

		if originallyUnbounded {
			if isComposite {
				err = queries.UpdateBlockRangeEndComposite(m.Ctx, tx, mtreeTableName, compositeTypeName, nil, pos)
			} else {
				err = queries.UpdateBlockRangeEnd(m.Ctx, tx, mtreeTableName, nil, pos)
			}
		} else {
			if isComposite {
				err = queries.UpdateBlockRangeEndComposite(m.Ctx, tx, mtreeTableName, compositeTypeName, end, pos)
			} else {
				err = queries.UpdateBlockRangeEnd(m.Ctx, tx, mtreeTableName, end[0], pos)
			}
		}
		if err != nil {
			return nil, err
		}
		modifiedPositions = append(modifiedPositions, pos)
	}

	return modifiedPositions, nil
}

func (m *MerkleTreeTask) performMerges(tx pgx.Tx) ([]int64, error) {
	var allModifiedPositions []int64
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	for {
		blocksToMerge, err := queries.FindBlocksToMerge(m.Ctx, tx, mtreeTableName, m.SimplePrimaryKey, m.Schema, m.Table, m.Key, 0.25, []int64{})
		if err != nil {
			return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTableName, err)
		}

		if len(blocksToMerge) == 0 {
			break
		}
		fmt.Printf("Found %d blocks that may need merging\n", len(blocksToMerge))

		modified, err := m.mergeBlocks(tx, blocksToMerge)
		if err != nil {
			return nil, err
		}

		if len(modified) == 0 {
			// No useful merges could be made in this pass, so we're done.
			break
		}
		allModifiedPositions = append(allModifiedPositions, modified...)

		// Reset positions after each pass to ensure pos+1 logic works correctly in the next iteration.
		// Use a temporary offset to avoid unique key violations during re-sequencing.
		if err := queries.UpdateAllLeafNodePositionsToTemp(m.Ctx, tx, mtreeTableName, TempOffset); err != nil {
			return nil, fmt.Errorf("failed to move leaf nodes to temporary positions: %w", err)
		}

		if err := queries.ResetPositionsByStartFromTemp(m.Ctx, tx, mtreeTableName, TempOffset); err != nil {
			return nil, fmt.Errorf("failed to reset positions after merges: %w", err)
		}
	}

	return allModifiedPositions, nil
}

func getNodePairs(nodes []map[string]any) [][2]map[string]any {
	var pairs [][2]map[string]any
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			pairs = append(pairs, [2]map[string]any{nodes[i], nodes[j]})
		}
	}
	return pairs
}

func (m *MerkleTreeTask) DiffMtree() (err error) {
	initialCtx := map[string]any{
		"output":     m.Output,
		"batch_size": m.BatchSize,
	}
	resultCtx := map[string]any{}
	recorder, start := m.startLifecycle(taskstore.TaskTypeMtreeDiff, initialCtx)
	defer func() {
		resultCtx["diff_summary"] = m.DiffResult.Summary
		m.finishLifecycle(recorder, start, err, resultCtx)
	}()

	// Reject a negative cap up front so a bad value can't silently disable the
	// row bound (mirrors table_diff's validation).
	if m.MaxDiffRows < 0 {
		return fmt.Errorf("max_diff_rows must be >= 0, got %d", m.MaxDiffRows)
	}

	if err = m.UpdateMtree(true); err != nil {
		// A missing tree already carries a complete, actionable message;
		// prefixing it with update-failure context only buries the fix.
		if errors.Is(err, ErrMtreeNotFound) {
			return err
		}
		return fmt.Errorf("failed to update merkle tree before diff: %w", err)
	}
	if len(m.CDCSkippedNodes) > 0 {
		resultCtx["cdc_skipped_nodes"] = m.CDCSkippedNodes
	}
	if err := m.loadNodeOriginNames(); err != nil {
		logger.Warn("mtree diff: unable to load node origin names; using raw node_origin values: %v", err)
	}
	nodePairs := getNodePairs(m.ClusterNodes)
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	// pairDiffWork collects, per node pair, the pkey ranges to row-compare plus
	// the mismatched leaf positions and open pools, so blocks that turn out to
	// hold no row differences (stale hashes) can be refreshed afterwards.
	type pairDiffWork struct {
		node1     map[string]any
		node2     map[string]any
		batches   [][2][]any
		positions []int64
		pool1     *pgxpool.Pool
		pool2     *pgxpool.Pool
	}
	allNodePairBatches := make(map[string]*pairDiffWork)

	m.StartTime = time.Now()
	// Resolve the differing-row cap from the mtree config when the caller left
	// it unset, mirroring how every other mtree tunable is sourced from the
	// mtree section. Without a bound a heavily diverged table collects every
	// differing row in memory and OOMs the process.
	if m.MaxDiffRows == 0 {
		if cfg := config.Get(); cfg != nil && cfg.MTree.Diff.MaxDiffRows > 0 {
			m.MaxDiffRows = cfg.MTree.Diff.MaxDiffRows
		}
	}
	m.diffRowCounts = make(map[string]int64)
	m.diffLimitWarned = false
	m.pairCompareErrs = make(map[string]bool)
	m.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Schema:          m.Schema,
			Table:           m.Table,
			Nodes:           m.NodeList,
			MaxDiffRows:     m.MaxDiffRows,
			StartTime:       time.Now().Format(time.RFC3339),
			DiffRowsCount:   make(map[string]int),
			CDCSkippedNodes: m.CDCSkippedNodes,
		},
	}
	m.diffRowKeySets = make(map[string]map[string]map[string]struct{})

	for _, pair := range nodePairs {
		node1 := pair[0]
		node2 := pair[1]
		logger.Info("Comparing merkle trees between %s and %s", node1["Name"], node2["Name"])

		pool1, err := auth.GetClusterNodeConnection(m.Ctx, node1, m.connOpts())
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", node1["Name"], err)
		}
		defer pool1.Close()

		pool2, err := auth.GetClusterNodeConnection(m.Ctx, node2, m.connOpts())
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", node2["Name"], err)
		}
		defer pool2.Close()

		root1, err := queries.GetRootNode(m.Ctx, pool1, mtreeTableName)
		if err != nil {
			return fmt.Errorf("failed to get root node on %s: %w", node1["Name"], err)
		}

		root2, err := queries.GetRootNode(m.Ctx, pool2, mtreeTableName)
		if err != nil {
			return fmt.Errorf("failed to get root node on %s: %w", node2["Name"], err)
		}

		if root1 == nil || root2 == nil {
			logger.Warn("Merkle tree not found on one or both nodes for table %s.%s", m.Schema, m.Table)
			continue
		}

		if bytes.Equal(root1.NodeHash, root2.NodeHash) {
			logger.Info("Merkle trees are identical")
			continue
		}

		logger.Info("Trees differ - traversing to find mismatched leaf nodes...")

		rootLevel, err := queries.GetMaxNodeLevel(m.Ctx, pool1, mtreeTableName)
		if err != nil {
			return fmt.Errorf("failed to get max node level on %s: %w", node1["Name"], err)
		}

		mismatchedLeaves, err := m.findMismatchedLeaves(pool1, pool2, rootLevel, root1.NodePosition)
		if err != nil {
			return fmt.Errorf("failed to find mismatched leaves: %w", err)
		}

		if len(mismatchedLeaves) == 0 {
			logger.Info("No mismatched leaf nodes found")
			continue
		}
		positions := make([]int64, 0, len(mismatchedLeaves))
		for pos := range mismatchedLeaves {
			positions = append(positions, pos)
		}

		batches, err := m.getPkeyBatches(pool1, pool2, positions)
		if err != nil {
			return fmt.Errorf("failed to get pkey batches: %w", err)
		}
		logger.Info("Found %d mismatched blocks", len(batches))
		nodePairKey := fmt.Sprintf("%s/%s", node1["Name"], node2["Name"])
		entry, exists := allNodePairBatches[nodePairKey]
		if !exists {
			entry = &pairDiffWork{node1: node1, node2: node2, pool1: pool1, pool2: pool2}
			allNodePairBatches[nodePairKey] = entry
		}
		entry.batches = append(entry.batches, batches...)
		entry.positions = append(entry.positions, positions...)

	}

	const rangesPerWorkItem = 1
	var workItems []CompareRangesWorkItem
	for _, entry := range allNodePairBatches {
		for i := 0; i < len(entry.batches); i += rangesPerWorkItem {
			end := min(i+rangesPerWorkItem, len(entry.batches))
			workItems = append(workItems, CompareRangesWorkItem{
				Node1:  entry.node1,
				Node2:  entry.node2,
				Ranges: entry.batches[i:end],
			})
		}
	}

	if len(workItems) > 0 {
		m.CompareRanges(workItems)

		// Mismatched blocks whose row comparison found no differences had stale
		// tree hashes, not data divergence (typically rows applied by
		// replication that a past drain missed). Say so -- a bare "Found N
		// mismatched blocks" followed by "TABLES MATCH" reads as a
		// contradiction -- and rehash those leaves from live data so the
		// phantom mismatch does not recur on every subsequent diff. A zero diff
		// count only proves staleness when every work item for the pair was
		// actually compared and no row filter narrowed the comparison, so pairs
		// with lost work items -- and any run under --until, whose comparison
		// deliberately excludes rows past the cutoff -- are left alone.
		for pairKey, entry := range allNodePairBatches {
			if len(entry.positions) == 0 || m.DiffResult.Summary.DiffRowsCount[pairKey] > 0 {
				continue
			}
			if m.pairCompareErrs[pairKey] {
				logger.Warn("comparison between %s and %s was incomplete (worker errors); "+
					"skipping stale-block refresh -- re-run the diff",
					entry.node1["Name"], entry.node2["Name"])
				continue
			}
			if m.untilTime != nil {
				continue
			}
			logger.Info("The mismatched block(s) between %s and %s contained no row differences; "+
				"the tree hashes were stale. Refreshing them from live data",
				entry.node1["Name"], entry.node2["Name"])
			healed := true
			for _, side := range []struct {
				pool *pgxpool.Pool
				name any
			}{{entry.pool1, entry.node1["Name"]}, {entry.pool2, entry.node2["Name"]}} {
				if err := m.refreshStaleLeaves(side.pool, entry.positions); err != nil {
					healed = false
					logger.Warn("failed to refresh stale blocks on %s (the diff result is unaffected; "+
						"the mismatch may reappear on the next diff): %v", side.name, err)
				}
			}
			if healed {
				if m.DiffResult.Summary.StaleBlocksRefreshed == nil {
					m.DiffResult.Summary.StaleBlocksRefreshed = make(map[string]int)
				}
				m.DiffResult.Summary.StaleBlocksRefreshed[pairKey] = len(entry.positions)
				r1, err1 := queries.GetRootNode(m.Ctx, entry.pool1, mtreeTableName)
				r2, err2 := queries.GetRootNode(m.Ctx, entry.pool2, mtreeTableName)
				if err1 == nil && err2 == nil && r1 != nil && r2 != nil && !bytes.Equal(r1.NodeHash, r2.NodeHash) {
					logger.Warn("trees between %s and %s still differ after refreshing stale blocks; "+
						"their leaf ranges have likely diverged -- run 'ace mtree build' to realign them",
						entry.node1["Name"], entry.node2["Name"])
				}
			}
		}

		endTime := time.Now()
		m.DiffResult.Summary.EndTime = endTime.Format(time.RFC3339)
		m.DiffResult.Summary.TimeTaken = endTime.Sub(m.StartTime).String()
		m.DiffResult.Summary.PrimaryKey = m.Key
		if m.DiffResult.Summary.DiffRowLimitReached {
			logger.Warn("mtree table-diff stopped after reaching max_diff_rows=%d; additional differences may exist", m.MaxDiffRows)
		}
		if diffPath, _, writeErr := utils.WriteDiffReport(m.DiffResult, m.Schema, m.Table, m.Output); writeErr != nil {
			return writeErr
		} else if diffPath != "" {
			resultCtx["diff_file"] = diffPath
		}
	}

	resultCtx["mismatched_pairs"] = len(m.DiffResult.NodeDiffs)

	if len(m.CDCSkippedNodes) > 0 {
		logger.Warn("diff completed in best-effort mode: the CDC drain was skipped for "+
			"node(s) %v because the replication slot was held by another consumer "+
			"(expected: 'mtree listen'). Recent changes on those node(s) may not be "+
			"reflected, so divergence can be under-reported. Re-run with no concurrent "+
			"'mtree listen' or other mtree operation holding the slot for a "+
			"guaranteed-current diff.", m.CDCSkippedNodes)
	}

	return nil
}

// refreshStaleLeaves rehashes exactly the given leaf blocks from live table
// data and rebuilds the parent chain, in one transaction on one node. Used
// when a diff resolves a leaf-hash mismatch as a false positive: the data
// matches, so only the stored hash is behind, and without a refresh the
// phantom mismatch would recur on every diff until a full rebuild. Blocks
// dirtied concurrently by CDC keep their dirty state and counters for the
// next update.
func (m *MerkleTreeTask) refreshStaleLeaves(pool *pgxpool.Pool, positions []int64) error { //nolint:gocyclo // linear mark->rehash->rebuild->clear pipeline; the count is per-step error returns, not branching logic
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	tx, err := pool.Begin(m.Ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(m.Ctx)

	if err := queries.MarkLeavesDirtyByPositions(m.Ctx, tx, mtreeTableName, positions); err != nil {
		return err
	}
	// Reuse the update pipeline: fetch the (now dirty) block ranges, rehash
	// them from the live table, rebuild parents, clear the flags -- but only
	// for the requested positions. GetDirtyAndNewBlocks also returns blocks a
	// concurrent CDC consumer dirtied; rehash-and-clear on those would zero
	// their counters without split evaluation and could clear a dirty flag for
	// a change committed after the hash read. Leaving them dirty hands them to
	// the next update, which owns that logic.
	blocks, err := queries.GetDirtyAndNewBlocks(m.Ctx, tx, mtreeTableName, m.SimplePrimaryKey, m.Key)
	if err != nil {
		return err
	}
	requested := make(map[int64]struct{}, len(positions))
	for _, p := range positions {
		requested[p] = struct{}{}
	}
	kept := blocks[:0]
	for _, b := range blocks {
		if _, ok := requested[b.NodePosition]; ok {
			kept = append(kept, b)
		}
	}
	blocks = kept
	if len(blocks) == 0 {
		return tx.Commit(m.Ctx)
	}
	numWorkers := int(float64(runtime.NumCPU()) * m.MaxCpuRatio)
	if numWorkers < 1 {
		numWorkers = 1
	}
	if err := m.computeLeafHashes(pool, tx, blocks, numWorkers, "Refreshing stale blocks:"); err != nil {
		return err
	}
	if err := m.buildParentNodes(tx); err != nil {
		return err
	}
	affected := make([]int64, 0, len(blocks))
	for _, b := range blocks {
		affected = append(affected, b.NodePosition)
	}
	if err := queries.ClearDirtyFlags(m.Ctx, tx, mtreeTableName, affected); err != nil {
		return err
	}
	return tx.Commit(m.Ctx)
}

func (m *MerkleTreeTask) findMismatchedLeaves(pool1, pool2 *pgxpool.Pool, parentLevel int, parentPosition int64) (map[int64]bool, error) {
	mismatched := make(map[int64]bool)
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	children1, err := queries.GetNodeChildren(m.Ctx, pool1, mtreeTableName, parentLevel, int(parentPosition))
	if err != nil {
		return nil, err
	}
	children2, err := queries.GetNodeChildren(m.Ctx, pool2, mtreeTableName, parentLevel, int(parentPosition))
	if err != nil {
		return nil, err
	}

	maxLen := max(len(children1), len(children2))

	for i := range maxLen {
		var child1, child2 *types.NodeChild
		if i < len(children1) {
			child1 = &children1[i]
		}
		if i < len(children2) {
			child2 = &children2[i]
		}

		if child1 == nil || child2 == nil {
			existingChild := child1
			if existingChild == nil {
				existingChild = child2
			}
			if existingChild.NodeLevel == 0 {
				mismatched[existingChild.NodePosition] = true
			} else {
				// To handle structural differences, we must recurse using the node that has children.
				// But we need to decide which pool to use to continue traversal.
				// We pass both pools down and let the recursive call figure it out.
				childMismatches, err := m.findMismatchedLeaves(pool1, pool2, existingChild.NodeLevel, existingChild.NodePosition)
				if err != nil {
					return nil, err
				}
				for pos := range childMismatches {
					mismatched[pos] = true
				}
			}
		} else if !bytes.Equal(child1.NodeHash, child2.NodeHash) {
			if child1.NodeLevel == 0 {
				mismatched[child1.NodePosition] = true
			} else {
				childMismatches, err := m.findMismatchedLeaves(pool1, pool2, child1.NodeLevel, child1.NodePosition)
				if err != nil {
					return nil, err
				}
				for pos := range childMismatches {
					mismatched[pos] = true
				}
			}
		}
	}

	return mismatched, nil
}

func (m *MerkleTreeTask) getPkeyBatches(pool1, pool2 *pgxpool.Pool, mismatchedPositions []int64) ([][2][]any, error) {
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	leafRanges1, err := queries.GetLeafRanges(m.Ctx, pool1, mtreeTableName, mismatchedPositions, m.SimplePrimaryKey, m.Key)
	if err != nil {
		return nil, err
	}
	leafRanges2, err := queries.GetLeafRanges(m.Ctx, pool2, mtreeTableName, mismatchedPositions, m.SimplePrimaryKey, m.Key)
	if err != nil {
		return nil, err
	}

	allRanges := append(leafRanges1, leafRanges2...)

	// GeneratePkeyOffsetsQuery always emits a last leaf with range_end =
	// NULL, so the boundary set must track open sides explicitly —
	// otherwise rows past the reference's last_row are never queried.
	// GetLeafRanges returns NULL bounds as []any{nil} (simple PK) or
	// []any{nil, nil, ...} (composite PK), not as a Go nil slice — use
	// allNil to normalise.
	boundaries := []any{}
	hasOpenStart, hasOpenEnd := false, false
	for _, r := range allRanges {
		if allNil(r.RangeStart) {
			hasOpenStart = true
		} else {
			boundaries = append(boundaries, r.RangeStart)
		}
		if allNil(r.RangeEnd) {
			hasOpenEnd = true
		} else {
			boundaries = append(boundaries, r.RangeEnd)
		}
	}

	uniqueBoundaries := make(map[string]any)
	for _, b := range boundaries {
		key := fmt.Sprint(b)
		uniqueBoundaries[key] = b
	}

	sortedBoundaries := make([]any, 0, len(uniqueBoundaries))
	for _, b := range uniqueBoundaries {
		sortedBoundaries = append(sortedBoundaries, b)
	}

	sort.Slice(sortedBoundaries, func(i, j int) bool {
		return m.compareBoundaries(sortedBoundaries[i], sortedBoundaries[j]) < 0
	})

	var slices [][2]any
	switch len(sortedBoundaries) {
	case 0:
		if hasOpenStart || hasOpenEnd {
			slices = append(slices, [2]any{nil, nil})
		}
	case 1:
		// Skip the single-point slice when an open-side slice below
		// already subsumes it.
		if !hasOpenStart && !hasOpenEnd {
			s := sortedBoundaries[0]
			slices = append(slices, [2]any{s, s})
		}
	default:
		for i := 0; i < len(sortedBoundaries)-1; i++ {
			s := sortedBoundaries[i]
			e := sortedBoundaries[i+1]
			if m.intervalInUnion(s, e, allRanges) {
				slices = append(slices, [2]any{s, e})
			}
		}
	}

	if len(sortedBoundaries) > 0 {
		if hasOpenStart {
			slices = append(slices, [2]any{nil, sortedBoundaries[0]})
		}
		if hasOpenEnd {
			slices = append(slices, [2]any{sortedBoundaries[len(sortedBoundaries)-1], nil})
		}
	}

	if len(slices) == 0 {
		return [][2][]any{}, nil
	}

	var batches [][2][]any
	for _, slice := range slices {
		start := boundaryToSlice(slice[0])
		end := boundaryToSlice(slice[1])
		batches = append(batches, [2][]any{start, end})
	}

	return batches, nil
}

func (m *MerkleTreeTask) compareBoundaries(b1, b2 any) int {
	if b1 == nil && b2 == nil {
		return 0
	}
	if b1 == nil {
		return -1
	}
	if b2 == nil {
		return 1
	}

	b1Slice := boundaryToSlice(b1)
	b2Slice := boundaryToSlice(b2)

	if b1Slice == nil && b2Slice == nil {
		return 0
	}
	if b1Slice == nil {
		return -1
	}
	if b2Slice == nil {
		return 1
	}

	for k := range m.Key {
		if k >= len(b1Slice) {
			return -1
		}
		if k >= len(b2Slice) {
			return 1
		}
		val1 := b1Slice[k]
		val2 := b2Slice[k]

		if val1 == nil && val2 == nil {
			continue
		}
		if val1 == nil {
			return -1
		}
		if val2 == nil {
			return 1
		}

		switch v1 := val1.(type) {
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			v1Int := reflect.ValueOf(v1).Int()
			v2Int := reflect.ValueOf(val2).Int()
			if v1Int < v2Int {
				return -1
			}
			if v1Int > v2Int {
				return 1
			}
		case float32, float64:
			v1Float := reflect.ValueOf(v1).Float()
			v2Float := reflect.ValueOf(val2).Float()
			if v1Float < v2Float {
				return -1
			}
			if v1Float > v2Float {
				return 1
			}
		case string:
			if v1 < val2.(string) {
				return -1
			}
			if v1 > val2.(string) {
				return 1
			}
		case time.Time:
			if v1.Before(val2.(time.Time)) {
				return -1
			}
			if v1.After(val2.(time.Time)) {
				return 1
			}
		default:
			s1 := fmt.Sprintf("%v", val1)
			s2 := fmt.Sprintf("%v", val2)
			if s1 < s2 {
				return -1
			}
			if s1 > s2 {
				return 1
			}
		}
	}
	if len(b1Slice) < len(b2Slice) {
		return -1
	}
	if len(b1Slice) > len(b2Slice) {
		return 1
	}
	return 0
}

func (m *MerkleTreeTask) intervalInUnion(s, e any, intervals []types.LeafRange) bool {
	for _, interval := range intervals {
		start := interval.RangeStart
		end := interval.RangeEnd

		eBeforeOrEqualStart := false
		if start != nil {
			eBeforeOrEqualStart = m.compareBoundaries(e, start) <= 0
		}

		sAfterOrEqualEnd := false
		if end != nil {
			sAfterOrEqualEnd = m.compareBoundaries(s, end) >= 0
		}

		if !eBeforeOrEqualStart && !sAfterOrEqualEnd {
			return true
		}
	}
	return false
}

func boundaryToSlice(b any) []any {
	if b == nil {
		return nil
	}
	if slice, ok := b.([]any); ok {
		return slice
	}
	return []any{b}
}

func (m *MerkleTreeTask) mergeBlocks(tx pgx.Tx, blocksToMerge []types.BlockRange) ([]int64, error) {
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()
	isComposite := !m.SimplePrimaryKey
	var modifiedPositions []int64

	compositeTypeIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
	compositeTypeName := compositeTypeIdentifier.Sanitize()

	if err := queries.DeleteParentNodes(m.Ctx, tx, mtreeTableName); err != nil {
		return nil, fmt.Errorf("failed to delete parent nodes: %w", err)
	}

	mergedPositions := make(map[int64]bool)

	for _, blk := range blocksToMerge {
		pos := blk.NodePosition

		if mergedPositions[pos] {
			continue
		}

		currentBlock, err := queries.GetBlockWithCount(m.Ctx, tx, mtreeTableName, m.Schema, m.Table, m.Key, isComposite, pos)
		if err != nil {
			return nil, fmt.Errorf("failed to get current block %d with count: %w", pos, err)
		}
		if currentBlock == nil {
			continue
		}

		// Attempt to merge with the next block
		nextBlock, err := queries.GetBlockWithCount(m.Ctx, tx, mtreeTableName, m.Schema, m.Table, m.Key, isComposite, pos+1)
		if err != nil {
			return nil, fmt.Errorf("failed to get next block for %d: %w", pos, err)
		}

		if nextBlock != nil && (currentBlock.Count+nextBlock.Count < int64(float64(m.BlockSize)*1.5)) {
			if isComposite {
				err = queries.UpdateBlockRangeEndComposite(m.Ctx, tx, mtreeTableName, compositeTypeName, nextBlock.RangeEnd, currentBlock.NodePosition)
			} else {
				err = queries.UpdateBlockRangeEnd(m.Ctx, tx, mtreeTableName, valueOrNil(nextBlock.RangeEnd), currentBlock.NodePosition)
			}
			if err != nil {
				return nil, err
			}

			if err := queries.DeleteBlock(m.Ctx, tx, mtreeTableName, nextBlock.NodePosition); err != nil {
				return nil, err
			}
			modifiedPositions = append(modifiedPositions, currentBlock.NodePosition)
			mergedPositions[nextBlock.NodePosition] = true
		}
	}
	return modifiedPositions, nil
}

func (m *MerkleTreeTask) buildParentNodes(conn queries.DBQuerier) error {
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	var err error
	if tx, ok := conn.(pgx.Tx); ok {
		err = queries.DeleteParentNodes(m.Ctx, tx, mtreeTableName)
	} else if pool, ok := conn.(*pgxpool.Pool); ok {
		err = queries.DeleteParentNodes(m.Ctx, pool, mtreeTableName)
	} else {
		return fmt.Errorf("unsupported connection type for DeleteParentNodes")
	}

	if err != nil {
		return err
	}

	level := 0
	for {
		var count int
		var buildErr error
		if tx, ok := conn.(pgx.Tx); ok {
			count, buildErr = queries.BuildParentNodes(m.Ctx, tx, mtreeTableName, level)
		} else if pool, ok := conn.(*pgxpool.Pool); ok {
			count, buildErr = queries.BuildParentNodes(m.Ctx, pool, mtreeTableName, level)
		} else {
			return fmt.Errorf("unsupported connection type for BuildParentNodes")
		}

		if buildErr != nil {
			return fmt.Errorf("failed to build parent nodes at level %d: %w", level, buildErr)
		}
		if count <= 1 {
			break
		}
		level++
	}

	return nil
}

type LeafHashResult struct {
	BlockID int64
	Hash    []byte
	Err     error
}

func (m *MerkleTreeTask) computeLeafHashes(pool *pgxpool.Pool, tx pgx.Tx, ranges []types.BlockRange, numWorkers int, barMessage string) error {
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	jobs := make(chan types.BlockRange, len(ranges))
	results := make(chan LeafHashResult, len(ranges))

	p := mpb.New(mpb.WithOutput(os.Stderr))
	bar := p.AddBar(int64(len(ranges)),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name(barMessage, decor.WC{W: 25}),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(" | "),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
		),
	)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go m.leafHashWorker(&wg, jobs, results, pool, bar)
	}

	for _, r := range ranges {
		jobs <- r
	}
	close(jobs)

	wg.Wait()
	close(results)

	p.Wait()

	leafHashes := make(map[int64][]byte)
	for result := range results {
		if result.Err != nil {
			return result.Err
		}
		leafHashes[result.BlockID] = result.Hash
	}

	if err := queries.UpdateLeafHashesBatch(m.Ctx, tx, mtreeTableName, leafHashes); err != nil {
		return fmt.Errorf("failed to update leaf hashes in batch: %w", err)
	}

	return nil
}

func (m *MerkleTreeTask) leafHashWorker(wg *sync.WaitGroup, jobs <-chan types.BlockRange, results chan<- LeafHashResult, pool *pgxpool.Pool, bar *mpb.Bar) {
	defer wg.Done()

	// Use reference column types for hash computation (schemas are validated to match)
	refColTypes := m.ColTypes["_ref"]

	for block := range jobs {
		leafHash, err := queries.ComputeLeafHashes(m.Ctx, pool, m.Schema, m.Table, m.SimplePrimaryKey, m.Key, block.RangeStart, block.RangeEnd, m.Cols, refColTypes)
		if err != nil {
			results <- LeafHashResult{BlockID: block.NodePosition, Err: fmt.Errorf("failed to compute hash for block %d: %w", block.NodePosition, err)}
			bar.Increment()
			continue
		}
		results <- LeafHashResult{BlockID: block.NodePosition, Hash: leafHash}
		bar.Increment()
	}
}

func (m *MerkleTreeTask) insertBlockRanges(conn queries.DBQuerier, ranges []types.BlockRange) error {
	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}

	if m.SimplePrimaryKey {
		if err := queries.InsertBlockRangesBatchSimple(m.Ctx, conn, mtreeTableIdentifier.Sanitize(), ranges); err != nil {
			return err
		}
	} else {
		if err := queries.InsertBlockRangesBatchComposite(m.Ctx, conn, mtreeTableIdentifier.Sanitize(), ranges, len(m.Key)); err != nil {
			return err
		}
	}

	return nil
}

func (m *MerkleTreeTask) createMtreeObjects(tx pgx.Tx, totalRows int64, numBlocks int) error {

	// Schema and the bytea_xor function are created by MtreeInit's Phase A
	// and are required for BuildMtree to make sense (build needs the CDC
	// publication and metadata that init sets up — if init ran, schema and
	// XOR function exist). Recreating them here is redundant, and on Spock
	// clusters CREATE OR REPLACE FUNCTION unconditionally writes to
	// pg_proc, so it races with Spock's DDL apply of the matching
	// replicated DDL from n1's own build — producing SQLSTATE XX000
	// "tuple concurrently updated". The CREATE TABLE IF NOT EXISTS below
	// is race-safe because IF NOT EXISTS short-circuits when the relation
	// already exists.
	err := queries.CreateMetadataTable(m.Ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}

	err = queries.UpdateMetadata(m.Ctx, tx, m.Schema, m.Table, totalRows, m.BlockSize, numBlocks, !m.SimplePrimaryKey, queries.CurrentHashVersion)
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	mtreeTableIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()
	err = queries.DropMtreeTable(m.Ctx, tx, mtreeTableName)
	if err != nil {
		return fmt.Errorf("failed to render drop mtree table sql: %w", err)
	}

	if m.SimplePrimaryKey {
		pkeyType, err := queries.GetPkeyType(m.Ctx, tx, m.Schema, m.Table, m.Key[0])
		if err != nil {
			return err
		}
		err = queries.CreateSimpleMtreeTable(m.Ctx, tx, mtreeTableName, pkeyType)
		if err != nil {
			return fmt.Errorf("failed to render create simple mtree table sql: %w", err)
		}
	} else {
		keyTypeColumns := make([]string, len(m.Key))
		for i, col := range m.Key {
			colType, err := queries.GetPkeyType(m.Ctx, tx, m.Schema, m.Table, col)
			if err != nil {
				return err
			}
			keyTypeColumns[i] = fmt.Sprintf("%s %s", pgx.Identifier{col}.Sanitize(), colType)
		}

		compositeTypeIdentifier := pgx.Identifier{m.aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
		compositeTypeName := compositeTypeIdentifier.Sanitize()

		err = queries.DropCompositeType(m.Ctx, tx, compositeTypeName)
		if err != nil {
			return fmt.Errorf("failed to render drop composite type sql: %w", err)
		}

		err = queries.CreateCompositeType(m.Ctx, tx, compositeTypeName, strings.Join(keyTypeColumns, ", "))
		if err != nil {
			return fmt.Errorf("failed to render create composite type sql: %w", err)
		}

		err = queries.CreateCompositeMtreeTable(m.Ctx, tx, mtreeTableName, compositeTypeName)
		if err != nil {
			return fmt.Errorf("failed to render create composite mtree table sql: %w", err)
		}
	}
	err = m.buildParentNodes(tx)
	if err != nil {
		return err
	}

	return nil
}

func computeSamplingParameters(rowCount int64) (string, float64) {
	sampleMethod := "BERNOULLI"
	samplePercent := 100.0

	if rowCount <= 10000 {
		return sampleMethod, samplePercent
	}
	if rowCount <= 100000 {
		samplePercent = 10
	} else if rowCount <= 1000000 {
		samplePercent = 1
	} else if rowCount <= 100000000 {
		sampleMethod = "SYSTEM"
		samplePercent = 0.1
	} else {
		sampleMethod = "SYSTEM"
		samplePercent = 0.01
	}
	return sampleMethod, samplePercent
}
