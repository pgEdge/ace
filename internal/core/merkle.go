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

package core

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
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/internal/cdc"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

func aceSchema() string {
	return config.Cfg.MTree.Schema
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
	SkipDBUpdate      bool

	TaskStore     *taskstore.Store
	TaskStorePath string

	DiffResult     types.DiffOutput
	diffMutex      sync.Mutex
	diffRowKeySets map[string]map[string]map[string]struct{}
	StartTime      time.Time

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
		node1Name := work.Node1["Name"].(string)
		pool1, ok := pools[node1Name]
		if !ok {
			var err error
			pool1, err = auth.GetClusterNodeConnection(m.Ctx, work.Node1, auth.ConnectionOptions{})
			if err != nil {
				logger.Error("worker failed to connect to %s: %v", node1Name, err)
				bar.Increment()
				continue
			}
			pools[node1Name] = pool1
		}

		node2Name := work.Node2["Name"].(string)
		pool2, ok := pools[node2Name]
		if !ok {
			var err error
			pool2, err = auth.GetClusterNodeConnection(m.Ctx, work.Node2, auth.ConnectionOptions{})
			if err != nil {
				logger.Error("worker failed to connect to %s: %v", node2Name, err)
				bar.Increment()
				continue
			}
			pools[node2Name] = pool2
		}

		err := m.processWorkItem(work, pool1, pool2)
		if err != nil {
			nodePairKey := fmt.Sprintf("%s/%s", work.Node1["Name"], work.Node2["Name"])
			logger.Error("failed to process work item for %s: %v", nodePairKey, err)
		}
		bar.Increment()
	}
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

	rowHashQuery, orderByStr := buildRowHashQuery(m.QualifiedTableName, m.Key, m.Cols, whereClause)
	logger.Debug("Row-hash Query: %s, Args: %v", rowHashQuery, args)

	rowsH1, err := pool1.Query(m.Ctx, rowHashQuery, args...)
	if err != nil {
		return fmt.Errorf("worker failed to get row hashes from %s: %v", work.Node1["Name"], err)
	}
	node1Hashes, err := readRowHashes(rowsH1, len(m.Key))
	if err != nil {
		return fmt.Errorf("failed to read row hashes from %s: %v", work.Node1["Name"], err)
	}

	rowsH2, err := pool2.Query(m.Ctx, rowHashQuery, args...)
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
	nodePairKey := fmt.Sprintf("%s/%s", work.Node1["Name"], work.Node2["Name"])

	if isComposite {
		for i := 0; i < len(mismatchedComposite); i += fetchBatchSize {
			end := i + fetchBatchSize
			if end > len(mismatchedComposite) {
				end = len(mismatchedComposite)
			}
			batch := mismatchedComposite[i:end]

			q, qArgs := buildFetchRowsSQLComposite(m.QualifiedTableName, m.Key, orderByStr, batch)

			r1, err := pool1.Query(m.Ctx, q, qArgs...)
			if err != nil {
				return fmt.Errorf("failed to fetch rows (composite) from %s: %v", work.Node1["Name"], err)
			}
			pr1, err := processRows(r1)
			if err != nil {
				return fmt.Errorf("failed to process rows (composite) from %s: %v", work.Node1["Name"], err)
			}

			r2, err := pool2.Query(m.Ctx, q, qArgs...)
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
			end := i + fetchBatchSize
			if end > len(mismatchedSimple) {
				end = len(mismatchedSimple)
			}
			batch := mismatchedSimple[i:end]

			q, qArgs := buildFetchRowsSQLSimple(m.QualifiedTableName, m.Key[0], orderByStr, batch)

			r1, err := pool1.Query(m.Ctx, q, qArgs...)
			if err != nil {
				return fmt.Errorf("failed to fetch rows from %s: %v", work.Node1["Name"], err)
			}
			pr1, err := processRows(r1)
			if err != nil {
				return fmt.Errorf("failed to process rows from %s: %v", work.Node1["Name"], err)
			}

			r2, err := pool2.Query(m.Ctx, q, qArgs...)
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

	var currentDiffRowsForPair int
	for _, row := range diffResult.Node1OnlyRows {
		added, err := m.addRowToDiff(nodePairKey, node1Name, row)
		if err != nil {
			return err
		}
		if added {
			currentDiffRowsForPair++
		}
	}
	for _, row := range diffResult.Node2OnlyRows {
		added, err := m.addRowToDiff(nodePairKey, node2Name, row)
		if err != nil {
			return err
		}
		if added {
			currentDiffRowsForPair++
		}
	}
	for _, modRow := range diffResult.ModifiedRows {
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

	key, err := m.buildRowKey(row)
	if err != nil {
		return false, err
	}

	if _, exists := nodeSet[key]; exists {
		return false, nil
	}

	nodeSet[key] = struct{}{}
	m.DiffResult.NodeDiffs[nodePairKey].Rows[nodeName] = append(m.DiffResult.NodeDiffs[nodePairKey].Rows[nodeName], row)
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

func buildRowHashQuery(tableName string, key []string, cols []string, whereClause string) (string, string) {
	pkQuoted := make([]string, len(key))
	for i, k := range key {
		pkQuoted[i] = pgx.Identifier{k}.Sanitize()
	}
	colQuoted := make([]string, len(cols))
	for i, c := range cols {
		colQuoted[i] = pgx.Identifier{c}.Sanitize()
	}

	for i := range colQuoted {
		colQuoted[i] = fmt.Sprintf("COALESCE(%s::text, '')", colQuoted[i])
	}
	concatExpr := fmt.Sprintf("concat_ws('|', %s)", strings.Join(colQuoted, ", "))

	orderBy := strings.Join(pkQuoted, ", ")
	selectList := strings.Join(pkQuoted, ", ") + ", encode(digest(" + concatExpr + ",'sha256'),'hex') as row_hash"

	query := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s", selectList, tableName, whereClause, orderBy)
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

func buildFetchRowsSQLSimple(tableName, pk string, orderBy string, keys []any) (string, []any) {
	placeholders := make([]string, len(keys))
	args := make([]any, len(keys))
	for i := range keys {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = keys[i]
	}
	where := fmt.Sprintf("%s IN (%s)", pgx.Identifier{pk}.Sanitize(), strings.Join(placeholders, ","))
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s ORDER BY %s", tableName, where, orderBy)
	return q, args
}

func buildFetchRowsSQLComposite(tableName string, pk []string, orderBy string, keys [][]any) (string, []any) {
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
	where := fmt.Sprintf("( %s ) IN ( %s )", strings.Join(tupleCols, ","), strings.Join(tuples, ","))
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s ORDER BY %s", tableName, where, orderBy)
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
			rowMap[i] = types.KVPair{Key: string(field.Name), Value: values[i]}
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

	cfg := config.Cfg.MTree.CDC

	for _, nodeInfo := range m.ClusterNodes {
		logger.Info("Initialising Merkle tree objects on node: %s", nodeInfo["Name"])

		lsn, err := cdc.SetupReplicationSlot(m.Ctx, nodeInfo)
		if err != nil {
			return fmt.Errorf("failed to set up replication slot on node %s: %w", nodeInfo["Name"], err)
		}

		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, auth.ConnectionOptions{})
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		if err = queries.CreateSchema(m.Ctx, pool, aceSchema()); err != nil {
			return fmt.Errorf("failed to create schema '%s': %w", aceSchema(), err)
		}

		tx, err := pool.Begin(m.Ctx)
		if err != nil {
			return fmt.Errorf("failed to begin transaction on node %s: %w", nodeInfo["Name"], err)
		}
		defer tx.Rollback(m.Ctx)

		if err = queries.CreateXORFunction(m.Ctx, tx); err != nil {
			return fmt.Errorf("failed to create xor function: %w", err)
		}

		if err = queries.CreateCDCMetadataTable(m.Ctx, tx); err != nil {
			return fmt.Errorf("failed to create cdc metadata table: %w", err)
		}

		if err := cdc.SetupPublication(m.Ctx, tx, cfg.PublicationName); err != nil {
			return fmt.Errorf("failed to setup publication on node %s: %w", nodeInfo["Name"], err)
		}

		if err = queries.UpdateCDCMetadata(m.Ctx, tx, cfg.PublicationName, cfg.SlotName, lsn.String(), []string{}); err != nil {
			return fmt.Errorf("failed to update cdc metadata: %w", err)
		}

		if err := tx.Commit(m.Ctx); err != nil {
			return fmt.Errorf("failed to commit transaction on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Merkle tree objects initialised on node: %s", nodeInfo["Name"])
	}
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
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, auth.ConnectionOptions{})
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

	cfg := config.Cfg.MTree.CDC

	for _, nodeInfo := range m.ClusterNodes {
		logger.Info("Tearing down Merkle tree objects for table '%s' on node: %s", m.QualifiedTableName, nodeInfo["Name"])

		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, auth.ConnectionOptions{})
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

		mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
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
			compositeTypeIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
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
		DerivedFields: types.DerivedFields{
			ColTypes:  make(map[string]map[string]string),
			PKeyTypes: make(map[string]string),
		},
		Ctx: context.Background(),
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
	cfg := config.Cfg.MTree.Diff

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
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, auth.ConnectionOptions{})
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", nodeInfo["Name"], err)
		}
		defer pool.Close()

		if _, err := pool.Exec(m.Ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto;"); err != nil {
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
	cfg := config.Cfg.MTree.CDC
	pools := make(map[string]*pgxpool.Pool, len(m.ClusterNodes))

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

		if count > maxRows {
			maxRows = count
			refNode = nodeInfo
		}
	}

	if refNode == nil {
		return fmt.Errorf("could not determine a reference node; failed to get row estimates from all nodes")
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

		offsetsQuery, err := queries.GeneratePkeyOffsetsQuery(m.Schema, m.Table, keyColumns, sampleMethod, samplePercent, numBlocks)
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

	for _, nodeInfo := range m.ClusterNodes {
		logger.Info("Processing node: %s", nodeInfo["Name"])
		pool, ok := pools[nodeInfo["Name"].(string)]
		if !ok {
			return fmt.Errorf("could not find node %s in pools", nodeInfo["Name"])
		}
		publicationName := cfg.PublicationName
		err := queries.AlterPublicationAddTable(m.Ctx, pool, publicationName, m.QualifiedTableName)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == tableAlreadyInPublicationError {
				logger.Info("Table %s is already in publication %s on node %s", m.QualifiedTableName, publicationName, nodeInfo["Name"])
			} else {
				pool.Close()
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

		slotName, startLSN, tables, err := queries.GetCDCMetadata(m.Ctx, tx, publicationName)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to get cdc metadata on node %s: %w", nodeInfo["Name"], err)
		}

		if !slices.Contains(tables, m.QualifiedTableName) {
			tables = append(tables, m.QualifiedTableName)
		}

		err = queries.UpdateCDCMetadata(m.Ctx, tx, publicationName, slotName, startLSN, tables)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to update cdc metadata on node %s: %w", nodeInfo["Name"], err)
		}
		logger.Info("Updated CDC metadata for table %s on node %s", m.QualifiedTableName, nodeInfo["Name"])

		logger.Info("Creating Merkle Tree objects on %s...", nodeInfo["Name"])
		err = m.createMtreeObjects(tx, maxRows, numBlocks)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to create mtree objects on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Inserting block ranges on %s...", nodeInfo["Name"])
		err = m.insertBlockRanges(tx, blockRanges)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to insert block ranges on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Computing leaf hashes on %s...", nodeInfo["Name"])
		err = m.computeLeafHashes(pool, tx, blockRanges, numWorkers, "Computing leaf hashes:")
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to compute leaf hashes on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Building parent nodes on %s...", nodeInfo["Name"])
		err = m.buildParentNodes(tx)
		if err != nil {
			pool.Close()
			return fmt.Errorf("failed to build parent nodes on node %s: %w", nodeInfo["Name"], err)
		}

		logger.Info("Merkle tree built successfully on %s", nodeInfo["Name"])
		tx.Commit(m.Ctx)
		pool.Close()
	}

	resultCtx["max_rows"] = maxRows
	resultCtx["num_blocks"] = numBlocks
	resultCtx["block_ranges_provided"] = len(blockRanges) > 0

	return nil
}

func (m *MerkleTreeTask) UpdateMtree(skipAllChecks bool) (err error) {
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

	if !m.NoCDC {
		cdcCfg := config.Cfg.MTree.CDC
		timeout := 30 * time.Second
		if cdcCfg.CDCProcessingTimeout > 0 {
			timeout = time.Duration(cdcCfg.CDCProcessingTimeout) * time.Second
		}

		_, cancel := context.WithTimeout(m.Ctx, timeout)
		defer cancel()

		for _, nodeInfo := range m.ClusterNodes {
			if err := cdc.UpdateFromCDC(nodeInfo); err != nil {
				return fmt.Errorf("CDC update failed for node %s: %w", nodeInfo["Name"], err)
			}
		}
	}

	var blockSize int
	var foundBlockSize bool
	for _, nodeInfo := range m.ClusterNodes {
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, auth.ConnectionOptions{})
		if err != nil {
			return fmt.Errorf("error getting connection pool for node %s: %w", nodeInfo["Name"], err)
		}

		blockSize, err = queries.GetBlockSizeFromMetadata(m.Ctx, pool, m.Schema, m.Table)
		if err != nil {
			pool.Close()
			return fmt.Errorf("error getting block size from metadata on node %s: %w", nodeInfo["Name"], err)
		}

		pool.Close()
		foundBlockSize = true
	}

	if !foundBlockSize {
		return fmt.Errorf("could not determine block size from any node")
	}
	m.BlockSize = blockSize

	for _, nodeInfo := range m.ClusterNodes {
		fmt.Printf("\nUpdating Merkle tree on node: %s\n", nodeInfo["Name"])
		pool, err := auth.GetClusterNodeConnection(m.Ctx, nodeInfo, auth.ConnectionOptions{})
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
			compositeTypeIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
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

		mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
		mtreeTableName := mtreeTableIdentifier.Sanitize()

		blocksToUpdate, err := queries.GetDirtyAndNewBlocks(m.Ctx, tx, mtreeTableName, m.SimplePrimaryKey, m.Key)
		if err != nil {
			return fmt.Errorf("error getting dirty blocks on node %s: %w", nodeInfo["Name"], err)
		}

		if len(blocksToUpdate) == 0 {
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()
	isComposite := !m.SimplePrimaryKey
	var modifiedPositions []int64

	compositeTypeIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
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

	if err = m.UpdateMtree(true); err != nil {
		return fmt.Errorf("failed to update merkle tree before diff: %w", err)
	}
	nodePairs := getNodePairs(m.ClusterNodes)
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()

	allNodePairBatches := make(map[string]struct {
		node1   map[string]any
		node2   map[string]any
		batches [][2][]any
	})

	m.StartTime = time.Now()
	m.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Schema:        m.Schema,
			Table:         m.Table,
			Nodes:         m.NodeList,
			StartTime:     time.Now().Format(time.RFC3339),
			DiffRowsCount: make(map[string]int),
		},
	}
	m.diffRowKeySets = make(map[string]map[string]map[string]struct{})

	for _, pair := range nodePairs {
		node1 := pair[0]
		node2 := pair[1]
		logger.Info("Comparing merkle trees between %s and %s", node1["Name"], node2["Name"])

		pool1, err := auth.GetClusterNodeConnection(m.Ctx, node1, auth.ConnectionOptions{})
		if err != nil {
			return fmt.Errorf("failed to get connection pool for node %s: %w", node1["Name"], err)
		}
		defer pool1.Close()

		pool2, err := auth.GetClusterNodeConnection(m.Ctx, node2, auth.ConnectionOptions{})
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
			entry = struct {
				node1   map[string]any
				node2   map[string]any
				batches [][2][]any
			}{node1: node1, node2: node2, batches: [][2][]any{}}
		}
		entry.batches = append(entry.batches, batches...)
		allNodePairBatches[nodePairKey] = entry

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
		endTime := time.Now()
		m.DiffResult.Summary.EndTime = endTime.Format(time.RFC3339)
		m.DiffResult.Summary.TimeTaken = endTime.Sub(m.StartTime).String()
		m.DiffResult.Summary.PrimaryKey = m.Key
		if diffPath, _, writeErr := utils.WriteDiffReport(m.DiffResult, m.Schema, m.Table, m.Output); writeErr != nil {
			return writeErr
		} else if diffPath != "" {
			resultCtx["diff_file"] = diffPath
		}
	}

	resultCtx["mismatched_pairs"] = len(m.DiffResult.NodeDiffs)

	return nil
}

func (m *MerkleTreeTask) findMismatchedLeaves(pool1, pool2 *pgxpool.Pool, parentLevel int, parentPosition int64) (map[int64]bool, error) {
	mismatched := make(map[int64]bool)
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
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

	boundaries := []any{}
	for _, r := range allRanges {
		if r.RangeStart != nil {
			boundaries = append(boundaries, r.RangeStart)
		}
		if r.RangeEnd != nil {
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

	if len(sortedBoundaries) == 0 {
		return [][2][]any{}, nil
	}

	var slices [][2]any
	if len(sortedBoundaries) == 1 {
		s := sortedBoundaries[0]
		slices = append(slices, [2]any{s, s})
	} else {
		for i := 0; i < len(sortedBoundaries)-1; i++ {
			s := sortedBoundaries[i]
			e := sortedBoundaries[i+1]
			if m.intervalInUnion(s, e, allRanges) {
				slices = append(slices, [2]any{s, e})
			}
		}
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()
	isComposite := !m.SimplePrimaryKey
	var modifiedPositions []int64

	compositeTypeIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
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

	for block := range jobs {
		leafHash, err := queries.ComputeLeafHashes(m.Ctx, pool, m.Schema, m.Table, m.SimplePrimaryKey, m.Key, block.RangeStart, block.RangeEnd)
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
	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}

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

	err := queries.CreateSchema(m.Ctx, tx, aceSchema())
	if err != nil {
		return fmt.Errorf("failed to create schema '%s': %w", aceSchema(), err)
	}

	err = queries.CreateXORFunction(m.Ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to create xor function: %w", err)
	}

	err = queries.CreateMetadataTable(m.Ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to create metadata table: %w", err)
	}

	err = queries.UpdateMetadata(context.Background(), tx, m.Schema, m.Table, totalRows, m.BlockSize, numBlocks, !m.SimplePrimaryKey)
	if err != nil {
		return fmt.Errorf("failed to update metadata: %w", err)
	}

	mtreeTableIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("ace_mtree_%s_%s", m.Schema, m.Table)}
	mtreeTableName := mtreeTableIdentifier.Sanitize()
	err = queries.DropMtreeTable(context.Background(), tx, mtreeTableName)
	if err != nil {
		return fmt.Errorf("failed to render drop mtree table sql: %w", err)
	}

	if m.SimplePrimaryKey {
		pkeyType, err := queries.GetPkeyType(context.Background(), tx, m.Schema, m.Table, m.Key[0])
		if err != nil {
			return err
		}
		err = queries.CreateSimpleMtreeTable(context.Background(), tx, mtreeTableName, pkeyType)
		if err != nil {
			return fmt.Errorf("failed to render create simple mtree table sql: %w", err)
		}
	} else {
		keyTypeColumns := make([]string, len(m.Key))
		for i, col := range m.Key {
			colType, err := queries.GetPkeyType(context.Background(), tx, m.Schema, m.Table, col)
			if err != nil {
				return err
			}
			keyTypeColumns[i] = fmt.Sprintf("%s %s", pgx.Identifier{col}.Sanitize(), colType)
		}

		compositeTypeIdentifier := pgx.Identifier{aceSchema(), fmt.Sprintf("%s_%s_key_type", m.Schema, m.Table)}
		compositeTypeName := compositeTypeIdentifier.Sanitize()

		err = queries.DropCompositeType(context.Background(), tx, compositeTypeName)
		if err != nil {
			return fmt.Errorf("failed to render drop composite type sql: %w", err)
		}

		err = queries.CreateCompositeType(context.Background(), tx, compositeTypeName, strings.Join(keyTypeColumns, ", "))
		if err != nil {
			return fmt.Errorf("failed to render create composite type sql: %w", err)
		}

		err = queries.CreateCompositeMtreeTable(context.Background(), tx, mtreeTableName, compositeTypeName)
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
