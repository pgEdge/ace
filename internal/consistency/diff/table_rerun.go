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
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
)

func (t *TableDiffTask) ExecuteRerunTask() error {
	startTime := time.Now()

	if err := CheckDiffFileFormat(t.DiffFilePath, t); err != nil {
		return err
	}
	logger.Info("Successfully loaded and validated diff file: %s", t.DiffFilePath)

	if err := utils.ReadClusterInfo(t); err != nil {
		return fmt.Errorf("error loading cluster information for rerun: %w", err)
	}

	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeMap)
		utils.ApplyDatabaseCredentials(combinedMap, t.Database)
		clusterNodes = append(clusterNodes, combinedMap)
	}
	t.ClusterNodes = clusterNodes

	if err := t.RunChecks(true); err != nil {
		return fmt.Errorf("pre-run checks failed for rerun: %w", err)
	}

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		if !utils.Contains(t.NodeList, name) {
			continue
		}
		pool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
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

	// Collect all unique primary keys from the original diff report
	allPkeys, err := t.collectPkeysFromDiff()
	if err != nil {
		return fmt.Errorf("failed to collect primary keys from diff file: %w", err)
	}
	if len(allPkeys) == 0 {
		logger.Info("No differences found in the original report. Nothing to rerun.")
		return nil
	}
	logger.Info("Found %d unique rows with differences in the original report to re-check.", len(allPkeys))

	// pkeyStrs[i] is the report key of the key values pkeyValues[i].
	pkeyStrs := make([]string, 0, len(allPkeys))
	pkeyValues := make([][]any, 0, len(allPkeys))
	for pkStr, pkMap := range allPkeys {
		pkVals := make([]any, 0, len(t.Key))
		for _, pkCol := range t.Key {
			pkVals = append(pkVals, pkMap[pkCol])
		}
		pkeyStrs = append(pkeyStrs, pkStr)
		pkeyValues = append(pkeyValues, pkVals)
	}

	fetchedRowsByNode := make(map[string]map[string]types.OrderedMap)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errs := make(chan error, len(t.NodeList))

	for _, nodeName := range t.NodeList {
		wg.Add(1)
		go func(n string) {
			defer wg.Done()
			pool := t.Pools[n]
			rows, fErr := fetchRowsByPkeys(t.Ctx, pool, t, pkeyStrs, pkeyValues)
			if fErr != nil {
				errs <- fmt.Errorf("failed to fetch rows for node %s: %w", n, fErr)
				return
			}
			mu.Lock()
			fetchedRowsByNode[n] = rows
			mu.Unlock()
			logger.Debug("Fetched %d rows for node %s", len(rows), n)
		}(nodeName)
	}
	wg.Wait()
	close(errs)

	for e := range errs {
		return e
	}

	newDiffResult, err := t.reCompareDiffs(fetchedRowsByNode)
	if err != nil {
		return fmt.Errorf("failed during re-comparison: %w", err)
	}

	newDiffResult.Summary.TimeTaken = time.Since(startTime).String()
	newDiffResult.Summary.EndTime = time.Now().Format(time.RFC3339)

	if len(newDiffResult.NodeDiffs) > 0 {
		outputFileName := fmt.Sprintf("%s_%s_rerun-diffs-%s.json",
			strings.ReplaceAll(t.Schema, ".", "_"),
			strings.ReplaceAll(t.Table, ".", "_"),
			time.Now().Format("20060102150405"),
		)
		totalPersistentDiffs := 0
		for _, count := range newDiffResult.Summary.DiffRowsCount {
			totalPersistentDiffs += count
		}
		logger.Warn("%s Found %d persistent differences. Writing new report to %s", utils.CrossMark, totalPersistentDiffs, outputFileName)
		jsonData, mErr := json.MarshalIndent(newDiffResult, "", "  ")
		if mErr != nil {
			return fmt.Errorf("failed to marshal new diff report: %w", mErr)
		}
		if wErr := utils.WriteFileSecure(outputFileName, jsonData); wErr != nil {
			return fmt.Errorf("failed to write new diff report: %w", wErr)
		}
	} else {
		logger.Info("%s All previously reported differences have been resolved.", utils.CheckMark)
	}

	return nil
}

func CheckDiffFileFormat(filePath string, task *TableDiffTask) error {
	if filePath == "" {
		return fmt.Errorf("a diff file path must be provided for rerun mode via --rerun-from")
	}
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read diff file %s: %w", filePath, err)
	}

	var diffOutput types.DiffOutput
	if err := json.Unmarshal(data, &diffOutput); err != nil {
		return fmt.Errorf("failed to unmarshal diff file %s: %w", filePath, err)
	}
	if err := diffOutput.CheckFormatVersion(); err != nil {
		return fmt.Errorf("diff file %s: %w", filePath, err)
	}

	summary := diffOutput.Summary
	if summary.Schema == "" || summary.Table == "" || len(summary.Nodes) == 0 || len(summary.PrimaryKey) == 0 {
		return fmt.Errorf("diff file is missing essential summary information (schema, table, nodes, primaryKey)")
	}

	task.DiffResult = diffOutput
	task.Schema = summary.Schema
	task.Table = summary.Table
	task.QualifiedTableName = fmt.Sprintf("%s.%s", task.Schema, task.Table)
	task.NodeList = summary.Nodes
	task.Key = summary.PrimaryKey
	task.SimplePrimaryKey = len(task.Key) == 1

	// Inherit settings from the original run
	task.BlockSize = summary.BlockSize
	task.ConcurrencyFactor = summary.ConcurrencyFactor
	task.CompareUnitSize = summary.CompareUnitSize

	return nil
}

func (t *TableDiffTask) collectPkeysFromDiff() (map[string]map[string]any, error) {
	allPkeys := make(map[string]map[string]any)

	for _, nodePairDiff := range t.DiffResult.NodeDiffs {
		for _, rows := range nodePairDiff.Rows {
			for _, row := range rows {
				pkVal := make(map[string]any)
				for _, pkCol := range t.Key {
					if pkData, ok := row.Get(pkCol); ok {
						pkVal[pkCol] = pkData
					} else {
						return nil, fmt.Errorf("primary key column '%s' not found in a diff row", pkCol)
					}
				}
				pkStr, err := utils.StringifyKey(pkVal, t.Key)
				if err != nil {
					return nil, fmt.Errorf("failed to stringify key: %w", err)
				}
				allPkeys[pkStr] = pkVal
			}
		}
	}
	return allPkeys, nil
}

// rerunKeyPosCol is the column of the temporary key table that holds the
// position of a key in the list of keys from the report.
const rerunKeyPosCol = "ace_report_key_pos"

// fetchRowsByPkeys efficiently fetches a list of rows from a node by their primary keys.
// It uses a temporary table and a JOIN for high performance with large numbers of keys.
//
// pkeyStrs[i] is the report key of the row with the key values pkeyVals[i].
// The result maps a report key to the row read for it. A row is matched to
// its report key by its position in the key table, not by a key printed
// from the values read: a value read from the database can print in another
// form than the same value in the report (a timestamp, for example). With a
// printed key such a row would look missing on every node.
// TODO: Can this be separated out into a common function that can be used by other tasks?
func fetchRowsByPkeys(ctx context.Context, pool *pgxpool.Pool, t *TableDiffTask, pkeyStrs []string, pkeyVals [][]any) (map[string]types.OrderedMap, error) {
	if len(pkeyVals) == 0 {
		return make(map[string]types.OrderedMap), nil
	}
	if len(pkeyStrs) != len(pkeyVals) {
		return nil, fmt.Errorf("%d report keys for %d key value sets", len(pkeyStrs), len(pkeyVals))
	}
	for _, pkCol := range t.Key {
		if pkCol == rerunKeyPosCol {
			return nil, fmt.Errorf("primary key column %s has the name of the key position column of the rerun", pkCol)
		}
	}
	posIdent := pgx.Identifier{rerunKeyPosCol}.Sanitize()

	pkColTypes, err := queries.GetPkeyColumnTypes(ctx, pool, t.Schema, t.Table, t.Key)
	if err != nil {
		return nil, fmt.Errorf("could not determine primary key column types: %w", err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	tempTableName := fmt.Sprintf("rerun_pkeys_%s", strings.ReplaceAll(t.TaskID, "-", ""))
	sanitisedTempTable := pgx.Identifier{tempTableName}.Sanitize()

	var pkColDefs []string
	for _, pkCol := range t.Key {
		colType, ok := pkColTypes[pkCol]
		if !ok {
			return nil, fmt.Errorf("could not find type for primary key column: %s", pkCol)
		}
		pkColDefs = append(pkColDefs, fmt.Sprintf("%s %s", pgx.Identifier{pkCol}.Sanitize(), colType))
	}
	pkColDefs = append(pkColDefs, posIdent+" integer")

	createTempTableSQL := fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s) ON COMMIT PRESERVE ROWS", sanitisedTempTable, strings.Join(pkColDefs, ", "))
	logger.Debug("Creating temporary table for pkeys: %s", createTempTableSQL)
	_, err = tx.Exec(ctx, createTempTableSQL) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary table: %w", err)
	}

	logger.Debug("Copying %d pkey sets into temporary table %s", len(pkeyVals), tempTableName)
	copyCols := append(append(make([]string, 0, len(t.Key)+1), t.Key...), rerunKeyPosCol)
	copyRows := make([][]any, len(pkeyVals))
	for i, vals := range pkeyVals {
		copyRows[i] = append(append(make([]any, 0, len(vals)+1), vals...), int32(i))
	}
	_, err = tx.CopyFrom(ctx, pgx.Identifier{tempTableName}, copyCols, pgx.CopyFromRows(copyRows))
	if err != nil {
		return nil, fmt.Errorf("failed to copy primary keys to temporary table: %w", err)
	}

	schemaTable := fmt.Sprintf("%s.%s", pgx.Identifier{t.Schema}.Sanitize(), pgx.Identifier{t.Table}.Sanitize())

	var joinConditions []string
	for _, pkCol := range t.Key {
		sanitisedPkCol := pgx.Identifier{pkCol}.Sanitize()
		joinConditions = append(joinConditions, fmt.Sprintf("t.%s = temp.%s", sanitisedPkCol, sanitisedPkCol))
	}

	colTypes, err := queries.GetColumnTypes(ctx, tx, t.Schema, t.Table)
	if err != nil {
		return nil, fmt.Errorf("could not determine column types: %w", err)
	}

	// The position of the key comes first; it is not a part of the row.
	selectCols := make([]string, 0, len(t.Cols)+3)
	selectCols = append(selectCols, "temp."+posIdent)
	selectCols = append(selectCols, "pg_xact_commit_timestamp(t.xmin) as commit_ts", "to_json(pg_xact_commit_timestamp_origin(t.xmin))->>'roident' as node_origin")
	for _, col := range t.Cols {
		// The columns are fetched in the form of a diff report, so that the rerun
		// report compares and repairs the same way.
		selectCols = append(selectCols, utils.SelectColExpr("t", col, colTypes[col]))
	}

	fetchSQL := fmt.Sprintf("SELECT %s FROM %s t JOIN %s temp ON %s",
		strings.Join(selectCols, ", "), schemaTable, sanitisedTempTable, strings.Join(joinConditions, " AND "))

	logger.Debug("Fetching rows with pkeys from temporary table: %s", fetchSQL)
	pgRows, err := tx.Query(ctx, fetchSQL) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("failed to query rows using temp table join: %w", err)
	}
	defer pgRows.Close()

	// The values are read in the form of a diff report.
	results := make(map[string]types.OrderedMap)
	for pgRows.Next() {
		rowData, err := utils.ScanRow(pgRows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan re-run row: %w", err)
		}

		pos, ok := rowData[0].Value.(int32)
		if !ok || pos < 0 || int(pos) >= len(pkeyStrs) {
			return nil, fmt.Errorf("re-run row has an invalid key position %v", rowData[0].Value)
		}
		results[pkeyStrs[pos]] = rowData[1:]
	}

	if err := pgRows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration failed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return results, nil
}

func (t *TableDiffTask) reCompareDiffs(fetchedRowsByNode map[string]map[string]types.OrderedMap) (*types.DiffOutput, error) {
	newDiffResult := &types.DiffOutput{
		FormatVersion: types.DiffFormatVersion,
		NodeDiffs:     make(map[string]types.DiffByNodePair),
		Summary:       t.DiffResult.Summary,
	}
	newDiffResult.Summary.DiffRowsCount = make(map[string]int)
	newDiffResult.Summary.TotalRowsChecked = 0
	newDiffResult.Summary.MismatchedRangesCount = 0

	originalDiff := t.DiffResult

	for pairKey, nodePairDiff := range originalDiff.NodeDiffs {
		nodes := strings.Split(pairKey, "/")
		if len(nodes) != 2 {
			continue
		}
		node1, node2 := nodes[0], nodes[1]

		originalNode1Rows := make(map[string]types.OrderedMap)
		originalNode2Rows := make(map[string]types.OrderedMap)
		allPkeysForPair := make(map[string]bool)

		for _, row := range nodePairDiff.Rows[node1] {
			pkStr, _ := utils.StringifyOrderedMapKey(row, t.Key)
			originalNode1Rows[pkStr] = row
			allPkeysForPair[pkStr] = true
		}
		for _, row := range nodePairDiff.Rows[node2] {
			pkStr, _ := utils.StringifyOrderedMapKey(row, t.Key)
			originalNode2Rows[pkStr] = row
			allPkeysForPair[pkStr] = true
		}

		newDiffsForPair := types.DiffByNodePair{
			Rows: make(map[string][]types.OrderedMap),
		}
		persistentDiffCount := 0

		for pkStr := range allPkeysForPair {
			newRow1, nowOnNode1 := fetchedRowsByNode[node1][pkStr]
			newRow2, nowOnNode2 := fetchedRowsByNode[node2][pkStr]

			// The row is gone from both nodes: the nodes agree now. Counting
			// it would report a difference with no row to show or repair.
			if !nowOnNode1 && !nowOnNode2 {
				continue
			}

			isDifferent := false
			if !nowOnNode1 || !nowOnNode2 {
				isDifferent = true
			} else {
				var err error
				isDifferent, err = areRowsDifferent(newRow1, newRow2, t.Cols)
				if err != nil {
					return nil, fmt.Errorf("failed to compare rows for pkey %s: %w", pkStr, err)
				}
			}

			if isDifferent {
				persistentDiffCount++
				if nowOnNode1 {
					rowAsMap := utils.OrderedMapToMap(newRow1)
					rowWithMeta := t.withSpockMetadata(rowAsMap, node1)
					newDiffsForPair.Rows[node1] = append(newDiffsForPair.Rows[node1], utils.MapToOrderedMap(rowWithMeta, t.Cols))
				}
				if nowOnNode2 {
					rowAsMap := utils.OrderedMapToMap(newRow2)
					rowWithMeta := t.withSpockMetadata(rowAsMap, node2)
					newDiffsForPair.Rows[node2] = append(newDiffsForPair.Rows[node2], utils.MapToOrderedMap(rowWithMeta, t.Cols))
				}
			}
		}

		if persistentDiffCount > 0 {
			newDiffResult.NodeDiffs[pairKey] = newDiffsForPair
			newDiffResult.Summary.DiffRowsCount[pairKey] = persistentDiffCount
		}
	}
	return newDiffResult, nil
}

func areRowsDifferent(row1, row2 types.OrderedMap, dataCols []string) (bool, error) {
	for _, col := range dataCols {
		val1, ok1 := row1.Get(col)
		val2, ok2 := row2.Get(col)

		if !ok1 || !ok2 {
			return false, fmt.Errorf("column %s not found in one of the rows", col)
		}

		if !reflect.DeepEqual(val1, val2) {
			return true, nil
		}
	}
	return false, nil
}
