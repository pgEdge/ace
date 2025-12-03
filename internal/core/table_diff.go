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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type Range struct {
	Start any
	End   any
}

func buildFilteredViewName(taskID, table string) string {
	sum := sha256.Sum256([]byte(taskID))
	hash := hex.EncodeToString(sum[:8]) // use first 16 hex chars
	return fmt.Sprintf("ace_%s_%s_filtered", hash, table)
}

type TableDiffTask struct {
	types.Task
	types.DerivedFields
	QualifiedTableName string
	DBName             string
	Nodes              string

	BaseTable           string
	FilteredViewName    string
	FilteredViewCreated bool

	BlockSize         int
	ConcurrencyFactor int
	Output            string
	TableFilter       string
	QuietMode         bool

	Mode              string
	OverrideBlockSize bool

	DiffFilePath string

	InvokeMethod string
	ClientRole   string

	DiffSummary map[string]string

	SkipDBUpdate bool

	TaskStore     *taskstore.Store
	TaskStorePath string

	Pools map[string]*pgxpool.Pool

	blockHashSQLCache map[hashBoundsKey]string
	blockHashSQLMu    sync.Mutex

	CompareUnitSize int
	MaxDiffRows     int64

	DiffResult types.DiffOutput
	diffMutex  sync.Mutex

	firstError   error
	firstErrorMu sync.Mutex

	totalDiffRows      atomic.Int64
	diffLimitTriggered atomic.Bool

	Ctx context.Context
}

// Implement ClusterConfigProvider interface for TableDiffTask
func (t *TableDiffTask) GetClusterName() string        { return t.ClusterName }
func (t *TableDiffTask) GetDBName() string             { return t.DBName }
func (t *TableDiffTask) SetDBName(name string)         { t.DBName = name }
func (t *TableDiffTask) GetNodes() string              { return t.Nodes }
func (t *TableDiffTask) GetNodeList() []string         { return t.NodeList }
func (t *TableDiffTask) SetNodeList(nl []string)       { t.NodeList = nl }
func (t *TableDiffTask) SetDatabase(db types.Database) { t.Database = db }
func (t *TableDiffTask) GetClusterNodes() []map[string]any {
	return t.ClusterNodes
}
func (t *TableDiffTask) SetClusterNodes(cn []map[string]any) { t.ClusterNodes = cn }

func (t *TableDiffTask) recordError(err error) {
	if err == nil {
		return
	}
	t.firstErrorMu.Lock()
	if t.firstError == nil {
		t.firstError = err
	}
	t.firstErrorMu.Unlock()
}

func (t *TableDiffTask) getFirstError() error {
	t.firstErrorMu.Lock()
	defer t.firstErrorMu.Unlock()
	return t.firstError
}

func (t *TableDiffTask) shouldStopDueToLimit() bool {
	if t.MaxDiffRows <= 0 {
		return false
	}
	if t.diffLimitTriggered.Load() {
		return true
	}
	return t.totalDiffRows.Load() >= t.MaxDiffRows
}

// It's imperative for the caller to hold the diffMutex while calling this function
func (t *TableDiffTask) incrementDiffRowsLocked(delta int) bool {
	if delta <= 0 || t.MaxDiffRows <= 0 {
		return false
	}

	total := t.totalDiffRows.Add(int64(delta))
	if total >= t.MaxDiffRows {
		t.DiffResult.Summary.DiffRowLimitReached = true
		if t.diffLimitTriggered.CompareAndSwap(false, true) {
			logger.Warn("table-diff: detected %d differences which meets/exceeds max_diff_rows limit (%d); stopping early", total, t.MaxDiffRows)
		}
		return true
	}
	return false
}

type RecursiveDiffTask struct {
	Node1Name                 string
	Node2Name                 string
	CurrentRange              Range
	CurrentEstimatedBlockSize int
}

type HashTask struct {
	nodeName   string
	rangeIndex int
	r          Range
}

type HashResult struct {
	hash string
	err  error
}

type hashBoundsKey struct {
	hasLower bool
	hasUpper bool
}

type RangeResults map[string]HashResult

func (t *TableDiffTask) getBlockHashSQL(hasLower, hasUpper bool) (string, error) {
	key := hashBoundsKey{hasLower: hasLower, hasUpper: hasUpper}

	t.blockHashSQLMu.Lock()
	defer t.blockHashSQLMu.Unlock()

	if t.blockHashSQLCache == nil {
		t.blockHashSQLCache = make(map[hashBoundsKey]string)
	}

	if sql, ok := t.blockHashSQLCache[key]; ok {
		return sql, nil
	}

	query, err := queries.BlockHashSQL(t.Schema, t.Table, t.Key, "TD_BLOCK_HASH" /* mode */, hasLower, hasUpper)
	if err != nil {
		return "", err
	}
	t.blockHashSQLCache[key] = query
	return query, nil
}

func NewTableDiffTask() *TableDiffTask {
	return &TableDiffTask{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskType:   taskstore.TaskTypeTableDiff,
			TaskStatus: taskstore.StatusPending,
		},
		Mode:              "diff",
		InvokeMethod:      "cli",
		DiffSummary:       make(map[string]string),
		blockHashSQLCache: make(map[hashBoundsKey]string),
		DerivedFields: types.DerivedFields{
			HostMap: make(map[string]string),
		},
		Ctx: context.Background(),
	}
}

func (t *TableDiffTask) fetchRows(nodeName string, r Range) ([]types.OrderedMap, error) {
	pool, ok := t.Pools[nodeName]
	if !ok {
		return nil, fmt.Errorf("no pool for node %s", nodeName)
	}

	if len(t.Key) == 0 {
		return nil, fmt.Errorf("primary key not defined for table %s.%s", t.Schema, t.Table)
	}

	quotedSchema := pgx.Identifier{t.Schema}.Sanitize()
	quotedTable := pgx.Identifier{t.Table}.Sanitize()
	quotedSchemaTable := fmt.Sprintf("%s.%s", quotedSchema, quotedTable)

	var colTypes map[string]string
	var colTypesKey string
	for _, nodeInfo := range t.ClusterNodes {
		if name, ok := nodeInfo["Name"].(string); ok && name == nodeName {
			publicIP, _ := nodeInfo["PublicIP"].(string)

			var portStr string
			switch v := nodeInfo["Port"].(type) {
			case string:
				portStr = v
			case float64:
				portStr = fmt.Sprintf("%.f", v)
			default:
				portStr = "5432"
			}
			colTypesKey = fmt.Sprintf("%s:%s", publicIP, portStr)
			colTypes = t.ColTypes[colTypesKey]
			break
		}
	}

	if colTypes == nil {
		return nil, fmt.Errorf("could not find column types for node %s using key %s", nodeName, colTypesKey)
	}

	selectCols := make([]string, 0, len(t.Cols)+2)
	selectCols = append(selectCols, "pg_xact_commit_timestamp(xmin) as commit_ts", "to_json(spock.xact_commit_timestamp_origin(xmin))->>'roident' as node_origin")

	for _, colName := range t.Cols {
		colType := colTypes[colName]
		quotedColName := pgx.Identifier{colName}.Sanitize()

		// We cast user defined types and arrays to TEXT to avoid scan errors with unknown OIDs
		if strings.HasSuffix(colType, "[]") ||
			strings.Contains(strings.ToLower(colType), "json") ||
			strings.Contains(strings.ToLower(colType), "bytea") ||
			!utils.IsKnownScalarType(colType) {
			selectCols = append(selectCols, fmt.Sprintf("%s::TEXT AS %s", quotedColName, quotedColName))
		} else {
			selectCols = append(selectCols, quotedColName)
		}
	}

	selectColsStr := strings.Join(selectCols, ", ")

	quotedKeyCols := make([]string, len(t.Key))
	for i, k := range t.Key {
		quotedKeyCols[i] = pgx.Identifier{k}.Sanitize()
	}

	orderByClause := ""
	if len(t.Key) > 0 {
		orderByClause = "ORDER BY " + strings.Join(quotedKeyCols, ", ")
	}

	var querySQL string
	args := []any{}

	var conditions []string
	paramIndex := 1

	if r.Start != nil {
		startVal := r.Start
		if len(t.Key) == 1 {
			// Simple primary key
			conditions = append(conditions, fmt.Sprintf("%s >= $%d", quotedKeyCols[0], paramIndex))
			args = append(args, startVal)
			paramIndex++
		} else {
			// Composite primary key
			startVals, ok := startVal.([]any)
			if !ok || len(startVals) != len(t.Key) {
				return nil, fmt.Errorf("r.Start is not a valid composite key for table %s.%s (expected %d values, got %T with value %v)", t.Schema, t.Table, len(t.Key), startVal, startVal)
			}

			pkTupleStr := fmt.Sprintf("ROW(%s)", strings.Join(quotedKeyCols, ", "))

			placeholders := make([]string, len(t.Key))
			for i := 0; i < len(t.Key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i)
			}
			placeholderTupleStr := fmt.Sprintf("ROW(%s)", strings.Join(placeholders, ", "))

			conditions = append(conditions, fmt.Sprintf("%s >= %s", pkTupleStr, placeholderTupleStr))
			args = append(args, startVals...)
			paramIndex += len(t.Key)
		}
	}

	if r.End != nil {
		endVal := r.End
		if len(t.Key) == 1 {
			conditions = append(conditions, fmt.Sprintf("%s < $%d", quotedKeyCols[0], paramIndex))
			args = append(args, endVal)
		} else {
			endVals, ok := endVal.([]any)
			if !ok || len(endVals) != len(t.Key) {
				return nil, fmt.Errorf("r.End is not a valid composite key for table %s.%s (expected %d values, got %T with value %v)", t.Schema, t.Table, len(t.Key), endVal, endVal)
			}

			pkTupleStr := fmt.Sprintf("ROW(%s)", strings.Join(quotedKeyCols, ", "))

			placeholders := make([]string, len(t.Key))
			for i := 0; i < len(t.Key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i)
			}
			placeholderTupleStr := fmt.Sprintf("ROW(%s)", strings.Join(placeholders, ", "))

			conditions = append(conditions, fmt.Sprintf("%s < %s", pkTupleStr, placeholderTupleStr))
			args = append(args, endVals...)
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	querySQL = fmt.Sprintf("SELECT %s FROM %s %s %s", selectColsStr, quotedSchemaTable, whereClause, orderByClause)

	logger.Debug("[%s] Fetching rows for range: Start=%v, End=%v. SQL: %s, Args: %v", nodeName, r.Start, r.End, querySQL, args)

	pgRows, err := pool.Query(t.Ctx, querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows for range on node %s (SQL: %s, Args: %v): %w", nodeName, querySQL, args, err)
	}
	defer pgRows.Close()

	var results []types.OrderedMap
	colsDesc := pgRows.FieldDescriptions()

	for pgRows.Next() {
		rowValues := make([]any, len(colsDesc))
		rowValPtrs := make([]any, len(colsDesc))
		for i := range rowValues {
			rowValPtrs[i] = &rowValues[i]
		}

		if err := pgRows.Scan(rowValPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row on node %s: %w", nodeName, err)
		}

		rowData := make(types.OrderedMap, len(colsDesc))
		for i, colD := range colsDesc {
			val := rowValues[i]
			var processedVal any
			if val == nil {
				processedVal = nil
			} else {
				switch v := val.(type) {
				case pgtype.Numeric:
					var fValue float64
					if v.Status == pgtype.Present {
						v.AssignTo(&fValue)
						processedVal = fValue
					} else {
						processedVal = nil
					}
				case pgtype.Timestamp:
					if v.Status == pgtype.Present {
						processedVal = v.Time
					} else {
						processedVal = nil
					}
				case pgtype.Timestamptz:
					if v.Status == pgtype.Present {
						processedVal = v.Time
					} else {
						processedVal = nil
					}
				case pgtype.Date:
					if v.Status == pgtype.Present {
						processedVal = v.Time
					} else {
						processedVal = nil
					}
				case pgtype.Bytea:
					if v.Status == pgtype.Present {
						processedVal = v.Bytes
					} else {
						processedVal = nil
					}
				case pgtype.Interval:
					if v.Status == pgtype.Present {
						if encoded, err := v.EncodeText(nil, nil); err == nil {
							processedVal = string(encoded)
						} else {
							processedVal = nil
						}
					} else {
						processedVal = nil
					}
				case string:
					processedVal = v
				case int8, int16, int32, int64, int,
					uint8, uint16, uint32, uint64, uint,
					float32, float64, bool:
					processedVal = v
				case pgtype.JSON, pgtype.JSONB:
					if v.(interface{ GetStatus() pgtype.Status }).GetStatus() != pgtype.Present {
						processedVal = nil
					} else {
						var dataHolder any
						if assignable, ok := v.(interface{ AssignTo(dst any) error }); ok {
							err := assignable.AssignTo(&dataHolder)
							if err == nil {
								if marshalled, mErr := json.Marshal(dataHolder); mErr == nil {
									processedVal = string(marshalled)
								} else {
									processedVal = fmt.Sprint(dataHolder)
								}
							} else {
								processedVal = nil
							}
						} else {
							processedVal = nil
						}
					}
				default:
					if marshaler, ok := val.(json.Marshaler); ok {
						if marshalled, err := marshaler.MarshalJSON(); err == nil {
							processedVal = string(marshalled)
						} else {
							processedVal = fmt.Sprint(val)
						}
					} else if stringer, ok := val.(fmt.Stringer); ok {
						processedVal = stringer.String()
					} else {
						processedVal = fmt.Sprint(val)
					}
				}
			}
			rowData[i] = types.KVPair{Key: string(colD.Name), Value: processedVal}
		}
		results = append(results, rowData)
	}
	if err := pgRows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error on node %s: %w", nodeName, err)
	}
	return results, nil
}

func (t *TableDiffTask) compareBlocks(
	node1, node2 string,
	r Range,
) (utils.DiffResult, error) {
	n1Rows, err := t.fetchRows(node1, r)
	if err != nil {
		return utils.DiffResult{}, fmt.Errorf("failed to fetch rows for node %s in range %v-%v: %w", node1, r.Start, r.End, err)
	}
	n2Rows, err := t.fetchRows(node2, r)
	if err != nil {
		return utils.DiffResult{}, fmt.Errorf("failed to fetch rows for node %s in range %v-%v: %w", node2, r.Start, r.End, err)
	}

	diffResult, err := utils.CompareRowSets(n1Rows, n2Rows, t.Key, t.Cols)
	if err != nil {
		return utils.DiffResult{}, fmt.Errorf("failed to compare row sets for %s vs %s: %w", node1, node2, err)
	}

	if len(diffResult.Node1OnlyRows) > 0 || len(diffResult.Node2OnlyRows) > 0 || len(diffResult.ModifiedRows) > 0 {
		logger.Debug("[%s vs %s] Comparison for range Start=%v, End=%v: N1Only=%d, N2Only=%d, Modified=%d",
			node1, node2, r.Start, r.End,
			len(diffResult.Node1OnlyRows), len(diffResult.Node2OnlyRows), len(diffResult.ModifiedRows))
	}

	return diffResult, nil
}

func (t *TableDiffTask) Validate() error {
	if t.ClusterName == "" || t.QualifiedTableName == "" {
		return fmt.Errorf("cluster_name and table_name are required arguments")
	}

	if t.BlockSize > config.Cfg.TableDiff.MaxBlockSize && !t.OverrideBlockSize {
		return fmt.Errorf("block row size should be <= %d", config.Cfg.TableDiff.MaxBlockSize)
	}
	if t.BlockSize < config.Cfg.TableDiff.MinBlockSize && !t.OverrideBlockSize {
		return fmt.Errorf("block row size should be >= %d", config.Cfg.TableDiff.MinBlockSize)
	}

	if t.MaxDiffRows < 0 {
		return fmt.Errorf("max_diff_rows must be >= 0")
	}
	if t.MaxDiffRows == 0 && config.Cfg.TableDiff.MaxDiffRows > 0 {
		t.MaxDiffRows = config.Cfg.TableDiff.MaxDiffRows
	}

	if t.ConcurrencyFactor > 10 || t.ConcurrencyFactor < 1 {
		return fmt.Errorf("invalid value range for concurrency_factor, must be between 1 and 10")
	}

	if t.Output != "json" && t.Output != "html" {
		return fmt.Errorf("table-diff currently supports only json and html output formats")
	}

	nodeList, err := utils.ParseNodes(t.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. E.g., nodes=\"n1,n2\". Error: %w", err)
	}

	t.NodeList = nodeList

	if len(nodeList) > 3 {
		return fmt.Errorf("table-diff currently supports up to a three-way table comparison")
	}

	if t.Nodes != "all" && len(nodeList) == 1 {
		return fmt.Errorf("table-diff needs at least two nodes to compare")
	}

	err = utils.ReadClusterInfo(t)
	if err != nil {
		return fmt.Errorf("error loading cluster information: %w", err)
	}

	logger.Info("Cluster %s exists", t.ClusterName)

	parts := strings.Split(t.QualifiedTableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("tableName %s must be of form 'schema.table_name'", t.QualifiedTableName)
	}
	schema, table := parts[0], parts[1]

	// Sanitise inputs here
	if err := queries.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := queries.SanitiseIdentifier(table); err != nil {
		return err
	}

	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		if len(nodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !utils.Contains(nodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeMap)
		utils.ApplyDatabaseCredentials(combinedMap, t.Database)
		clusterNodes = append(clusterNodes, combinedMap)
	}

	if t.Nodes != "all" && len(nodeList) > 1 {
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
		t.NodeList = []string{}
		for _, node := range clusterNodes {
			t.NodeList = append(t.NodeList, node["Name"].(string))
		}
	}

	t.Schema = schema
	t.Table = table
	t.ClusterNodes = clusterNodes

	return nil
}

func (t *TableDiffTask) RunChecks(skipValidation bool) (err error) {
	defer func() {
		if err != nil && t.FilteredViewCreated {
			t.cleanupFilteredView()
		}
	}()

	if !skipValidation {
		if err = t.Validate(); err != nil {
			return err
		}
	}

	var cols, key []string
	hostMap := make(map[string]string)

	schema := t.Schema
	table := t.Table
	if t.BaseTable == "" {
		t.BaseTable = table
	}
	var filteredViewName string
	if t.TableFilter != "" {
		filteredViewName = buildFilteredViewName(t.TaskID, table)
		t.FilteredViewName = filteredViewName
	}

	for _, nodeInfo := range t.ClusterNodes {
		hostname, _ := nodeInfo["Name"].(string)
		hostIP, _ := nodeInfo["PublicIP"].(string)
		user, _ := nodeInfo["DBUser"].(string)

		port, ok := nodeInfo["Port"].(string)
		if !ok {
			port = "5432"
		}

		if !utils.Contains(t.NodeList, hostname) {
			continue
		}

		conn, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, auth.ConnectionOptions{Role: t.ClientRole})
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", hostname, err)
		}
		defer conn.Close()

		currCols, err := queries.GetColumns(t.Ctx, conn, schema, table)
		if err != nil {
			return fmt.Errorf("failed to get columns for table %s.%s on node %s: %w", schema, table, hostname, err)
		}
		if len(currCols) == 0 {
			return fmt.Errorf("table '%s.%s' not found on %s, or the current user does not have adequate privileges", schema, table, hostname)
		}

		currKey, err := queries.GetPrimaryKey(t.Ctx, conn, schema, table)
		if err != nil {
			return fmt.Errorf("failed to get primary key for table %s.%s on node %s: %w", schema, table, hostname, err)
		}
		if len(currKey) == 0 {
			return fmt.Errorf("no primary key found for '%s.%s'", schema, table)
		}

		if len(cols) == 0 && len(key) == 0 {
			cols = currCols
			key = currKey
		}

		if !reflect.DeepEqual(currCols, cols) || !reflect.DeepEqual(currKey, key) {
			return fmt.Errorf("table schemas don't match between nodes")
		}

		cols = currCols
		key = currKey

		colTypes, err := queries.GetColumnTypes(t.Ctx, conn, schema, table)
		if err != nil {
			return fmt.Errorf("failed to get column types for table %s on node %s: %w", table, hostname, err)
		}

		colTypesKey := fmt.Sprintf("%s:%s", hostIP, port)

		if t.ColTypes == nil {
			t.ColTypes = make(map[string]map[string]string)
		}
		t.ColTypes[colTypesKey] = colTypes

		actualPrivs, err := queries.CheckUserPrivileges(t.Ctx, conn, user, schema, table)
		if err != nil {
			return fmt.Errorf("failed to check user privileges on node %s: %w", hostname, err)
		}

		if !actualPrivs.TableSelect {
			return fmt.Errorf("user \"%s\" does not have the necessary privileges to run table-diff on table \"%s.%s\" on node \"%s\"",
				user, schema, table, hostname)
		}

		hostMap[hostIP+":"+port] = hostname

		if t.TableFilter != "" {
			sanitisedViewName := pgx.Identifier{filteredViewName}.Sanitize()
			sanitisedSchema := pgx.Identifier{schema}.Sanitize()
			sanitisedTable := pgx.Identifier{table}.Sanitize()
			viewSQL := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT * FROM %s.%s WHERE %s",
				sanitisedViewName, sanitisedSchema, sanitisedTable, t.TableFilter)

			_, err = conn.Exec(t.Ctx, viewSQL)
			if err != nil {
				return fmt.Errorf("failed to create filtered view: %w", err)
			}
			t.FilteredViewCreated = true

			hasRowsSQL := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s) AS has_rows", sanitisedViewName)
			var hasRows bool
			err = conn.QueryRow(t.Ctx, hasRowsSQL).Scan(&hasRows)
			if err != nil {
				return fmt.Errorf("failed to check if view has rows: %w", err)
			}

			if !hasRows {
				return fmt.Errorf("table filter produced no rows")
			}

			t.FilteredViewCreated = true
		}
	}

	logger.Info("Connections successful to nodes in cluster")

	t.HostMap = hostMap
	t.Cols = cols
	t.Key = key
	t.SimplePrimaryKey = len(key) == 1

	if len(t.ColTypes) > 1 {
		var refNode string
		var refTypes map[string]string
		for node, types := range t.ColTypes {
			refNode = node
			refTypes = types
			break
		}

		for node, types := range t.ColTypes {
			if node == refNode {
				continue
			}

			mismatchedCols := make(map[string][2]string)
			for col, refType := range refTypes {
				if nodeType, exists := types[col]; exists && nodeType != refType {
					mismatchedCols[col] = [2]string{refType, nodeType}
				}
			}

			if len(mismatchedCols) > 0 {
				var mismatches []string
				for col, typePair := range mismatchedCols {
					mismatches = append(mismatches, fmt.Sprintf("  Column '%s': %s=%s, %s=%s",
						col, refNode, typePair[0], node, typePair[1]))
				}
				logger.Info("Warning: Column types mismatch detected between %s and %s:\n%s",
					refNode, node, strings.Join(mismatches, "\n"))
			}
		}
	}

	logger.Info("Table %s is comparable across nodes", t.QualifiedTableName)

	if err := t.CheckColumnSize(); err != nil {
		return err
	}

	if t.TableFilter != "" {
		t.Table = filteredViewName
	}

	return nil
}

func (t *TableDiffTask) cleanupFilteredView() {
	if t.TableFilter == "" || t.FilteredViewName == "" || !t.FilteredViewCreated {
		return
	}

	schemaIdent := pgx.Identifier{t.Schema}.Sanitize()
	viewIdent := pgx.Identifier{t.FilteredViewName}.Sanitize()
	dropSQL := fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s.%s", schemaIdent, viewIdent)

	for _, nodeInfo := range t.ClusterNodes {
		name, _ := nodeInfo["Name"].(string)
		if len(t.NodeList) > 0 && !utils.Contains(t.NodeList, name) {
			continue
		}

		pool, err := auth.GetClusterNodeConnection(context.Background(), nodeInfo, auth.ConnectionOptions{Role: t.ClientRole})
		if err != nil {
			logger.Warn("table-diff: failed to get connection for filtered view cleanup on node %s: %v", name, err)
			continue
		}

		if _, err := pool.Exec(context.Background(), dropSQL); err != nil {
			logger.Warn("table-diff: failed to drop filtered view %s.%s on node %s: %v", t.Schema, t.FilteredViewName, name, err)
		} else {
			logger.Info("table-diff: dropped filtered view %s.%s on node %s", t.Schema, t.FilteredViewName, name)
		}

		pool.Close()
	}

	t.FilteredViewCreated = false
	if t.BaseTable != "" {
		t.Table = t.BaseTable
	}
}

func (t *TableDiffTask) CloneForSchedule(ctx context.Context) *TableDiffTask {
	cloned := NewTableDiffTask()
	cloned.ClusterName = t.ClusterName
	cloned.QualifiedTableName = t.QualifiedTableName
	cloned.DBName = t.DBName
	cloned.Nodes = t.Nodes
	cloned.BlockSize = t.BlockSize
	cloned.ConcurrencyFactor = t.ConcurrencyFactor
	cloned.Output = t.Output
	cloned.TableFilter = t.TableFilter
	cloned.QuietMode = t.QuietMode
	cloned.Mode = t.Mode
	cloned.OverrideBlockSize = t.OverrideBlockSize
	cloned.TaskStore = t.TaskStore
	cloned.TaskStorePath = t.TaskStorePath
	cloned.SkipDBUpdate = t.SkipDBUpdate
	cloned.ClientRole = t.ClientRole
	cloned.InvokeMethod = t.InvokeMethod
	cloned.CompareUnitSize = t.CompareUnitSize
	cloned.MaxDiffRows = t.MaxDiffRows
	cloned.Ctx = ctx
	return cloned
}

func (t *TableDiffTask) CheckColumnSize() error {
	for hostPort, types := range t.ColTypes {
		parts := strings.Split(hostPort, ":")
		if len(parts) != 2 {
			continue
		}

		host, portStr := parts[0], parts[1]
		port, _ := strconv.Atoi(portStr)

		var pool *pgxpool.Pool
		for _, nodeInfo := range t.ClusterNodes {
			nodeHost, _ := nodeInfo["PublicIP"].(string)

			var nodePort float64
			if nodePortVal, ok := nodeInfo["Port"]; ok {
				switch v := nodePortVal.(type) {
				case string:
					nodePort, _ = strconv.ParseFloat(v, 64)
				case float64:
					nodePort = v
				}
			}

			if nodePort == 0 {
				nodePort = 5432
			}

			if nodeHost == host && int(nodePort) == port {
				var err error
				pool, err = auth.GetClusterNodeConnection(t.Ctx, nodeInfo, auth.ConnectionOptions{Role: t.ClientRole})
				if err != nil {
					return fmt.Errorf("failed to connect to node %s:%d: %w", host, port, err)
				}
				break
			}
		}

		if pool == nil {
			continue
		}

		for colName, colType := range types {
			if !strings.Contains(colType, "bytea") {
				continue
			}

			maxSize, err := queries.MaxColumnSize(context.Background(), pool, t.Schema, t.Table, colName)
			logger.Debug("Column %s of table %s.%s has max size %d", colName, t.Schema, t.Table, maxSize)
			if err != nil {
				pool.Close()
				return fmt.Errorf("failed to check size of bytea column %s: %w", colName, err)
			}

			if maxSize > 1000000 {
				pool.Close()
				return fmt.Errorf("refusing to perform table-diff. Data in column %s of table %s.%s is larger than 1 MB",
					colName, t.Schema, t.Table)
			}

		}
		pool.Close()
	}

	return nil
}

func (t *TableDiffTask) ExecuteTask() (err error) {
	startTime := time.Now()

	if t.Mode == "rerun" {
		t.Task.TaskType = taskstore.TaskTypeTableRerun
	} else if t.Task.TaskType == "" {
		t.Task.TaskType = taskstore.TaskTypeTableDiff
	}

	if strings.TrimSpace(t.TaskID) == "" {
		t.TaskID = uuid.NewString()
	}
	t.Task.StartedAt = startTime
	t.Task.TaskStatus = taskstore.StatusRunning
	t.Task.ClusterName = t.ClusterName

	t.totalDiffRows.Store(0)
	t.diffLimitTriggered.Store(false)

	var recorder *taskstore.Recorder

	if !t.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(t.TaskStore, t.TaskStorePath)
		if recErr != nil {
			logger.Warn("table-diff: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if t.TaskStore == nil && rec.Store() != nil {
				t.TaskStore = rec.Store()
			}

			ctx := map[string]any{
				"qualified_table": t.QualifiedTableName,
				"mode":            t.Mode,
				"nodes":           t.Nodes,
			}
			if t.TableFilter != "" {
				ctx["table_filter"] = t.TableFilter
			}

			record := taskstore.Record{
				TaskID:      t.TaskID,
				TaskType:    taskstore.TaskTypeTableDiff,
				Status:      taskstore.StatusRunning,
				ClusterName: t.ClusterName,
				SchemaName:  t.Schema,
				TableName:   t.Table,
				StartedAt:   startTime,
				TaskContext: ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("table-diff: unable to write initial task status (%v)", err)
			}
		}
	}

	defer t.cleanupFilteredView()

	defer func() {
		finishedAt := time.Now()
		t.Task.FinishedAt = finishedAt
		t.Task.TimeTaken = finishedAt.Sub(startTime).Seconds()

		status := taskstore.StatusFailed
		if err == nil {
			status = taskstore.StatusCompleted
		}
		t.Task.TaskStatus = status

		if recorder != nil && recorder.Created() {
			ctx := map[string]any{
				"diff_summary": t.DiffResult.Summary,
			}
			if len(t.DiffResult.NodeDiffs) > 0 {
				ctx["diff_pairs"] = len(t.DiffResult.NodeDiffs)
			}
			if err != nil {
				ctx["error"] = err.Error()
			}

			updateErr := recorder.Update(taskstore.Record{
				TaskID:       t.TaskID,
				Status:       status,
				DiffFilePath: t.DiffFilePath,
				FinishedAt:   finishedAt,
				TimeTaken:    t.Task.TimeTaken,
				TaskContext:  ctx,
			})
			if updateErr != nil {
				logger.Warn("table-diff: unable to update task status (%v)", updateErr)
			}
		}

		if recorder != nil && recorder.OwnsStore() {
			storePtr := recorder.Store()
			if closeErr := recorder.Close(); closeErr != nil {
				logger.Warn("table-diff: failed to close task store (%v)", closeErr)
			}
			if storePtr != nil && t.TaskStore == storePtr {
				t.TaskStore = nil
			}
		}
	}()

	if t.Mode == "rerun" {
		return t.ExecuteRerunTask()
	}

	logger.Debug("Using CompareUnitSize: %d", t.CompareUnitSize)

	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	maxConcurrent := runtime.NumCPU() * t.ConcurrencyFactor
	logger.Info("Using %d CPUs, max concurrent workers = %d", runtime.NumCPU(), maxConcurrent)
	sem := make(chan struct{}, maxConcurrent)

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		pool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, auth.ConnectionOptions{Role: t.ClientRole})
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", name, err)
		}
		pools[name] = pool
		defer pool.Close()
	}
	t.Pools = pools

	if _, err = t.getBlockHashSQL(true, true); err != nil {
		return fmt.Errorf("failed to build block-hash SQL: %w", err)
	}

	var maxCount int64
	var maxNode string
	var totalEstimatedRowsAcrossNodes int64

	for name, pool := range pools {
		var count int64
		// TODO: Estimates cannot be used on views. But we can't run a count(*)
		// on millions of rows either. Need to find a better way to do this.
		if t.TableFilter == "" {
			count, err = queries.GetRowCountEstimate(t.Ctx, pool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to render estimate row count query: %w", err)
			}
		} else {
			sanitisedSchema := pgx.Identifier{t.Schema}.Sanitize()
			sanitisedTable := pgx.Identifier{t.Table}.Sanitize()
			countQuerySQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sanitisedSchema, sanitisedTable)
			logger.Debug("[%s] Executing count query for filtered table: %s", name, countQuerySQL)
			err = pool.QueryRow(t.Ctx, countQuerySQL).Scan(&count)
			if err != nil {
				return fmt.Errorf("failed to get row count for %s.%s on node %s (query: %s): %w", t.Schema, t.Table, name, countQuerySQL, err)
			}
		}

		totalEstimatedRowsAcrossNodes += int64(count)
		logger.Debug("Table contains %d rows (estimated) on %s", count, name)
		if count > maxCount {
			maxCount = count
			maxNode = name
		}
	}
	if maxNode == "" {
		return fmt.Errorf("unable to determine node with highest row count (or any row counts)")
	}

	t.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Schema:            t.Schema,
			Table:             t.Table,
			Nodes:             t.NodeList,
			BlockSize:         t.BlockSize,
			CompareUnitSize:   t.CompareUnitSize,
			ConcurrencyFactor: t.ConcurrencyFactor,
			MaxDiffRows:       t.MaxDiffRows,
			StartTime:         startTime.Format(time.RFC3339),
			TotalRowsChecked:  int64(maxCount),
			DiffRowsCount:     make(map[string]int),
		},
	}

	sampleMethod := "BERNOULLI"
	samplePercent := 0.0
	switch {
	case maxCount > 1e8:
		sampleMethod = "SYSTEM"
		samplePercent = 0.01
	case maxCount > 1e6:
		sampleMethod = "SYSTEM"
		samplePercent = 0.1
	case maxCount > 1e5:
		samplePercent = 1
	case maxCount > 1e4:
		samplePercent = 10
	default:
		samplePercent = 100
	}

	var ranges []Range
	/* Determine if we should use direct PKey offset generation.
	 * Essentially, we don't want to use probabilistic sampling for tables with
	 * less than 10,000 rows to avoid non-deterministic results.

	 * TODO: table-filter should also support probabilistic sampling.
	 */
	// if (maxCount > 0 && maxCount <= 10000) || t.TableFilter != "" {
	// 	logger.Info("Using direct primary key offset generation for table %s.%s (maxCount: %d, tableFilter: '%s')",
	// 		t.Schema, t.Table, maxCount, t.TableFilter)
	// 	r, err := t.getPkeyOffsets(ctx, pools[maxNode])
	// 	if err != nil {
	// 		return logger.Error("failed to get pkey offsets directly: %w", err)
	// 	}
	// 	ranges = r
	// } else {
	ntileCount := int(math.Ceil(float64(maxCount) / float64(t.BlockSize)))
	if ntileCount == 0 && maxCount > 0 {
		ntileCount = 1
	}

	querySQL, err := queries.GeneratePkeyOffsetsQuery(t.Schema, t.Table, t.Key, sampleMethod, samplePercent, ntileCount)
	logger.Debug("Generated offsets query: %s", querySQL)
	if err != nil {
		return fmt.Errorf("failed to generate offsets query: %w", err)
	}
	pkRangesRows, err := pools[maxNode].Query(t.Ctx, querySQL)
	if err != nil {
		return fmt.Errorf("offsets query execution failed on %s: %w", maxNode, err)
	}
	defer pkRangesRows.Close()

	numPKCols := len(t.Key)
	totalScanCols := 2 * numPKCols
	if totalScanCols == 0 {
		return fmt.Errorf("primary key not defined, cannot determine columns to scan for ranges")
	}
	scanDest := make([]any, totalScanCols)
	scanDestPtrs := make([]any, totalScanCols)
	for i := range scanDest {
		scanDestPtrs[i] = &scanDest[i]
	}

	for pkRangesRows.Next() {
		if err := pkRangesRows.Scan(scanDestPtrs...); err != nil {
			return fmt.Errorf("scanning offset row failed (expected %d columns for %d PKs): %w", totalScanCols, numPKCols, err)
		}

		var rStart, rEnd any

		if numPKCols == 1 {
			rStart = scanDest[0]
			rEnd = scanDest[1]
		} else {
			startKeyParts := make([]any, numPKCols)
			copy(startKeyParts, scanDest[0:numPKCols])
			rStart = startKeyParts

			endKeyParts := make([]any, numPKCols)
			copy(endKeyParts, scanDest[numPKCols:2*numPKCols])
			allNil := true
			for _, v := range endKeyParts {
				if v != nil {
					allNil = false
					break
				}
			}
			if allNil {
				rEnd = nil
			} else {
				rEnd = endKeyParts
			}
		}
		ranges = append(ranges, Range{Start: rStart, End: rEnd})
	}
	if err := pkRangesRows.Err(); err != nil {
		return fmt.Errorf("offset rows iteration error: %w", err)
	}

	if len(ranges) > 0 && ranges[0].Start != nil {
		firstOriginalStart := ranges[0].Start
		newInitialRange := Range{Start: nil, End: firstOriginalStart}
		ranges = append([]Range{newInitialRange}, ranges...)
	}
	// }

	logger.Debug("Created %d initial ranges to compare", len(ranges))
	logger.Debug("Ranges: %v", ranges)
	t.DiffResult.Summary.InitialRangesCount = len(ranges)

	resultsMap := make(map[int]RangeResults)
	var resultsMutex sync.Mutex

	var nodeNames []string
	for name := range pools {
		nodeNames = append(nodeNames, name)
	}
	sort.Strings(nodeNames)

	totalHashTasks := len(nodeNames) * len(ranges)
	p := mpb.New(mpb.WithOutput(os.Stderr))
	bar := p.AddBar(int64(totalHashTasks),
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Hashing initial ranges: ", decor.WC{W: 18}),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(" | "),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
		),
	)

	/*
		We use the following approach:
		1. Generate a list of ranges to hash, and create a HashTask for each range.
		2. Each HashTask is independent of the others, and can be executed in parallel.
		3. We don't immediately perform the comparisons, but instead store the results in a map.
		4. Once they're ready, we use a binary search approach to narrow down the ranges that have mismatches.
	*/
	hashTaskQueue := make(chan HashTask, totalHashTasks)
	var initialHashWg sync.WaitGroup
	for i := 0; i < maxConcurrent; i++ {
		initialHashWg.Add(1)
		go func() {
			defer initialHashWg.Done()
			for task := range hashTaskQueue {
				sem <- struct{}{}
				queryCtx, cancel := context.WithTimeout(t.Ctx, 60*time.Second)
				hashValue, hErr := t.hashRange(queryCtx, task.nodeName, task.r)
				cancel()
				<-sem

				if hErr != nil {
					errWrap := fmt.Errorf("initial hash failed for node %s range %v-%v: %w", task.nodeName, task.r.Start, task.r.End, hErr)
					t.recordError(errWrap)
				}

				resultsMutex.Lock()
				if _, ok := resultsMap[task.rangeIndex]; !ok {
					resultsMap[task.rangeIndex] = make(RangeResults)
				}
				resultsMap[task.rangeIndex][task.nodeName] = HashResult{hash: hashValue, err: hErr}
				resultsMutex.Unlock()
				bar.Increment()
			}
		}()
	}

	for rangeIdx, currentRange := range ranges {
		for _, nodeName := range nodeNames {
			hashTaskQueue <- HashTask{nodeName: nodeName, rangeIndex: rangeIdx, r: currentRange}
		}
	}
	close(hashTaskQueue)
	initialHashWg.Wait()

	if err := t.getFirstError(); err != nil {
		return err
	}

	logger.Info("Initial hash calculations complete. Proceeding with comparisons for mismatches...")

	var diffWg sync.WaitGroup
	var mismatchedTasks []RecursiveDiffTask

	for rangeIdx := 0; rangeIdx < len(ranges); rangeIdx++ {
		currentRange := ranges[rangeIdx]
		for i := 0; i < len(nodeNames); i++ {
			node1 := nodeNames[i]
			for j := i + 1; j < len(nodeNames); j++ {
				node2 := nodeNames[j]

				r1, r1ok := resultsMap[rangeIdx][node1]
				r2, r2ok := resultsMap[rangeIdx][node2]

				if !r1ok || !r2ok || r1.err != nil || r2.err != nil {
					logger.Info("ERROR: Cannot compare range %d (%v-%v) for %s/%s due to missing initial hash or error. N1OK: %t, N2OK: %t, N1Err: %v, N2Err: %v",
						rangeIdx, currentRange.Start, currentRange.End, node1, node2, r1ok, r2ok, r1.err, r2.err)
					continue
				}

				if r1.hash != r2.hash {
					logger.Debug("%s Mismatch in initial range %d (%v-%v) for %s vs %s. Hashes: %s... / %s... narrowing down diffs...",
						utils.CrossMark, rangeIdx, currentRange.Start, currentRange.End, node1, node2, utils.SafeCut(r1.hash, 8), utils.SafeCut(r2.hash, 8))
					mismatchedTasks = append(mismatchedTasks, RecursiveDiffTask{
						Node1Name:                 node1,
						Node2Name:                 node2,
						CurrentRange:              currentRange,
						CurrentEstimatedBlockSize: t.BlockSize,
					})

				} else {
					logger.Debug("%s Match in initial range %d (%v-%v) for %s vs %s", utils.CheckMark, rangeIdx, currentRange.Start, currentRange.End, node1, node2)
				}
			}
		}
	}

	if len(mismatchedTasks) > 0 {
		logger.Debug("Found %d initial mismatched ranges. Narrowing down differences.", len(mismatchedTasks))

		diffBar := p.AddBar(int64(len(mismatchedTasks)),
			mpb.PrependDecorators(
				decor.Name("Analysing mismatches: ", decor.WC{W: 18}),
				decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(
				decor.Elapsed(decor.ET_STYLE_GO),
				decor.Name(" | "),
				decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
			),
		)

		for _, task := range mismatchedTasks {
			if t.shouldStopDueToLimit() {
				diffBar.Increment()
				continue
			}
			diffWg.Add(1)
			go func(task RecursiveDiffTask) {
				defer diffBar.Increment()
				t.recursiveDiff(ctx, task, &diffWg)
			}(task)
		}
	}

	diffWg.Wait()
	p.Wait()

	if t.diffLimitTriggered.Load() {
		t.DiffResult.Summary.DiffRowLimitReached = true
	}

	if err := t.getFirstError(); err != nil {
		return err
	}

	logger.Info("Table diff comparison completed for %s", t.QualifiedTableName)
	if t.DiffResult.Summary.DiffRowLimitReached {
		logger.Warn("table-diff stopped after reaching max_diff_rows=%d; additional differences may exist", t.MaxDiffRows)
	}

	endTime := time.Now()
	t.DiffResult.Summary.EndTime = endTime.Format(time.RFC3339)
	t.DiffResult.Summary.TimeTaken = endTime.Sub(startTime).String()

	t.AddPrimaryKeyToDiffSummary()

	jsonPath, _, err := utils.WriteDiffReport(t.DiffResult, t.Schema, t.Table, t.Output)
	if err != nil {
		return err
	}
	if jsonPath != "" {
		t.DiffFilePath = jsonPath
	}

	return nil
}

func (t *TableDiffTask) hashRange(
	ctx context.Context,
	node string,
	r Range,
) (string, error) {
	pool, ok := t.Pools[node]
	if !ok {
		return "", fmt.Errorf("no pool for node %s", node)
	}
	startTime := time.Now()
	var hash string

	numPKCols := len(t.Key)

	startVals, hasLower, err := extractRangeBoundValues(r.Start, numPKCols)
	if err != nil {
		return "", err
	}
	endVals, hasUpper, err := extractRangeBoundValues(r.End, numPKCols)
	if err != nil {
		return "", err
	}

	query, err := t.getBlockHashSQL(hasLower, hasUpper)
	if err != nil {
		return "", fmt.Errorf("failed to build block-hash SQL: %w", err)
	}

	args := make([]any, 0, len(startVals)+len(endVals))
	if hasLower {
		args = append(args, startVals...)
	}
	if hasUpper {
		args = append(args, endVals...)
	}

	logger.Debug("[%s] Hashing range: Start=%v, End=%v. SQL: %s, Args: %v", node, r.Start, r.End, query, args)

	err = pool.QueryRow(ctx, query, args...).Scan(&hash)

	if err != nil {
		duration := time.Since(startTime)
		logger.Debug("[%s] ERROR after %v for range Start=%v, End=%v (using query: '%s', args: %v): %v", node, duration, r.Start, r.End, query, args, err)
		return "", fmt.Errorf("BlockHash query failed for %s range %v-%v: %w", node, r.Start, r.End, err)
	}

	duration := time.Since(startTime)
	if duration > 10000*time.Millisecond {
		logger.Debug("[%s] Slow query? Range Start=%v, End=%v took %v", node, r.Start, r.End, duration)
	} else {
		logger.Debug("[%s] Range Start=%v, End=%v took %v", node, r.Start, r.End, duration)
	}
	return hash, nil
}

func extractRangeBoundValues(bound any, numPKCols int) ([]any, bool, error) {
	if bound == nil {
		return nil, false, nil
	}
	if numPKCols == 1 {
		return []any{bound}, true, nil
	}

	processSlice := func(vals []any) ([]any, bool, error) {
		if len(vals) != numPKCols {
			return nil, false, fmt.Errorf("range bound expected %d values, got %d", numPKCols, len(vals))
		}
		if rangeSliceAllNil(vals) {
			return nil, false, nil
		}
		return vals, true, nil
	}

	if vals, ok := bound.([]any); ok {
		return processSlice(vals)
	}
	if valsIface, ok := bound.([]interface{}); ok {
		vals := make([]any, len(valsIface))
		copy(vals, valsIface)
		return processSlice(vals)
	}

	return nil, false, fmt.Errorf("unsupported range bound type %T", bound)
}

func rangeSliceAllNil(vals []any) bool {
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

func (t *TableDiffTask) generateSubRanges(
	node string,
	parentRange Range,
	numSplits int,
) ([]Range, error) {
	if numSplits <= 0 {
		return nil, fmt.Errorf("numSplits must be positive")
	}
	if len(t.Key) == 0 {
		return nil, fmt.Errorf("primary key not defined")
	}
	pool, ok := t.Pools[node]
	if !ok {
		return nil, fmt.Errorf("no pool for node %s", node)
	}

	quotedKeyCols := make([]string, len(t.Key))
	for i, k := range t.Key {
		quotedKeyCols[i] = pgx.Identifier{k}.Sanitize()
	}
	pkColsStr := strings.Join(quotedKeyCols, ", ")
	pkTupleStr := fmt.Sprintf("ROW(%s)", pkColsStr)
	schemaTable := fmt.Sprintf("%s.%s", pgx.Identifier{t.Schema}.Sanitize(), pgx.Identifier{t.Table}.Sanitize())

	var conditions []string
	args := []any{}
	paramIdx := 1

	if parentRange.Start != nil {
		startVal := parentRange.Start
		if len(t.Key) == 1 {
			conditions = append(conditions, fmt.Sprintf("%s >= $%d", quotedKeyCols[0], paramIdx))
			args = append(args, startVal)
			paramIdx++
		} else {
			startVals, ok := startVal.([]any)
			if !ok {
				return nil, fmt.Errorf("generateSubRanges: parentRange.Start is not a valid composite key")
			}
			placeholders := make([]string, len(t.Key))
			for i := 0; i < len(t.Key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIdx+i)
			}
			placeholderTupleStr := fmt.Sprintf("ROW(%s)", strings.Join(placeholders, ", "))
			conditions = append(conditions, fmt.Sprintf("%s >= %s", pkTupleStr, placeholderTupleStr))
			args = append(args, startVals...)
			paramIdx += len(t.Key)
		}
	}

	if parentRange.End != nil {
		endVal := parentRange.End
		if len(t.Key) == 1 {
			// In hashRange, the end is exclusive (<), but here for counting and splitting
			// we use inclusive (<=) to match fetchRows. This is acceptable because
			// we are splitting a mismatched range, and slight overlap is okay.
			conditions = append(conditions, fmt.Sprintf("%s <= $%d", quotedKeyCols[0], paramIdx))
			args = append(args, endVal)
			paramIdx++
		} else {
			endVals, ok := endVal.([]any)
			if !ok {
				return nil, fmt.Errorf("generateSubRanges: parentRange.End is not a valid composite key")
			}
			placeholders := make([]string, len(t.Key))
			for i := 0; i < len(t.Key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIdx+i)
			}
			placeholderTupleStr := fmt.Sprintf("ROW(%s)", strings.Join(placeholders, ", "))
			conditions = append(conditions, fmt.Sprintf("%s <= %s", pkTupleStr, placeholderTupleStr))
			args = append(args, endVals...)
			paramIdx += len(t.Key)
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	countQuery := fmt.Sprintf("SELECT COUNT(1) FROM %s %s", schemaTable, whereClause)
	var count int64
	err := pool.QueryRow(t.Ctx, countQuery, args...).Scan(&count)
	if err != nil {
		logger.Debug("[%s] Failed to count rows in parent range %v-%v for splitting: %v. SQL: %s, Args: %v", node, parentRange.Start, parentRange.End, err, countQuery, args)
		return nil, fmt.Errorf("failed to count for split: %w", err)
	}

	if count < 2 || (numSplits > 1 && count < int64(numSplits)) {
		logger.Debug("[%s] Cannot split range %v-%v further, row count %d, requested splits %d", node, parentRange.Start, parentRange.End, count, numSplits)
		return []Range{parentRange}, nil
	}

	if numSplits == 2 {
		sqlOffset := (count / 2) - 1
		if sqlOffset < 0 {
			sqlOffset = 0
		}

		orderByPK := strings.Join(quotedKeyCols, ", ")

		medianQueryArgs := make([]any, len(args))
		copy(medianQueryArgs, args)
		medianQueryArgs = append(medianQueryArgs, sqlOffset)

		medianQuery := fmt.Sprintf("SELECT %s FROM %s %s ORDER BY %s LIMIT 1 OFFSET $%d",
			pkColsStr, schemaTable, whereClause, orderByPK, paramIdx)

		var medianPKVal any
		numPKCols := len(t.Key)

		if numPKCols == 1 {
			err = pool.QueryRow(t.Ctx, medianQuery, medianQueryArgs...).Scan(&medianPKVal)
		} else {
			scanDest := make([]any, numPKCols)
			scanDestPtrs := make([]any, numPKCols)
			for i := range scanDest {
				scanDestPtrs[i] = &scanDest[i]
			}
			err = pool.QueryRow(t.Ctx, medianQuery, medianQueryArgs...).Scan(scanDestPtrs...)
			if err == nil {
				medianPKVal = append([]any{}, scanDest...)
			}
		}

		if err != nil {
			logger.Debug("[%s] Failed to find median PK for range %v-%v: %v. SQL: %s, Args: %v", node, parentRange.Start, parentRange.End, err, medianQuery, medianQueryArgs)
			return []Range{parentRange}, nil
		}

		range1End := medianPKVal
		range2Start := medianPKVal

		if reflect.DeepEqual(parentRange.Start, medianPKVal) && reflect.DeepEqual(medianPKVal, parentRange.End) {
			logger.Debug("[%s] Median PK %v is same as range bounds %v-%v. Cannot split.", node, medianPKVal, parentRange.Start, parentRange.End)
			return []Range{parentRange}, nil
		}

		subRanges := []Range{
			{Start: parentRange.Start, End: range1End},
		}
		if !reflect.DeepEqual(medianPKVal, parentRange.End) {
			subRanges = append(subRanges, Range{Start: range2Start, End: parentRange.End})
		} else if len(subRanges) == 1 && reflect.DeepEqual(subRanges[0], parentRange) {
			logger.Debug("[%s] Split resulted in first sub-range same as parent. No effective split for %v-%v.", node, parentRange.Start, parentRange.End)
			return []Range{parentRange}, nil
		}

		logger.Debug("[%s] Split range %v-%v into %d sub-ranges using median %v (count: %d). SubRanges: %v", node, parentRange.Start, parentRange.End, len(subRanges), medianPKVal, count, subRanges)
		return subRanges, nil
	}

	logger.Info("[%s] generateSubRangesViaNtile not fully implemented for numSplits=%d, returning parent range.", node, numSplits)
	return []Range{parentRange}, nil
}

func (t *TableDiffTask) recursiveDiff(
	ctx context.Context,
	task RecursiveDiffTask,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	if t.shouldStopDueToLimit() {
		return
	}

	node1Name := task.Node1Name
	node2Name := task.Node2Name
	currentRange := task.CurrentRange
	currentEstimatedBlockSize := task.CurrentEstimatedBlockSize

	finalCompareUnitSize := max(t.CompareUnitSize, 1)

	isSmallEnough := currentEstimatedBlockSize <= finalCompareUnitSize
	if !isSmallEnough && currentRange.Start != nil && currentRange.End != nil && reflect.DeepEqual(currentRange.Start, currentRange.End) {
		isSmallEnough = true
	}

	if isSmallEnough {
		logger.Debug("[%s vs %s] Range %v-%v (est. size %d) is <= compare_unit_size %d. Fetching rows.",
			node1Name, node2Name, currentRange.Start, currentRange.End, currentEstimatedBlockSize, finalCompareUnitSize)

		diffInfo, err := t.compareBlocks(node1Name, node2Name, currentRange)
		if err != nil {
			errWrap := fmt.Errorf("fetch and compare rows for %s/%s in range %v-%v: %w", node1Name, node2Name, currentRange.Start, currentRange.End, err)
			t.recordError(errWrap)
			logger.Info("ERROR during fetchAndCompareRows for %s/%s, range %v-%v: %v", node1Name, node2Name, currentRange.Start, currentRange.End, errWrap)
			return
		}

		pairKey := node1Name + "/" + node2Name
		if strings.Compare(node1Name, node2Name) > 0 {
			pairKey = node2Name + "/" + node1Name
		}

		var currentDiffRowsForPair int
		limitReached := false
		if len(diffInfo.Node1OnlyRows) > 0 || len(diffInfo.Node2OnlyRows) > 0 || len(diffInfo.ModifiedRows) > 0 {
			t.diffMutex.Lock()

			if _, ok := t.DiffResult.NodeDiffs[pairKey]; !ok {
				t.DiffResult.NodeDiffs[pairKey] = types.DiffByNodePair{
					Rows: make(map[string][]types.OrderedMap),
				}
			}

			if _, ok := t.DiffResult.NodeDiffs[pairKey].Rows[node1Name]; !ok {
				t.DiffResult.NodeDiffs[pairKey].Rows[node1Name] = []types.OrderedMap{}
			}
			if _, ok := t.DiffResult.NodeDiffs[pairKey].Rows[node2Name]; !ok {
				t.DiffResult.NodeDiffs[pairKey].Rows[node2Name] = []types.OrderedMap{}
			}

			for _, row := range diffInfo.Node1OnlyRows {
				if t.shouldStopDueToLimit() {
					limitReached = true
					break
				}
				rowAsMap := utils.OrderedMapToMap(row)
				rowWithMeta := utils.AddSpockMetadata(rowAsMap)
				rowAsOrderedMap := utils.MapToOrderedMap(rowWithMeta, t.Cols)
				t.DiffResult.NodeDiffs[pairKey].Rows[node1Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node1Name], rowAsOrderedMap)
				currentDiffRowsForPair++
				if t.incrementDiffRowsLocked(1) {
					limitReached = true
					break
				}
			}

			if !limitReached {
				for _, row := range diffInfo.Node2OnlyRows {
					if t.shouldStopDueToLimit() {
						limitReached = true
						break
					}
					rowAsMap := utils.OrderedMapToMap(row)
					rowWithMeta := utils.AddSpockMetadata(rowAsMap)
					rowAsOrderedMap := utils.MapToOrderedMap(rowWithMeta, t.Cols)
					t.DiffResult.NodeDiffs[pairKey].Rows[node2Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node2Name], rowAsOrderedMap)
					currentDiffRowsForPair++
					if t.incrementDiffRowsLocked(1) {
						limitReached = true
						break
					}
				}
			}

			if !limitReached {
				for _, modRow := range diffInfo.ModifiedRows {
					if t.shouldStopDueToLimit() {
						limitReached = true
						break
					}
					node1DataAsMap := utils.OrderedMapToMap(modRow.Node1Data)
					node1DataWithMeta := utils.AddSpockMetadata(node1DataAsMap)
					node1DataAsOrderedMap := utils.MapToOrderedMap(node1DataWithMeta, t.Cols)
					t.DiffResult.NodeDiffs[pairKey].Rows[node1Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node1Name], node1DataAsOrderedMap)

					node2DataAsMap := utils.OrderedMapToMap(modRow.Node2Data)
					node2DataWithMeta := utils.AddSpockMetadata(node2DataAsMap)
					node2DataAsOrderedMap := utils.MapToOrderedMap(node2DataWithMeta, t.Cols)
					t.DiffResult.NodeDiffs[pairKey].Rows[node2Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node2Name], node2DataAsOrderedMap)
					currentDiffRowsForPair++
					if t.incrementDiffRowsLocked(1) {
						limitReached = true
						break
					}
				}
			}

			if t.DiffResult.Summary.DiffRowsCount == nil {
				t.DiffResult.Summary.DiffRowsCount = make(map[string]int)
			}
			t.DiffResult.Summary.DiffRowsCount[pairKey] += currentDiffRowsForPair
			t.diffMutex.Unlock()

			if limitReached || t.shouldStopDueToLimit() {
				return
			}
		}
		return
	}

	if t.shouldStopDueToLimit() {
		return
	}

	subRanges, err := t.generateSubRanges(node1Name, currentRange, 2)
	if err != nil {
		errWrap := fmt.Errorf("generate sub-ranges for %s/%s in range %v-%v: %w", node1Name, node2Name, currentRange.Start, currentRange.End, err)
		t.recordError(errWrap)
		logger.Info("ERROR generating sub-ranges for %s/%s, range %v-%v: %v. Stopping recursion for this path.",
			node1Name, node2Name, currentRange.Start, currentRange.End, errWrap)
		return
	}

	if len(subRanges) == 0 {
		logger.Debug("[%s vs %s] Range %v-%v could not be split further (generateSubRangesViaNtile returned empty). Treating as unit.",
			node1Name, node2Name, currentRange.Start, currentRange.End)
		// Fallback: treat current range as the smallest unit and compare.
		// Call self with current range but force small enough size
		task.CurrentEstimatedBlockSize = finalCompareUnitSize
		newWg := &sync.WaitGroup{}
		newWg.Add(1)
		t.recursiveDiff(ctx, task, newWg)
		newWg.Wait()
		return
	}

	// If generateSubRanges returns the original range, it means it couldn't split.
	if len(subRanges) == 1 && reflect.DeepEqual(subRanges[0], currentRange) {
		logger.Debug("[%s vs %s] generateSubRangesViaNtile returned same range %v-%v. Fetching rows.",
			node1Name, node2Name, currentRange.Start, currentRange.End)
		task.CurrentEstimatedBlockSize = finalCompareUnitSize
		newWg := &sync.WaitGroup{}
		newWg.Add(1)
		t.recursiveDiff(ctx, task, newWg)
		newWg.Wait()
		return
	}

	for _, sr := range subRanges {
		if t.shouldStopDueToLimit() {
			return
		}

		newEstimatedBlockSize := currentEstimatedBlockSize / len(subRanges)
		if newEstimatedBlockSize <= 0 {
			newEstimatedBlockSize = 1
		}

		hashCtx, cancelHash := context.WithTimeout(ctx, 60*time.Second)

		hash1Chan := make(chan HashResult, 1)
		hash2Chan := make(chan HashResult, 1)

		go func() {
			h, e := t.hashRange(hashCtx, node1Name, sr)
			hash1Chan <- HashResult{hash: h, err: e}
		}()
		go func() {
			h, e := t.hashRange(hashCtx, node2Name, sr)
			hash2Chan <- HashResult{hash: h, err: e}
		}()

		res1 := <-hash1Chan
		res2 := <-hash2Chan
		cancelHash()

		if res1.err != nil {
			errWrap := fmt.Errorf("hashing sub-range %v-%v for %s: %w", sr.Start, sr.End, node1Name, res1.err)
			t.recordError(errWrap)
			logger.Info("ERROR hashing sub-range %v-%v for %s: %v", sr.Start, sr.End, node1Name, errWrap)
			continue
		}
		if res2.err != nil {
			errWrap := fmt.Errorf("hashing sub-range %v-%v for %s: %w", sr.Start, sr.End, node2Name, res2.err)
			t.recordError(errWrap)
			logger.Info("ERROR hashing sub-range %v-%v for %s: %v", sr.Start, sr.End, node2Name, errWrap)
			continue
		}

		if res1.hash != res2.hash {
			logger.Debug("%s Mismatch in sub-range %v-%v for %s (%s...) vs %s (%s...). Recursing.",
				utils.CrossMark, sr.Start, sr.End, node1Name, utils.SafeCut(res1.hash, 8), node2Name, utils.SafeCut(res2.hash, 8))

			if t.shouldStopDueToLimit() {
				return
			}

			wg.Add(1)
			go t.recursiveDiff(ctx, RecursiveDiffTask{
				Node1Name:                 node1Name,
				Node2Name:                 node2Name,
				CurrentRange:              sr,
				CurrentEstimatedBlockSize: newEstimatedBlockSize,
			}, wg)
		} else {
			logger.Debug("%s Match in sub-range %v-%v for %s vs %s.", utils.CheckMark, sr.Start, sr.End, node1Name, node2Name)
		}
	}
}

func (t *TableDiffTask) AddPrimaryKeyToDiffSummary() {
	if t.DiffResult.Summary.PrimaryKey == nil {
		t.DiffResult.Summary.PrimaryKey = t.Key
	}
}
