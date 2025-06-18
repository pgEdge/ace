package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/helpers"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type LogLevel int

const (
	LevelInfo LogLevel = iota
	LevelDebug
)

type Logger struct {
	level LogLevel
	*log.Logger
}

type Range struct {
	Start any
	End   any
}

type TableDiffTask struct {
	types.Task
	types.DerivedFields
	QualifiedTableName string
	DBName             string
	Nodes              string

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

	Pools map[string]*pgxpool.Pool

	BlockHashSQL string

	CompareUnitSize int

	DiffResult types.DiffOutput
	diffMutex  sync.Mutex
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

type NodePairDiff struct {
	Node1OnlyRows []map[string]any
	Node2OnlyRows []map[string]any
	ModifiedRows  []struct {
		Pkey      string
		Node1Data map[string]any
		Node2Data map[string]any
	}
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

type RangeResults map[string]HashResult

type PairKey struct {
	rangeIndex int
	node1      string
	node2      string
}

func NewLogger(out *os.File, level LogLevel) *Logger {
	return &Logger{
		level:  level,
		Logger: log.New(out, "", log.LstdFlags),
	}
}

func (l *Logger) Info(format string, v ...any) {
	if l.level <= LevelInfo {
		l.Printf("[INFO] "+format, v...)
	}
}

func (l *Logger) Debug(format string, v ...any) {
	if l.level >= LevelDebug {
		l.Printf("[DEBUG] "+format, v...)
	}
}

func (l *Logger) SetLevel(level LogLevel) {
	l.level = level
}

var logger = NewLogger(os.Stdout, LevelInfo)

func NewTableDiffTask() *TableDiffTask {
	return &TableDiffTask{
		Mode:         "diff",
		InvokeMethod: "cli",
		DiffSummary:  make(map[string]string),
		DerivedFields: types.DerivedFields{
			HostMap: make(map[string]string),
		},
	}
}

func sanitise(identifier string) string {
	return pgx.Identifier{identifier}.Sanitize()
}

func stringifyKey(pkValues map[string]any, pkCols []string) string {
	if len(pkCols) == 0 {
		return ""
	}
	if len(pkCols) == 1 {
		return fmt.Sprintf("%v", pkValues[pkCols[0]])
	}
	sortedPkCols := make([]string, len(pkCols))
	copy(sortedPkCols, pkCols)
	sort.Strings(sortedPkCols)

	var parts []string
	for _, col := range sortedPkCols {
		parts = append(parts, fmt.Sprintf("%v", pkValues[col]))
	}
	// TODO: Need to revisit this separator
	return strings.Join(parts, "||")
}

func (t *TableDiffTask) fetchRows(ctx context.Context, nodeName string, r Range) ([]map[string]any, error) {
	pool, ok := t.Pools[nodeName]
	if !ok {
		return nil, fmt.Errorf("no pool for node %s", nodeName)
	}

	if len(t.Key) == 0 {
		return nil, fmt.Errorf("primary key not defined for table %s.%s", t.Schema, t.Table)
	}

	quotedSchema := sanitise(t.Schema)
	quotedTable := sanitise(t.Table)
	quotedSchemaTable := fmt.Sprintf("%s.%s", quotedSchema, quotedTable)

	quotedSelectCols := make([]string, len(t.Cols))
	for i, c := range t.Cols {
		quotedSelectCols[i] = sanitise(c)
	}
	selectColsStr := strings.Join(quotedSelectCols, ", ")

	quotedKeyCols := make([]string, len(t.Key))
	for i, k := range t.Key {
		quotedKeyCols[i] = sanitise(k)
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

			conditions = append(conditions, fmt.Sprintf("%s <= %s", pkTupleStr, placeholderTupleStr))
			args = append(args, endVals...)
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = "WHERE " + strings.Join(conditions, " AND ")
	}

	querySQL = fmt.Sprintf("SELECT %s FROM %s %s %s", selectColsStr, quotedSchemaTable, whereClause, orderByClause)

	logger.Debug("[%s] Fetching rows for range: Start=%v, End=%v. SQL: %s, Args: %v", nodeName, r.Start, r.End, querySQL, args)

	pgRows, err := pool.Query(ctx, querySQL, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows for range on node %s (SQL: %s, Args: %v): %w", nodeName, querySQL, args, err)
	}
	defer pgRows.Close()

	var results []map[string]any
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

		rowData := make(map[string]any)
		for i, colD := range colsDesc {
			val := rowValues[i]
			switch v := val.(type) {
			case pgtype.Numeric:
				var fValue float64
				if v.Status == pgtype.Present {
					v.AssignTo(&fValue)
					rowData[string(colD.Name)] = fValue
				} else {
					rowData[string(colD.Name)] = nil
				}
			case pgtype.Timestamp:
				if v.Status == pgtype.Present {
					rowData[string(colD.Name)] = v.Time
				} else {
					rowData[string(colD.Name)] = nil
				}
			case pgtype.Timestamptz:
				if v.Status == pgtype.Present {
					rowData[string(colD.Name)] = v.Time
				} else {
					rowData[string(colD.Name)] = nil
				}
			case pgtype.Date:
				if v.Status == pgtype.Present {
					rowData[string(colD.Name)] = v.Time
				} else {
					rowData[string(colD.Name)] = nil
				}
			case pgtype.Bytea:
				if v.Status == pgtype.Present {
					rowData[string(colD.Name)] = v.Bytes
				} else {
					rowData[string(colD.Name)] = nil
				}
			case pgtype.JSON, pgtype.JSONB:
				if v == nil || v.(interface{ GetStatus() pgtype.Status }).GetStatus() != pgtype.Present {
					rowData[string(colD.Name)] = nil
				} else {
					var dataHolder any
					if assignable, ok := v.(interface{ AssignTo(dst any) error }); ok {
						err := assignable.AssignTo(&dataHolder)
						if err != nil {
							rowData[string(colD.Name)] = nil
						} else {
							rowData[string(colD.Name)] = dataHolder
						}
					} else {
						rowData[string(colD.Name)] = nil
					}
				}
			default:
				rowData[string(colD.Name)] = val
			}
		}
		results = append(results, rowData)
	}
	if err := pgRows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error on node %s: %w", nodeName, err)
	}
	return results, nil
}

func (t *TableDiffTask) compareBlocks(
	ctx context.Context,
	node1, node2 string,
	r Range,
) (*NodePairDiff, error) {
	n1Rows, err := t.fetchRows(ctx, node1, r)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows for node %s in range %v-%v: %w", node1, r.Start, r.End, err)
	}
	n2Rows, err := t.fetchRows(ctx, node2, r)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch rows for node %s in range %v-%v: %w", node2, r.Start, r.End, err)
	}

	lookupN1 := make(map[string]map[string]any)
	for _, row := range n1Rows {
		pkVal := make(map[string]any)
		for _, pkCol := range t.Key {
			pkVal[pkCol] = row[pkCol]
		}
		lookupN1[stringifyKey(pkVal, t.Key)] = row
	}

	lookupN2 := make(map[string]map[string]any)
	for _, row := range n2Rows {
		pkVal := make(map[string]any)
		for _, pkCol := range t.Key {
			pkVal[pkCol] = row[pkCol]
		}
		lookupN2[stringifyKey(pkVal, t.Key)] = row
	}

	diffResult := &NodePairDiff{}

	for pkStr, n1Row := range lookupN1 {
		n2Row, existsInN2 := lookupN2[pkStr]
		if !existsInN2 {
			diffResult.Node1OnlyRows = append(diffResult.Node1OnlyRows, n1Row)
		} else {
			var mismatch bool
			for _, colName := range t.Cols {
				val1, ok1 := n1Row[colName]
				val2, ok2 := n2Row[colName]

				if ok1 != ok2 {
					mismatch = true
					break
				}
				if !ok1 && !ok2 {
					continue
				}

				// TODO: Need to revisit this
				if !reflect.DeepEqual(val1, val2) {
					mismatch = true
					break
				}
			}

			if mismatch {
				diffResult.ModifiedRows = append(diffResult.ModifiedRows, struct {
					Pkey      string
					Node1Data map[string]any
					Node2Data map[string]any
				}{Pkey: pkStr, Node1Data: n1Row, Node2Data: n2Row})
			}
			// Remove from lookupN2 to find elements only in N2 later
			delete(lookupN2, pkStr)
		}
	}

	// Any remaining rows in lookupN2 are only in node2
	for _, n2Row := range lookupN2 {
		diffResult.Node2OnlyRows = append(diffResult.Node2OnlyRows, n2Row)
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

	if t.ConcurrencyFactor > 10 || t.ConcurrencyFactor < 1 {
		return fmt.Errorf("invalid value range for concurrency_factor, must be between 1 and 10")
	}

	if t.Output != "csv" && t.Output != "json" && t.Output != "html" {
		return fmt.Errorf("table-diff currently supports only csv, json and html output formats")
	}

	nodeList, err := ParseNodes(t.Nodes)
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

	err = readClusterInfo(t)
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
	if err := helpers.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := helpers.SanitiseIdentifier(table); err != nil {
		return err
	}

	/*
		We've not eliminated our dependence on the cluster json file just yet.
		So, for convenience, we'll append the chosen db credentials to the
		cluster nodes.
	*/
	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		if len(nodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !Contains(nodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)

		maps.Copy(combinedMap, nodeMap)

		combinedMap["DBName"] = t.Database.DBName
		combinedMap["DBUser"] = t.Database.DBUser
		combinedMap["DBPassword"] = t.Database.DBPassword

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

func (t *TableDiffTask) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	var cols, key []string
	hostMap := make(map[string]string)
	requiredPrivileges := []string{"SELECT"}
	schema := t.Schema
	table := t.Table

	for _, nodeInfo := range t.ClusterNodes {
		hostname, _ := nodeInfo["Name"].(string)
		hostIP, _ := nodeInfo["PublicIP"].(string)
		user, _ := nodeInfo["DBUser"].(string)
		port, _ := nodeInfo["Port"].(float64)
		if port == 0 {
			port = 5432
		}

		if !Contains(t.NodeList, hostname) {
			continue
		}

		conn, err := auth.GetClusterNodeConnection(nodeInfo, t.ClientRole)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", hostname, err)
		}
		defer conn.Close()

		currCols, err := GetColumns(conn, schema, table)
		if err != nil {
			return fmt.Errorf("failed to get columns for table %s.%s on node %s: %w", schema, table, hostname, err)
		}
		if len(currCols) == 0 {
			return fmt.Errorf("table '%s.%s' not found on %s, or the current user does not have adequate privileges", schema, table, hostname)
		}

		currKey, err := GetPrimaryKey(conn, schema, table)
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

		colTypes, err := GetColumnTypes(conn, table)
		if err != nil {
			return fmt.Errorf("failed to get column types for table %s on node %s: %w", table, hostname, err)
		}

		colTypesKey := fmt.Sprintf("%s:%d", hostIP, int(port))

		if t.ColTypes == nil {
			t.ColTypes = make(map[string]map[string]string)
		}
		t.ColTypes[colTypesKey] = colTypes

		authorized, missingPrivileges, err := CheckUserPrivileges(conn, user, schema, table, requiredPrivileges)
		if err != nil {
			return fmt.Errorf("failed to check user privileges on node %s: %w", hostname, err)
		}

		if !authorized {
			var missingPrivs []string
			for _, priv := range requiredPrivileges {
				for missingPriv := range missingPrivileges {
					if strings.HasSuffix(missingPriv, strings.ToLower(priv)) {
						missingPrivs = append(missingPrivs, priv)
					}
				}
			}

			return fmt.Errorf("user \"%s\" does not have the necessary privileges to run %s on table \"%s.%s\" on node \"%s\"",
				user, strings.Join(missingPrivs, ", "), schema, table, hostname)
		}

		hostMap[hostIP+":"+fmt.Sprint(int(port))] = hostname

		if t.TableFilter != "" {
			viewName := fmt.Sprintf("%s_%s_filtered", t.TaskID, table)
			sanitisedViewName := sanitise(viewName)
			sanitisedSchema := sanitise(schema)
			sanitisedTable := sanitise(table)
			viewSQL := fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS SELECT * FROM %s.%s WHERE %s",
				sanitisedViewName, sanitisedSchema, sanitisedTable, t.TableFilter)

			_, err = conn.Exec(context.Background(), viewSQL)
			if err != nil {
				return fmt.Errorf("failed to create filtered view: %w", err)
			}

			hasRowsSQL := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s) AS has_rows", sanitisedViewName)
			var hasRows bool
			err = conn.QueryRow(context.Background(), hasRowsSQL).Scan(&hasRows)
			if err != nil {
				return fmt.Errorf("failed to check if view has rows: %w", err)
			}

			if !hasRows {
				return fmt.Errorf("table filter produced no rows")
			}
		}
	}

	logger.Info("Connections successful to nodes in cluster")

	t.HostMap = hostMap
	t.Cols = cols
	t.Key = key
	t.SimplePrimaryKey = len(key) == 1

	if t.ColTypes != nil && len(t.ColTypes) > 1 {
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

	if t.DiffFilePath != "" {
		if err := CheckDiffFileFormat(t.DiffFilePath, t); err != nil {
			return err
		}
	}

	if t.TableFilter != "" {
		t.Table = fmt.Sprintf("%s_%s_filtered", t.TaskID, t.Table)
	}

	return nil
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
			nodePort, _ := nodeInfo["Port"].(float64)
			if nodePort == 0 {
				nodePort = 5432
			}

			if nodeHost == host && int(nodePort) == port {
				conn, err := auth.GetClusterNodeConnection(nodeInfo, t.ClientRole)
				if err != nil {
					return fmt.Errorf("failed to connect to node %s:%d: %w", host, port, err)
				}
				defer conn.Close()
				pool = conn
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

			avgSize, err := helpers.AvgColumnSize(context.Background(), pool, t.Schema, t.Table, colName)
			logger.Debug("Column %s of table %s.%s has average size %d", colName, t.Schema, t.Table, avgSize)
			if err != nil {
				return fmt.Errorf("failed to check size of bytea column %s: %w", colName, err)
			}

			if avgSize > 1000000 {
				return fmt.Errorf("refusing to perform table-diff. Data in column %s of table %s.%s is larger than 1 MB",
					colName, t.Schema, t.Table)
			}
		}
	}

	return nil
}

func (t *TableDiffTask) ExecuteTask(debugMode bool) error {
	startTime := time.Now()
	logger.SetLevel(LevelInfo)
	if debugMode {
		logger.SetLevel(LevelDebug)
		logger.Info("Debug logging enabled")
	}

	logger.Debug("Using CompareUnitSize: %d", t.CompareUnitSize)

	ctx := context.Background()

	maxConcurrent := runtime.NumCPU() * t.ConcurrencyFactor
	logger.Info("Using %d CPUs, max concurrent workers = %d", runtime.NumCPU(), maxConcurrent)
	sem := make(chan struct{}, maxConcurrent)

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		pool, err := auth.GetClusterNodeConnection(nodeInfo, t.ClientRole)
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", name, err)
		}
		pools[name] = pool
		defer pool.Close()
	}
	t.Pools = pools

	blockHashSQL, err := helpers.BlockHashSQL(t.Schema, t.Table, t.Key)
	if err != nil {
		return fmt.Errorf("failed to build block-hash SQL: %w", err)
	}
	t.BlockHashSQL = blockHashSQL

	schemaName := pgtype.Name{String: t.Schema, Status: pgtype.Present}
	tableName := pgtype.Name{String: t.Table, Status: pgtype.Present}

	var maxCount int
	var maxNode string
	var totalEstimatedRowsAcrossNodes int64

	for name, pool := range pools {
		q := queries.NewQuerier(pool)
		var countPtr *int
		var count int
		// TODO: Estimates cannot be used on views. But we can't run a count(*)
		// on millions of rows either. Need to find a better way to do this.
		if t.TableFilter == "" {
			countPtr, err = q.EstimateRowCount(ctx, schemaName, tableName)
			if err != nil {
				logger.Info("Error getting row count on %s: %v. This might affect range generation.", name, err)
				continue
			}
		} else {
			sanitisedSchema := sanitise(t.Schema)
			sanitisedTable := sanitise(t.Table)
			countQuerySQL := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", sanitisedSchema, sanitisedTable)
			logger.Debug("[%s] Executing count query for filtered table: %s", name, countQuerySQL)
			err = pool.QueryRow(ctx, countQuerySQL).Scan(&count)
			if err != nil {
				return fmt.Errorf("failed to get row count for %s.%s on node %s (query: %s): %w", t.Schema, t.Table, name, countQuerySQL, err)
			}
		}

		if countPtr != nil {
			count = int(*countPtr)
		}
		totalEstimatedRowsAcrossNodes += int64(count)
		logger.Debug("Table contains %d rows (estimated) on %s", count, name)
		if count > maxCount {
			maxCount = count
			maxNode = name
		}
	}
	if maxNode == "" {
		return fmt.Errorf("Unable to determine node with highest row count (or any row counts)")
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
	// 		return fmt.Errorf("failed to get pkey offsets directly: %w", err)
	// 	}
	// 	ranges = r
	// } else {
	ntileCount := int(math.Ceil(float64(maxCount) / float64(t.BlockSize)))
	if ntileCount == 0 && maxCount > 0 {
		ntileCount = 1
	}

	querySQL, err := helpers.GeneratePkeyOffsetsQuery(t.Schema, t.Table, t.Key, sampleMethod, samplePercent, ntileCount)
	logger.Debug("Generated offsets query: %s", querySQL)
	if err != nil {
		return fmt.Errorf("failed to generate offsets query: %w", err)
	}
	pkRangesRows, err := pools[maxNode].Query(ctx, querySQL)
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
			rEnd = endKeyParts
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

	logger.Info("Created %d initial ranges to compare", len(ranges))
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
	p := mpb.New()
	bar := p.AddBar(int64(totalHashTasks),
		mpb.PrependDecorators(
			decor.Name("Hashing ranges: ", decor.WC{W: 18}),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
			decor.Name(" | "),
			decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
		),
	)

	/*
		So, here's the approach I'm using:
		1. Generate a list of ranges to hash, and create a HashTask for each range.
		2. Each HashTask is independent of the others, and can be executed in parallel.
		3. I don't immediately perform the comparisons, but instead store the results in a map.
		4. Once they're ready, I use a binary search approach to narrow down the ranges that have mismatches.
	*/
	hashTaskQueue := make(chan HashTask, totalHashTasks)
	var initialHashWg sync.WaitGroup
	for i := 0; i < maxConcurrent; i++ {
		initialHashWg.Add(1)
		go func() {
			defer initialHashWg.Done()
			for task := range hashTaskQueue {
				sem <- struct{}{}
				queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
				hashValue, hErr := t.hashRange(queryCtx, task.nodeName, task.r)
				cancel()
				<-sem

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
	p.Wait()

	logger.Info("Initial hash calculations complete. Proceeding with comparisons for mismatches...")

	var diffWg sync.WaitGroup
	// var mismatchedRangesCountAtomic int32

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
					logger.Debug("✗ Mismatch in initial range %d (%v-%v) for %s vs %s. Hashes: %s... / %s... narrowing down diffs...",
						rangeIdx, currentRange.Start, currentRange.End, node1, node2, safeCut(r1.hash, 8), safeCut(r2.hash, 8))
					diffWg.Add(1)
					go t.recursiveDiff(ctx, RecursiveDiffTask{
						Node1Name:                 node1,
						Node2Name:                 node2,
						CurrentRange:              currentRange,
						CurrentEstimatedBlockSize: t.BlockSize,
					}, &diffWg)
				} else {
					logger.Debug("✓ Match in initial range %d (%v-%v) for %s vs %s", rangeIdx, currentRange.Start, currentRange.End, node1, node2)
				}
			}
		}
	}

	diffWg.Wait()
	// t.DiffResult.Summary.MismatchedRangesCount = int(mismatchedRangesCountAtomic)

	logger.Info("Table diff comparison completed for %s", t.QualifiedTableName)

	endTime := time.Now()
	t.DiffResult.Summary.EndTime = endTime.Format(time.RFC3339)
	t.DiffResult.Summary.TimeTaken = endTime.Sub(startTime).String()

	if len(t.DiffResult.NodeDiffs) > 0 {
		outputFileName := fmt.Sprintf("%s_%s_diffs-%s.json",
			strings.ReplaceAll(t.Schema, ".", "_"),
			strings.ReplaceAll(t.Table, ".", "_"),
			time.Now().Format("20060102150405"),
		)

		jsonData, err := json.MarshalIndent(t.DiffResult, "", "  ")
		if err != nil {
			logger.Info("ERROR marshalling diff output to JSON: %v", err)
			return fmt.Errorf("failed to marshal diffs: %w", err)
		}

		err = os.WriteFile(outputFileName, jsonData, 0644)
		if err != nil {
			logger.Info("ERROR writing diff output to file %s: %v", outputFileName, err)
			return fmt.Errorf("failed to write diffs file: %w", err)
		}
		logger.Info("Diff report written to %s", outputFileName)
	} else {
		logger.Info("No differences found. Diff file not created.")
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

	skipMinCheck := r.Start == nil
	skipMaxCheck := r.End == nil

	numPKCols := len(t.Key)
	sqlArgs := make([]any, 0, 2+2*numPKCols)

	sqlArgs = append(sqlArgs, skipMinCheck)

	if r.Start == nil {
		for i := 0; i < numPKCols; i++ {
			sqlArgs = append(sqlArgs, nil)
		}
	} else {
		if numPKCols == 1 {
			sqlArgs = append(sqlArgs, r.Start)
		} else {
			startVals, ok := r.Start.([]any)
			if !ok || len(startVals) != numPKCols {
				return "", fmt.Errorf("[%s] r.Start is not a valid composite key for hashing (expected %d values, got %T with value %v)", node, numPKCols, r.Start, r.Start)
			}
			sqlArgs = append(sqlArgs, startVals...)
		}
	}

	sqlArgs = append(sqlArgs, skipMaxCheck)

	if r.End == nil {
		for i := 0; i < numPKCols; i++ {
			sqlArgs = append(sqlArgs, nil)
		}
	} else {
		if numPKCols == 1 {
			sqlArgs = append(sqlArgs, r.End)
		} else {
			endVals, ok := r.End.([]any)
			if !ok || len(endVals) != numPKCols {
				return "", fmt.Errorf("[%s] r.End is not a valid composite key for hashing (expected %d values, got %T with value %v)", node, numPKCols, r.End, r.End)
			}
			sqlArgs = append(sqlArgs, endVals...)
		}
	}

	logger.Debug("[%s] Hashing range: Start=%v, End=%v. SQL: %s, Args: %v", node, r.Start, r.End, t.BlockHashSQL, sqlArgs)

	err := pool.QueryRow(ctx, t.BlockHashSQL, sqlArgs...).Scan(&hash)

	if err != nil {
		duration := time.Since(startTime)
		logger.Info("[%s] ERROR after %v for range Start=%v, End=%v (using query: '%s', args: %v): %v", node, duration, r.Start, r.End, t.BlockHashSQL, sqlArgs, err)
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

func (t *TableDiffTask) generateSubRanges(
	ctx context.Context,
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
		quotedKeyCols[i] = sanitise(k)
	}
	pkColsStr := strings.Join(quotedKeyCols, ", ")
	pkTupleStr := fmt.Sprintf("ROW(%s)", pkColsStr)
	schemaTable := fmt.Sprintf("%s.%s", sanitise(t.Schema), sanitise(t.Table))

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
	err := pool.QueryRow(ctx, countQuery, args...).Scan(&count)
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
			err = pool.QueryRow(ctx, medianQuery, medianQueryArgs...).Scan(&medianPKVal)
		} else {
			scanDest := make([]any, numPKCols)
			scanDestPtrs := make([]any, numPKCols)
			for i := range scanDest {
				scanDestPtrs[i] = &scanDest[i]
			}
			err = pool.QueryRow(ctx, medianQuery, medianQueryArgs...).Scan(scanDestPtrs...)
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

	node1Name := task.Node1Name
	node2Name := task.Node2Name
	currentRange := task.CurrentRange
	currentEstimatedBlockSize := task.CurrentEstimatedBlockSize

	finalCompareUnitSize := t.CompareUnitSize
	if finalCompareUnitSize < 1 {
		finalCompareUnitSize = 1
	}

	isSmallEnough := currentEstimatedBlockSize <= finalCompareUnitSize
	if !isSmallEnough && currentRange.Start != nil && currentRange.End != nil && reflect.DeepEqual(currentRange.Start, currentRange.End) {
		isSmallEnough = true
	}

	if isSmallEnough {
		logger.Debug("[%s vs %s] Range %v-%v (est. size %d) is <= compare_unit_size %d. Fetching rows.",
			node1Name, node2Name, currentRange.Start, currentRange.End, currentEstimatedBlockSize, finalCompareUnitSize)

		diffInfo, err := t.compareBlocks(ctx, node1Name, node2Name, currentRange)
		if err != nil {
			logger.Info("ERROR during fetchAndCompareRows for %s/%s, range %v-%v: %v", node1Name, node2Name, currentRange.Start, currentRange.End, err)
			return
		}

		pairKey := node1Name + "/" + node2Name
		if strings.Compare(node1Name, node2Name) > 0 {
			pairKey = node2Name + "/" + node1Name
		}

		var currentDiffRowsForPair int
		if diffInfo != nil && (len(diffInfo.Node1OnlyRows) > 0 || len(diffInfo.Node2OnlyRows) > 0 || len(diffInfo.ModifiedRows) > 0) {
			t.diffMutex.Lock()

			if _, ok := t.DiffResult.NodeDiffs[pairKey]; !ok {
				t.DiffResult.NodeDiffs[pairKey] = types.DiffByNodePair{
					Rows: make(map[string][]map[string]any),
				}
			}

			if _, ok := t.DiffResult.NodeDiffs[pairKey].Rows[node1Name]; !ok {
				t.DiffResult.NodeDiffs[pairKey].Rows[node1Name] = []map[string]any{}
			}
			if _, ok := t.DiffResult.NodeDiffs[pairKey].Rows[node2Name]; !ok {
				t.DiffResult.NodeDiffs[pairKey].Rows[node2Name] = []map[string]any{}
			}

			for _, row := range diffInfo.Node1OnlyRows {
				t.DiffResult.NodeDiffs[pairKey].Rows[node1Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node1Name], row)
				currentDiffRowsForPair++
			}
			for _, row := range diffInfo.Node2OnlyRows {
				t.DiffResult.NodeDiffs[pairKey].Rows[node2Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node2Name], row)
				currentDiffRowsForPair++
			}
			for _, modRow := range diffInfo.ModifiedRows {
				t.DiffResult.NodeDiffs[pairKey].Rows[node1Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node1Name], modRow.Node1Data)
				t.DiffResult.NodeDiffs[pairKey].Rows[node2Name] = append(t.DiffResult.NodeDiffs[pairKey].Rows[node2Name], modRow.Node2Data)
				currentDiffRowsForPair++
			}

			if t.DiffResult.Summary.DiffRowsCount == nil {
				t.DiffResult.Summary.DiffRowsCount = make(map[string]int)
			}
			t.DiffResult.Summary.DiffRowsCount[pairKey] += currentDiffRowsForPair
			t.diffMutex.Unlock()
		}
		return
	}

	subRanges, err := t.generateSubRanges(ctx, node1Name, currentRange, 2)
	if err != nil {
		logger.Info("ERROR generating sub-ranges for %s/%s, range %v-%v: %v. Stopping recursion for this path.",
			node1Name, node2Name, currentRange.Start, currentRange.End, err)
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
			logger.Info("ERROR hashing sub-range %v-%v for %s: %v", sr.Start, sr.End, node1Name, res1.err)
			continue
		}
		if res2.err != nil {
			logger.Info("ERROR hashing sub-range %v-%v for %s: %v", sr.Start, sr.End, node2Name, res2.err)
			continue
		}

		if res1.hash != res2.hash {
			logger.Debug("✗ Mismatch in sub-range %v-%v for %s (%s...) vs %s (%s...). Recursing.",
				sr.Start, sr.End, node1Name, safeCut(res1.hash, 8), node2Name, safeCut(res2.hash, 8))
			wg.Add(1)
			go t.recursiveDiff(ctx, RecursiveDiffTask{
				Node1Name:                 node1Name,
				Node2Name:                 node2Name,
				CurrentRange:              sr,
				CurrentEstimatedBlockSize: newEstimatedBlockSize,
			}, wg)
		} else {
			logger.Debug("✓ Match in sub-range %v-%v for %s vs %s.", sr.Start, sr.End, node1Name, node2Name)
		}
	}
}

func safeCut(s string, n int) string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

func (t *TableDiffTask) getPkeyOffsets(ctx context.Context, pool *pgxpool.Pool) ([]Range, error) {
	if len(t.Key) == 0 {
		return nil, fmt.Errorf("primary key not defined for table %s.%s", t.Schema, t.Table)
	}

	schemaIdent := sanitise(t.Schema)
	tableIdent := sanitise(t.Table)

	quotedKeyCols := make([]string, len(t.Key))
	for i, k := range t.Key {
		quotedKeyCols[i] = sanitise(k)
	}
	// Using this for both select and order by to keep things simple
	pkStr := strings.Join(quotedKeyCols, ", ")

	querySQL := fmt.Sprintf("SELECT %s FROM %s.%s ORDER BY %s", pkStr, schemaIdent, tableIdent, pkStr)

	pgRows, err := pool.Query(ctx, querySQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary keys for direct offset generation from %s.%s: %w", t.Schema, t.Table, err)
	}
	defer pgRows.Close()

	var allPks []any
	numPKCols := len(t.Key)
	scanDest := make([]any, numPKCols)
	scanDestPtrs := make([]any, numPKCols)
	for i := range scanDest {
		scanDestPtrs[i] = &scanDest[i]
	}

	for pgRows.Next() {
		if err := pgRows.Scan(scanDestPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan primary key value from %s.%s: %w", t.Schema, t.Table, err)
		}
		if numPKCols == 1 {
			allPks = append(allPks, scanDest[0])
		} else {
			/* PKs are composite, so create a new slice for each PK to avoid allPks
			 * elements pointing to the same underlying scanDest array.
			 */
			currentPkComposite := make([]any, numPKCols)
			copy(currentPkComposite, scanDest)
			allPks = append(allPks, currentPkComposite)
		}
	}

	if err := pgRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over primary key rows from %s.%s: %w", t.Schema, t.Table, err)
	}

	if len(allPks) == 0 {
		logger.Info("[%s.%s] No primary key values found, returning empty ranges for direct generation.", t.Schema, t.Table)
		return []Range{}, nil
	}

	var ranges []Range
	// As always the case, our first range needs to be (NULL, first_pkey)
	ranges = append(ranges, Range{Start: nil, End: allPks[0]})

	currentPkIndex := 0
	for currentPkIndex < len(allPks) {
		currentBlockStartPkey := allPks[currentPkIndex]

		nextPKeyIndexForRangeEnd := currentPkIndex + t.BlockSize

		if nextPKeyIndexForRangeEnd < len(allPks) {
			rangeEndValue := allPks[nextPKeyIndexForRangeEnd]
			ranges = append(ranges, Range{Start: currentBlockStartPkey, End: rangeEndValue})
			currentPkIndex = nextPKeyIndexForRangeEnd
		} else {
			if !(currentPkIndex == 0 && len(allPks) <= t.BlockSize && len(allPks) == 1) {
				ranges = append(ranges, Range{Start: currentBlockStartPkey, End: nil})
			}
			break
		}
	}

	logger.Debug("[%s.%s] Generated %d ranges without sampling from %d pkeys with block_size %d.", t.Schema, t.Table, len(ranges), len(allPks), t.BlockSize)
	return ranges, nil
}
