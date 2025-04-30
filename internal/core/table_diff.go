package core

import (
	"context"
	"fmt"
	"log"
	"maps"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/helpers"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/types"
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

type TableDiffTask struct {
	types.Task
	types.DerivedFields
	TableName string
	DBName    string
	Nodes     string

	BlockSize   int
	MaxCPURatio float64
	Output      string
	BatchSize   int
	TableFilter string
	QuietMode   bool

	Mode              string
	OverrideBlockSize bool

	DiffFilePath string

	InvokeMethod string
	ClientRole   string

	ConnectionPool *pgxpool.Pool

	DiffSummary map[string]string

	SkipDBUpdate bool

	Pools map[string]*pgxpool.Pool
}

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

type Range struct {
	Start interface{}
	End   interface{}
}

func (t *TableDiffTask) Validate() error {
	if t.ClusterName == "" || t.TableName == "" {
		return fmt.Errorf("cluster_name and table_name are required arguments")
	}

	if t.BlockSize > config.Cfg.TableDiff.MaxBlockSize && !t.OverrideBlockSize {
		return fmt.Errorf("block row size should be <= %d", config.Cfg.TableDiff.MaxBlockSize)
	}
	if t.BlockSize < config.Cfg.TableDiff.MinBlockSize && !t.OverrideBlockSize {
		return fmt.Errorf("block row size should be >= %d", config.Cfg.TableDiff.MinBlockSize)
	}

	if t.MaxCPURatio > 1.0 || t.MaxCPURatio < 0.0 {
		return fmt.Errorf("invalid value range for max_cpu_ratio, must be between 0.0 and 1.0")
	}

	if t.Output != "csv" && t.Output != "json" && t.Output != "html" {
		return fmt.Errorf("table-diff currently supports only csv, json and html output formats")
	}

	nodeList, err := ParseNodes(t.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. E.g., nodes=\"n1,n2\". Error: %w", err)
	}

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

	parts := strings.Split(t.TableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("tableName %s must be of form 'schema.table_name'", t.TableName)
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
			nameVal, _ := nodeMap["name"].(string)
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
				if name, ok := node["name"].(string); ok && name == n {
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
	// t.NodeList = nodeList
	// t.DerivedFields.Database = database["db_name"].(string)
	t.ClusterNodes = clusterNodes

	// b, err := json.MarshalIndent(t.ClusterNodes, "", "  ")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// fmt.Println(string(b))

	return nil
}

func (t *TableDiffTask) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := t.Validate(); err != nil {
			return err
		}
	}

	var cols, key []string
	// connParams := []map[string]interface{}{}
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

		// connParams = append(connParams, params)
		hostMap[hostIP+":"+fmt.Sprint(int(port))] = hostname

		if t.TableFilter != "" {
			viewName := fmt.Sprintf("%s_%s_filtered", t.TaskID, table)
			viewSQL := fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM %s.%s WHERE %s",
				viewName, schema, table, t.TableFilter)

			_, err = conn.Exec(context.Background(), viewSQL)
			if err != nil {
				return fmt.Errorf("failed to create filtered view: %w", err)
			}

			hasRowsSQL := fmt.Sprintf("SELECT EXISTS (SELECT 1 FROM %s) AS has_rows", viewName)
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

	logger.Info("Table %s is comparable across nodes", t.TableName)

	// // TODO: add this back later
	// if err := t.CheckColumnSize(); err != nil {
	// 	return err
	// }

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

// func (t *TableDiffTask) CheckColumnSize() error {
// 	for hostPort, types := range t.ColTypes {
// 		parts := strings.Split(hostPort, ":")
// 		if len(parts) != 2 {
// 			continue
// 		}

// 		host, portStr := parts[0], parts[1]
// 		port, _ := strconv.Atoi(portStr)

// 		var pool *pgxpool.Pool
// 		for _, nodeInfo := range t.ClusterNodes {
// 			nodeHost, _ := nodeInfo["public_ip"].(string)
// 			nodePort, _ := nodeInfo["port"].(float64)
// 			if nodePort == 0 {
// 				nodePort = 5432
// 			}

// 			if nodeHost == host && int(nodePort) == port {
// 				conn, err := auth.GetClusterNodeConnection(nodeInfo, t.ClientRole)
// 				if err != nil {
// 					return fmt.Errorf("failed to connect to node %s:%d: %w", host, port, err)
// 				}
// 				defer conn.Close()
// 				pool = conn
// 				break
// 			}
// 		}

// 		if pool == nil {
// 			continue
// 		}

// 		for colName, colType := range types {
// 			if !strings.Contains(colType, "bytea") {
// 				continue
// 			}

// 			var avgSize int64
// 			q := queries.NewQuerier(pool)
// 			err := q.CheckColumnSize(context.Background(), queries.CheckColumnSizeParams{
// 				Schema: t.Schema,
// 				Table:  t.Table,
// 				Column: colName,
// 			})
// 			if err != nil {
// 				return fmt.Errorf("failed to check size of bytea column %s: %w", colName, err)
// 			}

// 			if avgSize > 1000000 {
// 				return fmt.Errorf("refusing to perform table-diff. Data in column %s of table %s.%s is larger than 1 MB",
// 					colName, t.Schema, t.Table)
// 			}
// 		}
// 	}

// 	return nil
// }

func (t *TableDiffTask) ExecuteTask() error {
	logger.SetLevel(LevelInfo)
	if len(os.Args) > 1 && os.Args[1] == "--debug" {
		logger.SetLevel(LevelDebug)
		logger.Info("Debug logging enabled")
	}

	ctx := context.Background()

	maxConcurrent := runtime.NumCPU() * config.Cfg.TableDiff.ConcurrencyFactor
	logger.Info("Using %d CPUs, max concurrent workers = %d", runtime.NumCPU(), maxConcurrent)
	sem := make(chan struct{}, maxConcurrent)

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		pool, err := auth.GetClusterNodeConnection(nodeInfo, t.ClientRole)
		if err != nil {
			logger.Info("Failed to connect to node %s: %v", name, err)
			continue
		}
		pools[name] = pool
		defer pool.Close()
	}

	t.Pools = pools

	schema, table := t.Schema, t.Table
	schemaName := pgtype.Name{String: schema, Status: pgtype.Present}
	tableName := pgtype.Name{String: table, Status: pgtype.Present}

	var maxCount int
	var maxNode string
	for name, pool := range pools {
		q := queries.NewQuerier(pool)
		countPtr, err := q.EstimateRowCount(ctx, schemaName, tableName)
		if err != nil {
			logger.Info("Error getting row count on %s: %v", name, err)
			continue
		}
		count := int(*countPtr)
		logger.Info("Table contains %d rows (estimated) on %s", count, name)
		if count > maxCount {
			maxCount = count
			maxNode = name
		}
	}
	if maxNode == "" {
		return fmt.Errorf("unable to determine node with highest row count")
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
	ntileCount := int(math.Ceil(float64(maxCount) / float64(t.BlockSize)))

	querySQL, err := helpers.GeneratePkeyOffsetsQuery(schema, table, t.Key, sampleMethod, samplePercent, ntileCount)
	if err != nil {
		return fmt.Errorf("failed to generate offsets query: %w", err)
	}
	rows, err := pools[maxNode].Query(ctx, querySQL)
	if err != nil {
		return fmt.Errorf("offsets query execution failed on %s: %w", maxNode, err)
	}
	defer rows.Close()

	var ranges []Range
	for rows.Next() {
		var startVal, endVal interface{}
		if err := rows.Scan(&startVal, &endVal); err != nil {
			return fmt.Errorf("scanning offset row failed: %w", err)
		}
		ranges = append(ranges, Range{Start: startVal, End: endVal})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("offset rows iteration error: %w", err)
	}
	logger.Info("Created %d ranges to compare", len(ranges))

	for _, name := range t.NodeList {
		if len(ranges) > 0 {
			if _, err := t.hashRange(ctx, name, ranges[0]); err != nil {
				logger.Info("Warning: warmup failed for %s: %v", name, err)
			}
		}
	}

	var names []string
	for name := range pools {
		names = append(names, name)
	}
	sort.Strings(names)
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			t.comparePair(ctx, names[i], names[j], ranges, sem)
		}
	}
	return nil
}

func (t *TableDiffTask) comparePair(
	ctx context.Context,
	name1 string,
	name2 string,
	ranges []Range, sem chan struct{},
) {
	var wg sync.WaitGroup
	for idx, r := range ranges {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, r Range) {
			defer wg.Done()
			defer func() { <-sem }()

			logger.Debug("[%s/%s] Processing range %d-%d", name1, name2, r.Start, r.End)
			queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			type res struct {
				hash string
				err  error
			}
			ch1, ch2 := make(chan res, 1), make(chan res, 1)
			go func() {
				h, err := t.hashRange(queryCtx, name1, r)
				ch1 <- res{h, err}
			}()
			go func() {
				h, err := t.hashRange(queryCtx, name2, r)
				ch2 <- res{h, err}
			}()
			r1, r2 := <-ch1, <-ch2
			if r1.err != nil {
				logger.Info("ERROR on %s/%s range %d-%d: %v", name1, name2, r.Start, r.End, r1.err)
				return
			}
			if r2.err != nil {
				logger.Info("ERROR on %s/%s range %d-%d: %v", name1, name2, r.Start, r.End, r2.err)
				return
			}
			if r1.hash != r2.hash {
				logger.Info("✗ %s/%s range %d-%d mismatch: %s vs %s", name1, name2, r.Start, r.End, r1.hash[:8], r2.hash[:8])
			} else {
				logger.Debug("✓ %s/%s range %d-%d match", name1, name2, r.Start, r.End)
			}
		}(idx, r)
	}
	wg.Wait()
	logger.Info("Completed comparison for %s vs %s", name1, name2)
}

func (t *TableDiffTask) hashRange(ctx context.Context, node string, r Range) (string, error) {
	pool, ok := t.Pools[node]
	if !ok {
		return "", fmt.Errorf("no pool for node %s", node)
	}
	startTime := time.Now()
	hash, err := helpers.BlockHash(ctx, pool, t.Schema, t.Table, t.Cols, t.Key[0], r.Start, r.End)
	if err != nil {
		return "", fmt.Errorf("BlockHash failed for %s: %w", node, err)
	}
	if dur := time.Since(startTime); dur > 200*time.Millisecond {
		logger.Debug("[%s] Slow query? Range %d-%d took %v", node, r.Start, r.End, dur)
	}
	return hash, nil
}
