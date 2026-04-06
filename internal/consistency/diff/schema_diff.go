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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
)

type SchemaDiffCmd struct {
	types.Task

	ClusterName       string
	DBName            string
	SchemaName        string
	Nodes             string
	Quiet             bool
	SkipTables        string
	SkipFile          string
	DDLOnly           bool
	skipTablesList    []string
	tableList         []string
	missingTables     []MissingTableInfo
	nodeList          []string
	clusterNodes      []map[string]any
	database          types.Database
	ConnectionPool    *pgxpool.Pool
	ConcurrencyFactor float64
	MaxConnections    int
	BlockSize         int
	CompareUnitSize   int
	Output            string
	TableFilter       string
	OverrideBlockSize bool
	Ctx               context.Context

	SkipDBUpdate  bool
	TaskStore     *taskstore.Store
	TaskStorePath string
}

type SchemaObjects struct {
	Tables    []string `json:"tables"`
	Views     []string `json:"views"`
	Functions []string `json:"functions"`
	Indices   []string `json:"indices"`
}

func (so SchemaObjects) IsEmpty() bool {
	return len(so.Tables) == 0 && len(so.Views) == 0 && len(so.Functions) == 0 && len(so.Indices) == 0
}

type NodeSchemaReport struct {
	NodeName string        `json:"node_name"`
	Objects  SchemaObjects `json:"objects"`
}

type NodeComparisonReport struct {
	Status string              `json:"status"`
	Diffs  map[string]NodeDiff `json:"diffs,omitempty"`
}

type NodeDiff struct {
	MissingObjects SchemaObjects `json:"missing_objects"`
	ExtraObjects   SchemaObjects `json:"extra_objects"`
}

func (c *SchemaDiffCmd) GetClusterName() string              { return c.ClusterName }
func (c *SchemaDiffCmd) GetDBName() string                   { return c.DBName }
func (c *SchemaDiffCmd) SetDBName(name string)               { c.DBName = name }
func (c *SchemaDiffCmd) GetNodes() string                    { return c.Nodes }
func (c *SchemaDiffCmd) GetNodeList() []string               { return c.nodeList }
func (c *SchemaDiffCmd) SetNodeList(nodes []string)          { c.nodeList = nodes }
func (c *SchemaDiffCmd) SetDatabase(db types.Database)       { c.database = db }
func (c *SchemaDiffCmd) GetClusterNodes() []map[string]any   { return c.clusterNodes }
func (c *SchemaDiffCmd) SetClusterNodes(cn []map[string]any) { c.clusterNodes = cn }

func NewSchemaDiffTask() *SchemaDiffCmd {
	return &SchemaDiffCmd{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskType:   taskstore.TaskTypeSchemaDiff,
			TaskStatus: taskstore.StatusPending,
		},
		Ctx: context.Background(),
	}
}

func (c *SchemaDiffCmd) parseSkipList() error {
	var raw []string
	if c.SkipTables != "" {
		raw = append(raw, strings.Split(c.SkipTables, ",")...)
	}
	if c.SkipFile != "" {
		file, err := os.Open(c.SkipFile)
		if err != nil {
			return fmt.Errorf("could not open skip file: %w", err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			raw = append(raw, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading skip file: %w", err)
		}
	}

	// Normalize entries: accept both "table" and "schema.table" forms.
	// If schema-qualified, the schema must match c.SchemaName.
	tables := make([]string, 0, len(raw))
	for _, entry := range raw {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if parts := strings.SplitN(entry, ".", 2); len(parts) == 2 {
			if parts[0] != c.SchemaName {
				return fmt.Errorf("skip table %q: schema %q does not match target schema %q",
					entry, parts[0], c.SchemaName)
			}
			entry = strings.TrimSpace(parts[1])
			if entry == "" {
				return fmt.Errorf("skip table %q: missing table name after schema qualifier", parts[0]+".")
			}
		}
		tables = append(tables, entry)
	}
	c.skipTablesList = tables
	return nil
}

func (c *SchemaDiffCmd) Validate() error {
	if c.ClusterName == "" {
		return fmt.Errorf("cluster name is required")
	}
	if c.SchemaName == "" {
		return fmt.Errorf("schema name is required")
	}

	nodeList, err := utils.ParseNodes(c.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. E.g., nodes=\"n1,n2\". Error: %w", err)
	}
	c.SetNodeList(nodeList)

	if len(nodeList) > 3 {
		return fmt.Errorf("schema-diff currently supports up to a three-way schema comparison")
	}

	if c.Nodes != "all" && len(nodeList) == 1 {
		return fmt.Errorf("schema-diff needs at least two nodes to compare")
	}

	return nil
}

func (c *SchemaDiffCmd) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := c.Validate(); err != nil {
			return err
		}
	}

	if err := c.parseSkipList(); err != nil {
		return err
	}

	if err := utils.ReadClusterInfo(c); err != nil {
		return err
	}
	if len(c.clusterNodes) == 0 {
		return fmt.Errorf("no nodes found in cluster config")
	}

	// Query tables from every node and build a union.
	nodeNames := make([]string, 0, len(c.clusterNodes))
	tablePresence := make(map[string]map[string]bool) // table -> {nodeName: true}

	for _, nodeInfo := range c.clusterNodes {
		nodeName := nodeInfo["Name"].(string)
		nodeNames = append(nodeNames, nodeName)

		nodeWithDBInfo := make(map[string]any)
		maps.Copy(nodeWithDBInfo, nodeInfo)
		utils.ApplyDatabaseCredentials(nodeWithDBInfo, c.database)
		if portVal, ok := nodeWithDBInfo["Port"]; ok {
			if portFloat, isFloat := portVal.(float64); isFloat {
				nodeWithDBInfo["Port"] = strconv.Itoa(int(portFloat))
			}
		}

		pool, err := auth.GetClusterNodeConnection(c.Ctx, nodeWithDBInfo, auth.ConnectionOptions{})
		if err != nil {
			return fmt.Errorf("could not connect to node %s: %w", nodeName, err)
		}

		schemaExists, err := queries.CheckSchemaExists(c.Ctx, pool, c.SchemaName)
		if err != nil {
			pool.Close()
			return fmt.Errorf("could not check if schema exists on node %s: %w", nodeName, err)
		}
		if !schemaExists {
			pool.Close()
			return fmt.Errorf("schema %s not found on node %s", c.SchemaName, nodeName)
		}

		tables, err := queries.GetTablesInSchema(c.Ctx, pool, c.SchemaName)
		pool.Close()
		if err != nil {
			return fmt.Errorf("could not get tables in schema on node %s: %w", nodeName, err)
		}

		for _, t := range tables {
			if tablePresence[t] == nil {
				tablePresence[t] = make(map[string]bool)
			}
			tablePresence[t][nodeName] = true
		}
	}

	// Partition into common (all nodes) vs partial (some nodes).
	var commonTables []string
	var missingTables []MissingTableInfo
	for table, presence := range tablePresence {
		if len(presence) == len(nodeNames) {
			commonTables = append(commonTables, table)
		} else {
			var presentOn, missingFrom []string
			for _, n := range nodeNames {
				if presence[n] {
					presentOn = append(presentOn, n)
				} else {
					missingFrom = append(missingFrom, n)
				}
			}
			missingTables = append(missingTables, MissingTableInfo{
				Table:       fmt.Sprintf("%s.%s", c.SchemaName, table),
				PresentOn:   presentOn,
				MissingFrom: missingFrom,
			})
		}
	}
	sort.Strings(commonTables)
	sort.Slice(missingTables, func(i, j int) bool {
		return missingTables[i].Table < missingTables[j].Table
	})

	c.tableList = commonTables
	c.missingTables = missingTables

	if len(c.tableList) == 0 && len(c.missingTables) == 0 {
		return fmt.Errorf("no tables found in schema %s", c.SchemaName)
	}

	return nil
}

func (task *SchemaDiffCmd) schemaObjectDiff() error {
	var allNodeObjects []NodeSchemaReport

	for _, nodeInfo := range task.clusterNodes {
		nodeName := nodeInfo["Name"].(string)
		nodeWithDBInfo := make(map[string]any)
		maps.Copy(nodeWithDBInfo, nodeInfo)
		utils.ApplyDatabaseCredentials(nodeWithDBInfo, task.database)
		if portVal, ok := nodeWithDBInfo["Port"]; ok {
			if portFloat, isFloat := portVal.(float64); isFloat {
				nodeWithDBInfo["Port"] = strconv.Itoa(int(portFloat))
			}
		}

		pool, err := auth.GetClusterNodeConnection(task.Ctx, nodeWithDBInfo, auth.ConnectionOptions{})
		if err != nil {
			logger.Warn("could not connect to node %s: %v. Skipping.", nodeName, err)
			continue
		}
		defer pool.Close()

		objects, err := getObjectsForSchema(task.Ctx, pool, task.SchemaName)
		if err != nil {
			logger.Warn("could not get schema objects for node %s: %v. Skipping.", nodeName, err)
			continue
		}

		allNodeObjects = append(allNodeObjects, NodeSchemaReport{
			NodeName: nodeName,
			Objects:  *objects,
		})
	}

	if len(allNodeObjects) < 2 {
		fmt.Println("{\"status\": \"Not enough nodes to compare (at least 2 required).\"}")
		return nil
	}

	finalReport := make(map[string]NodeComparisonReport)
	for i := 0; i < len(allNodeObjects); i++ {
		for j := i + 1; j < len(allNodeObjects); j++ {
			referenceNode := allNodeObjects[i]
			compareNode := allNodeObjects[j]

			refObjects := referenceNode.Objects
			cmpObjects := compareNode.Objects

			missingTables, extraTables := utils.DiffStringSlices(refObjects.Tables, cmpObjects.Tables)
			missingViews, extraViews := utils.DiffStringSlices(refObjects.Views, cmpObjects.Views)
			missingFunctions, extraFunctions := utils.DiffStringSlices(refObjects.Functions, cmpObjects.Functions)
			missingIndices, extraIndices := utils.DiffStringSlices(refObjects.Indices, cmpObjects.Indices)

			refExtraObjects := SchemaObjects{
				Tables:    missingTables,
				Views:     missingViews,
				Functions: missingFunctions,
				Indices:   missingIndices,
			}
			refMissingObjects := SchemaObjects{
				Tables:    extraTables,
				Views:     extraViews,
				Functions: extraFunctions,
				Indices:   extraIndices,
			}

			comparisonKey := fmt.Sprintf("%s/%s", referenceNode.NodeName, compareNode.NodeName)

			var report NodeComparisonReport
			if refMissingObjects.IsEmpty() && refExtraObjects.IsEmpty() {
				report = NodeComparisonReport{
					Status: "IDENTICAL",
				}
			} else {
				report = NodeComparisonReport{
					Status: "MISMATCH",
					Diffs: map[string]NodeDiff{
						referenceNode.NodeName: {
							MissingObjects: refMissingObjects,
							ExtraObjects:   refExtraObjects,
						},
						compareNode.NodeName: {
							MissingObjects: refExtraObjects,
							ExtraObjects:   refMissingObjects,
						},
					},
				}
			}
			finalReport[comparisonKey] = report
		}
	}

	output, err := json.MarshalIndent(finalReport, "", "  ")
	if err != nil {
		return fmt.Errorf("could not marshal diff to json: %w", err)
	}
	fmt.Println(string(output))

	return nil
}

func (task *SchemaDiffCmd) SchemaTableDiff() (err error) {
	if err := task.RunChecks(false); err != nil {
		return err
	}

	startTime := time.Now()

	if strings.TrimSpace(task.TaskID) == "" {
		task.TaskID = uuid.NewString()
	}
	if task.Task.TaskType == "" {
		task.Task.TaskType = taskstore.TaskTypeSchemaDiff
	}
	task.Task.StartedAt = startTime
	task.Task.TaskStatus = taskstore.StatusRunning
	task.Task.ClusterName = task.ClusterName

	var recorder *taskstore.Recorder
	if !task.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(task.TaskStore, task.TaskStorePath)
		if recErr != nil {
			logger.Warn("schema-diff: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if task.TaskStore == nil && rec.Store() != nil {
				task.TaskStore = rec.Store()
			}

			ctx := map[string]any{
				"schema":       task.SchemaName,
				"ddl_only":     task.DDLOnly,
				"table_filter": task.TableFilter,
				"tables_total": len(task.tableList),
				"skip_tables":  task.SkipTables,
				"skip_file":    task.SkipFile,
			}

			record := taskstore.Record{
				TaskID:      task.TaskID,
				TaskType:    taskstore.TaskTypeSchemaDiff,
				Status:      taskstore.StatusRunning,
				ClusterName: task.ClusterName,
				SchemaName:  task.SchemaName,
				StartedAt:   startTime,
				TaskContext: ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("schema-diff: unable to write initial task status (%v)", err)
			}
		}
	}

	var tablesProcessed, tablesFailed int
	var failedTables []FailedTableInfo
	var skippedTables []string
	var summary DiffSummary

	defer func() {
		finishedAt := time.Now()
		task.Task.FinishedAt = finishedAt
		task.Task.TimeTaken = finishedAt.Sub(startTime).Seconds()

		status := taskstore.StatusFailed
		if err == nil {
			status = taskstore.StatusCompleted
		}
		task.Task.TaskStatus = status

		if recorder != nil && recorder.Created() {
			ctx := map[string]any{
				"tables_total":   len(task.tableList),
				"tables_diffed":  tablesProcessed,
				"tables_failed":  tablesFailed,
				"tables_skipped": len(skippedTables),
				"ddl_only":       task.DDLOnly,
			}
			if len(failedTables) > 0 {
				names := make([]string, len(failedTables))
				for i, ft := range failedTables {
					names[i] = ft.Table
				}
				ctx["failed_tables"] = names
			}
			if err != nil {
				ctx["error"] = err.Error()
			}

			updateErr := recorder.Update(taskstore.Record{
				TaskID:      task.TaskID,
				Status:      status,
				FinishedAt:  finishedAt,
				TimeTaken:   task.Task.TimeTaken,
				TaskContext: ctx,
			})
			if updateErr != nil {
				logger.Warn("schema-diff: unable to update task status (%v)", updateErr)
			}
		}

		if recorder != nil && recorder.OwnsStore() {
			storePtr := recorder.Store()
			if closeErr := recorder.Close(); closeErr != nil {
				logger.Warn("schema-diff: failed to close task store (%v)", closeErr)
			}
			if storePtr != nil && task.TaskStore == storePtr {
				task.TaskStore = nil
			}
		}
	}()

	if task.DDLOnly {
		return task.schemaObjectDiff()
	}

	for _, tableName := range task.tableList {
		var skipped bool
		for _, skip := range task.skipTablesList {
			if skip == tableName {
				if !task.Quiet {
					logger.Info("Skipping table: %s", tableName)
				}
				skipped = true
				break
			}
		}
		if skipped {
			skippedTables = append(skippedTables, fmt.Sprintf("%s.%s", task.SchemaName, tableName))
			continue
		}

		qualifiedTableName := fmt.Sprintf("%s.%s", task.SchemaName, tableName)
		if !task.Quiet {
			logger.Info("Diffing table: %s", qualifiedTableName)
		}

		tdTask := NewTableDiffTask()
		tdTask.ClusterName = task.ClusterName
		tdTask.DBName = task.DBName
		tdTask.Nodes = task.Nodes
		tdTask.QualifiedTableName = qualifiedTableName
		tdTask.ConcurrencyFactor = task.ConcurrencyFactor
		tdTask.MaxConnections = task.MaxConnections
		tdTask.BlockSize = task.BlockSize
		tdTask.CompareUnitSize = task.CompareUnitSize
		tdTask.Output = task.Output
		tdTask.TableFilter = task.TableFilter
		tdTask.OverrideBlockSize = task.OverrideBlockSize
		tdTask.QuietMode = task.Quiet
		tdTask.Ctx = task.Ctx

		if err := tdTask.Validate(); err != nil {
			logger.Warn("validation for table %s failed: %v", qualifiedTableName, err)
			tablesFailed++
			failedTables = append(failedTables, FailedTableInfo{Table: qualifiedTableName, Err: err})
			continue
		}

		if err := tdTask.RunChecks(true); err != nil {
			logger.Warn("checks for table %s failed: %v", qualifiedTableName, err)
			tablesFailed++
			failedTables = append(failedTables, FailedTableInfo{Table: qualifiedTableName, Err: err})
			continue
		}
		if err := tdTask.ExecuteTask(); err != nil {
			logger.Warn("error during comparison for table %s: %v", qualifiedTableName, err)
			tablesFailed++
			failedTables = append(failedTables, FailedTableInfo{Table: qualifiedTableName, Err: err})
			continue
		}

		if len(tdTask.DiffResult.NodeDiffs) > 0 {
			summary.DifferedTables = append(summary.DifferedTables, qualifiedTableName)
		} else {
			summary.MatchedTables = append(summary.MatchedTables, qualifiedTableName)
		}
		tablesProcessed++
	}

	summary.FailedTables = failedTables
	summary.SkippedTables = skippedTables
	summary.MissingTables = task.missingTables
	return summary.PrintAndFinalize("Schema diff", "schema "+task.SchemaName)
}

func (task *SchemaDiffCmd) CloneForSchedule(ctx context.Context) *SchemaDiffCmd {
	clone := NewSchemaDiffTask()
	clone.ClusterName = task.ClusterName
	clone.DBName = task.DBName
	clone.SchemaName = task.SchemaName
	clone.Nodes = task.Nodes
	clone.SkipTables = task.SkipTables
	clone.SkipFile = task.SkipFile
	clone.Quiet = task.Quiet
	clone.DDLOnly = task.DDLOnly
	clone.BlockSize = task.BlockSize
	clone.ConcurrencyFactor = task.ConcurrencyFactor
	clone.MaxConnections = task.MaxConnections
	clone.CompareUnitSize = task.CompareUnitSize
	clone.Output = task.Output
	clone.TableFilter = task.TableFilter
	clone.OverrideBlockSize = task.OverrideBlockSize
	clone.SkipDBUpdate = task.SkipDBUpdate
	clone.TaskStore = task.TaskStore
	clone.TaskStorePath = task.TaskStorePath
	clone.Ctx = ctx
	return clone
}

func getObjectsForSchema(ctx context.Context, pool *pgxpool.Pool, schemaName string) (*SchemaObjects, error) {
	tables, err := queries.GetTablesInSchema(ctx, pool, schemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query tables: %w", err)
	}
	var tableNames []string
	tableNames = append(tableNames, tables...)

	views, err := queries.GetViewsInSchema(ctx, pool, schemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query views: %w", err)
	}
	var viewNames []string
	viewNames = append(viewNames, views...)

	functions, err := queries.GetFunctionsInSchema(ctx, pool, schemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query functions: %w", err)
	}
	var functionSignatures []string
	functionSignatures = append(functionSignatures, functions...)

	indices, err := queries.GetIndicesInSchema(ctx, pool, schemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query indices: %w", err)
	}
	var indexNames []string
	indexNames = append(indexNames, indices...)

	return &SchemaObjects{
		Tables:    tableNames,
		Views:     viewNames,
		Functions: functionSignatures,
		Indices:   indexNames,
	}, nil
}
