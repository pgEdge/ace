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
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
)

type RepsetDiffCmd struct {
	types.Task

	ClusterName       string
	DBName            string
	RepsetName        string
	Nodes             string
	Quiet             bool
	SkipTables        string
	SkipFile          string
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

	ClientRole   string
	InvokeMethod string

	SkipDBUpdate  bool
	TaskStore     *taskstore.Store
	TaskStorePath string
}

func (c *RepsetDiffCmd) GetClusterName() string              { return c.ClusterName }
func (c *RepsetDiffCmd) GetDBName() string                   { return c.DBName }
func (c *RepsetDiffCmd) SetDBName(name string)               { c.DBName = name }
func (c *RepsetDiffCmd) GetNodes() string                    { return c.Nodes }
func (c *RepsetDiffCmd) GetNodeList() []string               { return c.nodeList }
func (c *RepsetDiffCmd) SetNodeList(nodes []string)          { c.nodeList = nodes }
func (c *RepsetDiffCmd) SetDatabase(db types.Database)       { c.database = db }
func (c *RepsetDiffCmd) GetClusterNodes() []map[string]any   { return c.clusterNodes }
func (c *RepsetDiffCmd) SetClusterNodes(cn []map[string]any) { c.clusterNodes = cn }

func NewRepsetDiffTask() *RepsetDiffCmd {
	return &RepsetDiffCmd{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskType:   taskstore.TaskTypeRepsetDiff,
			TaskStatus: taskstore.StatusPending,
		},
		Ctx: context.Background(),
	}
}

func (c *RepsetDiffCmd) connOpts() auth.ConnectionOptions {
	return auth.ConnectionOptions{PoolSize: c.MaxConnections}
}

func (c *RepsetDiffCmd) parseSkipList() error {
	var tables []string
	if c.SkipTables != "" {
		tables = append(tables, strings.Split(c.SkipTables, ",")...)
	}
	if c.SkipFile != "" {
		file, err := os.Open(c.SkipFile)
		if err != nil {
			return fmt.Errorf("could not open skip file: %w", err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			tables = append(tables, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("error reading skip file: %w", err)
		}
	}
	c.skipTablesList = tables
	return nil
}

func (c *RepsetDiffCmd) RunChecks(skipValidation bool) error {
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

	// Query repset tables from every node that has the repset and build a union.
	// In a uni-directional setup the repset may only exist on the publisher, but
	// the tables themselves exist on all nodes, so we still diff them across
	// every node.
	tablePresence := make(map[string]map[string]bool) // table -> {nodeName: true}
	var repsetNodeNames []string

	for _, nodeInfo := range c.clusterNodes {
		nodeName := nodeInfo["Name"].(string)

		nodeWithDBInfo := make(map[string]any)
		maps.Copy(nodeWithDBInfo, nodeInfo)
		utils.ApplyDatabaseCredentials(nodeWithDBInfo, c.database)
		if portVal, ok := nodeWithDBInfo["Port"]; ok {
			if portFloat, isFloat := portVal.(float64); isFloat {
				nodeWithDBInfo["Port"] = strconv.Itoa(int(portFloat))
			}
		}

		pool, err := auth.GetClusterNodeConnection(c.Ctx, nodeWithDBInfo, c.connOpts())
		if err != nil {
			return fmt.Errorf("could not connect to node %s: %w", nodeName, err)
		}

		repsetExists, err := queries.CheckRepSetExists(c.Ctx, pool, c.RepsetName)
		if err != nil {
			pool.Close()
			return fmt.Errorf("could not check if repset exists on node %s: %w", nodeName, err)
		}
		if !repsetExists {
			pool.Close()
			logger.Warn("repset %s not found on node %s, skipping for table discovery", c.RepsetName, nodeName)
			continue
		}
		repsetNodeNames = append(repsetNodeNames, nodeName)

		tables, err := queries.GetTablesInRepSet(c.Ctx, pool, c.RepsetName)
		pool.Close()
		if err != nil {
			return fmt.Errorf("could not get tables in repset on node %s: %w", nodeName, err)
		}

		for _, t := range tables {
			if tablePresence[t] == nil {
				tablePresence[t] = make(map[string]bool)
			}
			tablePresence[t][nodeName] = true
		}
	}

	if len(repsetNodeNames) == 0 {
		return fmt.Errorf("repset %s not found on any node", c.RepsetName)
	}

	// Build the full table list (union) and track tables not in the repset
	// on every node. All tables are still diffed (the data exists on all
	// nodes), but asymmetric repset membership is reported in the summary.
	var allTables []string
	var missingTables []MissingTableInfo
	for table, presence := range tablePresence {
		allTables = append(allTables, table)
		if len(presence) < len(repsetNodeNames) {
			var presentOn, missingFrom []string
			for _, n := range repsetNodeNames {
				if presence[n] {
					presentOn = append(presentOn, n)
				} else {
					missingFrom = append(missingFrom, n)
				}
			}
			missingTables = append(missingTables, MissingTableInfo{
				Table:       table,
				PresentOn:   presentOn,
				MissingFrom: missingFrom,
			})
		}
	}
	sort.Strings(allTables)
	sort.Slice(missingTables, func(i, j int) bool {
		return missingTables[i].Table < missingTables[j].Table
	})

	c.tableList = allTables
	c.missingTables = missingTables

	if len(c.tableList) == 0 {
		return fmt.Errorf("no tables found in repset %s", c.RepsetName)
	}

	return nil
}

func (c *RepsetDiffCmd) Validate() error {
	if c.ClusterName == "" {
		return fmt.Errorf("cluster name is required")
	}
	if c.RepsetName == "" {
		return fmt.Errorf("repset name is required")
	}
	cfg := config.Get()
	if c.MaxConnections == 0 && cfg != nil {
		c.MaxConnections = cfg.TableDiff.MaxConnections
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("max_connections must be >= 1 (or 0 to derive from concurrency factor)")
	}
	return nil
}

func RepsetDiff(task *RepsetDiffCmd) (err error) {
	if err := task.RunChecks(false); err != nil {
		return err
	}

	startTime := time.Now()

	if strings.TrimSpace(task.TaskID) == "" {
		task.TaskID = uuid.NewString()
	}
	if task.Task.TaskType == "" {
		task.Task.TaskType = taskstore.TaskTypeRepsetDiff
	}
	task.Task.StartedAt = startTime
	task.Task.TaskStatus = taskstore.StatusRunning
	task.Task.ClusterName = task.ClusterName

	var recorder *taskstore.Recorder
	if !task.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(task.TaskStore, task.TaskStorePath)
		if recErr != nil {
			logger.Warn("repset-diff: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if task.TaskStore == nil && rec.Store() != nil {
				task.TaskStore = rec.Store()
			}

			ctx := map[string]any{
				"repset":       task.RepsetName,
				"tables_total": len(task.tableList),
				"skip_tables":  task.SkipTables,
				"skip_file":    task.SkipFile,
				"table_filter": task.TableFilter,
			}

			record := taskstore.Record{
				TaskID:      task.TaskID,
				TaskType:    taskstore.TaskTypeRepsetDiff,
				Status:      taskstore.StatusRunning,
				ClusterName: task.ClusterName,
				StartedAt:   startTime,
				TaskContext: ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("repset-diff: unable to write initial task status (%v)", err)
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
				logger.Warn("repset-diff: unable to update task status (%v)", updateErr)
			}
		}

		if recorder != nil && recorder.OwnsStore() {
			storePtr := recorder.Store()
			if closeErr := recorder.Close(); closeErr != nil {
				logger.Warn("repset-diff: failed to close task store (%v)", closeErr)
			}
			if storePtr != nil && task.TaskStore == storePtr {
				task.TaskStore = nil
			}
		}
	}()

	for _, tableName := range task.tableList {
		var skipped bool
		for _, skip := range task.skipTablesList {
			if strings.TrimSpace(skip) == tableName {
				if !task.Quiet {
					logger.Info("Skipping table: %s", tableName)
				}
				skipped = true
				break
			}
		}
		if skipped {
			skippedTables = append(skippedTables, tableName)
			continue
		}

		if !task.Quiet {
			logger.Info("Diffing table: %s", tableName)
		}

		tdTask := NewTableDiffTask()
		tdTask.ClusterName = task.ClusterName
		tdTask.DBName = task.DBName
		tdTask.Nodes = task.Nodes
		tdTask.QualifiedTableName = tableName
		tdTask.ConcurrencyFactor = task.ConcurrencyFactor
		tdTask.MaxConnections = task.MaxConnections
		tdTask.BlockSize = task.BlockSize
		tdTask.CompareUnitSize = task.CompareUnitSize
		tdTask.Output = task.Output
		tdTask.TableFilter = task.TableFilter
		tdTask.OverrideBlockSize = task.OverrideBlockSize
		tdTask.QuietMode = task.Quiet
		tdTask.Ctx = task.Ctx
		tdTask.SkipDBUpdate = task.SkipDBUpdate
		tdTask.TaskStorePath = task.TaskStorePath

		if err := tdTask.Validate(); err != nil {
			logger.Warn("validation for table %s failed: %v", tableName, err)
			tablesFailed++
			failedTables = append(failedTables, FailedTableInfo{Table: tableName, Err: err})
			continue
		}

		if err := tdTask.RunChecks(true); err != nil {
			logger.Warn("checks for table %s failed: %v", tableName, err)
			tablesFailed++
			failedTables = append(failedTables, FailedTableInfo{Table: tableName, Err: err})
			continue
		}
		if err := tdTask.ExecuteTask(); err != nil {
			logger.Warn("error during comparison for table %s: %v", tableName, err)
			tablesFailed++
			failedTables = append(failedTables, FailedTableInfo{Table: tableName, Err: err})
			continue
		}

		if len(tdTask.DiffResult.NodeDiffs) > 0 {
			summary.DifferedTables = append(summary.DifferedTables, tableName)
		} else {
			summary.MatchedTables = append(summary.MatchedTables, tableName)
		}
		tablesProcessed++
	}

	summary.FailedTables = failedTables
	summary.SkippedTables = skippedTables
	summary.MissingTables = task.missingTables
	return summary.PrintAndFinalize("Repset diff", "repset "+task.RepsetName)
}

func (task *RepsetDiffCmd) CloneForSchedule(ctx context.Context) *RepsetDiffCmd {
	clone := NewRepsetDiffTask()
	clone.ClusterName = task.ClusterName
	clone.DBName = task.DBName
	clone.RepsetName = task.RepsetName
	clone.Nodes = task.Nodes
	clone.SkipTables = task.SkipTables
	clone.SkipFile = task.SkipFile
	clone.Quiet = task.Quiet
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
