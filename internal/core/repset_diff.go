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
	"bufio"
	"context"
	"fmt"
	"maps"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	utils "github.com/pgedge/ace/pkg/common"
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
	nodeList          []string
	clusterNodes      []map[string]any
	database          types.Database
	ConnectionPool    *pgxpool.Pool
	ConcurrencyFactor int
	BlockSize         int
	CompareUnitSize   int
	Output            string
	TableFilter       string
	OverrideBlockSize bool
	Ctx               context.Context

	IsAsync             bool
	ScheduleFrequency   string
	frequency           time.Duration
	BackgroundScheduler gocron.Scheduler

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

	firstNode := c.clusterNodes[0]

	nodeWithDBInfo := make(map[string]any)
	maps.Copy(nodeWithDBInfo, firstNode)
	nodeWithDBInfo["DBName"] = c.database.DBName
	nodeWithDBInfo["DBUser"] = c.database.DBUser
	nodeWithDBInfo["DBPassword"] = c.database.DBPassword

	if portVal, ok := nodeWithDBInfo["Port"]; ok {
		if portFloat, isFloat := portVal.(float64); isFloat {
			nodeWithDBInfo["Port"] = strconv.Itoa(int(portFloat))
		}
	}

	pool, err := auth.GetClusterNodeConnection(c.Ctx, nodeWithDBInfo, "")
	if err != nil {
		return fmt.Errorf("could not connect to database: %w", err)
	}
	defer pool.Close()

	repsetExists, err := queries.CheckRepSetExists(c.Ctx, pool, c.RepsetName)
	if err != nil {
		return fmt.Errorf("could not check if repset exists: %w", err)
	}
	if !repsetExists {
		return fmt.Errorf("repset %s not found", c.RepsetName)
	}

	tables, err := queries.GetTablesInRepSet(c.Ctx, pool, c.RepsetName)
	if err != nil {
		return fmt.Errorf("could not get tables in repset: %w", err)
	}

	c.tableList = tables

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
	if c.IsAsync {
		if strings.TrimSpace(c.ScheduleFrequency) == "" {
			return fmt.Errorf("run frequency should be specified with --every when --schedule is used")
		}
		freq, err := time.ParseDuration(c.ScheduleFrequency)
		if err != nil {
			return fmt.Errorf("could not parse schedule duration: %w", err)
		}
		c.frequency = freq
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

	var tablesProcessed, tablesFailed, tablesSkipped int
	var failedTables []string

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
				"tables_skipped": tablesSkipped,
			}
			if len(failedTables) > 0 {
				ctx["failed_tables"] = failedTables
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
			tablesSkipped++
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
			failedTables = append(failedTables, tableName)
			continue
		}

		if err := tdTask.RunChecks(true); err != nil {
			logger.Warn("checks for table %s failed: %v", tableName, err)
			tablesFailed++
			failedTables = append(failedTables, tableName)
			continue
		}
		if err := tdTask.ExecuteTask(); err != nil {
			logger.Warn("error during comparison for table %s: %v", tableName, err)
			tablesFailed++
			failedTables = append(failedTables, tableName)
			continue
		}

		tablesProcessed++
	}

	return nil
}

func (task *RepsetDiffCmd) RunScheduledTask() error {
	if err := task.Validate(); err != nil {
		return err
	}
	scheduler, err := gocron.NewScheduler()
	if err != nil {
		return fmt.Errorf("could not initialise scheduler: %w", err)
	}
	task.BackgroundScheduler = scheduler

	ctx := task.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	runOnce := func(runCtx context.Context) error {
		if runCtx.Err() != nil {
			return runCtx.Err()
		}
		runTask := task.cloneForScheduledRun(runCtx)
		if err := RepsetDiff(runTask); err != nil {
			return err
		}
		return nil
	}

	if err := runOnce(ctx); err != nil {
		_ = scheduler.Shutdown()
		return fmt.Errorf("initial repset diff failed: %w", err)
	}

	job, err := scheduler.NewJob(
		gocron.DurationJob(task.frequency),
		gocron.NewTask(func() {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := runOnce(ctx); err != nil {
				logger.Error("scheduled repset-diff run failed: %v", err)
			}
		}),
	)
	if err != nil {
		_ = scheduler.Shutdown()
		return fmt.Errorf("could not schedule repset-diff job: %w", err)
	}

	logger.Info("Scheduled repset-diff job (ID: %s) every %s", job.ID(), task.frequency)

	scheduler.Start()

	<-ctx.Done()

	logger.Info("Shutting down scheduled repset-diff job")
	if shutdownErr := scheduler.Shutdown(); shutdownErr != nil {
		return fmt.Errorf("shutdown scheduler: %w", shutdownErr)
	}

	return nil
}

func (task *RepsetDiffCmd) cloneForScheduledRun(ctx context.Context) *RepsetDiffCmd {
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
