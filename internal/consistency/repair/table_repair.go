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

package repair

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	planner "github.com/pgedge/ace/internal/consistency/repair/plan"
	auth "github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
)

type RepairReport struct {
	OperationType           string         `json:"operation_type"`
	Mode                    string         `json:"mode"`
	Timestamp               string         `json:"time_stamp"`
	SuppliedArgs            map[string]any `json:"supplied_args"`
	DatabaseCredentialsUsed types.Database `json:"database_credentials_used"`
	Changes                 map[string]any `json:"changes"`
	RunTimeSeconds          float64        `json:"run_time,omitempty"`
}

type TableRepairTask struct {
	types.Task
	types.DerivedFields

	QualifiedTableName string
	DBName             string
	Nodes              string

	DiffFilePath  string
	SourceOfTruth string

	RepairPlanPath string
	RepairPlan     *planner.RepairPlanFile

	QuietMode      bool
	DryRun         bool
	InsertOnly     bool
	UpsertOnly     bool
	FireTriggers   bool
	GenerateReport bool
	FixNulls       bool // TBD
	Bidirectional  bool
	RecoveryMode   bool
	PreserveOrigin bool

	InvokeMethod string // TBD
	ClientRole   string // TBD

	SkipDBUpdate bool

	TaskStore     *taskstore.Store
	TaskStorePath string

	Pools map[string]*pgxpool.Pool

	RawDiffs types.DiffOutput
	report   *RepairReport

	planRuleMatches map[string]map[string]string // populated when using repair plans
	planStaleChecks map[string]map[string]planStaleCheck
	staleSkipLog    *staleSkipLogger

	autoSelectedSourceOfTruth string
	autoSelectionFailedNode   string
	autoSelectionDetails      map[string]map[string]string

	Ctx context.Context
}

// Defining these getters and setters to satisfy ClusterConfigProvider interface
func (tr *TableRepairTask) GetClusterName() string              { return tr.ClusterName }
func (tr *TableRepairTask) GetDBName() string                   { return tr.DBName }
func (tr *TableRepairTask) SetDBName(name string)               { tr.DBName = name }
func (tr *TableRepairTask) GetNodes() string                    { return tr.Nodes }
func (tr *TableRepairTask) GetNodeList() []string               { return tr.NodeList }
func (tr *TableRepairTask) SetNodeList(nl []string)             { tr.NodeList = nl }
func (tr *TableRepairTask) SetDatabase(db types.Database)       { tr.Database = db }
func (tr *TableRepairTask) GetClusterNodes() []map[string]any   { return tr.ClusterNodes }
func (tr *TableRepairTask) SetClusterNodes(cn []map[string]any) { tr.ClusterNodes = cn }

func NewTableRepairTask() *TableRepairTask {
	return &TableRepairTask{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskType:   taskstore.TaskTypeTableRepair,
			TaskStatus: taskstore.StatusPending,
		},
		InvokeMethod:   "cli",
		PreserveOrigin: false,
		Pools:          make(map[string]*pgxpool.Pool),
		DerivedFields: types.DerivedFields{
			HostMap: make(map[string]string),
		},
		Ctx: context.Background(),
	}
}

func (t *TableRepairTask) closePools() {
	for name, pool := range t.Pools {
		if pool != nil {
			pool.Close()
		}
		delete(t.Pools, name)
	}
}

func (t *TableRepairTask) connOpts() auth.ConnectionOptions {
	return auth.ConnectionOptions{}
}

func (t *TableRepairTask) setRole(tx pgx.Tx, nodeName string) error {
	role := strings.TrimSpace(t.ClientRole)
	requireRole := t.InvokeMethod != "cli"
	if role == "" {
		if requireRole {
			return fmt.Errorf("client role in cert CN cannot be null")
		}
		return nil
	}

	roleSQL := fmt.Sprintf("SET ROLE %s", pgx.Identifier{role}.Sanitize())
	if _, err := tx.Exec(t.Ctx, roleSQL); err != nil {
		return fmt.Errorf("setting role %s on %s: %w", role, nodeName, err)
	}
	logger.Debug("SET ROLE %s on %s", role, nodeName)
	return nil
}

func (tr *TableRepairTask) checkRepairOptionsCompatibility() error {
	incompatibleOptions := []struct {
		condition bool
		message   string
	}{
		{tr.Bidirectional && tr.UpsertOnly, "bidirectional and upsert_only cannot be used together"},
		{tr.Bidirectional && tr.FixNulls, "bidirectional and fix_nulls cannot be used together"},
		{tr.FixNulls && tr.InsertOnly, "insert_only and fix_nulls cannot be used together"},
		{tr.FixNulls && tr.UpsertOnly, "upsert_only and fix_nulls cannot be used together"},
		{tr.InsertOnly && tr.UpsertOnly, "insert_only and upsert_only cannot be used together"},
		{strings.TrimSpace(tr.RepairPlanPath) != "" && tr.FixNulls, "repair-plan and fix_nulls cannot be used together"},
		{strings.TrimSpace(tr.RepairPlanPath) != "" && tr.Bidirectional, "repair-plan and bidirectional cannot be used together"},
	}

	for _, rule := range incompatibleOptions {
		if rule.condition {
			return fmt.Errorf("%s", rule.message)
		}
	}
	return nil
}

func (tr *TableRepairTask) checkIfSourceOfTruthIsNeeded() bool {
	// Advanced repair plans can encode SOT choices per rule, so skip mandatory SoT when a plan is supplied.
	if strings.TrimSpace(tr.RepairPlanPath) != "" || tr.RepairPlan != nil {
		return false
	}
	if tr.RecoveryMode {
		// in recovery mode we'll auto-select if missing
		return false
	}
	casesNotNeeded := []bool{
		tr.FixNulls,
		tr.Bidirectional && tr.InsertOnly,
	}

	for _, skip := range casesNotNeeded {
		if skip {
			return false
		}
	}

	return true
}

func (t *TableRepairTask) ValidateAndPrepare() error {
	success := false
	defer func() {
		if !success {
			t.closePools()
		}
	}()

	if t.ClusterName == "" {
		return fmt.Errorf("cluster_name is required")
	}
	if t.QualifiedTableName == "" {
		return fmt.Errorf("table_name is required")
	}
	if t.DiffFilePath == "" {
		return fmt.Errorf("diff_file_path is required")
	}

	if err := t.checkRepairOptionsCompatibility(); err != nil {
		return fmt.Errorf("repair options are incompatible: %w", err)
	}

	if t.checkIfSourceOfTruthIsNeeded() && t.SourceOfTruth == "" {
		return fmt.Errorf("source_of_truth is required unless --fix-nulls or --bidirectional is specified")
	}

	parts := strings.Split(t.QualifiedTableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("table_name must be in schema.table format, got: %s", t.QualifiedTableName)
	}
	t.Schema = strings.TrimSpace(parts[0])
	t.Table = strings.TrimSpace(parts[1])

	if t.Schema == "" || t.Table == "" {
		return fmt.Errorf("schema and table name parts cannot be empty in %s", t.QualifiedTableName)
	}

	// Reading nodelist is unnecessary since the diff file will contain that info.
	// TODO: Remove this once checks are handled correctly in readClusterInfo
	nodeList, err := utils.ParseNodes(t.Nodes)
	if err != nil {
		return fmt.Errorf("nodes should be a comma-separated list of nodenames. E.g., nodes=\"n1,n2\". Error: %w", err)
	}
	t.NodeList = nodeList

	if err := utils.ReadClusterInfo(t); err != nil {
		return fmt.Errorf("failed to read cluster info: %w", err)
	}

	for _, nodeInfo := range t.ClusterNodes {
		hostname, okHostname := nodeInfo["Name"].(string)
		publicIP, okPublicIP := nodeInfo["PublicIP"].(string)
		port, okPort := nodeInfo["Port"].(string)

		if !okHostname || !okPublicIP || !okPort {
			logger.Warn("Skipping node with incomplete info: %+v", nodeInfo)
			continue
		}
		t.HostMap[fmt.Sprintf("%s:%s", publicIP, port)] = hostname
	}

	foundSourceOfTruth := false
	if t.SourceOfTruth != "" {
		for _, nodeInfo := range t.ClusterNodes {
			if name, ok := nodeInfo["Name"].(string); ok && name == t.SourceOfTruth {
				foundSourceOfTruth = true
				break
			}
		}
		if !foundSourceOfTruth {
			return fmt.Errorf("source_of_truth node '%s' not found in cluster '%s' or is not active", t.SourceOfTruth, t.ClusterName)
		}
	}

	diffData, err := os.ReadFile(t.DiffFilePath)
	if err != nil {
		return fmt.Errorf("failed to read diff file %s: %w", t.DiffFilePath, err)
	}
	if err := json.Unmarshal(diffData, &t.RawDiffs); err != nil {
		return fmt.Errorf("failed to unmarshal diff file %s: %w", t.DiffFilePath, err)
	}

	diffSchema := strings.TrimSpace(t.RawDiffs.Summary.Schema)
	diffTable := strings.TrimSpace(t.RawDiffs.Summary.Table)
	if diffSchema == "" || diffTable == "" {
		return fmt.Errorf("diff file %s is missing schema/table metadata; cannot verify repair target", t.DiffFilePath)
	}
	if diffSchema != t.Schema || diffTable != t.Table {
		return fmt.Errorf("diff file %s was generated for %s.%s but repair target is %s.%s", t.DiffFilePath, diffSchema, diffTable, t.Schema, t.Table)
	}
	if t.RawDiffs.Summary.TableFilter != "" {
		logger.Info("Diff file was generated with table filter: %s", t.RawDiffs.Summary.TableFilter)
	}
	if strings.TrimSpace(t.RawDiffs.Summary.AgainstOrigin) != "" && !t.RecoveryMode {
		return fmt.Errorf("diff file indicates an origin-scoped comparison (--against-origin); re-run table-repair with --recovery-mode or provide an explicit source_of_truth")
	}

	if strings.TrimSpace(t.RepairPlanPath) != "" {
		if err := t.loadRepairPlan(strings.TrimSpace(t.RepairPlanPath)); err != nil {
			return err
		}
	}

	if t.RawDiffs.NodeDiffs == nil {
		return fmt.Errorf("invalid diff file format: missing 'diffs' field or it's not a map")
	}

	involvedNodeNames := make(map[string]bool)
	for nodePairKey := range t.RawDiffs.NodeDiffs {
		nodesInPair := strings.Split(nodePairKey, "/")
		if len(nodesInPair) != 2 {
			return fmt.Errorf("invalid node pair key in diff file: %s", nodePairKey)
		}
		involvedNodeNames[nodesInPair[0]] = true
		involvedNodeNames[nodesInPair[1]] = true
	}

	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		if len(t.NodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !utils.Contains(t.NodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeMap)
		utils.ApplyDatabaseCredentials(combinedMap, t.Database)
		clusterNodes = append(clusterNodes, combinedMap)
	}

	t.ClusterNodes = clusterNodes

	if strings.TrimSpace(t.RawDiffs.Summary.AgainstOrigin) != "" && t.RecoveryMode && t.SourceOfTruth == "" {
		failedNode := strings.TrimSpace(t.RawDiffs.Summary.AgainstOriginResolved)
		if failedNode == "" {
			failedNode = strings.TrimSpace(t.RawDiffs.Summary.AgainstOrigin)
		}
		if failedNode == "" {
			return fmt.Errorf("recovery-mode requires failed node information in diff summary")
		}
		selected, details, err := t.autoSelectSourceOfTruth(failedNode, involvedNodeNames)
		if err != nil {
			return err
		}
		t.autoSelectedSourceOfTruth = selected
		t.autoSelectionFailedNode = failedNode
		t.autoSelectionDetails = details
		t.SourceOfTruth = selected
		logger.Info("table-repair: recovery-mode selected %s as source_of_truth (failed node: %s)", selected, failedNode)
	}

	// Repair needs these privileges. Perhaps we can pare this down depending
	// on the repair options, but for now we'll keep it as is.
	requiredPrivileges := types.UserPrivileges{
		TableSelect: true,
		TableInsert: true,
		TableUpdate: true,
		TableDelete: true,
	}

	var refCols []string
	var refKey []string
	var refColTypes map[string]string
	var refNode string

	for _, nodeInfo := range t.ClusterNodes {
		nodeName, _ := nodeInfo["Name"].(string)
		if nodeName == t.SourceOfTruth || involvedNodeNames[nodeName] {
			connPool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
			if err != nil {
				logger.Warn("Failed to connect to node %s: %v. Will attempt to proceed if it's not critical or SoT.", nodeName, err)
				if nodeName == t.SourceOfTruth {
					return fmt.Errorf("failed to connect to source_of_truth node %s: %w", nodeName, err)
				}
				continue
			}
			t.Pools[nodeName] = connPool

			cols, err := queries.GetColumns(t.Ctx, connPool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to get columns for %s.%s on node %s: %w", t.Schema, t.Table, nodeName, err)
			}
			t.Cols = cols

			pKey, err := queries.GetPrimaryKey(t.Ctx, connPool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to get primary key for %s.%s on node %s: %w", t.Schema, t.Table, nodeName, err)
			}
			if len(pKey) == 0 {
				return fmt.Errorf("no primary key found for %s.%s on node %s", t.Schema, t.Table, nodeName)
			}
			t.Key = pKey
			t.SimplePrimaryKey = len(pKey) == 1

			if refCols == nil {
				refCols = cols
				refKey = pKey
				refNode = nodeName
			} else {
				if !slices.Equal(cols, refCols) {
					return fmt.Errorf("table columns differ between nodes %s and %s", refNode, nodeName)
				}
				if !slices.Equal(pKey, refKey) {
					return fmt.Errorf("primary key definition differs between nodes %s and %s", refNode, nodeName)
				}
			}

			publicIP, _ := nodeInfo["PublicIP"].(string)
			port, _ := nodeInfo["Port"].(string)
			colTypes, err := queries.GetColumnTypes(t.Ctx, connPool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to get column types for %s on node %s: %w", t.Table, nodeName, err)
			}
			if t.ColTypes == nil {
				t.ColTypes = make(map[string]map[string]string)
			}
			t.ColTypes[fmt.Sprintf("%s:%s", publicIP, port)] = colTypes

			if refColTypes == nil {
				refColTypes = colTypes
			} else if !reflect.DeepEqual(colTypes, refColTypes) {
				return fmt.Errorf("column types differ between nodes %s and %s", refNode, nodeName)
			}

			dbUser, _ := nodeInfo["DBUser"].(string)
			if dbUser == "" {
				dbUser = t.Database.DBUser
			}
			privs, err := queries.CheckUserPrivileges(t.Ctx, connPool, dbUser, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to check user privileges on node %s: %w", nodeName, err)
			}

			missingPrivs := []string{}
			if requiredPrivileges.TableSelect && !privs.TableSelect {
				missingPrivs = append(missingPrivs, "SELECT")
			}
			if requiredPrivileges.TableInsert && !privs.TableInsert {
				missingPrivs = append(missingPrivs, "INSERT")
			}
			if requiredPrivileges.TableUpdate && !privs.TableUpdate {
				missingPrivs = append(missingPrivs, "UPDATE")
			}
			if requiredPrivileges.TableDelete && !privs.TableDelete {
				missingPrivs = append(missingPrivs, "DELETE")
			}

			if len(missingPrivs) > 0 {
				return fmt.Errorf("user '%s' on node '%s' is missing privileges: %s for table %s.%s",
					dbUser, nodeName, strings.Join(missingPrivs, ", "), t.Schema, t.Table)
			}

		}
	}

	if len(involvedNodeNames) == 0 {
		return fmt.Errorf("failed to connect to any relevant node to verify schema or permissions")
	}
	if t.SourceOfTruth != "" && t.Pools[t.SourceOfTruth] == nil {
		return fmt.Errorf("failed to establish a connection to the source_of_truth node: %s", t.SourceOfTruth)
	}

	logger.Debug("Table repair task validated and prepared successfully.")
	success = true
	return nil
}

func (t *TableRepairTask) loadRepairPlan(planPath string) error {
	plan, err := planner.LoadRepairPlanFile(planPath)
	if err != nil {
		return fmt.Errorf("load repair plan: %w", err)
	}

	tableKey := fmt.Sprintf("%s.%s", t.Schema, t.Table)
	if _, ok := plan.Tables[tableKey]; !ok {
		return fmt.Errorf("repair plan %s does not include table %s", planPath, tableKey)
	}

	t.RepairPlan = plan
	return nil
}

func (t *TableRepairTask) initialiseReport() *RepairReport {
	report := &RepairReport{
		Changes: make(map[string]any),
	}

	if t.DryRun {
		report.OperationType = "DRY_RUN"
		report.Mode = "DRY_RUN"
	} else {
		report.OperationType = "table-repair"
		report.Mode = "LIVE_RUN"
	}
	now := time.Now()
	report.Timestamp = now.Format("2006-01-02 15:04:05") + fmt.Sprintf(".%03d", now.Nanosecond()/1e6)

	report.SuppliedArgs = map[string]any{
		"cluster_name":     t.ClusterName,
		"diff_file_path":   t.DiffFilePath,
		"repair_plan_path": t.RepairPlanPath,
		"source_of_truth":  t.SourceOfTruth,
		"table_name":       t.QualifiedTableName,
		"dbname":           t.DBName,
		"dry_run":          t.DryRun,
		"quiet":            t.QuietMode,
		"insert_only":      t.InsertOnly,
		"upsert_only":      t.UpsertOnly,
		"fire_triggers":    t.FireTriggers,
		"generate_report":  t.GenerateReport,
		"bidirectional":    t.Bidirectional,
		"recovery_mode":    t.RecoveryMode,
	}

	if t.autoSelectedSourceOfTruth != "" {
		report.Changes["auto_source_of_truth"] = map[string]any{
			"selected":    t.autoSelectedSourceOfTruth,
			"failed_node": t.autoSelectionFailedNode,
			"lsn_probe":   t.autoSelectionDetails,
		}
	}

	dbInfoForReport := t.Database
	dbInfoForReport.DBPassword = ""
	report.DatabaseCredentialsUsed = dbInfoForReport
	return report
}

func writeReportToFile(report *RepairReport) error {
	now := time.Now()
	reportFolder := "reports"
	dateFolderName := now.Format("2006-01-02")
	reportDir := filepath.Join(reportFolder, dateFolderName)

	if err := os.MkdirAll(reportDir, 0755); err != nil {
		return fmt.Errorf("failed to create report directory %s: %w", reportDir, err)
	}

	var fileNamePrefix string
	if report.Mode == "DRY_RUN" {
		fileNamePrefix = "dry_run_report_"
	} else {
		fileNamePrefix = "repair_report_"
	}
	fileNameSuffix := now.Format("150405") + fmt.Sprintf(".%03d", now.Nanosecond()/1e6)
	fileName := fmt.Sprintf("%s%s.json", fileNamePrefix, fileNameSuffix)
	filePath := filepath.Join(reportDir, fileName)

	reportData, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal report to JSON: %w", err)
	}

	if err := os.WriteFile(filePath, reportData, 0644); err != nil {
		return fmt.Errorf("failed to write report to file %s: %w", filePath, err)
	}

	logger.Info("Wrote report to %s", filePath)
	return nil
}

func (t *TableRepairTask) Run(skipValidation bool) (err error) {
	if !skipValidation {
		if err := t.ValidateAndPrepare(); err != nil {
			return fmt.Errorf("task validation and preparation failed: %w", err)
		}
	}

	defer t.closePools()
	defer t.closeStaleSkipLog()

	startTime := time.Now()

	if strings.TrimSpace(t.TaskID) == "" {
		t.TaskID = uuid.NewString()
	}
	if t.Task.TaskType == "" {
		t.Task.TaskType = taskstore.TaskTypeTableRepair
	}
	t.Task.StartedAt = startTime
	t.Task.TaskStatus = taskstore.StatusRunning
	t.Task.ClusterName = t.ClusterName

	var recorder *taskstore.Recorder
	if !t.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(t.TaskStore, t.TaskStorePath)
		if recErr != nil {
			logger.Warn("table-repair: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if t.TaskStore == nil && rec.Store() != nil {
				t.TaskStore = rec.Store()
			}

			ctx := map[string]any{
				"qualified_table":  t.QualifiedTableName,
				"diff_file":        t.DiffFilePath,
				"repair_plan_path": t.RepairPlanPath,
				"source_of_truth":  t.SourceOfTruth,
				"dry_run":          t.DryRun,
				"insert_only":      t.InsertOnly,
				"upsert_only":      t.UpsertOnly,
				"fire_triggers":    t.FireTriggers,
				"bidirectional":    t.Bidirectional,
				"generate_report":  t.GenerateReport,
			}

			record := taskstore.Record{
				TaskID:       t.TaskID,
				TaskType:     taskstore.TaskTypeTableRepair,
				Status:       taskstore.StatusRunning,
				ClusterName:  t.ClusterName,
				SchemaName:   t.Schema,
				TableName:    t.Table,
				DiffFilePath: t.DiffFilePath,
				StartedAt:    startTime,
				TaskContext:  ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("table-repair: unable to write initial task status (%v)", err)
			}
		}
	}

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
				"dry_run": t.DryRun,
			}
			if t.report != nil {
				ctx["repair_report"] = t.report
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
				logger.Warn("table-repair: unable to update task status (%v)", updateErr)
			}
		}

		if recorder != nil && recorder.OwnsStore() {
			storePtr := recorder.Store()
			if closeErr := recorder.Close(); closeErr != nil {
				logger.Warn("table-repair: failed to close task store (%v)", closeErr)
			}
			if storePtr != nil && t.TaskStore == storePtr {
				t.TaskStore = nil
			}
		}
	}()

	if t.GenerateReport {
		t.report = t.initialiseReport()
	}

	defer func() {
		if t.GenerateReport && t.report != nil {
			t.report.RunTimeSeconds = time.Since(startTime).Seconds()
			if err := writeReportToFile(t.report); err != nil {
				logger.Warn("Warning: failed to write repair report: %v", err)
			}
		}
	}()

	if t.FixNulls {
		return t.runFixNulls(startTime)
	}

	if t.DryRun {
		output, err := getDryRunOutput(t)
		if err != nil {
			return fmt.Errorf("failed to generate dry run output: %w", err)
		}
		fmt.Print(output)
		return nil
	}

	if t.Bidirectional {
		return t.runBidirectionalRepair()
	}

	return t.runUnidirectionalRepair(startTime)
}

type rowData struct {
	data     map[string]any
	pkValues []any
	pkMap    map[string]any
	pkKey    string
}

type nullUpdate struct {
	pkValues  []any
	pkMap     map[string]any
	columns   map[string]any
	sourceRow types.OrderedMap // Source row providing the non-null value (for origin tracking)
}

func (t *TableRepairTask) runFixNulls(startTime time.Time) error {
	logger.Info("Starting fix-nulls repair for %s on cluster %s", t.QualifiedTableName, t.ClusterName)
	defer t.closePools()

	nullUpdates, err := t.buildNullUpdates()
	if err != nil {
		return fmt.Errorf("failed to prepare null updates: %w", err)
	}

	if t.DryRun {
		output, err := t.getFixNullsDryRunOutput(nullUpdates)
		if err != nil {
			return fmt.Errorf("failed to generate fix-nulls dry run output: %w", err)
		}
		fmt.Print(output)
		return nil
	}

	totalCellsUpdated := make(map[string]int)
	var repairErrors []string

	for nodeName, updates := range nullUpdates {
		if len(updates) == 0 {
			continue
		}

		pool, ok := t.Pools[nodeName]
		if !ok || pool == nil {
			logger.Debug("Connection pool for node %s not found, attempting to connect.", nodeName)
			var nodeInfo map[string]any
			for _, ni := range t.ClusterNodes {
				if name, _ := ni["Name"].(string); name == nodeName {
					nodeInfo = ni
					break
				}
			}

			if nodeInfo == nil {
				errStr := fmt.Sprintf("no node info for %s", nodeName)
				logger.Error("Could not find node info for %s. Skipping repairs for this node.", nodeName)
				repairErrors = append(repairErrors, errStr)
				continue
			}

			pool, err = auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
			if err != nil {
				logger.Error("Failed to connect to node %s: %v. Skipping repairs for this node.", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("connection failed for %s: %v", nodeName, err))
				continue
			}
			t.Pools[nodeName] = pool
			logger.Debug("Successfully connected to node %s and created a new connection pool.", nodeName)
		}

		tx, err := pool.Begin(t.Ctx)
		if err != nil {
			logger.Error("starting transaction on node %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("tx begin failed for %s: %v", nodeName, err))
			continue
		}

		spockRepairModeActive := false
		if _, err := tx.Exec(t.Ctx, "SELECT spock.repair_mode(true)"); err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("enabling spock.repair_mode(true) on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(true) failed for %s: %v", nodeName, err))
			continue
		}
		spockRepairModeActive = true
		logger.Debug("spock.repair_mode(true) set on %s", nodeName)

		if t.FireTriggers {
			_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'local'")
		} else {
			_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'replica'")
		}
		if err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("setting session_replication_role on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("session_replication_role failed for %s: %v", nodeName, err))
			continue
		}
		logger.Debug("session_replication_role set on %s (fire_triggers: %v)", nodeName, t.FireTriggers)

		if err := t.setRole(tx, nodeName); err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("%v", err)
			repairErrors = append(repairErrors, err.Error())
			continue
		}

		colTypes, _, err := t.getColTypesForNode(nodeName)
		if err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("%s", err.Error())
			repairErrors = append(repairErrors, err.Error())
			continue
		}

		columnSet := make(map[string]struct{})
		for _, nu := range updates {
			for col := range nu.columns {
				columnSet[col] = struct{}{}
			}
		}
		var columns []string
		for col := range columnSet {
			columns = append(columns, col)
		}
		sort.Strings(columns)

		nodeCellsUpdated := 0
		nodeFailed := false

		for _, col := range columns {
			colType, ok := colTypes[col]
			if !ok {
				nodeFailed = true
				tx.Rollback(t.Ctx)
				errStr := fmt.Sprintf("column type for %s not found on node %s", col, nodeName)
				logger.Error("%s", errStr)
				repairErrors = append(repairErrors, errStr)
				break
			}

			var rowsForCol []*nullUpdate
			for _, nu := range updates {
				if _, ok := nu.columns[col]; ok {
					rowsForCol = append(rowsForCol, nu)
				}
			}
			if len(rowsForCol) == 0 {
				continue
			}

			// Extract origin information for fix-nulls updates if preserve-origin is enabled
			var originInfoMap map[string]*rowOriginInfo
			if t.PreserveOrigin {
				originInfoMap = make(map[string]*rowOriginInfo)
				for _, nu := range rowsForCol {
					if nu.sourceRow != nil {
						pkeyStr, err := utils.StringifyKey(nu.pkMap, t.Key)
						if err != nil {
							// Try alternative method
							pkeyStr, err = utils.StringifyOrderedMapKey(nu.sourceRow, t.Key)
						}
						if err == nil {
							if originInfo := extractOriginInfoFromRow(nu.sourceRow); originInfo != nil {
								originInfoMap[pkeyStr] = originInfo
							}
						}
					}
				}
			}

			updatedCount, err := t.applyFixNullsUpdates(tx, col, colType, rowsForCol, colTypes, originInfoMap, nodeName)
			if err != nil {
				nodeFailed = true
				tx.Rollback(t.Ctx)
				logger.Error("executing fix-nulls updates for column %s on node %s: %v", col, nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("fix-nulls updates failed for %s on %s: %v", col, nodeName, err))
				break
			}
			nodeCellsUpdated += updatedCount
			logger.Info("Updated %d column values for column %s on %s", updatedCount, col, nodeName)
		}

		if nodeFailed {
			continue
		}

		if spockRepairModeActive {
			_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(false)")
			if err != nil {
				tx.Rollback(t.Ctx)
				logger.Error("disabling spock.repair_mode(false) on %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(false) failed for %s: %v", nodeName, err))
				continue
			}
			logger.Debug("spock.repair_mode(false) set on %s", nodeName)
		}

		if err := tx.Commit(t.Ctx); err != nil {
			logger.Error("committing transaction on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("commit failed for %s: %v", nodeName, err))
			continue
		}
		logger.Debug("Transaction committed successfully on %s", nodeName)

		totalCellsUpdated[nodeName] = nodeCellsUpdated

		if t.report != nil && nodeCellsUpdated > 0 {
			t.populateFixNullsReport(nodeName, updates, "updated_rows")
		}
	}

	t.FinishedAt = time.Now()
	t.TimeTaken = float64(t.FinishedAt.Sub(startTime).Milliseconds())
	runTimeStr := fmt.Sprintf("%.3fs", t.TimeTaken/1000)

	if len(repairErrors) > 0 {
		logger.Error("Fix-nulls repair of %s failed in %s with errors: %s", t.QualifiedTableName, runTimeStr, strings.Join(repairErrors, "; "))
		t.TaskStatus = "FAILED"
		t.TaskContext = strings.Join(repairErrors, "; ")
		return fmt.Errorf("fix-nulls repair encountered errors: %s", t.TaskContext)
	}

	totalCells := 0
	totalRows := 0
	var updatedNodes []string
	for node, count := range totalCellsUpdated {
		if count > 0 {
			updatedNodes = append(updatedNodes, node)
			totalCells += count
			if updates, ok := nullUpdates[node]; ok {
				totalRows += len(updates)
			}
		}
	}
	sort.Strings(updatedNodes)

	if totalCells == 0 {
		logger.Info("Fix-nulls repair of %s complete in %s. No null differences found.", t.QualifiedTableName, runTimeStr)
		t.TaskStatus = "COMPLETED"
		t.TaskContext = "No null repairs needed"
		return nil
	}

	logger.Info("Fix-nulls repair of %s complete in %s. Nodes %s updated (%d column values across %d rows).",
		t.QualifiedTableName,
		runTimeStr,
		strings.Join(updatedNodes, ", "),
		totalCells,
		totalRows,
	)

	t.TaskStatus = "COMPLETED"
	t.TaskContext = fmt.Sprintf("Fix-nulls updated %d column values across %d rows on nodes: %s", totalCells, totalRows, strings.Join(updatedNodes, ", "))

	return nil
}

func (t *TableRepairTask) buildNullUpdates() (map[string]map[string]*nullUpdate, error) {
	updatesByNode := make(map[string]map[string]*nullUpdate)

	for nodePair, diffs := range t.RawDiffs.NodeDiffs {
		nodes := strings.Split(nodePair, "/")
		if len(nodes) != 2 {
			logger.Warn("Warning: Invalid node pair key '%s', skipping", nodePair)
			continue
		}
		node1Name, node2Name := nodes[0], nodes[1]

		node1Rows := diffs.Rows[node1Name]
		node2Rows := diffs.Rows[node2Name]

		node1Index, err := buildRowIndex(node1Rows, t.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to index rows for %s: %w", node1Name, err)
		}
		node2Index, err := buildRowIndex(node2Rows, t.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to index rows for %s: %w", node2Name, err)
		}

		for pkKey, row1 := range node1Index {
			row2, ok := node2Index[pkKey]
			if !ok {
				continue
			}

			for _, col := range t.Cols {
				isPk := slices.Contains(t.Key, col)
				if isPk {
					continue
				}

				val1 := row1.data[col]
				val2 := row2.data[col]

				if val1 == nil && val2 != nil {
					// Update node1 with value from node2 - find the source row from node2
					var sourceRow types.OrderedMap
					for _, r := range node2Rows {
						pkeyStr, err := utils.StringifyOrderedMapKey(r, t.Key)
						if err == nil && pkeyStr == pkKey {
							sourceRow = r
							break
						}
					}
					addNullUpdate(updatesByNode, node1Name, row1, col, val2, sourceRow)
				} else if val2 == nil && val1 != nil {
					// Update node2 with value from node1 - find the source row from node1
					var sourceRow types.OrderedMap
					for _, r := range node1Rows {
						pkeyStr, err := utils.StringifyOrderedMapKey(r, t.Key)
						if err == nil && pkeyStr == pkKey {
							sourceRow = r
							break
						}
					}
					addNullUpdate(updatesByNode, node2Name, row2, col, val1, sourceRow)
				}
			}
		}
	}

	return updatesByNode, nil
}

func buildRowIndex(rows []types.OrderedMap, keyCols []string) (map[string]rowData, error) {
	index := make(map[string]rowData, len(rows))
	for _, row := range rows {
		rowMap := utils.StripSpockMetadata(utils.OrderedMapToMap(row))

		pkVals := make([]any, len(keyCols))
		pkMap := make(map[string]any, len(keyCols))
		for i, key := range keyCols {
			val, ok := rowMap[key]
			if !ok {
				return nil, fmt.Errorf("primary key column %s not found in row", key)
			}
			pkVals[i] = val
			pkMap[key] = val
		}

		pkKey, err := utils.StringifyKey(rowMap, keyCols)
		if err != nil {
			return nil, fmt.Errorf("failed to stringify primary key: %w", err)
		}

		index[pkKey] = rowData{
			data:     rowMap,
			pkValues: pkVals,
			pkMap:    pkMap,
			pkKey:    pkKey,
		}
	}
	return index, nil
}

func addNullUpdate(updates map[string]map[string]*nullUpdate, nodeName string, row rowData, col string, value any, sourceRow types.OrderedMap) {
	if value == nil {
		return
	}

	if _, ok := updates[nodeName]; !ok {
		updates[nodeName] = make(map[string]*nullUpdate)
	}

	nodeUpdates := updates[nodeName]
	nu, ok := nodeUpdates[row.pkKey]
	if !ok {
		nu = &nullUpdate{
			pkValues:  row.pkValues,
			pkMap:     row.pkMap,
			columns:   make(map[string]any),
			sourceRow: sourceRow,
		}
		nodeUpdates[row.pkKey] = nu
	}

	if _, exists := nu.columns[col]; !exists {
		nu.columns[col] = value
	}

	// Update source row if not set (retain first source)
	if nu.sourceRow == nil {
		nu.sourceRow = sourceRow
	}
}

func (t *TableRepairTask) getFixNullsDryRunOutput(updates map[string]map[string]*nullUpdate) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n######## DRY RUN for table %s (fix-nulls) ########\n\n", t.QualifiedTableName))

	if len(updates) == 0 {
		sb.WriteString("  No null differences found. No repairs needed.\n")
		sb.WriteString("\n######## END DRY RUN ########\n")
		return sb.String(), nil
	}

	var nodeNames []string
	for nodeName := range updates {
		nodeNames = append(nodeNames, nodeName)
	}
	sort.Strings(nodeNames)

	for _, nodeName := range nodeNames {
		nodeUpdates := updates[nodeName]
		if len(nodeUpdates) == 0 {
			continue
		}

		columnSet := make(map[string]struct{})
		cellCount := 0
		for _, nu := range nodeUpdates {
			cellCount += len(nu.columns)
			for col := range nu.columns {
				columnSet[col] = struct{}{}
			}
		}

		var columns []string
		for col := range columnSet {
			columns = append(columns, col)
		}
		sort.Strings(columns)

		sb.WriteString(fmt.Sprintf("  Node %s: Would update %d rows (%d column values) across columns [%s].\n",
			nodeName,
			len(nodeUpdates),
			cellCount,
			strings.Join(columns, ", "),
		))

		if t.report != nil && cellCount > 0 {
			t.populateFixNullsReport(nodeName, nodeUpdates, "would_update_rows")
		}
	}

	sb.WriteString("\n######## END DRY RUN ########\n")
	return sb.String(), nil
}

func (t *TableRepairTask) populateFixNullsReport(nodeName string, nodeUpdates map[string]*nullUpdate, field string) {
	if t.report == nil || len(nodeUpdates) == 0 {
		return
	}

	rows := make([]map[string]any, 0, len(nodeUpdates))
	for _, nu := range nodeUpdates {
		rowEntry := make(map[string]any, len(nu.pkMap)+1)
		for k, v := range nu.pkMap {
			rowEntry[k] = v
		}
		updatesCopy := make(map[string]any, len(nu.columns))
		for col, val := range nu.columns {
			updatesCopy[col] = val
		}
		rowEntry["updates"] = updatesCopy
		rows = append(rows, rowEntry)
	}

	if _, ok := t.report.Changes[nodeName]; !ok {
		t.report.Changes[nodeName] = make(map[string]any)
	}
	t.report.Changes[nodeName].(map[string]any)[field] = rows
}

func (t *TableRepairTask) applyFixNullsUpdates(tx pgx.Tx, column string, columnType string, updates []*nullUpdate, colTypes map[string]string, originInfoMap map[string]*rowOriginInfo, nodeName string) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}

	// Group updates by (origin, LSN, timestamp) if preserve-origin is enabled
	var originGroups map[originBatchKey][]*nullUpdate
	if t.PreserveOrigin && originInfoMap != nil && len(originInfoMap) > 0 {
		originGroups = make(map[originBatchKey][]*nullUpdate)
		for _, nu := range updates {
			pkeyStr, err := utils.StringifyKey(nu.pkMap, t.Key)
			if err != nil {
				// Try alternative method if available
				if nu.sourceRow != nil {
					pkeyStr, err = utils.StringifyOrderedMapKey(nu.sourceRow, t.Key)
				}
			}
			if err != nil {
				continue
			}
			originInfo, hasOrigin := originInfoMap[pkeyStr]
			var batchKey originBatchKey
			if hasOrigin && originInfo != nil {
				batchKey = makeOriginBatchKey(originInfo)
			}
			originGroups[batchKey] = append(originGroups[batchKey], nu)
		}
	} else {
		// No origin tracking - process all updates together
		originGroups = map[originBatchKey][]*nullUpdate{
			{}: updates,
		}
	}

	totalUpdated := 0

	// Track which origin sessions are already set up in this transaction
	setupSessions := make(map[string]bool)

	// Process each origin group separately
	for batchKey, originUpdates := range originGroups {
		if len(originUpdates) == 0 {
			continue
		}

		// Set up replication origin session and xact if we have origin info and preserve-origin is enabled.
		// If we cannot obtain an origin LSN, we gracefully fall back to regular repair for this group.
		preserveThisGroup := t.PreserveOrigin && batchKey.nodeOrigin != ""
		if preserveThisGroup {
			// Parse timestamp from batch key
			var commitTS *time.Time
			if batchKey.timestamp != "" {
				ts, err := time.Parse(time.RFC3339Nano, batchKey.timestamp)
				if err != nil {
					return totalUpdated, fmt.Errorf("failed to parse timestamp from batch key: %w", err)
				}
				commitTS = &ts
			}

			// Get or generate LSN for this batch
			// If LSN is in the batch key (from metadata), use it
			// Otherwise, fetch LSN from survivor node and add timestamp-based offset for uniqueness
			var lsn *uint64
			if batchKey.lsn != 0 {
				// LSN from metadata - use directly
				lsnCopy := batchKey.lsn
				lsn = &lsnCopy
			} else {
				// No LSN in metadata - need to fetch from survivor node and generate unique LSN per timestamp
				var survivorNode string
				for poolNode := range t.Pools {
					if poolNode != batchKey.nodeOrigin && poolNode != nodeName {
						survivorNode = poolNode
						break
					}
				}
				if survivorNode == "" && t.SourceOfTruth != "" && t.SourceOfTruth != batchKey.nodeOrigin {
					survivorNode = t.SourceOfTruth
				}

				if survivorNode == "" {
					return totalUpdated, fmt.Errorf("no survivor node available to fetch LSN for origin node %s", batchKey.nodeOrigin)
				}

				fetchedLSN, err := t.getOriginLSNForNode(batchKey.nodeOrigin, survivorNode)
				if err != nil {
					return totalUpdated, fmt.Errorf("failed to get origin LSN for node %s: %w", batchKey.nodeOrigin, err)
				}
				if fetchedLSN == nil {
					// Graceful fallback: skip origin-preserving setup for this group.
					preserveThisGroup = false
					logger.Warn("preserve-origin: falling back to regular fix-nulls for node %s (origin=%s, timestamp=%s)", nodeName, batchKey.nodeOrigin, batchKey.timestamp)
				}

				if preserveThisGroup {
					baseLSN := *fetchedLSN

					// Use base LSN + timestamp-based offset to ensure uniqueness per timestamp
					// This allows different timestamps to have different LSNs for proper tracking
					// The offset is derived from the timestamp to maintain ordering
					offset := uint64(0)
					if commitTS != nil {
						// Use microseconds since epoch as offset (modulo to keep reasonable)
						offset = uint64(commitTS.UnixMicro() % 1000000)
					}
					uniqueLSN := baseLSN + offset
					lsn = &uniqueLSN
				}
			}

			if preserveThisGroup {
				// Step 1: Setup session (once per origin node per transaction)
				if !setupSessions[batchKey.nodeOrigin] {
					_, err := t.setupReplicationOriginSession(tx, batchKey.nodeOrigin)
					if err != nil {
						return totalUpdated, fmt.Errorf("failed to setup replication origin session for node %s: %w", batchKey.nodeOrigin, err)
					}
					setupSessions[batchKey.nodeOrigin] = true
				}

				// Step 2: Setup xact with this batch's specific LSN and timestamp (BEFORE DML)
				if err := t.setupReplicationOriginXact(tx, lsn, commitTS); err != nil {
					t.resetReplicationOriginSession(tx) // Cleanup on error
					return totalUpdated, fmt.Errorf("failed to setup replication origin xact for node %s (LSN=%v, TS=%v): %w", batchKey.nodeOrigin, lsn, commitTS, err)
				}
			}
		}

		// Process batches for this origin group
		batchSize := 500
		for i := 0; i < len(originUpdates); i += batchSize {
			end := i + batchSize
			if end > len(originUpdates) {
				end = len(originUpdates)
			}
			batch := originUpdates[i:end]

			updateSQL, args, err := t.buildFixNullsBatchSQL(column, columnType, batch, colTypes)
			if err != nil {
				if preserveThisGroup {
					t.resetReplicationOriginXact(tx)
					t.resetReplicationOriginSession(tx)
				}
				return totalUpdated, err
			}

			tag, err := tx.Exec(t.Ctx, updateSQL, args...)
			if err != nil {
				if preserveThisGroup {
					t.resetReplicationOriginXact(tx)
					t.resetReplicationOriginSession(tx)
				}
				return totalUpdated, fmt.Errorf("error executing fix-nulls batch for column %s: %w", column, err)
			}
			totalUpdated += int(tag.RowsAffected())
		}

		// Note: Xact reset happens AFTER commit in the calling function
		// Session reset also happens AFTER commit in the calling function
	}

	return totalUpdated, nil
}

func (t *TableRepairTask) buildFixNullsBatchSQL(column string, columnType string, batch []*nullUpdate, colTypes map[string]string) (string, []any, error) {
	var sb strings.Builder
	args := make([]any, 0, len(batch)*(len(t.Key)+1))
	paramIdx := 1

	sb.WriteString("WITH updates(")
	for i, pkCol := range t.Key {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(pgx.Identifier{pkCol}.Sanitize())
	}
	sb.WriteString(", value) AS (VALUES ")

	for i, nu := range batch {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(")
		for j, pkCol := range t.Key {
			if j > 0 {
				sb.WriteString(", ")
			}
			pkType, ok := colTypes[pkCol]
			if !ok {
				return "", nil, fmt.Errorf("column type for primary key %s not found", pkCol)
			}
			convertedPK, err := convertValueForType(nu.pkValues[j], pkType)
			if err != nil {
				return "", nil, fmt.Errorf("convert primary key %s value: %w", pkCol, err)
			}
			sb.WriteString(fmt.Sprintf("$%d::%s", paramIdx, pkType))
			args = append(args, convertedPK)
			paramIdx++
		}
		sb.WriteString(", ")
		convertedVal, err := convertValueForType(nu.columns[column], columnType)
		if err != nil {
			return "", nil, fmt.Errorf("convert value for column %s: %w", column, err)
		}
		sb.WriteString(fmt.Sprintf("$%d::%s", paramIdx, columnType))
		args = append(args, convertedVal)
		paramIdx++
		sb.WriteString(")")
	}

	sb.WriteString(") UPDATE ")
	sb.WriteString(pgx.Identifier{t.Schema}.Sanitize())
	sb.WriteString(".")
	sb.WriteString(pgx.Identifier{t.Table}.Sanitize())
	sb.WriteString(" AS t SET ")
	sb.WriteString(pgx.Identifier{column}.Sanitize())
	sb.WriteString(" = updates.value::")
	sb.WriteString(columnType)
	sb.WriteString(" FROM updates WHERE ")

	for i, pkCol := range t.Key {
		if i > 0 {
			sb.WriteString(" AND ")
		}
		ident := pgx.Identifier{pkCol}.Sanitize()
		sb.WriteString("t.")
		sb.WriteString(ident)
		sb.WriteString(" = updates.")
		sb.WriteString(ident)
	}

	return sb.String(), args, nil
}

func convertValueForType(val any, colType string) (any, error) {
	if val == nil {
		return nil, nil
	}

	if tVal, ok := val.(time.Time); ok {
		return tVal, nil
	}

	return utils.ConvertToPgxType(val, colType)
}

func (t *TableRepairTask) getColTypesForNode(nodeName string) (map[string]string, string, error) {
	for hostPort, mappedName := range t.HostMap {
		if mappedName == nodeName {
			if colTypes, ok := t.ColTypes[hostPort]; ok {
				return colTypes, hostPort, nil
			}
			return nil, hostPort, fmt.Errorf("column types for target node '%s' (key: %s) not found", nodeName, hostPort)
		}
	}

	return nil, "", fmt.Errorf("could not find host:port key for target node %s to get col types", nodeName)
}

func (t *TableRepairTask) runUnidirectionalRepair(startTime time.Time) error {
	logger.Info("Starting table repair for %s on cluster %s", t.QualifiedTableName, t.ClusterName)

	// Core repair logic begins here
	totalOps := make(map[string]map[string]int) // node -> "upserted"/"deleted" -> count
	var repairErrors []string

	divergentNodes := make(map[string]bool)
	fullUpserts, fullDeletes, err := calculateRepairSets(t)
	if err != nil {
		return fmt.Errorf("failed to calculate repair sets: %w", err)
	}
	for nodeName := range fullUpserts {
		divergentNodes[nodeName] = true
		totalOps[nodeName] = map[string]int{"upserted": 0, "deleted": 0}
	}
	for nodeName := range fullDeletes {
		divergentNodes[nodeName] = true
		if _, exists := totalOps[nodeName]; !exists {
			totalOps[nodeName] = map[string]int{"upserted": 0, "deleted": 0}
		}
	}

	/* NOTE: We will be skipping checking the spock version entirely since
	 * most users are on spock 4.0 or above.
	 */

	skipDeletes := (t.UpsertOnly || t.InsertOnly) && t.RepairPlan == nil

	for nodeName := range divergentNodes {
		logger.Info("Processing repairs for divergent node: %s", nodeName)
		divergentPool, ok := t.Pools[nodeName]
		if !ok || divergentPool == nil {
			logger.Debug("Connection pool for divergent node %s not found, attempting to connect.", nodeName)
			var nodeInfo map[string]any
			for _, ni := range t.ClusterNodes {
				if name, _ := ni["Name"].(string); name == nodeName {
					nodeInfo = ni
					break
				}
			}

			if nodeInfo == nil {
				logger.Error("Could not find node info for %s. Skipping repairs for this node.", nodeName)
				repairErrors = append(repairErrors, fmt.Sprintf("no node info for %s", nodeName))
				continue
			}

			var err error
			divergentPool, err = auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
			if err != nil {
				logger.Error("Failed to connect to node %s: %v. Skipping repairs for this node.", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("connection failed for %s: %v", nodeName, err))
				continue
			}
			t.Pools[nodeName] = divergentPool
			logger.Debug("Successfully connected to node %s and created a new connection pool.", nodeName)
		}

		tx, err := divergentPool.Begin(t.Ctx)
		if err != nil {
			logger.Error("starting transaction on node %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("tx begin failed for %s: %v", nodeName, err))
			continue
		}

		var spockRepairModeActive bool = false
		_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(true)")
		if err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("enabling spock.repair_mode(true) on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(true) failed for %s: %v", nodeName, err))
			continue
		}
		spockRepairModeActive = true
		logger.Debug("spock.repair_mode(true) set on %s", nodeName)

		if t.FireTriggers {
			_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'local'")
		} else {
			_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'replica'")
		}
		if err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("setting session_replication_role on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("session_replication_role failed for %s: %v", nodeName, err))
			continue
		}
		logger.Debug("session_replication_role set on %s (fire_triggers: %v)", nodeName, t.FireTriggers)

		if err := t.setRole(tx, nodeName); err != nil {
			tx.Rollback(t.Ctx)
			logger.Error("%v", err)
			repairErrors = append(repairErrors, err.Error())
			continue
		}

		// TODO: DROP PRIVILEGES HERE!

		// Process deletes first
		if !skipDeletes {
			nodeDeletes := fullDeletes[nodeName]
			if len(nodeDeletes) > 0 {
				deletedCount, err := executeDeletes(t.Ctx, tx, t, nodeName, nodeDeletes, nil)
				if err != nil {
					tx.Rollback(t.Ctx)
					logger.Error("executing deletes on node %s: %v", nodeName, err)
					repairErrors = append(repairErrors, fmt.Sprintf("delete ops failed for %s: %v", nodeName, err))
					continue
				}
				totalOps[nodeName]["deleted"] = deletedCount
				logger.Info("Executed %d delete operations on %s", deletedCount, nodeName)

				if t.report != nil {
					if _, ok := t.report.Changes[nodeName]; !ok {
						t.report.Changes[nodeName] = make(map[string]any)
					}
					rows := make([]map[string]any, 0, len(nodeDeletes))
					for _, row := range nodeDeletes {
						rows = append(rows, row)
					}
					t.report.Changes[nodeName].(map[string]any)["deleted_rows"] = rows
					if t.RepairPlan != nil && len(t.planRuleMatches[nodeName]) > 0 {
						t.report.Changes[nodeName].(map[string]any)["rule_matches"] = t.planRuleMatches[nodeName]
					}
				}
			}
		}

		// And now for the upserts
		nodeUpserts := fullUpserts[nodeName]
		if len(nodeUpserts) > 0 {
			targetNodeColTypes, _, err := t.getColTypesForNode(nodeName)
			if err != nil {
				tx.Rollback(t.Ctx)
				errStr := fmt.Sprintf("column types for target node '%s' not found for upserts: %v", nodeName, err)
				logger.Error("%s", errStr)
				repairErrors = append(repairErrors, errStr)
				continue
			}

			// Extract origin information from source rows if preserve-origin is enabled
			var originInfoMap map[string]*rowOriginInfo
			if t.PreserveOrigin {
				originInfoMap = make(map[string]*rowOriginInfo)
				// Extract origin info from all source rows in the diff
				// For repair plans, we'll extract from both nodes and use the appropriate one
				// For source-of-truth repairs, we extract from the source of truth
				for nodePair, diffs := range t.RawDiffs.NodeDiffs {
					nodes := strings.Split(nodePair, "/")
					if len(nodes) != 2 {
						continue
					}
					node1Name, node2Name := nodes[0], nodes[1]

					// Extract from both nodes - we'll use the one that matches the source
					for _, sourceNode := range []string{node1Name, node2Name} {
						sourceRows := diffs.Rows[sourceNode]
						for _, row := range sourceRows {
							pkeyStr, err := utils.StringifyOrderedMapKey(row, t.Key)
							if err != nil {
								continue
							}
							// Only add if this row is being upserted to the target node
							if _, isBeingUpserted := nodeUpserts[pkeyStr]; isBeingUpserted {
								if originInfo := extractOriginInfoFromRow(row); originInfo != nil {
									// For repair plans, prefer the source node that's providing the data
									// For source-of-truth, prefer the source of truth
									if t.RepairPlan == nil {
										// Source-of-truth: only use if it's the source of truth
										if sourceNode == t.SourceOfTruth {
											originInfoMap[pkeyStr] = originInfo
										}
									} else {
										// Repair plan: use the first one we find (will be overridden if needed)
										if _, exists := originInfoMap[pkeyStr]; !exists {
											originInfoMap[pkeyStr] = originInfo
										}
									}
								}
							}
						}
					}
				}
			}

			// Close the current transaction before executing upserts with per-timestamp transactions
			// Reset spock.repair_mode temporarily
			if spockRepairModeActive {
				_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(false)")
				if err != nil {
					tx.Rollback(t.Ctx)
					logger.Error("disabling spock.repair_mode(false) on %s before upserts: %v", nodeName, err)
					repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(false) failed for %s: %v", nodeName, err))
					continue
				}
			}

			// Commit the current transaction (which handled deletes if any)
			logger.Debug("Committing transaction on %s before calling executeUpsertsWithTimestamps", nodeName)
			err = tx.Commit(t.Ctx)
			if err != nil {
				// Note: If Commit fails, transaction is automatically rolled back by PostgreSQL
				logger.Error("committing transaction on %s before upserts: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("commit failed for %s: %v", nodeName, err))
				continue
			}
			logger.Debug("Successfully committed transaction, now calling executeUpsertsWithTimestamps")

			// Execute upserts with per-timestamp transactions
			upsertedCount, err := executeUpsertsWithTimestamps(divergentPool, t, nodeName, nodeUpserts, targetNodeColTypes, originInfoMap)
			if err != nil {
				logger.Error("executing upserts on node %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("upsert ops failed for %s: %v", nodeName, err))
				continue
			}
			totalOps[nodeName]["upserted"] = upsertedCount
			logger.Info("Executed %d upsert operations on %s", upsertedCount, nodeName)

			if t.report != nil {
				if _, ok := t.report.Changes[nodeName]; !ok {
					t.report.Changes[nodeName] = make(map[string]any)
				}
				rows := make([]map[string]any, 0, len(nodeUpserts))
				for _, row := range nodeUpserts {
					rows = append(rows, row)
				}
				t.report.Changes[nodeName].(map[string]any)["upserted_rows"] = rows
				if t.RepairPlan != nil && len(t.planRuleMatches[nodeName]) > 0 {
					t.report.Changes[nodeName].(map[string]any)["rule_matches"] = t.planRuleMatches[nodeName]
				}
			}

			// All transactions for this node are now complete, continue to next node
			continue
		}

		// If we reach here, there were no upserts, so commit the delete transaction

		if spockRepairModeActive {
			// TODO: Need to elevate privileges here, but might be difficult
			// with pgx transactions and connection pooling.
			_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(false)")
			if err != nil {
				tx.Rollback(t.Ctx)
				logger.Error("disabling spock.repair_mode(false) on %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(false) failed for %s: %v", nodeName, err))
				continue
			}
			logger.Debug("spock.repair_mode(false) set on %s", nodeName)
		}

		err = tx.Commit(t.Ctx)
		if err != nil {
			logger.Error("committing transaction on node %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("commit failed for %s: %v", nodeName, err))
			continue
		}
		logger.Debug("Transaction committed successfully on %s", nodeName)
	}

	t.FinishedAt = time.Now()
	t.TimeTaken = float64(t.FinishedAt.Sub(startTime).Milliseconds())
	runTimeStr := fmt.Sprintf("%.3fs", t.TimeTaken/1000)

	if len(repairErrors) > 0 {
		logger.Error("Repair of %s failed in %s with errors: %s", t.QualifiedTableName, runTimeStr, strings.Join(repairErrors, "; "))
		t.TaskStatus = "FAILED"
		t.TaskContext = strings.Join(repairErrors, "; ")
	} else {
		totalUpserted := 0
		totalDeleted := 0
		repairedNodes := make(map[string]bool)
		for node, ops := range totalOps {
			if ops["upserted"] > 0 || ops["deleted"] > 0 {
				repairedNodes[node] = true
			}
			totalUpserted += ops["upserted"]
			totalDeleted += ops["deleted"]
		}

		summaryParts := []string{}
		if totalUpserted > 0 {
			op := "upserted"
			if t.InsertOnly {
				op = "inserted"
			}
			summaryParts = append(summaryParts, fmt.Sprintf("%d %s", totalUpserted, op))
		}
		if totalDeleted > 0 {
			summaryParts = append(summaryParts, fmt.Sprintf("%d deleted", totalDeleted))
		}

		if len(repairedNodes) > 0 {
			nodeList := []string{}
			for node := range repairedNodes {
				nodeList = append(nodeList, node)
			}
			logger.Info("Repair of %s complete in %s. Nodes %s repaired (%s).",
				t.QualifiedTableName,
				runTimeStr,
				strings.Join(nodeList, ", "),
				strings.Join(summaryParts, ", "),
			)
		} else {
			logger.Info("Repair of %s complete in %s. No differences found.",
				t.QualifiedTableName,
				runTimeStr,
			)
		}

		t.TaskStatus = "COMPLETED"
		summary := strings.Builder{}
		for node, ops := range totalOps {
			summary.WriteString(fmt.Sprintf("Node %s: %d upserted, %d deleted. ", node, ops["upserted"], ops["deleted"]))
		}
		t.TaskContext = strings.TrimSpace(summary.String())
	}

	return nil
}

func (t *TableRepairTask) runBidirectionalRepair() error {
	startTime := time.Now()
	logger.Info("Starting bidirectional table repair for %s on cluster %s", t.QualifiedTableName, t.ClusterName)

	totalOps := make(map[string]int)
	var repairErrors []string

	for nodePairKey, diffs := range t.RawDiffs.NodeDiffs {
		nodes := strings.Split(nodePairKey, "/")
		if len(nodes) != 2 {
			logger.Warn("Warning: Invalid node pair key '%s', skipping", nodePairKey)
			continue
		}
		node1Name, node2Name := nodes[0], nodes[1]
		logger.Info("Processing node pair: %s/%s", node1Name, node2Name)

		node1Rows := diffs.Rows[node1Name]
		node2Rows := diffs.Rows[node2Name]

		node1RowsByPKey := make(map[string]types.OrderedMap)
		for _, row := range node1Rows {
			pkeyStr, err := utils.StringifyOrderedMapKey(row, t.Key)
			if err != nil {
				repairErrors = append(repairErrors, fmt.Sprintf("stringify pkey failed for %s: %v", node1Name, err))
				continue
			}
			node1RowsByPKey[pkeyStr] = row
		}

		node2RowsByPKey := make(map[string]types.OrderedMap)
		for _, row := range node2Rows {
			pkeyStr, err := utils.StringifyOrderedMapKey(row, t.Key)
			if err != nil {
				repairErrors = append(repairErrors, fmt.Sprintf("stringify pkey failed for %s: %v", node2Name, err))
				continue
			}
			node2RowsByPKey[pkeyStr] = row
		}

		insertsForNode1 := make(map[string]map[string]any)
		for pkey, row := range node2RowsByPKey {
			if _, exists := node1RowsByPKey[pkey]; !exists {
				insertsForNode1[pkey] = utils.StripSpockMetadata(utils.OrderedMapToMap(row))
			}
		}

		insertsForNode2 := make(map[string]map[string]any)
		for pkey, row := range node1RowsByPKey {
			if _, exists := node2RowsByPKey[pkey]; !exists {
				insertsForNode2[pkey] = utils.StripSpockMetadata(utils.OrderedMapToMap(row))
			}
		}

		if len(insertsForNode1) > 0 {
			count, err := t.performBirectionalInserts(node1Name, insertsForNode1)
			if err != nil {
				repairErrors = append(repairErrors, fmt.Sprintf("inserts failed for %s: %v", node1Name, err))
			} else {
				totalOps[node1Name] += count
				if t.report != nil {
					if _, ok := t.report.Changes[nodePairKey]; !ok {
						t.report.Changes[nodePairKey] = make(map[string]any)
					}
					changeMap := t.report.Changes[nodePairKey].(map[string]any)
					if _, ok := changeMap[node1Name]; !ok {
						changeMap[node1Name] = make(map[string]any)
					}
					rows := make([]map[string]any, 0, len(insertsForNode1))
					for _, row := range insertsForNode1 {
						rows = append(rows, row)
					}
					changeMap[node1Name].(map[string]any)["inserted_rows"] = rows
				}
			}
		}
		if len(insertsForNode2) > 0 {
			count, err := t.performBirectionalInserts(node2Name, insertsForNode2)
			if err != nil {
				repairErrors = append(repairErrors, fmt.Sprintf("inserts failed for %s: %v", node2Name, err))
			} else {
				totalOps[node2Name] += count
				if t.report != nil {
					if _, ok := t.report.Changes[nodePairKey]; !ok {
						t.report.Changes[nodePairKey] = make(map[string]any)
					}
					changeMap := t.report.Changes[nodePairKey].(map[string]any)
					if _, ok := changeMap[node2Name]; !ok {
						changeMap[node2Name] = make(map[string]any)
					}
					rows := make([]map[string]any, 0, len(insertsForNode2))
					for _, row := range insertsForNode2 {
						rows = append(rows, row)
					}
					changeMap[node2Name].(map[string]any)["inserted_rows"] = rows
				}
			}
		}
	}

	t.FinishedAt = time.Now()
	t.TimeTaken = float64(t.FinishedAt.Sub(startTime).Milliseconds())
	runTimeStr := fmt.Sprintf("%.3fs", t.TimeTaken/1000)

	if len(repairErrors) > 0 {
		logger.Error("Bidirectional repair of %s failed in %s with errors: %s", t.QualifiedTableName, runTimeStr, strings.Join(repairErrors, "; "))
		t.TaskStatus = "FAILED"
		t.TaskContext = strings.Join(repairErrors, "; ")
	} else {
		totalInserted := 0
		repairedNodes := make(map[string]bool)
		for node, count := range totalOps {
			if count > 0 {
				repairedNodes[node] = true
			}
			totalInserted += count
		}

		if totalInserted > 0 {
			nodeList := []string{}
			for node := range repairedNodes {
				nodeList = append(nodeList, node)
			}
			logger.Info("Bidirectional repair of %s complete in %s. Nodes %s repaired (%d rows inserted).",
				t.QualifiedTableName,
				runTimeStr,
				strings.Join(nodeList, ", "),
				totalInserted,
			)
		} else {
			logger.Info("Bidirectional repair of %s complete in %s. No differences found.",
				t.QualifiedTableName,
				runTimeStr,
			)
		}
		t.TaskStatus = "COMPLETED"
		summary := strings.Builder{}
		for node, count := range totalOps {
			summary.WriteString(fmt.Sprintf("Node %s: %d inserted. ", node, count))
		}
		t.TaskContext = strings.TrimSpace(summary.String())
	}

	return nil
}

func (t *TableRepairTask) performBirectionalInserts(nodeName string, inserts map[string]map[string]any) (int, error) {
	pool, ok := t.Pools[nodeName]
	if !ok || pool == nil {
		return 0, fmt.Errorf("no connection pool for node %s", nodeName)
	}

	tx, err := pool.Begin(t.Ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction on %s: %w", nodeName, err)
	}
	defer tx.Rollback(t.Ctx)

	_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(true)")
	if err != nil {
		return 0, fmt.Errorf("failed to enable spock.repair_mode(true) on %s: %w", nodeName, err)
	}
	logger.Info("spock.repair_mode(true) set on %s", nodeName)

	if t.FireTriggers {
		_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'local'")
	} else {
		_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'replica'")
	}
	if err != nil {
		return 0, fmt.Errorf("failed to set session_replication_role on %s: %w", nodeName, err)
	}
	logger.Info("session_replication_role set on %s (fire_triggers: %v)", nodeName, t.FireTriggers)

	if err := t.setRole(tx, nodeName); err != nil {
		return 0, err
	}

	targetNodeHostPortKey := ""
	for hostPort, mappedName := range t.HostMap {
		if mappedName == nodeName {
			targetNodeHostPortKey = hostPort
			break
		}
	}
	if targetNodeHostPortKey == "" {
		return 0, fmt.Errorf("could not find host:port key for target node %s", nodeName)
	}
	targetNodeColTypes, ok := t.ColTypes[targetNodeHostPortKey]
	if !ok {
		return 0, fmt.Errorf("column types for target node '%s' (key: %s) not found", nodeName, targetNodeHostPortKey)
	}

	// Bidirectional is always insert only
	originalInsertOnly := t.InsertOnly
	t.InsertOnly = true
	// Extract origin information from source rows for bidirectional repair
	var originInfoMap map[string]*rowOriginInfo
	if t.PreserveOrigin {
		originInfoMap = make(map[string]*rowOriginInfo)
		// For bidirectional, origin is the node providing the data
		// We need to find which node pair this insert came from
		for nodePairKey, diffs := range t.RawDiffs.NodeDiffs {
			nodes := strings.Split(nodePairKey, "/")
			if len(nodes) != 2 {
				continue
			}
			var sourceNode string
			if nodes[0] == nodeName {
				sourceNode = nodes[1] // Data coming from the other node
			} else if nodes[1] == nodeName {
				sourceNode = nodes[0] // Data coming from the other node
			} else {
				continue
			}

			sourceRows := diffs.Rows[sourceNode]
			for _, row := range sourceRows {
				pkeyStr, err := utils.StringifyOrderedMapKey(row, t.Key)
				if err != nil {
					continue
				}
				if _, exists := inserts[pkeyStr]; exists {
					if originInfo := extractOriginInfoFromRow(row); originInfo != nil {
						originInfoMap[pkeyStr] = originInfo
					}
				}
			}
		}
	}

	insertedCount, err := executeUpserts(tx, t, nodeName, inserts, targetNodeColTypes, originInfoMap)
	t.InsertOnly = originalInsertOnly

	if err != nil {
		return 0, fmt.Errorf("failed to execute inserts on %s: %w", nodeName, err)
	}
	logger.Info("Executed %d insert operations on %s", insertedCount, nodeName)

	_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(false)")
	if err != nil {
		return 0, fmt.Errorf("failed to disable spock.repair_mode(false) on %s: %w", nodeName, err)
	}
	logger.Info("spock.repair_mode(false) set on %s", nodeName)

	if err := tx.Commit(t.Ctx); err != nil {
		return 0, fmt.Errorf("failed to commit transaction on %s: %w", nodeName, err)
	}
	logger.Info("Transaction committed successfully on %s", nodeName)

	return insertedCount, nil
}

// executeDeletes handles deleting rows in batches.
func executeDeletes(ctx context.Context, tx pgx.Tx, task *TableRepairTask, nodeName string, deletes map[string]map[string]any, colTypes map[string]string) (int, error) {
	if err := task.filterStaleRepairs(ctx, tx, nodeName, deletes, colTypes, "delete"); err != nil {
		return 0, err
	}

	keysToDelete := make([]any, 0, len(deletes))

	for pkeyString := range deletes {
		rowMap := deletes[pkeyString]
		if task.SimplePrimaryKey {
			pkeyValue, ok := rowMap[task.Key[0]]
			if !ok {
				return 0, fmt.Errorf("primary key column %s not found in row data for pkey string %s", task.Key[0], pkeyString)
			}
			keysToDelete = append(keysToDelete, pkeyValue)
		} else {
			compositeKey := make([]any, len(task.Key))
			for i, keyCol := range task.Key {
				pkeyValue, ok := rowMap[keyCol]
				if !ok {
					return 0, fmt.Errorf("composite primary key column %s not found in row data for pkey string %s", keyCol, pkeyString)
				}
				compositeKey[i] = pkeyValue
			}
			keysToDelete = append(keysToDelete, compositeKey)
		}
	}

	if len(keysToDelete) == 0 {
		return 0, nil
	}

	totalDeletedCount := 0
	// TODO: Make this configurable
	batchSize := 1000

	tableIdent := pgx.Identifier{task.Schema, task.Table}.Sanitize()

	for i := 0; i < len(keysToDelete); i += batchSize {
		end := i + batchSize
		if end > len(keysToDelete) {
			end = len(keysToDelete)
		}
		batchKeys := keysToDelete[i:end]

		var deleteSQL strings.Builder
		args := []any{}
		paramIdx := 1

		deleteSQL.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", tableIdent))

		if task.SimplePrimaryKey {
			deleteSQL.WriteString(fmt.Sprintf("%s IN (", pgx.Identifier{task.Key[0]}.Sanitize()))
			for j, key := range batchKeys {
				if j > 0 {
					deleteSQL.WriteString(", ")
				}
				deleteSQL.WriteString(fmt.Sprintf("$%d", paramIdx))
				args = append(args, key)
				paramIdx++
			}
			deleteSQL.WriteString(")")
		} else {
			keyColSanitised := make([]string, len(task.Key))
			for k, keyCol := range task.Key {
				keyColSanitised[k] = pgx.Identifier{keyCol}.Sanitize()
			}

			deleteSQL.WriteString(fmt.Sprintf("(%s) IN (", strings.Join(keyColSanitised, ", ")))

			for j, key := range batchKeys {
				compositeKey, ok := key.([]any)
				if !ok {
					return 0, fmt.Errorf("expected composite key to be []interface{}, got %T", key)
				}
				if len(compositeKey) != len(task.Key) {
					return 0, fmt.Errorf("composite key length mismatch: expected %d, got %d", len(task.Key), len(compositeKey))
				}
				if j > 0 {
					deleteSQL.WriteString(", ")
				}
				deleteSQL.WriteString("(")
				for k, val := range compositeKey {
					if k > 0 {
						deleteSQL.WriteString(", ")
					}
					deleteSQL.WriteString(fmt.Sprintf("$%d", paramIdx))
					args = append(args, val)
					paramIdx++
				}
				deleteSQL.WriteString(")")
			}
			deleteSQL.WriteString(")")
		}

		cmdTag, err := tx.Exec(ctx, deleteSQL.String(), args...)
		if err != nil {
			return totalDeletedCount, fmt.Errorf("error executing delete batch: %w (SQL: %s, Args: %v)", err, deleteSQL.String(), args)
		}
		totalDeletedCount += int(cmdTag.RowsAffected())
	}

	return totalDeletedCount, nil
}

// rowOriginInfo holds origin metadata for a row
type rowOriginInfo struct {
	nodeOrigin string
	commitTS   *time.Time
	lsn        *uint64
}

// originBatchKey is used to group rows by their origin node, LSN, and timestamp
// for per-row accurate preserve-origin tracking. Rows with identical keys are
// batched together and processed with a single xact setup.
type originBatchKey struct {
	nodeOrigin string
	lsn        uint64 // 0 if nil
	timestamp  string // empty if nil, RFC3339Nano format for comparison
}

// makeOriginBatchKey creates a batch key from origin info for grouping rows.
// Returns zero-value key for rows without origin information.
func makeOriginBatchKey(originInfo *rowOriginInfo) originBatchKey {
	if originInfo == nil || originInfo.nodeOrigin == "" {
		return originBatchKey{} // zero value
	}

	key := originBatchKey{nodeOrigin: originInfo.nodeOrigin}

	if originInfo.lsn != nil {
		key.lsn = *originInfo.lsn
	}

	if originInfo.commitTS != nil {
		key.timestamp = originInfo.commitTS.Format(time.RFC3339Nano)
	}

	return key
}

// extractOriginInfoFromRow extracts origin information from a row's metadata.
// Returns nil if no origin information is available.
func extractOriginInfoFromRow(row types.OrderedMap) *rowOriginInfo {
	rowMap := utils.OrderedMapToMap(row)

	// Check for metadata in _spock_metadata_ field
	var meta map[string]any
	if rawMeta, ok := rowMap["_spock_metadata_"].(map[string]any); ok {
		meta = rawMeta
	} else {
		meta = make(map[string]any)
	}

	// Also check for direct fields (for backward compatibility)
	if val, ok := rowMap["node_origin"]; ok {
		meta["node_origin"] = val
	}
	if val, ok := rowMap["commit_ts"]; ok {
		meta["commit_ts"] = val
	}

	var nodeOrigin string
	var commitTS *time.Time

	if originVal, ok := meta["node_origin"]; ok && originVal != nil {
		originStr := strings.TrimSpace(fmt.Sprintf("%v", originVal))
		if originStr != "" && originStr != "0" && originStr != "local" {
			nodeOrigin = originStr
		}
	}

	if tsVal, ok := meta["commit_ts"]; ok && tsVal != nil {
		var ts time.Time
		var err error
		switch v := tsVal.(type) {
		case time.Time:
			ts = v
		case string:
			ts, err = time.Parse(time.RFC3339, v)
			if err != nil {
				// Try other formats
				ts, err = time.Parse("2006-01-02 15:04:05.999999-07", v)
			}
		}
		if err == nil {
			commitTS = &ts
		}
	}

	if nodeOrigin == "" {
		return nil
	}

	return &rowOriginInfo{
		nodeOrigin: nodeOrigin,
		commitTS:   commitTS,
	}
}

// executeUpserts handles upserting rows in batches.
// originInfoMap maps primary key strings to their origin information.
// If originInfoMap is nil or empty, origin tracking is skipped.
func executeUpserts(tx pgx.Tx, task *TableRepairTask, nodeName string, upserts map[string]map[string]any, colTypes map[string]string, originInfoMap map[string]*rowOriginInfo) (int, error) {
	if err := task.filterStaleRepairs(task.Ctx, tx, nodeName, upserts, colTypes, "upsert"); err != nil {
		return 0, err
	}

	// Group rows by (origin, LSN, timestamp) if preserve-origin is enabled and we have origin info
	var originGroups map[originBatchKey]map[string]map[string]any // batchKey -> pkey -> row
	rowsWithoutOrigin := 0
	if task.PreserveOrigin && originInfoMap != nil && len(originInfoMap) > 0 {
		originGroups = make(map[originBatchKey]map[string]map[string]any)
		for pkey, row := range upserts {
			originInfo, hasOrigin := originInfoMap[pkey]
			var batchKey originBatchKey
			if hasOrigin && originInfo != nil {
				batchKey = makeOriginBatchKey(originInfo)
				if batchKey.nodeOrigin == "" {
					rowsWithoutOrigin++
				}
			} else {
				rowsWithoutOrigin++
			}
			if originGroups[batchKey] == nil {
				originGroups[batchKey] = make(map[string]map[string]any)
			}
			originGroups[batchKey][pkey] = row
		}
		if rowsWithoutOrigin > 0 {
			logger.Warn("preserve-origin enabled but %d rows missing origin metadata - these will be repaired without origin tracking", rowsWithoutOrigin)
		}
	} else {
		// No origin tracking - process all rows together
		originGroups = map[originBatchKey]map[string]map[string]any{
			{}: upserts,
		}
	}

	totalUpsertedCount := 0
	orderedCols := task.Cols

	// Track which origin sessions are already set up in this transaction
	setupSessions := make(map[string]bool)

	// Process each origin group separately
	for batchKey, originUpserts := range originGroups {
		if len(originUpserts) == 0 {
			continue
		}

		// Set up replication origin session and xact if we have origin info and preserve-origin is enabled.
		// If we cannot obtain an origin LSN, we gracefully fall back to regular repair for this group.
		preserveThisGroup := task.PreserveOrigin && batchKey.nodeOrigin != ""
		if preserveThisGroup {
			// Parse timestamp from batch key
			var commitTS *time.Time
			if batchKey.timestamp != "" {
				ts, err := time.Parse(time.RFC3339Nano, batchKey.timestamp)
				if err != nil {
					return totalUpsertedCount, fmt.Errorf("failed to parse timestamp from batch key: %w", err)
				}
				commitTS = &ts
			}

			// Get or generate LSN for this batch
			// If LSN is in the batch key (from metadata), use it
			// Otherwise, fetch LSN from survivor node and add timestamp-based offset for uniqueness
			var lsn *uint64
			if batchKey.lsn != 0 {
				// LSN from metadata - use directly
				lsnCopy := batchKey.lsn
				lsn = &lsnCopy
			} else {
				// No LSN in metadata - need to fetch from survivor node and generate unique LSN per timestamp
				var survivorNode string
				for poolNode := range task.Pools {
					if poolNode != batchKey.nodeOrigin && poolNode != nodeName {
						survivorNode = poolNode
						break
					}
				}
				if survivorNode == "" && task.SourceOfTruth != "" && task.SourceOfTruth != batchKey.nodeOrigin {
					survivorNode = task.SourceOfTruth
				}

				if survivorNode == "" {
					return totalUpsertedCount, fmt.Errorf("no survivor node available to fetch LSN for origin node %s", batchKey.nodeOrigin)
				}

				fetchedLSN, err := task.getOriginLSNForNode(batchKey.nodeOrigin, survivorNode)
				if err != nil {
					return totalUpsertedCount, fmt.Errorf("failed to get origin LSN for node %s: %w", batchKey.nodeOrigin, err)
				}
				if fetchedLSN == nil {
					// Graceful fallback: skip origin-preserving setup for this group.
					preserveThisGroup = false
					logger.Warn("preserve-origin: falling back to regular upsert for node %s (origin=%s, timestamp=%s)", nodeName, batchKey.nodeOrigin, batchKey.timestamp)
				}

				if preserveThisGroup {
					baseLSN := *fetchedLSN

					// Use base LSN + timestamp-based offset to ensure uniqueness per timestamp
					// This allows different timestamps to have different LSNs for proper tracking
					// The offset is derived from the timestamp to maintain ordering
					offset := uint64(0)
					if commitTS != nil {
						// Use microseconds since epoch as offset (modulo to keep reasonable)
						offset = uint64(commitTS.UnixMicro() % 1000000)
					}
					uniqueLSN := baseLSN + offset
					lsn = &uniqueLSN
				}
			}

			if preserveThisGroup {
				// Step 1: Setup session (once per origin node per transaction)
				if !setupSessions[batchKey.nodeOrigin] {
					_, err := task.setupReplicationOriginSession(tx, batchKey.nodeOrigin)
					if err != nil {
						return totalUpsertedCount, fmt.Errorf("failed to setup replication origin session for node %s: %w", batchKey.nodeOrigin, err)
					}
					setupSessions[batchKey.nodeOrigin] = true
				}

				// Step 2: Setup xact with this batch's specific LSN and timestamp (BEFORE DML)
				if err := task.setupReplicationOriginXact(tx, lsn, commitTS); err != nil {
					task.resetReplicationOriginSession(tx) // Cleanup on error
					return totalUpsertedCount, fmt.Errorf("failed to setup replication origin xact for node %s (LSN=%v, TS=%v): %w", batchKey.nodeOrigin, lsn, commitTS, err)
				}
			}
		}

		// Convert rows to typed format
		rowsToUpsert := make([][]any, 0, len(originUpserts))
		for _, rowMap := range originUpserts {
			typedRow := make([]any, len(orderedCols))
			for i, colName := range orderedCols {
				val, valExists := rowMap[colName]
				pgType, typeExists := colTypes[colName]

				if !valExists {
					typedRow[i] = nil
					continue
				}
				if !typeExists {
					return totalUpsertedCount, fmt.Errorf("type for column %s not found in target node's colTypes", colName)
				}

				convertedVal, err := utils.ConvertToPgxType(val, pgType)
				if err != nil {
					return totalUpsertedCount, fmt.Errorf("error converting value for column %s (value: %v, type: %s): %w", colName, val, pgType, err)
				}
				typedRow[i] = convertedVal
			}
			rowsToUpsert = append(rowsToUpsert, typedRow)
		}

		if len(rowsToUpsert) == 0 {
			// Reset xact if we set it up
			if preserveThisGroup {
				task.resetReplicationOriginXact(tx)
			}
			continue
		}

		// Process batches for this origin group
		// TODO: Make this configurable
		batchSize := 1000

		// For the max placeholders issue
		if len(orderedCols) > 0 && batchSize*len(orderedCols) > 65500 {
			batchSize = 65500 / len(orderedCols)
			if batchSize == 0 {
				batchSize = 1
			}
		}

		tableIdent := pgx.Identifier{task.Schema, task.Table}.Sanitize()
		colIdents := make([]string, len(orderedCols))
		for i, col := range orderedCols {
			colIdents[i] = pgx.Identifier{col}.Sanitize()
		}
		colsSQL := strings.Join(colIdents, ", ")

		pkColIdents := make([]string, len(task.Key))
		for i, pkCol := range task.Key {
			pkColIdents[i] = pgx.Identifier{pkCol}.Sanitize()
		}
		pkSQL := strings.Join(pkColIdents, ", ")

		for i := 0; i < len(rowsToUpsert); i += batchSize {
			end := i + batchSize
			if end > len(rowsToUpsert) {
				end = len(rowsToUpsert)
			}
			batchRows := rowsToUpsert[i:end]

			var upsertSQL strings.Builder
			args := []any{}
			paramIdx := 1

			upsertSQL.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableIdent, colsSQL))
			for j, row := range batchRows {
				if j > 0 {
					upsertSQL.WriteString(", ")
				}
				upsertSQL.WriteString("(")
				for k, val := range row {
					if k > 0 {
						upsertSQL.WriteString(", ")
					}
					upsertSQL.WriteString(fmt.Sprintf("$%d", paramIdx))
					args = append(args, val)
					paramIdx++
				}
				upsertSQL.WriteString(")")
			}

			upsertSQL.WriteString(fmt.Sprintf(" ON CONFLICT (%s) ", pkSQL))
			if task.InsertOnly {
				upsertSQL.WriteString("DO NOTHING")
			} else {
				upsertSQL.WriteString("DO UPDATE SET ")
				setClauses := make([]string, 0, len(orderedCols))
				for _, col := range orderedCols {
					isPkCol := false
					for _, pk := range task.Key {
						if col == pk {
							isPkCol = true
							break
						}
					}
					if !isPkCol {
						sanitisedCol := pgx.Identifier{col}.Sanitize()
						setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", sanitisedCol, sanitisedCol))
					}
				}
				upsertSQL.WriteString(strings.Join(setClauses, ", "))
			}

			cmdTag, err := tx.Exec(task.Ctx, upsertSQL.String(), args...)
			if err != nil {
				// Reset xact and session before returning error
				if preserveThisGroup {
					task.resetReplicationOriginXact(tx)
					task.resetReplicationOriginSession(tx)
				}
				return totalUpsertedCount, fmt.Errorf("error executing upsert batch: %w (SQL: %s, Args: %v)", err, upsertSQL.String(), args)
			}
			totalUpsertedCount += int(cmdTag.RowsAffected())
		}

		// Note: Xact reset happens AFTER commit in the calling function
		// Session reset also happens AFTER commit in the calling function
	}

	return totalUpsertedCount, nil
}

// executeUpsertsWithTimestamps handles upserting rows with per-timestamp transaction management.
// This allows each unique timestamp group to be committed separately, preserving per-row timestamp accuracy.
func executeUpsertsWithTimestamps(pool *pgxpool.Pool, task *TableRepairTask, nodeName string, upserts map[string]map[string]any, colTypes map[string]string, originInfoMap map[string]*rowOriginInfo) (int, error) {
	// Group rows by (origin, LSN, timestamp) if preserve-origin is enabled and we have origin info
	var originGroups map[originBatchKey]map[string]map[string]any // batchKey -> pkey -> row
	rowsWithoutOrigin := 0
	if task.PreserveOrigin && originInfoMap != nil && len(originInfoMap) > 0 {
		originGroups = make(map[originBatchKey]map[string]map[string]any)
		for pkey, row := range upserts {
			originInfo, hasOrigin := originInfoMap[pkey]
			var batchKey originBatchKey
			if hasOrigin && originInfo != nil {
				batchKey = makeOriginBatchKey(originInfo)
				if batchKey.nodeOrigin == "" {
					rowsWithoutOrigin++
				}
			} else {
				rowsWithoutOrigin++
			}
			if originGroups[batchKey] == nil {
				originGroups[batchKey] = make(map[string]map[string]any)
			}
			originGroups[batchKey][pkey] = row
		}
		if rowsWithoutOrigin > 0 {
			logger.Warn("preserve-origin enabled but %d rows missing origin metadata - these will be repaired without origin tracking", rowsWithoutOrigin)
		}
	} else {
		// No origin tracking - process all rows together in one transaction
		originGroups = map[originBatchKey]map[string]map[string]any{
			{}: upserts,
		}
	}

	totalUpsertedCount := 0

	// Process each timestamp group in its own transaction
	for batchKey, originUpserts := range originGroups {
		if len(originUpserts) == 0 {
			continue
		}

		logger.Info("Processing timestamp group: origin='%s', lsn=%d, timestamp='%s', rows=%d",
			batchKey.nodeOrigin, batchKey.lsn, batchKey.timestamp, len(originUpserts))

		// Start a new transaction for this timestamp group
		tx, err := pool.Begin(task.Ctx)
		if err != nil {
			return totalUpsertedCount, fmt.Errorf("starting transaction for timestamp group on node %s: %w", nodeName, err)
		}

		// Enable spock repair mode
		_, err = tx.Exec(task.Ctx, "SELECT spock.repair_mode(true)")
		if err != nil {
			tx.Rollback(task.Ctx)
			return totalUpsertedCount, fmt.Errorf("enabling spock.repair_mode(true) on %s: %w", nodeName, err)
		}

		// Set session replication role
		if task.FireTriggers {
			_, err = tx.Exec(task.Ctx, "SET session_replication_role = 'local'")
		} else {
			_, err = tx.Exec(task.Ctx, "SET session_replication_role = 'replica'")
		}
		if err != nil {
			tx.Rollback(task.Ctx)
			return totalUpsertedCount, fmt.Errorf("setting session_replication_role on %s: %w", nodeName, err)
		}

		// Set role if needed
		if err := task.setRole(tx, nodeName); err != nil {
			tx.Rollback(task.Ctx)
			return totalUpsertedCount, err
		}

		// Setup replication origin for this timestamp group.
		// If we cannot obtain an origin LSN, we gracefully fall back to regular repair for this group.
		preserveThisGroup := task.PreserveOrigin && batchKey.nodeOrigin != ""
		if preserveThisGroup {
			logger.Info("Setting up replication origin for %s with timestamp %s", batchKey.nodeOrigin, batchKey.timestamp)

			// Parse timestamp from batch key
			var commitTS *time.Time
			if batchKey.timestamp != "" {
				ts, err := time.Parse(time.RFC3339Nano, batchKey.timestamp)
				if err != nil {
					tx.Rollback(task.Ctx)
					return totalUpsertedCount, fmt.Errorf("failed to parse timestamp from batch key: %w", err)
				}
				commitTS = &ts
				logger.Info("Parsed commit timestamp: %v", commitTS)
			}

			// Get or generate LSN for this batch
			var lsn *uint64
			if batchKey.lsn != 0 {
				lsnCopy := batchKey.lsn
				lsn = &lsnCopy
			} else {
				// Fetch base LSN from survivor node
				var survivorNode string
				for poolNode := range task.Pools {
					if poolNode != batchKey.nodeOrigin && poolNode != nodeName {
						survivorNode = poolNode
						break
					}
				}
				if survivorNode == "" && task.SourceOfTruth != "" && task.SourceOfTruth != batchKey.nodeOrigin {
					survivorNode = task.SourceOfTruth
				}

				if survivorNode != "" {
					fetchedLSN, err := task.getOriginLSNForNode(batchKey.nodeOrigin, survivorNode)
					if err != nil {
						tx.Rollback(task.Ctx)
						return totalUpsertedCount, fmt.Errorf("failed to get origin LSN for node %s: %w", batchKey.nodeOrigin, err)
					}
					if fetchedLSN == nil {
						preserveThisGroup = false
						logger.Warn("preserve-origin: falling back to regular upsert for node %s (origin=%s, timestamp=%s)", nodeName, batchKey.nodeOrigin, batchKey.timestamp)
					} else {
						baseLSN := *fetchedLSN
						// Use base LSN + timestamp-based offset for uniqueness
						offset := uint64(0)
						if commitTS != nil {
							offset = uint64(commitTS.UnixMicro() % 1000000)
						}
						uniqueLSN := baseLSN + offset
						lsn = &uniqueLSN
					}
				} else {
					tx.Rollback(task.Ctx)
					return totalUpsertedCount, fmt.Errorf("no survivor node available to fetch LSN for origin node %s", batchKey.nodeOrigin)
				}
			}

			if preserveThisGroup {
				// Setup session (once per transaction)
				_, err := task.setupReplicationOriginSession(tx, batchKey.nodeOrigin)
				if err != nil {
					tx.Rollback(task.Ctx)
					return totalUpsertedCount, fmt.Errorf("failed to setup replication origin session for node %s: %w", batchKey.nodeOrigin, err)
				}
				logger.Info("Set up replication origin session for %s", batchKey.nodeOrigin)

				// Setup xact with this specific timestamp
				if err := task.setupReplicationOriginXact(tx, lsn, commitTS); err != nil {
					task.resetReplicationOriginSession(tx)
					tx.Rollback(task.Ctx)
					return totalUpsertedCount, fmt.Errorf("failed to setup replication origin xact for node %s (LSN=%v, TS=%v): %w", batchKey.nodeOrigin, lsn, commitTS, err)
				}
				logger.Info("Set up replication origin xact with LSN=%v, timestamp=%v", lsn, commitTS)
			}
		}

		// Execute the upserts for this timestamp group
		count, err := executeUpsertsInTransaction(tx, task, nodeName, originUpserts, colTypes)
		if err != nil {
			// Reset origin tracking
			if preserveThisGroup {
				task.resetReplicationOriginXact(tx)
				task.resetReplicationOriginSession(tx)
			}
			tx.Rollback(task.Ctx)
			return totalUpsertedCount, fmt.Errorf("executing upserts for timestamp group: %w", err)
		}
		totalUpsertedCount += count

		// Reset origin tracking BEFORE commit to clean up the connection
		// This ensures the connection is returned to the pool in a clean state
		if preserveThisGroup {
			if err := task.resetReplicationOriginXact(tx); err != nil {
				logger.Warn("failed to reset replication origin xact before commit: %v", err)
				// Continue - this is a cleanup operation
			}
			if err := task.resetReplicationOriginSession(tx); err != nil {
				logger.Warn("failed to reset replication origin session before commit: %v", err)
				// Continue - this is a cleanup operation
			}
		}

		// Commit this timestamp group's transaction
		err = tx.Commit(task.Ctx)
		if err != nil {
			// On error, try to disable repair mode before rollback
			tx.Exec(task.Ctx, "SELECT spock.repair_mode(false)")
			tx.Rollback(task.Ctx)
			return totalUpsertedCount, fmt.Errorf("committing timestamp group transaction on %s: %w", nodeName, err)
		}

		// Disable spock repair mode AFTER commit (on the connection)
		if _, err := pool.Exec(task.Ctx, "SELECT spock.repair_mode(false)"); err != nil {
			logger.Warn("failed to disable spock.repair_mode after commit: %v", err)
		}

		logger.Debug("Committed transaction for timestamp group: %d rows with origin=%s, timestamp=%s",
			count, batchKey.nodeOrigin, batchKey.timestamp)
	}

	return totalUpsertedCount, nil
}

// executeUpsertsInTransaction performs the actual upsert operations within an existing transaction.
// This is called by executeUpsertsWithTimestamps for each timestamp group.
func executeUpsertsInTransaction(tx pgx.Tx, task *TableRepairTask, nodeName string, upserts map[string]map[string]any, colTypes map[string]string) (int, error) {
	if err := task.filterStaleRepairs(task.Ctx, tx, nodeName, upserts, colTypes, "upsert"); err != nil {
		return 0, err
	}

	orderedCols := task.Cols
	totalUpsertedCount := 0
	batchSize := 500

	// Build table identifier using Schema and Table for consistency with other functions
	tableIdent := pgx.Identifier{task.Schema, task.Table}.Sanitize()

	// Convert map to slice for batching
	upsertRows := make([]map[string]any, 0, len(upserts))
	for _, row := range upserts {
		upsertRows = append(upsertRows, row)
	}

	// Process in batches
	for i := 0; i < len(upsertRows); i += batchSize {
		end := i + batchSize
		if end > len(upsertRows) {
			end = len(upsertRows)
		}
		batch := upsertRows[i:end]

		var upsertSQL strings.Builder
		args := make([]any, 0, len(batch)*len(orderedCols))

		upsertSQL.WriteString("INSERT INTO ")
		upsertSQL.WriteString(tableIdent)
		upsertSQL.WriteString(" (")
		for i, col := range orderedCols {
			if i > 0 {
				upsertSQL.WriteString(", ")
			}
			upsertSQL.WriteString(pgx.Identifier{col}.Sanitize())
		}
		upsertSQL.WriteString(") VALUES ")

		for rowIdx, row := range batch {
			if rowIdx > 0 {
				upsertSQL.WriteString(", ")
			}
			upsertSQL.WriteString("(")
			for colIdx, col := range orderedCols {
				if colIdx > 0 {
					upsertSQL.WriteString(", ")
				}
				val := row[col]
				args = append(args, val)
				upsertSQL.WriteString(fmt.Sprintf("$%d", len(args)))
			}
			upsertSQL.WriteString(")")
		}

		upsertSQL.WriteString(" ON CONFLICT (")
		for i, pkCol := range task.Key {
			if i > 0 {
				upsertSQL.WriteString(", ")
			}
			upsertSQL.WriteString(pgx.Identifier{pkCol}.Sanitize())
		}
		upsertSQL.WriteString(") DO UPDATE SET ")

		setClauses := []string{}
		for _, col := range orderedCols {
			isPkCol := false
			for _, pkCol := range task.Key {
				if col == pkCol {
					isPkCol = true
					break
				}
			}
			if !isPkCol {
				sanitisedCol := pgx.Identifier{col}.Sanitize()
				setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", sanitisedCol, sanitisedCol))
			}
		}

		// If there are no non-PK columns, we can't update anything, so just use DO NOTHING
		if len(setClauses) == 0 {
			upsertSQL.WriteString("DO NOTHING")
		} else {
			upsertSQL.WriteString(strings.Join(setClauses, ", "))
		}

		cmdTag, err := tx.Exec(task.Ctx, upsertSQL.String(), args...)
		if err != nil {
			return totalUpsertedCount, fmt.Errorf("error executing upsert batch: %w", err)
		}
		totalUpsertedCount += int(cmdTag.RowsAffected())
	}

	return totalUpsertedCount, nil
}

// getDryRunOutput generates the dry run message string.
func getDryRunOutput(task *TableRepairTask) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("\n######## DRY RUN for table %s ########\n\n", task.QualifiedTableName))

	if task.Bidirectional {
		// TODO: Need to ensure that no more than 2 nodes are specified for bidirectional repair
		// Or we just leave it as a footgun, but warn in the docs.
		anyInserts := false
		for nodePairKey, diffs := range task.RawDiffs.NodeDiffs {
			nodes := strings.Split(nodePairKey, "/")
			if len(nodes) != 2 {
				continue
			}
			node1Name, node2Name := nodes[0], nodes[1]

			node1Rows := diffs.Rows[node1Name]
			node2Rows := diffs.Rows[node2Name]

			node1RowsByPKey := make(map[string]types.OrderedMap)
			for _, row := range node1Rows {
				pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
				if err != nil {
					return "", fmt.Errorf("error stringifying pkey for row on %s: %w", node1Name, err)
				}
				node1RowsByPKey[pkeyStr] = row
			}

			node2RowsByPKey := make(map[string]types.OrderedMap)
			for _, row := range node2Rows {
				pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
				if err != nil {
					return "", fmt.Errorf("error stringifying pkey for row on %s: %w", node2Name, err)
				}
				node2RowsByPKey[pkeyStr] = row
			}

			insertsForNode1 := make(map[string]types.OrderedMap)
			for pkey, row := range node2RowsByPKey {
				if _, exists := node1RowsByPKey[pkey]; !exists {
					insertsForNode1[pkey] = row
				}
			}

			insertsForNode2 := make(map[string]types.OrderedMap)
			for pkey, row := range node1RowsByPKey {
				if _, exists := node2RowsByPKey[pkey]; !exists {
					insertsForNode2[pkey] = row
				}
			}

			if len(insertsForNode1) > 0 || len(insertsForNode2) > 0 {
				anyInserts = true
				sb.WriteString(fmt.Sprintf("  Node Pair '%s/%s':\n", node1Name, node2Name))
				if len(insertsForNode2) > 0 {
					sb.WriteString(fmt.Sprintf("    - Would INSERT %d rows from %s into %s.\n", len(insertsForNode2), node1Name, node2Name))
				}
				if len(insertsForNode1) > 0 {
					sb.WriteString(fmt.Sprintf("    - Would INSERT %d rows from %s into %s.\n", len(insertsForNode1), node2Name, node1Name))
				}

				if task.report != nil {
					reportChanges := make(map[string]any)
					if len(insertsForNode2) > 0 {
						rows := make([]map[string]any, 0, len(insertsForNode2))
						for _, row := range insertsForNode2 {
							rows = append(rows, utils.OrderedMapToMap(row))
						}
						reportChanges[fmt.Sprintf("would_insert_into_%s", node2Name)] = rows
					}
					if len(insertsForNode1) > 0 {
						rows := make([]map[string]any, 0, len(insertsForNode1))
						for _, row := range insertsForNode1 {
							rows = append(rows, utils.OrderedMapToMap(row))
						}
						reportChanges[fmt.Sprintf("would_insert_into_%s", node1Name)] = rows
					}
					task.report.Changes[nodePairKey] = reportChanges
				}
			}
		}

		if !anyInserts {
			sb.WriteString("  All nodes are in sync. No repairs needed.\n")
		}

	} else {
		fullUpserts, fullDeletes, err := calculateRepairSets(task)
		if err != nil {
			sb.WriteString(fmt.Sprintf("Error calculating changes for dry run: %v\n", err))
			return sb.String(), err
		}

		if len(fullUpserts) == 0 && len(fullDeletes) == 0 {
			if task.RepairPlan != nil {
				sb.WriteString("  All nodes are in sync according to the repair plan. No repairs needed.\n")
			} else {
				sb.WriteString("  All nodes are in sync with the source of truth. No repairs needed.\n")
			}
		} else {
			// To ensure a consistent output order for nodes
			var nodeNames []string
			for nodeName := range fullUpserts {
				nodeNames = append(nodeNames, nodeName)
			}
			for nodeName := range fullDeletes {
				if _, exists := fullUpserts[nodeName]; !exists {
					nodeNames = append(nodeNames, nodeName)
				}
			}

			for _, nodeName := range nodeNames {
				upserts := fullUpserts[nodeName]
				deletes := fullDeletes[nodeName]
				if len(upserts) == 0 && len(deletes) == 0 {
					continue
				}

				if task.report != nil {
					nodeChanges := make(map[string]any)
					if len(upserts) > 0 {
						rows := make([]map[string]any, 0, len(upserts))
						for _, row := range upserts {
							rows = append(rows, row)
						}
						if task.InsertOnly {
							nodeChanges["would_insert"] = rows
						} else {
							nodeChanges["would_upsert"] = rows
						}
					}
					if len(deletes) > 0 {
						rows := make([]map[string]any, 0, len(deletes))
						for _, row := range deletes {
							rows = append(rows, row)
						}
						if task.RepairPlan != nil {
							nodeChanges["would_delete"] = rows
						} else if !task.UpsertOnly && !task.InsertOnly {
							nodeChanges["would_delete"] = rows
						} else {
							nodeChanges["skipped_deletes"] = rows
						}
					}
					if task.RepairPlan != nil && len(task.planRuleMatches[nodeName]) > 0 {
						nodeChanges["rule_matches"] = task.planRuleMatches[nodeName]
					}
					task.report.Changes[nodeName] = nodeChanges
				}

				if task.RepairPlan != nil || (!task.UpsertOnly && !task.InsertOnly) {
					sb.WriteString(fmt.Sprintf("  Node %s: Would attempt to UPSERT %d rows and DELETE %d rows.\n", nodeName, len(upserts), len(deletes)))
				} else if task.InsertOnly {
					sb.WriteString(fmt.Sprintf("  Node %s: Would attempt to INSERT %d rows.\n", nodeName, len(upserts)))
					if len(deletes) > 0 {
						sb.WriteString(fmt.Sprintf("    Additionally, %d rows exist on %s that are not on %s (deletes skipped).\n", len(deletes), nodeName, task.SourceOfTruth))
					}
				} else { // UpsertOnly
					sb.WriteString(fmt.Sprintf("  Node %s: Would attempt to UPSERT %d rows.\n", nodeName, len(upserts)))
					if len(deletes) > 0 {
						sb.WriteString(fmt.Sprintf("    Additionally, %d rows exist on %s that are not on %s (deletes skipped).\n", len(deletes), nodeName, task.SourceOfTruth))
					}
				}
				if task.RepairPlan != nil && len(task.planRuleMatches[nodeName]) > 0 {
					ruleCounts := make(map[string]int)
					for _, ruleName := range task.planRuleMatches[nodeName] {
						ruleCounts[ruleName]++
					}
					var parts []string
					for rule, count := range ruleCounts {
						parts = append(parts, fmt.Sprintf("%s=%d", rule, count))
					}
					sb.WriteString(fmt.Sprintf("    Rule usage: %s\n", strings.Join(parts, ", ")))
				}
			}
		}
	}
	sb.WriteString("\n######## END DRY RUN ########\n")
	return sb.String(), nil
}

func calculateRepairSets(task *TableRepairTask) (map[string]map[string]map[string]any, map[string]map[string]map[string]any, error) {
	if task.RepairPlan != nil {
		return CalculatePlanRepairSets(task)
	}
	return calculateRepairSetsWithSourceOfTruth(task)
}

func calculateRepairSetsWithSourceOfTruth(task *TableRepairTask) (map[string]map[string]map[string]any, map[string]map[string]map[string]any, error) {
	fullRowsToUpsert := make(map[string]map[string]map[string]any) // nodeName -> string(pkey) -> rowData
	fullRowsToDelete := make(map[string]map[string]map[string]any) // nodeName -> string(pkey) -> rowData

	if task.SourceOfTruth == "" {
		return nil, nil, fmt.Errorf("source_of_truth must be set to calculate repair sets")
	}

	for nodePair, diffs := range task.RawDiffs.NodeDiffs {
		nodes := strings.Split(nodePair, "/")
		node1Name := nodes[0]
		node2Name := nodes[1]

		var sourceRows []types.OrderedMap
		var targetRows []types.OrderedMap
		var targetNode string

		if node1Name == task.SourceOfTruth {
			sourceRows = diffs.Rows[node1Name]
			targetRows = diffs.Rows[node2Name]
			targetNode = node2Name
		} else if node2Name == task.SourceOfTruth {
			sourceRows = diffs.Rows[node2Name]
			targetRows = diffs.Rows[node1Name]
			targetNode = node1Name
		} else {
			continue
		}

		if fullRowsToUpsert[targetNode] == nil {
			fullRowsToUpsert[targetNode] = make(map[string]map[string]any)
		}
		if fullRowsToDelete[targetNode] == nil {
			fullRowsToDelete[targetNode] = make(map[string]map[string]any)
		}

		sourceRowsByPKey := make(map[string]map[string]any)
		for _, row := range sourceRows {
			pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
			if err != nil {
				return nil, nil, fmt.Errorf("error stringifying pkey for source row on %s: %w", task.SourceOfTruth, err)
			}
			cleanRow := utils.StripSpockMetadata(utils.OrderedMapToMap(row))
			sourceRowsByPKey[pkeyStr] = cleanRow
			fullRowsToUpsert[targetNode][pkeyStr] = cleanRow
		}

		targetRowsByPKey := make(map[string]map[string]any)
		for _, row := range targetRows {
			pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
			if err != nil {
				return nil, nil, fmt.Errorf("error stringifying pkey for target row on %s: %w", targetNode, err)
			}
			cleanRow := utils.StripSpockMetadata(utils.OrderedMapToMap(row))
			targetRowsByPKey[pkeyStr] = cleanRow
			if _, existsInSource := sourceRowsByPKey[pkeyStr]; !existsInSource {
				fullRowsToDelete[targetNode][pkeyStr] = cleanRow
			}
		}
	}
	return fullRowsToUpsert, fullRowsToDelete, nil
}

func (t *TableRepairTask) fetchLSNsForNode(pool *pgxpool.Pool, failedNode, survivor string) (originLSN *uint64, slotLSN *uint64, err error) {
	var originStr *string
	originStr, err = queries.GetSpockOriginLSNForNode(t.Ctx, pool, failedNode, survivor)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch origin lsn on %s: %w", survivor, err)
	}
	if originStr != nil {
		if val, parseErr := pglogrepl.ParseLSN(*originStr); parseErr == nil {
			tmp := uint64(val)
			originLSN = &tmp
		}
	}

	var slotStr *string
	slotStr, err = queries.GetSpockSlotLSNForNode(t.Ctx, pool, failedNode)
	if err != nil {
		return originLSN, nil, fmt.Errorf("failed to fetch slot lsn on %s: %w", survivor, err)
	}
	if slotStr != nil {
		if val, parseErr := pglogrepl.ParseLSN(*slotStr); parseErr == nil {
			tmp := uint64(val)
			slotLSN = &tmp
		}
	}

	return originLSN, slotLSN, nil
}

func formatLSN(val *uint64) string {
	if val == nil {
		return ""
	}
	return pglogrepl.LSN(*val).String()
}

func (t *TableRepairTask) autoSelectSourceOfTruth(failedNode string, involved map[string]bool) (string, map[string]map[string]string, error) {
	lsnDetails := make(map[string]map[string]string)

	type candidate struct {
		node    string
		val     uint64
		valType string
	}
	var best *candidate

	for _, nodeInfo := range t.ClusterNodes {
		nodeName, _ := nodeInfo["Name"].(string)
		if nodeName == "" || nodeName == failedNode {
			continue
		}
		if len(involved) > 0 && !involved[nodeName] {
			continue
		}

		pool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
		if err != nil {
			logger.Warn("recovery-mode: failed to connect to %s for LSN probe: %v", nodeName, err)
			continue
		}
		originLSN, slotLSN, err := t.fetchLSNsForNode(pool, failedNode, nodeName)
		if err != nil {
			logger.Warn("recovery-mode: failed to fetch LSNs on %s: %v", nodeName, err)
			pool.Close()
			continue
		}

		if t.Pools[nodeName] == nil {
			t.Pools[nodeName] = pool
		} else {
			pool.Close()
		}

		lsnDetails[nodeName] = map[string]string{
			"origin_lsn": formatLSN(originLSN),
			"slot_lsn":   formatLSN(slotLSN),
		}

		var candidateVal uint64
		var candidateType string
		if originLSN != nil {
			candidateVal = *originLSN
			candidateType = "origin"
		} else if slotLSN != nil {
			candidateVal = *slotLSN
			candidateType = "slot"
		} else {
			continue
		}

		if best == nil || candidateVal > best.val {
			best = &candidate{node: nodeName, val: candidateVal, valType: candidateType}
		} else if candidateVal == best.val {
			return "", lsnDetails, fmt.Errorf("nodes %s and %s have identical %s LSNs; specify source_of_truth explicitly", best.node, nodeName, candidateType)
		}
	}

	if best == nil {
		return "", lsnDetails, fmt.Errorf("unable to determine source_of_truth in recovery-mode: no LSNs available for failed node %s", failedNode)
	}

	return best.node, lsnDetails, nil
}

// getOriginLSNForNode fetches the origin LSN for a given origin node from a survivor node.
// If LSN is not available, returns (nil, nil) so callers can gracefully fall back
// to non-origin-preserving repair.
func (t *TableRepairTask) getOriginLSNForNode(originNodeName, survivorNodeName string) (*uint64, error) {
	survivorPool, ok := t.Pools[survivorNodeName]
	if !ok || survivorPool == nil {
		return nil, fmt.Errorf("no connection pool for survivor node %s", survivorNodeName)
	}

	originLSN, _, err := t.fetchLSNsForNode(survivorPool, originNodeName, survivorNodeName)
	if err != nil {
		logger.Warn("preserve-origin: failed to fetch origin LSN for node %s from survivor %s; falling back to regular repair: %v", originNodeName, survivorNodeName, err)
		return nil, nil
	}

	if originLSN == nil {
		logger.Warn("preserve-origin: origin LSN not available for node %s on survivor %s; falling back to regular repair (timestamps will be current)", originNodeName, survivorNodeName)
		return nil, nil
	}

	return originLSN, nil
}

// setupReplicationOriginSession sets up the replication origin for the session.
// This should be called before starting the transaction or at the very start.
// Returns the origin ID for use in xact setup.
func (t *TableRepairTask) setupReplicationOriginSession(tx pgx.Tx, originNodeName string) (uint32, error) {
	// Normalize origin node name - use "node_X" format for replication origin
	originName := fmt.Sprintf("node_%s", originNodeName)

	// Get or create replication origin
	originID, err := queries.GetReplicationOriginByName(t.Ctx, tx, originName)
	if err != nil {
		return 0, fmt.Errorf("failed to get replication origin by name '%s': %w", originName, err)
	}

	if originID == nil {
		// Create the replication origin if it doesn't exist
		createdID, createErr := queries.CreateReplicationOrigin(t.Ctx, tx, originName)
		if createErr != nil {
			return 0, fmt.Errorf("failed to create replication origin '%s': %w", originName, createErr)
		}
		originID = &createdID
		logger.Debug("Created replication origin '%s' with ID %d", originName, *originID)
	} else {
		logger.Debug("Found existing replication origin '%s' with ID %d", originName, *originID)
	}

	// Set up the replication origin session
	if err := queries.SetupReplicationOriginSession(t.Ctx, tx, originName); err != nil {
		return 0, fmt.Errorf("failed to setup replication origin session for '%s': %w", originName, err)
	}

	return *originID, nil
}

// setupReplicationOriginXact sets up the transaction-level LSN and timestamp.
// This must be called within the transaction, before any DML operations.
func (t *TableRepairTask) setupReplicationOriginXact(tx pgx.Tx, originLSN *uint64, originTimestamp *time.Time) error {
	if originLSN == nil {
		return fmt.Errorf("origin LSN is required for xact setup")
	}

	lsnStr := pglogrepl.LSN(*originLSN).String()

	if err := queries.SetupReplicationOriginXact(t.Ctx, tx, lsnStr, originTimestamp); err != nil {
		return fmt.Errorf("failed to setup replication origin xact with LSN %s: %w", lsnStr, err)
	}

	logger.Debug("Set replication origin xact LSN to %s", lsnStr)
	if originTimestamp != nil {
		logger.Debug("Set replication origin xact timestamp to %s", originTimestamp.Format(time.RFC3339))
	}

	return nil
}

// resetReplicationOriginXact resets the transaction-level replication origin state (for error cleanup within transaction).
func (t *TableRepairTask) resetReplicationOriginXact(tx pgx.Tx) error {
	if err := queries.ResetReplicationOriginXact(t.Ctx, tx); err != nil {
		return fmt.Errorf("failed to reset replication origin xact: %w", err)
	}
	logger.Debug("Reset replication origin xact")
	return nil
}

// resetReplicationOriginSession resets the session-level replication origin state (for error cleanup within transaction).
func (t *TableRepairTask) resetReplicationOriginSession(tx pgx.Tx) error {
	if err := queries.ResetReplicationOriginSession(t.Ctx, tx); err != nil {
		return fmt.Errorf("failed to reset replication origin session: %w", err)
	}
	logger.Debug("Reset replication origin session")
	return nil
}
