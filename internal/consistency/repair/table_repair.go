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
//
// When adding preserve-origin or replication-origin session code: do not ignore
// ResetReplicationOriginSession errors in defers. Handle them (e.g. log and avoid
// reusing the connection) so stale session origin cannot bleed into later batches.

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
	pgxv5type "github.com/jackc/pgx/v5/pgtype"
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

	spockPerNode map[string]bool

	// FailedOps lists the row operations that the last run did not apply.
	FailedOps []FailedRowOp
	// InDoubtOps lists the row operations of a command whose COMMIT got no
	// answer from the server: they may or may not be applied.
	InDoubtOps []FailedRowOp
	commandSeq map[string]int

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

// setRole makes the client role of the task the role of tx until tx ends.
// Outside the CLI a client role is required.
func (t *TableRepairTask) setRole(tx pgx.Tx, nodeName string) error {
	role := strings.TrimSpace(t.ClientRole)
	requireRole := t.InvokeMethod != "cli"
	if role == "" {
		if requireRole {
			return fmt.Errorf("client role in cert CN cannot be null")
		}
		return nil
	}

	// LOCAL: one connection runs many command transactions. A session-level
	// role would stay after the first commit, and the next command would set
	// session_replication_role and the replication origin as this role.
	roleSQL := fmt.Sprintf("SET LOCAL ROLE %s", pgx.Identifier{role}.Sanitize())
	if _, err := tx.Exec(t.Ctx, roleSQL); err != nil { // nosemgrep
		return fmt.Errorf("setting role %s on %s: %w", role, nodeName, err)
	}
	logger.Debug("SET LOCAL ROLE %s on %s", role, nodeName)
	return nil
}

// isSpockAvailable returns whether spock is installed on the given node,
// detecting lazily on first check per node. Returns an error if detection
// fails so callers don't silently fall back to the wrong repair mode.
func (t *TableRepairTask) isSpockAvailable(nodeName string) (bool, error) {
	if t.spockPerNode == nil {
		t.spockPerNode = make(map[string]bool)
	}
	if _, checked := t.spockPerNode[nodeName]; !checked {
		pool := t.Pools[nodeName]
		if pool == nil {
			return false, fmt.Errorf("no connection pool for node %s", nodeName)
		}
		spockInstalled, err := queries.CheckSpockInstalled(t.Ctx, pool)
		if err != nil {
			return false, fmt.Errorf("failed to detect spock extension on %s: %w", nodeName, err)
		}
		t.spockPerNode[nodeName] = spockInstalled
		logger.Info("spock extension on %s: %v", nodeName, spockInstalled)
	}
	return t.spockPerNode[nodeName], nil
}

// setupTransactionMode enables spock repair mode (when available), sets the session replication
// role, and applies the client role for a repair transaction.
// Returns true if spock repair mode was activated, false otherwise.
func (t *TableRepairTask) setupTransactionMode(tx pgx.Tx, nodeName string) (bool, error) {
	spockRepairModeActive := false
	spock, err := t.isSpockAvailable(nodeName)
	if err != nil {
		return false, err
	}
	if spock {
		if _, err := tx.Exec(t.Ctx, "SELECT spock.repair_mode(true)"); err != nil {
			return false, fmt.Errorf("enabling spock.repair_mode(true) on %s: %w", nodeName, err)
		}
		logger.Debug("spock.repair_mode(true) set on %s", nodeName)
		spockRepairModeActive = true
	}

	if t.FireTriggers {
		_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'local'")
	} else {
		_, err = tx.Exec(t.Ctx, "SET session_replication_role = 'replica'")
	}
	if err != nil {
		return false, fmt.Errorf("setting session_replication_role on %s: %w", nodeName, err)
	}
	logger.Debug("session_replication_role set on %s (fire_triggers: %v)", nodeName, t.FireTriggers)

	if err := t.setRole(tx, nodeName); err != nil {
		return false, err
	}
	return spockRepairModeActive, nil
}

// disableSpockRepairMode disables spock repair mode on the given transaction.
// When spock is not available, this is a no-op.
func (t *TableRepairTask) disableSpockRepairMode(tx pgx.Tx, nodeName string) error {
	spock, err := t.isSpockAvailable(nodeName)
	if err != nil {
		return err
	}
	if !spock {
		return nil
	}
	_, err = tx.Exec(t.Ctx, "SELECT spock.repair_mode(false)")
	if err != nil {
		return fmt.Errorf("disabling spock.repair_mode(false) on %s: %w", nodeName, err)
	}
	logger.Debug("spock.repair_mode(false) set on %s", nodeName)
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
	if err := t.RawDiffs.CheckFormatVersion(); err != nil {
		return fmt.Errorf("diff file %s: %w", t.DiffFilePath, err)
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

			tree, err := queries.GetRelationTree(t.Ctx, connPool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to read inheritance tree for %s.%s on node %s: %w", t.Schema, t.Table, nodeName, err)
			}
			if tree == nil {
				return fmt.Errorf("table '%s.%s' not found on node %s", t.Schema, t.Table, nodeName)
			}
			if reason := tree.UnsupportedReason(); reason != "" {
				hint := ""
				if tree.Root.RelKind == "v" || tree.Root.RelKind == "m" {
					hint, err = queries.HotTableHint(t.Ctx, connPool, t.Schema, t.Table)
					if err != nil {
						return fmt.Errorf("failed to look up the hot table for view %s.%s on node %s: %w", t.Schema, t.Table, nodeName, err)
					}
				}
				return fmt.Errorf("'%s.%s' %s (node %s).%s", t.Schema, t.Table, reason, nodeName, hint)
			}

			cols, colTypes, err := queries.GetColumns(t.Ctx, connPool, t.Schema, t.Table)
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
			if !privs.SchemaUsage {
				missingPrivs = append(missingPrivs, "USAGE on schema "+t.Schema)
			}
			if requiredPrivileges.TableSelect && !privs.TableSelect {
				missingPrivs = append(missingPrivs, "SELECT"+privs.SelectHint())
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

	// The stale check matches the keys of the report with keys read from the
	// target now, so the report must have the current value form.
	if t.RawDiffs.FormatVersion == 0 {
		logger.Info("Diff file %s has no format version; reading it as version 0", t.DiffFilePath)
	}
	if err := utils.UpgradeDiffOutput(&t.RawDiffs, queries.NumericColumns(refColTypes)); err != nil {
		return fmt.Errorf("diff file %s: %w", t.DiffFilePath, err)
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

	if err := utils.MkdirAllSecure(reportDir); err != nil {
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

	if err := utils.WriteFileSecure(filePath, reportData); err != nil {
		return fmt.Errorf("failed to write report to file %s: %w", filePath, err)
	}

	logger.Info("Wrote report to %s", filePath)
	return nil
}

// Run runs the repair and records the task. It returns an error if the repair
// could not run or if any row operation failed; the task status is FAILED
// then. Unless skipValidation is set, it validates the task first.
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

		spockRepairModeActive, err := t.setupTransactionMode(tx, nodeName)
		if err != nil {
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

		// Build per-column update lists
		columnsByRow := make(map[string][]*nullUpdate)
		for _, col := range columns {
			for _, nu := range updates {
				if _, ok := nu.columns[col]; ok {
					columnsByRow[col] = append(columnsByRow[col], nu)
				}
			}
		}

		// Extract origin info once for all columns
		var originInfoMap map[string]*rowOriginInfo
		if t.PreserveOrigin {
			originInfoMap = make(map[string]*rowOriginInfo)
			for _, nu := range updates {
				if nu.sourceRow != nil {
					pkeyStr, err := utils.StringifyKey(nu.pkMap, t.Key)
					if err != nil {
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

		nodeCellsUpdated := 0
		nodeFailed := false

		if t.PreserveOrigin && len(originInfoMap) > 0 {
			// Commit the initial tx (repair_mode setup) first, then use
			// per-batch-key transactions for origin preservation.
			if spockRepairModeActive {
				if err := t.disableSpockRepairMode(tx, nodeName); err != nil {
					tx.Rollback(t.Ctx)
					logger.Error("%v", err)
					repairErrors = append(repairErrors, err.Error())
					continue
				}
			}
			if err := tx.Commit(t.Ctx); err != nil {
				logger.Error("committing pre-fixnulls transaction on %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("commit failed for %s: %v", nodeName, err))
				continue
			}
			spockRepairModeActive = false

			updatedCount, err := t.executePreserveOriginFixNulls(pool, nodeName, columns, columnsByRow, colTypes, originInfoMap)
			if err != nil {
				logger.Error("executing preserve-origin fix-nulls on node %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("preserve-origin fix-nulls failed for %s: %v", nodeName, err))
				nodeFailed = true
			}
			nodeCellsUpdated = updatedCount
			logger.Info("Updated %d column values on %s (preserve-origin)", updatedCount, nodeName)
		} else {
			// Standard path: all updates in the shared transaction
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

				rowsForCol := columnsByRow[col]
				if len(rowsForCol) == 0 {
					continue
				}

				updatedCount, err := t.applyFixNullsUpdates(tx, col, colType, rowsForCol, colTypes, nodeName)
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

			if !nodeFailed {
				if spockRepairModeActive {
					if err := t.disableSpockRepairMode(tx, nodeName); err != nil {
						tx.Rollback(t.Ctx)
						logger.Error("%v", err)
						repairErrors = append(repairErrors, err.Error())
						nodeFailed = true
					}
				}
				if !nodeFailed {
					if err := tx.Commit(t.Ctx); err != nil {
						logger.Error("committing transaction on %s: %v", nodeName, err)
						repairErrors = append(repairErrors, fmt.Sprintf("commit failed for %s: %v", nodeName, err))
						nodeFailed = true
					}
				}
			}
		}

		if nodeFailed {
			continue
		}
		logger.Debug("Fix-nulls completed on %s", nodeName)

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

		// Build pk -> OrderedMap lookups for source row retrieval (only needed for preserve-origin)
		var node1RawByPK, node2RawByPK map[string]types.OrderedMap
		if t.PreserveOrigin {
			node1RawByPK = make(map[string]types.OrderedMap, len(node1Rows))
			for _, r := range node1Rows {
				if pk, err := utils.StringifyOrderedMapKey(r, t.Key); err == nil {
					node1RawByPK[pk] = r
				}
			}
			node2RawByPK = make(map[string]types.OrderedMap, len(node2Rows))
			for _, r := range node2Rows {
				if pk, err := utils.StringifyOrderedMapKey(r, t.Key); err == nil {
					node2RawByPK[pk] = r
				}
			}
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
					addNullUpdate(updatesByNode, node1Name, row1, col, val2, node2RawByPK[pkKey])
				} else if val2 == nil && val1 != nil {
					addNullUpdate(updatesByNode, node2Name, row2, col, val1, node1RawByPK[pkKey])
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

// applyFixNullsUpdates executes fix-nulls UPDATE statements in a shared tx.
// This is the standard (non-preserve-origin) path.
func (t *TableRepairTask) applyFixNullsUpdates(tx pgx.Tx, column string, columnType string, updates []*nullUpdate, colTypes map[string]string, nodeName string) (int, error) {
	if len(updates) == 0 {
		return 0, nil
	}

	totalUpdated := 0
	batchSize := 500
	for i := 0; i < len(updates); i += batchSize {
		end := i + batchSize
		if end > len(updates) {
			end = len(updates)
		}
		batch := updates[i:end]

		updateSQL, args, err := t.buildFixNullsBatchSQL(column, columnType, batch, colTypes)
		if err != nil {
			return totalUpdated, err
		}

		tag, err := tx.Exec(t.Ctx, updateSQL, args...)
		if err != nil {
			return totalUpdated, fmt.Errorf("error executing fix-nulls batch for column %s: %w", column, err)
		}
		totalUpdated += int(tag.RowsAffected())
	}

	return totalUpdated, nil
}

// executePreserveOriginFixNulls handles fix-nulls updates with per-batch-key
// transactions so that pg_replication_origin_xact_setup correctly preserves
// per-row origin metadata. Each unique (origin, timestamp) combination gets
// its own transaction.
// executePreserveOriginFixNulls handles fix-nulls updates with per-batch-key
// transactions so that pg_replication_origin_xact_setup preserves per-row timestamps.
// Note: filterStaleRepairs is not called here because fix-nulls only patches individual
// NULL columns (not full row replacements) and uses []*nullUpdate rather than the
// map[string]map[string]any format that filterStaleRepairs expects.
func (t *TableRepairTask) executePreserveOriginFixNulls(pool *pgxpool.Pool, nodeName string, columns []string, columnsByRow map[string][]*nullUpdate, colTypes map[string]string, originInfoMap map[string]*rowOriginInfo) (int, error) {
	// Group all updates by origin batch key.
	type colUpdate struct {
		column     string
		columnType string
		updates    []*nullUpdate
	}
	batchGroups := make(map[originBatchKey][]colUpdate)

	for _, col := range columns {
		colType := colTypes[col]
		var rowsForCol []*nullUpdate
		for _, nu := range columnsByRow[col] {
			rowsForCol = append(rowsForCol, nu)
		}
		if len(rowsForCol) == 0 {
			continue
		}

		// Sub-group this column's updates by origin
		perOrigin := make(map[originBatchKey][]*nullUpdate)
		for _, nu := range rowsForCol {
			pkeyStr, err := utils.StringifyKey(nu.pkMap, t.Key)
			if err != nil {
				if nu.sourceRow != nil {
					pkeyStr, err = utils.StringifyOrderedMapKey(nu.sourceRow, t.Key)
				}
			}
			var bk originBatchKey
			if err == nil {
				if oi, ok := originInfoMap[pkeyStr]; ok && oi != nil {
					bk = makeOriginBatchKey(oi)
				}
			}
			perOrigin[bk] = append(perOrigin[bk], nu)
		}
		for bk, updates := range perOrigin {
			batchGroups[bk] = append(batchGroups[bk], colUpdate{
				column: col, columnType: colType, updates: updates,
			})
		}
	}

	conn, err := pool.Acquire(t.Ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire connection for preserve-origin fix-nulls: %w", err)
	}
	defer func() {
		if resetErr := queries.ResetReplicationOriginSession(t.Ctx, conn); resetErr != nil {
			logger.Warn("failed to reset replication origin session on connection release: %v", resetErr)
		}
		conn.Release()
	}()

	totalUpdated := 0
	currentSessionOrigin := ""

	for batchKey, colUpdates := range batchGroups {
		if len(colUpdates) == 0 {
			continue
		}

		// Manage session-level origin
		if batchKey.nodeOrigin != "" && batchKey.nodeOrigin != currentSessionOrigin {
			if currentSessionOrigin != "" {
				if resetErr := queries.ResetReplicationOriginSession(t.Ctx, conn); resetErr != nil {
					logger.Warn("failed to reset replication origin session before switching origin: %v", resetErr)
				}
			}
			originName := fmt.Sprintf("node_%s", batchKey.nodeOrigin)
			originID, err := queries.GetReplicationOriginByName(t.Ctx, conn, originName)
			if err != nil {
				return totalUpdated, fmt.Errorf("failed to get replication origin '%s': %w", originName, err)
			}
			if originID == nil {
				createdID, createErr := queries.CreateReplicationOrigin(t.Ctx, conn, originName)
				if createErr != nil {
					return totalUpdated, fmt.Errorf("failed to create replication origin '%s': %w", originName, createErr)
				}
				originID = &createdID
			}
			if err := queries.SetupReplicationOriginSession(t.Ctx, conn, originName); err != nil {
				return totalUpdated, fmt.Errorf("failed to setup origin session for '%s': %w", originName, err)
			}
			currentSessionOrigin = batchKey.nodeOrigin
		} else if batchKey.nodeOrigin == "" && currentSessionOrigin != "" {
			if resetErr := queries.ResetReplicationOriginSession(t.Ctx, conn); resetErr != nil {
				logger.Warn("failed to reset replication origin session for empty origin batch: %v", resetErr)
			}
			currentSessionOrigin = ""
		}

		batchTx, err := conn.Begin(t.Ctx)
		if err != nil {
			return totalUpdated, fmt.Errorf("failed to begin batch transaction for origin %s: %w", batchKey.nodeOrigin, err)
		}

		if _, err := t.setupTransactionMode(batchTx, nodeName); err != nil {
			batchTx.Rollback(t.Ctx)
			return totalUpdated, fmt.Errorf("failed to setup transaction mode for origin batch: %w", err)
		}

		commitTS, err := parseBatchKeyTimestamp(batchKey)
		if err != nil {
			batchTx.Rollback(t.Ctx)
			return totalUpdated, fmt.Errorf("failed to parse batch key timestamp: %w", err)
		}

		lsn, ok, err := t.resolveOriginLSN(batchKey, nodeName, commitTS)
		if err != nil {
			batchTx.Rollback(t.Ctx)
			return totalUpdated, fmt.Errorf("failed to resolve origin LSN: %w", err)
		}
		if ok && lsn != nil && commitTS != nil {
			if err := t.setupReplicationOriginXact(batchTx, lsn, commitTS); err != nil {
				batchTx.Rollback(t.Ctx)
				return totalUpdated, fmt.Errorf("failed to setup origin xact: %w", err)
			}
		}

		for _, cu := range colUpdates {
			batchSize := 500
			for i := 0; i < len(cu.updates); i += batchSize {
				end := i + batchSize
				if end > len(cu.updates) {
					end = len(cu.updates)
				}
				updateSQL, args, err := t.buildFixNullsBatchSQL(cu.column, cu.columnType, cu.updates[i:end], colTypes)
				if err != nil {
					batchTx.Rollback(t.Ctx)
					return totalUpdated, err
				}
				tag, err := batchTx.Exec(t.Ctx, updateSQL, args...)
				if err != nil {
					batchTx.Rollback(t.Ctx)
					return totalUpdated, fmt.Errorf("error executing fix-nulls batch for column %s: %w", cu.column, err)
				}
				totalUpdated += int(tag.RowsAffected())
			}
		}

		if err := t.disableSpockRepairMode(batchTx, nodeName); err != nil {
			batchTx.Rollback(t.Ctx)
			return totalUpdated, fmt.Errorf("failed to disable repair mode for origin batch: %w", err)
		}

		if err := batchTx.Commit(t.Ctx); err != nil {
			return totalUpdated, fmt.Errorf("failed to commit batch transaction for origin %s: %w", batchKey.nodeOrigin, err)
		}
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
		// For "timestamp without time zone" columns, wrap in pgxv5type.Timestamp
		// so pgx sends the value as "timestamp" instead of "timestamptz". This prevents
		// PostgreSQL from applying a session-timezone conversion.
		lower := strings.ToLower(colType)
		if lower == "timestamp without time zone" || (strings.HasPrefix(lower, "timestamp") && !strings.Contains(lower, "with time zone")) {
			return pgxv5type.Timestamp{Time: tVal, Valid: true}, nil
		}
		return tVal, nil
	}

	return utils.ConvertToPgxType(val, colType)
}

// getColTypesForNode returns the column types of the table on nodeName and
// the host:port key of the node.
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

// runUnidirectionalRepair applies on each node the row operations of the
// source of truth or of the repair plan: first the deletes, then the updates,
// then the inserts. A failed command does not stop the other commands.
func (t *TableRepairTask) runUnidirectionalRepair(startTime time.Time) error {
	logger.Info("Starting table repair for %s on cluster %s", t.QualifiedTableName, t.ClusterName)
	t.FailedOps = nil
	t.InDoubtOps = nil

	sets, skippedDeletes, err := calculateRepairSets(t)
	if err != nil {
		return fmt.Errorf("failed to calculate repair sets: %w", err)
	}
	for node, rows := range skippedDeletes {
		if len(rows) > 0 {
			logger.Info("%d rows exist only on %s; deletes are skipped", len(rows), node)
		}
	}

	totalOps := make(map[string]map[string]int) // node -> operation -> applied rows

	for _, nodeName := range sets.nodes() {
		logger.Info("Processing repairs for divergent node: %s", nodeName)
		if err := t.ensurePool(nodeName); err != nil {
			return t.finishRepair(startTime, totalOps, []string{err.Error()})
		}
		colTypes, _, err := t.getColTypesForNode(nodeName)
		if err != nil {
			return t.finishRepair(startTime, totalOps, []string{err.Error()})
		}

		// The deletes run first, then the updates, then the inserts. A
		// delete or an update can free a value of a unique key that a later
		// row needs, for example a key that prints in another way but is
		// equal for the index: -0 and 0, or 1.5 and 1.50.
		var inserted, updated, deleted []map[string]any
		deleted, err = t.applyRows(nodeName, opDelete, sets.deletes[nodeName], colTypes, nil)
		if err == nil {
			updated, err = t.applyRows(nodeName, opUpdate, sets.updates[nodeName], colTypes,
				t.extractOriginInfoForNode(nodeName, sets.updates[nodeName]))
		}
		if err == nil {
			inserted, err = t.applyRows(nodeName, opInsert, sets.inserts[nodeName], colTypes,
				t.extractOriginInfoForNode(nodeName, sets.inserts[nodeName]))
		}

		totalOps[nodeName] = map[string]int{opInsert: len(inserted), opUpdate: len(updated), opDelete: len(deleted)}
		logger.Info("Applied on %s: %d inserted, %d updated, %d deleted", nodeName, len(inserted), len(updated), len(deleted))
		t.reportNodeChanges(nodeName, inserted, updated, deleted)
		if err != nil {
			return t.finishRepair(startTime, totalOps, []string{err.Error()})
		}
	}

	return t.finishRepair(startTime, totalOps, nil)
}

// ensurePool opens a connection pool for nodeName if there is none yet.
func (t *TableRepairTask) ensurePool(nodeName string) error {
	if pool, ok := t.Pools[nodeName]; ok && pool != nil {
		return nil
	}
	var nodeInfo map[string]any
	for _, ni := range t.ClusterNodes {
		if name, _ := ni["Name"].(string); name == nodeName {
			nodeInfo = ni
			break
		}
	}
	if nodeInfo == nil {
		return fmt.Errorf("no node info for %s", nodeName)
	}
	pool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
	if err != nil {
		return fmt.Errorf("connection failed for %s: %w", nodeName, err)
	}
	t.Pools[nodeName] = pool
	return nil
}

// inDoubtRowsOf returns the row operations of nodeName whose commit result is
// unknown.
func (t *TableRepairTask) inDoubtRowsOf(nodeName string) []FailedRowOp {
	var out []FailedRowOp
	for _, f := range t.InDoubtOps {
		if f.Node == nodeName {
			out = append(out, f)
		}
	}
	return out
}

// failedRowsOf returns the failed row operations of nodeName.
func (t *TableRepairTask) failedRowsOf(nodeName string) []FailedRowOp {
	var out []FailedRowOp
	for _, f := range t.FailedOps {
		if f.Node == nodeName {
			out = append(out, f)
		}
	}
	return out
}

// reportNodeChanges adds the applied and the failed row operations of
// nodeName to the repair report.
func (t *TableRepairTask) reportNodeChanges(nodeName string, inserted, updated, deleted []map[string]any) {
	if t.report == nil {
		return
	}
	changes, ok := t.report.Changes[nodeName].(map[string]any)
	if !ok {
		changes = make(map[string]any)
		t.report.Changes[nodeName] = changes
	}
	for field, rows := range map[string][]map[string]any{"inserted_rows": inserted, "updated_rows": updated, "deleted_rows": deleted} {
		if len(rows) > 0 {
			changes[field] = rows
		}
	}
	if failed := t.failedRowsOf(nodeName); len(failed) > 0 {
		changes["failed_rows"] = failed
	}
	if doubt := t.inDoubtRowsOf(nodeName); len(doubt) > 0 {
		changes["in_doubt_rows"] = doubt
	}
	if t.RepairPlan != nil && len(t.planRuleMatches[nodeName]) > 0 {
		changes["rule_matches"] = t.planRuleMatches[nodeName]
	}
}

// finishRepair logs the result of a repair and sets the task status. It
// returns an error if a node could not be repaired or a row operation failed.
// The caller sets the final task status from this error, so a failed repair
// must return one even though TaskStatus is already FAILED.
func (t *TableRepairTask) finishRepair(startTime time.Time, totalOps map[string]map[string]int, repairErrors []string) error {
	t.FinishedAt = time.Now()
	t.TimeTaken = float64(t.FinishedAt.Sub(startTime).Milliseconds())
	runTimeStr := fmt.Sprintf("%.3fs", t.TimeTaken/1000)

	nodes := make([]string, 0, len(totalOps))
	for node := range totalOps {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)

	var summary strings.Builder
	var repaired []string
	sums := make(map[string]int)
	for _, node := range nodes {
		ops := totalOps[node]
		failed := len(t.failedRowsOf(node))
		summary.WriteString(fmt.Sprintf("Node %s: %d inserted, %d updated, %d deleted, %d failed. ",
			node, ops[opInsert], ops[opUpdate], ops[opDelete], failed))
		if ops[opInsert]+ops[opUpdate]+ops[opDelete] > 0 {
			repaired = append(repaired, node)
		}
		for op, n := range ops {
			sums[op] += n
		}
	}
	t.TaskContext = strings.TrimSpace(summary.String())

	// XXX A repair that ran all its commands but could not apply some rows
	// ends as FAILED, the same as a repair that could not run at all; a
	// caller cannot tell the two cases apart by the status.
	if len(t.FailedOps) > 0 {
		repairErrors = append(repairErrors, fmt.Sprintf("%d row operations failed", len(t.FailedOps)))
	}
	if len(repairErrors) > 0 {
		msg := strings.Join(repairErrors, "; ")
		logger.Error("Repair of %s finished in %s with errors: %s. %s", t.QualifiedTableName, runTimeStr, msg, t.TaskContext)
		t.TaskStatus = "FAILED"
		t.TaskContext = strings.TrimSpace(msg + ". " + t.TaskContext)
		return fmt.Errorf("repair of %s failed: %s", t.QualifiedTableName, msg)
	}

	if len(repaired) > 0 {
		logger.Info("Repair of %s complete in %s. Nodes %s repaired (%d inserted, %d updated, %d deleted).",
			t.QualifiedTableName, runTimeStr, strings.Join(repaired, ", "), sums[opInsert], sums[opUpdate], sums[opDelete])
	} else {
		logger.Info("Repair of %s complete in %s. No differences found.", t.QualifiedTableName, runTimeStr)
	}
	t.TaskStatus = "COMPLETED"
	return nil
}

// runBidirectionalRepair inserts on each node of a pair the rows that only
// the other node has. Rows that both nodes have are not changed.
func (t *TableRepairTask) runBidirectionalRepair() error {
	startTime := time.Now()
	logger.Info("Starting bidirectional table repair for %s on cluster %s", t.QualifiedTableName, t.ClusterName)
	t.FailedOps = nil
	t.InDoubtOps = nil

	totalOps := make(map[string]int)
	var repairErrors []string

pairs:
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

		for _, side := range []struct {
			node string
			rows map[string]map[string]any
		}{{node1Name, insertsForNode1}, {node2Name, insertsForNode2}} {
			if len(side.rows) == 0 {
				continue
			}
			// FailedOps holds the failures of all node pairs so far; this pair
			// reports only the ones that its own inserts add.
			before := len(t.FailedOps)
			applied, err := t.performBirectionalInserts(side.node, side.rows)
			totalOps[side.node] += len(applied)
			if t.report != nil {
				if _, ok := t.report.Changes[nodePairKey]; !ok {
					t.report.Changes[nodePairKey] = make(map[string]any)
				}
				changeMap := t.report.Changes[nodePairKey].(map[string]any)
				nodeChanges := map[string]any{"inserted_rows": applied}
				if failed := t.FailedOps[before:]; len(failed) > 0 {
					nodeChanges["failed_rows"] = failed
				}
				if doubt := t.inDoubtRowsOf(side.node); len(doubt) > 0 {
					nodeChanges["in_doubt_rows"] = doubt
				}
				changeMap[side.node] = nodeChanges
			}
			if err != nil {
				repairErrors = append(repairErrors, fmt.Sprintf("inserts failed for %s: %v", side.node, err))
				break pairs
			}
		}
	}

	ops := make(map[string]map[string]int, len(totalOps))
	for node, n := range totalOps {
		ops[node] = map[string]int{opInsert: n}
	}
	return t.finishRepair(startTime, ops, repairErrors)
}

// performBirectionalInserts inserts rows on nodeName and returns the rows
// that were applied. It returns an error if it cannot start or if applyRows
// stops.
func (t *TableRepairTask) performBirectionalInserts(nodeName string, inserts map[string]map[string]any) ([]map[string]any, error) {
	pool, ok := t.Pools[nodeName]
	if !ok || pool == nil {
		return nil, fmt.Errorf("no connection pool for node %s", nodeName)
	}

	targetNodeHostPortKey := ""
	for hostPort, mappedName := range t.HostMap {
		if mappedName == nodeName {
			targetNodeHostPortKey = hostPort
			break
		}
	}
	if targetNodeHostPortKey == "" {
		return nil, fmt.Errorf("could not find host:port key for target node %s", nodeName)
	}
	targetNodeColTypes, ok := t.ColTypes[targetNodeHostPortKey]
	if !ok {
		return nil, fmt.Errorf("column types for target node '%s' (key: %s) not found", nodeName, targetNodeHostPortKey)
	}

	// Extract origin information from source rows for bidirectional repair
	var originInfoMap map[string]*rowOriginInfo
	if t.PreserveOrigin {
		originInfoMap = make(map[string]*rowOriginInfo)
		for nodePairKey, diffs := range t.RawDiffs.NodeDiffs {
			nodes := strings.Split(nodePairKey, "/")
			if len(nodes) != 2 {
				continue
			}
			var sourceNode string
			if nodes[0] == nodeName {
				sourceNode = nodes[1]
			} else if nodes[1] == nodeName {
				sourceNode = nodes[0]
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

	applied, err := t.applyRows(nodeName, opInsert, inserts, targetNodeColTypes, originInfoMap)
	logger.Info("Executed %d insert operations on %s", len(applied), nodeName)
	return applied, err
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

// parseBatchKeyTimestamp parses the RFC3339Nano timestamp from an originBatchKey.
// Returns nil if the timestamp is empty.
func parseBatchKeyTimestamp(batchKey originBatchKey) (*time.Time, error) {
	if batchKey.timestamp == "" {
		return nil, nil
	}
	ts, err := time.Parse(time.RFC3339Nano, batchKey.timestamp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp from batch key: %w", err)
	}
	return &ts, nil
}

// parseNumericTimestamp converts a Unix timestamp (seconds, ms, µs, or ns) to time.Time.
// Numeric timestamp support: handles Unix timestamps as seconds/ms/µs/ns based on magnitude.
// Auto-detection prevents timestamp misinterpretation (e.g., treating milliseconds as seconds).
// Supports diverse sources: JSON unmarshaling, PostgreSQL numeric types, external metadata.
// All scales are normalized to time.Time for consistent origin batch key generation.
func parseNumericTimestamp(n int64) time.Time {
	switch {
	case n >= 1e18, n <= -1e18:
		return time.Unix(0, n)
	case n >= 1e15, n <= -1e15:
		return time.Unix(0, n*1000)
	case n >= 1e12, n <= -1e12:
		return time.Unix(0, n*1e6)
	default:
		return time.Unix(n, 0)
	}
}

// extractOriginInfoFromRow extracts origin information from a row's metadata.
// Returns nil if no origin information is available.
func extractOriginInfoFromRow(row types.OrderedMap) *rowOriginInfo {
	// Defensive NULL check: diff output may contain nil rows in edge cases.
	// Returning nil means "no origin metadata available"; caller groups under zero-origin batch key.
	// Rows without origin metadata are repaired without timestamp/origin preservation.
	// Prevents panics while allowing repair to proceed.
	if row == nil {
		return nil
	}
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
			// Timestamp parsing priority: RFC3339Nano first (canonical format, preserves ns precision).
			// Fallback to RFC3339 (standard ISO 8601) for external sources or legacy metadata.
			// Final fallback to PostgreSQL text format for µs precision.
			// Ensures backward compatibility while preserving maximum precision.
			ts, err = time.Parse(time.RFC3339Nano, v)
			if err != nil {
				ts, err = time.Parse(time.RFC3339, v)
			}
			if err != nil {
				ts, err = time.Parse("2006-01-02 15:04:05.999999-07", v)
			}
		case int64:
			// Numeric timestamp types: metadata may contain timestamps as int64/int/uint64/float64.
			// All delegated to parseNumericTimestamp for scale auto-detection (seconds/ms/µs/ns).
			// JSON unmarshaling produces different types depending on value size and source.
			// Unified handling ensures timestamp preservation regardless of metadata format.
			ts = parseNumericTimestamp(v)
		case int:
			ts = parseNumericTimestamp(int64(v))
		case uint64:
			ts = parseNumericTimestamp(int64(v))
		case float64:
			ts = parseNumericTimestamp(int64(v))
		case json.Number:
			if i64, err := v.Int64(); err == nil {
				ts = parseNumericTimestamp(i64)
			} else {
				logger.Warn("extractOriginInfoFromRow: failed to parse json.Number commit_ts %q: %v", v.String(), err)
				return nil
			}
		default:
			logger.Warn("extractOriginInfoFromRow: unhandled commit_ts type %T, skipping origin preservation", tsVal)
			return nil
		}
		if err != nil {
			logger.Warn("extractOriginInfoFromRow: failed to parse commit_ts %q: %v, skipping origin preservation", tsVal, err)
			return nil
		}
		if ts.IsZero() {
			return nil
		}
		commitTS = &ts
	}

	if nodeOrigin == "" {
		return nil
	}

	return &rowOriginInfo{
		nodeOrigin: nodeOrigin,
		commitTS:   commitTS,
	}
}

// groupUpsertsByOrigin groups upsert rows by origin batch key.
// When preserve-origin is disabled or no origin info is available, all rows are
// grouped under a single zero-value key.
func groupUpsertsByOrigin(upserts map[string]map[string]any, originInfoMap map[string]*rowOriginInfo, preserveOrigin bool) map[originBatchKey]map[string]map[string]any {
	if preserveOrigin && originInfoMap != nil && len(originInfoMap) > 0 {
		groups := make(map[originBatchKey]map[string]map[string]any)
		rowsWithoutOrigin := 0
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
			if groups[batchKey] == nil {
				groups[batchKey] = make(map[string]map[string]any)
			}
			groups[batchKey][pkey] = row
		}
		if rowsWithoutOrigin > 0 {
			logger.Warn("preserve-origin enabled but %d rows missing origin metadata - these will be repaired without origin tracking", rowsWithoutOrigin)
		}
		return groups
	}
	return map[originBatchKey]map[string]map[string]any{
		{}: upserts,
	}
}

// extractOriginInfoForNode extracts origin metadata from diff rows for the
// given node's upserts. Returns nil if preserve-origin is disabled.
func (t *TableRepairTask) extractOriginInfoForNode(nodeName string, nodeUpserts map[string]map[string]any) map[string]*rowOriginInfo {
	if !t.PreserveOrigin {
		return nil
	}
	originInfoMap := make(map[string]*rowOriginInfo)
	logger.Debug("preserve-origin: extracting origin info for %d upserts on %s (SoT=%s)", len(nodeUpserts), nodeName, t.SourceOfTruth)
	for nodePair, diffs := range t.RawDiffs.NodeDiffs {
		nodes := strings.Split(nodePair, "/")
		if len(nodes) != 2 {
			continue
		}
		node1Name, node2Name := nodes[0], nodes[1]
		for _, sourceNode := range []string{node1Name, node2Name} {
			sourceRows := diffs.Rows[sourceNode]
			for _, row := range sourceRows {
				pkeyStr, err := utils.StringifyOrderedMapKey(row, t.Key)
				if err != nil {
					continue
				}
				if _, isBeingUpserted := nodeUpserts[pkeyStr]; !isBeingUpserted {
					continue
				}
				originInfo := extractOriginInfoFromRow(row)
				if originInfo == nil {
					continue
				}
				if t.RepairPlan == nil {
					if sourceNode == t.SourceOfTruth {
						originInfoMap[pkeyStr] = originInfo
					}
				} else {
					if _, exists := originInfoMap[pkeyStr]; !exists {
						originInfoMap[pkeyStr] = originInfo
					}
				}
			}
		}
	}
	logger.Debug("preserve-origin: origin info map has %d entries for %d upserts", len(originInfoMap), len(nodeUpserts))
	return originInfoMap
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
		sets, skippedDeletes, err := calculateRepairSets(task)
		if err != nil {
			sb.WriteString(fmt.Sprintf("Error calculating changes for dry run: %v\n", err))
			return sb.String(), err
		}

		nodeNames := sets.nodes()
		for node, rows := range skippedDeletes {
			if len(rows) > 0 && !slices.Contains(nodeNames, node) {
				nodeNames = append(nodeNames, node)
			}
		}
		sort.Strings(nodeNames)

		if sets.empty() && len(nodeNames) == 0 {
			if task.RepairPlan != nil {
				sb.WriteString("  All nodes are in sync according to the repair plan. No repairs needed.\n")
			} else {
				sb.WriteString("  All nodes are in sync with the source of truth. No repairs needed.\n")
			}
		}

		rowList := func(rows map[string]map[string]any) []map[string]any {
			out := make([]map[string]any, 0, len(rows))
			for _, pk := range sortedKeys(rows) {
				out = append(out, rows[pk])
			}
			return out
		}
		for _, nodeName := range nodeNames {
			inserts, updates, deletes := sets.inserts[nodeName], sets.updates[nodeName], sets.deletes[nodeName]
			skipped := skippedDeletes[nodeName]

			if task.report != nil {
				nodeChanges := make(map[string]any)
				if len(inserts) > 0 {
					nodeChanges["would_insert"] = rowList(inserts)
				}
				if len(updates) > 0 {
					nodeChanges["would_update"] = rowList(updates)
				}
				if len(deletes) > 0 {
					nodeChanges["would_delete"] = rowList(deletes)
				}
				if len(skipped) > 0 {
					nodeChanges["skipped_deletes"] = rowList(skipped)
				}
				if task.RepairPlan != nil && len(task.planRuleMatches[nodeName]) > 0 {
					nodeChanges["rule_matches"] = task.planRuleMatches[nodeName]
				}
				task.report.Changes[nodeName] = nodeChanges
			}

			sb.WriteString(fmt.Sprintf("  Node %s: Would INSERT %d rows, UPDATE %d rows and DELETE %d rows.\n",
				nodeName, len(inserts), len(updates), len(deletes)))
			if len(skipped) > 0 {
				sb.WriteString(fmt.Sprintf("    Additionally, %d rows exist on %s that are not on %s (deletes skipped).\n", len(skipped), nodeName, task.SourceOfTruth))
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
				sort.Strings(parts)
				sb.WriteString(fmt.Sprintf("    Rule usage: %s\n", strings.Join(parts, ", ")))
			}
		}
	}
	sb.WriteString("\n######## END DRY RUN ########\n")
	return sb.String(), nil
}

// calculateRepairSets returns the row operations of the repair, from the
// repair plan or from the source of truth. --insert-only drops the updates;
// without a repair plan, --insert-only and --upsert-only also drop the
// deletes. The dropped deletes are returned as skipped.
func calculateRepairSets(task *TableRepairTask) (sets *repairSets, skippedDeletes map[string]map[string]map[string]any, err error) {
	if task.RepairPlan != nil {
		sets, err = calculatePlanRepairSets(task)
	} else {
		sets, err = calculateRepairSetsWithSourceOfTruth(task)
	}
	if err != nil {
		return nil, nil, err
	}
	if task.InsertOnly {
		sets.updates = make(map[string]map[string]map[string]any)
	}
	if task.RepairPlan == nil && (task.InsertOnly || task.UpsertOnly) {
		skippedDeletes = sets.deletes
		sets.deletes = make(map[string]map[string]map[string]any)
	}
	return sets, skippedDeletes, nil
}

// calculateRepairSetsWithSourceOfTruth matches the rows of each node pair
// with the source of truth by key text. A row of the source that the target
// also has is an update, a row of the source only is an insert, and a row of
// the target only is a delete. Keys that the nodes hold as equal but print in
// a different way are one insert and one delete of the same row.
func calculateRepairSetsWithSourceOfTruth(task *TableRepairTask) (*repairSets, error) {
	sets := newRepairSets()

	if task.SourceOfTruth == "" {
		return nil, fmt.Errorf("source_of_truth must be set to calculate repair sets")
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

		targetRowsByPKey := make(map[string]map[string]any)
		for _, row := range targetRows {
			pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
			if err != nil {
				return nil, fmt.Errorf("error stringifying pkey for target row on %s: %w", targetNode, err)
			}
			targetRowsByPKey[pkeyStr] = utils.StripSpockMetadata(utils.OrderedMapToMap(row))
		}

		sourceRowsByPKey := make(map[string]map[string]any)
		for _, row := range sourceRows {
			pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
			if err != nil {
				return nil, fmt.Errorf("error stringifying pkey for source row on %s: %w", task.SourceOfTruth, err)
			}
			cleanRow := utils.StripSpockMetadata(utils.OrderedMapToMap(row))
			sourceRowsByPKey[pkeyStr] = cleanRow
			_, onTarget := targetRowsByPKey[pkeyStr]
			sets.write(targetNode, pkeyStr, cleanRow, onTarget)
		}

		for pkeyStr, row := range targetRowsByPKey {
			if _, existsInSource := sourceRowsByPKey[pkeyStr]; !existsInSource {
				sets.remove(targetNode, pkeyStr, row)
			}
		}
	}
	return sets, nil
}

func (t *TableRepairTask) fetchLSNsForNode(pool *pgxpool.Pool, failedNode, survivor string) (originLSN *uint64, slotLSN *uint64, err error) {
	spock, err := t.isSpockAvailable(survivor)
	if err != nil {
		return nil, nil, fmt.Errorf("detecting spock on %s: %w", survivor, err)
	}
	var originStr *string
	if spock {
		originStr, err = queries.GetSpockOriginLSNForNode(t.Ctx, pool, failedNode)
	} else {
		originStr, err = queries.GetNativeOriginLSNForNode(t.Ctx, pool, failedNode)
	}
	if err != nil {
		if !spock {
			// Native PG queries may fail if subscription naming doesn't match; treat as no data
			logger.Warn("failed to fetch native origin lsn on %s: %v", survivor, err)
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("failed to fetch origin lsn on %s: %w", survivor, err)
	}
	if originStr != nil {
		if val, parseErr := pglogrepl.ParseLSN(*originStr); parseErr == nil {
			tmp := uint64(val)
			originLSN = &tmp
		}
	}

	var slotStr *string
	if spock {
		slotStr, err = queries.GetSpockSlotLSNForNode(t.Ctx, pool, failedNode)
	} else {
		slotStr, err = queries.GetNativeSlotLSNForNode(t.Ctx, pool, failedNode)
	}
	if err != nil {
		if !spock {
			// Native PG queries may fail if subscription naming doesn't match; treat as no data
			logger.Warn("failed to fetch native slot lsn on %s: %v", survivor, err)
			return originLSN, nil, nil
		}
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

		// Store pool before fetchLSNsForNode so isSpockAvailable can find it
		if t.Pools[nodeName] == nil {
			t.Pools[nodeName] = pool
		} else {
			pool.Close()
			pool = t.Pools[nodeName]
		}

		originLSN, slotLSN, err := t.fetchLSNsForNode(pool, failedNode, nodeName)
		if err != nil {
			logger.Warn("recovery-mode: failed to fetch LSNs on %s: %v", nodeName, err)
			continue
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

// resolveOriginLSN finds the LSN for an origin batch key. If the batch key
// contains an LSN from metadata it is used directly; otherwise the LSN is
// fetched from a survivor node and offset by the commit timestamp for
// uniqueness. Returns (nil, false, nil) when the LSN is unavailable for
// graceful fallback.
func (t *TableRepairTask) resolveOriginLSN(batchKey originBatchKey, nodeName string, commitTS *time.Time) (*uint64, bool, error) {
	if batchKey.lsn != 0 {
		lsnCopy := batchKey.lsn
		return &lsnCopy, true, nil
	}

	// Find a survivor node to fetch LSN from
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
		logger.Warn("preserve-origin: no survivor node available to fetch LSN for origin %s, falling back to regular repair", batchKey.nodeOrigin)
		return nil, false, nil
	}

	fetchedLSN, err := t.getOriginLSNForNode(batchKey.nodeOrigin, survivorNode)
	if err != nil {
		return nil, false, fmt.Errorf("failed to get origin LSN for node %s: %w", batchKey.nodeOrigin, err)
	}
	if fetchedLSN == nil {
		return nil, false, nil
	}

	baseLSN := *fetchedLSN
	offset := uint64(0)
	if commitTS != nil {
		offset = uint64(commitTS.UnixMicro() % 1000000)
	}
	uniqueLSN := baseLSN + offset
	return &uniqueLSN, true, nil
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
		logger.Debug("Set replication origin xact timestamp to %s", originTimestamp.Format(time.RFC3339Nano))
	}

	return nil
}
