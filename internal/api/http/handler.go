package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/consistency/repair"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
)

type tableDiffRequest struct {
	Cluster           string   `json:"cluster"`
	Table             string   `json:"table"`
	DBName            string   `json:"dbname"`
	Nodes             []string `json:"nodes"`
	BlockSize         int      `json:"block_size"`
	Concurrency       int      `json:"concurrency_factor"`
	CompareUnitSize   int      `json:"compare_unit_size"`
	MaxDiffRows       int64    `json:"max_diff_rows"`
	TableFilter       string   `json:"table_filter"`
	OverrideBlockSize bool     `json:"override_block_size"`
	Quiet             bool     `json:"quiet"`
}

type tableRerunRequest struct {
	Cluster  string   `json:"cluster"`
	DiffFile string   `json:"diff_file"`
	DBName   string   `json:"dbname"`
	Nodes    []string `json:"nodes"`
	Quiet    bool     `json:"quiet"`
}

type tableRepairRequest struct {
	Cluster        string   `json:"cluster"`
	Table          string   `json:"table"`
	DBName         string   `json:"dbname"`
	Nodes          []string `json:"nodes"`
	DiffFile       string   `json:"diff_file"`
	RepairPlan     string   `json:"repair_plan"`
	SourceOfTruth  string   `json:"source_of_truth"`
	Quiet          bool     `json:"quiet"`
	DryRun         bool     `json:"dry_run"`
	InsertOnly     bool     `json:"insert_only"`
	UpsertOnly     bool     `json:"upsert_only"`
	FireTriggers   bool     `json:"fire_triggers"`
	GenerateReport bool     `json:"generate_report"`
	FixNulls       bool     `json:"fix_nulls"`
	Bidirectional  bool     `json:"bidirectional"`
	PreserveOrigin *bool    `json:"preserve_origin,omitempty"`
}

type spockDiffRequest struct {
	Cluster string   `json:"cluster"`
	DBName  string   `json:"dbname"`
	Nodes   []string `json:"nodes"`
	Output  string   `json:"output"`
}

type schemaDiffRequest struct {
	Cluster           string   `json:"cluster"`
	Schema            string   `json:"schema"`
	DBName            string   `json:"dbname"`
	Nodes             []string `json:"nodes"`
	SkipTables        string   `json:"skip_tables"`
	SkipFile          string   `json:"skip_file"`
	DDLOnly           bool     `json:"ddl_only"`
	BlockSize         int      `json:"block_size"`
	Concurrency       int      `json:"concurrency_factor"`
	CompareUnitSize   int      `json:"compare_unit_size"`
	Output            string   `json:"output"`
	OverrideBlockSize bool     `json:"override_block_size"`
	Quiet             bool     `json:"quiet"`
}

type repsetDiffRequest struct {
	Cluster           string   `json:"cluster"`
	Repset            string   `json:"repset"`
	DBName            string   `json:"dbname"`
	Nodes             []string `json:"nodes"`
	SkipTables        string   `json:"skip_tables"`
	SkipFile          string   `json:"skip_file"`
	BlockSize         int      `json:"block_size"`
	Concurrency       int      `json:"concurrency_factor"`
	CompareUnitSize   int      `json:"compare_unit_size"`
	Output            string   `json:"output"`
	OverrideBlockSize bool     `json:"override_block_size"`
	Quiet             bool     `json:"quiet"`
}

type mtreeClusterRequest struct {
	Cluster string   `json:"cluster"`
	DBName  string   `json:"dbname"`
	Nodes   []string `json:"nodes"`
	Quiet   bool     `json:"quiet"`
}

type mtreeTableRequest struct {
	Cluster string   `json:"cluster"`
	Table   string   `json:"table"`
	DBName  string   `json:"dbname"`
	Nodes   []string `json:"nodes"`
	Quiet   bool     `json:"quiet"`
}

type mtreeBuildRequest struct {
	Cluster           string   `json:"cluster"`
	Table             string   `json:"table"`
	DBName            string   `json:"dbname"`
	Nodes             []string `json:"nodes"`
	BlockSize         int      `json:"block_size"`
	MaxCPURatio       float64  `json:"max_cpu_ratio"`
	OverrideBlockSize bool     `json:"override_block_size"`
	Analyse           bool     `json:"analyse"`
	RecreateObjects   bool     `json:"recreate_objects"`
	WriteRanges       bool     `json:"write_ranges"`
	RangesFile        string   `json:"ranges_file"`
	Quiet             bool     `json:"quiet"`
}

type mtreeUpdateRequest struct {
	Cluster     string   `json:"cluster"`
	Table       string   `json:"table"`
	DBName      string   `json:"dbname"`
	Nodes       []string `json:"nodes"`
	MaxCPURatio float64  `json:"max_cpu_ratio"`
	Rebalance   bool     `json:"rebalance"`
	Quiet       bool     `json:"quiet"`
}

type mtreeDiffRequest struct {
	Cluster     string   `json:"cluster"`
	Table       string   `json:"table"`
	DBName      string   `json:"dbname"`
	Nodes       []string `json:"nodes"`
	MaxCPURatio float64  `json:"max_cpu_ratio"`
	Output      string   `json:"output"`
	SkipUpdate  bool     `json:"skip_update"`
	Quiet       bool     `json:"quiet"`
}

type taskSubmissionResponse struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

type taskStatusResponse struct {
	TaskID      string         `json:"task_id"`
	TaskType    string         `json:"task_type"`
	Status      string         `json:"status"`
	Cluster     string         `json:"cluster"`
	Schema      string         `json:"schema,omitempty"`
	Table       string         `json:"table,omitempty"`
	Repset      string         `json:"repset,omitempty"`
	StartedAt   string         `json:"started_at,omitempty"`
	FinishedAt  string         `json:"finished_at,omitempty"`
	TimeTaken   float64        `json:"time_taken,omitempty"`
	TaskContext map[string]any `json:"task_context,omitempty"`
}

func (s *APIServer) handleTableDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	defer r.Body.Close()

	var req tableDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	tableName := strings.TrimSpace(req.Table)
	if tableName == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	task := diff.NewTableDiffTask()
	task.ClusterName = cluster
	task.QualifiedTableName = tableName
	task.DBName = strings.TrimSpace(req.DBName)
	task.BlockSize = s.resolveBlockSize(req.BlockSize)
	task.ConcurrencyFactor = s.resolveConcurrency(req.Concurrency)
	task.CompareUnitSize = s.resolveCompareUnitSize(req.CompareUnitSize)
	task.MaxDiffRows = s.resolveMaxDiffRows(req.MaxDiffRows)
	task.Output = "json"
	task.Nodes = s.resolveNodes(req.Nodes)
	task.TableFilter = strings.TrimSpace(req.TableFilter)
	task.OverrideBlockSize = req.OverrideBlockSize
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.QuietMode = req.Quiet
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := task.RunChecks(true); err != nil {
		logger.Error("table-diff pre-run checks failed: %v", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.ExecuteTask()
	}); err != nil {
		logger.Error("failed to enqueue table-diff task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	resp := taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *APIServer) resolveBlockSize(requested int) int {
	if requested > 0 {
		return requested
	}
	if cfg := s.cfg; cfg != nil && cfg.TableDiff.DiffBlockSize > 0 {
		return cfg.TableDiff.DiffBlockSize
	}
	return 100000
}

func (s *APIServer) resolveConcurrency(requested int) int {
	if requested > 0 {
		return requested
	}
	if cfg := s.cfg; cfg != nil && cfg.TableDiff.ConcurrencyFactor > 0 {
		return cfg.TableDiff.ConcurrencyFactor
	}
	return 1
}

func (s *APIServer) resolveCompareUnitSize(requested int) int {
	if requested > 0 {
		return requested
	}
	if cfg := s.cfg; cfg != nil && cfg.TableDiff.CompareUnitSize > 0 {
		return cfg.TableDiff.CompareUnitSize
	}
	return 10000
}

func (s *APIServer) resolveMaxDiffRows(requested int64) int64 {
	if requested > 0 {
		return requested
	}
	if cfg := s.cfg; cfg != nil && cfg.TableDiff.MaxDiffRows > 0 {
		return cfg.TableDiff.MaxDiffRows
	}
	return 0
}

func (s *APIServer) resolveNodes(nodes []string) string {
	if len(nodes) == 0 {
		return "all"
	}
	clean := make([]string, 0, len(nodes))
	seen := make(map[string]struct{})
	for _, n := range nodes {
		trimmed := strings.TrimSpace(n)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		clean = append(clean, trimmed)
	}
	if len(clean) == 0 {
		return "all"
	}
	return strings.Join(clean, ",")
}

func (s *APIServer) handleTableRerun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req tableRerunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	diffFile := strings.TrimSpace(req.DiffFile)
	if diffFile == "" {
		writeError(w, http.StatusBadRequest, "diff_file is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := diff.NewTableDiffTask()
	task.Mode = "rerun"
	task.ClusterName = cluster
	task.DiffFilePath = diffFile
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.ExecuteTask()
	}); err != nil {
		logger.Error("failed to enqueue table-rerun task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleTableRepair(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req tableRepairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	tableName := strings.TrimSpace(req.Table)
	if tableName == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}

	diffFile := strings.TrimSpace(req.DiffFile)
	if diffFile == "" {
		writeError(w, http.StatusBadRequest, "diff_file is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := repair.NewTableRepairTask()
	task.ClusterName = cluster
	task.QualifiedTableName = tableName
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.DiffFilePath = diffFile
	task.RepairPlanPath = strings.TrimSpace(req.RepairPlan)
	task.SourceOfTruth = strings.TrimSpace(req.SourceOfTruth)
	task.QuietMode = req.Quiet
	task.DryRun = req.DryRun
	task.InsertOnly = req.InsertOnly
	task.UpsertOnly = req.UpsertOnly
	task.FireTriggers = req.FireTriggers
	task.GenerateReport = req.GenerateReport
	task.FixNulls = req.FixNulls
	task.Bidirectional = req.Bidirectional
	// PreserveOrigin defaults to true if not explicitly set
	if req.PreserveOrigin != nil {
		task.PreserveOrigin = *req.PreserveOrigin
	}
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.ValidateAndPrepare(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.Run(true)
	}); err != nil {
		logger.Error("failed to enqueue table-repair task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleSpockDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req spockDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := diff.NewSpockDiffTask()
	task.ClusterName = cluster
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.Output = strings.TrimSpace(req.Output)
	if task.Output == "" {
		task.Output = "json"
	}
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.RunChecks(true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.ExecuteTask()
	}); err != nil {
		logger.Error("failed to enqueue spock-diff task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleSchemaDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req schemaDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	schema := strings.TrimSpace(req.Schema)
	if schema == "" {
		writeError(w, http.StatusBadRequest, "schema is required")
		return
	}

	task := diff.NewSchemaDiffTask()
	task.ClusterName = cluster
	task.SchemaName = schema
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.SkipTables = req.SkipTables
	task.SkipFile = req.SkipFile
	task.Quiet = req.Quiet
	task.DDLOnly = req.DDLOnly
	task.BlockSize = s.resolveBlockSize(req.BlockSize)
	task.ConcurrencyFactor = s.resolveConcurrency(req.Concurrency)
	task.CompareUnitSize = s.resolveCompareUnitSize(req.CompareUnitSize)
	task.Output = strings.TrimSpace(req.Output)
	if task.Output == "" {
		task.Output = "json"
	}
	task.OverrideBlockSize = req.OverrideBlockSize
	task.Ctx = r.Context()
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.RunChecks(true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.SchemaTableDiff()
	}); err != nil {
		logger.Error("failed to enqueue schema-diff task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleRepsetDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req repsetDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	repset := strings.TrimSpace(req.Repset)
	if repset == "" {
		writeError(w, http.StatusBadRequest, "repset is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := diff.NewRepsetDiffTask()
	task.ClusterName = cluster
	task.RepsetName = repset
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.SkipTables = req.SkipTables
	task.SkipFile = req.SkipFile
	task.Quiet = req.Quiet
	task.BlockSize = s.resolveBlockSize(req.BlockSize)
	task.ConcurrencyFactor = s.resolveConcurrency(req.Concurrency)
	task.CompareUnitSize = s.resolveCompareUnitSize(req.CompareUnitSize)
	task.Output = strings.TrimSpace(req.Output)
	if task.Output == "" {
		task.Output = "json"
	}
	task.OverrideBlockSize = req.OverrideBlockSize
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.RunChecks(true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return diff.RepsetDiff(task)
	}); err != nil {
		logger.Error("failed to enqueue repset-diff task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) resolveClusterName(name string) (string, error) {
	cluster := strings.TrimSpace(name)
	if cluster == "" {
		cluster = config.DefaultCluster()
	}
	if cluster == "" {
		return "", fmt.Errorf("cluster is required")
	}
	return cluster, nil
}

func (s *APIServer) handleMtreeInit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req mtreeClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = cluster
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.Mode = "init"
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.MtreeInit()
	}); err != nil {
		logger.Error("failed to enqueue mtree init task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleMtreeTeardown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req mtreeClusterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = cluster
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.Mode = "teardown"
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.MtreeTeardown()
	}); err != nil {
		logger.Error("failed to enqueue mtree teardown task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleMtreeTeardownTable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req mtreeTableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = cluster
	task.QualifiedTableName = table
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.Mode = "teardown-table"
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.MtreeTeardownTable()
	}); err != nil {
		logger.Error("failed to enqueue mtree teardown-table task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleMtreeBuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req mtreeBuildRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = cluster
	task.QualifiedTableName = table
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.BlockSize = s.resolveMtreeBlockSize(req.BlockSize)
	task.MaxCpuRatio = s.resolveMtreeMaxCPURatio(req.MaxCPURatio)
	task.OverrideBlockSize = req.OverrideBlockSize
	task.Analyse = req.Analyse
	task.RecreateObjects = req.RecreateObjects
	task.WriteRanges = req.WriteRanges
	task.RangesFile = strings.TrimSpace(req.RangesFile)
	task.Mode = "build"
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.RunChecks(true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.BuildMtree()
	}); err != nil {
		logger.Error("failed to enqueue mtree build task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleMtreeUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req mtreeUpdateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = cluster
	task.QualifiedTableName = table
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.MaxCpuRatio = s.resolveMtreeMaxCPURatio(req.MaxCPURatio)
	task.Rebalance = req.Rebalance
	task.Mode = "update"
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.RunChecks(true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.UpdateMtree(true)
	}); err != nil {
		logger.Error("failed to enqueue mtree update task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) handleMtreeDiff(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}
	if s.taskStore == nil {
		writeError(w, http.StatusServiceUnavailable, "task store unavailable")
		return
	}

	var req mtreeDiffRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON payload")
		return
	}
	defer r.Body.Close()

	cluster, err := s.resolveClusterName(req.Cluster)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	table := strings.TrimSpace(req.Table)
	if table == "" {
		writeError(w, http.StatusBadRequest, "table is required")
		return
	}

	clientInfo, ok := getClientInfo(r.Context())
	if !ok || strings.TrimSpace(clientInfo.role) == "" {
		writeError(w, http.StatusUnauthorized, "client identity unavailable")
		return
	}

	task := mtree.NewMerkleTreeTask()
	task.ClusterName = cluster
	task.QualifiedTableName = table
	task.DBName = strings.TrimSpace(req.DBName)
	task.Nodes = s.resolveNodes(req.Nodes)
	task.QuietMode = req.Quiet
	task.MaxCpuRatio = s.resolveMtreeMaxCPURatio(req.MaxCPURatio)
	task.Output = strings.TrimSpace(req.Output)
	if task.Output == "" {
		task.Output = "json"
	}
	task.NoCDC = req.SkipUpdate
	task.Mode = "diff"
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.SkipDBUpdate = false
	task.TaskStore = s.taskStore
	task.TaskStorePath = s.cfg.Server.TaskStorePath

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.RunChecks(true); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.enqueueTask(task.TaskID, func(ctx context.Context) error {
		task.Ctx = ctx
		return task.DiffMtree()
	}); err != nil {
		logger.Error("failed to enqueue mtree diff task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	writeJSON(w, http.StatusAccepted, taskSubmissionResponse{
		TaskID: task.TaskID,
		Status: "QUEUED",
	})
}

func (s *APIServer) resolveMtreeBlockSize(requested int) int {
	if requested > 0 {
		return requested
	}
	return 10000
}

func (s *APIServer) resolveMtreeMaxCPURatio(requested float64) float64 {
	if requested > 0 {
		return requested
	}
	return 0.5
}

func (s *APIServer) handleTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeError(w, http.StatusMethodNotAllowed, "only GET is supported")
		return
	}

	if s.taskStore == nil {
		writeError(w, http.StatusInternalServerError, "task store unavailable")
		return
	}

	taskID := strings.TrimPrefix(r.URL.Path, "/api/v1/tasks/")
	taskID = strings.Trim(taskID, "/")
	if taskID == "" {
		writeError(w, http.StatusBadRequest, "task_id is required")
		return
	}

	rec, err := s.taskStore.Get(taskID)
	if errors.Is(err, taskstore.ErrNotFound) {
		writeError(w, http.StatusNotFound, "task not found")
		return
	}
	if err != nil {
		logger.Error("failed to fetch task %s: %v", taskID, err)
		writeError(w, http.StatusInternalServerError, "failed to fetch task status")
		return
	}

	resp := taskStatusResponse{
		TaskID:      rec.TaskID,
		TaskType:    rec.TaskType,
		Status:      rec.Status,
		Cluster:     rec.ClusterName,
		Schema:      rec.SchemaName,
		Table:       rec.TableName,
		Repset:      rec.RepsetName,
		TimeTaken:   rec.TimeTaken,
		TaskContext: rec.TaskContext,
	}
	if !rec.StartedAt.IsZero() {
		resp.StartedAt = rec.StartedAt.Format(time.RFC3339Nano)
	}
	if !rec.FinishedAt.IsZero() {
		resp.FinishedAt = rec.FinishedAt.Format(time.RFC3339Nano)
	}

	writeJSON(w, http.StatusOK, resp)
}
