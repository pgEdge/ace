package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pgedge/ace/internal/core"
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
	TableFilter       string   `json:"table_filter"`
	OverrideBlockSize bool     `json:"override_block_size"`
}

type tableDiffResponse struct {
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

	cluster := strings.TrimSpace(req.Cluster)
	if cluster == "" {
		cluster = config.DefaultCluster()
	}
	if cluster == "" {
		writeError(w, http.StatusBadRequest, "cluster is required")
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

	task := core.NewTableDiffTask()
	task.ClusterName = cluster
	task.QualifiedTableName = tableName
	task.DBName = strings.TrimSpace(req.DBName)
	task.BlockSize = s.resolveBlockSize(req.BlockSize)
	task.ConcurrencyFactor = s.resolveConcurrency(req.Concurrency)
	task.CompareUnitSize = s.resolveCompareUnitSize(req.CompareUnitSize)
	task.Output = "json"
	task.Nodes = s.resolveNodes(req.Nodes)
	task.TableFilter = strings.TrimSpace(req.TableFilter)
	task.OverrideBlockSize = req.OverrideBlockSize
	task.Ctx = r.Context()
	task.ClientRole = clientInfo.role
	task.InvokeMethod = "api"
	task.QuietMode = true
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

	if err := s.enqueueTableDiffTask(task); err != nil {
		logger.Error("failed to enqueue table-diff task: %v", err)
		writeError(w, http.StatusInternalServerError, "unable to enqueue task")
		return
	}

	resp := tableDiffResponse{
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

func (s *APIServer) enqueueTableDiffTask(task *core.TableDiffTask) error {
	if s == nil || task == nil {
		return fmt.Errorf("invalid task")
	}
	if s.taskStore == nil {
		return fmt.Errorf("task store is not initialised")
	}
	s.wg.Add(1)
	go func(t *core.TableDiffTask) {
		defer s.wg.Done()
		ctx, cancel := context.WithCancel(s.jobCtx)
		defer cancel()
		t.Ctx = ctx
		if err := t.ExecuteTask(); err != nil {
			logger.Error("table-diff task %s failed: %v", t.TaskID, err)
		}
	}(task)
	return nil
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
