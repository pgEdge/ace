package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
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
	TaskID string           `json:"task_id"`
	Result types.DiffOutput `json:"result"`
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
	task.SkipDBUpdate = true
	task.QuietMode = true

	if err := task.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := task.RunChecks(true); err != nil {
		logger.Error("table-diff pre-run checks failed: %v", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := task.ExecuteTask(); err != nil {
		logger.Error("table-diff execution failed: %v", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := tableDiffResponse{
		TaskID: task.TaskID,
		Result: task.DiffResult,
	}
	writeJSON(w, http.StatusOK, resp)
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
