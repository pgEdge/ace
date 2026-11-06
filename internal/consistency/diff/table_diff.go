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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/keydiff"
	"github.com/pgedge/ace/internal/consistency/schema"
	"github.com/pgedge/ace/internal/consistency/slicer"
	auth "github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

// NoSelectPrivilegeError is returned by RunChecks when the user cannot read
// the table on Node. schema-diff and repset-diff skip such tables instead
// of failing.
type NoSelectPrivilegeError struct {
	User  string
	Table string // schema-qualified
	Node  string
	Hint  string // from UserPrivileges.SelectHint
}

func (e *NoSelectPrivilegeError) Error() string {
	return fmt.Sprintf("user \"%s\" cannot read table \"%s\" on node \"%s\"%s",
		e.User, e.Table, e.Node, e.Hint)
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

	BaseTable string

	BlockSize         int
	ConcurrencyFactor float64
	MaxConnections    int
	Output            string
	TableFilter       string
	QuietMode         bool

	AgainstOrigin string
	Until         string

	EffectiveFilter string

	Mode              string
	OverrideBlockSize bool

	DiffFilePath string

	InvokeMethod string
	ClientRole   string

	DiffSummary map[string]string

	SkipDBUpdate bool

	TaskStore     *taskstore.Store
	TaskStorePath string

	Pools map[string]*pgxpool.Pool

	blockHashSQLCache map[hashBoundsKey]string
	blockHashSQLMu    sync.Mutex

	NodeOriginNames map[string]map[string]string

	// CompareUnitSize is no longer used. It is kept so that old settings,
	// jobs and API requests still work, and a run warns about it.
	CompareUnitSize int
	MaxDiffRows     int64
	// KeyMapMaxMemory is the limit, in bytes, of the memory for the keys of
	// one mismatched block. 0 means table_diff.key_map_max_memory_mb from the
	// settings file; a negative value means no limit.
	KeyMapMaxMemory int64
	// MaxHTMLRows limits how many entries the HTML report shows for each
	// node pair. 0 means table_diff.max_html_rows from the config, and if
	// that is not set either, utils.DefaultMaxHTMLRows.
	MaxHTMLRows int64
	// MaxHTMLValueLength is the number of characters of a value that the
	// HTML report shows. 0 means table_diff.max_html_value_length from the
	// config, and if that is not set either,
	// utils.DefaultMaxHTMLValueLength.
	MaxHTMLValueLength int64

	// DiffResult holds the summary of the run. The differing rows are not
	// kept in memory: they go to the result directory, DiffResultDir.
	DiffResult types.DiffOutput
	diffMutex  sync.Mutex
	// DiffResultDir is the result directory of the last run, or empty if
	// the run found no differences.
	DiffResultDir string

	// result writes the differing rows of the current run.
	result *utils.DiffResultWriter

	firstError    error
	firstErrorMu  sync.Mutex
	errorRecorded atomic.Bool

	// diffRows counts the differing rows of all node pairs of the run
	// against max_diff_rows. diffLimitHit is set when the count reaches
	// the limit.
	diffRows     atomic.Int64
	diffLimitHit atomic.Bool

	// diffSem limits how many mismatched blocks are compared at the same
	// time. It is a buffered channel with one slot per comparison task. A
	// goroutine "acquires" a slot by sending into the channel (blocks when
	// full) and "releases" by receiving.
	//
	// The size of the channel is safe only because a goroutine that holds a
	// slot uses at most one connection to each node at a time: the
	// comparison of a block runs its queries one after another. With a
	// strict limit the pool of a node can have as few connections as there
	// are slots, so a goroutine that ran two queries on one node at the same
	// time would wait for the pool and hold its slot all that time.
	diffSem chan struct{}

	// keyWidths and scanChildren are set before the mismatched blocks are
	// compared and only read after that.
	keyWidths    map[string]int64
	scanChildren bool

	resolvedAgainstOrigin string
	untilTime             *time.Time

	// BlockTimeout and RunTimeout limit, in seconds, one query on one block
	// and the whole run. nil means the value from ace.yaml; 0 means no
	// limit.
	BlockTimeout *int
	RunTimeout   *int

	// blockTimeout is the block timeout of the current run.
	blockTimeout time.Duration

	// stopRun stops the context of the current run with a cause. It is set
	// only while the run is in progress.
	stopRun context.CancelCauseFunc

	Ctx context.Context
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

// recordError keeps the first error of the run and stops the run with it,
// so that the workers on all nodes stop.
func (t *TableDiffTask) recordError(err error) {
	if err == nil {
		return
	}
	t.firstErrorMu.Lock()
	defer t.firstErrorMu.Unlock()
	if t.firstError != nil {
		return
	}
	t.firstError = err
	t.errorRecorded.Store(true)
	// The first error stops the whole run, so the queries of the other
	// workers stop on every node at once. The stop happens under the lock,
	// so a later error cannot stop the run first and become its cause.
	if t.stopRun != nil {
		t.stopRun(err)
	}
}

func (t *TableDiffTask) getFirstError() error {
	t.firstErrorMu.Lock()
	defer t.firstErrorMu.Unlock()
	return t.firstError
}

func (t *TableDiffTask) hasError() bool {
	return t.errorRecorded.Load()
}

// pairKeyFor returns the canonical (lexically ordered) "nodeA/nodeB" key used to
// bucket diffs and the per-pair row limit.
func pairKeyFor(node1, node2 string) string {
	if strings.Compare(node1, node2) > 0 {
		return node2 + "/" + node1
	}
	return node1 + "/" + node2
}

// diffLimitReached tells whether the differing rows of the run reached
// max_diff_rows.
func (t *TableDiffTask) diffLimitReached() bool {
	return t.diffLimitHit.Load()
}

// countDiffRows adds the differing rows of one compared block to the count
// of the run. When the count reaches max_diff_rows, the blocks that are not
// compared yet are skipped. The limit is approximate: the blocks that are in
// work at that moment are still compared to the end.
func (t *TableDiffTask) countDiffRows(n int) {
	if n <= 0 || t.MaxDiffRows <= 0 {
		return
	}
	if t.diffRows.Add(int64(n)) >= t.MaxDiffRows && t.diffLimitHit.CompareAndSwap(false, true) {
		logger.Warn("table-diff: reached max_diff_rows limit (%d); the blocks that are not compared yet are skipped", t.MaxDiffRows)
	}
}

func (t *TableDiffTask) loadNodeOriginNames() error {
	if t.NodeOriginNames != nil {
		return nil
	}

	t.NodeOriginNames = make(map[string]map[string]string)

	var lastErr error
	for name, pool := range t.Pools {
		names, err := queries.GetNodeOriginNames(t.Ctx, pool)
		if err != nil {
			lastErr = err
			continue
		}
		t.NodeOriginNames[name] = names
	}

	if len(t.NodeOriginNames) == 0 && lastErr != nil {
		return lastErr
	}
	return nil
}

// flatNodeOriginNames merges all per-node origin maps into a single map for
// lookup purposes (e.g. resolving --against-origin). If the same roident
// appears on multiple nodes, the last one wins: roidents are global under
// spock, and any match suffices for user-facing name resolution.
func (t *TableDiffTask) flatNodeOriginNames() map[string]string {
	flat := make(map[string]string)
	for _, nodeMap := range t.NodeOriginNames {
		for id, name := range nodeMap {
			flat[id] = name
		}
	}
	return flat
}

func (t *TableDiffTask) resolveAgainstOrigin() error {
	if strings.TrimSpace(t.AgainstOrigin) == "" {
		return nil
	}
	flat := t.flatNodeOriginNames()
	if len(flat) == 0 {
		return fmt.Errorf("unable to resolve --against-origin: no node origin names available")
	}

	orig := strings.TrimSpace(t.AgainstOrigin)
	// direct match on origin id
	if _, ok := flat[orig]; ok {
		t.resolvedAgainstOrigin = orig
		return nil
	}

	// match on origin name
	for id, name := range flat {
		if name == orig {
			t.resolvedAgainstOrigin = id
			return nil
		}
	}

	// build a list of available origins for the error message
	available := make([]string, 0, len(flat))
	for id, name := range flat {
		if id != name {
			available = append(available, fmt.Sprintf("%s (%s)", id, name))
		} else {
			available = append(available, id)
		}
	}

	return fmt.Errorf("unable to resolve --against-origin %q; available origins: %s", t.AgainstOrigin, strings.Join(available, ", "))
}

func (t *TableDiffTask) buildEffectiveFilter() (string, error) {
	if t.untilTime == nil && strings.TrimSpace(t.Until) != "" {
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(t.Until))
		if err != nil {
			return "", fmt.Errorf("invalid value for --until (expected RFC3339 timestamp): %w", err)
		}
		t.untilTime = &parsed
	}

	var parts []string
	trimmed := strings.TrimSpace(t.TableFilter)
	if trimmed != "" {
		parts = append(parts, fmt.Sprintf("(%s)", trimmed))
	}

	if t.resolvedAgainstOrigin != "" {
		nodeID, err := strconv.Atoi(t.resolvedAgainstOrigin)
		if err != nil {
			return "", fmt.Errorf("resolved against-origin %q is not a valid numeric node ID", t.resolvedAgainstOrigin)
		}
		parts = append(parts, fmt.Sprintf("(to_json(pg_xact_commit_timestamp_origin(xmin))->>'roident' = '%d')", nodeID))
	}

	if f := queries.CommitTimestampFilter(t.untilTime); f != "" {
		parts = append(parts, f)
	}

	if len(parts) == 0 {
		return "", nil
	}
	return strings.Join(parts, " AND "), nil
}

func (t *TableDiffTask) withSpockMetadata(row map[string]any, nodeName string) map[string]any {
	row["node_origin"] = utils.TranslateNodeOrigin(row["node_origin"], t.NodeOriginNames[nodeName])
	return utils.AddSpockMetadata(row)
}

type HashResult struct {
	hash string
	rows int64
	err  error
}

type hashBoundsKey struct {
	hasLower bool
	hasUpper bool
}

func extractPlanRowEstimate(planJSON []byte) (int64, error) {
	var plans []map[string]any
	if err := json.Unmarshal(planJSON, &plans); err != nil {
		return 0, fmt.Errorf("failed to parse EXPLAIN output: %w", err)
	}
	if len(plans) == 0 {
		return 0, fmt.Errorf("EXPLAIN returned no plan")
	}

	rootPlan, ok := plans[0]["Plan"].(map[string]any)
	if !ok {
		return 0, fmt.Errorf("unexpected EXPLAIN plan format")
	}
	if rowsVal, ok := rootPlan["Plan Rows"]; ok {
		if rowsFloat, ok := rowsVal.(float64); ok {
			return int64(rowsFloat), nil
		}
	}
	return 0, fmt.Errorf("plan rows not found in EXPLAIN output")
}

func (t *TableDiffTask) estimateRowCount(pool *pgxpool.Pool, nodeName string) (int64, error) {
	schemaIdent := pgx.Identifier{t.Schema}.Sanitize()
	tableIdent := pgx.Identifier{t.Table}.Sanitize()

	query := fmt.Sprintf("EXPLAIN (FORMAT JSON) SELECT 1 FROM %s.%s", schemaIdent, tableIdent)
	if strings.TrimSpace(t.EffectiveFilter) != "" {
		query = fmt.Sprintf("%s WHERE %s", query, t.EffectiveFilter)
	}

	var planJSON []byte
	if err := pool.QueryRow(t.Ctx, query).Scan(&planJSON); err != nil { // nosemgrep
		return 0, fmt.Errorf("failed to estimate row count on node %s: %w", nodeName, err)
	}

	return extractPlanRowEstimate(planJSON)
}

func (t *TableDiffTask) ensureFilterHasRows(pool *pgxpool.Pool, nodeName string) error {
	if strings.TrimSpace(t.EffectiveFilter) == "" {
		return nil
	}

	schemaIdent := pgx.Identifier{t.Schema}.Sanitize()
	tableIdent := pgx.Identifier{t.Table}.Sanitize()
	sql := fmt.Sprintf("SELECT 1 FROM %s.%s WHERE %s LIMIT 1", schemaIdent, tableIdent, t.EffectiveFilter)

	var one int
	if err := pool.QueryRow(t.Ctx, sql).Scan(&one); err != nil { // nosemgrep
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("table filter produced no rows")
		}
		return fmt.Errorf("failed to validate table filter on node %s: %w", nodeName, err)
	}
	return nil
}

type RangeResults map[string]HashResult

// getBlockHashSQL returns the block hash query for a range with or without a
// lower bound and an upper bound. Each of the four queries is built once and
// kept for the run.
func (t *TableDiffTask) getBlockHashSQL(hasLower, hasUpper bool) (string, error) {
	key := hashBoundsKey{hasLower: hasLower, hasUpper: hasUpper}

	t.blockHashSQLMu.Lock()
	defer t.blockHashSQLMu.Unlock()

	if t.blockHashSQLCache == nil {
		t.blockHashSQLCache = make(map[hashBoundsKey]string)
	}

	if sql, ok := t.blockHashSQLCache[key]; ok {
		return sql, nil
	}

	query, err := queries.BlockHashSQL(t.Schema, t.Table, t.Key, "TD_BLOCK_HASH" /* mode */, hasLower, hasUpper, t.EffectiveFilter, t.Cols, t.hashColTypes())
	if err != nil {
		return "", err
	}
	t.blockHashSQLCache[key] = query
	return query, nil
}

func NewTableDiffTask() *TableDiffTask {
	return &TableDiffTask{
		Task: types.Task{
			TaskID:     uuid.NewString(),
			TaskType:   taskstore.TaskTypeTableDiff,
			TaskStatus: taskstore.StatusPending,
		},
		Mode:              "diff",
		InvokeMethod:      "cli",
		DiffSummary:       make(map[string]string),
		blockHashSQLCache: make(map[hashBoundsKey]string),
		DerivedFields: types.DerivedFields{
			HostMap: make(map[string]string),
		},
		Ctx: context.Background(),
	}
}

// nodeColTypes returns the column types of the table on nodeName.
func (t *TableDiffTask) nodeColTypes(nodeName string) (map[string]string, error) {
	var colTypesKey string
	for _, nodeInfo := range t.ClusterNodes {
		if name, ok := nodeInfo["Name"].(string); ok && name == nodeName {
			publicIP, _ := nodeInfo["PublicIP"].(string)

			var portStr string
			switch v := nodeInfo["Port"].(type) {
			case string:
				portStr = v
			case float64:
				portStr = fmt.Sprintf("%.f", v)
			default:
				portStr = "5432"
			}
			colTypesKey = fmt.Sprintf("%s:%s", publicIP, portStr)
			if colTypes := t.ColTypes[colTypesKey]; colTypes != nil {
				return colTypes, nil
			}
			break
		}
	}
	return nil, fmt.Errorf("could not find column types for node %s using key %s", nodeName, colTypesKey)
}

// hashColTypes returns the column types that the row hash uses. The schemas
// of the nodes were checked to match, so the types of any node are right,
// but the choice must be the same for every hash of the run: it is the node
// whose key comes first.
func (t *TableDiffTask) hashColTypes() map[string]string {
	keys := make([]string, 0, len(t.ColTypes))
	for k := range t.ColTypes {
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	return t.ColTypes[keys[0]]
}

// Validate checks the arguments of the task, fills the values that the
// task does not set from ace.yaml, and loads the cluster information.
func (t *TableDiffTask) Validate() error {
	if t.ClusterName == "" || t.QualifiedTableName == "" {
		return fmt.Errorf("cluster_name and table_name are required arguments")
	}

	cfg := config.Get()
	if t.MaxConnections < 0 || t.MaxConnections > config.MaxConnectionsPerNode {
		return fmt.Errorf("max_connections must be between 1 and %d (or 0 to take the value from the configuration file), got %d",
			config.MaxConnectionsPerNode, t.MaxConnections)
	}
	if err := cfg.CheckConnectionLimits(); err != nil {
		return err
	}
	if _, _, err := t.timeLimits(); err != nil {
		return err
	}
	if t.BlockSize > cfg.TableDiff.MaxBlockSize && !t.OverrideBlockSize {
		return fmt.Errorf("block row size should be <= %d", cfg.TableDiff.MaxBlockSize)
	}
	if t.BlockSize < cfg.TableDiff.MinBlockSize && !t.OverrideBlockSize {
		return fmt.Errorf("block row size should be >= %d", cfg.TableDiff.MinBlockSize)
	}

	if t.MaxDiffRows < 0 {
		return fmt.Errorf("max_diff_rows must be >= 0")
	}
	if t.MaxDiffRows == 0 && cfg.TableDiff.MaxDiffRows > 0 {
		t.MaxDiffRows = cfg.TableDiff.MaxDiffRows
	}

	if t.MaxHTMLRows < 0 {
		return fmt.Errorf("max_html_rows must be >= 0")
	}
	if cfg.TableDiff.MaxHTMLRows < 0 {
		return fmt.Errorf("table_diff.max_html_rows in the config must be >= 0, got %d", cfg.TableDiff.MaxHTMLRows)
	}
	if t.MaxHTMLRows == 0 && cfg.TableDiff.MaxHTMLRows > 0 {
		t.MaxHTMLRows = cfg.TableDiff.MaxHTMLRows
	}

	if t.MaxHTMLValueLength < 0 {
		return fmt.Errorf("max_html_value_length must be >= 0")
	}
	if cfg.TableDiff.MaxHTMLValueLength < 0 {
		return fmt.Errorf("table_diff.max_html_value_length in the config must be >= 0, got %d", cfg.TableDiff.MaxHTMLValueLength)
	}
	if t.MaxHTMLValueLength == 0 && cfg.TableDiff.MaxHTMLValueLength > 0 {
		t.MaxHTMLValueLength = cfg.TableDiff.MaxHTMLValueLength
	}

	if t.ConcurrencyFactor > 4.0 || t.ConcurrencyFactor <= 0 {
		return fmt.Errorf("invalid value range for concurrency_factor, must be > 0 and <= 4.0")
	}

	if t.Output != "json" && t.Output != "html" {
		return fmt.Errorf("table-diff currently supports only json and html output formats")
	}

	if trimmed := strings.TrimSpace(t.Until); trimmed != "" {
		parsed, err := time.Parse(time.RFC3339Nano, trimmed)
		if err != nil {
			return fmt.Errorf("invalid value for --until (expected RFC3339 timestamp): %w", err)
		}
		t.untilTime = &parsed
	} else {
		t.untilTime = nil
	}

	nodeList, err := utils.ParseNodes(t.Nodes)
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

	err = utils.ReadClusterInfo(t)
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
	if err := queries.SanitiseIdentifier(schema); err != nil {
		return err
	}
	if err := queries.SanitiseIdentifier(table); err != nil {
		return err
	}

	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		if len(nodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !utils.Contains(nodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)
		maps.Copy(combinedMap, nodeMap)
		utils.ApplyDatabaseCredentials(combinedMap, t.Database)
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

func (t *TableDiffTask) RunChecks(skipValidation bool) (err error) {
	if !skipValidation {
		if err = t.Validate(); err != nil {
			return err
		}
	}

	var cols, key []string
	var refHostname string
	var refNodeInfo map[string]any
	hostMap := make(map[string]string)

	schema := t.Schema
	table := t.Table
	if t.BaseTable == "" {
		t.BaseTable = table
	}
	for _, nodeInfo := range t.ClusterNodes {
		hostname, _ := nodeInfo["Name"].(string)
		hostIP, _ := nodeInfo["PublicIP"].(string)
		user, _ := nodeInfo["DBUser"].(string)

		port, ok := nodeInfo["Port"].(string)
		if !ok {
			port = "5432"
		}

		if !utils.Contains(t.NodeList, hostname) {
			continue
		}

		if err := func() error {
			conn, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, t.connOpts())
			if err != nil {
				return fmt.Errorf("failed to connect to node %s: %w", hostname, err)
			}
			defer conn.Close()

			tree, err := queries.GetRelationTree(t.Ctx, conn, schema, table)
			if err != nil {
				return fmt.Errorf("failed to read inheritance tree for %s.%s on node %s: %w", schema, table, hostname, err)
			}
			if tree == nil {
				return fmt.Errorf("table '%s.%s' not found on %s", schema, table, hostname)
			}
			if reason := tree.UnsupportedReason(); reason != "" {
				hint := ""
				if tree.Root.RelKind == "v" || tree.Root.RelKind == "m" {
					hint, err = queries.HotTableHint(t.Ctx, conn, schema, table)
					if err != nil {
						return fmt.Errorf("failed to look up the hot table for view %s.%s on node %s: %w", schema, table, hostname, err)
					}
				}
				return fmt.Errorf("'%s.%s' %s (node %s).%s", schema, table, reason, hostname, hint)
			}

			// Checked before the key and columns, so a table the user cannot
			// read is reported as such even if it would also fail those.
			actualPrivs, err := queries.CheckUserPrivileges(t.Ctx, conn, user, schema, table)
			if err != nil {
				return fmt.Errorf("failed to check user privileges on node %s: %w", hostname, err)
			}
			if !actualPrivs.SchemaUsage {
				return fmt.Errorf("user \"%s\" does not have USAGE on schema \"%s\" on node \"%s\"",
					user, schema, hostname)
			}
			if !actualPrivs.TableSelect {
				return &NoSelectPrivilegeError{
					User:  user,
					Table: schema + "." + table,
					Node:  hostname,
					Hint:  actualPrivs.SelectHint(),
				}
			}

			currCols, colTypes, err := queries.GetColumns(t.Ctx, conn, schema, table)
			if err != nil {
				return fmt.Errorf("failed to get columns for table %s.%s on node %s: %w", schema, table, hostname, err)
			}
			if len(currCols) == 0 {
				return fmt.Errorf("table '%s.%s' has no columns on %s", schema, table, hostname)
			}

			currKey, err := queries.GetPrimaryKey(t.Ctx, conn, schema, table)
			if err != nil {
				return fmt.Errorf("failed to get primary key for table %s.%s on node %s: %w", schema, table, hostname, err)
			}
			if len(currKey) == 0 {
				return fmt.Errorf("no primary key found for '%s.%s'", schema, table)
			}

			if len(cols) == 0 && len(key) == 0 {
				cols = currCols
				key = currKey
				refHostname = hostname
				refNodeInfo = nodeInfo
			}

			if !reflect.DeepEqual(currCols, cols) || !reflect.DeepEqual(currKey, key) {
				return t.diagnoseSchemaMismatch(schema, table, refNodeInfo, refHostname, cols, key, hostname, conn, currCols, currKey)
			}

			cols = currCols
			key = currKey

			colTypesKey := fmt.Sprintf("%s:%s", hostIP, port)

			if t.ColTypes == nil {
				t.ColTypes = make(map[string]map[string]string)
			}
			t.ColTypes[colTypesKey] = colTypes

			hostMap[hostIP+":"+port] = hostname

			if t.TableFilter != "" {
				logger.Info("Applying table filter for diff: %s", t.TableFilter)
			}
			return nil
		}(); err != nil {
			return err
		}
	}

	logger.Info("Connections successful to nodes in cluster")

	t.HostMap = hostMap
	t.Cols = cols
	t.Key = key
	t.SimplePrimaryKey = len(key) == 1

	if len(t.ColTypes) > 1 {
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

	return nil
}

// diagnoseSchemaMismatch runs after RunChecks' cheap column/key-name
// comparison finds a mismatch. It names the actual difference by reusing
// internal/consistency/schema's structural comparison, the same one
// schema-diff itself uses.
//
// It re-reads both nodes' full structural descriptors in a fresh
// REPEATABLE READ transaction each, reconnecting to the reference node
// since RunChecks already closed that connection. This cost is paid only
// on the error path.
func (t *TableDiffTask) diagnoseSchemaMismatch(
	schemaName, table string,
	refNodeInfo map[string]any, refHostname string, refCols, refKey []string,
	curHostname string, curConn *pgxpool.Pool, curCols, curKey []string,
) error {
	fallback := fmt.Errorf("table schemas don't match between nodes %s and %s: columns %v vs %v, key %v vs %v",
		refHostname, curHostname, refCols, curCols, refKey, curKey)

	refConn, err := auth.GetClusterNodeConnection(t.Ctx, refNodeInfo, t.connOpts())
	if err != nil {
		return fmt.Errorf("%w (could not reconnect to %s for a detailed diff: %v)", fallback, refHostname, err)
	}
	defer refConn.Close()

	refSnap, err := schema.CollectSnapshot(t.Ctx, refConn, refHostname, schemaName, []string{table})
	if err != nil {
		return fmt.Errorf("%w (could not collect a detailed diff from %s: %v)", fallback, refHostname, err)
	}
	curSnap, err := schema.CollectSnapshot(t.Ctx, curConn, curHostname, schemaName, []string{table})
	if err != nil {
		return fmt.Errorf("%w (could not collect a detailed diff from %s: %v)", fallback, curHostname, err)
	}

	divergences := schema.Compare(schemaName, []string{table}, refSnap, curSnap)
	if len(divergences) == 0 {
		// The name-only check disagreed but the structural comparison found
		// nothing reportable; fall back to what table-diff itself saw.
		return fallback
	}

	// FormatDivergences, not a local copy of it: it decodes the packed
	// values ("id, tenant", not "2:id6:tenant") and names the narrow side.
	var b strings.Builder
	fmt.Fprintf(&b, "table '%s.%s' schema differs between nodes %s and %s:\n", schemaName, table, refHostname, curHostname)
	b.WriteString(schema.FormatDivergences(divergences))
	return errors.New(b.String())
}

func (t *TableDiffTask) CloneForSchedule(ctx context.Context) *TableDiffTask {
	cloned := NewTableDiffTask()
	cloned.ClusterName = t.ClusterName
	cloned.QualifiedTableName = t.QualifiedTableName
	cloned.DBName = t.DBName
	cloned.Nodes = t.Nodes
	cloned.BlockSize = t.BlockSize
	cloned.ConcurrencyFactor = t.ConcurrencyFactor
	cloned.MaxConnections = t.MaxConnections
	cloned.BlockTimeout = t.BlockTimeout
	cloned.RunTimeout = t.RunTimeout
	cloned.Output = t.Output
	cloned.TableFilter = t.TableFilter
	cloned.QuietMode = t.QuietMode
	cloned.Mode = t.Mode
	cloned.OverrideBlockSize = t.OverrideBlockSize
	cloned.TaskStore = t.TaskStore
	cloned.TaskStorePath = t.TaskStorePath
	cloned.SkipDBUpdate = t.SkipDBUpdate
	cloned.ClientRole = t.ClientRole
	cloned.InvokeMethod = t.InvokeMethod
	cloned.CompareUnitSize = t.CompareUnitSize
	cloned.MaxDiffRows = t.MaxDiffRows
	cloned.MaxHTMLRows = t.MaxHTMLRows
	cloned.MaxHTMLValueLength = t.MaxHTMLValueLength
	cloned.AgainstOrigin = t.AgainstOrigin
	cloned.Until = t.Until
	cloned.Ctx = ctx
	return cloned
}

// compareWorkers returns the number of comparison tasks that run at the
// same time: ConcurrencyFactor times the number of CPUs of the client, at
// least one.
func (t *TableDiffTask) compareWorkers() int {
	n := int(math.Round(float64(runtime.NumCPU()) * t.ConcurrencyFactor))
	if n < 1 {
		n = 1
	}
	return n
}

// maxPoolSize returns the pool size that the comparison phase needs on each
// node. Each comparison task uses at most one connection to a node at a
// time, so the pool has room for all tasks and never makes a task wait. It
// is at least 4.
func (t *TableDiffTask) maxPoolSize() int {
	return max(t.compareWorkers(), 4)
}

func (t *TableDiffTask) connOpts() auth.ConnectionOptions {
	return auth.ConnectionOptions{PoolSize: t.maxPoolSize()}
}

// nodeLimit holds the number of connections of one node for one task.
type nodeLimit struct {
	// want is the number of connections that the task tries to open.
	want int
	// set is true when the user gave want: on the command line, in the HTTP
	// API, in a job or in the configuration file. Then want is a strict
	// limit for the whole task.
	set bool
	// opened is the number of connections that the server accepted.
	opened int
}

// nodeLimits returns the limit of every node in nodeNames: MaxConnections if
// it is set, else the value from the configuration file, else the built-in
// default. It reads the configuration once, so a reload of the file while
// the task runs does not change the limits of the task.
func (t *TableDiffTask) nodeLimits(nodeNames []string) map[string]*nodeLimit {
	cfg := config.Get()
	limits := make(map[string]*nodeLimit, len(nodeNames))
	for _, name := range nodeNames {
		l := &nodeLimit{want: t.MaxConnections, set: true}
		if t.MaxConnections <= 0 {
			l.want, l.set = cfg.ConnectionsFor(t.ClusterName, name)
		}
		limits[name] = l
	}
	return limits
}

// poolOpts returns the options of the pool of the node. A limit that the
// user sets is strict for the whole task: the pool has exactly that many
// connections, and the comparison phase also runs on them. With the
// built-in default the pool also has room for every comparison task. The
// pool keeps its connections for the whole task.
func (l *nodeLimit) poolOpts(maxPoolSize int) auth.ConnectionOptions {
	n := l.want
	if !l.set {
		n = max(n, maxPoolSize)
	}
	return auth.ConnectionOptions{PoolSize: n, KeepOpen: true}
}

// openNodeConns opens the connections of every node, up to the limit of the
// node, leaves them idle in the pools and writes their number to the limit.
// It opens the connections of all nodes at the same time. A node that has no
// free connection slot for all of them gives fewer, and the task goes on
// with them and writes a warning. A node that gives none stops the task.
func (t *TableDiffTask) openNodeConns(ctx context.Context, pools map[string]*pgxpool.Pool, nodeNames []string, limits map[string]*nodeLimit) error {
	type opened struct {
		n       int
		refusal *pgconn.PgError
		err     error
	}
	res := make([]opened, len(nodeNames))
	var wg sync.WaitGroup
	for i, name := range nodeNames {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := &res[i]
			r.n, r.refusal, r.err = auth.OpenConns(ctx, pools[name], limits[name].want)
		}()
	}
	wg.Wait()

	for i, name := range nodeNames {
		l, r := limits[name], res[i]
		if r.err != nil {
			return fmt.Errorf("failed to open connections to node %s: %w", name, r.err)
		}
		if r.n == 0 {
			return fmt.Errorf("node %s accepts no connection from ACE: %s", name, r.refusal.Message)
		}
		if r.n < l.want {
			logger.Warn("table-diff: node %s accepted %d of %d connections (%s); this node uses %d connections",
				name, r.n, l.want, r.refusal.Message, r.n)
		} else {
			logger.Info("table-diff: %d connections to node %s", r.n, name)
		}
		l.opened = r.n
	}
	return nil
}

// compareTasks returns the number of comparison tasks that run at the same
// time. A task uses at most one connection to a node at a time. On a node
// with a limit that the user sets, and on a node that gave fewer connections
// than its limit because its server has no free slot, the comparison phase
// must not open new connections, so there the number of tasks is at most the
// number of open connections.
func (t *TableDiffTask) compareTasks(limits map[string]*nodeLimit) int {
	tasks := t.compareWorkers()
	for _, l := range limits {
		if l.set || l.opened < l.want {
			tasks = min(tasks, l.opened)
		}
	}
	return tasks
}

// ExecuteTask runs one table-diff and writes the report. The run has its
// own context: the first error, the run timeout or the end of t.Ctx stops
// all its workers, and the error then names the cause of the stop. The task
// status is recorded in the task store.
func (t *TableDiffTask) ExecuteTask() (err error) {
	startTime := time.Now()

	// A wrong limit fails the run below, after the task status is
	// recorded.
	blockTimeout, runTimeout, limitsErr := t.timeLimits()
	t.blockTimeout = blockTimeout

	parentCtx := t.Ctx
	runCtx, stopRun := newRunContext(parentCtx, runTimeout)
	t.Ctx = runCtx
	t.stopRun = stopRun
	defer func() {
		stopRun(nil)
		t.stopRun = nil
		t.Ctx = parentCtx
	}()

	if t.Mode == "rerun" {
		t.Task.TaskType = taskstore.TaskTypeTableRerun
	} else if t.Task.TaskType == "" {
		t.Task.TaskType = taskstore.TaskTypeTableDiff
	}

	if strings.TrimSpace(t.TaskID) == "" {
		t.TaskID = uuid.NewString()
	}
	t.Task.StartedAt = startTime
	t.Task.TaskStatus = taskstore.StatusRunning
	t.Task.ClusterName = t.ClusterName

	t.diffRows.Store(0)
	t.diffLimitHit.Store(false)
	t.DiffResultDir = ""
	t.errorRecorded.Store(false)
	t.firstErrorMu.Lock()
	t.firstError = nil
	t.firstErrorMu.Unlock()

	var recorder *taskstore.Recorder

	if !t.SkipDBUpdate {
		rec, recErr := taskstore.NewRecorder(t.TaskStore, t.TaskStorePath)
		if recErr != nil {
			logger.Warn("table-diff: unable to initialise task store (%v)", recErr)
		} else {
			recorder = rec
			if t.TaskStore == nil && rec.Store() != nil {
				t.TaskStore = rec.Store()
			}

			ctx := map[string]any{
				"qualified_table": t.QualifiedTableName,
				"mode":            t.Mode,
				"nodes":           t.Nodes,
			}
			if t.TableFilter != "" {
				ctx["table_filter"] = t.TableFilter
			}

			record := taskstore.Record{
				TaskID:      t.TaskID,
				TaskType:    taskstore.TaskTypeTableDiff,
				Status:      taskstore.StatusRunning,
				ClusterName: t.ClusterName,
				SchemaName:  t.Schema,
				TableName:   t.Table,
				StartedAt:   startTime,
				TaskContext: ctx,
			}

			if err := recorder.Create(record); err != nil {
				logger.Warn("table-diff: unable to write initial task status (%v)", err)
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
				"diff_summary": t.DiffResult.Summary,
			}
			if n := t.DiffResult.Summary.DiffPairs(); n > 0 {
				ctx["diff_pairs"] = n
			}
			if t.DiffResultDir != "" {
				ctx["diff_result_dir"] = t.DiffResultDir
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
				logger.Warn("table-diff: unable to update task status (%v)", updateErr)
			}
		}

		if recorder != nil && recorder.OwnsStore() {
			storePtr := recorder.Store()
			if closeErr := recorder.Close(); closeErr != nil {
				logger.Warn("table-diff: failed to close task store (%v)", closeErr)
			}
			if storePtr != nil && t.TaskStore == storePtr {
				t.TaskStore = nil
			}
		}
	}()

	// When the run stops early, the errors of the workers only say that a
	// query was cancelled. Report the reason of the stop instead: the first
	// error, a time limit or a signal.
	defer func() {
		if err == nil {
			return
		}
		if cause := stopCause(runCtx); cause != nil && cause != err {
			logger.Debug("table-diff: stopped with error: %v", err)
			err = cause
		}
	}()

	if limitsErr != nil {
		return limitsErr
	}
	logger.Debug("table-diff: block timeout %s, run timeout %s (0s means no limit)", blockTimeout, runTimeout)

	if t.Mode == "rerun" {
		return t.ExecuteRerunTask()
	}
	t.DiffFilePath = ""

	if cfg := config.Get(); t.CompareUnitSize > 0 || (cfg != nil && cfg.TableDiff.CompareUnitSize > 0) {
		warnCompareUnitSize()
	}

	ctx := t.Ctx
	if ctx == nil {
		ctx = context.Background()
	}

	nodeNames := make([]string, 0, len(t.ClusterNodes))
	for _, nodeInfo := range t.ClusterNodes {
		nodeNames = append(nodeNames, nodeInfo["Name"].(string))
	}
	sort.Strings(nodeNames)
	limits := t.nodeLimits(nodeNames)

	pools := make(map[string]*pgxpool.Pool)
	for _, nodeInfo := range t.ClusterNodes {
		name := nodeInfo["Name"].(string)
		pool, err := auth.GetClusterNodeConnection(t.Ctx, nodeInfo, limits[name].poolOpts(t.maxPoolSize()))
		if err != nil {
			return fmt.Errorf("failed to connect to node %s: %w", name, err)
		}
		pools[name] = pool
		defer pool.Close()
	}
	t.Pools = pools

	// Open the connections of the task before any other work, and keep
	// them until the task ends.
	if err := t.openNodeConns(ctx, pools, nodeNames, limits); err != nil {
		return err
	}
	compareTasks := t.compareTasks(limits)
	t.diffSem = make(chan struct{}, compareTasks)
	logger.Info("Using %d CPUs, up to %d comparison tasks at the same time", runtime.NumCPU(), compareTasks)

	if err := t.loadNodeOriginNames(); err != nil {
		logger.Warn("table-diff: unable to load node origin names; using raw node_origin values: %v", err)
	}

	if err := t.resolveAgainstOrigin(); err != nil {
		return err
	}
	effectiveFilter, err := t.buildEffectiveFilter()
	if err != nil {
		return err
	}
	t.EffectiveFilter = effectiveFilter
	t.DiffSummary["effective_filter"] = effectiveFilter

	if _, err = t.getBlockHashSQL(true, true); err != nil {
		return fmt.Errorf("failed to build block-hash SQL: %w", err)
	}

	var maxCount int64
	var maxNode string

	for name, pool := range pools {
		count, err := t.estimateRowCount(pool, name)
		if err != nil {
			return fmt.Errorf("failed to estimate row count for %s on node %s: %w", t.QualifiedTableName, name, err)
		}

		logger.Debug("Table contains %d rows (estimated) on %s", count, name)
		if count > maxCount {
			maxCount = count
			maxNode = name
		}
	}
	if maxNode == "" {
		return fmt.Errorf("unable to determine node with highest row count (or any row counts)")
	}
	if err := t.ensureFilterHasRows(pools[maxNode], maxNode); err != nil {
		return err
	}

	t.DiffResult = types.DiffOutput{
		FormatVersion: types.DiffFormatVersion,
		Summary: types.DiffSummary{
			Schema:            t.Schema,
			Table:             t.BaseTable,
			TableFilter:       t.TableFilter,
			EffectiveFilter:   t.EffectiveFilter,
			Nodes:             t.NodeList,
			BlockSize:         t.BlockSize,
			CompareUnitSize:   t.CompareUnitSize,
			ConcurrencyFactor: t.ConcurrencyFactor,
			MaxDiffRows:       t.MaxDiffRows,
			StartTime:         startTime.Format(time.RFC3339),
			DiffRowsCount:     make(map[string]int),
			AgainstOrigin:     t.AgainstOrigin,
			AgainstOriginResolved: func() string {
				if t.resolvedAgainstOrigin != "" {
					flat := t.flatNodeOriginNames()
					if name, ok := flat[t.resolvedAgainstOrigin]; ok {
						return name
					}
				}
				return ""
			}(),
			Until: func() string {
				if t.untilTime != nil {
					return t.untilTime.Format(time.RFC3339)
				}
				return strings.TrimSpace(t.Until)
			}(),
			OriginOnly: t.resolvedAgainstOrigin != "",
		},
	}

	// The differing rows go to disk as soon as they are found. A run that
	// fails or stops leaves nothing behind.
	t.result = utils.NewDiffResultWriter(t.Schema, t.BaseTable, t.Key)
	defer func() {
		if err != nil {
			t.result.Abort()
		}
	}()

	// Block boundaries come from the anchor node (the highest row estimate).
	// The slicer cuts its key space into blocks of at most BlockSize rows
	// and sends them while the cut is still running, so hashing starts at
	// once.
	anchorPool := pools[maxNode]
	cutWorkers := slicer.CutWorkers(limits[maxNode].opened)
	sliceCfg := slicer.Config{
		Schema:    t.Schema,
		Table:     t.Table,
		Key:       t.Key,
		Filter:    t.EffectiveFilter,
		BlockSize: t.BlockSize,
		Workers:   cutWorkers,
	}
	plan, err := slicer.PlanParts(ctx, anchorPool, sliceCfg)
	if err != nil {
		return fmt.Errorf("failed to plan block boundaries on %s: %w", maxNode, err)
	}
	logger.Info("table-diff: block boundaries: %s", slicer.Describe(plan, sliceCfg, maxNode))

	// Every worker takes its connection now and keeps it until the end of
	// hashing. The pools already hold these connections idle, so no worker
	// waits for a connection and none is opened.
	held := make(map[string][]*pgxpool.Conn, len(nodeNames))
	for _, name := range nodeNames {
		conns, err := auth.AcquireConns(ctx, pools[name], limits[name].opened)
		if err != nil {
			for _, c := range held {
				auth.ReleaseConns(c)
			}
			return fmt.Errorf("failed to get the connections to node %s for hashing: %w", name, err)
		}
		held[name] = conns
	}
	// On the anchor, the first connections go to the cutting workers.
	cutConns := held[maxNode][:cutWorkers]
	held[maxNode] = held[maxNode][cutWorkers:]

	p := mpb.New(mpb.WithOutput(os.Stderr))
	// The number of blocks is known only when the cut ends, so the bar
	// starts with no total and grows.
	bar := p.AddBar(0,
		mpb.BarRemoveOnComplete(),
		mpb.PrependDecorators(
			decor.Name("Hashing initial ranges: ", decor.WC{W: 18}),
			decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
		),
		mpb.AppendDecorators(
			decor.Elapsed(decor.ET_STYLE_GO),
		),
	)

	/*
		We use the following approach:
		1. The slicer cuts the table into blocks on the anchor node. Every
		   block goes to one list as soon as it is cut; the cut never waits
		   for the hashing.
		2. Every node has its own hash workers and its own position in the
		   list, so a slow node does not hold back a fast one. Every worker
		   uses its own connection. On the anchor, a cutting worker that
		   has finished becomes one more hash worker on its connection.
		3. We don't immediately perform the comparisons, but instead store the results in a map.
		4. Once they're ready, each block whose hashes differ on two nodes is
		   compared row by row through a map of keys and row hashes.
	*/
	cutCtx, cancelCut := context.WithCancel(ctx)
	defer cancelCut()

	list := slicer.NewBlockList()
	hashers := newBlockHashers(ctx, t, list, bar, cancelCut)
	for _, name := range nodeNames {
		for _, c := range held[name] {
			hashers.addConn(name, c)
		}
	}
	logger.Info("table-diff: %d cutting workers and %d hash workers on %s", cutWorkers, len(held[maxNode]), maxNode)

	blockCh := make(chan slicer.Block, 4*cutWorkers)
	var (
		cutStats *slicer.Stats
		cutErr   error
	)
	go func() {
		defer close(blockCh)
		cutStats, cutErr = slicer.Cut(cutCtx, slicer.Queriers(cutConns), sliceCfg, plan, blockCh,
			func(i int, err error) {
				if err != nil {
					cutConns[i].Release()
					return
				}
				hashers.addConn(maxNode, cutConns[i])
			})
	}()

	// The loop only moves the blocks to the list, so the cut never waits.
	for b := range blockCh {
		if t.hasError() {
			// A hash failed and the diff will stop with that error. Stop
			// cutting too, but keep reading so the cut can exit.
			cancelCut()
			continue
		}
		idx := list.Add(b)
		if idx < 0 {
			// A hash worker has stopped the list after an error.
			continue
		}
		bar.SetTotal(int64((idx+1)*len(nodeNames)), false)
	}
	if cutErr != nil {
		list.Stop()
	} else {
		list.Close()
	}
	resultsMap := hashers.wait()
	bar.SetTotal(-1, true)

	// blocks are in arrival order, and the hash results are keyed by the
	// index of the block in it. The comparison below walks them in key
	// order with slicer.KeyOrder.
	blocks := list.Blocks()

	if err := t.getFirstError(); err != nil {
		return err
	}
	if cutErr != nil {
		return fmt.Errorf("failed to cut block boundaries on %s: %w", maxNode, cutErr)
	}

	logger.Info("table-diff: cut %s on %s", cutStats, maxNode)
	// The cut counted the rows of the anchor node that match the filter.
	// This is exact, unlike the planner estimate that chose the anchor.
	t.DiffResult.Summary.TotalRowsChecked = cutStats.Rows
	t.DiffResult.Summary.InitialRangesCount = len(blocks)

	logger.Info("Initial hash calculations complete. Proceeding with comparisons for mismatches...")

	var mismatchedTasks []mismatchTask

	for _, rangeIdx := range slicer.KeyOrder(blocks) {
		block := blocks[rangeIdx]
		currentRange := t.rangeFromBlock(block)
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
					logger.Debug("%s Mismatch in initial range %d (%v-%v) for %s vs %s. Hashes: %s... / %s...",
						utils.CrossMark, rangeIdx, currentRange.Start, currentRange.End, node1, node2, utils.SafeCut(r1.hash, 8), utils.SafeCut(r2.hash, 8))
					mismatchedTasks = append(mismatchedTasks, mismatchTask{
						node1: node1,
						node2: node2,
						r:     currentRange,
						part:  block.Part,
						seq:   block.Seq,
						rows1: r1.rows,
						rows2: r2.rows,
					})

				} else {
					logger.Debug("%s Match in initial range %d (%v-%v) for %s vs %s", utils.CheckMark, rangeIdx, currentRange.Start, currentRange.End, node1, node2)
				}
			}
		}
	}

	if len(mismatchedTasks) > 0 {
		logger.Debug("Found %d initial mismatched ranges. Comparing their rows.", len(mismatchedTasks))
		t.prepareKeyCompare(ctx, nodeNames)

		diffBar := p.AddBar(int64(len(mismatchedTasks)),
			mpb.PrependDecorators(
				decor.Name("Analysing mismatches: ", decor.WC{W: 18}),
				decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(
				decor.Elapsed(decor.ET_STYLE_GO),
				decor.Name(" | "),
				decor.OnComplete(decor.AverageETA(decor.ET_STYLE_GO), "done"),
			),
		)
		t.runCompares(ctx, mismatchedTasks, diffBar)
	}

	p.Wait()

	if t.diffLimitReached() {
		t.DiffResult.Summary.DiffRowLimitReached = true
	}

	if err := t.runError(ctx); err != nil {
		return err
	}

	logger.Info("Table diff comparison completed for %s", t.QualifiedTableName)
	if t.DiffResult.Summary.DiffRowLimitReached {
		logger.Warn("table-diff stopped after reaching max_diff_rows=%d; additional differences may exist", t.MaxDiffRows)
	}

	endTime := time.Now()
	t.DiffResult.Summary.EndTime = endTime.Format(time.RFC3339)
	t.DiffResult.Summary.TimeTaken = endTime.Sub(startTime).String()

	t.AddPrimaryKeyToDiffSummary()

	paths, err := t.result.Finish(ctx, t.resultManifest(), utils.DiffReportOptions{
		Format:             t.Output,
		MaxHTMLRows:        t.MaxHTMLRows,
		MaxHTMLValueLength: t.MaxHTMLValueLength,
	})
	if paths.JSON != "" {
		t.DiffFilePath = paths.JSON
		t.DiffResultDir = paths.Dir
	}
	return err
}

// resultManifest returns the description of the result of the run. The
// result writer adds the pairs.
func (t *TableDiffTask) resultManifest() utils.DiffResultManifest {
	colTypes := t.hashColTypes()
	cols := make([]utils.DiffResultColumn, 0, len(t.Cols))
	for _, c := range t.Cols {
		cols = append(cols, utils.DiffResultColumn{Name: c, Type: colTypes[c]})
	}
	return utils.DiffResultManifest{
		HashVersion: queries.CurrentHashVersion,
		Schema:      t.Schema,
		Table:       t.BaseTable,
		PrimaryKey:  t.Key,
		Columns:     cols,
		Nodes:       t.NodeList,
		Summary:     t.DiffResult.Summary,
	}
}

// runCompares compares each mismatched block in its own goroutine and waits
// until all of them end. Only as many goroutines as the diff semaphore
// allows exist at the same time. When the run stops, the blocks that wait
// for a slot are not compared and no error is recorded for them.
func (t *TableDiffTask) runCompares(ctx context.Context, tasks []mismatchTask, bar *mpb.Bar) {
	var wg sync.WaitGroup
	for i, task := range tasks {
		// After an error or at max_diff_rows, the blocks left are not
		// compared.
		if t.hasError() || t.diffLimitReached() {
			bar.IncrBy(len(tasks) - i)
			break
		}
		// Take a slot before the goroutine starts, so that at most as many
		// goroutines as there are slots exist at a time, however many
		// blocks mismatch. If the run is cancelled, the blocks left are
		// counted as done so that the progress bar can end. The first check
		// is needed because select takes a free slot or the end of the run
		// at random when both are ready.
		stopped := ctx.Err() != nil
		if !stopped {
			select {
			case t.diffSem <- struct{}{}:
			case <-ctx.Done():
				stopped = true
			}
		}
		if stopped {
			bar.IncrBy(len(tasks) - i)
			wg.Wait()
			return
		}
		// Waiting for the slot can take long; check again.
		if t.hasError() || t.diffLimitReached() {
			<-t.diffSem
			bar.IncrBy(len(tasks) - i)
			break
		}
		wg.Add(1)
		go func(task mismatchTask) {
			defer wg.Done()
			defer bar.Increment()
			defer func() { <-t.diffSem }()
			t.compareMismatch(ctx, task)
		}(task)
	}
	wg.Wait()
}

// runError returns the first error of the run. If there is none but the run
// was stopped, for example by the run timeout or a signal, it returns the
// cause of the stop. A stop can end the diff workers without an error, and
// then some ranges are not compared, so the result is not complete.
func (t *TableDiffTask) runError(ctx context.Context) error {
	if err := t.getFirstError(); err != nil {
		return err
	}
	return stopCause(ctx)
}

// rowQuerier is the part of pgxpool.Conn that a hash query needs.
type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// hashRangeOn runs the block hash query (queries.BlockHashSQL) for the range
// r of node on q, a connection to that node. It returns the hash as hex text
// and the number of rows in the range. A bound of r that is nil, or all NULL
// for a composite key, leaves the range open on that side.
func (t *TableDiffTask) hashRangeOn(
	ctx context.Context,
	q rowQuerier,
	node string,
	r Range,
) (string, int64, error) {
	startTime := time.Now()
	var (
		hash string
		rows int64
	)

	numPKCols := len(t.Key)

	startVals, hasLower, err := extractRangeBoundValues(r.Start, numPKCols)
	if err != nil {
		return "", 0, err
	}
	endVals, hasUpper, err := extractRangeBoundValues(r.End, numPKCols)
	if err != nil {
		return "", 0, err
	}

	query, err := t.getBlockHashSQL(hasLower, hasUpper)
	if err != nil {
		return "", 0, fmt.Errorf("failed to build block-hash SQL: %w", err)
	}

	args := make([]any, 0, len(startVals)+len(endVals))
	if hasLower {
		args = append(args, startVals...)
	}
	if hasUpper {
		args = append(args, endVals...)
	}

	logger.Debug("[%s] Hashing range: Start=%v, End=%v. SQL: %s, Args: %v", node, r.Start, r.End, query, args)

	err = q.QueryRow(ctx, query, args...).Scan(&hash, &rows)

	if err != nil {
		duration := time.Since(startTime)
		logger.Debug("[%s] ERROR after %v for range Start=%v, End=%v (using query: '%s', args: %v): %v", node, duration, r.Start, r.End, query, args, err)
		return "", 0, fmt.Errorf("BlockHash query failed for %s range %v-%v: %w", node, r.Start, r.End, err)
	}

	duration := time.Since(startTime)
	if duration > 10000*time.Millisecond {
		logger.Debug("[%s] Slow query? Range Start=%v, End=%v took %v", node, r.Start, r.End, duration)
	} else {
		logger.Debug("[%s] Range Start=%v, End=%v took %v", node, r.Start, r.End, duration)
	}
	return hash, rows, nil
}

func extractRangeBoundValues(bound any, numPKCols int) ([]any, bool, error) {
	if bound == nil {
		return nil, false, nil
	}
	if numPKCols == 1 {
		return []any{bound}, true, nil
	}

	processSlice := func(vals []any) ([]any, bool, error) {
		if len(vals) != numPKCols {
			return nil, false, fmt.Errorf("range bound expected %d values, got %d", numPKCols, len(vals))
		}
		if queries.AllNil(vals) {
			return nil, false, nil
		}
		return vals, true, nil
	}

	if vals, ok := bound.([]any); ok {
		return processSlice(vals)
	}
	if valsIface, ok := bound.([]interface{}); ok {
		vals := make([]any, len(valsIface))
		copy(vals, valsIface)
		return processSlice(vals)
	}

	return nil, false, fmt.Errorf("unsupported range bound type %T", bound)
}

// rangeFromBlock converts a slicer block into a Range: a simple key uses the
// value itself, a composite key uses a []any of all key values. nil means no
// bound.
func (t *TableDiffTask) rangeFromBlock(b slicer.Block) Range {
	conv := func(vals []any) any {
		if vals == nil {
			return nil
		}
		if len(t.Key) == 1 {
			return vals[0]
		}
		return vals
	}
	return Range{Start: conv(b.Start), End: conv(b.End)}
}

var compareUnitSizeWarning sync.Once

// warnCompareUnitSize says, once per process, that compare_unit_size is
// ignored.
func warnCompareUnitSize() {
	compareUnitSizeWarning.Do(func() {
		logger.Warn("compare_unit_size (--compare-unit-size) is no longer used and is ignored; " +
			"remove it from the command, the job or the settings file")
	})
}

// mismatchTask is one block whose hashes differ on two nodes. part and seq
// are the numbers of the block from the cut: its part and its position in
// the part.
type mismatchTask struct {
	node1, node2 string
	r            Range
	part, seq    int
	rows1, rows2 int64
}

// DefaultKeyMapMaxMemoryMB is the default limit, in megabytes, of the memory
// for the keys of one mismatched block.
const DefaultKeyMapMaxMemoryMB = 512

const keyMapMaxMemorySetting = "table_diff.key_map_max_memory_mb"

// keyMapMaxMemory returns the limit, in bytes, of the memory for the keys of
// one mismatched block. 0 means no limit, as does a setting of 0 or less.
func (t *TableDiffTask) keyMapMaxMemory() int64 {
	if t.KeyMapMaxMemory < 0 {
		return 0
	}
	if t.KeyMapMaxMemory > 0 {
		return t.KeyMapMaxMemory
	}
	mb := DefaultKeyMapMaxMemoryMB
	if cfg := config.Get(); cfg != nil && cfg.TableDiff.KeyMapMaxMemoryMB != nil {
		mb = *cfg.TableDiff.KeyMapMaxMemoryMB
	}
	if mb <= 0 {
		return 0
	}
	return int64(mb) << 20
}

// prepareKeyCompare reads, once and before the mismatched blocks are
// compared, what every comparison needs from the catalog: the average key
// width on each node (only with a memory limit) and whether the table has
// inheritance children on any node. The row read of all nodes uses one
// plan, and the plan for a table without children reads a child once for
// every key, so children on one node are enough to choose the plan for
// children. A failure here is not an error: the width is then unknown, and
// the children are assumed to exist, which is safe.
func (t *TableDiffTask) prepareKeyCompare(ctx context.Context, nodeNames []string) {
	t.keyWidths = make(map[string]int64, len(nodeNames))
	if t.keyMapMaxMemory() > 0 {
		for _, node := range nodeNames {
			w, err := keydiff.KeyWidth(ctx, t.Pools[node], t.Schema, t.Table, t.Key)
			if err != nil {
				logger.Debug("[%s] no key statistics for %s: %v", node, t.QualifiedTableName, err)
				w = 0
			}
			t.keyWidths[node] = w
		}
	}
	t.scanChildren = anyNode(nodeNames, func(node string) bool {
		has, err := keydiff.HasInheritanceChildren(ctx, t.Pools[node], t.Schema, t.Table)
		if err != nil {
			logger.Debug("[%s] cannot check the inheritance children of %s: %v", node, t.QualifiedTableName, err)
			return true
		}
		return has
	})
}

// anyNode reports whether f is true for at least one of nodes. It stops at
// the first such node. With no nodes it returns true, the safe answer for a
// check that cannot be made.
func anyNode(nodes []string, f func(node string) bool) bool {
	if len(nodes) == 0 {
		return true
	}
	for _, node := range nodes {
		if f(node) {
			return true
		}
	}
	return false
}

// compareMismatch finds the rows that differ on two nodes in one block whose
// hashes do not match, and adds them to the diff result.
func (t *TableDiffTask) compareMismatch(ctx context.Context, task mismatchTask) {
	pairKey := pairKeyFor(task.node1, task.node2)

	fail := func(err error) {
		var me *keydiff.MemoryLimitError
		if errors.As(err, &me) {
			err = fmt.Errorf("%w; raise %s or use smaller blocks", err, keyMapMaxMemorySetting)
		}
		t.recordError(fmt.Errorf("compare rows of %s in range %v-%v on %s and %s: %w",
			t.QualifiedTableName, task.r.Start, task.r.End, task.node1, task.node2, err))
	}

	startVals, hasLower, err := extractRangeBoundValues(task.r.Start, len(t.Key))
	if err != nil {
		fail(err)
		return
	}
	endVals, hasUpper, err := extractRangeBoundValues(task.r.End, len(t.Key))
	if err != nil {
		fail(err)
		return
	}
	args := make([]any, 0, len(startVals)+len(endVals))
	if hasLower {
		args = append(args, startVals...)
	}
	if hasUpper {
		args = append(args, endVals...)
	}

	side := func(node string, rows int64) (keydiff.Side, error) {
		colTypes, err := t.nodeColTypes(node)
		if err != nil {
			return keydiff.Side{}, err
		}
		pool, ok := t.Pools[node]
		if !ok {
			return keydiff.Side{}, fmt.Errorf("no pool for node %s", node)
		}
		// Every query of the comparison is one operation on the block, so
		// the block timeout limits each of them.
		conn := &blockQuerier{t: t, pool: pool}
		return keydiff.Side{Name: node, Conn: conn, ColTypes: colTypes, Rows: rows, RowsKnown: true}, nil
	}
	s1, err := side(task.node1, task.rows1)
	if err != nil {
		fail(err)
		return
	}
	s2, err := side(task.node2, task.rows2)
	if err != nil {
		fail(err)
		return
	}
	s1.KeyWidth = t.keyWidths[task.node1]
	s2.KeyWidth = t.keyWidths[task.node2]

	tbl := keydiff.Table{Schema: t.Schema, Name: t.Table, Key: t.Key, Cols: t.Cols, HashTypes: t.hashColTypes(),
		ScanChildren: t.scanChildren}
	block := keydiff.Block{
		Where:      queries.BlockWhereClause(t.Key, hasLower, hasUpper, t.EffectiveFilter),
		Args:       args,
		ReadFilter: t.EffectiveFilter,
	}
	out := t.result.OpenBlock(pairKey, task.node1, task.node2, task.part, task.seq)
	res, err := keydiff.Compare(ctx, tbl, block, s1, s2, keydiff.Options{
		MaxMemory: t.keyMapMaxMemory(),
	}, func(entries []keydiff.Entry) error {
		return t.writeEntries(out, task.node1, task.node2, entries)
	})
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		fail(err)
		return
	}
	logger.Debug("[%s vs %s] range %v-%v: %d differing keys (map on %s), %d only on %s, %d only on %s, %d changed",
		task.node1, task.node2, task.r.Start, task.r.End, res.DiffKeys, res.BuildNode,
		res.Only1, task.node1, res.Only2, task.node2, res.Changed)
	if n := res.Count(); n > 0 {
		t.diffMutex.Lock()
		t.DiffResult.Summary.DiffRowsCount[pairKey] += n
		t.diffMutex.Unlock()
		t.countDiffRows(n)
	}
}

// writeEntries writes the differing rows of one batch to the block file,
// with the spock metadata of each node.
func (t *TableDiffTask) writeEntries(out *utils.DiffBlockWriter, node1, node2 string, entries []keydiff.Entry) error {
	conv := func(row types.OrderedMap, node string) types.OrderedMap {
		if row == nil {
			return nil
		}
		return utils.MapToOrderedMap(t.withSpockMetadata(utils.OrderedMapToMap(row), node), t.Cols)
	}
	for _, e := range entries {
		var err error
		switch e.Kind {
		case keydiff.KindOnly1:
			err = out.Add(utils.DiffKindNode1Only, conv(e.Row1, node1), nil, e.Hash1[:], nil)
		case keydiff.KindOnly2:
			err = out.Add(utils.DiffKindNode2Only, nil, conv(e.Row2, node2), nil, e.Hash2[:])
		default:
			err = out.Add(utils.DiffKindChanged, conv(e.Row1, node1), conv(e.Row2, node2), e.Hash1[:], e.Hash2[:])
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TableDiffTask) AddPrimaryKeyToDiffSummary() {
	if t.DiffResult.Summary.PrimaryKey == nil {
		t.DiffResult.Summary.PrimaryKey = t.Key
	}
}
