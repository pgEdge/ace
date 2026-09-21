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
	"errors"
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
	"github.com/pgedge/ace/internal/consistency/schema"
	"github.com/pgedge/ace/internal/consistency/scope"
	"github.com/pgedge/ace/internal/infra/db"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/taskstore"
	"github.com/pgedge/ace/pkg/types"
)

// CompareData and CompareStructure are the two values SchemaDiffCmd.Compare
// accepts. CompareData is the default, preserving schema-diff's existing
// per-table data-diff behaviour.
const (
	CompareData      = "data"
	CompareStructure = "structure"
)

type SchemaDiffCmd struct {
	types.Task

	ClusterName string
	DBName      string
	SchemaName  string
	Nodes       string
	Quiet       bool
	SkipTables  string
	SkipFile    string
	DDLOnly     bool
	// Compare selects what schema-diff compares: CompareData (the default,
	// per-table data diff) or CompareStructure (structural comparison via
	// internal/consistency/schema).
	Compare string

	skipTablesList []string
	// checksRun records that RunChecks already ran for this task, so
	// SchemaTableDiff does not repeat the scope-resolution query and its
	// logging.
	//
	// It memoises tableList/missingTables, which describe the cluster as it
	// was when RunChecks ran, so it is scoped to one execution: every entry
	// point that starts a run clears it first. Leaving it set across runs
	// would silently compare a stale table list — the schema-diff scheduler
	// hands the same command back for each fire.
	checksRun         bool
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
	// OutputExplicit records whether the user actually passed --output, as
	// opposed to it carrying the flag's own default. --compare=structure
	// needs this distinction because "json" is that default: without it,
	// every run - including one that never mentioned --output - would look
	// like a request for the JSON rendering, silently changing what an
	// interactive run prints. See schemaStructureDiff.
	OutputExplicit    bool
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

// resolveCompareMode settles c.Compare before anything else runs.
// --ddl-only keeps selecting the existing table/view/function/index
// name-only diff (schemaObjectDiff). --compare=structure requests the
// symmetric, per-table structural comparison from
// internal/consistency/schema. If both are given, --compare=structure
// wins as the more specific request.
func (c *SchemaDiffCmd) resolveCompareMode() error {
	if c.Compare == "" {
		c.Compare = CompareData
		return nil
	}
	if c.Compare != CompareData && c.Compare != CompareStructure {
		return fmt.Errorf("invalid --compare value %q: must be %q or %q", c.Compare, CompareData, CompareStructure)
	}
	return nil
}

func (c *SchemaDiffCmd) Validate() error {
	if c.ClusterName == "" {
		return fmt.Errorf("cluster name is required")
	}
	if c.SchemaName == "" {
		return fmt.Errorf("schema name is required")
	}
	if err := c.resolveCompareMode(); err != nil {
		return err
	}
	if c.Compare == CompareStructure && c.OutputExplicit && !strings.EqualFold(c.Output, "json") {
		return fmt.Errorf("--output=%s is not supported with --compare=structure: structure mode has no per-table diff files to render, only findings - use --output=json or omit --output", c.Output)
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

	cfg := config.Get()
	if c.MaxConnections == 0 && cfg != nil {
		c.MaxConnections = cfg.TableDiff.MaxConnections
	}
	if c.MaxConnections < 0 {
		return fmt.Errorf("max_connections must be >= 1 (or 0 to derive from concurrency factor)")
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
	quotedOf := make(map[string]string)               // raw identifier -> quote_ident() form

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

		pool, err := auth.GetClusterNodeConnection(c.Ctx, nodeWithDBInfo, auth.ConnectionOptions{PoolSize: c.MaxConnections})
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

		resolved, err := scope.SchemaProvider{SchemaName: c.SchemaName}.Resolve(c.Ctx, pool)
		if err != nil {
			pool.Close()
			return fmt.Errorf("could not resolve tables in schema on node %s: %w", nodeName, err)
		}
		tables := make([]string, 0, len(resolved.Tables))
		for _, qn := range resolved.Tables {
			tables = append(tables, qn.Table)
		}

		// Quote this node's own names while its connection is still open,
		// so a table reported as missing is spelled the way the structural
		// findings below spell one: schema."odd.name", not the ambiguous
		// schema.odd.name. Every node renders a given name identically, so
		// merging each node's answers is safe, and a name that cannot be
		// quoted is printed raw rather than failing the run.
		if quoted, qerr := queries.QuoteIdentifiers(c.Ctx, pool, append([]string{c.SchemaName}, tables...)); qerr == nil {
			for raw, q := range quoted {
				quotedOf[raw] = q
			}
		} else if !c.Quiet {
			logger.Info("could not quote identifiers for display on node %s (names will be printed unquoted): %v", nodeName, qerr)
		}

		foreign, ferr := queries.GetForeignTablesInSchema(c.Ctx, pool, c.SchemaName)
		if ferr != nil {
			pool.Close()
			return fmt.Errorf("could not list foreign tables in schema on node %s: %w", nodeName, ferr)
		}
		if len(foreign) > 0 {
			logger.Info("Skipping %d foreign table(s) in schema %s on node %s: %s", len(foreign), c.SchemaName, nodeName, strings.Join(foreign, ", "))
		}
		views, verr := queries.GetViewsInSchema(c.Ctx, pool, c.SchemaName)
		if verr != nil {
			pool.Close()
			return fmt.Errorf("could not list views in schema on node %s: %w", nodeName, verr)
		}
		if len(views) > 0 {
			logger.Info("Skipping %d view(s) in schema %s on node %s (views are compared as DDL only): %s", len(views), c.SchemaName, nodeName, strings.Join(views, ", "))
		}
		pool.Close()

		for _, t := range tables {
			if tablePresence[t] == nil {
				tablePresence[t] = make(map[string]bool)
			}
			tablePresence[t][nodeName] = true
		}
	}

	// For --compare=structure, a table named by --skip-tables/--skip-file
	// must be left out of missing-table reporting too, not only out of the
	// per-table comparison. Otherwise a table the user explicitly excluded
	// would still show up as "missing on some nodes" and still force
	// schema.ExitIncompatible in schemaStructureDiff. This check must run
	// here, before schema.Qualify turns the raw table name into its
	// quoted, schema-qualified display form: c.skipTablesList holds the
	// raw, unqualified form (see parseSkipList), and matching against the
	// display form would miss any name that needs quoting.
	//
	// compare=data keeps its old behavior: a table missing on some nodes is
	// still reported there even if --skip-tables named it, since that mode
	// only uses the skip list to leave a table out of the per-table data
	// diff (see the skip check around the tableList loop below), not to
	// hide the fact that its presence is asymmetric across nodes.
	skipForMissingReport := make(map[string]bool, len(c.skipTablesList))
	if c.Compare == CompareStructure {
		for _, t := range c.skipTablesList {
			skipForMissingReport[t] = true
		}
	}

	// Partition into common (all nodes) vs partial (some nodes).
	var commonTables []string
	var missingTables []MissingTableInfo
	for table, presence := range tablePresence {
		if len(presence) == len(nodeNames) {
			commonTables = append(commonTables, table)
		} else {
			if skipForMissingReport[table] {
				continue
			}
			var presentOn, missingFrom []string
			for _, n := range nodeNames {
				if presence[n] {
					presentOn = append(presentOn, n)
				} else {
					missingFrom = append(missingFrom, n)
				}
			}
			missingTables = append(missingTables, MissingTableInfo{
				Table:       schema.Qualify(quotedOf, c.SchemaName, table),
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

	// An empty schema is a finding for --compare=data but not for
	// --compare=structure: nodes agreeing on an empty schema should exit 0,
	// not the same code an unreachable node gets.
	if len(c.tableList) == 0 && len(c.missingTables) == 0 && c.Compare != CompareStructure {
		return fmt.Errorf("no tables found in schema %s", c.SchemaName)
	}

	c.checksRun = true
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

		pool, err := auth.GetClusterNodeConnection(task.Ctx, nodeWithDBInfo, auth.ConnectionOptions{PoolSize: task.MaxConnections})
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

// schemaStructureDiff is --compare=structure's implementation. It compares
// every common table's actual structure - columns, replica identity,
// constraints - using the same internal/consistency/schema.CollectSnapshot
// / Compare pair that table-diff's preflight uses to explain a mismatch,
// so both call sites share one comparison layer.
//
// Every pair of participating nodes is compared (up to three, per
// Validate's node-count limit): with three nodes, "A matches B" does not
// imply "B matches C".
//
// Tables present on only some nodes were already found by RunChecks
// (task.missingTables) and are reported separately here: CollectSnapshot
// has no way to be told a table does not exist on a node, so running it on
// one would surface every column as individually absent instead of one
// clear line naming the table.
//
// --skip-tables/--skip-file apply here exactly as they do to the default
// per-table data diff: a listed table is excluded from comparison (and from
// the exit code) entirely. task.tableList itself is left untouched -
// RunChecks built it once for the whole run - so this function derives its
// own filtered compareTables instead. A table missing on some nodes is
// excluded the same way: RunChecks already leaves a skipped table out of
// task.missingTables (for this mode only), so it does not appear in the
// report and does not force schema.ExitIncompatible.
//
// --output=json switches what is printed from the prose report to a
// StructureDiffReport, for a script that wants the findings structured
// rather than parsed out of text. This only happens when --output was
// actually given (task.OutputExplicit): "json" is that flag's own default
// value, so without this guard every run - including one that never
// mentioned --output - would silently switch its default console output.
type StructureDiffReport struct {
	Schema        string                      `json:"schema"`
	Nodes         []string                    `json:"nodes"`
	MissingTables []MissingTableInfo          `json:"missing_tables,omitempty"`
	Comparisons   []StructureComparisonReport `json:"comparisons"`
	// ExitCode is the same code the process itself exits with (one of
	// schema.ExitIdentical .. schema.ExitIncompatible), repeated here so a
	// script reading only stdout does not also have to inspect the process's
	// exit status.
	ExitCode int `json:"exit_code"`
}

// StructureComparisonReport is one node pair's findings within a
// StructureDiffReport. An empty Divergences means this pair's structure
// matched exactly, over whatever tables ended up in scope.
type StructureComparisonReport struct {
	NodeA       string              `json:"node_a"`
	NodeB       string              `json:"node_b"`
	Divergences []schema.Divergence `json:"divergences"`
}

// schemaStructureDiff runs the --compare=structure mode: it reads one
// structural snapshot per node, compares the nodes, prints what differs, and
// reports the worst rank it found through the process's exit code.
//
// Every node is compared with every other node, not with one chosen
// reference, because which node is right is not something this command can
// decide. Findings are counted per distinct object and property rather than
// per node pair, so one drifted column is one finding however many pairs saw
// it.
//
// Tables named by --skip-tables/--skip-file are dropped here, after
// RunChecks has already settled which tables every node has; missing tables
// are reported from what RunChecks recorded. The return is nil when nothing
// differs, and otherwise a utils.ExitCodeError carrying that worst rank's
// code, which is what turns a structural difference into an exit status a
// script can act on.
func (task *SchemaDiffCmd) schemaStructureDiff() error {
	type nodeConn struct {
		name string
		pool *pgxpool.Pool
	}

	var conns []nodeConn
	defer func() {
		for _, c := range conns {
			c.pool.Close()
		}
	}()

	for _, nodeInfo := range task.clusterNodes {
		nodeName, _ := nodeInfo["Name"].(string)
		if !utils.Contains(task.nodeList, nodeName) {
			continue
		}

		nodeWithDBInfo := make(map[string]any)
		maps.Copy(nodeWithDBInfo, nodeInfo)
		utils.ApplyDatabaseCredentials(nodeWithDBInfo, task.database)
		if portVal, ok := nodeWithDBInfo["Port"]; ok {
			if portFloat, isFloat := portVal.(float64); isFloat {
				nodeWithDBInfo["Port"] = strconv.Itoa(int(portFloat))
			}
		}

		pool, err := auth.GetClusterNodeConnection(task.Ctx, nodeWithDBInfo, auth.ConnectionOptions{PoolSize: task.MaxConnections})
		if err != nil {
			return fmt.Errorf("could not connect to node %s: %w", nodeName, err)
		}
		conns = append(conns, nodeConn{name: nodeName, pool: pool})
	}

	if len(conns) < 2 {
		return fmt.Errorf("schema-diff --compare=structure needs at least two reachable nodes")
	}

	compareTables := task.tableList
	if len(task.skipTablesList) > 0 {
		skip := make(map[string]bool, len(task.skipTablesList))
		for _, t := range task.skipTablesList {
			skip[t] = true
		}
		var skipped []string
		compareTables = make([]string, 0, len(task.tableList))
		for _, t := range task.tableList {
			if skip[t] {
				skipped = append(skipped, t)
				continue
			}
			compareTables = append(compareTables, t)
		}
		if len(skipped) > 0 && !task.Quiet {
			logger.Info("Skipping %d table(s) excluded by --skip-tables/--skip-file in schema %s: %s",
				len(skipped), task.SchemaName, strings.Join(skipped, ", "))
		}
	}

	snapshots := make(map[string]schema.Snapshot, len(conns))
	for _, c := range conns {
		snap, err := schema.CollectSnapshot(task.Ctx, c.pool, c.name, task.SchemaName, compareTables)
		if err != nil {
			return fmt.Errorf("collecting structure snapshot on node %s: %w", c.name, err)
		}
		snapshots[c.name] = snap
	}

	worst := schema.ExitIdentical
	// Findings are counted per distinct (object, property), not summed over
	// node pairs: with three nodes, one column that drifted on node 3 is
	// found twice, by n1-vs-n3 and n2-vs-n3, and reporting "2 divergences"
	// for one drifted column reads as two problems.
	distinctFindings := make(map[string]bool)
	var report strings.Builder
	comparisons := make([]StructureComparisonReport, 0, len(conns)*(len(conns)-1)/2)

	// Tables missing from some nodes are reported first, since a schema
	// present on only one node should not open with "(no structural
	// differences)".
	if len(task.missingTables) > 0 {
		report.WriteString("=== tables missing on some nodes ===\n")
		for _, mt := range task.missingTables {
			fmt.Fprintf(&report, "  - %s: present on %s, missing from %s\n",
				mt.Table, strings.Join(mt.PresentOn, ", "), strings.Join(mt.MissingFrom, ", "))
		}
		if schema.ExitIncompatible > worst {
			worst = schema.ExitIncompatible
		}
	}

	for i := 0; i < len(conns); i++ {
		for j := i + 1; j < len(conns); j++ {
			a, b := conns[i].name, conns[j].name
			divs := schema.Compare(task.SchemaName, compareTables, snapshots[a], snapshots[b])
			comparisons = append(comparisons, StructureComparisonReport{NodeA: a, NodeB: b, Divergences: divs})
			for _, d := range divs {
				// FindingKey, not a key built here: a table's constraint
				// findings share one Object+Kind+Property and would
				// otherwise collapse into a single count.
				distinctFindings[d.FindingKey()] = true
			}
			if code := schema.WorstExitCode(divs); code > worst {
				worst = code
			}

			fmt.Fprintf(&report, "=== %s vs %s ===\n", a, b)
			switch {
			case len(compareTables) == 0 && len(task.tableList) > 0:
				// Every common table was named by --skip-tables/--skip-file.
				report.WriteString("  (every common table was excluded by --skip-tables/--skip-file)\n")
			case len(compareTables) == 0 && len(task.missingTables) == 0:
				// The schema exists on both nodes and is empty on both. That
				// is agreement, not a failure to compare, and it exits 0.
				report.WriteString("  (schema is empty on every node)\n")
			case len(compareTables) == 0:
				// No common tables exist to compare.
				report.WriteString("  (no tables in common to compare)\n")
			case len(divs) == 0:
				fmt.Fprintf(&report, "  (no structural differences across %d table(s))\n", len(compareTables))
			default:
				report.WriteString(schema.FormatDivergences(divs))
				report.WriteString("\n")
			}
		}
	}

	if task.OutputExplicit && strings.EqualFold(task.Output, "json") {
		nodeNames := make([]string, len(conns))
		for i, c := range conns {
			nodeNames[i] = c.name
		}
		encoded, err := json.MarshalIndent(StructureDiffReport{
			Schema:        task.SchemaName,
			Nodes:         nodeNames,
			MissingTables: task.missingTables,
			Comparisons:   comparisons,
			ExitCode:      worst,
		}, "", "  ")
		if err != nil {
			return fmt.Errorf("could not marshal structure diff report to json: %w", err)
		}
		fmt.Println(string(encoded))
	} else {
		fmt.Print(report.String())
	}

	if worst == schema.ExitIdentical {
		if !task.Quiet {
			logger.Info("schema structure diff: schema %s is identical across %d node(s)", task.SchemaName, len(conns))
		}
		return nil
	}

	summary := fmt.Sprintf("schema structure diff: %d divergence(s) found in schema %s across %d compared table(s)",
		len(distinctFindings), task.SchemaName, len(compareTables))
	switch {
	case len(compareTables) == 0 && len(task.tableList) > 0:
		summary = fmt.Sprintf("schema structure diff: every common table in schema %s was excluded by --skip-tables/--skip-file, so nothing could be compared",
			task.SchemaName)
	case len(compareTables) == 0:
		summary = fmt.Sprintf("schema structure diff: no table in schema %s exists on every node, so nothing could be compared",
			task.SchemaName)
	}
	if len(task.missingTables) > 0 {
		summary += fmt.Sprintf("; %d table(s) missing on some node(s)", len(task.missingTables))
	}

	return &utils.ExitCodeError{Code: worst, Err: errors.New(summary)}
}

func (task *SchemaDiffCmd) SchemaTableDiff() (err error) {
	// The caller may already have run the checks - internal/cli does, to
	// validate before committing to a run - so they are not repeated here.
	// Whoever ran them, the resolved scope belongs to this run only, and is
	// released with it: see checksRun.
	if !task.checksRun {
		if err := task.RunChecks(false); err != nil {
			return err
		}
	}
	defer func() { task.checksRun = false }()

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
				"compare":      task.Compare,
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

		// A found difference is a result, not a failure: --compare=structure
		// reports it via ExitCodeError so the task store can distinguish
		// "the schemas differ" from "the run broke".
		var divergence *utils.ExitCodeError
		status := taskstore.StatusFailed
		switch {
		case err == nil, errors.As(err, &divergence):
			status = taskstore.StatusCompleted
		}
		task.Task.TaskStatus = status

		if recorder != nil && recorder.Created() {
			ctx := map[string]any{
				"tables_total":   len(task.tableList),
				"tables_diffed":  tablesProcessed,
				"tables_failed":  tablesFailed,
				"tables_skipped": len(skippedTables),
				"compare":        task.Compare,
				"ddl_only":       task.DDLOnly,
			}
			if len(failedTables) > 0 {
				names := make([]string, len(failedTables))
				for i, ft := range failedTables {
					names[i] = ft.Table
				}
				ctx["failed_tables"] = names
			}
			switch {
			case divergence != nil:
				// Keyed apart from "error" so a reader can tell "the
				// schemas differ" from "the run broke".
				ctx["divergence"] = divergence.Error()
				ctx["exit_code"] = divergence.Code
			case err != nil:
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

	if task.Compare == CompareStructure {
		return task.schemaStructureDiff()
	}

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
	clone.Compare = task.Compare
	clone.BlockSize = task.BlockSize
	clone.ConcurrencyFactor = task.ConcurrencyFactor
	clone.MaxConnections = task.MaxConnections
	clone.CompareUnitSize = task.CompareUnitSize
	clone.Output = task.Output
	clone.OutputExplicit = task.OutputExplicit
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
