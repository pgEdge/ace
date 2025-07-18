/////////////////////////////////////////////////////////////////////////////
//
// ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the pgEdge Community License:
//      https://www.pgedge.com/communitylicense
//
/////////////////////////////////////////////////////////////////////////////

package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"sort"
	"strconv"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/types"
)

type SchemaDiffCmd struct {
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

func (c *SchemaDiffCmd) getTablesInSchema(db *pgxpool.Pool) error {
	rows, err := db.Query(context.Background(),
		"SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
		c.SchemaName)
	if err != nil {
		return fmt.Errorf("could not query tables in schema: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("could not scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	c.tableList = tables
	return nil
}

func (c *SchemaDiffCmd) RunChecks(skipValidation bool) error {
	if !skipValidation {
		if err := c.Validate(); err != nil {
			return err
		}
	}

	if err := readClusterInfo(c); err != nil {
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

	pool, err := auth.GetClusterNodeConnection(nodeWithDBInfo, "")
	if err != nil {
		return fmt.Errorf("could not connect to database: %w", err)
	}
	defer pool.Close()

	db := queries.NewQuerier(pool)

	schemaExists, err := db.CheckSchemaExists(context.Background(), pgtype.Name{String: c.SchemaName, Status: pgtype.Present})
	if err != nil {
		return fmt.Errorf("could not check if schema exists: %w", err)
	}
	if !schemaExists {
		return fmt.Errorf("schema %s not found", c.SchemaName)
	}

	tables, err := db.GetTablesInSchema(context.Background(), pgtype.Name{String: c.SchemaName, Status: pgtype.Present})
	if err != nil {
		return fmt.Errorf("could not get tables in schema: %w", err)
	}

	c.tableList = []string{}
	for _, table := range tables {
		c.tableList = append(c.tableList, table.String)
	}

	if len(c.tableList) == 0 {
		return fmt.Errorf("no tables found in schema %s", c.SchemaName)
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
	return nil
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

func diffStringSlices(a, b []string) (missing, extra []string) {
	sort.Strings(a)
	sort.Strings(b)

	aMap := make(map[string]struct{}, len(a))
	for _, s := range a {
		aMap[s] = struct{}{}
	}

	bMap := make(map[string]struct{}, len(b))
	for _, s := range b {
		bMap[s] = struct{}{}
	}

	for _, s := range a {
		if _, found := bMap[s]; !found {
			missing = append(missing, s)
		}
	}

	for _, s := range b {
		if _, found := aMap[s]; !found {
			extra = append(extra, s)
		}
	}

	return missing, extra
}

func getObjectsForSchema(db queries.Querier, schemaName string) (*SchemaObjects, error) {
	pgSchemaName := pgtype.Name{String: schemaName, Status: pgtype.Present}

	tables, err := db.GetTablesInSchema(context.Background(), pgSchemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query tables: %w", err)
	}
	var tableNames []string
	for _, t := range tables {
		tableNames = append(tableNames, t.String)
	}

	views, err := db.GetViewsInSchema(context.Background(), pgSchemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query views: %w", err)
	}
	var viewNames []string
	for _, v := range views {
		viewNames = append(viewNames, v.String)
	}

	functions, err := db.GetFunctionsInSchema(context.Background(), pgSchemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query functions: %w", err)
	}
	var functionSignatures []string
	for _, f := range functions {
		functionSignatures = append(functionSignatures, *f)
	}

	indices, err := db.GetIndicesInSchema(context.Background(), pgSchemaName)
	if err != nil {
		return nil, fmt.Errorf("could not query indices: %w", err)
	}
	var indexNames []string
	for _, i := range indices {
		indexNames = append(indexNames, i.String)
	}

	return &SchemaObjects{
		Tables:    tableNames,
		Views:     viewNames,
		Functions: functionSignatures,
		Indices:   indexNames,
	}, nil
}

func schemaDDLDiff(task *SchemaDiffCmd) error {
	var allNodeObjects []NodeSchemaReport

	for _, nodeInfo := range task.clusterNodes {
		nodeName := nodeInfo["Name"].(string)
		nodeWithDBInfo := make(map[string]any)
		maps.Copy(nodeWithDBInfo, nodeInfo)
		nodeWithDBInfo["DBName"] = task.database.DBName
		nodeWithDBInfo["DBUser"] = task.database.DBUser
		nodeWithDBInfo["DBPassword"] = task.database.DBPassword
		if portVal, ok := nodeWithDBInfo["Port"]; ok {
			if portFloat, isFloat := portVal.(float64); isFloat {
				nodeWithDBInfo["Port"] = strconv.Itoa(int(portFloat))
			}
		}

		pool, err := auth.GetClusterNodeConnection(nodeWithDBInfo, "")
		if err != nil {
			log.Printf("could not connect to node %s: %v. Skipping.", nodeName, err)
			continue
		}
		defer pool.Close()

		db := queries.NewQuerier(pool)
		objects, err := getObjectsForSchema(db, task.SchemaName)
		if err != nil {
			log.Printf("could not get schema objects for node %s: %v. Skipping.", nodeName, err)
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

			missingTables, extraTables := diffStringSlices(refObjects.Tables, cmpObjects.Tables)
			missingViews, extraViews := diffStringSlices(refObjects.Views, cmpObjects.Views)
			missingFunctions, extraFunctions := diffStringSlices(refObjects.Functions, cmpObjects.Functions)
			missingIndices, extraIndices := diffStringSlices(refObjects.Indices, cmpObjects.Indices)

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

func SchemaDiff(task *SchemaDiffCmd) error {
	if err := task.RunChecks(false); err != nil {
		return err
	}

	if task.DDLOnly {
		return schemaDDLDiff(task)
	}

	for _, tableName := range task.tableList {
		qualifiedTableName := fmt.Sprintf("%s.%s", task.SchemaName, tableName)
		if !task.Quiet {
			fmt.Printf("Diffing table: %s\n", qualifiedTableName)
		}

		tdTask := NewTableDiffTask()
		tdTask.ClusterName = task.ClusterName
		tdTask.DBName = task.DBName
		tdTask.Nodes = task.Nodes
		tdTask.QualifiedTableName = qualifiedTableName
		tdTask.ConcurrencyFactor = task.ConcurrencyFactor
		tdTask.BlockSize = task.BlockSize
		tdTask.CompareUnitSize = task.CompareUnitSize
		tdTask.Output = task.Output
		tdTask.TableFilter = task.TableFilter
		tdTask.OverrideBlockSize = task.OverrideBlockSize
		tdTask.QuietMode = task.Quiet

		if err := tdTask.Validate(); err != nil {
			log.Printf("validation for table %s failed: %v", qualifiedTableName, err)
			continue
		}

		if err := tdTask.RunChecks(true); err != nil {
			log.Printf("checks for table %s failed: %v", qualifiedTableName, err)
			continue
		}
		if err := tdTask.ExecuteTask(); err != nil {
			log.Printf("error during comparison for table %s: %v", qualifiedTableName, err)
			continue
		}

	}

	return nil
}
