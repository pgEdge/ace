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
	"bufio"
	"context"
	"fmt"
	"log"
	"maps"
	"os"
	"strconv"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/types"
)

type RepsetDiffCmd struct {
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

	repset, err := db.CheckRepSetExists(context.Background(), pgtype.Name{String: c.RepsetName, Status: pgtype.Present})
	if err != nil {
		return fmt.Errorf("could not check if repset exists: %w", err)
	}
	if repset.Status != pgtype.Present {
		return fmt.Errorf("repset %s not found", c.RepsetName)
	}

	tables, err := db.GetTablesInRepSet(context.Background(), pgtype.Name{String: c.RepsetName, Status: pgtype.Present})
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
	return nil
}

func RepsetDiff(task *RepsetDiffCmd) error {
	if err := task.RunChecks(false); err != nil {
		return err
	}

	for _, tableName := range task.tableList {
		var skipped bool
		for _, skip := range task.skipTablesList {
			if strings.TrimSpace(skip) == tableName {
				if !task.Quiet {
					fmt.Printf("Skipping table: %s\n", tableName)
				}
				skipped = true
				break
			}
		}
		if skipped {
			continue
		}

		if !task.Quiet {
			fmt.Printf("Diffing table: %s\n", tableName)
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

		if err := tdTask.Validate(); err != nil {
			log.Printf("validation for table %s failed: %v", tableName, err)
			continue
		}

		if err := tdTask.RunChecks(true); err != nil {
			log.Printf("checks for table %s failed: %v", tableName, err)
			continue
		}
		if err := tdTask.ExecuteTask(false); err != nil {
			log.Printf("error during comparison for table %s: %v", tableName, err)
			continue
		}
	}
	return nil
}
