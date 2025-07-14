package core

import (
	"context"
	"fmt"
	"log"
	"maps"
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
	ddlOnly           bool
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

func SchemaDiff(task *SchemaDiffCmd) error {
	if err := task.RunChecks(false); err != nil {
		return err
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
		if err := tdTask.ExecuteTask(false); err != nil {
			log.Printf("error during comparison for table %s: %v", qualifiedTableName, err)
			continue
		}

	}

	return nil
}
