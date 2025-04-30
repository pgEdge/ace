package core

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/types"
)

func ParseNodes(nodes interface{}) ([]string, error) {
	var nodeList []string

	switch v := nodes.(type) {
	case string:
		if v == "all" {
			return nil, nil
		}
		for _, s := range strings.Split(v, ",") {
			nodeList = append(nodeList, strings.TrimSpace(s))
		}
	case []string:
		nodeList = v
	default:
		return nil, fmt.Errorf("nodes must be a string or string slice")
	}

	if len(nodeList) > 0 {
		seen := make(map[string]bool)
		unique := []string{}

		for _, node := range nodeList {
			if _, ok := seen[node]; !ok {
				seen[node] = true
				unique = append(unique, node)
			}
		}

		if len(unique) < len(nodeList) {
			logger.Info("Ignoring duplicate node names")
			nodeList = unique
		}
	}

	return nodeList, nil
}

func CheckClusterExists(clusterName string) bool {
	clusterDir := "cluster/" + clusterName
	_, err := os.Stat(clusterDir)
	return err == nil
}

func GetColumns(db *pgxpool.Pool, schema, table string) ([]string, error) {
	var schemaName pgtype.Name
	var tableName pgtype.Name

	err := schemaName.Set(schema)
	if err != nil {
		return nil, err
	}
	err = tableName.Set(table)
	if err != nil {
		return nil, err
	}
	q := queries.NewQuerier(db)
	rows, err := q.GetColumns(context.Background(), schemaName, tableName)
	if err != nil {
		return nil, err
	}

	var columns []string
	for _, col := range rows {
		columns = append(columns, col.String)
	}

	if len(columns) == 0 {
		return nil, nil
	}

	return columns, nil
}

func GetPrimaryKey(db *pgxpool.Pool, schema, table string) ([]string, error) {
	var schemaName pgtype.Name
	var tableName pgtype.Name

	err := schemaName.Set(schema)
	if err != nil {
		return nil, err
	}
	err = tableName.Set(table)
	if err != nil {
		return nil, err
	}
	q := queries.NewQuerier(db)
	rows, err := q.GetPrimaryKey(context.Background(), schemaName, tableName)
	if err != nil {
		return nil, err
	}

	var keys []string
	for _, key := range rows {
		keys = append(keys, key.String)
	}

	if len(keys) == 0 {
		return nil, nil
	}

	return keys, nil
}

func GetColumnTypes(db *pgxpool.Pool, table string) (map[string]string, error) {
	var tableName pgtype.Name

	err := tableName.Set(table)
	if err != nil {
		return nil, err
	}
	q := queries.NewQuerier(db)
	rows, err := q.GetColumnTypes(context.Background(), tableName)
	if err != nil {
		return nil, err
	}

	types := make(map[string]string)
	for _, row := range rows {
		types[row.ColumnName.String] = *row.DataType
	}

	if len(types) == 0 {
		return nil, fmt.Errorf("could not fetch column types")
	}

	return types, nil
}

func CheckUserPrivileges(db *pgxpool.Pool, username, schema, table string, requiredPrivileges []string) (bool, map[string]bool, error) {
	q := queries.NewQuerier(db)
	result, err := q.CheckUserPrivileges(context.Background(), queries.CheckUserPrivilegesParams{
		Username:   username,
		SchemaName: schema,
		TableName:  table,
	})
	if err != nil {
		return false, nil, err
	}

	privileges := map[string]bool{
		"table_select":             *result.TableSelect,
		"table_create":             *result.TableCreate,
		"table_insert":             *result.TableInsert,
		"table_update":             *result.TableUpdate,
		"table_delete":             *result.TableDelete,
		"columns_select":           *result.ColumnsSelect,
		"table_constraints_select": *result.TableConstraintsSelect,
		"key_column_usage_select":  *result.KeyColumnUsageSelect,
	}

	missingPrivileges := make(map[string]bool)
	allPresent := true

	if len(requiredPrivileges) > 0 {
		for _, priv := range requiredPrivileges {
			privKey := "table_" + strings.ToLower(priv)
			if !privileges[privKey] {
				missingPrivileges[privKey] = false
				allPresent = false
			}
		}
	} else {
		for k, v := range privileges {
			if !v {
				missingPrivileges[k] = false
				allPresent = false
			}
		}
	}

	return allPresent, missingPrivileges, nil
}

func CheckDiffFileFormat(diffFilePath string, task *TableDiffTask) error {
	fileData, err := os.ReadFile(diffFilePath)
	if err != nil {
		return fmt.Errorf("could not read diff file: %w", err)
	}

	var diffJson map[string]interface{}
	if err := json.Unmarshal(fileData, &diffJson); err != nil {
		return fmt.Errorf("could not parse diff file as JSON: %w", err)
	}

	diffs, ok := diffJson["diffs"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("contents of diff file improperly formatted: missing 'diffs' object")
	}

	for nodePair, nodesData := range diffs {
		nodeNames := strings.Split(nodePair, "/")
		if len(nodeNames) != 2 {
			return fmt.Errorf("invalid node pair format: %s", nodePair)
		}

		nodes, ok := nodesData.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid nodes data for pair %s", nodePair)
		}

		for nodeName := range nodes {
			found := false
			for _, n := range nodeNames {
				if n == nodeName {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("node %s not found in pair %s", nodeName, nodePair)
			}
		}
	}

	return nil
}

func Contains(slice []string, value string) bool {
	return slices.Contains(slice, value)
}

func readClusterInfo(t *TableDiffTask) error {
	configPath := fmt.Sprintf("%s.json", t.ClusterName)
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("cluster configuration file not found for %s", t.ClusterName)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read cluster configuration: %v", err)
	}

	var config types.ClusterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse cluster configuration: %v", err)
	}

	if config.ClusterName != t.ClusterName {
		return fmt.Errorf("cluster name in configuration (%s) does not match requested cluster (%s)",
			config.ClusterName, t.ClusterName)
	}

	if t.DBName == "" && len(config.PGEdge.Databases) > 0 {
		t.DBName = config.PGEdge.Databases[0].DBName
		t.Database = config.PGEdge.Databases[0]
	}

	t.ClusterNodes = []map[string]any{}
	if len(t.NodeList) == 0 && t.Nodes == "all" {
		for i := range config.NodeGroups {
			node := config.NodeGroups[i]
			if node.IsActive == "on" {
				nodeMap := map[string]any{
					"Name":     node.Name,
					"PublicIP": node.PublicIP,
					"Port":     node.Port,
					"IsActive": node.IsActive,
				}
				t.ClusterNodes = append(t.ClusterNodes, nodeMap)
			}
		}
	} else {
		for i := range config.NodeGroups {
			node := config.NodeGroups[i]
			if node.IsActive == "on" {
				if slices.Contains(t.NodeList, node.Name) {
					nodeMap := map[string]any{
						"Name":     node.Name,
						"PublicIP": node.PublicIP,
						"Port":     node.Port,
						"IsActive": node.IsActive,
					}
					t.ClusterNodes = append(t.ClusterNodes, nodeMap)
				}
			}
		}
	}

	return nil
}
