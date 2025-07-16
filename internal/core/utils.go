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
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/types"
)

type ClusterConfigProvider interface {
	GetClusterName() string
	GetDBName() string
	SetDBName(string)
	GetNodes() string
	GetNodeList() []string
	SetNodeList([]string)
	SetDatabase(types.Database)
	GetClusterNodes() []map[string]any
	SetClusterNodes([]map[string]any)
}

func ParseNodes(nodes any) ([]string, error) {
	var nodeList []string

	switch v := nodes.(type) {
	case string:
		if v == "all" {
			return nil, nil
		}
		for s := range strings.SplitSeq(v, ",") {
			trimmed := strings.TrimSpace(s)
			if trimmed != "" {
				nodeList = append(nodeList, trimmed)
			}
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

func Contains(slice []string, value string) bool {
	return slices.Contains(slice, value)
}

// TODO: Need to revisit logging
func SetGlobalLogLevel(level LogLevel) {
	logger.SetLevel(level)
}

func readClusterInfo(t ClusterConfigProvider) error {
	configPath := fmt.Sprintf("%s.json", t.GetClusterName())
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return fmt.Errorf("cluster configuration file not found for %s", t.GetClusterName())
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read cluster configuration: %v", err)
	}

	var config types.ClusterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse cluster configuration: %v", err)
	}

	if config.ClusterName != t.GetClusterName() {
		return fmt.Errorf("cluster name in configuration (%s) does not match requested cluster (%s)",
			config.ClusterName, t.GetClusterName())
	}

	currentDBName := t.GetDBName()
	if currentDBName == "" && len(config.PGEdge.Databases) > 0 {
		t.SetDBName(config.PGEdge.Databases[0].DBName)
		t.SetDatabase(config.PGEdge.Databases[0])
	} else if currentDBName != "" {
		foundDB := false
		for _, db := range config.PGEdge.Databases {
			if db.DBName == currentDBName {
				t.SetDatabase(db)
				foundDB = true
				break
			}
		}
		if !foundDB {
			return fmt.Errorf("database %s not found in cluster configuration", currentDBName)
		}
	}

	clusterNodes := []map[string]any{}
	nodeList := t.GetNodeList()
	nodesFilter := t.GetNodes()

	if len(nodeList) == 0 && nodesFilter == "all" {
		for i := range config.NodeGroups {
			node := config.NodeGroups[i]
			if node.IsActive == "on" || node.IsActive == "yes" {
				nodeMap := map[string]any{
					"Name":     node.Name,
					"PublicIP": node.PublicIP,
					"Port":     node.Port,
					"IsActive": node.IsActive,
				}
				clusterNodes = append(clusterNodes, nodeMap)
			}
		}
		var activeNodeNames []string
		for _, cn := range clusterNodes {
			activeNodeNames = append(activeNodeNames, cn["Name"].(string))
		}
		t.SetNodeList(activeNodeNames)

	} else {
		for i := range config.NodeGroups {
			node := config.NodeGroups[i]
			if node.IsActive == "on" || node.IsActive == "yes" {
				var found bool
				for _, n := range nodeList {
					if n == node.Name {
						found = true
						break
					}
				}

				if found {
					nodeMap := map[string]any{
						"Name":     node.Name,
						"PublicIP": node.PublicIP,
						"Port":     node.Port,
						"IsActive": node.IsActive,
					}
					clusterNodes = append(clusterNodes, nodeMap)
				} else {
				}
			}
		}
	}
	t.SetClusterNodes(clusterNodes)

	return nil
}

// convertToPgxType converts a value from a JSON unmarshal to a type that pgx can handle
func convertToPgxType(val interface{}, pgType string) (interface{}, error) {
	if val == nil {
		return nil, nil
	}

	lowerPgType := strings.ToLower(pgType)
	basePgType := strings.TrimSuffix(lowerPgType, "[]")

	switch basePgType {

	case "bool", "boolean":
		if b, ok := val.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("expected bool for %s, got %T", pgType, val)

	case "smallint", "int2", "integer", "int", "int4", "bigint", "int8", "serial2", "serial4", "serial8":
		// JSON numbers might unmarshal to float64. Need to handle this.
		if f, ok := val.(float64); ok {
			return int64(f), nil
		}

		if i, ok := val.(int); ok {
			return int64(i), nil
		}

		if i64, ok := val.(int64); ok {
			return i64, nil
		}

		return nil, fmt.Errorf("expected integer type for %s, got %T", pgType, val)

	case "real", "float4", "double precision", "float8", "numeric", "decimal":
		if f, ok := val.(float64); ok {
			return f, nil
		}

		// If we could not convert to float64, try to convert to string
		// and use pgtype.Numeric
		if s, ok := val.(string); ok {
			num := pgtype.Numeric{}
			if err := num.Set(s); err == nil {
				return &num, nil
			}
		}

		return nil, fmt.Errorf("expected float/numeric type for %s, got %T", pgType, val)

	case "text", "varchar", "char", "bpchar", "name", "citext":
		if s, ok := val.(string); ok {
			return s, nil
		}

		return nil, fmt.Errorf("expected string type for %s, got %T", pgType, val)

	case "bytea":
		// While writing the diff file, we had converted bytea to hex string.
		// Now we convert it back to []byte.
		if s, ok := val.(string); ok {
			if strings.HasPrefix(s, "\\x") {
				decoded, err := hex.DecodeString(s[2:])
				if err == nil {
					return decoded, nil
				}
			}
			return s, nil
		}

		if b, ok := val.([]byte); ok {
			return b, nil
		}

		return nil, fmt.Errorf("expected string (hex/base64) or []byte for bytea, got %T for %s", val, pgType)

	case "date":
		if s, ok := val.(string); ok {
			t, err := time.Parse("2006-01-02", s)
			if err == nil {
				return t, nil
			}

			tFull, errFull := time.Parse(time.RFC3339Nano, s)
			if errFull == nil {
				return tFull.Truncate(24 * time.Hour), nil
			}
		}

		return nil, fmt.Errorf("expected date string (YYYY-MM-DD) for %s, got %v (%T)", pgType, val, val)

	case "timestamp", "timestamptz", "timestamp without time zone", "timestamp with time zone":
		if s, ok := val.(string); ok {
			t, err := time.Parse(time.RFC3339Nano, s)
			if err == nil {
				return t, nil
			}

			t, err = time.Parse("2006-01-02 15:04:05.999999-07", s)
			if err == nil {
				return t, nil
			}

			t, err = time.Parse("2006-01-02 15:04:05.999999", s)
			if err == nil {
				return t, nil
			}
		}

		return nil, fmt.Errorf("expected timestamp string (RFC3339Nano like) for %s, got %v (%T)", pgType, val, val)

	case "json", "jsonb":
		if s, ok := val.(string); ok {
			return s, nil
		}

		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal value to JSON for %s: %w", pgType, err)
		}

		return string(jsonBytes), nil

	case "uuid":
		if s, ok := val.(string); ok {
			return s, nil
		}

		return nil, fmt.Errorf("expected UUID string for %s, got %T", pgType, val)

	default:
		// For now, all other types are passed as strings.
		if s, ok := val.(string); ok {
			log.Printf("Warning: Passing raw string value '%s' for unknown or complex pgType '%s'", s, pgType)
			return s, nil
		}

		return val, nil
	}

	// TODO: Array handling!
}

// stringifyPKey converts a primary key (simple or composite) from a row map into a consistent string representation.
func stringifyPKey(row map[string]any, keyCols []string, isSimplePK bool) (string, error) {
	if len(keyCols) == 0 {
		return "", fmt.Errorf("no primary key columns defined")
	}

	if isSimplePK {
		val, ok := row[keyCols[0]]
		if !ok {
			return "", fmt.Errorf("primary key column %s not found in row", keyCols[0])
		}
		return fmt.Sprintf("%v", val), nil
	}

	// Composite key: sort and then join for consistency
	sortedKeyCols := make([]string, len(keyCols))
	copy(sortedKeyCols, keyCols)
	sort.Strings(sortedKeyCols)

	var pkParts []string
	for _, colName := range sortedKeyCols {
		val, ok := row[colName]
		if !ok {
			return "", fmt.Errorf("primary key column %s not found in row", colName)
		}
		pkParts = append(pkParts, fmt.Sprintf("%v", val))
	}
	return strings.Join(pkParts, "||"), nil
}
