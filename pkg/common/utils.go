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

package common

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
)

const (
	CheckMark = "\u2714"
	CrossMark = "\u2718"
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

func Contains(slice []string, value string) bool {
	return slices.Contains(slice, value)
}

func ReadClusterInfo(t ClusterConfigProvider) error {
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

// ConvertToPgxType converts a value from a JSON unmarshal to a type that pgx can handle
func ConvertToPgxType(val any, pgType string) (any, error) {
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

func SafeCut(s string, n int) string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

func IsKnownScalarType(colType string) bool {
	knownPrefixes := []string{
		"character", "text",
		"integer", "bigint", "smallint",
		"numeric", "decimal", "real", "double precision",
		"boolean",
		"bytea",
		"json", "jsonb",
		"uuid",
		"timestamp", "date", "time",
	}
	for _, prefix := range knownPrefixes {
		if strings.HasPrefix(colType, prefix) {
			return true
		}
	}
	return false
}

func StringifyKey(row map[string]any, pKeyCols []string) (string, error) {
	if len(pKeyCols) == 0 {
		return "", nil
	}
	// if len(pkValues) != len(pkCols) {
	// 	return "", fmt.Errorf("mismatch between pk value count (%d) and pk column count (%d)", len(pkValues), len(pkCols))
	// }

	if len(pKeyCols) == 1 {
		val, ok := row[pKeyCols[0]]
		if !ok {
			return "", fmt.Errorf("pk column '%s' not found in pk values map", pKeyCols[0])
		}
		return fmt.Sprintf("%v", val), nil
	}

	sortedPkCols := make([]string, len(pKeyCols))
	copy(sortedPkCols, pKeyCols)
	sort.Strings(sortedPkCols)

	var parts []string
	for _, col := range sortedPkCols {
		val, ok := row[col]
		if !ok {
			return "", fmt.Errorf("pk column '%s' not found in pk values map", col)
		}
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "||"), nil
}

func AddSpockMetadata(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	metadata := make(map[string]any)
	if commitTs, ok := row["commit_ts"]; ok {
		metadata["commit_ts"] = commitTs
		delete(row, "commit_ts")
	}
	if nodeOrigin, ok := row["node_origin"]; ok {
		metadata["node_origin"] = nodeOrigin
		delete(row, "node_origin")
	}
	row["_spock_metadata_"] = metadata
	return row
}

func StripSpockMetadata(row map[string]any) map[string]any {
	if row != nil {
		delete(row, "_spock_metadata_")
	}
	return row
}

func DiffStringSlices(a, b []string) (missing, extra []string) {
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

func CompareRowSets(rows1, rows2 []map[string]any, key, cols []string) (*types.NodePairDiff, error) {
	lookupN1 := make(map[string]map[string]any)
	for _, row := range rows1 {
		pkVal := make(map[string]any)
		for _, pkCol := range key {
			pkVal[pkCol] = row[pkCol]
		}
		pkStr, err := StringifyKey(pkVal, key)
		if err != nil {
			return nil, fmt.Errorf("failed to stringify n1 pkey: %w", err)
		}
		lookupN1[pkStr] = row
	}

	lookupN2 := make(map[string]map[string]any)
	for _, row := range rows2 {
		pkVal := make(map[string]any)
		for _, pkCol := range key {
			pkVal[pkCol] = row[pkCol]
		}
		pkStr, err := StringifyKey(pkVal, key)
		if err != nil {
			return nil, fmt.Errorf("failed to stringify n2 pkey: %w", err)
		}
		lookupN2[pkStr] = row
	}

	diffResult := &types.NodePairDiff{}

	for pkStr, n1Row := range lookupN1 {
		n2Row, existsInN2 := lookupN2[pkStr]
		if !existsInN2 {
			diffResult.Node1OnlyRows = append(diffResult.Node1OnlyRows, n1Row)
		} else {
			var mismatch bool
			for _, colName := range cols {
				val1, ok1 := n1Row[colName]
				val2, ok2 := n2Row[colName]

				if ok1 != ok2 {
					mismatch = true
					break
				}
				if !ok1 && !ok2 {
					continue
				}

				if !reflect.DeepEqual(val1, val2) {
					mismatch = true
					break
				}
			}

			if mismatch {
				diffResult.ModifiedRows = append(diffResult.ModifiedRows, struct {
					Pkey      string
					Node1Data map[string]any
					Node2Data map[string]any
				}{Pkey: pkStr, Node1Data: n1Row, Node2Data: n2Row})
			}
			delete(lookupN2, pkStr)
		}
	}

	for _, n2Row := range lookupN2 {
		diffResult.Node2OnlyRows = append(diffResult.Node2OnlyRows, n2Row)
	}

	return diffResult, nil
}

func WriteDiffReport(diffResult types.DiffOutput, schema, table string) error {
	if len(diffResult.NodeDiffs) > 0 {
		outputFileName := fmt.Sprintf("%s_%s_diffs-%s.json",
			strings.ReplaceAll(schema, ".", "_"),
			strings.ReplaceAll(table, ".", "_"),
			time.Now().Format("20060102150405"),
		)

		jsonData, err := json.MarshalIndent(diffResult, "", "  ")
		if err != nil {
			logger.Error("ERROR marshalling diff output to JSON: %v", err)
			return fmt.Errorf("failed to marshal diffs: %w", err)
		}

		err = os.WriteFile(outputFileName, jsonData, 0644)
		if err != nil {
			logger.Error("ERROR writing diff output to file %s: %v", outputFileName, err)
			return fmt.Errorf("failed to write diffs file: %w", err)
		}
		logger.Warn("%s TABLES DO NOT MATCH", CrossMark)

		for key, diffCount := range diffResult.Summary.DiffRowsCount {
			logger.Warn("Found %d differences between %s", diffCount, key)
		}

		logger.Info("Diff report written to %s", outputFileName)

	} else {
		logger.Info("%s TABLES MATCH", CheckMark)
	}
	return nil
}
