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

package common

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pgedge/ace/pkg/config"
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

func findPgServiceFile() (string, error) {
	if override := os.Getenv("ACE_PGSERVICEFILE"); override != "" {
		if _, err := os.Stat(override); err == nil {
			return override, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	if envPath := os.Getenv("PGSERVICEFILE"); envPath != "" {
		if _, err := os.Stat(envPath); err == nil {
			return envPath, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}

	candidates := []string{"pg_service.conf"}
	if home, err := os.UserHomeDir(); err == nil {
		candidates = append(candidates, filepath.Join(home, ".pg_service.conf"))
	}
	candidates = append(candidates, "/etc/pg_service.conf")

	for _, path := range candidates {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			return path, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
	}
	return "", os.ErrNotExist
}

func parsePgServiceFile(path string) (map[string]map[string]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	sections := make(map[string]map[string]string)
	scanner := bufio.NewScanner(file)
	var current map[string]string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			name := strings.TrimSpace(line[1 : len(line)-1])
			if name == "" {
				current = nil
				continue
			}
			current = make(map[string]string)
			sections[name] = current
			continue
		}
		if current == nil {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		value := strings.TrimSpace(parts[1])
		current[strings.ToLower(key)] = value
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return sections, nil
}

func loadClusterInfoFromServiceFile(t ClusterConfigProvider) (bool, error) {
	servicePath, err := findPgServiceFile()
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("failed to locate pg service file: %w", err)
	}

	sections, err := parsePgServiceFile(servicePath)
	if err != nil {
		return false, fmt.Errorf("failed to parse pg service file %s: %w", servicePath, err)
	}

	clusterName := t.GetClusterName()
	baseOptions := sections[clusterName]
	prefix := clusterName + "."

	nodeOptions := make(map[string]map[string]string)
	var nodeNames []string
	for name, opts := range sections {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		nodeName := strings.TrimPrefix(name, prefix)
		if nodeName == "" {
			continue
		}
		combined := make(map[string]string)

		maps.Copy(combined, baseOptions)
		maps.Copy(combined, opts)
		nodeOptions[nodeName] = combined
		nodeNames = append(nodeNames, nodeName)
	}

	if len(nodeOptions) == 0 {
		return false, nil
	}

	sort.Strings(nodeNames)

	nodeList := t.GetNodeList()
	nodesFilter := t.GetNodes()

	clusterNodes := []map[string]any{}
	selectedNames := []string{}
	presentNodes := make(map[string]struct{})

	currentDBName := t.GetDBName()
	var resolvedDB types.Database
	dbSet := false

	for _, nodeName := range nodeNames {
		if len(nodeList) > 0 && !Contains(nodeList, nodeName) {
			continue
		}

		opts := nodeOptions[nodeName]
		host := strings.TrimSpace(opts["host"])
		if host == "" {
			if h := strings.TrimSpace(opts["hostaddr"]); h != "" {
				host = h
			}
		}
		if host == "" {
			return false, fmt.Errorf("service %s.%s missing host/hostaddr in %s", clusterName, nodeName, servicePath)
		}

		port := strings.TrimSpace(opts["port"])
		if port == "" {
			port = "5432"
		}

		dbName := strings.TrimSpace(opts["dbname"])
		if currentDBName != "" {
			if dbName == "" {
				dbName = currentDBName
			} else if dbName != currentDBName {
				return false, fmt.Errorf("service %s.%s refers to database %s; expected %s", clusterName, nodeName, dbName, currentDBName)
			}
		} else if dbName == "" {
			return false, fmt.Errorf("service %s.%s missing dbname in %s", clusterName, nodeName, servicePath)
		}

		user := strings.TrimSpace(opts["user"])

		if !dbSet {
			resolvedDB = types.Database{
				DBName: dbName,
				DBUser: user,
			}
			if dbName != "" {
				t.SetDBName(dbName)
			}
			dbSet = true
		} else {
			if resolvedDB.DBName == "" {
				resolvedDB.DBName = dbName
			}
			if resolvedDB.DBUser == "" {
				resolvedDB.DBUser = user
			}
		}

		cfg := config.Cfg
		useCertAuth := cfg != nil && cfg.CertAuth.UseCertAuth

		password := ""
		if !useCertAuth {
			password = strings.TrimSpace(opts["password"])
			if password != "" {
				resolvedDB.DBPassword = password
			}
		} else {
			if mode := strings.TrimSpace(opts["sslmode"]); mode != "" {
				resolvedDB.SSLMode = mode
			}
			if cert := strings.TrimSpace(opts["sslcert"]); cert != "" {
				resolvedDB.SSLCert = cert
			}
			if key := strings.TrimSpace(opts["sslkey"]); key != "" {
				resolvedDB.SSLKey = key
			}
			if root := strings.TrimSpace(opts["sslrootcert"]); root != "" {
				resolvedDB.SSLRootCert = root
			}
		}

		nodeMap := map[string]any{
			"Name":        nodeName,
			"Service":     fmt.Sprintf("%s.%s", clusterName, nodeName),
			"PublicIP":    host,
			"Host":        host,
			"Port":        port,
			"IsActive":    "yes",
			"DBName":      dbName,
			"DBUser":      user,
			"DBPassword":  password,
			"SSLMode":     resolvedDB.SSLMode,
			"SSLCert":     resolvedDB.SSLCert,
			"SSLKey":      resolvedDB.SSLKey,
			"SSLRootCert": resolvedDB.SSLRootCert,
		}
		clusterNodes = append(clusterNodes, nodeMap)
		selectedNames = append(selectedNames, nodeName)
		presentNodes[nodeName] = struct{}{}
	}

	if len(clusterNodes) == 0 {
		return false, fmt.Errorf("no matching nodes found for cluster %s in %s", clusterName, servicePath)
	}

	if len(nodeList) > 0 {
		var missing []string
		for _, requested := range nodeList {
			if _, ok := presentNodes[requested]; !ok {
				missing = append(missing, requested)
			}
		}
		if len(missing) > 0 {
			return false, fmt.Errorf("specified nodename(s) %s not present in cluster %s", strings.Join(missing, ", "), clusterName)
		}
	} else if nodesFilter == "all" {
		toSet := make([]string, len(selectedNames))
		copy(toSet, selectedNames)
		t.SetNodeList(toSet)
	}

	if !dbSet {
		return false, fmt.Errorf("no database information found for cluster %s in %s", clusterName, servicePath)
	}

	t.SetDatabase(resolvedDB)
	t.SetClusterNodes(clusterNodes)
	return true, nil
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
	clusterName := t.GetClusterName()

	if loaded, err := loadClusterInfoFromServiceFile(t); err != nil {
		return err
	} else if loaded {
		return nil
	}

	configPath := fmt.Sprintf("%s.json", clusterName)
	if _, err := os.Stat(configPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("cluster configuration not found for %s: expected pg_service entry or %s", clusterName, configPath)
		}
		return fmt.Errorf("failed to access cluster configuration file %s: %w", configPath, err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read cluster configuration: %v", err)
	}

	var config types.ClusterConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("failed to parse cluster configuration: %v", err)
	}

	if config.ClusterName != clusterName {
		return fmt.Errorf("cluster name in configuration (%s) does not match requested cluster (%s)",
			config.ClusterName, clusterName)
	}

	currentDBName := t.GetDBName()
	if currentDBName == "" && len(config.PGEdge.Databases) > 0 {
		db := config.PGEdge.Databases[0]
		t.SetDBName(db.DBName)
		t.SetDatabase(db)
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
				}
			}
		}
	}
	t.SetClusterNodes(clusterNodes)

	return nil
}

// ApplyDatabaseCredentials ensures the provided map has the database level
// connection details (user/password/ssl). Existing non-empty values are left
// untouched so node-specific overrides are preserved.
func ApplyDatabaseCredentials(dst map[string]any, db types.Database) {
	if dst == nil {
		return
	}

	setIfEmpty := func(key, val string) {
		if val == "" {
			return
		}
		if existing, ok := dst[key]; ok {
			if s, ok := existing.(string); ok && strings.TrimSpace(s) != "" {
				return
			}
		}
		dst[key] = val
	}

	setIfEmpty("DBName", db.DBName)
	setIfEmpty("DBUser", db.DBUser)
	setIfEmpty("DBPassword", db.DBPassword)
	setIfEmpty("SSLMode", db.SSLMode)
	setIfEmpty("SSLCert", db.SSLCert)
	setIfEmpty("SSLKey", db.SSLKey)
	setIfEmpty("SSLRootCert", db.SSLRootCert)
}

func SafeCut(s string, n int) string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

// RowKeyFromStrings joins already-rendered primary-key parts into the
// in-memory key that matches a row across nodes.
//
// Joined raw, the composite keys ("a|b","c") and ("a","b|c") produce the same
// string, so one of the two rows silently drops out of the comparison -- in a
// consistency checker that is a missed difference, not a cosmetic bug. So a
// part is quoted, but only when it has to be: when it is empty, or contains
// the delimiter or a quote. An ordinary key therefore still reads as
// 123|abc, which matters because these keys are interpolated into
// operator-facing messages ("row missing on n2 (pk 123)").
//
// That rule keeps distinct part lists distinct. Reading left to right, every
// element says where it ends: a bare element holds no quote, so it cannot be
// mistaken for a quoted one, and holds no delimiter, so it ends at the next
// one; a quoted element starts with a quote and ends where the string literal
// closes. ("a|b","c") is "a|b"|c and ("a","b|c") is a|"b|c".
//
// The result is a matching key, not a primary key: it is never persisted, sent
// to the server or parsed back. Code that needs real pkey values has to carry
// them separately.
func RowKeyFromStrings(parts []string) string {
	encoded := make([]string, len(parts))
	for i, p := range parts {
		if p == "" || strings.ContainsAny(p, "|\"") {
			encoded[i] = strconv.Quote(p)
		} else {
			encoded[i] = p
		}
	}
	return strings.Join(encoded, "|")
}

// StringifyKey renders the primary-key columns of a row into the in-memory key
// that matches that row against the same row on another node. The parts are
// encoded with RowKeyFromStrings, so a value that itself contains the
// delimiter cannot collide with a different composite key.
//
// Like every RowKeyFromStrings result, this is a matching key and not a pkey
// value: it must not reach SQL, a report or repair.
func StringifyKey(row map[string]any, pkeyCols []string) (string, error) {
	pkeyParts := make([]string, len(pkeyCols))
	for i, pkeyCol := range pkeyCols {
		val, ok := row[pkeyCol]
		if !ok {
			return "", fmt.Errorf("pkey column %s not found in row", pkeyCol)
		}
		pkeyParts[i] = fmt.Sprintf("%v", val)
	}
	return RowKeyFromStrings(pkeyParts), nil
}

func AddSpockMetadata(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	metadata := make(map[string]any)
	if nodeOrigin, ok := row["node_origin"]; ok {
		metadata["node_origin"] = nodeOrigin
		delete(row, "node_origin")
	}
	if commitTs, ok := row["commit_ts"]; ok {
		metadata["commit_ts"] = commitTs
		delete(row, "commit_ts")
	}
	row["_spock_metadata_"] = metadata
	return row
}

func TranslateNodeOrigin(raw any, nodeNames map[string]string) any {
	if raw == nil {
		return nil
	}
	origin := strings.TrimSpace(fmt.Sprintf("%v", raw))
	if origin == "" {
		return nil
	}
	if origin == "0" {
		return "local"
	}
	if nodeNames != nil {
		if name, ok := nodeNames[origin]; ok {
			return name
		}
	}
	return raw
}

func StripSpockMetadata(row map[string]any) map[string]any {
	newRow := make(map[string]any)
	for k, v := range row {
		if k != "_spock_metadata_" && k != "node_origin" && k != "commit_ts" {
			newRow[k] = v
		}
	}
	return newRow
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

func OrderedMapToMap(om types.OrderedMap) map[string]any {
	m := make(map[string]any, len(om))
	for _, kv := range om {
		m[kv.Key] = kv.Value
	}
	return m
}

// StringifyOrderedMapKey is StringifyKey for an OrderedMap. It must stay
// byte-for-byte compatible with StringifyKey: the two are used interchangeably
// on one map, so a row keyed by either has to be found by the other.
func StringifyOrderedMapKey(row types.OrderedMap, pkeyCols []string) (string, error) {
	pkeyParts := make([]string, len(pkeyCols))
	for i, pkeyCol := range pkeyCols {
		val, ok := row.Get(pkeyCol)
		if !ok {
			return "", fmt.Errorf("pkey column %s not found in row", pkeyCol)
		}
		pkeyParts[i] = fmt.Sprintf("%v", val)
	}
	return RowKeyFromStrings(pkeyParts), nil
}

func MapToOrderedMap(m map[string]any, cols []string) types.OrderedMap {
	om := make(types.OrderedMap, 0, len(cols))
	colFound := make(map[string]bool, len(cols))
	for _, col := range cols {
		if val, ok := m[col]; ok {
			om = append(om, types.KVPair{Key: col, Value: val})
			colFound[col] = true
		}
	}

	var extraKeys []string
	for k := range m {
		if !colFound[k] {
			extraKeys = append(extraKeys, k)
		}
	}
	sort.Strings(extraKeys)
	for _, k := range extraKeys {
		om = append(om, types.KVPair{Key: k, Value: m[k]})
	}

	return om
}
