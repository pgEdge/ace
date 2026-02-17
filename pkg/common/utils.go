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
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgtype"
	pgxv5type "github.com/jackc/pgx/v5/pgtype"
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

// ConvertToPgxType converts a value from a JSON unmarshal to a type that pgx can handle
func ConvertToPgxType(val any, pgType string) (any, error) {
	if val == nil {
		return nil, nil
	}

	if s, ok := val.(string); ok {
		if strings.TrimSpace(s) == "<nil>" {
			return nil, nil
		}
	}

	lowerPgType := strings.ToLower(pgType)
	basePgType := strings.TrimSuffix(lowerPgType, "[]")
	normalizedType := strings.Split(basePgType, "(")[0]

	if strings.Contains(lowerPgType, "json") {
		switch v := val.(type) {
		case string:
			return v, nil
		case []byte:
			return string(v), nil
		case json.RawMessage:
			return string(v), nil
		default:
			marshalled, err := json.Marshal(v)
			if err == nil {
				return string(marshalled), nil
			}
			return nil, fmt.Errorf("failed to marshal value to JSON for %s: %w", pgType, err)
		}
	}

	if strings.HasSuffix(lowerPgType, "[]") {
		literal, err := buildPgArrayLiteral(val)
		if err != nil {
			return nil, err
		}
		return literal, nil
	}

	switch normalizedType {

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

	case "text", "varchar", "character varying", "char", "character", "bpchar", "name", "citext":
		if s, ok := val.(string); ok {
			return s, nil
		}

		return nil, fmt.Errorf("expected string type for %s, got %T", pgType, val)

	case "bytea":
		// While writing the diff file, we had converted bytea to hex string.
		// Now we convert it back to []byte. We also attempt base64 decode if no \x prefix.
		if s, ok := val.(string); ok {
			if strings.HasPrefix(s, "\\x") {
				decoded, err := hex.DecodeString(s[2:])
				if err == nil {
					return decoded, nil
				}
			}
			if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
				return decoded, nil
			}
			if decoded, err := hex.DecodeString(s); err == nil {
				return decoded, nil
			}
			return []byte(s), nil
		}

		if b, ok := val.([]byte); ok {
			return b, nil
		}
		if b, ok := val.([]uint8); ok {
			return []byte(b), nil
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

			// Fallback: let Postgres cast from the raw string
			return s, nil
		}

		return nil, fmt.Errorf("expected date string (YYYY-MM-DD) for %s, got %v (%T)", pgType, val, val)

	case "timestamp", "timestamp without time zone":
		if s, ok := val.(string); ok {
			t, err := parseTimestampString(s)
			if err == nil {
				// Return pgxv5type.Timestamp so pgx sends the value as "timestamp"
				// instead of "timestamptz". This avoids PostgreSQL
				// applying a session-timezone conversion when inserting into
				// a "timestamp without time zone" column.
				return pgxv5type.Timestamp{Time: t, Valid: true}, nil
			}

			// Fallback: let Postgres cast from the raw string
			return s, nil
		}

		return nil, fmt.Errorf("expected timestamp string (RFC3339Nano like) for %s, got %v (%T)", pgType, val, val)

	case "timestamptz", "timestamp with time zone":
		if s, ok := val.(string); ok {
			t, err := parseTimestampString(s)
			if err == nil {
				return t, nil
			}

			// Fallback: let Postgres cast from the raw string
			return s, nil
		}

		return nil, fmt.Errorf("expected timestamp string (RFC3339Nano like) for %s, got %v (%T)", pgType, val, val)

	case "time", "time without time zone":
		if s, ok := val.(string); ok {
			usec, err := parseTimeToMicroseconds(s)
			if err == nil {
				return pgxv5type.Time{Microseconds: usec, Valid: true}, nil
			}
			// Fallback: let Postgres cast from the raw string
			return s, nil
		}
		return nil, fmt.Errorf("expected time string (HH:MM:SS) for %s, got %v (%T)", pgType, val, val)

	case "timetz", "time with time zone":
		// timetz is not registered in pgx v5's default type map, so the
		// safest approach is to pass the string through and let Postgres parse it.
		if s, ok := val.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("expected time string for %s, got %v (%T)", pgType, val, val)

	case "interval":
		if s, ok := val.(string); ok {
			return s, nil
		}
		if iv, ok := val.(pgtype.Interval); ok {
			if iv.Status != pgtype.Present {
				return nil, nil
			}
			if encoded, err := iv.EncodeText(nil, nil); err == nil {
				return string(encoded), nil
			}
			// Fallback: duration string based on microseconds
			dur := time.Duration(iv.Microseconds) * time.Microsecond
			return dur.String(), nil
		}
		return fmt.Sprintf("%v", val), nil

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
		// For all other types, fall back to string representations to keep repairs viable.
		if s, ok := val.(string); ok {
			log.Printf("Warning: Passing raw string value '%s' for unknown or complex pgType '%s'", s, pgType)
			return s, nil
		}
		if stringer, ok := val.(fmt.Stringer); ok {
			str := stringer.String()
			log.Printf("Warning: Stringified value '%s' for unknown or complex pgType '%s'", str, pgType)
			return str, nil
		}
		log.Printf("Warning: Converting value of type %T to string for pgType '%s'", val, pgType)
		return fmt.Sprint(val), nil
	}
}

// parseTimeToMicroseconds parses a PostgreSQL time string (HH:MM:SS[.ffffff])
// into microseconds since midnight. It stops at any timezone offset suffix so
// the same helper works for both "time" and "timetz" text representations.
func parseTimeToMicroseconds(s string) (int64, error) {
	// Strip timezone offset if present (e.g., "10:30:00-07" or "10:30:00+05:30")
	core := s
	for i := 8; i < len(core); i++ {
		if core[i] == '+' || core[i] == '-' {
			core = core[:i]
			break
		}
	}

	if len(core) < 8 || core[2] != ':' || core[5] != ':' {
		return 0, fmt.Errorf("invalid time format: %s", s)
	}

	hours, err := strconv.ParseInt(core[0:2], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid time hours: %s", s)
	}
	minutes, err := strconv.ParseInt(core[3:5], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid time minutes: %s", s)
	}
	seconds, err := strconv.ParseInt(core[6:8], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid time seconds: %s", s)
	}

	usec := hours*3_600_000_000 + minutes*60_000_000 + seconds*1_000_000

	if len(core) > 9 && core[8] == '.' {
		frac := core[9:]
		n, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid time fractional seconds: %s", s)
		}
		for i := len(frac); i < 6; i++ {
			n *= 10
		}
		usec += n
	}

	return usec, nil
}

func parseTimestampString(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999-07", s); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02 15:04:05.999999", s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unable to parse timestamp string: %s", s)
}

func buildPgArrayLiteral(val any) (string, error) {
	// If caller already provided a string literal, normalise [] to {} and return.
	if s, ok := val.(string); ok {
		trimmed := strings.TrimSpace(s)
		switch {
		case strings.HasPrefix(trimmed, "{") && strings.HasSuffix(trimmed, "}"):
			return trimmed, nil
		case strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]"):
			return "{" + strings.TrimSuffix(strings.TrimPrefix(trimmed, "["), "]") + "}", nil
		default:
			return "{" + trimmed + "}", nil
		}
	}

	// Normalise known slice types to []any
	toAnySlice := func(v any) []any {
		switch slice := v.(type) {
		case []any:
			return slice
		case []string:
			res := make([]any, len(slice))
			for i, e := range slice {
				res[i] = e
			}
			return res
		case []int:
			res := make([]any, len(slice))
			for i, e := range slice {
				res[i] = e
			}
			return res
		case []int64:
			res := make([]any, len(slice))
			for i, e := range slice {
				res[i] = e
			}
			return res
		case []float64:
			res := make([]any, len(slice))
			for i, e := range slice {
				res[i] = e
			}
			return res
		case []bool:
			res := make([]any, len(slice))
			for i, e := range slice {
				res[i] = e
			}
			return res
		default:
			return nil
		}
	}

	anySlice := toAnySlice(val)
	if anySlice == nil {
		return "", fmt.Errorf("unsupported array value type %T", val)
	}

	needsQuote := func(s string) bool {
		return strings.ContainsAny(s, `{}, " \t\n\r`) || len(s) == 0
	}

	escapeElem := func(elem any) string {
		if elem == nil {
			return "NULL"
		}
		if s, ok := elem.(string); ok {
			str := strings.ReplaceAll(s, `\`, `\\`)
			str = strings.ReplaceAll(str, `"`, `\"`)
			return `"` + str + `"`
		}
		str := fmt.Sprint(elem)
		if needsQuote(str) {
			str = strings.ReplaceAll(str, `\`, `\\`)
			str = strings.ReplaceAll(str, `"`, `\"`)
			return `"` + str + `"`
		}
		return str
	}

	parts := make([]string, len(anySlice))
	for i, e := range anySlice {
		parts[i] = escapeElem(e)
	}

	return "{" + strings.Join(parts, ",") + "}", nil
}

func SafeCut(s string, n int) string {
	if len(s) < n {
		return s
	}
	return s[:n]
}

func IsKnownScalarType(colType string) bool {
	// "time with time zone" (timetz) is NOT registered in the pgx v5
	// default type map, so it cannot be scanned into *interface{}. Exclude it
	// so the diff layer casts it to ::TEXT.
	if strings.HasPrefix(colType, "time with time zone") {
		return false
	}

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

func StringifyKey(row map[string]any, pkeyCols []string) (string, error) {
	var pkeyParts []string
	for _, pkeyCol := range pkeyCols {
		val, ok := row[pkeyCol]
		if !ok {
			return "", fmt.Errorf("pkey column %s not found in row", pkeyCol)
		}
		pkeyParts = append(pkeyParts, fmt.Sprintf("%v", val))
	}
	return strings.Join(pkeyParts, "|"), nil
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

type DiffResult struct {
	Node1OnlyRows []types.OrderedMap
	Node2OnlyRows []types.OrderedMap
	ModifiedRows  []ModifiedRow
}

type ModifiedRow struct {
	PKey      string
	Node1Data types.OrderedMap
	Node2Data types.OrderedMap
}

func CompareRowSets(rows1, rows2 []types.OrderedMap, pkeyCols []string, dataCols []string) (DiffResult, error) {
	map1, err := rowsToMap(rows1, pkeyCols)
	if err != nil {
		return DiffResult{}, fmt.Errorf("failed to convert rows1 to map: %w", err)
	}
	map2, err := rowsToMap(rows2, pkeyCols)
	if err != nil {
		return DiffResult{}, fmt.Errorf("failed to convert rows2 to map: %w", err)
	}

	var result DiffResult

	for pkey, row1 := range map1 {
		row2, ok := map2[pkey]
		if !ok {
			result.Node1OnlyRows = append(result.Node1OnlyRows, row1)
		} else {
			isModified, err := areRowsModified(row1, row2, dataCols)
			if err != nil {
				return DiffResult{}, fmt.Errorf("failed to compare rows for pkey %s: %w", pkey, err)
			}
			if isModified {
				result.ModifiedRows = append(result.ModifiedRows, ModifiedRow{PKey: pkey, Node1Data: row1, Node2Data: row2})
			}
		}
	}

	for pkey, row2 := range map2 {
		if _, ok := map1[pkey]; !ok {
			result.Node2OnlyRows = append(result.Node2OnlyRows, row2)
		}
	}
	return result, nil
}

func rowsToMap(rows []types.OrderedMap, pkeyCols []string) (map[string]types.OrderedMap, error) {
	rowMap := make(map[string]types.OrderedMap, len(rows))
	for _, row := range rows {
		pkey, err := buildPKey(row, pkeyCols)
		if err != nil {
			return nil, fmt.Errorf("failed to build pkey for row: %w", err)
		}
		rowMap[pkey] = row
	}
	return rowMap, nil
}

// NormalizeNumericString strips trailing fractional zeros from a numeric string,
// acting as the Go-side equivalent of PostgreSQL's trim_scale().
// Examples: "3000.00" → "3000", "3000.10" → "3000.1", "3000" → "3000"
func NormalizeNumericString(s string) string {
	dotIdx := strings.IndexByte(s, '.')
	if dotIdx < 0 {
		return s
	}
	// Trim trailing zeros after the decimal point
	trimmed := strings.TrimRight(s, "0")
	// If all fractional digits were zeros, remove the decimal point too
	trimmed = strings.TrimRight(trimmed, ".")
	return trimmed
}

// stringifyValue converts a value to its string representation for comparison,
// normalizing numeric types to strip trailing fractional zeros.
func stringifyValue(val any) string {
	switch v := val.(type) {
	case pgtype.Numeric:
		// pgtype v1 (standalone, used in table_diff/table_rerun paths)
		if v.Status == pgtype.Present {
			text, err := v.EncodeText(nil, nil)
			if err == nil {
				return NormalizeNumericString(string(text))
			}
		}
		return fmt.Sprintf("%v", val)
	case pgxv5type.Numeric:
		// pgx v5 pgtype (used in merkle's processRows via rows.Values())
		if v.Valid {
			text, err := v.MarshalJSON()
			if err == nil {
				return NormalizeNumericString(string(text))
			}
		}
		return fmt.Sprintf("%v", val)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", val)
	}
}

func areRowsModified(row1, row2 types.OrderedMap, dataCols []string) (bool, error) {
	row1Map := OrderedMapToMap(row1)
	row2Map := OrderedMapToMap(row2)

	for _, col := range dataCols {
		val1, ok1 := row1Map[col]
		val2, ok2 := row2Map[col]

		if !ok1 || !ok2 {
			return false, fmt.Errorf("column %s not found in one of the rows", col)
		}

		sVal1 := stringifyValue(val1)
		sVal2 := stringifyValue(val2)

		if sVal1 != sVal2 {
			return true, nil
		}
	}
	return false, nil
}

func buildPKey(row types.OrderedMap, pkeyCols []string) (string, error) {
	rowMap := OrderedMapToMap(row)
	var pkeyParts []string
	for _, pkeyCol := range pkeyCols {
		val, ok := rowMap[pkeyCol]
		if !ok {
			return "", fmt.Errorf("pkey column %s not found in row", pkeyCol)
		}
		pkeyParts = append(pkeyParts, fmt.Sprintf("%v", val))
	}
	return strings.Join(pkeyParts, "|"), nil
}

func OrderedMapToMap(om types.OrderedMap) map[string]any {
	m := make(map[string]any, len(om))
	for _, kv := range om {
		m[kv.Key] = kv.Value
	}
	return m
}

func StringifyOrderedMapKey(row types.OrderedMap, pkeyCols []string) (string, error) {
	var pkeyParts []string
	for _, pkeyCol := range pkeyCols {
		val, ok := row.Get(pkeyCol)
		if !ok {
			return "", fmt.Errorf("pkey column %s not found in row", pkeyCol)
		}
		pkeyParts = append(pkeyParts, fmt.Sprintf("%v", val))
	}
	return strings.Join(pkeyParts, "|"), nil
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

func WriteDiffReport(diffResult types.DiffOutput, schema, table, format string) (string, string, error) {
	if len(diffResult.NodeDiffs) == 0 {
		logger.Info("%s TABLES MATCH", CheckMark)
		return "", "", nil
	}

	for _, nodePairDiff := range diffResult.NodeDiffs {
		for nodeName, rows := range nodePairDiff.Rows {
			sort.SliceStable(rows, func(i, j int) bool {
				pkValuesI := getPKValues(rows[i], diffResult.Summary.PrimaryKey)
				pkValuesJ := getPKValues(rows[j], diffResult.Summary.PrimaryKey)
				return comparePKValues(pkValuesI, pkValuesJ) < 0
			})
			nodePairDiff.Rows[nodeName] = rows
		}
	}

	outputPrefix := fmt.Sprintf("%s_%s_diffs-%s",
		strings.ReplaceAll(schema, ".", "_"),
		strings.ReplaceAll(table, ".", "_"),
		time.Now().Format("20060102150405"),
	)
	jsonFileName := outputPrefix + ".json"

	jsonData, err := json.MarshalIndent(diffResult, "", "  ")
	if err != nil {
		logger.Error("ERROR marshalling diff output to JSON: %v", err)
		return "", "", fmt.Errorf("failed to marshal diffs: %w", err)
	}

	if err := os.WriteFile(jsonFileName, jsonData, 0644); err != nil {
		logger.Error("ERROR writing diff output to file %s: %v", jsonFileName, err)
		return "", "", fmt.Errorf("failed to write diffs file: %w", err)
	}

	logger.Warn("%s TABLES DO NOT MATCH", CrossMark)
	for key, diffCount := range diffResult.Summary.DiffRowsCount {
		logger.Warn("Found %d differences between %s", diffCount, key)
	}
	logger.Info("Diff report written to %s", jsonFileName)

	var htmlPath string
	if strings.EqualFold(format, "html") {
		htmlPath, err = writeHTMLDiffReport(diffResult, jsonFileName)
		if err != nil {
			return "", "", err
		}
		if htmlPath != "" {
			logger.Info("HTML diff report written to %s", htmlPath)
		}
	}

	return jsonFileName, htmlPath, nil
}

func getPKValues(row types.OrderedMap, pkey []string) []any {
	var values []any
	for _, keyCol := range pkey {
		for _, kv := range row {
			if kv.Key == keyCol {
				values = append(values, kv.Value)
				break
			}
		}
	}
	return values
}

func comparePKValues(valuesA, valuesB []any) int {
	for i := 0; i < len(valuesA); i++ {
		valA := valuesA[i]
		valB := valuesB[i]

		if valA == nil && valB == nil {
			continue
		}
		if valA == nil {
			return -1 // nil is considered smaller
		}
		if valB == nil {
			return 1
		}

		valAKind := reflect.TypeOf(valA).Kind()
		valBKind := reflect.TypeOf(valB).Kind()

		if isNumeric(valAKind) && isNumeric(valBKind) {
			floatA, errA := toFloat64(valA)
			floatB, errB := toFloat64(valB)

			if errA == nil && errB == nil {
				if floatA < floatB {
					return -1
				}
				if floatA > floatB {
					return 1
				}
				continue
			}
		}

		switch vA := valA.(type) {
		case string:
			if vB, ok := valB.(string); ok {
				if vA < vB {
					return -1
				}
				if vA > vB {
					return 1
				}
			}
		case time.Time:
			if vB, ok := valB.(time.Time); ok {
				if vA.Before(vB) {
					return -1
				}
				if vA.After(vB) {
					return 1
				}
			}
		}

		// Fallback to string comparison for other types or if type assertion fails
		sA := fmt.Sprintf("%v", valA)
		sB := fmt.Sprintf("%v", valB)
		if sA < sB {
			return -1
		}
		if sA > sB {
			return 1
		}
	}
	return 0
}

func isNumeric(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func toFloat64(v any) (float64, error) {
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return val.Float(), nil
	default:
		return 0, fmt.Errorf("unsupported type for numeric conversion: %T", v)
	}
}
