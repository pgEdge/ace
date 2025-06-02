package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/types"
)

type TableRepairTask struct {
	types.Task
	types.DerivedFields

	QualifiedTableName string
	DBName             string
	Nodes              string

	DiffFilePath  string
	SourceOfTruth string

	QuietMode      bool
	DryRun         bool // TBD
	InsertOnly     bool
	UpsertOnly     bool
	FireTriggers   bool
	GenerateReport bool // TBD
	FixNulls       bool // TBD
	Bidirectional  bool // TBD

	InvokeMethod string // TBD
	ClientRole   string // TBD

	Pools map[string]*pgxpool.Pool

	rawDiffs types.DiffOutput
	mu       sync.Mutex
}

// Defining these getters and setters to satisfy ClusterConfigProvider interface
func (tr *TableRepairTask) GetClusterName() string              { return tr.ClusterName }
func (tr *TableRepairTask) GetDBName() string                   { return tr.DBName }
func (tr *TableRepairTask) SetDBName(name string)               { tr.DBName = name }
func (tr *TableRepairTask) GetNodes() string                    { return tr.Nodes }
func (tr *TableRepairTask) GetNodeList() []string               { return tr.NodeList }
func (tr *TableRepairTask) SetNodeList(nl []string)             { tr.NodeList = nl }
func (tr *TableRepairTask) SetDatabase(db types.Database)       { tr.Database = db }
func (tr *TableRepairTask) GetClusterNodes() []map[string]any   { return tr.ClusterNodes }
func (tr *TableRepairTask) SetClusterNodes(cn []map[string]any) { tr.ClusterNodes = cn }

func NewTableRepairTask() *TableRepairTask {
	return &TableRepairTask{
		InvokeMethod: "cli",
		Pools:        make(map[string]*pgxpool.Pool),
		DerivedFields: types.DerivedFields{
			HostMap: make(map[string]string),
		},
	}
}

func (tr *TableRepairTask) checkRepairOptionsCompatibility() error {
	incompatibleOptions := []struct {
		condition bool
		message   string
	}{
		{tr.Bidirectional && tr.UpsertOnly, "bidirectional and upsert_only cannot be used together"},
		{tr.Bidirectional && tr.FixNulls, "bidirectional and fix_nulls cannot be used together"},
		{tr.FixNulls && tr.InsertOnly, "insert_only and fix_nulls cannot be used together"},
		{tr.FixNulls && tr.UpsertOnly, "upsert_only and fix_nulls cannot be used together"},
		{tr.InsertOnly && tr.UpsertOnly, "insert_only and upsert_only cannot be used together"},
	}

	for _, rule := range incompatibleOptions {
		if rule.condition {
			return fmt.Errorf(rule.message)
		}
	}
	return nil
}

func (tr *TableRepairTask) checkIfSourceOfTruthIsNeeded() bool {
	casesNotNeeded := []struct {
		condition bool
		needed    bool
	}{
		{tr.FixNulls, false},
		{tr.Bidirectional && tr.InsertOnly, false},
	}

	for _, rule := range casesNotNeeded {
		if rule.condition {
			return rule.needed
		}
	}

	return true
}

func (t *TableRepairTask) ValidateAndPrepare() error {
	if t.ClusterName == "" {
		return fmt.Errorf("cluster_name is required")
	}
	if t.QualifiedTableName == "" {
		return fmt.Errorf("table_name is required")
	}
	if t.DiffFilePath == "" {
		return fmt.Errorf("diff_file_path is required")
	}

	if err := t.checkRepairOptionsCompatibility(); err != nil {
		return fmt.Errorf("repair options are incompatible: %w", err)
	}

	if !t.checkIfSourceOfTruthIsNeeded() {
		return fmt.Errorf("source_of_truth is required unless --fix-nulls or --bidirectional is specified")
	}

	parts := strings.Split(t.QualifiedTableName, ".")
	if len(parts) != 2 {
		return fmt.Errorf("table_name must be in schema.table format, got: %s", t.QualifiedTableName)
	}
	t.Schema = strings.TrimSpace(parts[0])
	t.Table = strings.TrimSpace(parts[1])

	if t.Schema == "" || t.Table == "" {
		return fmt.Errorf("schema and table name parts cannot be empty in %s", t.QualifiedTableName)
	}

	if err := readClusterInfo(t); err != nil {
		return fmt.Errorf("failed to read cluster info: %w", err)
	}

	// TODO: Revisit to optimise
	for _, nodeInfo := range t.ClusterNodes {
		hostname, okHostname := nodeInfo["Name"].(string)
		publicIP, okPublicIP := nodeInfo["PublicIP"].(string)
		port, okPort := nodeInfo["Port"].(string) // From JSON, often float64
		if !okHostname || !okPublicIP || !okPort {
			log.Printf("Warning: Skipping node with incomplete info: %+v", nodeInfo)
			continue
		}
		t.HostMap[fmt.Sprintf("%s:%s", publicIP, port)] = hostname
	}

	foundSourceOfTruth := false
	if t.SourceOfTruth != "" {
		for _, nodeInfo := range t.ClusterNodes {
			if name, ok := nodeInfo["Name"].(string); ok && name == t.SourceOfTruth {
				foundSourceOfTruth = true
				break
			}
		}
		if !foundSourceOfTruth {
			return fmt.Errorf("source_of_truth node '%s' not found in cluster '%s' or is not active", t.SourceOfTruth, t.ClusterName)
		}
	}

	diffData, err := os.ReadFile(t.DiffFilePath)
	if err != nil {
		return fmt.Errorf("failed to read diff file %s: %w", t.DiffFilePath, err)
	}
	if err := json.Unmarshal(diffData, &t.rawDiffs); err != nil {
		return fmt.Errorf("failed to unmarshal diff file %s: %w", t.DiffFilePath, err)
	}

	if t.rawDiffs.NodeDiffs == nil {
		return fmt.Errorf("invalid diff file format: missing 'diffs' field or it's not a map")
	}

	involvedNodeNames := make(map[string]bool)
	for nodePairKey := range t.rawDiffs.NodeDiffs {
		nodesInPair := strings.Split(nodePairKey, "/")
		if len(nodesInPair) != 2 {
			return fmt.Errorf("invalid node pair key in diff file: %s", nodePairKey)
		}
		involvedNodeNames[nodesInPair[0]] = true
		involvedNodeNames[nodesInPair[1]] = true
	}

	var clusterNodes []map[string]any
	for _, nodeMap := range t.ClusterNodes {
		if len(t.NodeList) > 0 {
			nameVal, _ := nodeMap["Name"].(string)
			if !Contains(t.NodeList, nameVal) {
				continue
			}
		}
		combinedMap := make(map[string]any)

		maps.Copy(combinedMap, nodeMap)

		combinedMap["DBName"] = t.Database.DBName
		combinedMap["DBUser"] = t.Database.DBUser
		combinedMap["DBPassword"] = t.Database.DBPassword

		clusterNodes = append(clusterNodes, combinedMap)
	}

	t.ClusterNodes = clusterNodes

	var connectedNodes []map[string]any
	// Repair needs these privileges. Perhaps we can pare this down depending
	// on the repair options, but for now we'll keep it as is.
	requiredPrivileges := []string{"SELECT", "INSERT", "UPDATE", "DELETE"}

	for _, nodeInfo := range t.ClusterNodes {
		nodeName, _ := nodeInfo["Name"].(string)
		if nodeName == t.SourceOfTruth || involvedNodeNames[nodeName] {
			connPool, err := auth.GetClusterNodeConnection(nodeInfo, t.ClientRole)
			if err != nil {
				log.Printf("Warning: Failed to connect to node %s: %v. Will attempt to proceed if it's not critical or SoT.", nodeName, err)
				if nodeName == t.SourceOfTruth {
					return fmt.Errorf("failed to connect to source_of_truth node %s: %w", nodeName, err)
				}
				continue
			}
			t.Pools[nodeName] = connPool
			connectedNodes = append(connectedNodes, nodeInfo)

			cols, err := GetColumns(connPool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to get columns for %s.%s on node %s: %w", t.Schema, t.Table, nodeName, err)
			}
			t.Cols = cols

			pKey, err := GetPrimaryKey(connPool, t.Schema, t.Table)
			if err != nil {
				return fmt.Errorf("failed to get primary key for %s.%s on node %s: %w", t.Schema, t.Table, nodeName, err)
			}
			if len(pKey) == 0 {
				return fmt.Errorf("no primary key found for %s.%s on node %s", t.Schema, t.Table, nodeName)
			}
			t.Key = pKey
			t.SimplePrimaryKey = len(pKey) == 1

			publicIP, _ := nodeInfo["PublicIP"].(string)
			port, _ := nodeInfo["Port"].(string)
			colTypes, err := GetColumnTypes(connPool, t.Table)
			if err != nil {
				return fmt.Errorf("failed to get column types for %s on node %s: %w", t.Table, nodeName, err)
			}
			if t.ColTypes == nil {
				t.ColTypes = make(map[string]map[string]string)
			}
			t.ColTypes[fmt.Sprintf("%s:%s", publicIP, port)] = colTypes

			dbUser, _ := nodeInfo["DBUser"].(string)
			if dbUser == "" {
				dbUser = t.Database.DBUser
			}

			authorized, missingPrivsMap, err := CheckUserPrivileges(connPool, dbUser, t.Schema, t.Table, requiredPrivileges)
			if err != nil {
				return fmt.Errorf("failed to check user privileges on node %s: %w", nodeName, err)
			}
			if !authorized {
				var missingPrivs []string
				for priv, present := range missingPrivsMap {
					if !present {
						missingPrivs = append(missingPrivs, strings.Replace(priv, "table_", "", 1))
					}
				}
				return fmt.Errorf("user '%s' on node '%s' is missing privileges: %s for table %s.%s",
					dbUser, nodeName, strings.Join(missingPrivs, ", "), t.Schema, t.Table)
			}
		}
	}

	if len(involvedNodeNames) == 0 {
		return fmt.Errorf("failed to connect to any relevant node to verify schema or permissions")
	}
	if t.SourceOfTruth != "" && t.Pools[t.SourceOfTruth] == nil {
		return fmt.Errorf("failed to establish a connection to the source_of_truth node: %s", t.SourceOfTruth)
	}

	log.Println("Table repair task validated and prepared successfully.")
	return nil
}

func (t *TableRepairTask) Run(skipValidation bool) error {
	if !skipValidation {
		if err := t.ValidateAndPrepare(); err != nil {
			return fmt.Errorf("task validation and preparation failed: %w", err)
		}
	}

	defer func() {
		for nodeName, pool := range t.Pools {
			if pool != nil {
				pool.Close()
				log.Printf("Closed connection pool for node: %s", nodeName)
			}
		}
	}()

	startTime := time.Now()
	log.Printf("Starting table repair for %s on cluster %s", t.QualifiedTableName, t.ClusterName)

	// Core repair logic begins here
	totalOps := make(map[string]map[string]int) // node -> "upserted"/"deleted" -> count
	var repairErrors []string

	divergentNodes := make(map[string]bool)
	fullUpserts, fullDeletes, err := calculateRepairSets(t)
	if err != nil {
		return fmt.Errorf("failed to calculate repair sets: %w", err)
	}
	for nodeName := range fullUpserts {
		divergentNodes[nodeName] = true
		totalOps[nodeName] = map[string]int{"upserted": 0, "deleted": 0}
	}
	for nodeName := range fullDeletes {
		divergentNodes[nodeName] = true
		if _, exists := totalOps[nodeName]; !exists {
			totalOps[nodeName] = map[string]int{"upserted": 0, "deleted": 0}
		}
	}

	/* NOTE: We will be skipping checking the spock version entirely since
	 * most users are on spock 4.0 or above.
	 */

	for nodeName := range divergentNodes {
		log.Printf("Processing repairs for divergent node: %s", nodeName)
		divergentPool, ok := t.Pools[nodeName]
		if !ok || divergentPool == nil {
			log.Printf("Error: Connection pool for divergent node %s not found or not connected. Skipping repairs for this node.", nodeName)
			repairErrors = append(repairErrors, fmt.Sprintf("no connection to %s", nodeName))
			continue
		}

		tx, err := divergentPool.Begin(context.Background())
		if err != nil {
			log.Printf("Error starting transaction on node %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("tx begin failed for %s: %v", nodeName, err))
			continue
		}

		var spockRepairModeActive bool = false
		_, err = tx.Exec(context.Background(), "SELECT spock.repair_mode(true)")
		if err != nil {
			tx.Rollback(context.Background())
			log.Printf("Error enabling spock.repair_mode(true) on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(true) failed for %s: %v", nodeName, err))
			continue
		}
		spockRepairModeActive = true
		log.Printf("spock.repair_mode(true) set on %s", nodeName)

		if t.FireTriggers {
			_, err = tx.Exec(context.Background(), "SET session_replication_role = 'local'")
		} else {
			_, err = tx.Exec(context.Background(), "SET session_replication_role = 'replica'")
		}
		if err != nil {
			tx.Rollback(context.Background())
			log.Printf("Error setting session_replication_role on %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("session_replication_role failed for %s: %v", nodeName, err))
			continue
		}
		log.Printf("session_replication_role set on %s (fire_triggers: %v)", nodeName, t.FireTriggers)

		// TODO: DROP PRIVILEGES HERE!

		// Process deletes first
		if !t.UpsertOnly && !t.InsertOnly {
			nodeDeletes := fullDeletes[nodeName]
			if len(nodeDeletes) > 0 {
				deletedCount, err := executeDeletes(tx, t, nodeDeletes)
				if err != nil {
					tx.Rollback(context.Background())
					log.Printf("Error executing deletes on node %s: %v", nodeName, err)
					repairErrors = append(repairErrors, fmt.Sprintf("delete ops failed for %s: %v", nodeName, err))
					continue
				}
				totalOps[nodeName]["deleted"] = deletedCount
				log.Printf("Executed %d delete operations on %s", deletedCount, nodeName)
			}
		}

		// And now for the upserts
		nodeUpserts := fullUpserts[nodeName]
		if len(nodeUpserts) > 0 {
			targetNodeHostPortKey := ""
			for hostPort, mappedName := range t.HostMap {
				if mappedName == nodeName {
					targetNodeHostPortKey = hostPort
					break
				}
			}
			if targetNodeHostPortKey == "" {
				tx.Rollback(context.Background())
				errStr := fmt.Sprintf("could not find host:port key for target node %s to get col types", nodeName)
				log.Printf("Error: %s", errStr)
				repairErrors = append(repairErrors, errStr)
				continue
			}
			targetNodeColTypes, ok := t.ColTypes[targetNodeHostPortKey]
			if !ok {
				tx.Rollback(context.Background())
				errStr := fmt.Sprintf("column types for target node '%s' (key: %s) not found for upserts", nodeName, targetNodeHostPortKey)
				log.Printf("Error: %s", errStr)
				repairErrors = append(repairErrors, errStr)
				continue
			}

			upsertedCount, err := executeUpserts(tx, t, nodeUpserts, targetNodeColTypes)
			if err != nil {
				tx.Rollback(context.Background())
				log.Printf("Error executing upserts on node %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("upsert ops failed for %s: %v", nodeName, err))
				continue
			}
			totalOps[nodeName]["upserted"] = upsertedCount
			log.Printf("Executed %d upsert operations on %s", upsertedCount, nodeName)
		}

		if spockRepairModeActive {
			// TODO: Need to elevate privileges here, but might be difficult
			// with pgx transactions and connection pooling.
			_, err = tx.Exec(context.Background(), "SELECT spock.repair_mode(false)")
			if err != nil {
				tx.Rollback(context.Background())
				log.Printf("Error disabling spock.repair_mode(false) on %s: %v", nodeName, err)
				repairErrors = append(repairErrors, fmt.Sprintf("spock.repair_mode(false) failed for %s: %v", nodeName, err))
				continue
			}
			log.Printf("spock.repair_mode(false) set on %s", nodeName)
		}

		err = tx.Commit(context.Background())
		if err != nil {
			log.Printf("Error committing transaction on node %s: %v", nodeName, err)
			repairErrors = append(repairErrors, fmt.Sprintf("commit failed for %s: %v", nodeName, err))
			continue
		}
		log.Printf("Transaction committed successfully on %s", nodeName)
	}

	if len(repairErrors) > 0 {
		log.Printf("Table repair for %s finished with errors: %s", t.QualifiedTableName, strings.Join(repairErrors, "; "))
		t.TaskStatus = "FAILED"
		t.TaskContext = strings.Join(repairErrors, "; ")
	} else {
		log.Printf("Table repair for %s completed successfully.", t.QualifiedTableName)
		t.TaskStatus = "COMPLETED"
		summary := strings.Builder{}
		for node, ops := range totalOps {
			summary.WriteString(fmt.Sprintf("Node %s: %d upserted, %d deleted. ", node, ops["upserted"], ops["deleted"]))
		}
		t.TaskContext = strings.TrimSpace(summary.String())
	}

	log.Printf("Total operations: %v", totalOps)

	log.Println("*** SUMMARY ***")
	for nodeName := range divergentNodes {
		ops := totalOps[nodeName]
		if t.InsertOnly {
			log.Printf("%s INSERTED = %d rows", nodeName, ops["upserted"])
		} else {
			log.Printf("%s UPSERTED = %d rows", nodeName, ops["upserted"])
		}
	}
	fmt.Println()
	if !t.UpsertOnly && !t.InsertOnly {
		for nodeName := range divergentNodes {
			ops := totalOps[nodeName]
			log.Printf("%s DELETED = %d rows", nodeName, ops["deleted"])
		}
	}

	t.FinishedAt = time.Now()
	t.TimeTaken = t.FinishedAt.Sub(startTime).Seconds()
	log.Printf("RUN TIME = %.2f seconds", t.TimeTaken)

	// TODO: Update task metrics in a local DB
	return nil
}

// executeDeletes handles deleting rows in batches.
func executeDeletes(tx pgx.Tx, task *TableRepairTask, deletes map[string]map[string]any) (int, error) {
	keysToDelete := make([]any, 0, len(deletes))

	for pkeyString := range deletes {
		rowMap := deletes[pkeyString]
		if task.SimplePrimaryKey {
			pkeyValue, ok := rowMap[task.Key[0]]
			if !ok {
				return 0, fmt.Errorf("primary key column %s not found in row data for pkey string %s", task.Key[0], pkeyString)
			}
			keysToDelete = append(keysToDelete, pkeyValue)
		} else {
			compositeKey := make([]any, len(task.Key))
			for i, keyCol := range task.Key {
				pkeyValue, ok := rowMap[keyCol]
				if !ok {
					return 0, fmt.Errorf("composite primary key column %s not found in row data for pkey string %s", keyCol, pkeyString)
				}
				compositeKey[i] = pkeyValue
			}
			keysToDelete = append(keysToDelete, compositeKey)
		}
	}

	if len(keysToDelete) == 0 {
		return 0, nil
	}

	totalDeletedCount := 0
	// TODO: Make this configurable
	batchSize := 1000

	tableIdent := pgx.Identifier{task.Schema, task.Table}.Sanitize()

	for i := 0; i < len(keysToDelete); i += batchSize {
		end := i + batchSize
		if end > len(keysToDelete) {
			end = len(keysToDelete)
		}
		batchKeys := keysToDelete[i:end]

		var deleteSQL strings.Builder
		args := []interface{}{}
		paramIdx := 1

		deleteSQL.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", tableIdent))

		if task.SimplePrimaryKey {
			deleteSQL.WriteString(fmt.Sprintf("%s IN (", pgx.Identifier{task.Key[0]}.Sanitize()))
			for j, key := range batchKeys {
				if j > 0 {
					deleteSQL.WriteString(", ")
				}
				deleteSQL.WriteString(fmt.Sprintf("$%d", paramIdx))
				args = append(args, key)
				paramIdx++
			}
			deleteSQL.WriteString(")")
		} else {
			keyColSanitised := make([]string, len(task.Key))
			for k, keyCol := range task.Key {
				keyColSanitised[k] = pgx.Identifier{keyCol}.Sanitize()
			}

			deleteSQL.WriteString(fmt.Sprintf("(%s) IN (", strings.Join(keyColSanitised, ", ")))

			for j, key := range batchKeys {
				compositeKey, ok := key.([]any)
				if !ok {
					return 0, fmt.Errorf("expected composite key to be []interface{}, got %T", key)
				}
				if len(compositeKey) != len(task.Key) {
					return 0, fmt.Errorf("composite key length mismatch: expected %d, got %d", len(task.Key), len(compositeKey))
				}
				if j > 0 {
					deleteSQL.WriteString(", ")
				}
				deleteSQL.WriteString("(")
				for k, val := range compositeKey {
					if k > 0 {
						deleteSQL.WriteString(", ")
					}
					deleteSQL.WriteString(fmt.Sprintf("$%d", paramIdx))
					args = append(args, val)
					paramIdx++
				}
				deleteSQL.WriteString(")")
			}
			deleteSQL.WriteString(")")
		}

		cmdTag, err := tx.Exec(context.Background(), deleteSQL.String(), args...)
		if err != nil {
			return totalDeletedCount, fmt.Errorf("error executing delete batch: %w (SQL: %s, Args: %v)", err, deleteSQL.String(), args)
		}
		totalDeletedCount += int(cmdTag.RowsAffected())
	}

	return totalDeletedCount, nil
}

// executeUpserts handles upserting rows in batches.
func executeUpserts(tx pgx.Tx, task *TableRepairTask, upserts map[string]map[string]any, colTypes map[string]string) (int, error) {
	rowsToUpsert := make([][]any, 0, len(upserts))
	orderedCols := task.Cols

	for _, rowMap := range upserts {
		typedRow := make([]any, len(orderedCols))
		for i, colName := range orderedCols {
			val, valExists := rowMap[colName]
			pgType, typeExists := colTypes[colName]

			if !valExists {
				typedRow[i] = nil
				continue
			}
			if !typeExists {
				return 0, fmt.Errorf("type for column %s not found in target node's colTypes", colName)
			}

			convertedVal, err := convertToPgxType(val, pgType)
			if err != nil {
				return 0, fmt.Errorf("error converting value for column %s (value: %v, type: %s): %w", colName, val, pgType, err)
			}
			typedRow[i] = convertedVal
		}
		rowsToUpsert = append(rowsToUpsert, typedRow)
	}

	if len(rowsToUpsert) == 0 {
		return 0, nil
	}

	totalUpsertedCount := 0
	// TODO: Make this configurable
	batchSize := 1000

	// For the max placeholders issue
	if len(orderedCols) > 0 && batchSize*len(orderedCols) > 65500 {
		batchSize = 65500 / len(orderedCols)
		if batchSize == 0 {
			batchSize = 1
		}
	}

	tableIdent := pgx.Identifier{task.Schema, task.Table}.Sanitize()
	colIdents := make([]string, len(orderedCols))
	for i, col := range orderedCols {
		colIdents[i] = pgx.Identifier{col}.Sanitize()
	}
	colsSQL := strings.Join(colIdents, ", ")

	pkColIdents := make([]string, len(task.Key))
	for i, pkCol := range task.Key {
		pkColIdents[i] = pgx.Identifier{pkCol}.Sanitize()
	}
	pkSQL := strings.Join(pkColIdents, ", ")

	for i := 0; i < len(rowsToUpsert); i += batchSize {
		end := i + batchSize
		if end > len(rowsToUpsert) {
			end = len(rowsToUpsert)
		}
		batchRows := rowsToUpsert[i:end]

		var upsertSQL strings.Builder
		args := []interface{}{}
		paramIdx := 1

		upsertSQL.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tableIdent, colsSQL))
		for j, row := range batchRows {
			if j > 0 {
				upsertSQL.WriteString(", ")
			}
			upsertSQL.WriteString("(")
			for k, val := range row {
				if k > 0 {
					upsertSQL.WriteString(", ")
				}
				upsertSQL.WriteString(fmt.Sprintf("$%d", paramIdx))
				args = append(args, val)
				paramIdx++
			}
			upsertSQL.WriteString(")")
		}

		upsertSQL.WriteString(fmt.Sprintf(" ON CONFLICT (%s) ", pkSQL))
		if task.InsertOnly {
			upsertSQL.WriteString("DO NOTHING")
		} else {
			upsertSQL.WriteString("DO UPDATE SET ")
			setClauses := make([]string, 0, len(orderedCols))
			for _, col := range orderedCols {
				isPkCol := false
				for _, pk := range task.Key {
					if col == pk {
						isPkCol = true
						break
					}
				}
				if !isPkCol {
					sanitisedCol := pgx.Identifier{col}.Sanitize()
					setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", sanitisedCol, sanitisedCol))
				}
			}
			upsertSQL.WriteString(strings.Join(setClauses, ", "))
		}

		cmdTag, err := tx.Exec(context.Background(), upsertSQL.String(), args...)
		if err != nil {
			return totalUpsertedCount, fmt.Errorf("error executing upsert batch: %w (SQL: %s, Args: %v)", err, upsertSQL.String(), args)
		}
		totalUpsertedCount += int(cmdTag.RowsAffected())
	}

	return totalUpsertedCount, nil
}

// getDryRunOutput generates the dry run message string.
// TODO: This is not fully implemented yet.
func getDryRunOutput(task *TableRepairTask) (string, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Dry run for table %s:\n", task.QualifiedTableName))

	fullUpserts, fullDeletes, err := calculateRepairSets(task)
	if err != nil {
		sb.WriteString(fmt.Sprintf("Error calculating changes for dry run: %v\n", err))
		return sb.String(), err
	}

	for nodeName, upserts := range fullUpserts {
		deletes := fullDeletes[nodeName]
		if !task.UpsertOnly && !task.InsertOnly {
			sb.WriteString(fmt.Sprintf("  Node %s: Would attempt to upsert %d rows and delete %d rows.\n", nodeName, len(upserts), len(deletes)))
		} else if task.InsertOnly {
			sb.WriteString(fmt.Sprintf("  Node %s: Would attempt to insert %d rows.\n", nodeName, len(upserts)))
			if len(deletes) > 0 {
				sb.WriteString(fmt.Sprintf("    Additionally, %d rows exist on %s not present on %s (deletes skipped).\n", len(deletes), nodeName, task.SourceOfTruth))
			}
		} else { // UpsertOnly
			sb.WriteString(fmt.Sprintf("  Node %s: Would attempt to upsert %d rows.\n", nodeName, len(upserts)))
			if len(deletes) > 0 {
				sb.WriteString(fmt.Sprintf("    Additionally, %d rows exist on %s not present on %s (deletes skipped).\n", len(deletes), nodeName, task.SourceOfTruth))
			}
		}
	}

	return sb.String(), nil
}

func calculateRepairSets(task *TableRepairTask) (map[string]map[string]map[string]any, map[string]map[string]map[string]any, error) {
	fullRowsToUpsert := make(map[string]map[string]map[string]any) // nodeName -> string(pkey) -> rowData
	fullRowsToDelete := make(map[string]map[string]map[string]any) // nodeName -> string(pkey) -> rowData

	if task.SourceOfTruth == "" {
		return nil, nil, fmt.Errorf("source_of_truth must be set to calculate repair sets")
	}

	for nodePair, diffs := range task.rawDiffs.NodeDiffs {
		nodes := strings.Split(nodePair, "/")
		node1Name := nodes[0]
		node2Name := nodes[1]

		var sourceRows []map[string]any
		var targetRows []map[string]any
		var targetNode string

		if node1Name == task.SourceOfTruth {
			sourceRows = diffs.Rows[node1Name]
			targetRows = diffs.Rows[node2Name]
			targetNode = node2Name
		} else if node2Name == task.SourceOfTruth {
			sourceRows = diffs.Rows[node2Name]
			targetRows = diffs.Rows[node1Name]
			targetNode = node1Name
		} else {
			continue
		}

		if fullRowsToUpsert[targetNode] == nil {
			fullRowsToUpsert[targetNode] = make(map[string]map[string]any)
		}
		if fullRowsToDelete[targetNode] == nil {
			fullRowsToDelete[targetNode] = make(map[string]map[string]any)
		}

		sourceRowsByPKey := make(map[string]map[string]any)
		for _, row := range sourceRows {
			pkeyStr, err := stringifyPKey(row, task.Key, task.SimplePrimaryKey)
			if err != nil {
				return nil, nil, fmt.Errorf("error stringifying pkey for source row on %s: %w", task.SourceOfTruth, err)
			}
			sourceRowsByPKey[pkeyStr] = row
			fullRowsToUpsert[targetNode][pkeyStr] = row
		}

		targetRowsByPKey := make(map[string]map[string]any)
		for _, row := range targetRows {
			pkeyStr, err := stringifyPKey(row, task.Key, task.SimplePrimaryKey)
			if err != nil {
				return nil, nil, fmt.Errorf("error stringifying pkey for target row on %s: %w", targetNode, err)
			}
			targetRowsByPKey[pkeyStr] = row
			if _, existsInSource := sourceRowsByPKey[pkeyStr]; !existsInSource {
				fullRowsToDelete[targetNode][pkeyStr] = row
			}
		}
	}
	return fullRowsToUpsert, fullRowsToDelete, nil
}
