package integration

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/pkg/types"
)

func newTestTableDiffTask(
	t *testing.T,
	qualifiedTableName string,
	nodes []string,
) *core.TableDiffTask {
	task := core.NewTableDiffTask()
	task.ClusterName = "test_cluster"
	task.DBName = dbName
	task.QualifiedTableName = qualifiedTableName
	task.Nodes = strings.Join(nodes, ",")
	task.Output = "json"
	task.BlockSize = 1000
	task.CompareUnitSize = 100
	task.ConcurrencyFactor = 1

	task.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Nodes:             nodes,
			BlockSize:         task.BlockSize,
			CompareUnitSize:   task.CompareUnitSize,
			ConcurrencyFactor: task.ConcurrencyFactor,
			DiffRowsCount:     make(map[string]int),
		},
	}

	return task
}

func newTestTableRepairTask(sourceOfTruthNode, qualifiedTableName, diffFilePath string) *core.TableRepairTask {
	task := core.NewTableRepairTask()
	task.ClusterName = "test_cluster"
	task.DBName = dbName
	task.SourceOfTruth = sourceOfTruthNode
	task.QualifiedTableName = qualifiedTableName
	task.DiffFilePath = diffFilePath
	task.Nodes = "all"
	return task
}

func repairTable(t *testing.T, qualifiedTableName, sourceOfTruthNode string) {
	t.Helper()

	files, err := filepath.Glob("*_diffs-*.json")
	if err != nil {
		t.Fatalf("Failed to find diff files: %v", err)
	}
	if len(files) == 0 {
		log.Println("No diff file found to repair from, skipping repair.")
		return
	}

	sort.Slice(files, func(i, j int) bool {
		fi, errI := os.Stat(files[i])
		if errI != nil {
			t.Logf("Warning: could not stat file %s: %v", files[i], errI)
			return false
		}
		fj, errJ := os.Stat(files[j])
		if errJ != nil {
			t.Logf("Warning: could not stat file %s: %v", files[j], errJ)
			return false
		}
		return fi.ModTime().After(fj.ModTime())
	})

	latestDiffFile := files[0]
	log.Printf("Using latest diff file for repair: %s", latestDiffFile)

	repairTask := newTestTableRepairTask(sourceOfTruthNode, qualifiedTableName, latestDiffFile)

	if err := repairTask.Run(false); err != nil {
		t.Fatalf("Failed to repair table: %v", err)
	}

	log.Printf("Table '%s' repaired successfully using %s as source of truth.", qualifiedTableName, sourceOfTruthNode)
}

func TestTableDiff_NoDifferences(t *testing.T) {
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err := tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err := tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed: %v", err)
	}

	if len(tdTask.DiffResult.NodeDiffs) != 0 {
		t.Errorf(
			"Expected no differences, but got %d node pair diffs. Result: %+v",
			len(tdTask.DiffResult.NodeDiffs),
			tdTask.DiffResult,
		)
	}

	totalDiffRows := 0
	for _, count := range tdTask.DiffResult.Summary.DiffRowsCount {
		totalDiffRows += count
	}
	if totalDiffRows != 0 {
		t.Errorf(
			"Expected 0 total diff rows in summary, got %d. Summary: %+v",
			totalDiffRows,
			tdTask.DiffResult.Summary,
		)
	}

	log.Println("TestTableDiff_NoDifferences completed.")
}

func TestTableDiff_DataOnlyOnNode1(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	// Truncate the table on the second node to create the diff
	_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
	if err != nil {
		t.Fatalf("Failed to truncate table %s on node2: %v", qualifiedTableName, err)
	}

	log.Printf("Data loaded only into %s for table %s", serviceN1, qualifiedTableName)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err = tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err := tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed: %v", err)
	}

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := tdTask.DiffResult.NodeDiffs[pairKey]
	if !ok {
		t.Fatalf(
			"Expected diffs for pair %s, but none found. Result: %+v",
			pairKey,
			tdTask.DiffResult,
		)
	}

	var expectedDiffCount int
	err = pgCluster.Node1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&expectedDiffCount)
	if err != nil {
		t.Fatalf("Failed to count rows in %s on node1: %v", qualifiedTableName, err)
	}

	node1OnlyRows := nodeDiffs.Rows[serviceN1]
	if len(node1OnlyRows) != expectedDiffCount {
		t.Errorf(
			"Expected %d rows only on %s, but got %d",
			expectedDiffCount,
			serviceN1,
			len(node1OnlyRows),
		)
	}

	node2OnlyRows := nodeDiffs.Rows[serviceN2]
	if len(node2OnlyRows) != 0 {
		t.Errorf(
			"Expected 0 rows only on %s, but got %d",
			serviceN2,
			len(node2OnlyRows),
		)
	}

	if tdTask.DiffResult.Summary.DiffRowsCount[pairKey] != expectedDiffCount {
		t.Errorf(
			"Expected summary diff count for pair %s to be %d, got %d. Summary: %+v",
			pairKey,
			expectedDiffCount,
			tdTask.DiffResult.Summary.DiffRowsCount[pairKey],
			tdTask.DiffResult.Summary,
		)
	}
	log.Println("TestTableDiff_DataOnlyOnNode1 completed.")
}

func TestTableDiff_DataOnlyOnNode2(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN2)
	})

	_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
	if err != nil {
		t.Fatalf("Failed to truncate table %s on node1: %v", qualifiedTableName, err)
	}

	log.Printf("Data loaded only into %s for table %s", serviceN2, qualifiedTableName)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err = tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err := tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed: %v", err)
	}

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := tdTask.DiffResult.NodeDiffs[pairKey]
	if !ok {
		t.Fatalf(
			"Expected diffs for pair %s, but none found. Result: %+v",
			pairKey,
			tdTask.DiffResult,
		)
	}

	var expectedDiffCount int
	err = pgCluster.Node2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&expectedDiffCount)
	if err != nil {
		t.Fatalf("Failed to count rows in %s on node2: %v", qualifiedTableName, err)
	}

	node1OnlyRows := nodeDiffs.Rows[serviceN1]
	if len(node1OnlyRows) != 0 {
		t.Errorf(
			"Expected 0 rows only on %s, but got %d",
			serviceN1,
			len(node1OnlyRows),
		)
	}

	node2OnlyRows := nodeDiffs.Rows[serviceN2]
	if len(node2OnlyRows) != expectedDiffCount {
		t.Errorf(
			"Expected %d rows only on %s, but got %d",
			expectedDiffCount,
			serviceN2,
			len(node2OnlyRows),
		)
	}

	if tdTask.DiffResult.Summary.DiffRowsCount[pairKey] != expectedDiffCount {
		t.Errorf(
			"Expected summary diff count for pair %s to be %d, got %d. Summary: %+v",
			pairKey,
			expectedDiffCount,
			tdTask.DiffResult.Summary.DiffRowsCount[pairKey],
			tdTask.DiffResult.Summary,
		)
	}
	log.Println("TestTableDiff_DataOnlyOnNode2 completed.")
}

func TestTableDiff_ModifiedRows(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	modifications := []struct {
		indexVal int
		field    string
		value    string
	}{
		{
			indexVal: 1,
			field:    "email",
			value:    "john.doe.updated@example.com",
		},
		{
			indexVal: 5,
			field:    "first_name",
			value:    "PeterUpdated",
		},
	}

	for _, mod := range modifications {
		updateSQL := fmt.Sprintf(
			"UPDATE %s.%s SET %s = $1 WHERE index = $2",
			testSchema,
			tableName,
			mod.field,
		)
		_, err := pgCluster.Node2Pool.Exec(ctx, updateSQL, mod.value, mod.indexVal)
		if err != nil {
			t.Fatalf(
				"Failed to update row with index %d on node %s: %v",
				mod.indexVal,
				serviceN2,
				err,
			)
		}
	}
	log.Printf(
		"%d rows modified on %s for table %s",
		len(modifications),
		serviceN2,
		qualifiedTableName,
	)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err := tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err = tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed: %v", err)
	}

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := tdTask.DiffResult.NodeDiffs[pairKey]
	if !ok {
		t.Fatalf(
			"Expected diffs for pair %s, but none found. Result: %+v",
			pairKey,
			tdTask.DiffResult,
		)
	}

	if len(nodeDiffs.Rows[serviceN1]) != len(modifications) {
		t.Errorf(
			"Expected %d modified rows to be reported for %s (original values), but got %d. Rows: %+v",
			len(
				modifications,
			),
			serviceN1,
			len(nodeDiffs.Rows[serviceN1]),
			nodeDiffs.Rows[serviceN1],
		)
	}
	if len(nodeDiffs.Rows[serviceN2]) != len(modifications) {
		t.Errorf(
			"Expected %d modified rows to be reported for %s (modified values), but got %d. Rows: %+v",
			len(
				modifications,
			),
			serviceN2,
			len(nodeDiffs.Rows[serviceN2]),
			nodeDiffs.Rows[serviceN2],
		)
	}

	expectedModifiedPKs := len(modifications)
	if tdTask.DiffResult.Summary.DiffRowsCount[pairKey] != expectedModifiedPKs {
		t.Errorf(
			"Expected summary diff count for pair %s to be %d (number of modified PKs), got %d. Summary: %+v",
			pairKey,
			expectedModifiedPKs,
			tdTask.DiffResult.Summary.DiffRowsCount[pairKey],
			tdTask.DiffResult.Summary,
		)
	}

	for _, mod := range modifications {
		found := false
		for _, rowN2 := range nodeDiffs.Rows[serviceN2] {
			if indexVal, ok := rowN2["index"]; ok &&
				indexVal == int32(mod.indexVal) {
				actualModifiedValue := fmt.Sprintf("%v", rowN2[mod.field])
				if actualModifiedValue != mod.value {
					t.Errorf(
						"For Index %d, field %s: expected modified value '%s', got '%s'",
						mod.indexVal,
						mod.field,
						mod.value,
						actualModifiedValue,
					)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf(
				"Modified row with Index %d not found in diff results for node %s",
				mod.indexVal,
				serviceN2,
			)
		}
	}

	log.Println("TestTableDiff_ModifiedRows completed.")
}

func TestTableDiff_MixedCaseIdentifiers(t *testing.T) {
	ctx := context.Background()
	tableName := "CustomersMixedCase"
	qualifiedTableName := fmt.Sprintf("%s.\"%s\"", testSchema, tableName)

	createMixedCaseTableSQL := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS "%s";
CREATE TABLE IF NOT EXISTS %s (
    "ID" INT PRIMARY KEY,
    "FirstName" VARCHAR(100),
    "LastName" VARCHAR(100),
    "EmailAddress" VARCHAR(100)
);`, testSchema, qualifiedTableName)

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, createMixedCaseTableSQL)
		if err != nil {
			t.Fatalf(
				"Failed to create mixed-case table %s on node %s: %v",
				qualifiedTableName,
				nodeName,
				err,
			)
		}
		_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
		if err != nil {
			t.Fatalf(
				"Failed to truncate mixed-case table %s on node %s: %v",
				qualifiedTableName,
				nodeName,
				err,
			)
		}
	}
	log.Printf("Mixed-case table %s created on both nodes", qualifiedTableName)

	commonRows := []map[string]any{
		{
			"ID":           101,
			"FirstName":    "Alice",
			"LastName":     "Smith",
			"EmailAddress": "alice.mixed@example.com",
		},
		{
			"ID":           102,
			"FirstName":    "Bob",
			"LastName":     "Johnson",
			"EmailAddress": "bob.mixed@example.com",
		},
	}

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		for _, row := range commonRows {
			insertSQL := fmt.Sprintf(
				"INSERT INTO %s (\"ID\", \"FirstName\", \"LastName\", \"EmailAddress\") VALUES ($1, $2, $3, $4)",
				qualifiedTableName,
			)
			_, err := pool.Exec(ctx, insertSQL,
				row["ID"], row["FirstName"], row["LastName"], row["EmailAddress"])
			if err != nil {
				t.Fatalf(
					"Failed to insert data into mixed-case table %s on node %s: %v",
					qualifiedTableName,
					nodeName,
					err,
				)
			}
		}
	}
	log.Printf("Data loaded into mixed-case table %s on both nodes", qualifiedTableName)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(
		t,
		fmt.Sprintf("%s.%s", testSchema, tableName),
		nodesToCompare,
	)

	err := tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err = tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed for mixed-case table: %v", err)
	}

	if len(tdTask.DiffResult.NodeDiffs) != 0 {
		t.Errorf(
			"Expected no differences for mixed-case table, but got %d node pair diffs. Result: %+v",
			len(tdTask.DiffResult.NodeDiffs),
			tdTask.DiffResult,
		)
	}
	totalDiffRows := 0
	for _, count := range tdTask.DiffResult.Summary.DiffRowsCount {
		totalDiffRows += count
	}
	if totalDiffRows != 0 {
		t.Errorf(
			"Expected 0 total diff rows in summary for mixed-case table, got %d. Summary: %+v",
			totalDiffRows,
			tdTask.DiffResult.Summary,
		)
	}

	log.Println("TestTableDiff_MixedCaseIdentifiers completed.")
}

func TestTableDiff_VariousDataTypes(t *testing.T) {
	ctx := context.Background()
	tableName := "data_type_test_table"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	createDataTypeTableSQL := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS "%s";
CREATE TABLE IF NOT EXISTS %s.%s (
    id INT PRIMARY KEY,
    col_smallint SMALLINT,
    col_integer INTEGER,
    col_bigint BIGINT,
    col_numeric NUMERIC(10, 2),
    col_real REAL,
    col_double DOUBLE PRECISION,
    col_varchar VARCHAR(100),
    col_text TEXT,
    col_char CHAR(10),
    col_boolean BOOLEAN,
    col_date DATE,
    col_timestamp TIMESTAMP,
    col_timestamptz TIMESTAMPTZ,
    col_jsonb JSONB,
    col_json JSON,
    col_bytea BYTEA,
    col_int_array INT[]
);`, testSchema, testSchema, tableName)

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		_, err := pool.Exec(ctx, createDataTypeTableSQL)
		if err != nil {
			t.Fatalf("Failed to create data_type_test_table on node %s: %v", nodeName, err)
		}
		_, err = pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
		if err != nil {
			t.Fatalf("Failed to truncate data_type_test_table on node %s: %v", nodeName, err)
		}
	}
	log.Printf("Table %s created on both nodes", qualifiedTableName)

	refTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	row1 := map[string]any{
		"id": 1, "col_smallint": int16(10), "col_integer": int32(100), "col_bigint": int64(1000),
		"col_numeric": "123.45", "col_real": float32(1.23), "col_double": float64(4.56789),
		"col_varchar": "varchar_data", "col_text": "text_data", "col_char": "char_data ",
		"col_boolean": true, "col_date": refTime.Format("2006-01-02"),
		"col_timestamp": refTime, "col_timestamptz": refTime,
		"col_jsonb": "{\"key\": \"value1\"}", "col_json": "{\"key\": \"value1\"}",
		"col_bytea": []byte("bytea_data_row1"), "col_int_array": []int32{1, 2, 3},
	}

	row2Node1Only := map[string]any{
		"id": 2, "col_smallint": int16(20), "col_integer": int32(200), "col_bigint": int64(2000),
		"col_varchar": "node1_only_varchar",
	}

	row3Node2Only := map[string]any{
		"id": 3, "col_smallint": int16(30), "col_integer": int32(300), "col_bigint": int64(3000),
		"col_varchar": "node2_only_varchar",
	}

	row4Base := map[string]any{
		"id": 4, "col_smallint": int16(40), "col_integer": int32(400), "col_bigint": int64(4000),
		"col_numeric": "456.78", "col_varchar": "original_varchar_row4",
		"col_jsonb": "{\"status\": \"pending\"}", "col_bytea": []byte("original_bytea_row4"),
	}
	row4Node2Modified := map[string]any{
		"id": 4, "col_smallint": int16(40), "col_integer": int32(400), "col_bigint": int64(4000),
		"col_numeric": "999.99", "col_varchar": "MODIFIED_varchar_row4",
		"col_jsonb": "{\"status\": \"approved\"}", "col_bytea": []byte("MODIFIED_bytea_row4"),
	}

	insertSQLTemplate := `INSERT INTO %s.%s (id, col_smallint, col_integer, col_bigint, col_numeric, col_real, col_double, col_varchar, col_text, col_char, col_boolean, col_date, col_timestamp, col_timestamptz, col_jsonb, col_json, col_bytea, col_int_array) ` +
		`VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

	insertRow := func(pool *pgxpool.Pool, data map[string]any) {
		_, err := pool.Exec(
			ctx,
			fmt.Sprintf(insertSQLTemplate, testSchema, tableName),
			data["id"],
			data["col_smallint"],
			data["col_integer"],
			data["col_bigint"],
			data["col_numeric"],
			data["col_real"],
			data["col_double"],
			data["col_varchar"],
			data["col_text"],
			data["col_char"],
			data["col_boolean"],
			data["col_date"],
			data["col_timestamp"],
			data["col_timestamptz"],
			data["col_jsonb"],
			data["col_json"],
			data["col_bytea"],
			data["col_int_array"],
		)
		if err != nil {
			t.Fatalf("Failed to insert row id %v: %v", data["id"], err)
		}
	}

	insertRow(pgCluster.Node1Pool, row1)
	insertRow(pgCluster.Node2Pool, row1)
	insertRow(pgCluster.Node1Pool, row2Node1Only)
	insertRow(pgCluster.Node2Pool, row3Node2Only)
	insertRow(pgCluster.Node1Pool, row4Base)
	insertRow(pgCluster.Node2Pool, row4Node2Modified)

	log.Printf("Data loaded into %s with variations", qualifiedTableName)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err := tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err := tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed for data type table: %v", err)
	}

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := tdTask.DiffResult.NodeDiffs[pairKey]
	if !ok {
		t.Fatalf(
			"Expected diffs for pair %s, but none found. Result: %+v",
			pairKey,
			tdTask.DiffResult,
		)
	}

	if len(nodeDiffs.Rows[serviceN1]) != 2 {
		t.Errorf(
			"Expected 2 rows in diffs for %s, got %d. Rows: %+v",
			serviceN1,
			len(nodeDiffs.Rows[serviceN1]),
			nodeDiffs.Rows[serviceN1],
		)
	}
	if len(nodeDiffs.Rows[serviceN2]) != 2 {
		t.Errorf(
			"Expected 2 rows in diffs for %s, got %d. Rows: %+v",
			serviceN2,
			len(nodeDiffs.Rows[serviceN2]),
			nodeDiffs.Rows[serviceN2],
		)
	}

	expectedTotalDiffPKs := 3
	if tdTask.DiffResult.Summary.DiffRowsCount[pairKey] != expectedTotalDiffPKs {
		t.Errorf(
			"Expected summary diff count for pair %s to be %d, got %d. Summary: %+v",
			pairKey,
			expectedTotalDiffPKs,
			tdTask.DiffResult.Summary.DiffRowsCount[pairKey],
			tdTask.DiffResult.Summary,
		)
	}

	foundRow2N1Only := false
	for _, r := range nodeDiffs.Rows[serviceN1] {
		if r["id"] == int32(2) {
			foundRow2N1Only = true
			break
		}
	}
	if !foundRow2N1Only {
		t.Errorf("Row with id=2 (N1 only) not found in %s diffs", serviceN1)
	}

	foundRow3N2Only := false
	for _, r := range nodeDiffs.Rows[serviceN2] {
		if r["id"] == int32(3) {
			foundRow3N2Only = true
			break
		}
	}
	if !foundRow3N2Only {
		t.Errorf("Row with id=3 (N2 only) not found in %s diffs", serviceN2)
	}

	foundRow4OriginalN1 := false
	foundRow4ModifiedN2 := false
	for _, r := range nodeDiffs.Rows[serviceN1] {
		if r["id"] == int32(4) && r["col_varchar"] == "original_varchar_row4" {
			foundRow4OriginalN1 = true
		}
	}
	for _, r := range nodeDiffs.Rows[serviceN2] {
		if r["id"] == int32(4) && r["col_varchar"] == "MODIFIED_varchar_row4" {
			foundRow4ModifiedN2 = true
		}
	}
	if !foundRow4OriginalN1 {
		t.Errorf("Original version of row id=4 not found in %s diffs", serviceN1)
	}
	if !foundRow4ModifiedN2 {
		t.Errorf("Modified version of row id=4 not found in %s diffs", serviceN2)
	}

	log.Println("TestTableDiff_VariousDataTypes completed.")
}

func TestTableDiff_TableFiltering(t *testing.T) {
	ctx := context.Background()
	tableName := "customers"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	t.Cleanup(func() {
		repairTable(t, qualifiedTableName, serviceN1)
	})

	updatesNode2 := []struct {
		indexVal int
		field    string
		value    string
	}{
		{
			indexVal: 1,
			field:    "email",
			value:    "mikhailtal@example.com",
		},
		{
			indexVal: 2,
			field:    "email",
			value:    "emmanuel.lasker@example.com",
		},
		{
			indexVal: 3,
			field:    "first_name",
			value:    "Paul Morphy",
		},
	}

	for _, mod := range updatesNode2 {
		updateSQL := fmt.Sprintf(
			"UPDATE %s.%s SET %s = $1 WHERE index = $2",
			testSchema,
			tableName,
			mod.field,
		)
		_, err := pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(true)")
		if err != nil {
			t.Fatalf("Failed to enable spock repair mode on node %s: %v", serviceN2, err)
		}
		_, err = pgCluster.Node2Pool.Exec(ctx, updateSQL, mod.value, mod.indexVal)
		if err != nil {
			t.Fatalf(
				"Failed to update row with index %d on node %s for filter test: %v",
				mod.indexVal,
				serviceN2,
				err,
			)
		}
		_, err = pgCluster.Node2Pool.Exec(ctx, "SELECT spock.repair_mode(false)")
		if err != nil {
			t.Fatalf("Failed to disable spock repair mode on node %s: %v", serviceN2, err)
		}
	}
	log.Printf("Data modified on %s for filter test", serviceN2)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)
	tdTask.TableFilter = "index <= 100"
	tdTask.TaskID = fmt.Sprintf("filter_test_%d", time.Now().UnixNano())

	err := tdTask.RunChecks(false)
	if err != nil {
		t.Fatalf("table-diff validations and checks failed: %v", err)
	}

	if err = tdTask.ExecuteTask(false); err != nil {
		t.Fatalf("ExecuteTask failed for table filtering test: %v", err)
	}

	pairKey := serviceN1 + "/" + serviceN2
	if strings.Compare(serviceN1, serviceN2) > 0 {
		pairKey = serviceN2 + "/" + serviceN1
	}

	nodeDiffs, ok := tdTask.DiffResult.NodeDiffs[pairKey]
	if !ok {
		t.Fatalf(
			"Expected diffs for pair %s, but none found. Result: %+v",
			pairKey,
			tdTask.DiffResult,
		)
	}

	expectedFilteredModifications := 3
	if len(nodeDiffs.Rows[serviceN1]) != expectedFilteredModifications {
		t.Errorf(
			"Expected %d modified rows (original) for %s due to filter, but got %d. Rows: %+v",
			expectedFilteredModifications,
			serviceN1,
			len(nodeDiffs.Rows[serviceN1]),
			nodeDiffs.Rows[serviceN1],
		)
	}
	if len(nodeDiffs.Rows[serviceN2]) != expectedFilteredModifications {
		t.Errorf(
			"Expected %d modified rows (updated) for %s due to filter, but got %d. Rows: %+v",
			expectedFilteredModifications,
			serviceN2,
			len(nodeDiffs.Rows[serviceN2]),
			nodeDiffs.Rows[serviceN2],
		)
	}
	if tdTask.DiffResult.Summary.DiffRowsCount[pairKey] != expectedFilteredModifications {
		t.Errorf(
			"Expected summary diff count for pair %s to be %d (filtered modifications), got %d. Summary: %+v",
			pairKey,
			expectedFilteredModifications,
			tdTask.DiffResult.Summary.DiffRowsCount[pairKey],
			tdTask.DiffResult.Summary,
		)
	}

	foundIndex1 := false
	foundIndex2 := false
	foundIndex3 := false
	for _, row := range nodeDiffs.Rows[serviceN2] {
		indexVal, _ := row["index"].(int32)
		email, _ := row["email"].(string)
		firstName, _ := row["first_name"].(string)

		if indexVal == 1 && email == "mikhailtal@example.com" {
			foundIndex1 = true
		}
		if indexVal == 2 && email == "emmanuel.lasker@example.com" {
			foundIndex2 = true
		}
		if indexVal == 3 && firstName == "Paul Morphy" {
			foundIndex3 = true
		}
	}
	if !foundIndex1 {
		t.Errorf("Expected modified row index 1 not found in filtered diffs")
	}
	if !foundIndex2 {
		t.Errorf("Expected modified row index 2 not found in filtered diffs")
	}
	if !foundIndex3 {
		t.Errorf("Expected modified row index 3 not found in filtered diffs")
	}

	log.Println("TestTableDiff_TableFiltering completed.")
}

func TestTableDiff_ByteaColumnSizeCheck(t *testing.T) {
	ctx := context.Background()
	tableName := "bytea_size_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	createTableSQL := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id INT PRIMARY KEY,
    data BYTEA
);`, qualifiedTableName)

	// Create table on both nodes and add cleanup to drop it
	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		_, err := pool.Exec(ctx, createTableSQL)
		if err != nil {
			t.Fatalf("Failed to create test table %s: %v", qualifiedTableName, err)
		}
	}
	t.Cleanup(func() {
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", qualifiedTableName))
			if err != nil {
				t.Logf("Failed to drop test table %s: %v", qualifiedTableName, err)
			}
		}
	})

	// --- Test Case 1: Data < 1MB (should pass) ---
	t.Run("DataUnder1MB", func(t *testing.T) {
		// Truncate before run
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualifiedTableName))
			if err != nil {
				t.Fatalf("Failed to truncate table %s: %v", qualifiedTableName, err)
			}
		}

		smallData := make([]byte, 500*1024) // 500 KB
		_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, $1)", qualifiedTableName), smallData)
		if err != nil {
			t.Fatalf("Failed to insert small data: %v", err)
		}
		_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, $1)", qualifiedTableName), smallData)
		if err != nil {
			t.Fatalf("Failed to insert small data: %v", err)
		}

		nodesToCompare := []string{serviceN1, serviceN2}
		tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

		err = tdTask.RunChecks(false)
		if err != nil {
			t.Errorf("RunChecks should succeed for bytea data < 1MB, but got error: %v", err)
		}
	})

	// --- Test Case 2: Data > 1MB (should fail) ---
	t.Run("DataOver1MB", func(t *testing.T) {
		// Truncate before run
		for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
			_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", qualifiedTableName))
			if err != nil {
				t.Fatalf("Failed to truncate table %s: %v", qualifiedTableName, err)
			}
		}
		largeData := make([]byte, 1024*1024+1) // > 1 MB
		_, err := pgCluster.Node1Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, $1)", qualifiedTableName), largeData)
		if err != nil {
			t.Fatalf("Failed to insert large data: %v", err)
		}
		_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("INSERT INTO %s (id, data) VALUES (1, $1)", qualifiedTableName), largeData)
		if err != nil {
			t.Fatalf("Failed to insert large data: %v", err)
		}

		nodesToCompare := []string{serviceN1, serviceN2}
		tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

		err = tdTask.RunChecks(false)
		if err == nil {
			t.Fatal("RunChecks should fail for bytea data > 1MB, but it succeeded")
		}
		if !strings.Contains(err.Error(), "refusing to perform table-diff") {
			t.Errorf("Error message should contain 'refusing to perform table-diff', but it was: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "is larger than 1 MB") {
			t.Errorf("Error message should contain 'is larger than 1 MB', but it was: %s", err.Error())
		}
	})
}
