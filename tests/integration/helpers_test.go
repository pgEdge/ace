// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package integration

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/repair"
	"github.com/stretchr/testify/require"
)

func createTestTable(ctx context.Context, pool *pgxpool.Pool, schemaName, tableName string) error {
	createTableSQL := fmt.Sprintf(`
CREATE SCHEMA IF NOT EXISTS "%s";
CREATE TABLE IF NOT EXISTS "%s"."%s" (
    index INT PRIMARY KEY,
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    company TEXT,
    city TEXT,
    country TEXT,
    phone_1 TEXT,
    phone_2 TEXT,
    email TEXT,
    subscription_date TIMESTAMP,
    website TEXT
);`, schemaName, schemaName, tableName)

	_, err := pool.Exec(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create table %s.%s: %w", schemaName, tableName, err)
	}
	log.Printf("Table %s.%s created successfully or already exists.", schemaName, tableName)
	return nil
}

func alterTableToCompositeKey(ctx context.Context, pool *pgxpool.Pool, schemaName, tableName string) error {
	alterSQL := fmt.Sprintf(`
		ALTER TABLE "%s"."%s" DROP CONSTRAINT IF EXISTS customers_pkey;
		ALTER TABLE "%s"."%s" ADD PRIMARY KEY (index, customer_id);
	`, schemaName, tableName, schemaName, tableName)

	_, err := pool.Exec(ctx, alterSQL)
	if err != nil {
		return fmt.Errorf("failed to alter table %s.%s to composite key: %w", schemaName, tableName, err)
	}
	log.Printf("Table %s.%s altered to composite key.", schemaName, tableName)
	return nil
}

func revertTableToSimpleKey(ctx context.Context, pool *pgxpool.Pool, schemaName, tableName string) error {
	alterSQL := fmt.Sprintf(`
		ALTER TABLE "%s"."%s" DROP CONSTRAINT IF EXISTS customers_pkey;
		ALTER TABLE "%s"."%s" ALTER COLUMN customer_id DROP NOT NULL;
		ALTER TABLE "%s"."%s" ADD PRIMARY KEY (index);
	`, schemaName, tableName, schemaName, tableName, schemaName, tableName)

	_, err := pool.Exec(ctx, alterSQL)
	if err != nil {
		return fmt.Errorf("failed to revert table %s.%s to simple key: %w", schemaName, tableName, err)
	}
	log.Printf("Table %s.%s reverted to simple key.", schemaName, tableName)
	return nil
}

func materializedViewExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schemaName, viewName string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 
			FROM pg_matviews 
			WHERE schemaname = $1 AND matviewname = $2
		)`, schemaName, viewName).Scan(&exists)
	require.NoError(t, err)
	return exists
}

func loadDataFromCSV(
	ctx context.Context,
	pool *pgxpool.Pool,
	schemaName, tableName, csvFilePath string,
) error {
	file, err := os.Open(csvFilePath)
	if err != nil {
		return fmt.Errorf("failed to open CSV file %s: %w", csvFilePath, err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		if err == io.EOF {
			return fmt.Errorf("CSV file %s is empty or has no header row", csvFilePath)
		}
		return fmt.Errorf("failed to read header from CSV %s: %w", csvFilePath, err)
	}

	mappedDBHeaders := make([]string, 0, len(headers))
	originalHeaderIndices := make([]int, 0, len(headers))

	headerToDBMap := map[string]string{
		"index":             "index",
		"customer id":       "customer_id",
		"first name":        "first_name",
		"last name":         "last_name",
		"company":           "company",
		"city":              "city",
		"country":           "country",
		"phone 1":           "phone_1",
		"phone_1":           "phone_1",
		"phone 2":           "phone_2",
		"phone_2":           "phone_2",
		"email":             "email",
		"subscription date": "subscription_date",
		"website":           "website",
	}

	for i, h := range headers {
		csvHeaderKey := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(h), "_", " "))

		dbColName, known := headerToDBMap[csvHeaderKey]
		if known {
			mappedDBHeaders = append(mappedDBHeaders, dbColName)
			originalHeaderIndices = append(originalHeaderIndices, i)
		} else {
			log.Printf("Warning: CSV header '%s' (normalized to '%s') in %s is not mapped and will be skipped.", h, csvHeaderKey, csvFilePath)
		}
	}

	if len(mappedDBHeaders) == 0 {
		return fmt.Errorf(
			"no usable CSV headers found in %s after mapping. Original headers: %v",
			csvFilePath,
			headers,
		)
	}
	log.Printf("Using mapped DB headers for %s.%s: %v", schemaName, tableName, mappedDBHeaders)

	qualifiedTableName := pgx.Identifier{schemaName, tableName}
	var rows [][]any
	rowCount := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		rowCount++
		if err != nil {
			return fmt.Errorf(
				"failed to read data record %d from CSV %s: %w",
				rowCount,
				csvFilePath,
				err,
			)
		}

		rowValues := make([]any, len(mappedDBHeaders))
		for i, dbHeaderName := range mappedDBHeaders {
			originalCsvColIndex := originalHeaderIndices[i]
			valueStr := record[originalCsvColIndex]

			if dbHeaderName == "index" {
				var intVal int
				if valueStr == "" {
					return fmt.Errorf(
						"primary key 'index' is empty in CSV row %d, file %s",
						rowCount,
						csvFilePath,
					)
				}
				_, errScan := fmt.Sscan(valueStr, &intVal)
				if errScan != nil {
					return fmt.Errorf(
						"failed to parse 'index' value '%s' to int in CSV row %d, file %s: %w",
						valueStr,
						rowCount,
						csvFilePath,
						errScan,
					)
				}
				rowValues[i] = intVal
			} else if dbHeaderName == "subscription_date" {
				if valueStr == "" {
					rowValues[i] = nil
				} else {
					layouts := []string{
						"2006-01-02",
						"1/2/2006 15:04",
						"1/2/2006",
						time.RFC3339,
						"2006-01-02T15:04:05Z07:00",
					}
					var parsedTime time.Time
					var errTime error
					success := false
					for _, layout := range layouts {
						parsedTime, errTime = time.Parse(layout, valueStr)
						if errTime == nil {
							success = true
							break
						}
					}

					if !success {
						return fmt.Errorf(
							"failed to parse 'subscription_date' value '%s' to time.Time with any known layout in CSV row %d, file %s: last error: %w",
							valueStr,
							rowCount,
							csvFilePath,
							errTime,
						)
					}
					rowValues[i] = parsedTime
				}
			} else {
				if valueStr == "" {
					rowValues[i] = nil
				} else {
					rowValues[i] = valueStr
				}
			}
		}
		rows = append(rows, rowValues)
	}

	if len(rows) == 0 {
		log.Printf(
			"No data rows found or processed in CSV %s for table %s.%s",
			csvFilePath,
			schemaName,
			tableName,
		)
		return nil
	}

	copyCount, err := pool.CopyFrom(
		ctx,
		qualifiedTableName,
		mappedDBHeaders,
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to copy data from CSV %s to table %s (parsed as %s): %w. Used DB Headers: %v",
			csvFilePath,
			tableName,
			qualifiedTableName.Sanitize(),
			err,
			mappedDBHeaders,
		)
	}

	log.Printf(
		"Successfully loaded %d rows from %s into %s.%s",
		copyCount,
		csvFilePath,
		schemaName,
		tableName,
	)
	return nil
}

func newTestTableRepairTask(sourceOfTruthNode, qualifiedTableName, diffFilePath string) *repair.TableRepairTask {
	task := repair.NewTableRepairTask()
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

	time.Sleep(2 * time.Second)

	if err := repairTask.Run(false); err != nil {
		t.Fatalf("Failed to repair table: %v", err)
	}

	log.Printf("Table '%s' repaired successfully using %s as source of truth.", qualifiedTableName, sourceOfTruthNode)
}

// getCommitTimestamp retrieves the commit timestamp for a specific row
func getCommitTimestamp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, qualifiedTableName string, id int) time.Time {
	t.Helper()
	var ts time.Time
	query := fmt.Sprintf("SELECT pg_xact_commit_timestamp(xmin) FROM %s WHERE id = $1", qualifiedTableName)
	err := pool.QueryRow(ctx, query, id).Scan(&ts)
	require.NoError(t, err, "Failed to get commit timestamp for row id %d", id)
	return ts
}

// getReplicationOrigin retrieves the replication origin name for a specific row.
// It uses spock.xact_commit_timestamp_origin() to extract the roident from the
// transaction's origin tracking, then resolves it to a human-readable name.
//
// For normally replicated rows, the roident is the spock node_id, resolved via
// spock.node to a node_name (e.g., "n3"), returned as "node_n3".
//
// For rows repaired with preserve-origin, the roident is a pg_replication_origin
// entry (e.g., "node_n3"), returned as-is.
func getReplicationOrigin(t *testing.T, ctx context.Context, pool *pgxpool.Pool, qualifiedTableName string, id int) string {
	t.Helper()

	// Extract the roident using spock's origin tracking function (same as table-diff code).
	var roidentStr *string
	query := fmt.Sprintf(
		`SELECT to_json(spock.xact_commit_timestamp_origin(xmin))->>'roident'
		 FROM %s WHERE id = $1`, qualifiedTableName)
	err := pool.QueryRow(ctx, query, id).Scan(&roidentStr)
	if err != nil || roidentStr == nil || *roidentStr == "" || *roidentStr == "0" {
		return ""
	}

	// Try resolving as a spock node_id first (normal replication path).
	var nodeName string
	err = pool.QueryRow(ctx,
		"SELECT node_name FROM spock.node WHERE node_id::text = $1", *roidentStr).Scan(&nodeName)
	if err == nil && nodeName != "" {
		return "node_" + nodeName
	}

	// Fall back to pg_replication_origin lookup (preserve-origin repair path).
	var originName string
	err = pool.QueryRow(ctx,
		"SELECT roname FROM pg_replication_origin WHERE roident::text = $1", *roidentStr).Scan(&originName)
	if err == nil && originName != "" {
		return originName
	}

	return ""
}

// compareTimestamps compares two timestamps with a tolerance in seconds
func compareTimestamps(t1, t2 time.Time, toleranceSeconds int) bool {
	diff := t1.Sub(t2)
	if diff < 0 {
		diff = -diff
	}
	return diff <= time.Duration(toleranceSeconds)*time.Second
}

// compareTimestampsExact compares two timestamps with precise duration-based tolerance
func compareTimestampsExact(t1, t2 time.Time, tolerance time.Duration) bool {
	diff := t1.Sub(t2)
	if diff < 0 {
		diff = -diff
	}
	return diff <= tolerance
}
