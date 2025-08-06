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

package integration

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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
