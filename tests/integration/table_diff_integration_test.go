package integration

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pgedge/ace/internal/core"
	"github.com/pgedge/ace/pkg/types"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	pgEdgeUser      = "pgedge"
	pgEdgePassword  = "password"
	adminUser       = "admin"
	adminPassword   = "password"
	dbName          = "example_db"
	serviceN1       = "postgres-n1"
	serviceN2       = "postgres-n2"
	hostPortN1      = "6432"
	hostPortN2      = "6433"
	containerPort   = "5432/tcp"
	composeFilePath = "docker-compose.yaml"
	startupTimeout  = 3 * time.Minute
	testSchema      = "public"
	// TODO: Add tests to trigger lower table sample rates -- i.e., use the 1M rows csv file
	defaultCsvFile = "../../test-data/customers.csv"
)

var pgCluster struct {
	Cluster      compose.ComposeStack
	Node1Host    string
	Node1Port    string
	Node1Pool    *pgxpool.Pool
	Node2Host    string
	Node2Port    string
	Node2Pool    *pgxpool.Pool
	ClusterName  string
	ClusterNodes []map[string]any
}

func setupPostgresCluster(t *testing.T) error {
	absComposeFile, err := filepath.Abs(composeFilePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for compose file: %w", err)
	}
	log.Printf("Using Docker Compose file: %s", absComposeFile)

	identifier := strings.ToLower(fmt.Sprintf("ace_integration_test_%d", time.Now().UnixNano()))

	waitN1 := wait.ForListeningPort(containerPort).
		WithStartupTimeout(startupTimeout).
		WithPollInterval(5 * time.Second)

	waitN2 := wait.ForListeningPort(containerPort).
		WithStartupTimeout(startupTimeout).
		WithPollInterval(5 * time.Second)

	composeStack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(identifier),
		compose.WithStackFiles(absComposeFile),
	)
	if err != nil {
		return fmt.Errorf("could not create compose stack: %w", err)
	}

	execError := composeStack.
		WaitForService(serviceN1, waitN1).
		WaitForService(serviceN2, waitN2).
		Up(context.Background(), compose.Wait(true))

	if execError != nil {
		return fmt.Errorf("could not run compose file: %w", execError)
	}

	pgCluster.Cluster = composeStack

	n1Container, err := composeStack.ServiceContainer(context.Background(), serviceN1)
	if err != nil {
		return fmt.Errorf("failed to get container for service %s: %w", serviceN1, err)
	}
	hostN1, err := n1Container.Host(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get host for %s: %w", serviceN1, err)
	}
	cPortN1, err := nat.NewPort("tcp", strings.Split(containerPort, "/")[0])
	if err != nil {
		return fmt.Errorf("failed to create nat.Port for %s: %w", serviceN1, err)
	}
	portN1Mapped, err := n1Container.MappedPort(context.Background(), cPortN1)
	if err != nil {
		return fmt.Errorf("failed to get mapped port for %s: %w", serviceN1, err)
	}
	pgCluster.Node1Host = hostN1
	pgCluster.Node1Port = portN1Mapped.Port()
	log.Printf(
		"Node 1 (%s) accessible at %s:%s",
		serviceN1,
		pgCluster.Node1Host,
		pgCluster.Node1Port,
	)

	poolN1, err := connectToNode(
		pgCluster.Node1Host,
		pgCluster.Node1Port,
		pgEdgeUser,
		pgEdgePassword,
		dbName,
	)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", serviceN1, err)
	}
	pgCluster.Node1Pool = poolN1
	log.Printf("Successfully connected to %s", serviceN1)

	n2Container, err := composeStack.ServiceContainer(context.Background(), serviceN2)
	if err != nil {
		return fmt.Errorf("failed to get container for service %s: %w", serviceN2, err)
	}
	hostN2, err := n2Container.Host(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get host for %s: %w", serviceN2, err)
	}
	cPortN2, err := nat.NewPort("tcp", strings.Split(containerPort, "/")[0])
	if err != nil {
		return fmt.Errorf("failed to create nat.Port for %s: %w", serviceN2, err)
	}
	portN2Mapped, err := n2Container.MappedPort(context.Background(), cPortN2)
	if err != nil {
		return fmt.Errorf("failed to get mapped port for %s: %w", serviceN2, err)
	}
	pgCluster.Node2Host = hostN2
	pgCluster.Node2Port = portN2Mapped.Port()
	log.Printf(
		"Node 2 (%s) accessible at %s:%s",
		serviceN2,
		pgCluster.Node2Host,
		pgCluster.Node2Port,
	)

	poolN2, err := connectToNode(
		pgCluster.Node2Host,
		pgCluster.Node2Port,
		pgEdgeUser,
		pgEdgePassword,
		dbName,
	)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", serviceN2, err)
	}
	pgCluster.Node2Pool = poolN2
	log.Printf("Successfully connected to %s", serviceN2)

	pgCluster.ClusterName = "test_cluster"
	pgCluster.ClusterNodes = []map[string]any{
		{
			"Name":       serviceN1,
			"PublicIP":   pgCluster.Node1Host,
			"Port":       float64(6432),
			"DBUser":     pgEdgeUser,
			"DBPassword": pgEdgePassword,
			"DBName":     dbName,
		},
		{
			"Name":       serviceN2,
			"PublicIP":   pgCluster.Node2Host,
			"Port":       float64(6433),
			"DBUser":     pgEdgeUser,
			"DBPassword": pgEdgePassword,
			"DBName":     dbName,
		},
	}

	// Need this for using pg's 'digest' function
	extensionSQL := "CREATE EXTENSION IF NOT EXISTS pgcrypto;"
	poolsToConfigure := []struct {
		Name string
		Pool *pgxpool.Pool
	}{
		{serviceN1, pgCluster.Node1Pool},
		{serviceN2, pgCluster.Node2Pool},
	}

	for _, node := range poolsToConfigure {
		if node.Pool == nil {
			log.Printf("Skipping pgcrypto creation for node %s as pool is nil", node.Name)
			continue
		}
		_, err := node.Pool.Exec(context.Background(), extensionSQL)
		if err != nil {
			return fmt.Errorf("failed to create pgcrypto extension on node %s: %w", node.Name, err)
		}
		log.Printf("Ensured pgcrypto extension exists on node %s", node.Name)
	}

	return nil
}

func teardownPostgresCluster(t *testing.T) {
	if pgCluster.Node1Pool != nil {
		pgCluster.Node1Pool.Close()
	}
	if pgCluster.Node2Pool != nil {
		pgCluster.Node2Pool.Close()
	}
	if pgCluster.Cluster != nil {
		execError := pgCluster.Cluster.Down(
			context.Background(),
			compose.RemoveOrphans(true),
			compose.RemoveImagesAll,
			compose.RemoveVolumes(true),
		)
		if execError != nil {
			t.Logf("Failed to tear down Docker Compose: %v", execError)
		}
	}
}

func TestMain(m *testing.M) {
	os.Setenv(
		"TESTCONTAINERS_RYUK_DISABLED",
		"true",
	)

	log.Println("Setting up PostgreSQL cluster for integration tests...")
	if err := setupPostgresCluster(&testing.T{}); err != nil {
		log.Fatalf("Failed to setup PostgreSQL cluster: %v", err)
	}
	log.Println("PostgreSQL cluster setup complete.")

	exitCode := m.Run()

	log.Println("Tearing down PostgreSQL cluster...")
	teardownPostgresCluster(&testing.T{})
	log.Println("PostgreSQL cluster teardown complete.")

	os.Exit(exitCode)
}

func connectToNode(host, port, user, password, dbname string) (*pgxpool.Pool, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		user, password, host, port, dbname)
	log.Printf("Attempting to connect to: %s:%s/%s with user %s", host, port, dbname, user)

	var pool *pgxpool.Pool
	var err error
	for i := 0; i < 10; i++ {
		pool, err = pgxpool.Connect(context.Background(), connStr)
		if err == nil {
			err = pool.Ping(context.Background())
			if err == nil {
				log.Printf("Successfully connected and pinged %s:%s/%s", host, port, dbname)
				return pool, nil
			}
			log.Printf("Ping failed for %s:%s/%s: %v. Retrying...", host, port, dbname, err)
			if pool != nil {
				pool.Close()
			}
		} else {
			log.Printf("Connection attempt %d failed for %s:%s/%s: %v. Retrying...", i+1, host, port, dbname, err)
		}
		time.Sleep(3 * time.Second)
	}
	return nil, fmt.Errorf(
		"failed to connect to PostgreSQL at %s:%s after multiple retries: %w",
		host,
		port,
		err,
	)
}

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

func newTestTableDiffTask(
	t *testing.T,
	qualifiedTableName string,
	nodes []string,
) *core.TableDiffTask {
	task := core.NewTableDiffTask()
	task.ClusterName = pgCluster.ClusterName
	task.DBName = dbName
	task.QualifiedTableName = qualifiedTableName
	task.Nodes = strings.Join(nodes, ",")
	task.NodeList = nodes
	task.Output = "json"
	task.BlockSize = 1000
	task.CompareUnitSize = 100
	task.ConcurrencyFactor = 1

	task.SetDBName(dbName)
	task.SetClusterNodes(pgCluster.ClusterNodes)
	task.DerivedFields.HostMap = make(map[string]string)
	if pgCluster.ClusterNodes != nil {
		for _, cn := range pgCluster.ClusterNodes {
			name, _ := cn["Name"].(string)
			publicIP, _ := cn["PublicIP"].(string)
			portF, _ := cn["Port"].(float64)
			port := int(portF)
			task.DerivedFields.HostMap[fmt.Sprintf("%s:%d", publicIP, port)] = name
		}
	}

	schemaParts := strings.SplitN(qualifiedTableName, ".", 2)
	if len(schemaParts) == 2 {
		task.Schema = schemaParts[0]
		task.Table = strings.Trim(schemaParts[1], "\"")
	} else {
		log.Printf("Warning: qualifiedTableName '%s' did not split into schema and table as expected. Using defaults.", qualifiedTableName)
		task.Schema = testSchema
		task.Table = qualifiedTableName
	}

	task.DiffResult = types.DiffOutput{
		NodeDiffs: make(map[string]types.DiffByNodePair),
		Summary: types.DiffSummary{
			Schema:            task.Schema,
			Table:             task.Table,
			Nodes:             nodes,
			BlockSize:         task.BlockSize,
			CompareUnitSize:   task.CompareUnitSize,
			ConcurrencyFactor: task.ConcurrencyFactor,
			DiffRowsCount:     make(map[string]int),
		},
	}

	task.Key = []string{"index"}
	task.SimplePrimaryKey = true
	task.Cols = []string{
		"index", "customer_id", "first_name", "last_name", "company",
		"city", "country", "phone_1", "phone_2", "email",
		"subscription_date", "website",
	}

	if task.Table == "CustomersMixedCase" {
		task.Key = []string{"ID"}
		task.Cols = []string{"ID", "FirstName", "LastName", "EmailAddress"}
		task.SimplePrimaryKey = true
	} else if task.Table == "data_type_test_table" {
		task.Key = []string{"id"}
		task.SimplePrimaryKey = true
		task.Cols = []string{
			"id", "col_smallint", "col_integer", "col_bigint", "col_numeric", "col_real", "col_double",
			"col_varchar", "col_text", "col_char", "col_boolean", "col_date", "col_timestamp",
			"col_timestamptz", "col_jsonb", "col_json", "col_bytea", "col_int_array",
		}
	} else if task.Table == "composite_key_table" {
		task.Key = []string{"org_id", "user_id"}
		task.SimplePrimaryKey = false
		task.Cols = []string{"org_id", "user_id", "data_col_1", "data_col_2"}
	} else if strings.HasSuffix(task.Table, "_filtered") {
		task.Key = []string{"index"}
		task.SimplePrimaryKey = true
		task.Cols = []string{
			"index", "customer_id", "first_name", "last_name", "company",
			"city", "country", "phone_1", "phone_2", "email",
			"subscription_date", "website",
		}
	}

	return task
}

func TestTableDiff_NoDifferences(t *testing.T) {
	ctx := context.Background()
	tableName := "customers_no_diff"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf("Failed to create table %s: %v", qualifiedTableName, err)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
		if err != nil {
			t.Fatalf("Failed to truncate table %s: %v", qualifiedTableName, err)
		}
	}

	csvPath, err := filepath.Abs(defaultCsvFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path for CSV file %s: %v", defaultCsvFile, err)
	}
	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := loadDataFromCSV(ctx, pool, testSchema, tableName, csvPath); err != nil {
			t.Fatalf(
				"Failed to load CSV data into %s on node %s: %v",
				qualifiedTableName,
				nodeName,
				err,
			)
		}
	}

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

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
	tableName := "customers_node1_only"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	csvPath, err := filepath.Abs(defaultCsvFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path for CSV file %s: %v", defaultCsvFile, err)
	}

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf("Failed to create table %s: %v", qualifiedTableName, err)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
		if err != nil {
			t.Fatalf("Failed to truncate table %s: %v", qualifiedTableName, err)
		}
	}

	if pgCluster.Node1Pool == nil {
		t.Fatal("pgCluster.Node1Pool is nil before loading data for DataOnlyOnNode1")
	}
	if err := loadDataFromCSV(ctx, pgCluster.Node1Pool, testSchema, tableName, csvPath); err != nil {
		t.Fatalf(
			"Failed to load CSV data into %s on node %s: %v",
			qualifiedTableName,
			serviceN1,
			err,
		)
	}
	log.Printf("Data loaded only into %s for table %s", serviceN1, qualifiedTableName)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

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

	file, _ := os.Open(csvPath)
	defer file.Close()
	csvReader := csv.NewReader(file)
	_, _ = csvReader.Read()
	expectedDiffCount := 0
	for {
		if _, err := csvReader.Read(); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error reading CSV for count: %v", err)
		}
		expectedDiffCount++
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
	tableName := "customers_node2_only"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	csvPath, err := filepath.Abs(defaultCsvFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path for CSV file %s: %v", defaultCsvFile, err)
	}

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf("Failed to create table %s: %v", qualifiedTableName, err)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
		if err != nil {
			t.Fatalf("Failed to truncate table %s: %v", qualifiedTableName, err)
		}
	}

	if pgCluster.Node2Pool == nil {
		t.Fatal("pgCluster.Node2Pool is nil before loading data for DataOnlyOnNode2")
	}
	if err := loadDataFromCSV(ctx, pgCluster.Node2Pool, testSchema, tableName, csvPath); err != nil {
		t.Fatalf(
			"Failed to load CSV data into %s on node %s: %v",
			qualifiedTableName,
			serviceN2,
			err,
		)
	}
	log.Printf("Data loaded only into %s for table %s", serviceN2, qualifiedTableName)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)

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

	file, _ := os.Open(csvPath)
	defer file.Close()
	csvReader := csv.NewReader(file)
	_, _ = csvReader.Read()
	expectedDiffCount := 0
	for {
		if _, err := csvReader.Read(); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("Error reading CSV for count: %v", err)
		}
		expectedDiffCount++
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
	tableName := "customers_modified"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	csvPath, err := filepath.Abs(defaultCsvFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path for CSV file %s: %v", defaultCsvFile, err)
	}

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf("Failed to create table %s on node %s: %v", qualifiedTableName, nodeName, err)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
		if err != nil {
			t.Fatalf(
				"Failed to truncate table %s on node %s: %v",
				qualifiedTableName,
				nodeName,
				err,
			)
		}
		if err := loadDataFromCSV(ctx, pool, testSchema, tableName, csvPath); err != nil {
			t.Fatalf(
				"Failed to load CSV data into %s on node %s: %v",
				qualifiedTableName,
				nodeName,
				err,
			)
		}
	}
	log.Printf("Identical data loaded into %s on both nodes", qualifiedTableName)

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

	tdTask.Cols = []string{"ID", "FirstName", "LastName", "EmailAddress"}
	tdTask.Key = []string{"ID"}
	tdTask.SimplePrimaryKey = true

	if err := tdTask.ExecuteTask(false); err != nil {
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
	tdTask.Cols = []string{
		"id",
		"col_smallint",
		"col_integer",
		"col_bigint",
		"col_numeric",
		"col_real",
		"col_double",
		"col_varchar",
		"col_text",
		"col_char",
		"col_boolean",
		"col_date",
		"col_timestamp",
		"col_timestamptz",
		"col_jsonb",
		"col_json",
		"col_bytea",
		"col_int_array",
	}
	tdTask.Key = []string{"id"}
	tdTask.SimplePrimaryKey = true

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
	tableName := "customers_filter_test"
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)
	csvPath, err := filepath.Abs(defaultCsvFile)
	if err != nil {
		t.Fatalf("Failed to get absolute path for CSV file %s: %v", defaultCsvFile, err)
	}

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeNameForLog := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			t.Fatalf(
				"Failed to create table %s on node %s: %v",
				qualifiedTableName,
				nodeNameForLog,
				err,
			)
		}
		_, err := pool.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s.%s CASCADE", testSchema, tableName))
		if err != nil {
			t.Fatalf(
				"Failed to truncate table %s on node %s: %v",
				qualifiedTableName,
				nodeNameForLog,
				err,
			)
		}
		if err := loadDataFromCSV(ctx, pool, testSchema, tableName, csvPath); err != nil {
			t.Fatalf(
				"Failed to load CSV data into %s on node %s: %v",
				qualifiedTableName,
				nodeNameForLog,
				err,
			)
		}
	}

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
		_, err := pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("SELECT spock.repair_mode(true)"))
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
		_, err = pgCluster.Node2Pool.Exec(ctx, fmt.Sprintf("SELECT spock.repair_mode(false)"))
		if err != nil {
			t.Fatalf("Failed to disable spock repair mode on node %s: %v", serviceN2, err)
		}
	}
	log.Printf("Data modified on %s for filter test (country = 'China')", serviceN2)

	nodesToCompare := []string{serviceN1, serviceN2}
	tdTask := newTestTableDiffTask(t, qualifiedTableName, nodesToCompare)
	tdTask.TableFilter = "index <= 100"
	tdTask.TaskID = fmt.Sprintf("filter_test_%d", time.Now().UnixNano())

	viewName := fmt.Sprintf("%s_%s_filtered", tdTask.TaskID, tableName)
	createViewSQLCmd := func(schema, viewBase, tableBase, filter string) string {
		return fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.\"%s\" AS SELECT * FROM %s.%s WHERE %s",
			schema, viewBase, schema, tableBase, filter)
	}
	_, err = pgCluster.Node1Pool.Exec(
		ctx,
		createViewSQLCmd(testSchema, viewName, tableName, tdTask.TableFilter),
	)
	if err != nil {
		t.Fatalf("Failed to create filtered view on Node1: %v", err)
	}
	_, err = pgCluster.Node2Pool.Exec(
		ctx,
		createViewSQLCmd(testSchema, viewName, tableName, tdTask.TableFilter),
	)
	if err != nil {
		t.Fatalf("Failed to create filtered view on Node2: %v", err)
	}
	tdTask.Table = viewName

	if err := tdTask.ExecuteTask(false); err != nil {
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
		t.Errorf("Expected modified row index 1 (Indonesia) not found in filtered diffs")
	}
	if !foundIndex2 {
		t.Errorf("Expected modified row index 2 (China) not found in filtered diffs")
	}
	if !foundIndex3 {
		t.Errorf("Expected modified row index 3 (China) not found in filtered diffs")
	}

	log.Println("TestTableDiff_TableFiltering completed.")
}
