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
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
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
	defaultCsvFilePath = "../../test-data/"
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

	log.Println("Test data loaded into n1.")

	clusterConfig := types.ClusterConfig{
		JSONVersion: "1.0",
		ClusterName: pgCluster.ClusterName,
		LogLevel:    "info",
		UpdateDate:  time.Now().Format(time.RFC3339),
		PGEdge: struct {
			PGVersion int               `json:"pg_version"`
			AutoStart string            `json:"auto_start"`
			Spock     types.SpockConfig `json:"spock"`
			Databases []types.Database  `json:"databases"`
		}{
			PGVersion: 16,
			AutoStart: "yes",
			Spock: types.SpockConfig{
				SpockVersion: "4.0.10",
				AutoDDL:      "yes",
			},
			Databases: []types.Database{
				{
					DBName:     dbName,
					DBUser:     pgEdgeUser,
					DBPassword: pgEdgePassword,
				},
			},
		},
		NodeGroups: []types.NodeGroup{
			{
				Name:     serviceN1,
				IsActive: "yes",
				PublicIP: pgCluster.Node1Host,
				Port:     pgCluster.Node1Port,
				Path:     "/usr/local/bin",
				SSH: struct {
					OSUser     string `json:"os_user"`
					PrivateKey string `json:"private_key"`
				}{OSUser: "pgedge"},
			},
			{
				Name:     serviceN2,
				IsActive: "yes",
				PublicIP: pgCluster.Node2Host,
				Port:     pgCluster.Node2Port,
				Path:     "/usr/local/bin",
				SSH: struct {
					OSUser     string `json:"os_user"`
					PrivateKey string `json:"private_key"`
				}{OSUser: "pgedge"},
			},
		},
	}

	jsonData, err := json.MarshalIndent(clusterConfig, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal cluster info: %w", err)
	}

	if err := os.WriteFile("test_cluster.json", jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write cluster info to test_cluster.json: %w", err)
	}

	log.Println("Postgres cluster setup complete.")
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
			compose.RemoveVolumes(true),
		)
		if execError != nil {
			t.Logf("Failed to tear down Docker Compose: %v", execError)
		}
	}
	log.Printf("Cleaning up diff files...")
	files, err := filepath.Glob("*_diffs-*.json")
	if err != nil {
		t.Logf("Error finding diff files: %v", err)
		return
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			t.Logf("Failed to remove diff file %s: %v", f, err)
		} else {
			log.Printf("Removed diff file: %s", f)
		}
	}
	if err := os.Remove("test_cluster.json"); err != nil && !os.IsNotExist(err) {
		t.Logf("failed to remove test_cluster.json: %v", err)
	}
}

func TestMain(m *testing.M) {
	os.Setenv(
		"TESTCONTAINERS_RYUK_DISABLED",
		"true",
	)

	if err := config.Init("../../ace.yaml"); err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Println("Setting up PostgreSQL cluster for integration tests...")
	if err := setupPostgresCluster(&testing.T{}); err != nil {
		log.Fatalf("Failed to setup PostgreSQL cluster: %v", err)
	}
	log.Println("PostgreSQL cluster setup complete.")

	log.Println("Creating and loading shared customers table...")
	if err := setupSharedCustomersTable("customers"); err != nil {
		log.Fatalf("Failed to setup shared customers table: %v", err)
	}
	log.Println("Shared customers table setup complete.")

	log.Println("Creating and loading shared customers_1M table...")
	if err := setupSharedCustomersTable("customers_1M"); err != nil {
		log.Fatalf("Failed to setup shared customers_1M table: %v", err)
	}
	log.Println("Shared customers_1M table setup complete.")

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
		pool, err = pgxpool.New(context.Background(), connStr)
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

func setupSharedCustomersTable(tableName string) error {
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", testSchema, tableName)

	for _, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		if err := createTestTable(ctx, pool, testSchema, tableName); err != nil {
			return fmt.Errorf("failed to create shared table %s: %w", qualifiedTableName, err)
		}
	}

	csvFilePath := defaultCsvFilePath + tableName + ".csv"

	csvPath, err := filepath.Abs(csvFilePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for CSV file %s: %w", csvFilePath, err)
	}

	for i, pool := range []*pgxpool.Pool{pgCluster.Node1Pool, pgCluster.Node2Pool} {
		nodeName := pgCluster.ClusterNodes[i]["Name"].(string)
		if err := loadDataFromCSV(ctx, pool, testSchema, tableName, csvPath); err != nil {
			return fmt.Errorf(
				"failed to load CSV data into %s on node %s: %w",
				qualifiedTableName,
				nodeName,
				err,
			)
		}
	}
	return nil
}
