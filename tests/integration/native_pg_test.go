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
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/compose"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	nativeUser          = "postgres"
	nativePassword      = "password"
	nativeDBName        = "testdb"
	nativeServiceN1     = "native-n1"
	nativeServiceN2     = "native-n2"
	nativeContainerPort = "5432/tcp"
	nativeComposeFile   = "docker-compose-native.yaml"
	nativeClusterName   = "native_test_cluster"
)

// nativeClusterState holds connections and config for vanilla PG test containers.
type nativeClusterState struct {
	stack  compose.ComposeStack
	n1Host string
	n1Port string
	n1Pool *pgxpool.Pool
	n2Host string
	n2Port string
	n2Pool *pgxpool.Pool
}

func setupNativeCluster(t *testing.T) *nativeClusterState {
	t.Helper()
	ctx := context.Background()

	absCompose, err := filepath.Abs(nativeComposeFile)
	require.NoError(t, err, "resolve native compose file path")

	identifier := strings.ToLower(fmt.Sprintf("ace_native_test_%d", time.Now().UnixNano()))

	waitN1 := wait.ForListeningPort(nat.Port(nativeContainerPort)).
		WithStartupTimeout(startupTimeout).
		WithPollInterval(5 * time.Second)
	waitN2 := wait.ForListeningPort(nat.Port(nativeContainerPort)).
		WithStartupTimeout(startupTimeout).
		WithPollInterval(5 * time.Second)

	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(identifier),
		compose.WithStackFiles(absCompose),
	)
	require.NoError(t, err, "create native compose stack")

	execErr := stack.
		WaitForService(nativeServiceN1, waitN1).
		WaitForService(nativeServiceN2, waitN2).
		Up(ctx, compose.Wait(true))
	require.NoError(t, execErr, "start native compose stack")

	state := &nativeClusterState{stack: stack}

	// Get mapped host/port for n1
	n1Container, err := stack.ServiceContainer(ctx, nativeServiceN1)
	require.NoError(t, err, "get native-n1 container")
	n1Host, err := n1Container.Host(ctx)
	require.NoError(t, err, "get native-n1 host")
	cPort, err := nat.NewPort("tcp", "5432")
	require.NoError(t, err)
	n1MappedPort, err := n1Container.MappedPort(ctx, cPort)
	require.NoError(t, err, "get native-n1 mapped port")
	state.n1Host = n1Host
	state.n1Port = n1MappedPort.Port()

	// Get mapped host/port for n2
	n2Container, err := stack.ServiceContainer(ctx, nativeServiceN2)
	require.NoError(t, err, "get native-n2 container")
	n2Host, err := n2Container.Host(ctx)
	require.NoError(t, err, "get native-n2 host")
	n2MappedPort, err := n2Container.MappedPort(ctx, cPort)
	require.NoError(t, err, "get native-n2 mapped port")
	state.n2Host = n2Host
	state.n2Port = n2MappedPort.Port()

	// Connect
	state.n1Pool, err = connectToNode(state.n1Host, state.n1Port, nativeUser, nativePassword, nativeDBName)
	require.NoError(t, err, "connect to native-n1")
	state.n2Pool, err = connectToNode(state.n2Host, state.n2Port, nativeUser, nativePassword, nativeDBName)
	require.NoError(t, err, "connect to native-n2")

	// Create pgcrypto extension on both nodes
	for _, pool := range []*pgxpool.Pool{state.n1Pool, state.n2Pool} {
		_, err = pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto")
		require.NoError(t, err, "create pgcrypto extension")
	}

	log.Printf("Native PG cluster ready: n1=%s:%s, n2=%s:%s", state.n1Host, state.n1Port, state.n2Host, state.n2Port)
	return state
}

func (s *nativeClusterState) teardown(t *testing.T) {
	t.Helper()
	if s.n1Pool != nil {
		s.n1Pool.Close()
	}
	if s.n2Pool != nil {
		s.n2Pool.Close()
	}
	if s.stack != nil {
		execErr := s.stack.Down(
			context.Background(),
			compose.RemoveOrphans(true),
			compose.RemoveVolumes(true),
		)
		if execErr != nil {
			t.Logf("Failed to tear down native compose stack: %v", execErr)
		}
	}
	// Clean up diff files and cluster config
	files, _ := filepath.Glob("*_diffs-*.json")
	for _, f := range files {
		os.Remove(f)
	}
	os.Remove(nativeClusterName + ".json")
}

// writeClusterConfig writes a cluster config JSON that the diff/repair
// tasks use to discover nodes and credentials.
func (s *nativeClusterState) writeClusterConfig(t *testing.T) {
	t.Helper()
	cfg := types.ClusterConfig{
		JSONVersion: "1.0",
		ClusterName: nativeClusterName,
		LogLevel:    "info",
		UpdateDate:  time.Now().Format(time.RFC3339),
		PGEdge: struct {
			PGVersion int               `json:"pg_version"`
			AutoStart string            `json:"auto_start"`
			Spock     types.SpockConfig `json:"spock"`
			Databases []types.Database  `json:"databases"`
		}{
			PGVersion: 17,
			AutoStart: "yes",
			Databases: []types.Database{
				{
					DBName:     nativeDBName,
					DBUser:     nativeUser,
					DBPassword: nativePassword,
				},
			},
		},
		NodeGroups: []types.NodeGroup{
			{
				Name:     nativeServiceN1,
				IsActive: "yes",
				PublicIP: s.n1Host,
				Port:     s.n1Port,
			},
			{
				Name:     nativeServiceN2,
				IsActive: "yes",
				PublicIP: s.n2Host,
				Port:     s.n2Port,
			},
		},
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err, "marshal native cluster config")
	require.NoError(t, os.WriteFile(nativeClusterName+".json", data, 0644), "write native cluster config")
}

// TestNativePG runs the full suite of native PostgreSQL (no spock) tests.
// It starts its own vanilla postgres:17 Docker Compose stack and runs the
// same shared test logic used by the spock tests, via testEnv abstraction.
func TestNativePG(t *testing.T) {
	state := setupNativeCluster(t)
	t.Cleanup(func() { state.teardown(t) })
	state.writeClusterConfig(t)

	ctx := context.Background()

	// Create the customers table on both nodes and load initial data from CSV.
	for _, pool := range []*pgxpool.Pool{state.n1Pool, state.n2Pool} {
		require.NoError(t, createTestTable(ctx, pool, testSchema, "customers"))
	}

	env := newNativeEnv(state)

	// Load CSV data so that shared tests (e.g. NoDifferences, DataOnlyOnNode1)
	// have a known baseline.
	csvPath, err := filepath.Abs(defaultCsvFilePath + "customers.csv")
	require.NoError(t, err)
	for _, pool := range env.pools() {
		require.NoError(t, loadDataFromCSV(ctx, pool, testSchema, "customers", csvPath),
			"load CSV into customers")
	}

	// ── Native-specific tests ─────────────────────────────────────────────

	t.Run("CheckSpockInstalled_ReturnsFalse", func(t *testing.T) {
		installed, err := queries.CheckSpockInstalled(ctx, state.n1Pool)
		require.NoError(t, err)
		assert.False(t, installed, "spock should not be installed on vanilla PG")
	})

	t.Run("SpockDiff_GracefulError", func(t *testing.T) {
		task := diff.NewSpockDiffTask()
		task.ClusterName = nativeClusterName
		task.DBName = nativeDBName
		task.Nodes = nativeServiceN1 + "," + nativeServiceN2
		task.Ctx = context.Background()
		task.SkipDBUpdate = true

		err := task.RunChecks(false)
		require.NoError(t, err, "spock-diff RunChecks should succeed (it just validates and connects)")

		err = task.ExecuteTask()
		require.Error(t, err, "spock-diff should fail on vanilla PG")
		assert.Contains(t, err.Error(), "spock extension", "error should mention spock extension")
	})

	t.Run("RepsetDiff_GracefulError", func(t *testing.T) {
		task := diff.NewRepsetDiffTask()
		task.ClusterName = nativeClusterName
		task.DBName = nativeDBName
		task.RepsetName = "default"
		task.Nodes = "all"
		task.SkipDBUpdate = true

		err := task.RunChecks(false)
		require.Error(t, err, "repset-diff should fail on vanilla PG")
		assert.Contains(t, err.Error(), "spock extension", "error should mention spock extension")
	})

	// ── Shared table-diff tests (simple PK) ───────────────────────────────

	t.Run("TableDiffSimplePK", func(t *testing.T) {
		t.Run("Customers", func(t *testing.T) {
			runCustomerTableDiffTests(t, env)
		})
		t.Run("MixedCaseIdentifiers", func(t *testing.T) {
			testTableDiff_MixedCaseIdentifiers(t, env, false)
		})
	})

	// ── Shared table-diff tests (composite PK) ───────────────────────────

	t.Run("TableDiffCompositePK", func(t *testing.T) {
		t.Run("Customers", func(t *testing.T) {
			for _, pool := range env.pools() {
				err := alterTableToCompositeKey(ctx, pool, env.Schema, "customers")
				require.NoError(t, err)
			}
			t.Cleanup(func() {
				for _, pool := range env.pools() {
					err := revertTableToSimpleKey(ctx, pool, env.Schema, "customers")
					require.NoError(t, err)
				}
			})
			runCustomerTableDiffTests(t, env)
		})
		t.Run("MixedCaseIdentifiers", func(t *testing.T) {
			testTableDiff_MixedCaseIdentifiers(t, env, true)
		})
	})

	// ── Shared table-repair tests ─────────────────────────────────────────

	t.Run("TableRepair_UnidirectionalDefault", func(t *testing.T) {
		testTableRepair_UnidirectionalDefault(t, env)
	})
	t.Run("TableRepair_InsertOnly", func(t *testing.T) {
		testTableRepair_InsertOnly(t, env)
	})
	t.Run("TableRepair_UpsertOnly", func(t *testing.T) {
		testTableRepair_UpsertOnly(t, env)
	})
	t.Run("TableRepair_Bidirectional", func(t *testing.T) {
		testTableRepair_Bidirectional(t, env)
	})
	t.Run("TableRepair_DryRun", func(t *testing.T) {
		testTableRepair_DryRun(t, env)
	})
	t.Run("TableRepair_GenerateReport", func(t *testing.T) {
		testTableRepair_GenerateReport(t, env)
	})
	t.Run("TableRepair_FixNulls", func(t *testing.T) {
		testTableRepair_FixNulls(t, env)
	})
	t.Run("TableRepair_FixNulls_DryRun", func(t *testing.T) {
		testTableRepair_FixNulls_DryRun(t, env)
	})
	t.Run("TableRepair_FixNulls_BidirectionalUpdate", func(t *testing.T) {
		testTableRepair_FixNulls_BidirectionalUpdate(t, env)
	})
}
