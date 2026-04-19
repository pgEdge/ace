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
	nativeServiceN3     = "native-n3"
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
	n3Host string
	n3Port string
	n3Pool *pgxpool.Pool
}

func setupNativeCluster(t *testing.T) *nativeClusterState {
	t.Helper()
	ctx := context.Background()

	absCompose, err := filepath.Abs(nativeComposeFile)
	require.NoError(t, err, "resolve native compose file path")

	identifier := strings.ToLower(fmt.Sprintf("ace_native_test_%d", time.Now().UnixNano()))

	waitForPort := func() *wait.HostPortStrategy {
		return wait.ForListeningPort(nat.Port(nativeContainerPort)).
			WithStartupTimeout(startupTimeout).
			WithPollInterval(5 * time.Second)
	}

	stack, err := compose.NewDockerComposeWith(
		compose.StackIdentifier(identifier),
		compose.WithStackFiles(absCompose),
	)
	require.NoError(t, err, "create native compose stack")

	execErr := stack.
		WaitForService(nativeServiceN1, waitForPort()).
		WaitForService(nativeServiceN2, waitForPort()).
		WaitForService(nativeServiceN3, waitForPort()).
		Up(ctx, compose.Wait(true))
	require.NoError(t, execErr, "start native compose stack")

	state := &nativeClusterState{stack: stack}

	cPort, err := nat.NewPort("tcp", "5432")
	require.NoError(t, err)

	// Helper to extract mapped host/port for a service.
	getHostPort := func(service string) (string, string) {
		c, cErr := stack.ServiceContainer(ctx, service)
		require.NoError(t, cErr, "get %s container", service)
		h, hErr := c.Host(ctx)
		require.NoError(t, hErr, "get %s host", service)
		p, pErr := c.MappedPort(ctx, cPort)
		require.NoError(t, pErr, "get %s mapped port", service)
		return h, p.Port()
	}

	state.n1Host, state.n1Port = getHostPort(nativeServiceN1)
	state.n2Host, state.n2Port = getHostPort(nativeServiceN2)
	state.n3Host, state.n3Port = getHostPort(nativeServiceN3)

	// Connect
	state.n1Pool, err = connectToNode(state.n1Host, state.n1Port, nativeUser, nativePassword, nativeDBName)
	require.NoError(t, err, "connect to native-n1")
	state.n2Pool, err = connectToNode(state.n2Host, state.n2Port, nativeUser, nativePassword, nativeDBName)
	require.NoError(t, err, "connect to native-n2")
	state.n3Pool, err = connectToNode(state.n3Host, state.n3Port, nativeUser, nativePassword, nativeDBName)
	require.NoError(t, err, "connect to native-n3")

	// Create pgcrypto extension on all nodes
	for _, pool := range []*pgxpool.Pool{state.n1Pool, state.n2Pool, state.n3Pool} {
		_, err = pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgcrypto")
		require.NoError(t, err, "create pgcrypto extension")
	}

	log.Printf("Native PG cluster ready: n1=%s:%s, n2=%s:%s, n3=%s:%s",
		state.n1Host, state.n1Port, state.n2Host, state.n2Port, state.n3Host, state.n3Port)
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
	if s.n3Pool != nil {
		s.n3Pool.Close()
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

	// Create and load customers_1M table (needed by merkle tree CDC and split tests).
	for _, pool := range env.pools() {
		require.NoError(t, createTestTable(ctx, pool, testSchema, "customers_1M"))
	}
	csv1MPath, err := filepath.Abs(defaultCsvFilePath + "customers_1M.csv")
	require.NoError(t, err)
	for _, pool := range env.pools() {
		require.NoError(t, loadDataFromCSV(ctx, pool, testSchema, "customers_1M", csv1MPath),
			"load CSV into customers_1M")
	}

	// ── Native-specific tests ─────────────────────────────────────────────

	t.Run("CheckSpockInstalled_ReturnsFalse", func(t *testing.T) {
		installed, err := queries.CheckSpockInstalled(ctx, state.n1Pool)
		require.NoError(t, err)
		assert.False(t, installed, "spock should not be installed on vanilla PG")
	})

	t.Run("GetNodeOriginNames_NativeSubscription", func(t *testing.T) {
		// Set up a real publication on n1 and subscription on n2 so that
		// pg_replication_origin gets populated with a subscription-linked entry.
		subName := "test_origin_sub"
		pubName := "test_origin_pub"

		// Create a test table and publication on n1.
		_, err := state.n1Pool.Exec(ctx,
			"CREATE TABLE IF NOT EXISTS public.origin_test (id int PRIMARY KEY, val text)")
		require.NoError(t, err)
		_, err = state.n1Pool.Exec(ctx,
			fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE public.origin_test", pubName))
		require.NoError(t, err)

		// Create the same table on n2 (subscription target).
		_, err = state.n2Pool.Exec(ctx,
			"CREATE TABLE IF NOT EXISTS public.origin_test (id int PRIMARY KEY, val text)")
		require.NoError(t, err)

		// Create a subscription on n2 pointing at n1 via Docker-internal hostname.
		connStr := fmt.Sprintf(
			"host=%s port=5432 dbname=%s user=%s password=%s",
			nativeServiceN1, nativeDBName, nativeUser, nativePassword)
		_, err = state.n2Pool.Exec(ctx,
			fmt.Sprintf("CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s",
				subName, connStr, pubName))
		require.NoError(t, err)

		t.Cleanup(func() {
			state.n2Pool.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", subName))
			state.n1Pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
			state.n1Pool.Exec(ctx, "DROP TABLE IF EXISTS public.origin_test")
			state.n2Pool.Exec(ctx, "DROP TABLE IF EXISTS public.origin_test")
		})

		// GetNodeOriginNames on n2 should route to GetNativeNodeOriginNames
		// (spock not installed) and return the subscription name as the value.
		names, err := queries.GetNodeOriginNames(ctx, state.n2Pool)
		require.NoError(t, err, "GetNodeOriginNames should succeed on native PG with subscriptions")
		require.NotEmpty(t, names, "should have at least one origin mapping")

		// Verify that the subscription name appears as a value in the map.
		found := false
		for _, name := range names {
			if name == subName {
				found = true
				break
			}
		}
		assert.True(t, found,
			"expected subscription name %q in origin names map, got: %v", subName, names)

		// The key should be a numeric roident (parseable as int).
		for id := range names {
			_, parseErr := fmt.Sscanf(id, "%d", new(int))
			assert.NoError(t, parseErr, "origin ID %q should be numeric", id)
		}
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
		t.Run("VariousDataTypes", func(t *testing.T) {
			testTableDiff_VariousDataTypes(t, env, false)
		})
		t.Run("UUIDColumn", func(t *testing.T) {
			testTableDiff_UUIDColumn(t, env, false)
		})
		t.Run("ByteaColumnSizeCheck", func(t *testing.T) {
			testTableDiff_ByteaColumnSizeCheck(t, env, false)
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
		t.Run("VariousDataTypes", func(t *testing.T) {
			testTableDiff_VariousDataTypes(t, env, true)
		})
		t.Run("UUIDColumn", func(t *testing.T) {
			testTableDiff_UUIDColumn(t, env, true)
		})
		t.Run("ByteaColumnSizeCheck", func(t *testing.T) {
			testTableDiff_ByteaColumnSizeCheck(t, env, true)
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
	t.Run("TableRepair_VariousDataTypes", func(t *testing.T) {
		testTableRepair_VariousDataTypes(t, env)
	})
	t.Run("TableRepair_TimestampAndTimeTypes", func(t *testing.T) {
		testTableRepair_TimestampAndTimeTypes(t, env)
	})
	t.Run("TableRepair_LargeBigintPK", func(t *testing.T) {
		testTableRepair_LargeBigintPK(t, env)
	})

	// ── Merkle tree tests ────────────────────────────────────────────────

	t.Run("MerkleTreeSimplePK", func(t *testing.T) {
		runMerkleTreeTests(t, env, "customers")
	})

	t.Run("MerkleTreeCompositePK", func(t *testing.T) {
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
		runMerkleTreeTests(t, env, "customers")
	})

	t.Run("MerkleTreeNumericScaleInvariance", func(t *testing.T) {
		testMerkleTreeNumericScaleInvariance(t, env)
	})

	// ── Native PG preserve-origin test ───────────────────────────────────
	// This test verifies the full diff → preserve-origin repair → verify
	// cycle on native PG with real logical replication, including that
	// GetNodeOriginNames returns subscription names and that repaired rows
	// retain their original replication origin.

	t.Run("TableRepair_PreserveOrigin_NativePG", func(t *testing.T) {
		testNativePreserveOrigin(t, state, env)
	})
}

// getNativeReplicationOrigin retrieves the replication origin for a row on
// native PG (no spock). Uses pg_xact_commit_timestamp_origin to get the
// roident, then resolves it via pg_replication_origin.roname.
func getNativeReplicationOrigin(t *testing.T, ctx context.Context, pool *pgxpool.Pool, qualifiedTableName string, id int) string {
	t.Helper()

	var roidentStr *string
	query := fmt.Sprintf(
		`SELECT (pg_xact_commit_timestamp_origin(xmin)).roident::text FROM %s WHERE id = $1`,
		qualifiedTableName)
	err := pool.QueryRow(ctx, query, id).Scan(&roidentStr)
	if err != nil || roidentStr == nil || *roidentStr == "" || *roidentStr == "0" {
		return ""
	}

	var originName string
	err = pool.QueryRow(ctx,
		"SELECT roname FROM pg_replication_origin WHERE roident::text = $1", *roidentStr).Scan(&originName)
	if err == nil && originName != "" {
		return originName
	}

	return *roidentStr
}

// testNativePreserveOrigin verifies the full preserve-origin cycle on native PG
// with 3 nodes and real logical replication. Mirrors TestTableRepair_PreserveOrigin
// (the spock version).
//
// Topology: data originates on n3, replicates to n1 and n2 via subscriptions.
// Both n1 and n2 see rows with a non-local origin (n3's subscription roident).
// We delete rows on n2, diff n1 vs n2, and repair with preserve-origin. Since
// n1's rows have non-local origin metadata, the repair can preserve it.
//
//  1. Create table on all 3 nodes
//  2. Set up publications on n3, subscriptions on n1 and n2
//  3. Insert data on n3, wait for replication to n1 and n2
//  4. Verify GetNodeOriginNames maps roident → subscription name
//  5. Capture baseline origins and timestamps on n1
//  6. Delete rows on n2 to simulate data loss
//  7. Run table-diff → preserve-origin repair (n1 as source of truth)
//  8. Verify repaired rows on n2 retain the original origin and timestamps
func testNativePreserveOrigin(t *testing.T, state *nativeClusterState, env *testEnv) {
	ctx := context.Background()
	tableName := "native_preserve_origin_test"
	qualifiedTableName := fmt.Sprintf("public.%s", tableName)
	pubName := "preserve_origin_pub"
	subN1Name := "sub_n3_to_n1"
	subN2Name := "sub_n3_to_n2"

	// Create table on all 3 nodes.
	for _, pool := range []*pgxpool.Pool{state.n1Pool, state.n2Pool, state.n3Pool} {
		_, err := pool.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s (id INT PRIMARY KEY, data TEXT)", qualifiedTableName))
		require.NoError(t, err)
	}

	// Publication on n3.
	_, err := state.n3Pool.Exec(ctx, fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", pubName, qualifiedTableName))
	require.NoError(t, err)

	// Subscriptions on n1 and n2 pointing at n3 (Docker-internal hostname).
	// Use copy_data=false to avoid transient sync origins.
	n3ConnStr := fmt.Sprintf("host=%s port=5432 dbname=%s user=%s password=%s",
		nativeServiceN3, nativeDBName, nativeUser, nativePassword)
	_, err = state.n1Pool.Exec(ctx, fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (copy_data = false)",
		subN1Name, n3ConnStr, pubName))
	require.NoError(t, err)
	_, err = state.n2Pool.Exec(ctx, fmt.Sprintf(
		"CREATE SUBSCRIPTION %s CONNECTION '%s' PUBLICATION %s WITH (copy_data = false)",
		subN2Name, n3ConnStr, pubName))
	require.NoError(t, err)

	t.Cleanup(func() {
		state.n1Pool.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", subN1Name))
		state.n2Pool.Exec(ctx, fmt.Sprintf("DROP SUBSCRIPTION IF EXISTS %s", subN2Name))
		state.n3Pool.Exec(ctx, fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", pubName))
		for _, pool := range []*pgxpool.Pool{state.n1Pool, state.n2Pool, state.n3Pool} {
			pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", qualifiedTableName))
		}
	})

	// Brief pause for subscriptions to connect and start streaming.
	time.Sleep(2 * time.Second)

	// Insert data on n3. The streaming apply workers on n1 and n2 will use
	// each subscription's main replication origin.
	sampleIDs := []int{1, 2, 3, 4, 5}
	for _, id := range sampleIDs {
		_, err := state.n3Pool.Exec(ctx,
			fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", qualifiedTableName),
			id, fmt.Sprintf("row_%d", id))
		require.NoError(t, err)
	}

	// Wait for replication to both n1 and n2.
	for _, pool := range []*pgxpool.Pool{state.n1Pool, state.n2Pool} {
		p := pool // capture for closure
		assertEventually(t, 30*time.Second, func() error {
			var count int
			if err := p.QueryRow(ctx,
				fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&count); err != nil {
				return err
			}
			if count < len(sampleIDs) {
				return fmt.Errorf("expected %d rows, got %d", len(sampleIDs), count)
			}
			return nil
		})
	}
	log.Println("Replication complete: all rows present on n1 and n2 (via streaming from n3)")

	// --- Verify GetNodeOriginNames on n1 maps roident → subscription name ---
	names, err := queries.GetNodeOriginNames(ctx, state.n1Pool)
	require.NoError(t, err)
	found := false
	for _, name := range names {
		if name == subN1Name {
			found = true
			break
		}
	}
	require.True(t, found, "GetNodeOriginNames on n1 should contain subscription %q, got: %v", subN1Name, names)
	log.Printf("GetNodeOriginNames on n1: %v", names)

	// --- Capture baseline origins and timestamps on n1 (source of truth) ---
	originalOrigins := make(map[int]string)
	originalTimestamps := make(map[int]time.Time)
	for _, id := range sampleIDs {
		origin := getNativeReplicationOrigin(t, ctx, state.n1Pool, qualifiedTableName, id)
		originalOrigins[id] = origin
		originalTimestamps[id] = getCommitTimestamp(t, ctx, state.n1Pool, qualifiedTableName, id)
		log.Printf("Row %d on n1: origin=%s ts=%s", id, origin, originalTimestamps[id].Format(time.RFC3339Nano))
	}

	// All rows on n1 were replicated from n3, so they must have non-local origin.
	for _, id := range sampleIDs {
		require.NotEmpty(t, originalOrigins[id],
			"Row %d on n1 should have a replication origin (was replicated from n3)", id)
	}
	log.Println("Baseline verified: all sample rows on n1 have non-local origin (from n3)")

	// Wait to ensure original timestamps are clearly in the past.
	time.Sleep(2 * time.Second)

	// --- Simulate data loss on n2 ---
	log.Println("Simulating data loss on n2...")
	tx, err := state.n2Pool.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "SET session_replication_role = 'replica'")
	require.NoError(t, err)
	for _, id := range sampleIDs {
		_, err = tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = $1", qualifiedTableName), id)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit(ctx))
	log.Printf("Deleted %d rows from n2", len(sampleIDs))

	// --- Run table-diff ---
	diffTask := env.newTableDiffTask(t, qualifiedTableName, []string{nativeServiceN1, nativeServiceN2})
	require.NoError(t, diffTask.RunChecks(false))
	require.NoError(t, diffTask.ExecuteTask())
	diffFile := getLatestDiffFile(t)
	require.NotEmpty(t, diffFile)
	log.Printf("Diff file: %s", diffFile)

	// --- Run preserve-origin repair ---
	repairTask := env.newTableRepairTask(nativeServiceN1, qualifiedTableName, diffFile)
	repairTask.RecoveryMode = true
	repairTask.PreserveOrigin = true

	err = repairTask.Run(false)
	require.NoError(t, err)
	if repairTask.TaskStatus == "FAILED" {
		t.Fatalf("Repair failed: %s", repairTask.TaskContext)
	}
	log.Println("Preserve-origin repair completed")

	// --- Verify all rows restored ---
	var count int
	err = state.n2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, len(sampleIDs), count, "All rows should be restored after repair")

	// --- Verify row content ---
	for _, id := range sampleIDs {
		var data string
		err := state.n2Pool.QueryRow(ctx,
			fmt.Sprintf("SELECT data FROM %s WHERE id = $1", qualifiedTableName), id).Scan(&data)
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("row_%d", id), data, "Row %d data mismatch", id)
	}

	// --- Verify origins are preserved ---
	// After preserve-origin repair, repaired rows on n2 should have an
	// ACE-created origin that encodes the original source (n3 via n1).
	for _, id := range sampleIDs {
		repairedOrigin := getNativeReplicationOrigin(t, ctx, state.n2Pool, qualifiedTableName, id)
		require.NotEmpty(t, repairedOrigin,
			"Row %d should have a replication origin after preserve-origin repair", id)
		log.Printf("Row %d: n1 origin=%s, repaired n2 origin=%s", id, originalOrigins[id], repairedOrigin)
	}

	// --- Verify timestamps are preserved ---
	preservedCount := 0
	for _, id := range sampleIDs {
		repairedTS := getCommitTimestamp(t, ctx, state.n2Pool, qualifiedTableName, id)
		originalTS := originalTimestamps[id]
		tsTrunc := repairedTS.Truncate(time.Microsecond)
		originalTsTrunc := originalTS.Truncate(time.Microsecond)
		if tsTrunc.Equal(originalTsTrunc) {
			preservedCount++
			log.Printf("  Row %d: timestamp PRESERVED (%s)", id, repairedTS.Format(time.RFC3339Nano))
		} else {
			log.Printf("  Row %d: timestamp NOT preserved (original=%s repaired=%s)",
				id, originalTS.Format(time.RFC3339Nano), repairedTS.Format(time.RFC3339Nano))
		}
	}
	require.Equal(t, len(sampleIDs), preservedCount,
		"All rows must have commit timestamp preserved: %d/%d", preservedCount, len(sampleIDs))

	log.Printf("Native PG preserve-origin verified: %d/%d origins and timestamps preserved", preservedCount, len(sampleIDs))
}
