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
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/internal/consistency/diff"
	"github.com/pgedge/ace/internal/consistency/mtree"
	"github.com/pgedge/ace/internal/consistency/repair"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testEnv abstracts the differences between spock and native PG test environments,
// allowing the same test logic to run against both.
type testEnv struct {
	N1Pool *pgxpool.Pool
	N2Pool *pgxpool.Pool
	N3Pool *pgxpool.Pool // nil for native PG (only 2 nodes)

	N1Host string
	N1Port string
	N2Host string
	N2Port string

	ServiceN1 string
	ServiceN2 string
	ServiceN3 string // empty for native PG

	DBName     string
	DBUser     string
	DBPassword string

	ClusterName  string
	ClusterNodes []map[string]any

	HasSpock bool
	Schema   string
}

func newSpockEnv() *testEnv {
	return &testEnv{
		N1Pool:       pgCluster.Node1Pool,
		N2Pool:       pgCluster.Node2Pool,
		N3Pool:       pgCluster.Node3Pool,
		N1Host:       pgCluster.Node1Host,
		N1Port:       pgCluster.Node1Port,
		N2Host:       pgCluster.Node2Host,
		N2Port:       pgCluster.Node2Port,
		ServiceN1:    serviceN1,
		ServiceN2:    serviceN2,
		ServiceN3:    serviceN3,
		DBName:       dbName,
		DBUser:       pgEdgeUser,
		DBPassword:   pgEdgePassword,
		ClusterName:  pgCluster.ClusterName,
		ClusterNodes: pgCluster.ClusterNodes,
		HasSpock:     true,
		Schema:       testSchema,
	}
}

func newNativeEnv(state *nativeClusterState) *testEnv {
	return &testEnv{
		N1Pool:    state.n1Pool,
		N2Pool:    state.n2Pool,
		N3Pool:    nil,
		N1Host:    state.n1Host,
		N1Port:    state.n1Port,
		N2Host:    state.n2Host,
		N2Port:    state.n2Port,
		ServiceN1: nativeServiceN1,
		ServiceN2: nativeServiceN2,
		ServiceN3: "",
		DBName:    nativeDBName,
		DBUser:    nativeUser,
		DBPassword: nativePassword,
		ClusterName:  nativeClusterName,
		ClusterNodes: []map[string]any{
			{
				"Name":       nativeServiceN1,
				"PublicIP":   state.n1Host,
				"Port":       state.n1Port,
				"DBUser":     nativeUser,
				"DBPassword": nativePassword,
				"DBName":     nativeDBName,
			},
			{
				"Name":       nativeServiceN2,
				"PublicIP":   state.n2Host,
				"Port":       state.n2Port,
				"DBUser":     nativeUser,
				"DBPassword": nativePassword,
				"DBName":     nativeDBName,
			},
		},
		HasSpock: false,
		Schema:   testSchema,
	}
}

// withRepairMode acquires a single connection from the pool, enables
// spock.repair_mode on it (when available), runs fn, then disables repair mode
// and releases the connection. On native PG this simply pins a connection for
// the duration of fn. All work inside fn must use the provided conn so that
// repair_mode is in effect.
func (e *testEnv) withRepairMode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, fn func(conn *pgxpool.Conn)) {
	t.Helper()
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err, "acquire connection for repair mode")
	defer conn.Release()

	if e.HasSpock {
		_, err := conn.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err, "enable spock.repair_mode")
		defer func() {
			_, err := conn.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err, "disable spock.repair_mode")
		}()
	}
	fn(conn)
}

// withRepairModeTx is like withRepairMode but operates on a transaction.
func (e *testEnv) withRepairModeTx(t *testing.T, ctx context.Context, tx pgx.Tx, fn func()) {
	t.Helper()
	if e.HasSpock {
		_, err := tx.Exec(ctx, "SELECT spock.repair_mode(true)")
		require.NoError(t, err, "enable spock.repair_mode in tx")
		defer func() {
			_, err := tx.Exec(ctx, "SELECT spock.repair_mode(false)")
			require.NoError(t, err, "disable spock.repair_mode in tx")
		}()
	}
	fn()
}

// pools returns the two primary pools (n1, n2) used by most tests.
func (e *testEnv) pools() []*pgxpool.Pool {
	return []*pgxpool.Pool{e.N1Pool, e.N2Pool}
}

// newTableDiffTask creates a TableDiffTask configured for this environment.
func (e *testEnv) newTableDiffTask(t *testing.T, qualifiedTableName string, nodes []string) *diff.TableDiffTask {
	t.Helper()
	task := diff.NewTableDiffTask()
	task.ClusterName = e.ClusterName
	task.DBName = e.DBName
	task.QualifiedTableName = qualifiedTableName
	task.Nodes = strings.Join(nodes, ",")
	task.Output = "json"
	task.BlockSize = 1000
	task.CompareUnitSize = 100
	task.ConcurrencyFactor = 1
	task.MaxDiffRows = math.MaxInt64

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

// newTableRepairTask creates a TableRepairTask configured for this environment.
func (e *testEnv) newTableRepairTask(sourceOfTruthNode, qualifiedTableName, diffFilePath string) *repair.TableRepairTask {
	task := repair.NewTableRepairTask()
	task.ClusterName = e.ClusterName
	task.DBName = e.DBName
	task.SourceOfTruth = sourceOfTruthNode
	task.QualifiedTableName = qualifiedTableName
	task.DiffFilePath = diffFilePath
	task.Nodes = "all"
	return task
}

// setupDivergence prepares a table with a known set of differences between n1 and n2.
// - 5 common rows (index 1-5)
// - 2 rows only on n1 (index 1001, 1002)
// - 2 rows only on n2 (index 2001, 2002)
// - 2 common rows modified on n2 (index 1, 2)
func (e *testEnv) setupDivergence(t *testing.T, ctx context.Context, qualifiedTableName string) {
	t.Helper()
	log.Println("Setting up data divergence for", qualifiedTableName)

	// Truncate on both nodes
	for _, pool := range e.pools() {
		e.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
			require.NoError(t, err, "truncate table")
		})
	}

	// Insert common rows
	for i := 1; i <= 5; i++ {
		for _, pool := range e.pools() {
			e.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
				_, err := conn.Exec(ctx,
					fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email) VALUES ($1, $2, $3, $4, $5)", qualifiedTableName),
					i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("FirstName%d", i), fmt.Sprintf("LastName%d", i), fmt.Sprintf("email%d@example.com", i))
				require.NoError(t, err)
			})
		}
	}

	// Rows only on n1
	e.withRepairMode(t, ctx, e.N1Pool, func(conn *pgxpool.Conn) {
		for i := 1001; i <= 1002; i++ {
			_, err := conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email) VALUES ($1, $2, $3, $4, $5)", qualifiedTableName),
				i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("N1OnlyFirst%d", i), fmt.Sprintf("N1OnlyLast%d", i), fmt.Sprintf("n1.only%d@example.com", i))
			require.NoError(t, err)
		}
	})

	// Rows only on n2
	e.withRepairMode(t, ctx, e.N2Pool, func(conn *pgxpool.Conn) {
		for i := 2001; i <= 2002; i++ {
			_, err := conn.Exec(ctx,
				fmt.Sprintf("INSERT INTO %s (index, customer_id, first_name, last_name, email) VALUES ($1, $2, $3, $4, $5)", qualifiedTableName),
				i, fmt.Sprintf("CUST-%d", i), fmt.Sprintf("N2OnlyFirst%d", i), fmt.Sprintf("N2OnlyLast%d", i), fmt.Sprintf("n2.only%d@example.com", i))
			require.NoError(t, err)
		}
	})

	// Modify rows on n2
	e.withRepairMode(t, ctx, e.N2Pool, func(conn *pgxpool.Conn) {
		for i := 1; i <= 2; i++ {
			_, err := conn.Exec(ctx,
				fmt.Sprintf("UPDATE %s SET email = $1 WHERE index = $2", qualifiedTableName),
				fmt.Sprintf("modified.email%d@example.com", i), i)
			require.NoError(t, err)
		}
	})

	log.Println("Data divergence setup complete.")
}

// setupNullDivergence creates divergence where the same rows exist on both nodes
// but with different columns set to NULL.
func (e *testEnv) setupNullDivergence(t *testing.T, ctx context.Context, qualifiedTableName string) {
	t.Helper()
	log.Println("Setting up null divergence for", qualifiedTableName)

	for _, pool := range e.pools() {
		e.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
			require.NoError(t, err, "truncate table")
		})
	}

	insertSQL := fmt.Sprintf(
		"INSERT INTO %s (index, customer_id, first_name, last_name, city) VALUES ($1, $2, $3, $4, $5)",
		qualifiedTableName,
	)

	// Node1: missing city for id 1, missing first_name for id 2
	e.withRepairMode(t, ctx, e.N1Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, insertSQL, 1, "CUST-1", "Michael", "Schumacher", nil)
		require.NoError(t, err)
		_, err = conn.Exec(ctx, insertSQL, 2, "CUST-2", nil, "Alonso", "Oviedo")
		require.NoError(t, err)
	})

	// Node2: missing last_name for id 1, missing city for id 2
	e.withRepairMode(t, ctx, e.N2Pool, func(conn *pgxpool.Conn) {
		_, err := conn.Exec(ctx, insertSQL, 1, "CUST-1", "Michael", nil, "Austria")
		require.NoError(t, err)
		_, err = conn.Exec(ctx, insertSQL, 2, "CUST-2", "Fernando", "Alonso", nil)
		require.NoError(t, err)
	})
}

// runTableDiff executes a table-diff and returns the path to the latest diff file.
func (e *testEnv) runTableDiff(t *testing.T, qualifiedTableName string, nodesToCompare []string) string {
	t.Helper()
	files, _ := filepath.Glob("*_diffs-*.json")
	for _, f := range files {
		os.Remove(f)
	}

	tdTask := e.newTableDiffTask(t, qualifiedTableName, nodesToCompare)
	err := tdTask.RunChecks(false)
	require.NoError(t, err, "table-diff validation failed")
	err = tdTask.ExecuteTask()
	require.NoError(t, err, "table-diff execution failed")

	latestDiffFile := getLatestDiffFile(t)
	require.NotEmpty(t, latestDiffFile, "No diff file was generated")
	return latestDiffFile
}

// assertNoTableDiff runs a diff and asserts that there are no differences.
func (e *testEnv) assertNoTableDiff(t *testing.T, qualifiedTableName string) {
	t.Helper()
	nodesToCompare := []string{e.ServiceN1, e.ServiceN2}
	tdTask := e.newTableDiffTask(t, qualifiedTableName, nodesToCompare)

	err := tdTask.RunChecks(false)
	require.NoError(t, err, "assertNoTableDiff: validation failed")
	err = tdTask.ExecuteTask()
	require.NoError(t, err, "assertNoTableDiff: execution failed")

	assert.Empty(t, tdTask.DiffResult.NodeDiffs, "Expected no differences after repair, but diffs were found")
}

// repairTable finds the latest diff file and runs a repair.
func (e *testEnv) repairTable(t *testing.T, qualifiedTableName, sourceOfTruthNode string) {
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

	repairTask := e.newTableRepairTask(sourceOfTruthNode, qualifiedTableName, latestDiffFile)
	time.Sleep(2 * time.Second)
	if err := repairTask.Run(false); err != nil {
		t.Fatalf("Failed to repair table: %v", err)
	}
	log.Printf("Table '%s' repaired successfully using %s as source of truth.", qualifiedTableName, sourceOfTruthNode)
}

// resetSharedTable truncates and reloads the given table from CSV on n1 and n2.
func (e *testEnv) resetSharedTable(t *testing.T, tableName string) {
	t.Helper()
	ctx := context.Background()
	qualifiedTableName := fmt.Sprintf("%s.%s", e.Schema, tableName)
	csvPath, err := filepath.Abs(defaultCsvFilePath + tableName + ".csv")
	require.NoError(t, err)
	for _, pool := range e.pools() {
		e.withRepairMode(t, ctx, pool, func(conn *pgxpool.Conn) {
			_, err := conn.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", qualifiedTableName))
			require.NoError(t, err, "truncate %s", qualifiedTableName)
		})
		require.NoError(t, loadDataFromCSV(ctx, pool, e.Schema, tableName, csvPath), "load CSV into %s", qualifiedTableName)
	}
}

// pairKey returns the canonical node pair key for diff results.
func (e *testEnv) pairKey() string {
	if strings.Compare(e.ServiceN1, e.ServiceN2) > 0 {
		return e.ServiceN2 + "/" + e.ServiceN1
	}
	return e.ServiceN1 + "/" + e.ServiceN2
}

// awaitDataSync waits until n1 and n2 have the same row count for the given
// table. This prevents inter-test bleed when a previous subtest's cleanup
// repair is still replicating.
func (e *testEnv) awaitDataSync(t *testing.T, qualifiedTableName string) {
	t.Helper()
	ctx := context.Background()
	assertEventually(t, 30*time.Second, func() error {
		var n1Count, n2Count int
		if err := e.N1Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&n1Count); err != nil {
			return fmt.Errorf("counting rows on n1: %w", err)
		}
		if err := e.N2Pool.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s", qualifiedTableName)).Scan(&n2Count); err != nil {
			return fmt.Errorf("counting rows on n2: %w", err)
		}
		if n1Count != n2Count {
			return fmt.Errorf("nodes not in sync for %s: n1=%d n2=%d", qualifiedTableName, n1Count, n2Count)
		}
		return nil
	})
}

// newMerkleTreeTask creates a MerkleTreeTask configured for this environment.
func (e *testEnv) newMerkleTreeTask(t *testing.T, qualifiedTableName string, nodes []string) *mtree.MerkleTreeTask {
	t.Helper()
	task := mtree.NewMerkleTreeTask()
	task.ClusterName = e.ClusterName
	task.DBName = e.DBName
	task.QualifiedTableName = qualifiedTableName
	task.Nodes = strings.Join(nodes, ",")
	task.BlockSize = 1000
	return task
}
