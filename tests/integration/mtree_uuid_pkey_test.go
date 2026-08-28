package integration

// "mtree table-diff" failed on any table with a uuid primary key, as soon as
// the two nodes really disagreed on a row.
//
// readRowHashes built its map key by printing the scanned pkey, and the keys of
// the mismatched rows were then used as parameters of the row-fetch query. pgx
// decodes a uuid as [16]byte, so the parameter was the text "[17 17 ...]" and
// the server rejected it with SQLSTATE 22P02. The worker error was logged and
// then dropped, so the run still finished with "TABLES MATCH".
//
// So the diff must find the differing row AND must not claim a match.
//
// The table is deliberately NOT added to a spock replication set. The two
// nodes have to stay divergent for the whole test, and spock.repair_mode does
// not achieve that: it was measured to still replicate the change to the peer,
// after which both nodes agree again and the diff has nothing to find. ACE
// does not need the spock repset either -- mtree init puts the table into ACE's
// own publication and slot.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

const uuidPkeyRowCount = 500

func TestMtreeUUIDPrimaryKeyDiff(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()

	tableName := "mtree_uuid_pkey"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()

	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, pool := range pools {
		_, err := pool.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+safe+" (id uuid PRIMARY KEY, val text)") // nosemgrep
		require.NoError(t, err)
	}
	t.Cleanup(func() {
		for _, pool := range pools {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		}
	})

	// md5 spreads the keys over the whole uuid range, so their byte order has
	// nothing to do with insertion order and the block bounds really have to be
	// sorted as uuids.
	for _, pool := range pools {
		_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
			"INSERT INTO %s (id, val) SELECT md5(g::text)::uuid, 'same' "+
				"FROM generate_series(1, %d) g", safe, uuidPkeyRowCount))
		require.NoError(t, err)
		_, err = pool.Exec(ctx, "ANALYZE "+safe) // nosemgrep
		require.NoError(t, err)
	}

	// Remove the diff report this test produces. Left behind, a later test that
	// repairs from "the most recent diff file" picks it up and fails with a
	// table mismatch -- measured against TestMerkleTreeSimplePK.
	t.Cleanup(func() {
		files, _ := filepath.Glob("*_diffs-*.json")
		for _, f := range files {
			os.Remove(f)
		}
		files, _ = filepath.Glob("*_diffs-*.html")
		for _, f := range files {
			os.Remove(f)
		}
	})

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	// Several blocks, so the bounds are really cut, sorted and merged. With one
	// block there is nothing to order and the test would pass by accident.
	task.BlockSize = 100
	task.OverrideBlockSize = true
	require.NoError(t, task.RunChecks(false))
	require.NoError(t, task.MtreeInit())
	t.Cleanup(func() { _ = task.MtreeTeardown() })
	require.NoError(t, task.BuildMtree())

	// Pick the row by the server's own ordering, so it lands inside a block
	// rather than on a boundary.
	var divergedID string
	require.NoError(t, env.N1Pool.QueryRow(ctx, fmt.Sprintf( // nosemgrep
		"SELECT id::text FROM %s ORDER BY id OFFSET %d LIMIT 1", safe, uuidPkeyRowCount/2)).Scan(&divergedID))

	tag, err := env.N1Pool.Exec(ctx, // nosemgrep
		"UPDATE "+safe+" SET val = 'diverged' WHERE id = $1::uuid", divergedID)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected(), "exactly one row must diverge")

	// Verify the arrange step before acting on it: if the change had reached
	// the peer, the nodes would still be identical and everything below would
	// pass or fail for the wrong reason.
	var n1Val, n2Val string
	require.NoError(t, env.N1Pool.QueryRow(ctx, // nosemgrep
		"SELECT val FROM "+safe+" WHERE id = $1::uuid", divergedID).Scan(&n1Val))
	require.NoError(t, env.N2Pool.QueryRow(ctx, // nosemgrep
		"SELECT val FROM "+safe+" WHERE id = $1::uuid", divergedID).Scan(&n2Val))
	require.NotEqual(t, n1Val, n2Val,
		"the nodes must actually diverge before the diff runs (n1=%q n2=%q)", n1Val, n2Val)

	// Before the fix this returned nil after it logged a 22P02 from the worker.
	require.NoError(t, task.DiffMtree(), "diff of a uuid-PK table must not fail")

	require.Empty(t, task.DiffResult.Summary.IncompletePairs,
		"the comparison must finish: a failed work item would make the result meaningless")

	total := 0
	for _, c := range task.DiffResult.Summary.DiffRowsCount {
		total += c
	}
	require.Equal(t, 1, total, "expected exactly the one diverging row to be reported")

	// table-repair reads this report, so the pkey has to be a value Postgres
	// can parse back, not a printed byte array.
	found := false
	for _, pairDiff := range task.DiffResult.NodeDiffs {
		for _, rows := range pairDiff.Rows {
			for _, row := range rows {
				for _, kv := range row {
					if kv.Key == "id" {
						require.Equal(t, divergedID, fmt.Sprintf("%v", kv.Value),
							"the primary key must come back as normal uuid text")
						found = true
					}
				}
			}
		}
	}
	require.True(t, found, "diff report must contain the diverging row")
}
