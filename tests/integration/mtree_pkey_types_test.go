package integration

// A property test over primary key types, instead of one hand-picked example
// per type.
//
// The property is the whole promise of mtree: if N rows differ between two
// nodes, the diff must report exactly those N. The diff builds the key ranges
// to compare from the bounds of the mismatched leaves. If the bounds were
// ordered in another way than the server orders the key type, a range would
// select nothing, the block would never be compared, and the diff would report
// a match it never checked. A wrong order does not always empty a range --
// often it only widens one -- so such a defect misses rows quietly instead of
// failing.
//
// Three things about the setup are load-bearing. The divergence has to span
// many blocks: with a single differing row there is one mismatched leaf, two
// bounds and a single comparison, which a wrong comparator gets right half the
// time by luck. That was measured -- with one row, breaking the uuid ordering
// on purpose did not turn this test red. The block size has to leave
// several blocks, because with one block there is nothing to order and every
// case passes by accident. And the table is deliberately NOT added to a spock
// replication set: the nodes have to stay divergent for the whole test, and
// spock.repair_mode does not achieve that -- it was measured to still
// replicate the change to the peer, after which both nodes agree again and the
// diff has nothing to find. ACE does not need the spock repset either, since
// mtree init puts the table into ACE's own publication and slot.

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pkeyTypeCase describes one primary key type. valuesSQL must produce exactly
// rowCount distinct values of pkType, in any order.
type pkeyTypeCase struct {
	name      string
	pkType    string
	valuesSQL string
}

const (
	pkeyTypeRowCount = 500
	// The diverging rows are spread over the interior of the key range, every
	// pkeyTypeDivergeEvery-th row. Both ends are left alone on purpose: the
	// last leaf of a tree carries range_end = NULL, so if it is among the
	// mismatched blocks getPkeyBatches adds an open-ended slice. That slice
	// covers everything the ordering may have got wrong, and the test then
	// passes even with the bound comparison fully reversed -- measured.
	pkeyTypeDivergeFrom  = 150
	pkeyTypeDivergeTo    = 350
	pkeyTypeDivergeEvery = 25
	pkeyTypeDivergedRows = (pkeyTypeDivergeTo-pkeyTypeDivergeFrom)/pkeyTypeDivergeEvery + 1
)

// pkeyTypeCases returns the key types to test, with pkeyTypeRowCount rows of
// each.
func pkeyTypeCases() []pkeyTypeCase {
	g := fmt.Sprintf("generate_series(1, %d) g", pkeyTypeRowCount)
	return []pkeyTypeCase{
		{
			name:      "bigint",
			pkType:    "BIGINT",
			valuesSQL: "SELECT (g * 7919)::bigint FROM " + g,
		},
		{
			name:   "uuid",
			pkType: "UUID",
			// Hashed, so the values are spread over the whole uuid range and
			// their byte order has nothing to do with insertion order.
			valuesSQL: "SELECT md5(g::text)::uuid FROM " + g,
		},
		{
			name:      "bytea",
			pkType:    "BYTEA",
			valuesSQL: "SELECT decode(md5(g::text), 'hex') FROM " + g,
		},
		{
			name:   "text_c_collation",
			pkType: `TEXT COLLATE "C"`,
			// Mixed case on purpose: this is where a byte-wise order and a
			// linguistic one disagree.
			valuesSQL: "SELECT CASE WHEN g % 2 = 0 THEN upper(md5(g::text)) ELSE md5(g::text) END FROM " + g,
		},
		{
			// The first character alternates case: the byte order puts
			// every uppercase before every lowercase, en_US interleaves
			// them. An order of the bounds by bytes loses rows here.
			name:      "text_en_us_collation",
			pkType:    `TEXT COLLATE "en_US.utf8"`,
			valuesSQL: "SELECT CASE WHEN g % 2 = 0 THEN chr(65 + g % 26) ELSE chr(97 + g % 26) END || md5(g::text) FROM " + g,
		},
		{
			name:      "timestamptz",
			pkType:    "TIMESTAMPTZ",
			valuesSQL: "SELECT '2020-01-01Z'::timestamptz + (g * 977) * interval '1 second' FROM " + g,
		},
		{
			name:      "double_precision",
			pkType:    "DOUBLE PRECISION",
			valuesSQL: "SELECT (g * 7919)::double precision / 97 FROM " + g,
		},
		{
			name:      "text_database_collation",
			pkType:    "TEXT",
			valuesSQL: "SELECT CASE WHEN g % 2 = 0 THEN upper(md5(g::text)) ELSE md5(g::text) END FROM " + g,
		},
	}
}

func TestMtreePkeyTypesFindDivergence(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, tc := range pkeyTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			tableName := "mtree_pkey_" + tc.name
			qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
			safe := pgx.Identifier{testSchema, tableName}.Sanitize()

			for _, pool := range pools {
				_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
					"CREATE TABLE IF NOT EXISTS %s (id %s PRIMARY KEY, val TEXT)", safe, tc.pkType))
				require.NoError(t, err)
			}
			t.Cleanup(func() {
				for _, pool := range pools {
					execOK(t, ctx, pool, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
				}
			})

			for _, pool := range pools {
				_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
					"INSERT INTO %s (id, val) SELECT v, 'same' FROM (%s) s(v)", safe, tc.valuesSQL))
				require.NoError(t, err)
				_, err = pool.Exec(ctx, "ANALYZE "+safe) // nosemgrep
				require.NoError(t, err)
			}

			t.Cleanup(func() { removeDiffResults(t, qualified) })

			task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
			// Several blocks, so the bounds really are cut, sorted and merged.
			// With one block there is nothing to order and every case would
			// pass by accident.
			task.BlockSize = 100
			task.OverrideBlockSize = true
			require.NoError(t, task.RunChecks(false))
			require.NoError(t, task.MtreeInit())
			t.Cleanup(func() { assert.NoError(t, task.MtreeTeardown()) })
			require.NoError(t, task.BuildMtree())

			// Diverge rows spread evenly over the key order, so every block
			// holds at least one and the bound set is large enough for a wrong
			// comparator to invert some pair of it.
			tag, err := env.N1Pool.Exec(ctx, fmt.Sprintf( // nosemgrep
				`WITH ranked AS (SELECT id, row_number() OVER (ORDER BY id) AS rn FROM %s)
				 UPDATE %s SET val = 'diverged'
				 WHERE id IN (SELECT id FROM ranked WHERE rn BETWEEN %d AND %d AND rn %% %d = 0)`,
				safe, safe, pkeyTypeDivergeFrom, pkeyTypeDivergeTo, pkeyTypeDivergeEvery))
			require.NoError(t, err)
			require.EqualValues(t, pkeyTypeDivergedRows, tag.RowsAffected(),
				"the divergence must land in the interior blocks")

			// Verify the arrange step before acting on it: a test that assumes
			// a divergence it never created would pass for the wrong reason.
			var stillSame int
			require.NoError(t, env.N2Pool.QueryRow(ctx, // nosemgrep
				"SELECT count(*) FROM "+safe+" WHERE val = 'diverged'").Scan(&stillSame))
			require.Zero(t, stillSame, "the change must not have reached the peer node")

			require.NoError(t, task.DiffMtree())

			total := 0
			for _, c := range task.DiffResult.Summary.DiffRowsCount {
				total += c
			}
			require.Equal(t, pkeyTypeDivergedRows, total,
				"%d rows differ, so the diff must report exactly that many; a smaller number "+
					"means some block was skipped because its bounds were ordered differently "+
					"than on the server", pkeyTypeDivergedRows)
		})
	}
}
