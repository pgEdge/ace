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
	"github.com/pgedge/ace/pkg/config"
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

// leafKeyTypeCase is a key type for a tree whose leaves differ on the two
// nodes. base makes the rows of both nodes from n = 1..2000; extra makes
// one more row for each n in 600..899, which sorts next to the base row of
// that n, and gives it n + 10000.
type leafKeyTypeCase struct {
	name  string
	setup []string
	ddl   string
	base  string
	extra string
}

// leafKeyTypeCases returns the key types to test with leaves that differ on
// the two nodes.
func leafKeyTypeCases() []leafKeyTypeCase {
	text := "CASE WHEN g % 2 = 0 THEN chr(65 + g % 26) ELSE chr(97 + g % 26) END || lpad(g::text, 6, '0')"
	return []leafKeyTypeCase{
		{
			name:  "text_en_us",
			ddl:   `(id TEXT COLLATE "en_US.utf8" PRIMARY KEY, n int, v text)`,
			base:  "SELECT " + text + ", g, 'v' FROM generate_series(1, 2000) g",
			extra: "SELECT " + text + " || 'x', g + 10000, 'v' FROM generate_series(600, 899) g",
		},
		{
			name: "enum",
			setup: []string{
				fmt.Sprintf("DROP TYPE IF EXISTS %s.leaf_color CASCADE", testSchema),
				fmt.Sprintf("CREATE TYPE %s.leaf_color AS ENUM ('red', 'green', 'blue')", testSchema),
			},
			ddl:   fmt.Sprintf("(c %s.leaf_color, n int, v text, PRIMARY KEY (c, n))", testSchema),
			base:  fmt.Sprintf("SELECT (ARRAY['red', 'green', 'blue'])[1 + g / 700]::%s.leaf_color, g, 'v' FROM generate_series(1, 2000) g", testSchema),
			extra: fmt.Sprintf("SELECT (ARRAY['red', 'green', 'blue'])[1 + g / 700]::%s.leaf_color, g + 10000, 'v' FROM generate_series(600, 899) g", testSchema),
		},
		{
			name:  "numeric",
			ddl:   "(id numeric PRIMARY KEY, n int, v text)",
			base:  "SELECT g * 1.5, g, 'v' FROM generate_series(1, 2000) g",
			extra: "SELECT g * 1.5 + 0.001, g + 10000, 'v' FROM generate_series(600, 899) g",
		},
	}
}

// After rows are added on one node only, mtree update splits the leaves of
// that node, and the two trees have different leaf bounds. The diff builds
// its ranges from the bounds of both nodes, sorted by the server in the
// order of the key type, and must report exactly the added rows.
func TestMtreeKeyTypesWithDifferentLeaves(t *testing.T) {
	ctx := context.Background()
	for _, tc := range leafKeyTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			env := newSpockEnv()
			for _, pool := range env.pools() {
				for _, sql := range tc.setup {
					_, err := pool.Exec(ctx, sql) // nosemgrep
					require.NoError(t, err, sql)
				}
			}
			tableName := "mtree_leaves_" + tc.name
			qualified, safe := newUnreplicatedTable(t, env, tableName, tc.ddl)
			for _, pool := range env.pools() {
				_, err := pool.Exec(ctx, "INSERT INTO "+safe+" "+tc.base) // nosemgrep
				require.NoError(t, err)
			}
			analyzeTable(t, env, safe)

			task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
			task.BlockSize = 100
			task.OverrideBlockSize = true
			require.NoError(t, task.RunChecks(false))
			require.NoError(t, task.MtreeInit())
			t.Cleanup(func() { assert.NoError(t, task.MtreeTeardown()) })
			require.NoError(t, task.BuildMtree())
			t.Cleanup(func() { removeDiffResults(t, qualified) })

			_, err := env.N2Pool.Exec(ctx, "INSERT INTO "+safe+" "+tc.extra) // nosemgrep
			require.NoError(t, err)
			require.NoError(t, task.DiffMtree())

			mtreeTable := pgx.Identifier{
				config.Cfg.MTree.Schema,
				fmt.Sprintf("ace_mtree_%s_%s", testSchema, tableName),
			}.Sanitize()
			leaves := func(pool *pgxpool.Pool) int {
				var n int
				require.NoError(t, pool.QueryRow(ctx,
					"SELECT count(*) FROM "+mtreeTable+" WHERE node_level = 0").Scan(&n)) // nosemgrep
				return n
			}
			require.Greater(t, leaves(env.N2Pool), leaves(env.N1Pool), "only n2 should have split leaves")

			nodeDiffs := diffPairs(t, task)[env.pairKey()]
			require.Empty(t, extractDiffIDsByColumn(nodeDiffs.Rows[env.ServiceN1], "n"))
			require.Equal(t, idRange(10600, 10899), extractDiffIDsByColumn(nodeDiffs.Rows[env.ServiceN2], "n"))
		})
	}
}

// A key column with another collation on one node orders the key in another
// way there. mtree builds its leaves and the ranges of the diff in the order
// of one node, so it refuses such a table; table-diff still compares it.
func TestKeyCollationDiffersByNode(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	tableName := "key_collation_by_node"
	qualified := fmt.Sprintf("%s.%s", testSchema, tableName)
	safe := pgx.Identifier{testSchema, tableName}.Sanitize()
	for i, pool := range env.pools() {
		coll := `"C"`
		if i == 1 {
			coll = `"en_US.utf8"`
		}
		_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
			"CREATE TABLE %s (id text COLLATE %s PRIMARY KEY, v text)", safe, coll))
		require.NoError(t, err)
		t.Cleanup(func() {
			execOK(t, context.Background(), pool, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
		})
		_, err = pool.Exec(ctx, "INSERT INTO "+safe+" VALUES ('a', 'x'), ('B', 'x'), ('c', 'x')") // nosemgrep
		require.NoError(t, err)
	}

	task := env.newMerkleTreeTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	err := task.RunChecks(false)
	require.Error(t, err)
	require.Contains(t, err.Error(), `primary key column "id" has collation`)
	require.Contains(t, err.Error(), "mtree needs the same key collations on every node")

	t.Cleanup(func() { removeDiffResults(t, qualified) })
	td := newTestTableDiffTask(t, qualified, []string{env.ServiceN1, env.ServiceN2})
	require.NoError(t, td.RunChecks(false))
	require.NoError(t, td.ExecuteTask())
}
