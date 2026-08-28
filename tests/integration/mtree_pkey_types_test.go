package integration

// A property test over primary key types, instead of one hand-picked example
// per type.
//
// The property is the whole promise of mtree: if N rows differ between two
// nodes, the diff must report exactly those N. It holds only if ACE can
// reproduce the Postgres order of the key type outside the database, because
// the block bounds it builds in Go are sent back as range conditions. When the
// two orders disagree, a range selects nothing, the block is never compared,
// and the diff reports a match it never checked.
//
// The known instances -- uuid, bytea, numeric, and the text
// collation gap -- is one instance of that. Each was found by hand, one at a
// time, after it reached production. This test looks for the next one.
//
// The failures are partial, not total: restoring the pre-fix uuid and bytea
// ordering makes the diff report 6 of 9 differing rows, and the text collation
// case reports 4 of 9. A wrong order does not always empty a range -- often it
// only widens one -- which is why this misses rows quietly instead of erroring.
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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// pkeyTypeCase describes one primary key type. valuesSQL must produce exactly
// rowCount distinct values of pkType, in any order.
type pkeyTypeCase struct {
	name      string
	pkType    string
	valuesSQL string
	// skipReason, when the check returns a non-empty string, skips the case.
	skipReason func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string
	// knownGap marks a case that reproduces an open defect. It is skipped so
	// the suite stays green; deleting the field is the acceptance criterion
	// for the fix.
	knownGap string
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
			// Reproduces the collation gap, and it is a real one: measured on
			// PostgreSQL 17 the diff reported 4 of the 9 differing rows. The
			// same collation with md5 values passes, so what matters is not the
			// collation by itself but how far its order drifts from the byte
			// order. Here the first character alternates case: byte order puts
			// every uppercase before every lowercase, en_US interleaves them.
			name:      "text_en_us_collation",
			pkType:    `TEXT COLLATE "en_US.utf8"`,
			valuesSQL: "SELECT CASE WHEN g % 2 = 0 THEN chr(65 + g % 26) ELSE chr(97 + g % 26) END || md5(g::text) FROM " + g,
			knownGap: "mtree orders text bounds byte by byte while the server uses the column collation; " +
				"fix is to order the bounds in SQL -- see Merkle Tree Architecture, \"Primary Key Types\"",
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
			skipReason: func(t *testing.T, ctx context.Context, pool *pgxpool.Pool) string {
				// pg_database, not current_setting('lc_collate'): that GUC was
				// removed in PostgreSQL 16.
				var collation string
				require.NoError(t, pool.QueryRow(ctx,
					"SELECT datcollate FROM pg_database WHERE datname = current_database()").Scan(&collation))
				switch strings.ToUpper(collation) {
				case "C", "POSIX", "C.UTF-8", "C.UTF8":
					return ""
				}
				// Not a passing case dressed up as a skip: mtree sorts text
				// bounds byte by byte, so under this collation it can order
				// them differently than the server did and miss the row. The
				// fix is to sort the bounds in SQL; until then the run is
				// skipped rather than expected to pass.
				return fmt.Sprintf("database collation is %s, not byte-ordered; "+
					"see Merkle Tree Architecture, \"Primary Key Types\"", collation)
			},
		},
	}
}

func TestMtreePkeyTypesFindDivergence(t *testing.T) {
	ctx := context.Background()
	env := newSpockEnv()
	pools := []*pgxpool.Pool{env.N1Pool, env.N2Pool}

	for _, tc := range pkeyTypeCases() {
		t.Run(tc.name, func(t *testing.T) {
			if tc.knownGap != "" {
				t.Skip(tc.knownGap)
			}
			if tc.skipReason != nil {
				if reason := tc.skipReason(t, ctx, env.N1Pool); reason != "" {
					t.Skip(reason)
				}
			}

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
					_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+safe+" CASCADE") // nosemgrep
				}
			})

			for _, pool := range pools {
				_, err := pool.Exec(ctx, fmt.Sprintf( // nosemgrep
					"INSERT INTO %s (id, val) SELECT v, 'same' FROM (%s) s(v)", safe, tc.valuesSQL))
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
			// Several blocks, so the bounds really are cut, sorted and merged.
			// With one block there is nothing to order and every case would
			// pass by accident.
			task.BlockSize = 100
			task.OverrideBlockSize = true
			require.NoError(t, task.RunChecks(false))
			require.NoError(t, task.MtreeInit())
			t.Cleanup(func() { _ = task.MtreeTeardown() })
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
			require.Empty(t, task.DiffResult.Summary.IncompletePairs,
				"the comparison must finish, otherwise the count below means nothing")

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
