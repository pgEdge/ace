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
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/stretchr/testify/require"
)

// blockHashes loads rows into a new temporary table and returns the mtree
// leaf hash and the table-diff block hash of the whole table. Temporary
// tables keep the test on one node: nothing is replicated.
func blockHashes(t *testing.T, ctx context.Context, conn *pgxpool.Conn, name, colDefs string, cols []string, colTypes map[string]string, values string) ([]byte, string) {
	t.Helper()

	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (id int PRIMARY KEY, %s)", name, colDefs)) // nosemgrep
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS pg_temp."+name) }) // nosemgrep
	if values != "" {
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES %s", name, values)) // nosemgrep
		require.NoError(t, err)
	}

	allCols := append([]string{"id"}, cols...)
	leaf, err := queries.ComputeLeafHashes(ctx, conn, "pg_temp", name, true, []string{"id"}, nil, nil, allCols, colTypes)
	require.NoError(t, err)

	tdSQL, err := queries.BlockHashSQL("pg_temp", name, []string{"id"}, "TD_BLOCK_HASH", false, false, "", allCols, colTypes)
	require.NoError(t, err)
	var td string
	require.NoError(t, conn.QueryRow(ctx, tdSQL).Scan(&td)) // nosemgrep

	checkBlockHash(t, ctx, conn, name, allCols, colTypes, leaf, td)
	return leaf, td
}

// checkBlockHash checks the mtree leaf hash and the table-diff block hash of
// the whole table name against each other and against the row hashes.
func checkBlockHash(t *testing.T, ctx context.Context, conn *pgxpool.Conn, name string, allCols []string, colTypes map[string]string, leaf []byte, td string) {
	t.Helper()

	// The mtree leaf and the table-diff block must be the same hash.
	require.Equal(t, hex.EncodeToString(leaf), td, "leaf hash and table-diff hash differ for %s", name)
	require.Len(t, leaf, sha256.Size)

	// The leaf must be built from the row hashes that the mtree diff reads.
	// Otherwise a leaf can differ while every row hash matches.
	rowExpr, err := queries.RowHashExpr("", allCols, colTypes)
	require.NoError(t, err)
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s FROM pg_temp.%s", rowExpr, name)) // nosemgrep
	require.NoError(t, err)
	var hashes [][]byte
	for rows.Next() {
		var h []byte
		require.NoError(t, rows.Scan(&h))
		require.Len(t, h, sha256.Size)
		hashes = append(hashes, h)
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, multisetHash(hashes), leaf, "leaf hash of %s does not match its row hashes", name)
}

// multisetHash is queries.BlockHashAggExpr written in Go: the sha256 of
// "count,sum1,sum2,sum3,sum4", where sumN is the sum of the N-th 64-bit word
// of every row hash, read as a signed big-endian integer. An empty block
// hashes to 32 zero bytes.
func multisetHash(rowHashes [][]byte) []byte {
	if len(rowHashes) == 0 {
		return make([]byte, sha256.Size)
	}
	var sums [4]big.Int
	for _, h := range rowHashes {
		for i := range sums {
			w := int64(binary.BigEndian.Uint64(h[8*i : 8*i+8]))
			sums[i].Add(&sums[i], big.NewInt(w))
		}
	}
	parts := []string{fmt.Sprint(len(rowHashes))}
	for i := range sums {
		parts = append(parts, sums[i].String())
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, ",")))
	return sum[:]
}

// inheritedBlockHashes loads rows into a new temporary parent table and its
// child, and returns the mtree leaf hash and the table-diff block hash of the
// parent. The parent's primary key does not cover the child, so the same key
// can be in both tables.
func inheritedBlockHashes(t *testing.T, ctx context.Context, conn *pgxpool.Conn, name, parentRows, childRows string) []byte {
	t.Helper()

	child := name + "_child"
	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (id int PRIMARY KEY, a text, b text)", name)) // nosemgrep
	require.NoError(t, err)
	_, err = conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (PRIMARY KEY (id)) INHERITS (%s)", child, name)) // nosemgrep
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS pg_temp."+name+" CASCADE") }) // nosemgrep
	for tbl, values := range map[string]string{name: parentRows, child: childRows} {
		if values == "" {
			continue
		}
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES %s", tbl, values)) // nosemgrep
		require.NoError(t, err)
	}

	allCols := []string{"id", "a", "b"}
	leaf, err := queries.ComputeLeafHashes(ctx, conn, "pg_temp", name, true, []string{"id"}, nil, nil, allCols, nil)
	require.NoError(t, err)
	tdSQL, err := queries.BlockHashSQL("pg_temp", name, []string{"id"}, "TD_BLOCK_HASH", false, false, "", allCols, nil)
	require.NoError(t, err)
	var td string
	require.NoError(t, conn.QueryRow(ctx, tdSQL).Scan(&td)) // nosemgrep

	checkBlockHash(t, ctx, conn, name, allCols, nil, leaf, td)
	return leaf
}

// TestBlockHashEncoding checks the block hash on a real server: different
// rows must give different hashes, and equal rows equal hashes. The block hash
// does not depend on the order of rows, so the test also checks cases where
// the order could matter.
func TestBlockHashEncoding(t *testing.T) {
	ctx := context.Background()
	conn, err := pgCluster.Node1Pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	textCols := []string{"a", "b"}
	textDefs := "a text, b text"

	mustDiffer := []struct {
		name  string
		left  string
		right string
	}{
		{"null_vs_empty_string", `(1, NULL, 'x')`, `(1, '', 'x')`},
		{"separator_moves", `(1, 'a|b', 'c')`, `(1, 'a', 'b|c')`},
		{"comma_moves", `(1, 'a,b', 'c')`, `(1, 'a', 'b,c')`},
		{"quote_and_paren", `(1, '"(', ')')`, `(1, '"', '()')`},
		{"backslash_moves", `(1, 'a\', 'b')`, `(1, 'a', '\b')`},
		{"text_null_vs_null", `(1, 'NULL', 'x')`, `(1, NULL, 'x')`},
		// concat_ws('|', ...) gave the text 1|a|b|2|c|d for both blocks.
		{"rows_merge_into_one", `(1, 'a', 'b'), (2, 'c', 'd')`, `(1, 'a', 'b|2|c|d')`},
		// The block hash ignores row order. Rows that swap their values must
		// still differ, because the primary key is part of each row hash.
		{"rows_swap_values", `(1, 'a', 'x'), (2, 'b', 'y')`, `(1, 'b', 'y'), (2, 'a', 'x')`},
		{"extra_row", `(1, 'a', 'x')`, `(1, 'a', 'x'), (2, 'a', 'x')`},
	}
	for i, tc := range mustDiffer {
		t.Run("differ/"+tc.name, func(t *testing.T) {
			left, _ := blockHashes(t, ctx, conn, fmt.Sprintf("bh_l%d", i), textDefs, textCols, nil, tc.left)
			right, _ := blockHashes(t, ctx, conn, fmt.Sprintf("bh_r%d", i), textDefs, textCols, nil, tc.right)
			require.NotEqual(t, left, right, "different rows must give different block hashes")
		})
	}

	t.Run("equal/same_rows", func(t *testing.T) {
		rows := `(1, 'a', NULL), (2, '', 'b,c'), (3, 'd|e', '"q"')`
		left, _ := blockHashes(t, ctx, conn, "bh_same_l", textDefs, textCols, nil, rows)
		right, _ := blockHashes(t, ctx, conn, "bh_same_r", textDefs, textCols, nil, rows)
		require.Equal(t, left, right)
	})

	t.Run("equal/numeric_trailing_zeros", func(t *testing.T) {
		// numeric without a fixed scale keeps 1.50 and 1.5 apart on disk;
		// trim_scale makes them hash the same.
		colTypes := map[string]string{"id": "integer", "amt": "numeric"}
		left, _ := blockHashes(t, ctx, conn, "bh_num_l", "amt numeric", []string{"amt"}, colTypes, `(1, 1.50), (2, 7.000)`)
		right, _ := blockHashes(t, ctx, conn, "bh_num_r", "amt numeric", []string{"amt"}, colTypes, `(1, 1.5), (2, 7)`)
		require.Equal(t, left, right)
	})

	t.Run("equal/insert_order", func(t *testing.T) {
		// The block hash depends on the rows, not on their physical order.
		left, _ := blockHashes(t, ctx, conn, "bh_ord_l", textDefs, textCols, nil, `(1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')`)
		right, _ := blockHashes(t, ctx, conn, "bh_ord_r", textDefs, textCols, nil, `(3, 'c', 'z'), (1, 'a', 'x'), (2, 'b', 'y')`)
		require.Equal(t, left, right)
	})

	t.Run("empty_block", func(t *testing.T) {
		leaf, _ := blockHashes(t, ctx, conn, "bh_empty", textDefs, textCols, nil, "")
		// A parent node is the XOR of its children, so an empty leaf must be
		// 32 zero bytes: then it does not change its parent.
		require.Equal(t, make([]byte, sha256.Size), leaf, "an empty block hashes to 32 zero bytes")
	})

	t.Run("single_row_pins_the_formula", func(t *testing.T) {
		// The row hash is the sha256 of the UTF8 text of ROW(...). The block
		// hash of one row is the sha256 of the count and of the four 64-bit
		// words of the row hash. This pins the whole formula, including the
		// way the bit(256) value is cut into signed words, independently of
		// multisetHash.
		leaf, _ := blockHashes(t, ctx, conn, "bh_one", textDefs, textCols, nil, `(1, 'café', NULL)`)
		row := sha256.Sum256([]byte("(1,café,)"))
		parts := []string{"1"}
		for i := 0; i < 4; i++ {
			parts = append(parts, fmt.Sprint(int64(binary.BigEndian.Uint64(row[8*i:8*i+8]))))
		}
		want := sha256.Sum256([]byte(strings.Join(parts, ",")))
		require.Equal(t, want[:], leaf)
	})

	t.Run("words_are_signed", func(t *testing.T) {
		// Cast to bigint, a bit(64) with the top bit set is negative. Find
		// rows whose row hashes start with a set and with a clear top bit,
		// and check them against multisetHash, which reads signed words.
		var neg, pos int
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT min(i) FILTER (WHERE get_byte(h, 0) >= 128), min(i) FILTER (WHERE get_byte(h, 0) < 128)
			FROM (SELECT i, sha256(convert_to(ROW(i, 'a', 'b')::text, 'UTF8')) AS h FROM generate_series(1, 64) AS i) s`).Scan(&neg, &pos))
		blockHashes(t, ctx, conn, "bh_sign", textDefs, textCols, nil, fmt.Sprintf(`(%d, 'a', 'b'), (%d, 'a', 'b')`, neg, pos))
	})

	// In an inheritance tree the parent's primary key does not cover the
	// children, so one key can appear more than once in a block. Each copy of
	// a row must count. A XOR of the row hashes would cancel two equal rows.
	inheritedDiffer := []struct {
		name                    string
		leftParent, leftChild   string
		rightParent, rightChild string
	}{
		// The row is in the parent and in the child on one node, and missing
		// on the other.
		{"duplicate_vs_missing",
			`(1, 'a', 'x'), (2, 'b', 'y')`, `(1, 'a', 'x')`,
			`(2, 'b', 'y')`, ``},
		// The row is in the parent and in the child on one node, and only in
		// the parent on the other.
		{"duplicate_vs_single",
			`(1, 'a', 'x')`, `(1, 'a', 'x')`,
			`(1, 'a', 'x')`, ``},
		// Both nodes have a duplicate, of different rows, and the same row
		// count: {X, X, Y} against {Y, W, W}. The XOR of the row hashes is Y
		// on both, so adding the row count to a XOR would not tell them apart.
		{"different_duplicates_same_count",
			`(1, 'a', 'x'), (2, 'b', 'y')`, `(1, 'a', 'x')`,
			`(2, 'b', 'y'), (3, 'c', 'z')`, `(3, 'c', 'z')`},
	}
	for i, tc := range inheritedDiffer {
		t.Run("differ/inherited_"+tc.name, func(t *testing.T) {
			left := inheritedBlockHashes(t, ctx, conn, fmt.Sprintf("bh_inh_l%d", i), tc.leftParent, tc.leftChild)
			right := inheritedBlockHashes(t, ctx, conn, fmt.Sprintf("bh_inh_r%d", i), tc.rightParent, tc.rightChild)
			require.NotEqual(t, left, right, "different multisets of rows must give different block hashes")
		})
	}

	t.Run("equal/inherited_row_placement", func(t *testing.T) {
		// ACE compares the rows of the whole tree. A row in the child hashes
		// the same as the same row in the parent.
		left := inheritedBlockHashes(t, ctx, conn, "bh_inh_pl", `(1, 'a', 'x'), (2, 'b', 'y')`, ``)
		right := inheritedBlockHashes(t, ctx, conn, "bh_inh_pr", `(1, 'a', 'x')`, `(2, 'b', 'y')`)
		require.Equal(t, left, right)
	})

	t.Run("equal/database_encoding", func(t *testing.T) {
		// Logical replication allows nodes with different database
		// encodings. The same rows must give the same hash in a LATIN1 and in
		// a UTF8 database: the row text is converted to UTF8 before it is
		// hashed. The client sends UTF8; the LATIN1 server converts it.
		const dbName = "ace_block_hash_latin1"
		_, err := conn.Exec(ctx, "CREATE DATABASE "+dbName+" ENCODING 'LATIN1' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0") // nosemgrep
		if err != nil {
			t.Skipf("cannot create a LATIN1 database: %v", err)
		}
		t.Cleanup(func() {
			_, _ = pgCluster.Node1Pool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)") // nosemgrep
		})

		cfg := pgCluster.Node1Pool.Config().Copy()
		cfg.ConnConfig.Database = dbName
		// pgx sends UTF8 but does not set client_encoding. Without it the
		// server takes the bytes as LATIN1 and stores other characters.
		if cfg.ConnConfig.RuntimeParams == nil {
			cfg.ConnConfig.RuntimeParams = map[string]string{}
		}
		cfg.ConnConfig.RuntimeParams["client_encoding"] = "UTF8"
		latinPool, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		t.Cleanup(latinPool.Close)
		latinConn, err := latinPool.Acquire(ctx)
		require.NoError(t, err)
		// Cleanups run in reverse order: blockHashes drops its tables
		// through this connection before it is released.
		t.Cleanup(latinConn.Release)

		var enc string
		require.NoError(t, latinConn.QueryRow(ctx, "SHOW server_encoding").Scan(&enc))
		require.Equal(t, "LATIN1", enc)

		// Every character here exists in LATIN1 and takes two bytes in UTF8.
		rows := `(1, 'café', 'Größe'), (2, 'Ångström', NULL), (3, '', 'ÿ,"ü"')`
		utf8Hash, _ := blockHashes(t, ctx, conn, "bh_enc", textDefs, textCols, nil, rows)
		latinHash, _ := blockHashes(t, ctx, latinConn, "bh_enc", textDefs, textCols, nil, rows)
		require.Equal(t, utf8Hash, latinHash, "the block hash must not depend on the database encoding")
	})

	t.Run("wide_table", func(t *testing.T) {
		// More than 100 columns, the limit for function arguments.
		const n = 150
		defs := make([]string, n)
		cols := make([]string, n)
		vals := make([]string, n)
		for i := range defs {
			cols[i] = fmt.Sprintf("c%d", i)
			defs[i] = cols[i] + " int"
			vals[i] = fmt.Sprint(i)
		}
		rows := "(1, " + strings.Join(vals, ", ") + ")"
		left, _ := blockHashes(t, ctx, conn, "bh_wide_l", strings.Join(defs, ", "), cols, nil, rows)
		vals[n-1] = "-1"
		changed := "(1, " + strings.Join(vals, ", ") + ")"
		right, _ := blockHashes(t, ctx, conn, "bh_wide_r", strings.Join(defs, ", "), cols, nil, changed)
		require.NotEqual(t, left, right, "a change in the last column must change the hash")
	})
}
