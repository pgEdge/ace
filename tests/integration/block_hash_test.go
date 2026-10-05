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
	"encoding/hex"
	"fmt"
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

	// The mtree leaf and the table-diff block must be the same hash.
	require.Equal(t, hex.EncodeToString(leaf), td, "leaf hash and table-diff hash differ for %s", name)
	require.Len(t, leaf, sha256.Size)

	// The leaf must be the XOR of the row hashes that the mtree diff reads.
	// Otherwise a leaf can differ while every row hash matches.
	rowExpr, err := queries.RowHashExpr("", allCols, colTypes)
	require.NoError(t, err)
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s FROM pg_temp.%s", rowExpr, name)) // nosemgrep
	require.NoError(t, err)
	var xor [sha256.Size]byte
	for rows.Next() {
		var h []byte
		require.NoError(t, rows.Scan(&h))
		require.Len(t, h, sha256.Size)
		for i := range xor {
			xor[i] ^= h[i]
		}
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, xor[:], leaf, "leaf hash of %s is not the XOR of its row hashes", name)

	return leaf, td
}

// TestBlockHashEncoding checks the block hash on a real server: different
// rows must give different hashes, and equal rows equal hashes. bit_xor does
// not depend on the order of rows, so the test also checks cases where the
// order could matter.
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
		// bit_xor ignores row order. Rows that swap their values must still
		// differ, because the primary key is part of each row hash.
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
		// bit_xor over no rows is NULL; the hash uses 256 zero bits instead.
		require.Equal(t, make([]byte, sha256.Size), leaf, "an empty block hashes to 32 zero bytes")
	})

	t.Run("single_row_is_sha256_of_row_text", func(t *testing.T) {
		// A block of one row hashes to the sha256 of the UTF8 text of
		// ROW(...). This pins the whole formula, including the way the
		// bit(256) value is turned back into 32 bytes.
		leaf, _ := blockHashes(t, ctx, conn, "bh_one", textDefs, textCols, nil, `(1, 'café', NULL)`)
		want := sha256.Sum256([]byte("(1,café,)"))
		require.Equal(t, want[:], leaf)
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
