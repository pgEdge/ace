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
func blockHashes(t *testing.T, ctx context.Context, conn *pgxpool.Conn, name, colDefs string, cols []string, values string) ([]byte, string) {
	t.Helper()

	_, err := conn.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (id int PRIMARY KEY, %s)", name, colDefs)) // nosemgrep
	require.NoError(t, err)
	t.Cleanup(func() { execOK(t, context.Background(), conn, "DROP TABLE IF EXISTS pg_temp."+name) }) // nosemgrep
	if values != "" {
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES %s", name, values)) // nosemgrep
		require.NoError(t, err)
	}

	allCols := append([]string{"id"}, cols...)
	form := hashFormOf(t, ctx, conn)
	leaf, err := queries.ComputeLeafHashes(ctx, conn, "pg_temp", name, []string{"id"}, map[string]string{"id": "integer"}, nil, nil, allCols, form)
	require.NoError(t, err)

	tdSQL, err := queries.BlockHashSQL("pg_temp", name, "TD_BLOCK_HASH", "TRUE", allCols, form)
	require.NoError(t, err)
	var (
		td   string
		rows int64
	)
	require.NoError(t, conn.QueryRow(ctx, tdSQL).Scan(&td, &rows)) // nosemgrep
	requireRowCount(t, ctx, conn, name, rows)

	checkBlockHash(t, ctx, conn, name, allCols, form, leaf, td)
	return leaf, td
}

// hashFormOf returns the hash form of the database of conn when the basis
// encoding is UTF8, as for a cluster with nodes in different encodings.
func hashFormOf(t *testing.T, ctx context.Context, conn *pgxpool.Conn) queries.HashForm {
	t.Helper()
	enc, err := queries.ServerEncoding(ctx, conn)
	require.NoError(t, err)
	form, err := queries.HashFormFor(enc, "UTF8")
	require.NoError(t, err)
	return form
}

// requireRowCount checks that the row count of the table-diff block hash
// query is the number of rows of the table.
func requireRowCount(t *testing.T, ctx context.Context, conn *pgxpool.Conn, name string, rows int64) {
	t.Helper()
	var want int64
	require.NoError(t, conn.QueryRow(ctx, "SELECT count(*) FROM pg_temp."+name).Scan(&want)) // nosemgrep
	require.Equal(t, want, rows, "row count of the block hash of %s", name)
}

// checkBlockHash checks the mtree leaf hash and the table-diff block hash of
// the whole table name against each other and against the row hashes.
func checkBlockHash(t *testing.T, ctx context.Context, conn *pgxpool.Conn, name string, allCols []string, form queries.HashForm, leaf []byte, td string) {
	t.Helper()

	// The mtree leaf and the table-diff block must be the same hash.
	require.Equal(t, hex.EncodeToString(leaf), td, "leaf hash and table-diff hash differ for %s", name)
	require.Len(t, leaf, sha256.Size)

	// The leaf must be built from the row hashes that the mtree diff reads.
	// Otherwise a leaf can differ while every row hash matches.
	rowExpr, err := queries.RowHashExpr("", allCols, form)
	require.NoError(t, err)
	rows, err := conn.Query(ctx, fmt.Sprintf("SELECT %s FROM pg_temp.%s", rowExpr, name)) // nosemgrep
	require.NoError(t, err)
	var hashes []int64
	for rows.Next() {
		var h int64
		require.NoError(t, rows.Scan(&h))
		hashes = append(hashes, h)
	}
	rows.Close()
	require.NoError(t, rows.Err())
	require.Equal(t, multisetHash(hashes), leaf, "leaf hash of %s does not match its row hashes", name)
}

// multisetHash is the block hash written in Go: the sha256 of "count,sum",
// where sum is the exact sum of the signed 64-bit row hashes. An empty
// block hashes to 32 zero bytes.
func multisetHash(rowHashes []int64) []byte {
	if len(rowHashes) == 0 {
		return make([]byte, sha256.Size)
	}
	var sum big.Int
	for _, h := range rowHashes {
		sum.Add(&sum, big.NewInt(h))
	}
	out := sha256.Sum256([]byte(fmt.Sprintf("%d,%s", len(rowHashes), sum.String())))
	return out[:]
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
	t.Cleanup(func() { execOK(t, context.Background(), conn, "DROP TABLE IF EXISTS pg_temp."+name+" CASCADE") }) // nosemgrep
	for tbl, values := range map[string]string{name: parentRows, child: childRows} {
		if values == "" {
			continue
		}
		_, err = conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s VALUES %s", tbl, values)) // nosemgrep
		require.NoError(t, err)
	}

	allCols := []string{"id", "a", "b"}
	form := hashFormOf(t, ctx, conn)
	leaf, err := queries.ComputeLeafHashes(ctx, conn, "pg_temp", name, []string{"id"}, map[string]string{"id": "integer"}, nil, nil, allCols, form)
	require.NoError(t, err)
	tdSQL, err := queries.BlockHashSQL("pg_temp", name, "TD_BLOCK_HASH", "TRUE", allCols, form)
	require.NoError(t, err)
	var (
		td   string
		rows int64
	)
	require.NoError(t, conn.QueryRow(ctx, tdSQL).Scan(&td, &rows)) // nosemgrep
	requireRowCount(t, ctx, conn, name, rows)

	checkBlockHash(t, ctx, conn, name, allCols, form, leaf, td)
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
		// The seed of a column makes these differ: with a plain XOR of the
		// column hashes they would give the same row hash.
		{"values_swap_columns", `(1, 'a', 'b')`, `(1, 'b', 'a')`},
		{"equal_values_in_two_columns", `(1, 'a', 'a')`, `(1, 'c', 'c')`},
		// The last hash of a row makes this differ: the value of one
		// column moves between two rows.
		{"one_column_swaps_rows", `(1, 'a', 'x'), (2, 'b', 'y')`, `(1, 'b', 'x'), (2, 'a', 'y')`},
	}
	for i, tc := range mustDiffer {
		t.Run("differ/"+tc.name, func(t *testing.T) {
			left, _ := blockHashes(t, ctx, conn, fmt.Sprintf("bh_l%d", i), textDefs, textCols, tc.left)
			right, _ := blockHashes(t, ctx, conn, fmt.Sprintf("bh_r%d", i), textDefs, textCols, tc.right)
			require.NotEqual(t, left, right, "different rows must give different block hashes")
		})
	}

	t.Run("equal/same_rows", func(t *testing.T) {
		rows := `(1, 'a', NULL), (2, '', 'b,c'), (3, 'd|e', '"q"')`
		left, _ := blockHashes(t, ctx, conn, "bh_same_l", textDefs, textCols, rows)
		right, _ := blockHashes(t, ctx, conn, "bh_same_r", textDefs, textCols, rows)
		require.Equal(t, left, right)
	})

	t.Run("differ/numeric_trailing_zeros", func(t *testing.T) {
		// numeric without a fixed scale keeps 1.50 and 1.5 apart on disk, and
		// the server prints them in a different way, so they are different
		// values.
		left, _ := blockHashes(t, ctx, conn, "bh_num_l", "amt numeric", []string{"amt"}, `(1, 1.50), (2, 7.000)`)
		right, _ := blockHashes(t, ctx, conn, "bh_num_r", "amt numeric", []string{"amt"}, `(1, 1.5), (2, 7)`)
		require.NotEqual(t, left, right)
	})

	t.Run("differ/float_negative_zero", func(t *testing.T) {
		// -0 = 0 for the type, but their texts differ.
		left, _ := blockHashes(t, ctx, conn, "bh_fz_l", "f float8", []string{"f"}, `(1, '-0')`)
		right, _ := blockHashes(t, ctx, conn, "bh_fz_r", "f float8", []string{"f"}, `(1, '0')`)
		require.NotEqual(t, left, right)
	})

	t.Run("equal/bpchar_trailing_spaces", func(t *testing.T) {
		// The cast of bpchar to text removes trailing spaces, so 'a' and
		// 'a  ' are identical values.
		left, _ := blockHashes(t, ctx, conn, "bh_bp_l", "c bpchar", []string{"c"}, `(1, 'a  ')`)
		right, _ := blockHashes(t, ctx, conn, "bh_bp_r", "c bpchar", []string{"c"}, `(1, 'a')`)
		require.Equal(t, left, right)
	})

	t.Run("numeric_array", func(t *testing.T) {
		// A table with a numeric array and a scalar numeric column: a change
		// of the scale in either column changes the hash.
		defs := "amts numeric[], amt numeric"
		cols := []string{"amts", "amt"}
		base, _ := blockHashes(t, ctx, conn, "bh_arr", defs, cols, `(1, '{1.50,2}', 1.50), (2, '{3}', 7.000)`)
		scale, _ := blockHashes(t, ctx, conn, "bh_arr_scale", defs, cols, `(1, '{1.50,2}', 1.5), (2, '{3}', 7)`)
		require.NotEqual(t, base, scale, "the scale of the scalar numeric column must change the hash")
		changed, _ := blockHashes(t, ctx, conn, "bh_arr_chg", defs, cols, `(1, '{1.50,3}', 1.50), (2, '{3}', 7.000)`)
		require.NotEqual(t, base, changed, "a change in an array element must change the hash")
	})

	t.Run("equal/insert_order", func(t *testing.T) {
		// The block hash depends on the rows, not on their physical order.
		left, _ := blockHashes(t, ctx, conn, "bh_ord_l", textDefs, textCols, `(1, 'a', 'x'), (2, 'b', 'y'), (3, 'c', 'z')`)
		right, _ := blockHashes(t, ctx, conn, "bh_ord_r", textDefs, textCols, `(3, 'c', 'z'), (1, 'a', 'x'), (2, 'b', 'y')`)
		require.Equal(t, left, right)
	})

	t.Run("empty_block", func(t *testing.T) {
		leaf, _ := blockHashes(t, ctx, conn, "bh_empty", textDefs, textCols, "")
		// A parent node is the XOR of its children, so an empty leaf must be
		// 32 zero bytes: then it does not change its parent.
		require.Equal(t, make([]byte, sha256.Size), leaf, "an empty block hashes to 32 zero bytes")
	})

	t.Run("single_row_pins_the_formula", func(t *testing.T) {
		// The row hash is computed by hash functions of the server. Its
		// value for one row is pinned here, so that a server version that
		// hashes in another way fails this test instead of giving false
		// differences between nodes. The block hash of one row is the
		// sha256 of "1,<row hash>".
		leaf, _ := blockHashes(t, ctx, conn, "bh_one", textDefs, textCols, `(1, 'café', NULL)`)
		const rowHash = int64(6857920088145939538)
		want := sha256.Sum256([]byte(fmt.Sprintf("1,%d", rowHash)))
		require.Equal(t, want[:], leaf)
	})

	t.Run("negative_and_positive_row_hashes", func(t *testing.T) {
		// The sum of the row hashes is exact: rows with negative and with
		// positive row hashes are checked against multisetHash.
		rowExpr, err := queries.RowHashExpr("", []string{"id", "a", "b"}, queries.HashFormBytes)
		require.NoError(t, err)
		var neg, pos int
		require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf(`
			SELECT min(id) FILTER (WHERE h < 0), min(id) FILTER (WHERE h >= 0)
			FROM (SELECT id, %s AS h FROM (SELECT i AS id, 'a'::text AS a, 'b'::text AS b
			      FROM generate_series(1, 64) AS i) r) s`, rowExpr)).Scan(&neg, &pos)) // nosemgrep
		blockHashes(t, ctx, conn, "bh_sign", textDefs, textCols, fmt.Sprintf(`(%d, 'a', 'b'), (%d, 'a', 'b')`, neg, pos))
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

	// Logical replication allows nodes with different database encodings.
	// The same rows must give the same hash in a UTF8 database (the fast
	// form) and in a database of another encoding (the form that converts
	// each value to UTF8). A SQL_ASCII database hashes its bytes as they are,
	// which are UTF8 bytes here.
	for _, tc := range []struct {
		encoding string
		rows     string
	}{
		// Every character exists in LATIN1 and takes two bytes in UTF8.
		{"LATIN1", `(1, 'café', 'Größe'), (2, 'Ångström', NULL), (3, '', 'ÿ,"ü"')`},
		// A multibyte encoding other than UTF8.
		{"EUC_JP", `(1, '日本語', 'カタカナ'), (2, 'ｶﾀｶﾅ', NULL), (3, '', '漢字,"かな"')`},
		{"SQL_ASCII", `(1, 'café', '日本語'), (2, 'Ångström', NULL), (3, '', 'ÿ,"ü"')`},
	} {
		t.Run("equal/database_encoding_"+tc.encoding, func(t *testing.T) {
			other := encodingConn(t, ctx, conn, tc.encoding)
			utf8Hash, _ := blockHashes(t, ctx, conn, "bh_enc", textDefs, textCols, tc.rows)
			otherHash, _ := blockHashes(t, ctx, other, "bh_enc", textDefs, textCols, tc.rows)
			require.Equal(t, utf8Hash, otherHash, "the block hash must not depend on the database encoding")
		})
	}

	t.Run("composite_with_null_field", func(t *testing.T) {
		// IS NOT NULL is false for a composite value with a NULL field, but
		// the value is not a NULL. The form that converts to UTF8 must hash
		// its text, as the bytes form does: (1,) and (2,) differ, and (,)
		// differs from a NULL.
		other := encodingConn(t, ctx, conn, "LATIN1")
		for _, c := range []*pgxpool.Conn{conn, other} {
			execOK(t, ctx, c, "CREATE TYPE pg_temp.bh_pair AS (x int, y int)")
			t.Cleanup(func() { execOK(t, context.Background(), c, "DROP TYPE IF EXISTS pg_temp.bh_pair") })
		}
		defs := "a pg_temp.bh_pair, b text"
		cols := []string{"a", "b"}
		rows := `(1, '(1,)', 'x'), (2, '(,)', 'y')`
		utf8Hash, _ := blockHashes(t, ctx, conn, "bh_cmp_l", defs, cols, rows)
		latinHash, _ := blockHashes(t, ctx, other, "bh_cmp_l", defs, cols, rows)
		require.Equal(t, utf8Hash, latinHash, "the block hash must not depend on the database encoding")
		changed, _ := blockHashes(t, ctx, other, "bh_cmp_r", defs, cols, `(1, '(2,)', 'x'), (2, NULL, 'y')`)
		require.NotEqual(t, latinHash, changed, "composite values with a NULL field must be hashed by their text")
	})

	t.Run("sql_ascii_bytes_not_utf8", func(t *testing.T) {
		// A SQL_ASCII database can hold bytes that are not valid UTF8. Its
		// hash does not convert them, so it does not fail.
		other := encodingConn(t, ctx, conn, "SQL_ASCII")
		blockHashes(t, ctx, other, "bh_raw", textDefs, textCols, `(1, convert_from('\xc4'::bytea, 'SQL_ASCII'), 'x')`)
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
		left, _ := blockHashes(t, ctx, conn, "bh_wide_l", strings.Join(defs, ", "), cols, rows)
		vals[n-1] = "-1"
		changed := "(1, " + strings.Join(vals, ", ") + ")"
		right, _ := blockHashes(t, ctx, conn, "bh_wide_r", strings.Join(defs, ", "), cols, changed)
		require.NotEqual(t, left, right, "a change in the last column must change the hash")
	})
}

// encodingConn creates a database with the server encoding enc on the
// server of conn and returns a connection to it. The client sends UTF8.
func encodingConn(t *testing.T, ctx context.Context, conn *pgxpool.Conn, enc string) *pgxpool.Conn {
	t.Helper()
	dbName := "ace_block_hash_" + strings.ToLower(enc)
	_, err := conn.Exec(ctx, "CREATE DATABASE "+dbName+" ENCODING '"+enc+"' LC_COLLATE 'C' LC_CTYPE 'C' TEMPLATE template0") // nosemgrep
	if err != nil {
		t.Skipf("cannot create a %s database: %v", enc, err)
	}
	t.Cleanup(func() {
		execOK(t, context.Background(), pgCluster.Node1Pool, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)") // nosemgrep
	})

	cfg := pgCluster.Node1Pool.Config().Copy()
	cfg.ConnConfig.Database = dbName
	// pgx sends UTF8 but does not set client_encoding. Without it the
	// server takes the bytes in its own encoding and stores other
	// characters.
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["client_encoding"] = "UTF8"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	other, err := pool.Acquire(ctx)
	require.NoError(t, err)
	// Cleanups run in reverse order: blockHashes drops its tables through
	// this connection before it is released.
	t.Cleanup(other.Release)

	got, err := queries.ServerEncoding(ctx, other)
	require.NoError(t, err)
	require.Equal(t, enc, got)
	return other
}
