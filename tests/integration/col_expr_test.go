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
	"strings"
	"testing"

	"github.com/pgedge/ace/db/queries"
	"github.com/stretchr/testify/require"
)

// fetchTextGaps are the column types whose ::TEXT cast prints a value in
// another form than the output function of the type, which the row hash uses
// (ROW(...)::text). For such a type the fetched text is not the hashed text.
// Equality is not affected, because the other form is as unambiguous, but
// the rule "the fetched value is the hashed value" does not hold for it.
var fetchTextGaps = map[string]string{
	"inet": `inet::text is text(inet), which always adds the netmask: '1.2.3.4/32', not '1.2.3.4'`,
}

// TestRowFetchMatchesRowHash checks on a real server that the diff engines
// fetch the value they hash: for every column, the text of the fetch
// expression inside ROW() is the text of the hash expression, and the row
// hash can be computed again from the fetch expressions. If one side changes
// and the other does not, two rows can hash the same and still be reported
// as different when their block is fetched because of another row.
//
// The test compares the text that the server prints. It does not check the
// Go value made of a fetched column that is not cast to ::TEXT (real,
// timestamptz, ...): the driver decodes such a column in the binary format.
func TestRowFetchMatchesRowHash(t *testing.T) {
	ctx := context.Background()
	conn, err := pgCluster.Node1Pool.Acquire(ctx)
	require.NoError(t, err)
	// Cleanups run in reverse order: the table is dropped through this
	// connection before it is released.
	t.Cleanup(conn.Release)

	const table = "col_expr_fetch"
	_, err = conn.Exec(ctx, `CREATE TEMP TABLE `+table+` (
		id         int PRIMARY KEY,
		i8         bigint,
		num        numeric,
		num_fixed  numeric(10,2),
		num_arr    numeric[],
		txt        text,
		chr        char(5),
		vchr       varchar(5),
		flag       boolean,
		r4         real,
		f8         double precision,
		ts         timestamp,
		tstz       timestamptz,
		d          date,
		tm         time,
		tmtz       timetz,
		iv         interval,
		u          uuid,
		b          bytea,
		j          json,
		jb         jsonb,
		ip         inet,
		net        cidr,
		mac        macaddr,
		pt         point,
		ir         int4range,
		dr         daterange,
		bits       bit(4),
		vbits      varbit,
		x          xml,
		m          money,
		txt_arr    text[]
	)`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS pg_temp."+table) }) // nosemgrep

	_, err = conn.Exec(ctx, `INSERT INTO `+table+` VALUES
		(1, 9007199254740993, 1.50, 3000.00, '{1.50,2}', 'a,b "c" (d) \e', 'ab', 'xy', true, 0.1, 0.1,
		 '2026-10-24 12:00:00.5', '2026-10-24 12:00:00.123456+02', '2026-10-24', '12:00:00.25', '12:00:00+03',
		 '1 day 2 hours', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\x00ff', '{"a": 1,  "b": [1, 2]}', '{"a": 1, "b": [1, 2]}',
		 '1.2.3.4', '10.0.0.0/8', '08:00:2b:01:02:03', '(1.5,2)', '[1,5)', '[2026-01-01,2026-02-01)',
		 B'1010', B'101', '<a>x &amp; y</a>', 12.34, '{"a b",NULL,""}'),
		(2, -1, 0.000, 7, '{}', '', '', '', false, 'NaN', '-0', 'infinity', '-infinity', 'infinity', '24:00', '00:00:00-12',
		 '-1 mon', '00000000-0000-0000-0000-000000000000', '', '[]', '[]', '::1', '::/0', '00:00:00:00:00:00', '(0,0)', 'empty', 'empty',
		 B'0000', B'', '', -0.01, '{}'),
		(3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
		 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)`)
	require.NoError(t, err)

	// Column types as the diff engines get them: format_type output.
	rows, err := conn.Query(ctx, `
		SELECT attname, pg_catalog.format_type(atttypid, atttypmod)
		FROM pg_catalog.pg_attribute
		WHERE attrelid = 'pg_temp.`+table+`'::regclass AND attnum > 0 AND NOT attisdropped
		ORDER BY attnum`)
	require.NoError(t, err)
	var cols []string
	colTypes := map[string]string{}
	for rows.Next() {
		var name, typ string
		require.NoError(t, rows.Scan(&name, &typ))
		cols = append(cols, name)
		colTypes[name] = typ
	}
	rows.Close()
	require.NoError(t, rows.Err())

	var hashCols []string
	for _, col := range cols {
		typ := colTypes[col]
		if reason, gap := fetchTextGaps[typ]; gap {
			t.Run(col, func(t *testing.T) { t.Skipf("known gap for %s: %s", typ, reason) })
			continue
		}
		hashCols = append(hashCols, col)
		t.Run(col, func(t *testing.T) {
			hashExpr := queries.ColExpr("", col, typ)
			fetchExpr := queries.FetchColExpr("", col, typ)
			var mismatch []string
			q := fmt.Sprintf(`SELECT id, ROW(%s)::text, ROW(%s)::text FROM pg_temp.%s
				WHERE ROW(%s)::text IS DISTINCT FROM ROW(%s)::text ORDER BY id`,
				hashExpr, fetchExpr, table, hashExpr, fetchExpr) // nosemgrep
			r, err := conn.Query(ctx, q)
			require.NoError(t, err, "query: %s", q)
			for r.Next() {
				var id int
				var hashed, fetched string
				require.NoError(t, r.Scan(&id, &hashed, &fetched))
				mismatch = append(mismatch, fmt.Sprintf("id %d: hashed %s, fetched %s", id, hashed, fetched))
			}
			r.Close()
			require.NoError(t, r.Err())
			require.Empty(t, mismatch, "column %s (%s): the fetched text is not the hashed text", col, typ)
		})
	}

	t.Run("whole_row", func(t *testing.T) {
		rowHash, err := queries.RowHashExpr("", hashCols, colTypes)
		require.NoError(t, err)
		fetchExprs := make([]string, len(hashCols))
		for i, col := range hashCols {
			fetchExprs[i] = queries.FetchColExpr("", col, colTypes[col])
		}
		recomputed := fmt.Sprintf("sha256(convert_to(ROW(%s)::text, 'UTF8'))", strings.Join(fetchExprs, ", "))
		var same bool
		var n int
		require.NoError(t, conn.QueryRow(ctx, fmt.Sprintf(
			"SELECT bool_and(%s = %s), count(*) FROM pg_temp.%s", rowHash, recomputed, table)).Scan(&same, &n)) // nosemgrep
		require.Equal(t, 3, n)
		require.True(t, same, "the row hash cannot be computed again from the fetched columns")
	})
}
