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

package keydiff

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/db/queries"
)

// Names of the service columns of the row read. They start with _ace_ so that
// they do not clash with the columns of the table.
const (
	keysAlias   = "_ace_keys"
	tableAlias  = "_ace_t"
	posCol      = "_ace_pos"
	rowHashCol  = "_ace_row_hash"
	keyColAlias = "_ace_k"
)

// maxReadKeys is the number of keys that one row read can have: each key
// column of each key is a parameter, and a query can have at most 65535.
func maxReadKeys(nKeyCols int) int {
	return 65535 / max(nKeyCols, 1)
}

// readServiceCols is the number of the columns before the columns of the
// table in the row read: the key position, the row hash, the commit
// timestamp and the origin.
const readServiceCols = 4

func (t Table) qualifiedName() string {
	return pgx.Identifier{t.Schema}.Sanitize() + "." + pgx.Identifier{t.Name}.Sanitize()
}

// rowHashExpr is the first rowHashSize bytes of the row hash of the block
// hash.
func (t Table) rowHashExpr() (string, error) {
	h, err := queries.RowHashExpr("", t.Cols)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("substring(%s FROM 1 FOR %d)", h, rowHashSize), nil
}

// keyQuery returns the query that lists, for every row of the block, the text
// of each key column and the row hash. The query has no ORDER BY: the client
// does not need any order.
func (t Table) keyQuery(where string) (string, error) {
	if len(t.Key) == 0 {
		return "", fmt.Errorf("table %s has no primary key", t.qualifiedName())
	}
	h, err := t.rowHashExpr()
	if err != nil {
		return "", err
	}
	exprs := make([]string, 0, len(t.Key)+1)
	for _, k := range t.Key {
		exprs = append(exprs, queries.ColExpr("", k))
	}
	exprs = append(exprs, h)
	if strings.TrimSpace(where) == "" {
		where = "TRUE"
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(exprs, ", "), t.qualifiedName(), where), nil
}

// keyQueryFormats asks for the key columns in the text format and for the
// row hash in the binary format. The text format gives the output of the type
// itself, which the row read gives back to the input function of the type.
func (t Table) keyQueryFormats() pgx.QueryResultFormats {
	f := make(pgx.QueryResultFormats, len(t.Key)+1)
	for i := range t.Key {
		f[i] = pgx.TextFormatCode
	}
	f[len(t.Key)] = pgx.BinaryFormatCode
	return f
}

// readFormats asks for the service columns of the row read in the binary
// format and for the columns of the table in the text format. The text
// format gives the output of the type of each column, which ACE keeps as it
// is.
func (t Table) readFormats() pgx.QueryResultFormats {
	f := make(pgx.QueryResultFormats, readServiceCols+len(t.Cols))
	for i := range f {
		f[i] = pgx.TextFormatCode
		if i < readServiceCols {
			f[i] = pgx.BinaryFormatCode
		}
	}
	return f
}

// ReadQuery returns the query that reads the full rows for nKeys keys. The
// keys are a VALUES list, one row for each key. Its parameters are, for each
// key in turn and for each key column in turn, the key text. The result has
// the position of the key, the row hash, the commit timestamp and the origin
// of the row, and then every column of the table as it is: the row read asks
// for the columns in the text format, so a value is the output of its type.
//
// The key value of a key column is a parameter written as $n::type, with the
// type of the column on this node without the type modifier (castTypes). The
// server gives the parameter that type and reads the key text with the input
// function of the type. A cast from text (k::type over a text[]) must not be
// used: a type can have its own cast from text, which takes the place of the
// input function. An array of the type is no better: unnest of an array of
// arrays gives the elements of the inner arrays. A type with a modifier
// could round or cut the key text of the other node, for example when the
// column is varchar(10) there and varchar(5) here, and then the lookup would
// find a row with another key or fail with an error.
//
// Each key is looked up in a LATERAL subquery. OFFSET 0 stops the planner
// from turning the subquery into a join, so the plan is a nested loop with
// one lookup for each key, however many keys there are. The lookup has an
// equality on every primary key column, so the planner reads it from the
// primary key index, unless the table is so small that a scan of it costs
// less. With Table.ScanChildren the keys are joined with the table instead,
// so that the planner may read a child without the index once for all keys.
//
// The lookup also keeps only the rows whose key prints exactly as the key
// value: format('%s', ...) gives the output of the type, and the "C"
// collation compares the bytes. Two keys that the server holds as equal but
// prints in a different way, such as -0 and 0 in a float column, therefore
// never find the same row: each key finds only the row that prints as it
// does, and the two rows are reported as rows of one node only. The printed
// expression must be the same as in the key query, or a key never matches
// itself. A ::text cast must not replace format(): for bpchar, boolean and
// inet it is not the output of the type.
//
// filter, if not empty, is added to the WHERE clause of the lookup as it is.
func (t Table) ReadQuery(castTypes map[string]string, filter string, nKeys int) (string, error) {
	if len(t.Key) == 0 {
		return "", fmt.Errorf("table %s has no primary key", t.qualifiedName())
	}
	if nKeys < 1 || nKeys > maxReadKeys(len(t.Key)) {
		return "", fmt.Errorf("row read of %s: %d keys, want 1 to %d", t.qualifiedName(), nKeys, maxReadKeys(len(t.Key)))
	}
	h, err := t.rowHashExpr()
	if err != nil {
		return "", err
	}

	types := make([]string, len(t.Key))
	keyCols := make([]string, 0, 1+len(t.Key))
	keyCols = append(keyCols, posCol)
	conds := make([]string, 0, 2*len(t.Key)+1)
	for i, k := range t.Key {
		typ, ok := castTypes[k]
		if !ok || strings.TrimSpace(typ) == "" {
			return "", fmt.Errorf("no cast type for key column %s of %s", k, t.qualifiedName())
		}
		types[i] = typ
		keyCols = append(keyCols, fmt.Sprintf("%s%d", keyColAlias, i+1))
		conds = append(conds, fmt.Sprintf("%s = %s.%s%d", pgx.Identifier{k}.Sanitize(), keysAlias, keyColAlias, i+1))
	}
	for i, k := range t.Key {
		conds = append(conds, fmt.Sprintf(`format('%%s', %s) COLLATE "C" = format('%%s', %s.%s%d)`,
			queries.ColExpr("", k), keysAlias, keyColAlias, i+1))
	}
	var keys strings.Builder
	p := 0
	for j := 0; j < nKeys; j++ {
		if j > 0 {
			keys.WriteString(", ")
		}
		fmt.Fprintf(&keys, "(%d", j+1)
		for _, typ := range types {
			p++
			fmt.Fprintf(&keys, ", $%d::%s", p, typ)
		}
		keys.WriteString(")")
	}
	if f := strings.TrimSpace(filter); f != "" {
		conds = append(conds, "("+f+")")
	}

	sel := make([]string, 0, len(t.Cols)+3)
	sel = append(sel,
		h+" AS "+rowHashCol,
		"pg_xact_commit_timestamp("+tableAlias+".xmin) AS commit_ts",
		"to_json(pg_xact_commit_timestamp_origin("+tableAlias+".xmin))->>'roident' AS node_origin")
	for _, c := range t.Cols {
		sel = append(sel, queries.ColExpr("", c))
	}

	if t.ScanChildren {
		// A plain join: the planner can hash the list of keys and read each
		// child once, or still look the keys up in an index where there
		// is one.
		return fmt.Sprintf(
			"SELECT %[1]s.%[2]s::bigint, %[5]s "+
				"FROM (VALUES %[3]s) AS %[1]s(%[4]s) "+
				"JOIN %[6]s AS %[8]s ON %[7]s",
			keysAlias, posCol,
			keys.String(),
			strings.Join(keyCols, ", "),
			strings.Join(sel, ", "),
			t.qualifiedName(),
			strings.Join(conds, " AND "),
			tableAlias,
		), nil
	}
	return fmt.Sprintf(
		"SELECT %[1]s.%[2]s::bigint, _ace_row.* "+
			"FROM (VALUES %[3]s) AS %[1]s(%[4]s) "+
			"CROSS JOIN LATERAL ("+
			"SELECT %[5]s FROM %[6]s AS %[8]s WHERE %[7]s OFFSET 0"+
			") AS _ace_row",
		keysAlias, posCol,
		keys.String(),
		strings.Join(keyCols, ", "),
		strings.Join(sel, ", "),
		t.qualifiedName(),
		strings.Join(conds, " AND "),
		tableAlias,
	), nil
}
