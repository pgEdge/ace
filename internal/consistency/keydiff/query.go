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

func (t Table) qualifiedName() string {
	return pgx.Identifier{t.Schema}.Sanitize() + "." + pgx.Identifier{t.Name}.Sanitize()
}

// rowHashExpr is the first rowHashSize bytes of the row hash of the block
// hash.
func (t Table) rowHashExpr() (string, error) {
	h, err := queries.RowHashExpr("", t.Cols, t.HashTypes)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("substring(%s FROM 1 FOR %d)", h, rowHashSize), nil
}

// keyQuery returns the query that lists, for every row of the block, the text
// of each key column and the row hash. A numeric key column goes through
// trim_scale(), as in the row hash, so that 1.50 and 1.5 give the same text.
// The query has no ORDER BY: the client does not need any order.
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
		exprs = append(exprs, queries.ColExpr("", k, t.HashTypes[k]))
	}
	exprs = append(exprs, h)
	if strings.TrimSpace(where) == "" {
		where = "TRUE"
	}
	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", strings.Join(exprs, ", "), t.qualifiedName(), where), nil
}

// keyQueryFormats asks for the key columns in the text format and for the
// row hash in the binary format. The text format gives the output of the type
// itself, which the row read casts back to the type.
func (t Table) keyQueryFormats() pgx.QueryResultFormats {
	f := make(pgx.QueryResultFormats, len(t.Key)+1)
	for i := range t.Key {
		f[i] = pgx.TextFormatCode
	}
	f[len(t.Key)] = pgx.BinaryFormatCode
	return f
}

// ReadQuery returns the query that reads the full rows for a list of keys.
// Its parameters are one text[] for each key column, all of the same length:
// element i of each array is the text of key i.
//
// Each key is cast back to the type of the column on this node, without the
// type modifier (castTypes), and looked up in a LATERAL subquery. A cast to
// a type with a modifier could round or cut the key text of the other node,
// for example when the column is varchar(10) there and varchar(5) here, and
// then the lookup would find a row with another key or fail with an error.
// OFFSET 0 stops the planner from turning the subquery into a join, so the
// plan is a nested loop with one lookup for each key, however many keys
// there are. The lookup has an equality on every
// primary key column, so the planner reads it from the primary key index,
// unless the table is so small that a scan of it costs less. With
// Table.ScanChildren the keys are joined with the table instead, so that the
// planner may read a child without the index once for all keys.
//
// The lookup also keeps only the rows whose key prints exactly as the key
// text: format('%s', ...) gives the output of the type, and the "C"
// collation compares the bytes. Two key texts that the server holds as
// equal but prints in a different way, such as -0 and 0 in a float column,
// therefore never find the same row: each text finds only the row that
// printed it, and the two rows are reported as rows of one node only.
// The printed expression must be the same as in the key query, trim_scale()
// included, or a numeric key never matches its own text. A ::text cast must
// not replace format(): for bpchar, boolean and inet it is not the output of
// the type.
//
// filter, if not empty, is added to the WHERE clause of the lookup as it is.
func (t Table) ReadQuery(colTypes, castTypes map[string]string, filter string) (string, error) {
	if len(t.Key) == 0 {
		return "", fmt.Errorf("table %s has no primary key", t.qualifiedName())
	}
	h, err := t.rowHashExpr()
	if err != nil {
		return "", err
	}

	params := make([]string, len(t.Key))
	keyCols := make([]string, len(t.Key))
	conds := make([]string, 0, 2*len(t.Key)+1)
	for i, k := range t.Key {
		typ, ok := castTypes[k]
		if !ok || strings.TrimSpace(typ) == "" {
			return "", fmt.Errorf("no cast type for key column %s of %s", k, t.qualifiedName())
		}
		alias := fmt.Sprintf("%s%d", keyColAlias, i+1)
		params[i] = fmt.Sprintf("$%d::text[]", i+1)
		keyCols[i] = alias
		conds = append(conds, fmt.Sprintf("%s = %s.%s::%s", pgx.Identifier{k}.Sanitize(), keysAlias, alias, typ))
	}
	for i, k := range t.Key {
		conds = append(conds, fmt.Sprintf(`format('%%s', %s) COLLATE "C" = %s.%s%d`,
			queries.ColExpr("", k, t.HashTypes[k]), keysAlias, keyColAlias, i+1))
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
		sel = append(sel, queries.SelectColExpr("", c, colTypes[c]))
	}

	if t.ScanChildren {
		// A plain join: the planner can hash the list of keys and read each
		// child once, or still look the keys up in an index where there
		// is one.
		return fmt.Sprintf(
			"SELECT %[1]s.%[2]s, %[5]s "+
				"FROM unnest(%[3]s) WITH ORDINALITY AS %[1]s(%[4]s, %[2]s) "+
				"JOIN %[6]s AS %[8]s ON %[7]s",
			keysAlias, posCol,
			strings.Join(params, ", "),
			strings.Join(keyCols, ", "),
			strings.Join(sel, ", "),
			t.qualifiedName(),
			strings.Join(conds, " AND "),
			tableAlias,
		), nil
	}
	return fmt.Sprintf(
		"SELECT %[1]s.%[2]s, _ace_row.* "+
			"FROM unnest(%[3]s) WITH ORDINALITY AS %[1]s(%[4]s, %[2]s) "+
			"CROSS JOIN LATERAL ("+
			"SELECT %[5]s FROM %[6]s AS %[8]s WHERE %[7]s OFFSET 0"+
			") AS _ace_row",
		keysAlias, posCol,
		strings.Join(params, ", "),
		strings.Join(keyCols, ", "),
		strings.Join(sel, ", "),
		t.qualifiedName(),
		strings.Join(conds, " AND "),
		tableAlias,
	), nil
}
