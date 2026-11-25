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

package queries

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/pgedge/ace/pkg/config"
)

// Tree states stored in ace_mtree_metadata.state.
const (
	TreeStateValid   = "valid"
	TreeStateInvalid = "invalid"
)

// Replica identity modes, as in pg_class.relreplident.
const (
	ReplicaIdentityDefault = "d"
	ReplicaIdentityIndex   = "i"
	ReplicaIdentityFull    = "f"
	ReplicaIdentityNothing = "n"
)

// TableShapeFormat is the version of the JSON form of TableShape. A stored
// shape with another version is treated as no shape.
const TableShapeFormat = 1

// TableShape is the part of a table definition that a Merkle tree depends
// on: the primary key, the replica identity and the columns, and the file
// that holds the rows.
type TableShape struct {
	Format int `json:"format"`
	// Filenode is pg_class.relfilenode of the table. DDL can change the
	// stored values only by a rewrite of the table, which gives it a new
	// file, and such a change sends no row change to CDC. A second rewrite
	// can bring back the old definition, but not the old file.
	Filenode uint32 `json:"filenode"`
	// ReplicaIdentity is one of the ReplicaIdentity* modes.
	ReplicaIdentity string `json:"replica_identity"`
	// IdentityColumns are the columns of the replica identity index, in
	// index order: the primary key for DEFAULT, the chosen index for USING
	// INDEX, none for FULL and NOTHING.
	IdentityColumns []string `json:"identity_columns"`
	// Key holds the primary key columns in key order.
	Key []ShapeColumn `json:"key"`
	// Columns holds all live columns in attnum order, generated columns
	// included.
	Columns []ShapeColumn `json:"columns"`
}

// ShapeColumn is one column of a TableShape.
type ShapeColumn struct {
	Name    string `json:"name"`
	Attnum  int16  `json:"attnum"`
	TypeOID uint32 `json:"type_oid"`
	Typmod  int32  `json:"typmod"`
	// Type is format_type of the column, for messages.
	Type string `json:"type"`
	// Generated is pg_attribute.attgenerated: "" for a plain column, "s"
	// for a stored and "v" for a virtual generated column. GenExpr is the
	// md5 of the generation expression.
	Generated string `json:"generated,omitempty"`
	GenExpr   string `json:"gen_expr,omitempty"`
	// Collation and CastType are set only in TableShape.Key. CastType is
	// the qualified type name at the time the shape was read; a rename of
	// the type keeps its OID, and only the OID decides whether the type
	// changed.
	Collation uint32 `json:"collation,omitempty"`
	CastType  string `json:"cast_type,omitempty"`
}

// KeyNames returns the names of the key columns in key order.
func (s TableShape) KeyNames() []string {
	names := make([]string, len(s.Key))
	for i, c := range s.Key {
		names[i] = c.Name
	}
	return names
}

// ColumnNames returns the names of all columns in attnum order.
func (s TableShape) ColumnNames() []string {
	names := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		names[i] = c.Name
	}
	return names
}

// IdentityProblem returns why the replica identity of s does not fit a
// Merkle tree, or "" if it fits. CDC finds the leaf of a change by the
// columns of the replica identity, so they must be the primary key columns.
func (s TableShape) IdentityProblem() string {
	if len(s.Key) == 0 {
		return "the table has no primary key"
	}
	switch s.ReplicaIdentity {
	case ReplicaIdentityDefault:
		return ""
	case ReplicaIdentityIndex:
		if sameColumnSet(s.IdentityColumns, s.KeyNames()) {
			return ""
		}
		return fmt.Sprintf("the replica identity is an index on (%s), not on the primary key columns (%s)",
			strings.Join(s.IdentityColumns, ", "), strings.Join(s.KeyNames(), ", "))
	case ReplicaIdentityFull:
		return "the replica identity is FULL"
	case ReplicaIdentityNothing:
		return "the replica identity is NOTHING"
	}
	return fmt.Sprintf("the replica identity has the unknown mode %q", s.ReplicaIdentity)
}

// IdentityText names the replica identity of s for a message.
func (s TableShape) IdentityText() string {
	switch s.ReplicaIdentity {
	case ReplicaIdentityDefault:
		return "DEFAULT"
	case ReplicaIdentityIndex:
		return fmt.Sprintf("USING INDEX on (%s)", strings.Join(s.IdentityColumns, ", "))
	case ReplicaIdentityFull:
		return "FULL"
	case ReplicaIdentityNothing:
		return "NOTHING"
	}
	return s.ReplicaIdentity
}

// KeyChange returns how the primary key or the replica identity of cur
// differs from s, the shape a tree was built for, or "" if they are the
// same. The leaves of a tree are cut in the order of its key, so any such
// change makes the tree stale.
func (s TableShape) KeyChange(cur TableShape) string {
	// For DEFAULT the identity columns are the primary key, which is
	// compared below.
	if s.ReplicaIdentity != cur.ReplicaIdentity ||
		(cur.ReplicaIdentity == ReplicaIdentityIndex && !sameColumnSet(s.IdentityColumns, cur.IdentityColumns)) {
		return fmt.Sprintf("the replica identity was %s, now %s", s.IdentityText(), cur.IdentityText())
	}
	if !slices.Equal(s.KeyNames(), cur.KeyNames()) {
		if len(cur.Key) == 0 {
			return fmt.Sprintf("the primary key was (%s), now the table has no primary key",
				strings.Join(s.KeyNames(), ", "))
		}
		return fmt.Sprintf("the primary key was (%s), now (%s)",
			strings.Join(s.KeyNames(), ", "), strings.Join(cur.KeyNames(), ", "))
	}
	for i, b := range s.Key {
		c := cur.Key[i]
		switch {
		case b.Attnum != c.Attnum:
			return fmt.Sprintf("the primary key column %s was dropped and added again", c.Name)
		case b.TypeOID != c.TypeOID || b.Typmod != c.Typmod:
			return fmt.Sprintf("the primary key column %s was %s, now %s", c.Name, b.Type, c.Type)
		case b.Collation != c.Collation:
			return fmt.Sprintf("the collation of the primary key column %s changed", c.Name)
		}
	}
	return ""
}

// ColumnsChanged reports whether the columns of cur differ from those of s
// in name, attnum, type, typmod or generation expression. The leaf hashes
// depend on them.
func (s TableShape) ColumnsChanged(cur TableShape) bool {
	return !slices.EqualFunc(s.Columns, cur.Columns, func(a, b ShapeColumn) bool {
		return a.Name == b.Name && a.Attnum == b.Attnum && a.TypeOID == b.TypeOID && a.Typmod == b.Typmod &&
			a.Generated == b.Generated && a.GenExpr == b.GenExpr
	})
}

// Rewritten reports whether the table of cur is stored in another file than
// that of s. The stored values can differ from those that were hashed, also
// when the definition is the same, so the leaf hashes are stale. A rewrite
// that keeps the values, such as VACUUM FULL or CLUSTER, gives the same
// answer: the cost is one more rehash, never a stale tree.
func (s TableShape) Rewritten(cur TableShape) bool {
	return s.Filenode != cur.Filenode
}

// sameColumnSet reports whether a and b hold the same column names in any
// order. An index may list one column twice, so a name must appear the same
// number of times in both lists.
func sameColumnSet(a, b []string) bool {
	return slices.Equal(slices.Sorted(slices.Values(a)), slices.Sorted(slices.Values(b)))
}

// GetTableShape reads the shape of schema.table from the catalog. It runs
// several statements, so db must be a transaction with one snapshot
// (REPEATABLE READ) for a consistent result.
func GetTableShape(ctx context.Context, db DBQuerier, schema, table string) (TableShape, error) {
	shape := TableShape{Format: TableShapeFormat, Key: []ShapeColumn{}, Columns: []ShapeColumn{}}

	sql, err := RenderSQL(SQLTemplates.GetTableShapeColumns, nil)
	if err != nil {
		return shape, fmt.Errorf("failed to render GetTableShapeColumns SQL: %w", err)
	}
	rows, err := db.Query(ctx, sql, schema, table)
	if err != nil {
		return shape, fmt.Errorf("query to get the columns of '%s.%s' failed: %w", schema, table, err)
	}
	defer rows.Close()

	keyByPos := map[int64]ShapeColumn{}
	for rows.Next() {
		var (
			c         ShapeColumn
			filenode  uint32
			collation uint32
			genExpr   *string
			keyPos    *int64
		)
		if err := rows.Scan(&c.Name, &c.Attnum, &c.TypeOID, &c.Typmod, &c.Type, &collation, &c.Generated, &genExpr, &keyPos, &filenode); err != nil {
			return shape, fmt.Errorf("failed to scan a column of '%s.%s': %w", schema, table, err)
		}
		if genExpr != nil {
			c.GenExpr = *genExpr
		}
		shape.Filenode = filenode
		shape.Columns = append(shape.Columns, c)
		if keyPos != nil {
			k := c
			k.Collation = collation
			keyByPos[*keyPos] = k
		}
	}
	if err := rows.Err(); err != nil {
		return shape, fmt.Errorf("error iterating over the columns of '%s.%s': %w", schema, table, err)
	}
	if len(shape.Columns) == 0 {
		return shape, fmt.Errorf("table '%s.%s' not found or has no columns", schema, table)
	}

	for pos := int64(1); pos <= int64(len(keyByPos)); pos++ {
		k, ok := keyByPos[pos]
		if !ok {
			return shape, fmt.Errorf("the primary key of '%s.%s' has no column at position %d", schema, table, pos)
		}
		shape.Key = append(shape.Key, k)
	}

	if len(shape.Key) > 0 {
		castTypes, err := GetCastTypes(ctx, db, schema, table)
		if err != nil {
			return shape, err
		}
		for i := range shape.Key {
			shape.Key[i].CastType = castTypes[shape.Key[i].Name]
		}
	}

	ri, err := GetReplicaIdentityKey(ctx, db, schema, []string{table})
	if err != nil {
		return shape, err
	}
	d, ok := ri[table]
	if !ok {
		return shape, fmt.Errorf("no replica identity found for '%s.%s'", schema, table)
	}
	shape.ReplicaIdentity = d.ReplicaIdentity
	shape.IdentityColumns = d.KeyColumns
	if shape.IdentityColumns == nil {
		shape.IdentityColumns = []string{}
	}
	return shape, nil
}

// TreeState is what ace_mtree_metadata records about a tree beyond its
// leaves.
type TreeState struct {
	// Shape is the table shape the tree was built for, or nil if the row
	// has no shape of the current TableShapeFormat.
	Shape         *TableShape
	ValidFromLSN  string
	State         string
	InvalidReason string
}

// EnsureTreeStateColumns adds the columns of TreeState to a metadata table
// that does not have them. It alters the table only when a column is
// missing, so that it does not take a lock on the table on each run.
func EnsureTreeStateColumns(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.CountTreeStateColumns, nil)
	if err != nil {
		return fmt.Errorf("failed to render CountTreeStateColumns SQL: %w", err)
	}
	var n int
	if err := db.QueryRow(ctx, sql, config.Get().MTree.Schema).Scan(&n); err != nil {
		return fmt.Errorf("failed to read the columns of the metadata table: %w", err)
	}
	if n == 4 {
		return nil
	}
	sql, err = RenderSQL(SQLTemplates.EnsureTreeStateColumns, nil)
	if err != nil {
		return fmt.Errorf("failed to render EnsureTreeStateColumns SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql); err != nil {
		return fmt.Errorf("failed to add the tree state columns to the metadata table: %w", err)
	}
	return nil
}

// GetTreeState reads the TreeState of the tree of schema.table. A missing
// metadata row gives an error that wraps pgx.ErrNoRows.
func GetTreeState(ctx context.Context, db DBQuerier, schema, table string) (TreeState, error) {
	var st TreeState
	sql, err := RenderSQL(SQLTemplates.GetTreeState, nil)
	if err != nil {
		return st, fmt.Errorf("failed to render GetTreeState SQL: %w", err)
	}
	var shapeJSON, validFrom, reason *string
	if err := db.QueryRow(ctx, sql, schema, table).Scan(&shapeJSON, &validFrom, &st.State, &reason); err != nil {
		return st, fmt.Errorf("query to get the tree state of '%s.%s' failed: %w", schema, table, err)
	}
	if shapeJSON != nil {
		var shape TableShape
		if err := json.Unmarshal([]byte(*shapeJSON), &shape); err != nil {
			return st, fmt.Errorf("failed to read the table shape of the tree of '%s.%s': %w", schema, table, err)
		}
		if shape.Format == TableShapeFormat {
			st.Shape = &shape
		}
	}
	if validFrom != nil {
		st.ValidFromLSN = *validFrom
	}
	if reason != nil {
		st.InvalidReason = *reason
	}
	return st, nil
}

// SetTreeInvalid marks the tree of schema.table as not valid, with reason.
func SetTreeInvalid(ctx context.Context, db DBQuerier, schema, table, reason string) error {
	sql, err := RenderSQL(SQLTemplates.SetTreeInvalid, nil)
	if err != nil {
		return fmt.Errorf("failed to render SetTreeInvalid SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql, TreeStateInvalid, reason, schema, table); err != nil {
		return fmt.Errorf("query to mark the tree of '%s.%s' as not valid failed: %w", schema, table, err)
	}
	return nil
}

// UpdateTreeShape writes a new table shape for the tree of schema.table.
func UpdateTreeShape(ctx context.Context, db DBQuerier, schema, table string, shape TableShape) error {
	data, err := json.Marshal(shape)
	if err != nil {
		return fmt.Errorf("failed to encode the table shape of '%s.%s': %w", schema, table, err)
	}
	sql, err := RenderSQL(SQLTemplates.UpdateTreeShape, nil)
	if err != nil {
		return fmt.Errorf("failed to render UpdateTreeShape SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql, string(data), schema, table); err != nil {
		return fmt.Errorf("query to update the table shape of '%s.%s' failed: %w", schema, table, err)
	}
	return nil
}
