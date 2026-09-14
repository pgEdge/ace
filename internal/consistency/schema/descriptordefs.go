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

// Package schema answers one question: how do two nodes' tables differ in
// structure? It knows nothing about replication or which tables were chosen
// for comparison — those are internal/consistency/topology's and
// internal/consistency/scope's jobs, and this package imports neither. Its
// input is a list of table names and a connection per node; its output is a
// flat list of differences with no notion of "origin" or "target".
//
// This split lets structure comparison run without a working replication
// topology — a freshly built node compared against a known-good one, a
// sanity check between two unrelated databases.
package schema

// Property is one named, textual fact about a structural object: a column's
// type, a constraint's definition, an index's key columns. It is text and
// not a richer type because every kind of object needs the same treatment —
// sort, compare, print — and inventing a type per property kind would only
// get in the way of that.
type Property struct {
	Name  string
	Value string
}

// Object is one structural thing on one node: a table, a column, a
// constraint. Properties is always kept sorted by Name — this is what makes
// two independently-collected Objects comparable property-by-property, and
// what makes their checksum stable. What identifies an Object across nodes
// is its ObjectID (below), never this struct's own fields: Name here is a
// display string and nothing more.
type Object struct {
	Kind string // "table" | "column" | "key" | "constraint" | "domain" | "range" | "composite" | "enum"
	// Name is the object as a person should see it — "public.orders",
	// "public.orders.discount", or for "domain"/"range"/"composite"/"enum"
	// the referenced type's own name ("public.city_budget"), since those
	// Objects exist once per distinct type, not once per column that
	// happens to use it. It is built by joining identifiers with dots, so
	// it is ambiguous by construction and must never be used as a key —
	// that is ObjectID's job.
	Name       string
	Properties []Property
}

// ObjectID identifies one structural object within one Snapshot.
//
// It is a struct, not a "kind:schema.table.column" string: a PostgreSQL
// identifier may itself contain a dot, so table "a.b"'s column "c" and
// table "a"'s column "b.c" would both land on "column:public.a.b.c" under a
// concatenated key, silently colliding in Objects. Kept as separate fields,
// the two cannot collide, and no escaping scheme has to be trusted.
type ObjectID struct {
	Kind   string // as Object.Kind
	Schema string // the namespace the object lives in
	// Name is the table's name for the table-scoped kinds ("table",
	// "column", "key", "constraint"), and the type's own name for
	// "domain"/"range"/"composite"/"enum".
	Name string
	// Attribute is the column name for Kind "column", and empty for every
	// other kind — a table and its own key or constraint set share
	// (Schema, Name) and are told apart by Kind alone.
	Attribute string
}

// TableID, ColumnID, KeyID, ConstraintID and TypeID are the only ways an
// ObjectID is built, so that no caller has to remember which field a
// column name goes in.
func TableID(schema, table string) ObjectID {
	return ObjectID{Kind: "table", Schema: schema, Name: table}
}

// ColumnID identifies one column of one table. The column name goes in
// Attribute and the table's in Name, which is what keeps a table and its
// columns from sharing a key.
func ColumnID(schema, table, column string) ObjectID {
	return ObjectID{Kind: "column", Schema: schema, Name: table, Attribute: column}
}

// KeyID identifies one table's replica identity. There is one such Object
// per table, not one per key column, because the key is compared as a whole:
// the columns (a, b) and (b, a) are different keys.
func KeyID(schema, table string) ObjectID {
	return ObjectID{Kind: "key", Schema: schema, Name: table}
}

// ConstraintID identifies one table's constraints, all of which live on a
// single Object. They cannot be split into one Object per constraint the way
// columns are: PostgreSQL invents a name for an unnamed constraint, so there
// is no name that means the same thing on both nodes to key them by.
func ConstraintID(schema, table string) ObjectID {
	return ObjectID{Kind: "constraint", Schema: schema, Name: table}
}

// TypeID identifies a referenced type. kind is "domain", "range",
// "composite" or "enum"; schema is the type's own namespace, which is not
// necessarily the schema being compared (a column can use a domain that
// lives elsewhere).
func TypeID(kind, schema, typeName string) ObjectID {
	return ObjectID{Kind: kind, Schema: schema, Name: typeName}
}

// DatabaseID identifies the one database-wide Object a Snapshot carries:
// the collation settings every uncollated column inherits. There is exactly
// one per snapshot, so it needs no schema or name to tell it apart.
func DatabaseID() ObjectID {
	return ObjectID{Kind: "database"}
}

// Snapshot is everything Collect read from one node. Objects and
// TableColumns are both keyed by ObjectID for the reason spelled out there.
type Snapshot struct {
	Node    string
	Objects map[ObjectID]Object

	// TableColumns maps a table's TableID to the names of its own columns,
	// in the order Collect read them (already alphabetical — see the
	// GetColumnDescriptors query). Comparing two tables' columns means
	// comparing the union of these two lists, one name at a time; this
	// field exists so that union can be built without scanning the whole
	// Objects map.
	TableColumns map[ObjectID][]string
}
