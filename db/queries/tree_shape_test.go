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
	"strings"
	"testing"
)

func shapeCol(name string, attnum int16, typ uint32, typmod int32, typeName string) ShapeColumn {
	return ShapeColumn{Name: name, Attnum: attnum, TypeOID: typ, Typmod: typmod, Type: typeName}
}

func shapeKeyCol(name string, attnum int16, typ uint32, typeName string, collation uint32) ShapeColumn {
	c := shapeCol(name, attnum, typ, -1, typeName)
	c.Collation = collation
	c.CastType = typeName
	return c
}

// shapeAB is a table (a int, b text, c numeric(10,2)) with the primary key
// (a, b) and REPLICA IDENTITY DEFAULT.
func shapeAB() TableShape {
	return TableShape{
		Format:          TableShapeFormat,
		ReplicaIdentity: "d",
		IdentityColumns: []string{"a", "b"},
		Key:             []ShapeColumn{shapeKeyCol("a", 1, 23, "integer", 0), shapeKeyCol("b", 2, 25, "text", 100)},
		Columns: []ShapeColumn{
			shapeCol("a", 1, 23, -1, "integer"), shapeCol("b", 2, 25, -1, "text"), shapeCol("c", 3, 1700, 655366, "numeric(10,2)"),
		},
	}
}

// TestIdentityProblem checks which replica identities a tree accepts: DEFAULT
// and an index on exactly the primary key columns, in any order.
func TestIdentityProblem(t *testing.T) {
	cases := []struct {
		name   string
		change func(*TableShape)
		want   string
	}{
		{"default", func(*TableShape) {}, ""},
		{"index on the key columns in another order", func(s *TableShape) {
			s.ReplicaIdentity, s.IdentityColumns = "i", []string{"b", "a"}
		}, ""},
		{"index on other columns", func(s *TableShape) {
			s.ReplicaIdentity, s.IdentityColumns = "i", []string{"a", "c"}
		}, "an index on (a, c), not on the primary key columns (a, b)"},
		{"index that repeats a key column", func(s *TableShape) {
			s.ReplicaIdentity, s.IdentityColumns = "i", []string{"a", "a"}
		}, "an index on (a, a), not on the primary key columns (a, b)"},
		{"full", func(s *TableShape) { s.ReplicaIdentity, s.IdentityColumns = "f", []string{} }, "FULL"},
		{"nothing", func(s *TableShape) { s.ReplicaIdentity, s.IdentityColumns = "n", []string{} }, "NOTHING"},
		{"no primary key", func(s *TableShape) { s.Key, s.IdentityColumns = nil, []string{} }, "no primary key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := shapeAB()
			tc.change(&s)
			got := s.IdentityProblem()
			if tc.want == "" && got != "" || tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("IdentityProblem() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestKeyChange checks that every change of the primary key or of the replica
// identity is reported, and that a change of a non-key column is not.
func TestKeyChange(t *testing.T) {
	cases := []struct {
		name   string
		change func(*TableShape)
		want   string
	}{
		{"same", func(*TableShape) {}, ""},
		{"non-key column type", func(s *TableShape) { s.Columns[2] = shapeCol("c", 3, 1700, 655364, "numeric(10,0)") }, ""},
		{"key order", func(s *TableShape) {
			s.Key[0], s.Key[1] = s.Key[1], s.Key[0]
			s.IdentityColumns = []string{"b", "a"}
		}, "the primary key was (a, b), now (b, a)"},
		{"other key", func(s *TableShape) {
			s.Key = []ShapeColumn{shapeKeyCol("c", 3, 1700, "numeric(10,2)", 0)}
			s.IdentityColumns = []string{"c"}
		}, "the primary key was (a, b), now (c)"},
		{"no key", func(s *TableShape) { s.Key, s.IdentityColumns = nil, []string{} }, "the primary key was (a, b), now the table has no primary key"},
		{"key column added again", func(s *TableShape) { s.Key[1].Attnum = 4 }, "the primary key column b was dropped and added again"},
		{"key type", func(s *TableShape) {
			s.Key[0].TypeOID, s.Key[0].Type = 20, "bigint"
		}, "the primary key column a was integer, now bigint"},
		{"key typmod", func(s *TableShape) {
			s.Key[1].Typmod, s.Key[1].Type = 14, "varchar(10)"
		}, "the primary key column b was text, now varchar(10)"},
		{"key collation", func(s *TableShape) { s.Key[1].Collation = 950 }, "the collation of the primary key column b changed"},
		{"key column renamed", func(s *TableShape) { s.Key[0].Name = "x" }, "the primary key was (a, b), now (x, b)"},
		{"replica identity full", func(s *TableShape) {
			s.ReplicaIdentity, s.IdentityColumns = "f", []string{}
		}, "the replica identity was DEFAULT, now FULL"},
		{"replica identity index on the key", func(s *TableShape) {
			s.ReplicaIdentity = "i"
		}, "the replica identity was DEFAULT, now USING INDEX on (a, b)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := shapeAB()
			tc.change(&cur)
			if got := shapeAB().KeyChange(cur); got != tc.want {
				t.Fatalf("KeyChange() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestKeyChangeIndexIdentity checks that another index on the primary key
// columns is still a replica identity on the key, but a change from one such
// index to another one on other columns is a change of the replica identity.
func TestKeyChangeIndexIdentity(t *testing.T) {
	built := shapeAB()
	built.ReplicaIdentity = "i"
	cur := shapeAB()
	cur.ReplicaIdentity, cur.IdentityColumns = "i", []string{"b", "a"}
	if got := built.KeyChange(cur); got != "" {
		t.Fatalf("KeyChange() = %q for the same index columns in another order", got)
	}
	cur.IdentityColumns = []string{"a", "c"}
	if got := built.KeyChange(cur); !strings.Contains(got, "the replica identity was USING INDEX on (a, b), now USING INDEX on (a, c)") {
		t.Fatalf("KeyChange() = %q", got)
	}
	// An index that repeats a key column is not on the key columns, in both
	// directions of the comparison.
	cur.IdentityColumns = []string{"a", "a"}
	if got := built.KeyChange(cur); got == "" {
		t.Fatal("KeyChange() = \"\" for an index on (a, a) after (a, b)")
	}
	if got := cur.KeyChange(built); got == "" {
		t.Fatal("KeyChange() = \"\" for an index on (a, b) after (a, a)")
	}
}

// TestColumnsChanged checks which column changes make the leaf hashes stale.
// A new name of a type, with the same OID, does not.
func TestColumnsChanged(t *testing.T) {
	cases := []struct {
		name   string
		change func(*TableShape)
		want   bool
	}{
		{"same", func(*TableShape) {}, false},
		{"typmod", func(s *TableShape) { s.Columns[2].Typmod = 655364 }, true},
		{"type", func(s *TableShape) { s.Columns[2].TypeOID = 701 }, true},
		{"added", func(s *TableShape) { s.Columns = append(s.Columns, shapeCol("d", 4, 23, -1, "integer")) }, true},
		{"dropped", func(s *TableShape) { s.Columns = s.Columns[:2] }, true},
		{"dropped and added again", func(s *TableShape) { s.Columns[2].Attnum = 4 }, true},
		{"renamed", func(s *TableShape) { s.Columns[2].Name = "d" }, true},
		{"type name only", func(s *TableShape) { s.Columns[2].Type = "numeric" }, false},
		{"generated column added", func(s *TableShape) {
			g := shapeCol("g", 4, 23, -1, "integer")
			g.Generated, g.GenExpr = "s", "e1"
			s.Columns = append(s.Columns, g)
		}, true},
		{"generation expression", func(s *TableShape) {
			s.Columns[2].Generated, s.Columns[2].GenExpr = "s", "e2"
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cur := shapeAB()
			tc.change(&cur)
			if got := shapeAB().ColumnsChanged(cur); got != tc.want {
				t.Fatalf("ColumnsChanged() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestRewritten checks that a new file of the table makes the leaf hashes
// stale also when the definition is the same.
func TestRewritten(t *testing.T) {
	built := shapeAB()
	built.Filenode = 16500
	cur := shapeAB()
	cur.Filenode = 16500
	if built.Rewritten(cur) {
		t.Fatal("Rewritten() = true for the same file")
	}
	cur.Filenode = 16620
	if !built.Rewritten(cur) {
		t.Fatal("Rewritten() = false for another file")
	}
	if built.ColumnsChanged(cur) {
		t.Fatal("ColumnsChanged() = true for a rewrite with the same columns")
	}
}
