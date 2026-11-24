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

package mtree

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/pgedge/ace/db/queries"
)

// TestInvalidTreeError checks that the error for a tree that is not valid
// wraps ErrMtreeInvalid and names mtree build, and that it also names mtree
// teardown-table when the replica identity of the table does not fit a tree.
func TestInvalidTreeError(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ClusterName = "c1"
	m.QualifiedTableName = "s.t"
	key := queries.ShapeColumn{Name: "a", Attnum: 1, TypeOID: 23, Typmod: -1, Type: "integer"}
	cur := queries.TableShape{
		Format: queries.TableShapeFormat, ReplicaIdentity: queries.ReplicaIdentityDefault,
		IdentityColumns: []string{"a"}, Key: []queries.ShapeColumn{key}, Columns: []queries.ShapeColumn{key},
	}

	err := m.invalidTreeError("n1", "the primary key was (b), now (a)", cur)
	if !errors.Is(err, ErrMtreeInvalid) || !errors.Is(fmt.Errorf("wrapped: %w", err), ErrMtreeInvalid) {
		t.Fatalf("errors.Is(%v, ErrMtreeInvalid) = false", err)
	}
	want := "the Merkle tree is not valid for s.t on node n1: the primary key was (b), now (a); run 'ace mtree build c1 s.t'"
	if err.Error() != want {
		t.Fatalf("Error() = %q, want %q", err.Error(), want)
	}

	cur.ReplicaIdentity, cur.IdentityColumns = queries.ReplicaIdentityNothing, []string{}
	msg := m.invalidTreeError("n1", "the replica identity was DEFAULT, now NOTHING", cur).Error()
	for _, part := range []string{"the replica identity is NOTHING now", "ace mtree build c1 s.t", "ace mtree teardown-table c1 s.t"} {
		if !strings.Contains(msg, part) {
			t.Fatalf("Error() = %q, want it to contain %q", msg, part)
		}
	}
}
