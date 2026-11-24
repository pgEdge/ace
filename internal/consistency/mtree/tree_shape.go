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
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
)

// ErrMtreeInvalid reports that the Merkle tree of the table no longer fits
// the table and must be built again.
var ErrMtreeInvalid = errors.New("the Merkle tree is not valid")

// olderTreeReason is the reason of a tree whose metadata has no table shape.
const olderTreeReason = "the tree was built by an older ACE version, which did not record the primary key and the columns of the table"

// readShape runs fn in a REPEATABLE READ transaction on pool, so that all
// catalog reads of fn see one snapshot, and commits it if fn succeeds.
func (m *MerkleTreeTask) readShape(pool *pgxpool.Pool, fn func(tx pgx.Tx) error) error {
	tx, err := pool.BeginTx(m.Ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return err
	}
	defer tx.Rollback(m.Ctx)
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(m.Ctx)
}

// taskMismatch returns a message if shape does not have the key and the
// columns that the checks of the task read: the leaves are cut and hashed
// with those, so a shape with others must not be recorded for them.
func (m *MerkleTreeTask) taskMismatch(shape queries.TableShape) string {
	if !slices.Equal(shape.KeyNames(), m.Key) || !slices.Equal(shape.ColumnNames(), m.Cols) {
		return "the primary key or the columns changed after the checks of this command"
	}
	return ""
}

// checkTreeShape compares the table on a node with the shape that its tree
// was built for. A tree that is not valid, a tree without a recorded shape,
// and a change of the primary key or of the replica identity give an error
// that wraps ErrMtreeInvalid; the last two also mark the tree as not valid
// on the node. Otherwise it returns cur, the shape of the table now, and
// whether the leaf hashes are stale because the columns changed or the table
// was rewritten since the shape was recorded.
func (m *MerkleTreeTask) checkTreeShape(pool *pgxpool.Pool, nodeName string) (cur queries.TableShape, columnsChanged bool, err error) {
	var invalid error
	err = m.readShape(pool, func(tx pgx.Tx) error {
		st, err := queries.GetTreeState(m.Ctx, tx, m.Schema, m.Table)
		if err != nil {
			if isMissingTreeErr(err) {
				return m.missingTreeError(nodeName, err)
			}
			return fmt.Errorf("error reading the tree state on node %s: %w", nodeName, err)
		}
		cur, err = queries.GetTableShape(m.Ctx, tx, m.Schema, m.Table)
		if err != nil {
			return fmt.Errorf("error reading the table on node %s: %w", nodeName, err)
		}
		if st.State == queries.TreeStateInvalid {
			return m.invalidTreeError(nodeName, st.InvalidReason, cur)
		}

		reason := olderTreeReason
		if st.Shape != nil {
			reason = st.Shape.KeyChange(cur)
		}
		if reason != "" {
			if err := queries.SetTreeInvalid(m.Ctx, tx, m.Schema, m.Table, reason); err != nil {
				return fmt.Errorf("error marking the tree as not valid on node %s: %w", nodeName, err)
			}
			// An error here would roll back the mark. Return nil so that
			// the mark is committed, and report the tree after the commit.
			invalid = m.invalidTreeError(nodeName, reason, cur)
			return nil
		}
		if problem := m.taskMismatch(cur); problem != "" {
			return fmt.Errorf("%s changed on node %s: %s; run the command again", m.QualifiedTableName, nodeName, problem)
		}
		columnsChanged = st.Shape.ColumnsChanged(cur) || st.Shape.Rewritten(cur)
		return nil
	})
	if err == nil {
		err = invalid
	}
	return cur, columnsChanged, err
}

// buildShapes reads the shape of the table on every node before mtree build
// puts it into the publication. The replica identity must fit a tree on
// every node and be the same on all nodes.
//
// The checks of the command compared the key types and collations across
// nodes, but in other transactions. The shape records them by OID, which
// later runs compare only with the same node, so a change between the checks
// and this read would never be seen. So they are compared again here, in
// the snapshot of the shape, with those of the reference node at the checks.
func (m *MerkleTreeTask) buildShapes(pools map[string]*pgxpool.Pool, nodeNames []string) (map[string]queries.TableShape, error) {
	shapes := make(map[string]queries.TableShape, len(nodeNames))
	for _, name := range nodeNames {
		var (
			shape      queries.TableShape
			colTypes   map[string]string
			collations map[string]string
		)
		err := m.readShape(pools[name], func(tx pgx.Tx) error {
			var err error
			if shape, err = queries.GetTableShape(m.Ctx, tx, m.Schema, m.Table); err != nil {
				return err
			}
			if _, colTypes, err = queries.GetColumns(m.Ctx, tx, m.Schema, m.Table); err != nil {
				return err
			}
			collations, err = queries.GetCollationOrders(m.Ctx, tx, m.Schema, m.Table)
			return err
		})
		if err != nil {
			return nil, fmt.Errorf("failed to read the table on node %s: %w", name, err)
		}
		if problem := shape.IdentityProblem(); problem != "" {
			return nil, fmt.Errorf("cannot build a Merkle tree on %s: on node %s %s; "+
				"mtree needs REPLICA IDENTITY DEFAULT, or USING INDEX on an index with exactly the primary key columns",
				m.QualifiedTableName, name, problem)
		}
		if problem := m.taskMismatch(shape); problem != "" {
			return nil, fmt.Errorf("%s changed on node %s: %s; run mtree build again", m.QualifiedTableName, name, problem)
		}
		if err := m.keyMatchesRef(name, colTypes, collations, m.Key); err != nil {
			return nil, fmt.Errorf("%s changed on node %s after the checks of this command: %w; run mtree build again",
				m.QualifiedTableName, name, err)
		}
		shapes[name] = shape
	}
	ref := nodeNames[0]
	for _, name := range nodeNames[1:] {
		if a, b := shapes[ref], shapes[name]; a.ReplicaIdentity != b.ReplicaIdentity {
			return nil, fmt.Errorf("cannot build a Merkle tree on %s: the replica identity is %s on node %s and %s on node %s; "+
				"use the same replica identity on every node",
				m.QualifiedTableName, a.IdentityText(), ref, b.IdentityText(), name)
		}
	}
	return shapes, nil
}

// shapeChange returns how the table now differs from start, the shape read
// at the start of a build, or "".
func shapeChange(start, now queries.TableShape) string {
	if reason := start.KeyChange(now); reason != "" {
		return reason
	}
	if start.ColumnsChanged(now) {
		return "the columns changed"
	}
	if start.Rewritten(now) {
		return "the table was rewritten"
	}
	return ""
}

// invalidTreeError builds the error for a tree that is not valid on a node.
// cur is the shape of the table now. If its replica identity does not fit,
// UPDATE and DELETE on the table can fail while the table is in the
// publication, so the error also names mtree teardown-table.
func (m *MerkleTreeTask) invalidTreeError(nodeName, reason string, cur queries.TableShape) error {
	fix := fmt.Sprintf("run '%s'", m.buildCommand())
	if problem := cur.IdentityProblem(); problem != "" {
		fix = fmt.Sprintf("%s now, so set a replica identity on the primary key and run '%s', "+
			"or run '%s' to remove the table from the CDC publication",
			problem, m.buildCommand(), m.teardownTableCommand())
	}
	return fmt.Errorf("%w for %s on node %s: %s; %s", ErrMtreeInvalid, m.QualifiedTableName, nodeName, reason, fix)
}
