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

package repair

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/logger"
)

// Kinds of row operations.
const (
	opInsert = "insert"
	opUpdate = "update"
	opDelete = "delete"
	// opFixNulls sets the columns of a row that are NULL on the node to the
	// values of the other node.
	opFixNulls = "update_nulls"
	// opConflict marks a key that got no row operation, because two node
	// pairs of a repair plan gave different ones.
	opConflict = "conflict"
)

// commandSize is the largest number of row operations in one command. One
// failed row rolls back all rows of its command, so a larger command saves
// round trips but loses more work to one bad row.
const commandSize = 1000

// maxLoggedKeys is the largest number of keys that the log lists for one
// failed command.
const maxLoggedKeys = 20

// FailedRowOp is a row operation that repair did not apply.
type FailedRowOp struct {
	Node      string         `json:"node"`
	Operation string         `json:"operation"`
	Key       map[string]any `json:"key"`
	Reason    string         `json:"reason"`
	// Command is the number of the command on its node, from 1. The row
	// operations of one command fail together. A conflict is not sent to the
	// node and has the number 0.
	Command int `json:"command"`
}

// repairSets holds the row operations of a repair: for each node, the rows to
// insert, to update and to delete, by key text. A key has at most one row
// operation on a node.
//
// With more than two nodes, a repair plan can give one key on one node a row
// operation from two node pairs. The diffs of the pairs are taken at
// different times, so the pairs can see the row on the node differently. If
// the two row operations are not the same, the key gets no row operation on
// this node: it goes to conflicts, and repair reports it as a failed row.
type repairSets struct {
	inserts map[string]map[string]map[string]any
	updates map[string]map[string]map[string]any
	deletes map[string]map[string]map[string]any

	conflicts map[string]map[string]rowConflict

	// pair is the node pair whose row operations are added now, and pairs
	// holds the pair that gave each row operation.
	pair  string
	pairs map[string]map[string]string
}

// rowConflict is a key that has no row operation on a node because two node
// pairs gave different ones. row is the row of the first row operation.
type rowConflict struct {
	row    map[string]any
	reason string
}

// newRepairSets returns empty row operation sets.
func newRepairSets() *repairSets {
	return &repairSets{
		inserts:   make(map[string]map[string]map[string]any),
		updates:   make(map[string]map[string]map[string]any),
		deletes:   make(map[string]map[string]map[string]any),
		conflicts: make(map[string]map[string]rowConflict),
		pairs:     make(map[string]map[string]string),
	}
}

// addRow sets row as the row of key pk on node in target.
func addRow(target map[string]map[string]map[string]any, node, pk string, row map[string]any) {
	if target[node] == nil {
		target[node] = make(map[string]map[string]any)
	}
	target[node][pk] = row
}

// write adds row as the new content of key pk on node: an UPDATE if node has
// the row, an INSERT if it does not.
func (s *repairSets) write(node, pk string, row map[string]any, onNode bool) {
	if onNode {
		s.add(opUpdate, node, pk, row)
	} else {
		s.add(opInsert, node, pk, row)
	}
}

// remove adds a delete of key pk on node; row is the row that the diff saw
// there.
func (s *repairSets) remove(node, pk string, row map[string]any) {
	s.add(opDelete, node, pk, row)
}

// add adds the row operation op of key pk on node. If the key already has a
// row operation on node, the new one must be the same: the same kind and,
// for an insert or an update, the same row. Otherwise the key moves to
// conflicts and keeps no row operation on node.
func (s *repairSets) add(op, node, pk string, row map[string]any) {
	if _, ok := s.conflicts[node][pk]; ok {
		return
	}
	sets := map[string]map[string]map[string]map[string]any{opInsert: s.inserts, opUpdate: s.updates, opDelete: s.deletes}
	for _, prevOp := range []string{opInsert, opUpdate, opDelete} {
		prev, ok := sets[prevOp][node][pk]
		if !ok {
			continue
		}
		if prevOp == op && (op == opDelete || reflect.DeepEqual(prev, row)) {
			return
		}
		delete(sets[prevOp][node], pk)
		reason := fmt.Sprintf("node pairs %s and %s give different row operations for this key: %s and %s",
			s.pairs[node][pk], s.pair, prevOp, op)
		if prevOp == op {
			reason = fmt.Sprintf("node pairs %s and %s give different rows to %s for this key",
				s.pairs[node][pk], s.pair, op)
		}
		if s.conflicts[node] == nil {
			s.conflicts[node] = make(map[string]rowConflict)
		}
		s.conflicts[node][pk] = rowConflict{row: prev, reason: reason}
		return
	}
	addRow(sets[op], node, pk, row)
	if s.pairs[node] == nil {
		s.pairs[node] = make(map[string]string)
	}
	s.pairs[node][pk] = s.pair
}

// nodes returns the names of the nodes that have row operations or
// conflicts, sorted.
func (s *repairSets) nodes() []string {
	seen := make(map[string]struct{})
	for _, m := range []map[string]map[string]map[string]any{s.inserts, s.updates, s.deletes} {
		for node, rows := range m {
			if len(rows) > 0 {
				seen[node] = struct{}{}
			}
		}
	}
	for node, keys := range s.conflicts {
		if len(keys) > 0 {
			seen[node] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for node := range seen {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// empty reports whether s has no row operations and no conflicts.
func (s *repairSets) empty() bool {
	return len(s.nodes()) == 0
}

// rowStatement returns the SQL text and the parameters of one row operation
// of the kind op for row. The parameters are, in this order: the key and the
// columns to set for an update and a NULL fix; all columns for an insert; the
// key for a delete. For an insert, an update and a delete the text is the same
// for all rows of a table, so the server prepares it once for each
// connection. A NULL fix sets only the columns of row outside the key, and
// only where they are still NULL, so that it never writes over a value that
// appeared after the diff.
//
// Every value is sent as text, the output of its type on the node of the
// diff, and the server reads it with the input function of the type:
// $n::text::type. castTypes gives the type without the type modifier; the
// column then applies its modifier when it gets the value, as for any
// assigned value: a text too long for varchar(n) is an error, not a text cut
// to fit. A key is found by the same rule as in the diff: the key of the row
// must be equal to the key text and print as the key text, byte for byte.
func (t *TableRepairTask) rowStatement(op string, row map[string]any, castTypes map[string]string) (string, []any, error) {
	table := pgx.Identifier{t.Schema, t.Table}.Sanitize()
	isKey := make(map[string]bool, len(t.Key))
	for _, k := range t.Key {
		isKey[k] = true
	}
	var cols []string
	switch op {
	case opInsert:
		cols = t.Cols
	case opUpdate:
		cols = append(cols, t.Key...)
		for _, c := range t.Cols {
			if !isKey[c] {
				cols = append(cols, c)
			}
		}
	case opFixNulls:
		cols = append(cols, t.Key...)
		var set []string
		for c := range row {
			if !isKey[c] {
				set = append(set, c)
			}
		}
		sort.Strings(set)
		cols = append(cols, set...)
	case opDelete:
		cols = t.Key
	default:
		return "", nil, fmt.Errorf("unknown row operation %q", op)
	}

	args := make([]any, len(cols))
	params := make([]string, len(cols))
	for i, c := range cols {
		typ, ok := castTypes[c]
		if !ok || typ == "" {
			return "", nil, fmt.Errorf("no type for column %s on the target node", c)
		}
		params[i] = fmt.Sprintf("$%d::text::%s", i+1, typ)
		if text, ok := valueText(row[c]); ok {
			args[i] = text
		}
	}

	if op == opInsert {
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = pgx.Identifier{c}.Sanitize()
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(params, ", ")), args, nil
	}

	where := make([]string, 0, 2*len(t.Key))
	for i, k := range t.Key {
		if args[i] == nil {
			return "", nil, fmt.Errorf("primary key column %s is NULL", k)
		}
		ident := pgx.Identifier{k}.Sanitize()
		where = append(where, fmt.Sprintf("%s = %s", ident, params[i]),
			fmt.Sprintf(`format('%%s', %s) COLLATE "C" = $%d`, queries.ColExpr("", k), i+1))
	}
	if op == opDelete {
		return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(where, " AND ")), args, nil
	}
	set := make([]string, 0, len(cols)-len(t.Key))
	for i, c := range cols[len(t.Key):] {
		ident := pgx.Identifier{c}.Sanitize()
		set = append(set, fmt.Sprintf("%s = %s", ident, params[len(t.Key)+i]))
		if op == opFixNulls {
			where = append(where, ident+" IS NULL")
		}
	}
	if len(set) == 0 {
		return "", nil, fmt.Errorf("table %s has no columns outside the primary key to update", table)
	}
	return fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(set, ", "), strings.Join(where, " AND ")), args, nil
}

// keyOf returns the key columns of row.
func (t *TableRepairTask) keyOf(row map[string]any) map[string]any {
	key := make(map[string]any, len(t.Key))
	for _, k := range t.Key {
		key[k] = row[k]
	}
	return key
}

// applyRows applies the row operations of the kind op on node. It sends them
// in commands of at most commandSize rows. Each command runs in its own
// transaction, and each row operation of it must change exactly one row.
// Otherwise the command is rolled back, all its row operations are recorded
// in FailedOps, and the next command runs. origins, if not empty, holds the
// origin of each row for --preserve-origin: rows of one origin go into the
// same commands, and each command gets the origin of its rows. applyRows
// returns the rows that were applied.
//
// An error that does not come from a row (the connection, the session or
// transaction setup, the replication origin) stops applyRows: it returns the
// rows applied so far and the error, and does not record the other rows as
// failed.
func (t *TableRepairTask) applyRows(node, op string, rows map[string]map[string]any, colTypes map[string]string, origins map[string]*rowOriginInfo) ([]map[string]any, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	if t.commandSeq == nil {
		t.commandSeq = make(map[string]int)
	}

	pool, ok := t.Pools[node]
	if !ok || pool == nil {
		return nil, fmt.Errorf("no connection pool for node %s", node)
	}
	conn, err := pool.Acquire(t.Ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot get a connection to node %s: %w", node, err)
	}
	sessionOrigin := ""
	defer func() {
		if sessionOrigin != "" {
			// A connection that keeps a replication origin would give this
			// origin to the next user of the pool, so it is closed instead
			// of being returned as it is.
			if err := queries.ResetReplicationOriginSession(t.Ctx, conn); err != nil {
				logger.Warn("failed to reset the replication origin session on %s, the connection is closed: %v", node, err)
				_ = conn.Conn().Close(t.Ctx)
			}
		}
		conn.Release()
	}()

	var applied []map[string]any
	groups := groupUpsertsByOrigin(rows, origins, len(origins) > 0)
	for _, bk := range sortedOriginKeys(groups) {
		group := groups[bk]
		keys := sortedKeys(group)
		useOrigin := len(origins) > 0 && bk.nodeOrigin != ""
		if useOrigin && bk.nodeOrigin != sessionOrigin {
			if err := t.setSessionOrigin(conn, &sessionOrigin, bk.nodeOrigin); err != nil {
				return applied, fmt.Errorf("%s on %s: %w", op, node, err)
			}
		} else if !useOrigin && sessionOrigin != "" {
			if err := queries.ResetReplicationOriginSession(t.Ctx, conn); err != nil {
				return applied, fmt.Errorf("%s on %s: cannot reset the replication origin session: %w", op, node, err)
			}
			sessionOrigin = ""
		}
		for start := 0; start < len(keys); start += commandSize {
			batch := keys[start:min(start+commandSize, len(keys))]
			done, err := t.runCommand(conn, node, op, batch, group, colTypes, bk, useOrigin)
			applied = append(applied, done...)
			if err != nil {
				return applied, err
			}
		}
	}
	return applied, nil
}

// setSessionOrigin makes origin the replication origin of the session of
// conn, and creates it if it does not exist.
func (t *TableRepairTask) setSessionOrigin(conn *pgxpool.Conn, current *string, origin string) error {
	if *current != "" {
		if err := queries.ResetReplicationOriginSession(t.Ctx, conn); err != nil {
			return fmt.Errorf("cannot reset the replication origin session: %w", err)
		}
		*current = ""
	}
	name := fmt.Sprintf("node_%s", origin)
	id, err := queries.GetReplicationOriginByName(t.Ctx, conn, name)
	if err != nil {
		return fmt.Errorf("cannot read the replication origin %s: %w", name, err)
	}
	if id == nil {
		if _, err := queries.CreateReplicationOrigin(t.Ctx, conn, name); err != nil {
			return fmt.Errorf("cannot create the replication origin %s: %w", name, err)
		}
	}
	if err := queries.SetupReplicationOriginSession(t.Ctx, conn, name); err != nil {
		return fmt.Errorf("cannot set up the replication origin session %s: %w", name, err)
	}
	*current = origin
	return nil
}

// runCommand runs one command: the row operations of keys, in one
// transaction. It returns the applied rows. If a row of the command failed,
// every row of it is in FailedOps; rows dropped by the stale check are only
// logged. An error in the setup or the end of the transaction does not come
// from a row; runCommand returns it, and records no row. If COMMIT gets no
// answer from the server, the command may or may not be committed: its rows
// go to InDoubtOps, and runCommand returns an error.
func (t *TableRepairTask) runCommand(conn *pgxpool.Conn, node, op string, keys []string, rows map[string]map[string]any, colTypes map[string]string, bk originBatchKey, useOrigin bool) ([]map[string]any, error) {
	t.commandSeq[node]++
	cmd := t.commandSeq[node]
	// setupErr wraps an error that does not come from a row; it stops the
	// repair.
	setupErr := func(err error) ([]map[string]any, error) {
		return nil, fmt.Errorf("%s command %d on %s: %w", op, cmd, node, err)
	}
	fail := func(keys []string, culprit, reason string) {
		for _, pk := range keys {
			r := reason
			if culprit != "" && pk != culprit {
				r = fmt.Sprintf("rolled back with its command: the row %s failed", culprit)
			}
			t.FailedOps = append(t.FailedOps, FailedRowOp{Node: node, Operation: op, Key: t.keyOf(rows[pk]), Reason: r, Command: cmd})
		}
		t.logFailedCommand(node, op, keys, reason)
	}

	tx, err := conn.Begin(t.Ctx)
	if err != nil {
		return setupErr(fmt.Errorf("cannot begin a transaction: %w", err))
	}
	defer tx.Rollback(t.Ctx)

	spockActive, err := t.setupTransactionMode(tx, node)
	if err != nil {
		return setupErr(err)
	}
	if useOrigin {
		commitTS, err := parseBatchKeyTimestamp(bk)
		if err != nil {
			return setupErr(err)
		}
		lsn, ok, err := t.resolveOriginLSN(bk, node, commitTS)
		if err != nil {
			return setupErr(err)
		}
		if ok && lsn != nil && commitTS != nil {
			if err := t.setupReplicationOriginXact(tx, lsn, commitTS); err != nil {
				return setupErr(err)
			}
		}
	}

	// The stale check of a repair plan drops the rows whose target row is
	// newer than the diff, and logs them.
	batch := make(map[string]map[string]any, len(keys))
	for _, pk := range keys {
		batch[pk] = rows[pk]
	}
	if err := t.filterStaleRepairs(t.Ctx, tx, node, batch, colTypes, op); err != nil {
		return setupErr(err)
	}
	keys = sortedKeys(batch)
	if len(keys) == 0 {
		return nil, nil
	}

	b := &pgx.Batch{}
	for _, pk := range keys {
		sql, args, err := t.rowStatement(op, batch[pk], colTypes)
		if err != nil {
			fail(keys, pk, err.Error())
			return nil, nil
		}
		b.Queue(sql, args...)
	}
	// After the first failed statement the server rejects all later ones of
	// the transaction, so only the first failure names the row at fault.
	br := tx.SendBatch(t.Ctx, b)
	culprit, reason := "", ""
	for _, pk := range keys {
		tag, err := br.Exec()
		if err != nil {
			if !rejectedByServer(err) {
				_ = br.Close()
				return setupErr(err)
			}
			culprit, reason = pk, err.Error()
			break
		}
		if n := tag.RowsAffected(); n != 1 {
			culprit, reason = pk, fmt.Sprintf("%s changed %d rows, want 1", op, n)
			break
		}
	}
	// An error that only the end of the batch reports belongs to the command,
	// not to one row of it.
	if err := br.Close(); culprit == "" && err != nil {
		if !rejectedByServer(err) {
			return setupErr(err)
		}
		fail(keys, "", err.Error())
		return nil, nil
	}
	if culprit != "" {
		fail(keys, culprit, reason)
		return nil, nil
	}

	if spockActive {
		if err := t.disableSpockRepairMode(tx, node); err != nil {
			return setupErr(err)
		}
	}
	if err := tx.Commit(t.Ctx); err != nil {
		// The server rolled the transaction back, for example on a deferred
		// constraint: this is a failure of the rows.
		if rejectedByServer(err) {
			fail(keys, "", fmt.Sprintf("commit failed: %v", err))
			return nil, nil
		}
		// No answer: the server may have committed before the connection
		// broke, so the rows are neither applied nor failed.
		reason := fmt.Sprintf("the result of the commit is unknown: %v", err)
		for _, pk := range keys {
			t.InDoubtOps = append(t.InDoubtOps, FailedRowOp{Node: node, Operation: op, Key: t.keyOf(rows[pk]), Reason: reason, Command: cmd})
		}
		t.logFailedCommand(node, op, keys, reason)
		return setupErr(fmt.Errorf("%s; run table-diff to see the state of its rows", reason))
	}

	out := make([]map[string]any, 0, len(keys))
	for _, pk := range keys {
		out = append(out, batch[pk])
	}
	return out, nil
}

// rejectedByServer reports whether err is an answer of the server that a
// statement or the commit failed; the server has then rolled the transaction
// back. Any other error, such as a broken connection or a cancelled context,
// is not about the rows, and during COMMIT it leaves the result unknown.
func rejectedByServer(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) || errors.Is(err, pgx.ErrTxCommitRollback)
}

// logFailedCommand logs a failed command with the keys of its rows. The
// other values of the rows are not logged: they can be large, and they can
// hold data that must not go to a log file.
func (t *TableRepairTask) logFailedCommand(node, op string, keys []string, reason string) {
	shown := keys
	more := ""
	if len(shown) > maxLoggedKeys {
		shown = shown[:maxLoggedKeys]
		more = fmt.Sprintf(" and %d more", len(keys)-maxLoggedKeys)
	}
	logger.Error("table-repair: %s of %d rows on %s failed: %s; keys: %s%s",
		op, len(keys), node, reason, strings.Join(shown, ", "), more)
}

// recordConflicts records the conflicts of sets in FailedOps, logs their keys
// and adds them to the repair report. They are known before any command runs,
// so the report has them even if a later setup error stops the repair before
// it reaches their node.
func (t *TableRepairTask) recordConflicts(sets *repairSets) {
	nodes := make([]string, 0, len(sets.conflicts))
	for node := range sets.conflicts {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		keys := make([]string, 0, len(sets.conflicts[node]))
		for pk := range sets.conflicts[node] {
			keys = append(keys, pk)
		}
		sort.Strings(keys)
		for _, pk := range keys {
			c := sets.conflicts[node][pk]
			t.FailedOps = append(t.FailedOps, FailedRowOp{Node: node, Operation: opConflict, Key: t.keyOf(c.row), Reason: c.reason})
		}
		t.logFailedCommand(node, opConflict, keys, "node pairs of the repair plan give different row operations")
		t.reportNodeChanges(node, nil, nil, nil)
	}
}

// sortedKeys returns the keys of rows in byte order, so that the commands of
// a node hold the same rows in every run of the same diff.
func sortedKeys(rows map[string]map[string]any) []string {
	keys := make([]string, 0, len(rows))
	for pk := range rows {
		keys = append(keys, pk)
	}
	sort.Strings(keys)
	return keys
}

// sortedOriginKeys returns the origin groups in a fixed order: by origin
// node, then by commit timestamp.
func sortedOriginKeys(groups map[originBatchKey]map[string]map[string]any) []originBatchKey {
	keys := make([]originBatchKey, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if a.nodeOrigin != b.nodeOrigin {
			return a.nodeOrigin < b.nodeOrigin
		}
		if a.timestamp != b.timestamp {
			return a.timestamp < b.timestamp
		}
		return a.lsn < b.lsn
	})
	return keys
}
