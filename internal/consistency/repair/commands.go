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
	// operations of one command fail together.
	Command int `json:"command"`
}

// repairSets holds the row operations of a repair: for each node, the rows to
// insert, to update and to delete, by key text.
type repairSets struct {
	inserts map[string]map[string]map[string]any
	updates map[string]map[string]map[string]any
	deletes map[string]map[string]map[string]any
}

// newRepairSets returns empty row operation sets.
func newRepairSets() *repairSets {
	return &repairSets{
		inserts: make(map[string]map[string]map[string]any),
		updates: make(map[string]map[string]map[string]any),
		deletes: make(map[string]map[string]map[string]any),
	}
}

// addRow sets row as the row of key pk on node in target. A second row for
// the same node and key replaces the first one.
//
// XXX With more than two nodes, a repair plan can give one key on one node
// a row from two node pairs; the pair that comes last wins, and the order of
// the pairs is not defined.
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
		addRow(s.updates, node, pk, row)
	} else {
		addRow(s.inserts, node, pk, row)
	}
}

// remove adds a delete of key pk on node; row is the row that the diff saw
// there.
func (s *repairSets) remove(node, pk string, row map[string]any) {
	addRow(s.deletes, node, pk, row)
}

// nodes returns the names of the nodes that have row operations, sorted.
func (s *repairSets) nodes() []string {
	seen := make(map[string]struct{})
	for _, m := range []map[string]map[string]map[string]any{s.inserts, s.updates, s.deletes} {
		for node, rows := range m {
			if len(rows) > 0 {
				seen[node] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for node := range seen {
		out = append(out, node)
	}
	sort.Strings(out)
	return out
}

// empty reports whether s has no row operations.
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
func (t *TableRepairTask) rowStatement(op string, row map[string]any, colTypes map[string]string) (string, []any, error) {
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
	for i, c := range cols {
		val, ok := row[c]
		if !ok || val == nil {
			continue
		}
		typ, ok := colTypes[c]
		if !ok {
			return "", nil, fmt.Errorf("no type for column %s on the target node", c)
		}
		v, err := convertValueForType(val, typ)
		if err != nil {
			return "", nil, fmt.Errorf("cannot convert the value of column %s to %s: %w", c, typ, err)
		}
		args[i] = v
	}

	if op == opInsert {
		names := make([]string, len(cols))
		params := make([]string, len(cols))
		for i, c := range cols {
			names[i] = pgx.Identifier{c}.Sanitize()
			params[i] = fmt.Sprintf("$%d", i+1)
		}
		return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(names, ", "), strings.Join(params, ", ")), args, nil
	}

	where := make([]string, len(t.Key))
	for i, k := range t.Key {
		where[i] = fmt.Sprintf("%s = $%d", pgx.Identifier{k}.Sanitize(), i+1)
	}
	if op == opDelete {
		return fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(where, " AND ")), args, nil
	}
	set := make([]string, 0, len(cols)-len(t.Key))
	for i, c := range cols[len(t.Key):] {
		ident := pgx.Identifier{c}.Sanitize()
		set = append(set, fmt.Sprintf("%s = $%d", ident, len(t.Key)+i+1))
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
