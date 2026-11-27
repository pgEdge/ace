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

package cdc

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pgedge/ace/pkg/logger"
)

// replConn is a replication connection after START_REPLICATION. Only the
// goroutine that reads the stream uses it.
type replConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	// SendStatus sends a standby status update with pos as the write, flush
	// and apply positions.
	SendStatus(ctx context.Context, pos pglogrepl.LSN) error
	Close(ctx context.Context) error
}

// pgReplConn is a replConn on a pgconn connection.
type pgReplConn struct {
	conn *pgconn.PgConn
}

func (c pgReplConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return c.conn.ReceiveMessage(ctx)
}

func (c pgReplConn) SendStatus(ctx context.Context, pos pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: pos})
}

func (c pgReplConn) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

// tableRef names a published table.
type tableRef struct {
	schema string
	table  string
}

// applyItem is one unit of work for the applier, in stream order. The
// applier marks all leaves of each table in markAll dirty, applies changes,
// and then, if end is not zero, makes end the applied position.
type applyItem struct {
	markAll []tableRef
	changes []cdcMsg
	// xid is the transaction of the changes, for the log.
	xid uint32
	// end is the end of the commit record of the transaction that this item
	// completes, or the drain target. Zero for a part of a transaction.
	end pglogrepl.LSN
}

// queueSize is the number of items that the stream can pass to the applier
// before it waits. Each item has at most one batch of changes.
const queueSize = 4

// maxStatusInterval is the longest time between two standby status updates.
const maxStatusInterval = 10 * time.Second

// stream reads a logical replication stream and applies its changes to the
// Merkle trees. One goroutine owns the replication connection: it reads the
// messages, decodes them, sends standby status updates, and passes the
// changes in batches to one applier goroutine through a bounded queue. It
// does no work on the tree tables, so it can send a status update while an
// apply waits, however long the apply takes.
//
// The position in the status updates is the applied position: the end of
// the commit record of the last transaction that the applier has fully
// applied. It is never ahead of the applied changes. On a reconnect the
// stream starts again from this position.
type stream struct {
	// continuous is false for a bounded drain, which stops at target.
	continuous bool
	target     pglogrepl.LSN
	batchSize  int
	// quiet is how long a bounded drain waits for a message before it checks
	// whether it has received everything up to target.
	quiet time.Duration
	// retryDelay is the wait before a new connect after a failed one, in a
	// continuous stream. A bounded drain does not connect again after a
	// failed connect.
	retryDelay time.Duration
	esc        *escalator

	// connect opens a replication connection, starts the stream from start
	// and returns the interval of the status updates for it.
	connect func(ctx context.Context, start pglogrepl.LSN) (replConn, time.Duration, error)
	// loadKey returns the primary key of the table of a relation message.
	loadKey func(rel *pglogrepl.RelationMessage) (*tableKey, error)
	// apply does the work of one item. It runs in the applier goroutine.
	apply func(item applyItem) error

	// State of the reading goroutine.
	conn       replConn
	connected  bool
	interval   time.Duration
	nextStatus time.Time
	relations  map[uint32]*pglogrepl.RelationMessage
	// tableKeys holds the primary key of each relation in relations. It is
	// read when the relation message comes, before any change of the
	// relation, so a table whose replica identity is not its primary key
	// stops the stream also on a change that needs no key, such as a
	// TRUNCATE. pgoutput sends a new relation message after a change of the
	// table, so the key is read again for it.
	tableKeys map[uint32]*tableKey
	// batch holds the changes of the current transaction that are not passed
	// to the applier yet. A batch passed to the applier is not used again.
	batch []cdcMsg
	xid   uint32
	// inTx is true between a begin message and its commit message. A bounded
	// drain does not stop in this window: the rest of the transaction is
	// not received yet.
	inTx bool
	// received is the end of the commit record of the last transaction
	// passed to the applier, or the position where the stream started.
	received pglogrepl.LSN
	// sent is the number of items passed to the applier.
	sent uint64

	// Shared with the applier.
	queue    chan applyItem
	applied  atomic.Uint64
	done     atomic.Uint64
	progress chan struct{}
	failed   chan struct{}
	failErr  error
	stop     atomic.Bool
	// recvCtx is the parent of each receive. The applier cancels it when it
	// fails, so the stream does not wait for the next message to see the
	// error.
	recvCtx    context.Context
	cancelRecv context.CancelFunc
}

// appliedLSN returns the applied position.
func (s *stream) appliedLSN() pglogrepl.LSN {
	return pglogrepl.LSN(s.applied.Load())
}

// run streams the changes from start and applies them until the stream
// ends: a bounded drain ends at its target, a continuous stream when ctx is
// cancelled, and both on an error. It returns true when a bounded drain has
// received and applied everything up to its target. When run returns, the
// applier has stopped and the connection is closed; appliedLSN then gives
// the final applied position.
func (s *stream) run(ctx context.Context, start pglogrepl.LSN) (bool, error) {
	s.relations = make(map[uint32]*pglogrepl.RelationMessage)
	s.tableKeys = make(map[uint32]*tableKey)
	s.applied.Store(uint64(start))
	s.received = start
	s.queue = make(chan applyItem, queueSize)
	s.progress = make(chan struct{}, 1)
	s.failed = make(chan struct{})
	s.recvCtx, s.cancelRecv = context.WithCancel(ctx)
	defer s.cancelRecv()

	applierDone := make(chan struct{})
	go s.runApplier(applierDone)
	defer func() {
		// An apply in progress is not cancelled: the stream waits for it.
		// The items after it are not applied. The status updates never
		// confirmed them, so the server sends them again to the next stream.
		s.stop.Store(true)
		close(s.queue)
		<-applierDone
		if s.conn != nil {
			if err := s.conn.SendStatus(context.WithoutCancel(ctx), s.appliedLSN()); err != nil {
				logger.Debug("standby status update before close failed: %v", err)
			}
			s.dropConn()
		}
	}()
	return s.receive(ctx)
}

// runApplier applies the items of the queue one by one, in stream order.
func (s *stream) runApplier(done chan struct{}) {
	defer close(done)
	for item := range s.queue {
		if s.stop.Load() {
			return
		}
		if err := s.apply(item); err != nil {
			s.failErr = err
			close(s.failed)
			s.cancelRecv()
			return
		}
		if item.end > s.appliedLSN() {
			s.applied.Store(uint64(item.end))
		}
		s.done.Add(1)
		select {
		case s.progress <- struct{}{}:
		default:
		}
	}
}

// statusTimer returns a timer that fires when the next status update is
// due, or nil when there is no connection.
func (s *stream) statusTimer() *time.Timer {
	if s.conn == nil {
		return nil
	}
	return time.NewTimer(time.Until(s.nextStatus))
}

// timerC returns the channel of t, or nil for a nil t. A receive from a nil
// channel waits forever.
func timerC(t *time.Timer) <-chan time.Time {
	if t == nil {
		return nil
	}
	return t.C
}

// sendStatus sends a standby status update with the applied position. On an
// error it closes the connection, and the stream connects again.
func (s *stream) sendStatus(ctx context.Context) {
	pos := s.appliedLSN()
	if err := s.conn.SendStatus(ctx, pos); err != nil {
		logger.Error("standby status update failed: %v", err)
		s.dropConn()
		return
	}
	logger.Debug("Sent standby status update with LSN %s", pos)
	s.nextStatus = time.Now().Add(s.interval)
}

// dropConn closes the connection.
func (s *stream) dropConn() {
	s.conn.Close(context.Background())
	s.conn = nil
}

// enqueue passes item to the applier. While the queue is full it keeps
// sending status updates.
func (s *stream) enqueue(ctx context.Context, item applyItem) error {
	for {
		t := s.statusTimer()
		select {
		case s.queue <- item:
			if t != nil {
				t.Stop()
			}
			s.sent++
			return nil
		case <-s.failed:
			return s.failErr
		case <-ctx.Done():
			return ctx.Err()
		case <-timerC(t):
			s.sendStatus(ctx)
		}
	}
}

// waitApplied waits until the applier has done every item passed to it.
// While it waits it keeps sending status updates.
func (s *stream) waitApplied(ctx context.Context) error {
	for s.done.Load() < s.sent {
		t := s.statusTimer()
		select {
		case <-s.progress:
		case <-s.failed:
			return s.failErr
		case <-ctx.Done():
			return ctx.Err()
		case <-timerC(t):
			s.sendStatus(ctx)
		}
		if t != nil {
			t.Stop()
		}
	}
	return nil
}

// reconnect opens a new connection. It first waits until the applier has
// done all items in the queue: a complete transaction in the queue would
// come again from the server and be applied twice. The part of the current
// transaction that is already passed to the applier comes again as well and
// is applied again, which adds its inserts and deletes to the counters once
// more. Leaf hashes are computed again from the table, so no change is lost.
func (s *stream) reconnect(ctx context.Context) error {
	if err := s.waitApplied(ctx); err != nil {
		return err
	}
	s.batch = nil
	s.inTx = false
	start := s.appliedLSN()
	if s.connected {
		logger.Info("No connection, trying to reconnect from LSN %s", start)
	}
	conn, interval, err := s.connect(ctx, start)
	if err != nil {
		if !s.continuous {
			return err
		}
		logger.Error("reconnect failed: %v", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.retryDelay):
		}
		return nil
	}
	s.conn = conn
	s.connected = true
	s.interval = interval
	s.nextStatus = time.Now().Add(interval)
	s.received = start
	return nil
}

// receive is the loop of the reading goroutine.
func (s *stream) receive(ctx context.Context) (bool, error) {
	for {
		if s.conn == nil {
			if err := s.reconnect(ctx); err != nil {
				return false, err
			}
			continue
		}
		if !time.Now().Before(s.nextStatus) {
			s.sendStatus(ctx)
			continue
		}

		timeout := time.Until(s.nextStatus)
		if !s.continuous && timeout > s.quiet {
			timeout = s.quiet
		}
		rCtx, cancel := context.WithTimeout(s.recvCtx, timeout)
		msg, err := s.conn.ReceiveMessage(rCtx)
		cancel()
		if err != nil {
			select {
			case <-s.failed:
				return false, s.failErr
			default:
			}
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			if pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded) {
				if !s.continuous && !s.inTx && s.received >= s.target {
					// Received every change up to the target. Otherwise
					// this is a quiet interval while the server decodes a
					// large transaction: proto_version '1' sends nothing
					// until the commit is decoded, so silence does not mean
					// that the stream is drained. The deadline of ctx
					// bounds the wait.
					logger.Info("Replication stream drained to target LSN %s.", s.target)
					return true, s.finish(ctx)
				}
				continue
			}
			logger.Error("ReceiveMessage failed: %v", err)
			s.dropConn()
			continue
		}

		reached, err := s.handle(ctx, msg)
		if err != nil {
			return false, err
		}
		if reached {
			return true, s.finish(ctx)
		}
		select {
		case <-s.failed:
			return false, s.failErr
		default:
		}
	}
}

// finish ends a bounded drain that has received everything up to its
// target: it waits for the applier and confirms the applied position.
func (s *stream) finish(ctx context.Context) error {
	if err := s.waitApplied(ctx); err != nil {
		return err
	}
	if s.conn != nil {
		if err := s.conn.SendStatus(ctx, s.appliedLSN()); err != nil {
			return fmt.Errorf("failed to send final standby status update: %w", err)
		}
	}
	return nil
}

// handle processes one message of the stream. It returns true when a
// bounded drain has received everything up to its target.
func (s *stream) handle(ctx context.Context, msg pgproto3.BackendMessage) (bool, error) {
	switch msg := msg.(type) {
	case *pgproto3.CopyData:
		if len(msg.Data) == 0 {
			return false, nil
		}
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				logger.Error("ParsePrimaryKeepaliveMessage failed: %v", err)
				return false, nil
			}
			logger.Debug("Primary Keepalive Message => ServerWALEnd: %s, ServerTime: %s, ReplyRequested: %t", pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)
			if pkm.ReplyRequested {
				s.sendStatus(ctx)
			}
			// ServerWALEnd of a logical keepalive is the position up to
			// which the walsender has decoded the WAL of the slot. When it
			// reaches the target, every commit up to the target is decoded,
			// and its changes came before this keepalive. So nothing that is
			// not received yet is at or before the target, and the drain is
			// complete. This is the usual case when the tracked tables did
			// not change but other tables did. A keepalive can come between
			// the changes of a transaction and its commit, so the drain does
			// not stop inside a transaction.
			if !s.continuous && !s.inTx && pkm.ServerWALEnd >= s.target {
				logger.Info("Server caught up to drain target LSN %s via keepalive; stopping replication stream", s.target)
				if s.received < s.target {
					// The applied position becomes the target after the
					// items before it are applied.
					if err := s.enqueue(ctx, applyItem{end: s.target}); err != nil {
						return false, err
					}
					s.received = s.target
				}
				return true, nil
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				logger.Error("ParseXLogData failed: %v", err)
				return false, nil
			}
			logicalMsg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				logger.Error("Parse logical replication message failed: %v", err)
				return false, nil
			}
			return s.handleLogical(ctx, logicalMsg)
		}
	case *pgproto3.ErrorResponse:
		logger.Error("replication stream aborted by server: severity=%s code=%s message=%q detail=%q hint=%q where=%q routine=%q",
			msg.Severity, msg.Code, msg.Message, msg.Detail, msg.Hint, msg.Where, msg.Routine)
		return false, fmt.Errorf("server aborted replication: %s (SQLSTATE %s)", msg.Message, msg.Code)
	default:
		logger.Info("Received unexpected message: %T", msg)
	}
	return false, nil
}

// handleLogical processes one pgoutput message.
func (s *stream) handleLogical(ctx context.Context, msg pglogrepl.Message) (bool, error) {
	switch msg := msg.(type) {
	case *pglogrepl.BeginMessage:
		s.xid = msg.Xid
		s.batch = nil
		s.inTx = true
	case *pglogrepl.CommitMessage:
		// The position is the end of the commit record. The server sends
		// again every transaction whose commit record starts at or after the
		// confirmed position, so the start of the commit record would not
		// confirm this transaction.
		item := applyItem{changes: s.batch, xid: s.xid, end: msg.TransactionEndLSN}
		s.batch = nil
		s.inTx = false
		if err := s.enqueue(ctx, item); err != nil {
			return false, err
		}
		s.received = msg.TransactionEndLSN
		if !s.continuous && s.received >= s.target {
			logger.Info("Reached drain target LSN %s; stopping replication stream", s.target)
			return true, nil
		}
		return false, nil
	case *pglogrepl.RelationMessage:
		pkey, err := s.loadKey(msg)
		if err != nil {
			return false, err
		}
		s.relations[msg.RelationID] = msg
		s.tableKeys[msg.RelationID] = pkey
	case *pglogrepl.InsertMessage:
		if err := s.bufferChange(ctx, "INSERT", s.relations[msg.RelationID], msg.Tuple); err != nil {
			return false, err
		}
	case *pglogrepl.UpdateMessage:
		if err := s.bufferChange(ctx, "UPDATE", s.relations[msg.RelationID], msg.NewTuple); err != nil {
			return false, err
		}
	case *pglogrepl.DeleteMessage:
		if err := s.bufferChange(ctx, "DELETE", s.relations[msg.RelationID], msg.OldTuple); err != nil {
			return false, err
		}
	case *pglogrepl.TruncateMessage:
		// pgoutput sends a TRUNCATE as one message with the relation OIDs
		// and no row changes, so the leaves of the rows cannot be found.
		// All leaves of the table are marked dirty instead, and the next
		// tree update hashes the table again from its rows. A cascaded
		// truncate lists every published relation that it truncates.
		// pgoutput sends the relation messages before this message; an
		// unknown relation stops the stream, because its tree would stay
		// stale.
		for _, relID := range msg.RelationIDs {
			rel, ok := s.relations[relID]
			if !ok {
				return false, fmt.Errorf("TRUNCATE references unknown relation OID %d; cannot mark its Merkle tree dirty", relID)
			}
			logger.Info("TRUNCATE decoded for %s.%s: all its blocks will be marked dirty for rehash on next tree update", rel.Namespace, rel.RelationName)
			if err := s.markAll(ctx, rel); err != nil {
				return false, err
			}
		}
	}

	// A large transaction is passed to the applier in batches, so memory
	// stays bounded however large the transaction is. proto_version '1'
	// sends a transaction only after its commit, so a part of it can be
	// applied before its commit message comes.
	if len(s.batch) >= s.batchSize {
		item := applyItem{changes: s.batch, xid: s.xid}
		s.batch = nil
		if err := s.enqueue(ctx, item); err != nil {
			return false, err
		}
	}
	return false, nil
}

// bufferChange adds one decoded change to the current batch.
//
// The adaptive drain applies to UPDATEs only. An update never moves a row to
// another leaf, so marking all leaves dirty covers it, and the stream stops
// tracking the updates of the table one by one. INSERTs and DELETEs are
// always tracked: their counters drive the split and merge of leaves, which
// marking all leaves dirty does not cover.
func (s *stream) bufferChange(ctx context.Context, op string, rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) error {
	if rel == nil {
		return fmt.Errorf("%s of a relation with no relation message", op)
	}
	if s.esc != nil && op == "UPDATE" {
		if s.esc.isEscalated(rel.Namespace, rel.RelationName) {
			return nil
		}
		if s.esc.noteChange(rel.Namespace, rel.RelationName) {
			logger.Info("adaptive drain: %s.%s crossed %d buffered UPDATE changes; all its blocks will be marked dirty and its updates are not tracked for the rest of this drain (inserts/deletes still tracked for block splits/merges)",
				rel.Namespace, rel.RelationName, s.esc.threshold(rel.Namespace, rel.RelationName))
			return s.markAll(ctx, rel)
		}
	}
	pkey := s.tableKeys[rel.RelationID]
	key, err := keyOfChange(rel, pkey, tup)
	if err != nil {
		return fmt.Errorf("%s on %s.%s: %w", op, rel.Namespace, rel.RelationName, err)
	}
	s.batch = append(s.batch, cdcMsg{
		operation: op,
		schema:    rel.Namespace,
		table:     rel.RelationName,
		pkey:      pkey,
		key:       key,
	})
	return nil
}

// markAll passes to the applier the current batch and the work to mark all
// leaves of the table of rel dirty. The UPDATEs of the table are dropped
// from the batch, because all leaves become dirty. Later UPDATEs of the
// table in this drain are not tracked: in the stream they come after the
// mark, and the applier applies the items in stream order.
func (s *stream) markAll(ctx context.Context, rel *pglogrepl.RelationMessage) error {
	s.batch = dropTableUpdates(s.batch, rel.Namespace, rel.RelationName)
	if s.esc != nil {
		s.esc.markEscalated(rel.Namespace, rel.RelationName)
	}
	item := applyItem{
		markAll: []tableRef{{schema: rel.Namespace, table: rel.RelationName}},
		changes: s.batch,
		xid:     s.xid,
	}
	s.batch = nil
	return s.enqueue(ctx, item)
}

// gucDuration matches the output of SHOW for a setting in milliseconds,
// such as "60s", "1min" or "0".
var gucDuration = regexp.MustCompile(`^(\d+)\s*(us|ms|s|min|h|d)?$`)

// parseGUCDuration parses the output of SHOW for a setting whose base unit is
// milliseconds. A number without a unit is in milliseconds.
func parseGUCDuration(text string) (time.Duration, error) {
	m := gucDuration.FindStringSubmatch(text)
	if m == nil {
		return 0, fmt.Errorf("cannot parse %q as a duration", text)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cannot parse %q as a duration: %w", text, err)
	}
	unit := map[string]time.Duration{
		"us":  time.Microsecond,
		"":    time.Millisecond,
		"ms":  time.Millisecond,
		"s":   time.Second,
		"min": time.Minute,
		"h":   time.Hour,
		"d":   24 * time.Hour,
	}[m[2]]
	return time.Duration(n) * unit, nil
}

// statusInterval returns the interval of the standby status updates for a
// server with the given wal_sender_timeout: half of the timeout, and at most
// maxStatusInterval. A timeout of 0 turns the timeout off on the server.
func statusInterval(walSenderTimeout time.Duration) time.Duration {
	if walSenderTimeout <= 0 {
		return maxStatusInterval
	}
	return min(walSenderTimeout/2, maxStatusInterval)
}

// readStatusInterval reads wal_sender_timeout on a replication connection
// before START_REPLICATION and returns the interval of the status updates.
// If the setting cannot be read, it returns maxStatusInterval.
func readStatusInterval(ctx context.Context, conn *pgconn.PgConn) time.Duration {
	results, err := conn.Exec(ctx, "SHOW wal_sender_timeout").ReadAll()
	if err == nil && (len(results) != 1 || len(results[0].Rows) != 1 || len(results[0].Rows[0]) != 1) {
		err = fmt.Errorf("unexpected result of SHOW wal_sender_timeout")
	}
	var timeout time.Duration
	if err == nil {
		timeout, err = parseGUCDuration(string(results[0].Rows[0][0]))
	}
	if err != nil {
		logger.Warn("cannot read wal_sender_timeout, sending standby status updates every %s: %v", maxStatusInterval, err)
		return maxStatusInterval
	}
	return statusInterval(timeout)
}
