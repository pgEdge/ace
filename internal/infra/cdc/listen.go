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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/db"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

// ErrSlotBusy signals that a bounded CDC drain could not run because the
// node's logical replication slot is already held by another active consumer
// (typically a running `mtree listen`). Bounded callers (UpdateMtree) treat
// this as a per-node skip rather than a fatal error; the continuous listener
// never returns it as terminal.
var ErrSlotBusy = errors.New("replication slot is held by another consumer")

// isSlotBusyErr reports whether err wraps a PostgreSQL "object in use" error
// (SQLSTATE 55006), which is what StartReplication returns when the slot is
// already active for another PID.
func isSlotBusyErr(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55006"
}

// Since we're using the native pgoutput plugin, there are some wire protocol
// specifics that need to be used for parsing the messages. pglogrepl helps
// abstract away a lot of the low-level details, but wherever raw codes, flags, OIDs,
// etc. are used, we have provided the necessary context when needed.

// cdcMsg is one decoded CDC change (INSERT/UPDATE/DELETE) buffered during a
// drain, carrying only what a Merkle-tree update needs to locate the change.
type cdcMsg struct {
	operation string
	schema    string
	table     string
	// pkey is the primary key of the table at the time of the change.
	pkey *tableKey
	// key holds the text of each primary key column of the changed row, as
	// pgoutput sends it, in the order of the primary key. The Merkle tree
	// update only needs the key (to mark the containing leaf block dirty and
	// bump its counters); the rest of the row is recomputed from the live
	// table at hash time. Keeping only the key -- rather than the whole tuple
	// -- bounds per-change memory to the key, not the row.
	key []string
}

// tableKey is the primary key of a published table and the way to take it
// from a change.
type tableKey struct {
	// pos is the position of each primary key column, in the order of the
	// primary key, among the columns of the relation message.
	pos []int
	// castTypes is the cast type of each primary key column, in the order of
	// the primary key.
	castTypes []string
}

// ListenForChanges runs a continuous (unbounded) CDC drain for a node, applying
// changes to its Merkle tree until ctx is cancelled. It is the long-running
// counterpart to UpdateFromCDC. An error stops the drain of this node and is
// logged; the caller gets no error, so the log is the only report of it. The
// error of a cancelled ctx is the normal stop and is not logged.
func ListenForChanges(ctx context.Context, nodeInfo map[string]any) {
	if err := processReplicationStream(ctx, nodeInfo, true); err != nil && ctx.Err() == nil {
		logger.Error("CDC listener on node %v stopped: %v", nodeInfo["Name"], err)
	}
}

// UpdateFromCDC runs one bounded CDC drain for a node: it catches the node's
// Merkle tree up to a WAL position that covers every commit visible at call
// time (see getDrainTargetLSN), then returns. It returns an error rather than
// reporting success if it cannot reach that snapshot, so a stale tree is never
// silently treated as current (ACE-190).
func UpdateFromCDC(ctx context.Context, nodeInfo map[string]any) error {
	return processReplicationStream(ctx, nodeInfo, false)
}

// FlushBatchSize bounds how many un-committed CDC changes a bounded
// (non-continuous) drain accumulates before applying them to the Merkle tree
// and freeing them, rather than holding a whole transaction's worth until its
// commit is decoded (proto_version '1' streams nothing until commit). Flushing
// sub-batches lets a single very large transaction drain with bounded memory.
// Only primary keys are buffered, so per-change memory scales with the key
// size, not the row width; this knob mainly trades flush round-trips against
// per-flush array size. The cdc_flush_batch_size config key overrides this
// default. It is a package var so tests can adjust it.
var FlushBatchSize = 10_000

// processReplicationStream drains a node's logical-replication slot and applies
// the decoded changes to its Merkle tree. With continuous=true it streams
// indefinitely, dispatching apply work to worker goroutines. With
// continuous=false it performs a bounded catch-up: it must reach the call-time
// snapshot (drainTargetLSN) before reporting success, applies changes
// synchronously, and flushes them in memory-bounded sub-batches so a single
// large transaction drains without buffering the whole transaction.
func processReplicationStream(ctx context.Context, nodeInfo map[string]any, continuous bool) error { //nolint:gocyclo // CDC drain protocol state machine; interleaved keepalive/XLogData branching is inherent and splitting it would obscure the LSN/stop bookkeeping
	pool, err := auth.GetClusterNodeConnection(ctx, nodeInfo, auth.ConnectionOptions{})
	if err != nil {
		logger.Error("failed to get connection pool: %v", err)
		return fmt.Errorf("failed to get connection pool: %w", err)
	}
	defer pool.Close()
	processingCtx := context.WithoutCancel(ctx)

	mtreeCfg := config.Get().MTree
	cfg := mtreeCfg.CDC
	mtreeSchema := mtreeCfg.Schema
	publication := cfg.PublicationName
	slotName := cfg.SlotName
	var startLSNStr string
	var pubCommitLSNStr string
	var tables []string
	func() {
		tx, err := pool.Begin(ctx)
		if err != nil {
			logger.Error("failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		metaSlot, metaLSN, metaTables, metaPubCommit, err := queries.GetCDCMetadata(ctx, tx, publication)
		if err != nil {
			logger.Error("failed to get cdc metadata: %v", err)
			startLSNStr = ""
			return
		}
		startLSNStr = metaLSN
		pubCommitLSNStr = metaPubCommit
		tables = metaTables
		if metaSlot != "" {
			slotName = metaSlot
		}
	}()

	if startLSNStr == "" {
		logger.Error("Could not retrieve LSN. Aborting CDC processing for this node.")
		return fmt.Errorf("Could not retrieve LSN. Aborting CDC processing for this node.")
	}

	startLSN, err := pglogrepl.ParseLSN(startLSNStr)
	if err != nil {
		logger.Error("failed to parse lsn: %v", err)
		return fmt.Errorf("failed to parse lsn: %v", err)
	}

	// Publication-commit guard. If metadata records the LSN at which the
	// publication was committed during MtreeInit, refuse to start a stream
	// from any LSN strictly older than that — pgoutput's historical
	// catalog snapshot would not yet contain the publication and the
	// stream would abort with "publication does not exist" (SQLSTATE
	// 42704).
	//
	// Empty pubCommitLSNStr means the metadata row was written by a
	// version of ACE that pre-dates this fix; we cannot check the
	// invariant and fall through with a warning.
	if pubCommitLSNStr == "" {
		logger.Warn("ace_cdc_metadata has no pub_commit_lsn for publication %s; cannot verify slot/publication ordering. If you see SQLSTATE 42704, run 'ace mtree teardown', 'ace mtree init', and 'ace mtree build' to recover", publication)
	} else {
		pubCommitLSN, err := pglogrepl.ParseLSN(pubCommitLSNStr)
		if err != nil {
			logger.Error("failed to parse pub_commit_lsn %s: %v", pubCommitLSNStr, err)
			return fmt.Errorf("failed to parse pub_commit_lsn %s: %w", pubCommitLSNStr, err)
		}
		if startLSN < pubCommitLSN {
			return fmt.Errorf(
				"ace_cdc_metadata.start_lsn (%s) is older than publication commit LSN (%s) "+
					"for slot %s; this typically indicates ace_cdc_metadata was replicated "+
					"cross-node by Spock, or the slot/metadata is from a prior init. "+
					"Run 'ace mtree teardown', 'ace mtree init', and 'ace mtree build' to recover",
				startLSN, pubCommitLSN, slotName,
			)
		}
	}

	drainTargetLSN := pglogrepl.LSN(0)
	if !continuous {
		drainTargetLSN, err = getDrainTargetLSN(ctx, pool)
		if err != nil {
			logger.Error("failed to get drain target LSN: %v", err)
			return fmt.Errorf("failed to get drain target LSN: %w", err)
		}

		if drainTargetLSN == startLSN {
			logger.Info("CDC already up-to-date at LSN %s; exiting", startLSN)
			return nil
		}
	}

	lastLSN := startLSN
	var lastFlushedLSN pglogrepl.LSN = lastLSN
	var lastLSNVal atomic.Uint64
	lastLSNVal.Store(uint64(lastLSN))
	// Bounded-drain (non-continuous) safety state. ACE-190: a bounded drain must
	// actually reach the call-time snapshot (drainTargetLSN) before it may report
	// success; otherwise UpdateMtree/DiffMtree would compare a stale tree and
	// silently miss real divergence. reachedTarget records whether we got there.
	reachedTarget := false
	flushInterval := 10 * time.Second
	if cfg.CDCMetadataFlushSec > 0 {
		flushInterval = time.Duration(cfg.CDCMetadataFlushSec) * time.Second
	}
	flushBatchSize := FlushBatchSize
	if cfg.CDCFlushBatchSize > 0 {
		flushBatchSize = cfg.CDCFlushBatchSize
	}
	// Adaptive drain (bounded mode only): once a table's decoded UPDATE count
	// crosses max(adaptive_drain_min_changes, adaptive_drain_fraction * rows),
	// per-update dirty-marking for it is replaced by one MarkAllLeavesDirty +
	// bulk rehash on the next tree update. Inserts/deletes are exempt: their
	// per-block counters drive split/merge maintenance, so they are always
	// tracked individually. Measured crossover: per-PK applies cost
	// ~60us/change while a full-tree rehash costs ~a tree build, so the
	// break-even sits near 1% of table rows. Escalation can only over-mark
	// dirty blocks (leaf hashes are recomputed from live data), never miss a
	// change. A negative fraction disables escalation.
	var esc *escalator
	if !continuous && cfg.AdaptiveDrainFraction >= 0 {
		fraction := cfg.AdaptiveDrainFraction
		if fraction == 0 {
			fraction = 0.01
		}
		minChanges := int64(cfg.AdaptiveDrainMinChanges)
		if minChanges <= 0 {
			minChanges = 1000
		}
		esc = newEscalator(minChanges, fraction, func(schema, table string) int64 {
			rows, err := queries.GetRowCountEstimateFromMetadata(ctx, pool, schema, table)
			if err != nil {
				logger.Warn("adaptive drain: no row estimate for %s.%s (using min-changes floor): %v", schema, table, err)
				return 0
			}
			return rows
		})
	}
	lastFlushTime := time.Now()
	var conn *pgconn.PgConn

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		},
	}

	connect := func() (*pgconn.PgConn, error) {
		c, err := auth.GetReplModeConnection(nodeInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to get replication connection: %w", err)
		}

		if err := pglogrepl.StartReplication(ctx, c, slotName, lastLSN, opts); err != nil {
			c.Close(context.Background())
			// If listen acquired the slot in the race window after the up-front
			// check, surface it as ErrSlotBusy so a bounded caller skips rather
			// than fails. (Continuous mode just retries via its reconnect loop.)
			if isSlotBusyErr(err) {
				return nil, fmt.Errorf("%w: %v", ErrSlotBusy, err)
			}
			return nil, fmt.Errorf("StartReplication failed: %w", err)
		}
		logger.Info("Logical replication started on slot %s from LSN %s", slotName, lastLSN)
		return c, nil
	}

	// Best-effort cooperation with a running `mtree listen`: a bounded drain
	// cannot attach to a slot that listen already holds (PostgreSQL allows one
	// active consumer per slot). Detect that up front and return ErrSlotBusy so
	// the caller can skip this node's drain instead of failing. Continuous mode
	// is the legitimate slot owner and is exempt.
	if !continuous {
		if pid, perr := queries.GetActiveSlotPID(ctx, pool, slotName); perr != nil {
			return fmt.Errorf("failed to check replication slot status: %w", perr)
		} else if pid != nil {
			return fmt.Errorf("%w (slot %s, pid %d)", ErrSlotBusy, slotName, *pid)
		}
	}

	conn, err = connect()
	if err != nil {
		logger.Error("initial connection failed: %v", err)
	}

	relations := make(map[uint32]*pglogrepl.RelationMessage)
	// tableKeys holds the primary key of each relation in relations. It is
	// read from the catalog when the relation message comes, before any
	// change of the relation, so a table whose replica identity is not its
	// primary key stops the stream also on a change that needs no key, such
	// as a TRUNCATE or an UPDATE after the adaptive drain marks all leaves.
	// pgoutput sends a new relation message after a change of the table, so
	// the key is read again for it.
	tableKeys := make(map[uint32]*tableKey)
	nextStandbyMessageDeadline := time.Now().Add(10 * time.Second)

	txChanges := make(map[uint32][]cdcMsg)
	var currentXID uint32
	// inTx is true between a BeginMessage and its CommitMessage. A bounded
	// drain must never stop inside that window: the transaction's changes sit
	// unapplied in txChanges, and stopping would discard them while the final
	// standby update may confirm past the commit -- permanently skipping the
	// transaction and leaving the Merkle tree stale.
	inTx := false

	// markTableTruncated reacts to a decoded TRUNCATE for one relation.
	// pgoutput publishes a truncate as a single message carrying only relation
	// OIDs -- no per-row DELETEs -- so per-PK tracking is impossible and the
	// only sound reaction is the adaptive drain's bulk path: mark every leaf
	// dirty so the next tree update rehashes the table from live (now empty)
	// data. Dropping the message instead would leave the tree at its
	// pre-truncate state and table-diff would report a truncated node as in
	// sync. Buffered UPDATEs for the table are purged as at escalation time
	// (redundant once every leaf is dirty); INSERT/DELETE tracking continues
	// for split/merge counter maintenance. Block merges from the truncate
	// itself lag until the next build, which costs rehash work, never
	// correctness. A failure to mark is terminal for the same reason as in
	// bufferChange: proceeding would silently miss the divergence.
	markTableTruncated := func(rel *pglogrepl.RelationMessage) error {
		// Keepalive before the blocking write, like the escalation path, so a
		// slow mark-all cannot trip wal_sender_timeout.
		if conn != nil {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
				logger.Warn("standby status update before truncate mark-all failed: %v", err)
			} else {
				nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
			}
		}
		// Sanitized: the mtree table is created quoted/case-preserved.
		mtreeTable := pgx.Identifier{mtreeSchema, fmt.Sprintf("ace_mtree_%s_%s", rel.Namespace, rel.RelationName)}.Sanitize()
		// processingCtx so the write survives the drain deadline, like every
		// other apply here.
		marked, err := queries.MarkAllLeavesDirty(processingCtx, pool, mtreeTable)
		if err != nil {
			return fmt.Errorf("failed to mark all leaves dirty for truncated table %s.%s: %w", rel.Namespace, rel.RelationName, err)
		}
		purgeTableChanges(txChanges, rel.Namespace, rel.RelationName)
		// All leaves are now dirty, which is the escalated state, so record it:
		// later UPDATEs on a re-populated table skip per-PK tracking rather than
		// re-running the threshold count and a redundant mark-all.
		if esc != nil {
			esc.markEscalated(rel.Namespace, rel.RelationName)
		}
		logger.Info("TRUNCATE decoded for %s.%s: marked all %d blocks dirty for rehash on next tree update", rel.Namespace, rel.RelationName, marked)
		return nil
	}

	// bufferChange records one decoded change for the bounded drain.
	// Escalation applies to UPDATEs ONLY: an update never changes a row's
	// block membership, so mark-all-dirty fully covers its effect on leaf
	// hashes, and skipping its per-PK tracking is pure savings. INSERTs and
	// DELETEs are always tracked per-PK regardless of escalation -- their
	// per-block counters drive block split/merge maintenance, and discarding
	// them would leave the tree structurally unbalanced until a full rebuild
	// (regression: the MerkleTree Split* suites fail if inserts are dropped).
	// Returns a terminal error only if the escalation's mark-all-dirty write
	// fails: proceeding without either per-PK marks or the bulk mark would
	// silently miss divergence.
	bufferChange := func(op string, rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) error {
		if rel == nil {
			return fmt.Errorf("%s of a relation with no relation message", op)
		}
		pkey := tableKeys[rel.RelationID]
		if esc != nil && op == "UPDATE" {
			if esc.isEscalated(rel.Namespace, rel.RelationName) {
				return nil
			}
			if esc.noteChange(rel.Namespace, rel.RelationName) {
				// Keepalive before the blocking write, like the sub-batch
				// flush below, so a slow mark-all cannot trip wal_sender_timeout.
				if conn != nil {
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
						logger.Warn("standby status update before adaptive-drain escalation failed: %v", err)
					} else {
						nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
					}
				}
				// Sanitized: the mtree table is created quoted/case-preserved.
				mtreeTable := pgx.Identifier{mtreeSchema, fmt.Sprintf("ace_mtree_%s_%s", rel.Namespace, rel.RelationName)}.Sanitize()
				// processingCtx so the write survives the drain deadline,
				// like every other apply here.
				marked, err := queries.MarkAllLeavesDirty(processingCtx, pool, mtreeTable)
				if err != nil {
					return fmt.Errorf("adaptive drain: failed to mark all leaves dirty for %s: %w", mtreeTable, err)
				}
				esc.markEscalated(rel.Namespace, rel.RelationName)
				purgeTableChanges(txChanges, rel.Namespace, rel.RelationName)
				logger.Info("adaptive drain: %s.%s crossed %d buffered UPDATE changes; marked all %d blocks dirty and skipping its per-update tracking for the rest of this drain (inserts/deletes still tracked for block splits/merges)",
					rel.Namespace, rel.RelationName, esc.threshold(rel.Namespace, rel.RelationName), marked)
				return nil
			}
		}
		key, err := keyOfChange(rel, pkey, tup)
		if err != nil {
			return fmt.Errorf("%s on %s.%s: %w", op, rel.Namespace, rel.RelationName, err)
		}
		txChanges[currentXID] = append(txChanges[currentXID], cdcMsg{
			operation: op,
			schema:    rel.Namespace,
			table:     rel.RelationName,
			pkey:      pkey,
			key:       key,
		})
		return nil
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	stopStreaming := false
	var processingErr error
	maxWorkers := int(pool.Config().MaxConns)
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	workerSem := make(chan struct{}, maxWorkers)

	// applyChanges applies a batch of buffered changes to the Merkle tree. In
	// continuous mode it dispatches to a worker goroutine for throughput. In a
	// bounded drain it applies synchronously, so lastLSN -- which drives the
	// slot's confirmed_flush_lsn (via the standby status updates) and the
	// metadata checkpoint -- never advances ahead of durably-applied work. That
	// ordering is what makes a crash safe: the slot is only ever confirmed past
	// a transaction once that transaction has been applied, so a crash mid-drain
	// causes the server to resend the in-flight transaction and we recover it on
	// the next run (see flushActiveTx for the counter-drift caveat).
	applyChanges := func(c []cdcMsg) {
		if len(c) == 0 {
			return
		}
		if continuous {
			wg.Add(1)
			go func(c []cdcMsg) {
				defer wg.Done()
				workerSem <- struct{}{}
				defer func() { <-workerSem }()
				if err := processChanges(processingCtx, pool, c, mtreeSchema); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}(c)
			return
		}
		if err := processChanges(processingCtx, pool, c, mtreeSchema); err != nil {
			processingErr = err
			stopStreaming = true
		}
	}

	// flushActiveTx applies and frees the changes buffered so far for the
	// in-flight transaction without waiting for its commit, bounding memory for
	// a single very large transaction (proto_version '1' streams a whole
	// transaction at once). Bounded drain only. It is safe because under v1 the
	// upstream transaction has already committed before the walsender streams
	// any of it, so there is no in-progress transaction that could still abort.
	// Caveat: on a crash the server resends the whole transaction and we
	// re-apply these already-flushed sub-batches, which over-counts the
	// per-block insert/delete counters. That drift only affects block-split
	// heuristics, never a leaf hash (recomputed from the live table), and resets
	// on the next tree update -- a change is never missed.
	flushActiveTx := func() {
		c := txChanges[currentXID]
		if len(c) == 0 {
			return
		}
		applyChanges(c)
		txChanges[currentXID] = c[:0]
	}

	// The metadata flusher and its write in progress stop when the receive
	// loop ends, not only when ctx is cancelled: on an error the loop ends
	// while ctx is still alive, and the wait for the flusher must not block
	// the return of the error.
	metaCtx, stopMeta := context.WithCancel(ctx)
	defer stopMeta()
	var metaWg sync.WaitGroup
	if continuous {
		metaWg.Add(1)
		go func() {
			defer metaWg.Done()
			ticker := time.NewTicker(flushInterval)
			defer ticker.Stop()
			for {
				select {
				case <-metaCtx.Done():
					return
				case <-ticker.C:
					current := pglogrepl.LSN(lastLSNVal.Load())
					if current > lastFlushedLSN {
						if err := flushMetadata(metaCtx, pool, publication, slotName, current, tables); err != nil {
							logger.Warn("failed to periodically flush CDC metadata: %v", err)
						} else {
							lastFlushedLSN = current
							lastFlushTime = time.Now()
						}
					}
				}
			}
		}()
	}

	for {
		if stopStreaming {
			break
		}
		if err := ctx.Err(); err != nil {
			logger.Info("CDC listener stopping due to context error: %v", err)
			if conn != nil {
				conn.Close(ctx)
			}
			return err
		}
		if conn == nil {
			logger.Info("No connection, trying to reconnect...")
			conn, err = connect()
			if err != nil {
				logger.Error("reconnect failed: %v", err)
				if !continuous {
					return err
				}
				time.Sleep(5 * time.Second)
			}
			nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)

			if !continuous {
				if conn == nil {
					return nil
				}
			}
			continue
		}

		if time.Now().After(nextStandbyMessageDeadline) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
				conn.Close(ctx)
				conn = nil
				logger.Error("SendStandbyStatusUpdate failed: %v", err)
				continue
			}
			logger.Debug("Sent Standby status update with LSN %s", lastLSN)
			nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
		}

		var timeout time.Duration
		if continuous {
			timeout = time.Until(nextStandbyMessageDeadline)
			if timeout < 0 {
				timeout = 0
			}
		} else {
			timeout = 1 * time.Second
		}

		rCtx, cancel := context.WithTimeout(ctx, timeout)
		msg, err := conn.ReceiveMessage(rCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				conn.Close(ctx)
				if continuous {
					logger.Info("Replication stopping due to context cancellation")
					return nil
				}
				if lastLSN < drainTargetLSN {
					return fmt.Errorf("CDC drain reached LSN %s of target %s on slot %s but did not "+
						"catch up within cdc_processing_timeout. Progress is durable: re-run "+
						"'ace mtree update' to continue draining, or raise cdc_processing_timeout to "+
						"drain in one pass (required when a single transaction is larger than the "+
						"timeout window): %w", lastLSN, drainTargetLSN, slotName, ctx.Err())
				}
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				if !continuous {
					if lastLSN >= drainTargetLSN && !inTx {
						// Received every change up to the call-time snapshot.
						logger.Info("Replication stream drained to target LSN %s.", drainTargetLSN)
						reachedTarget = true
						stopStreaming = true
					}
					// Otherwise this is a quiet interval while the server decodes
					// a large transaction: proto_version '1' streams nothing until
					// the commit is decoded, so silence here does NOT mean the
					// stream is drained (ACE-190). Keep waiting; the parent
					// context's cdc_processing_timeout bounds the wait and turns
					// an unreachable target into the error above.
				}
				continue
			}
			conn.Close(ctx)
			conn = nil
			logger.Error("ReceiveMessage failed: %v", err)
			continue
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					logger.Error("ParsePrimaryKeepaliveMessage failed: %v", err)
					continue
				}
				logger.Debug("Primary Keepalive Message => ServerWALEnd: %s, ServerTime: %s, ReplyRequested: %t", pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)

				if pkm.ReplyRequested {
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
						conn.Close(ctx)
						conn = nil
						logger.Error("SendStandbyStatusUpdate failed on keepalive: %v", err)
					} else {
						logger.Debug("Sent Standby status update with LSN %s", lastLSN)
						nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
					}
				}

				// ServerWALEnd on a logical keepalive is the walsender's decode
				// position (how far it has read this slot's WAL). When it reaches
				// drainTargetLSN the decoder has processed every commit at or
				// before the target, and those commits' changes were streamed --
				// and received, in LSN order -- ahead of this keepalive. So the
				// gap between lastLSN and the target holds no further tracked-table
				// changes and the drain is complete. This is the common case where
				// the tracked table did not change but other tables did, so no
				// data message ever advances lastLSN to the target. (Note: this
				// fires only after the decoder reaches the target; while it is
				// still decoding a large in-progress transaction, ServerWALEnd
				// stays below the target and we keep waiting -- see ACE-190.)
				// The !inTx guard: a keepalive can interleave between a
				// transaction's data messages and its commit message. Stopping
				// there would discard the buffered changes yet checkpoint at the
				// target, permanently skipping the transaction.
				if !continuous && drainTargetLSN != 0 && pkm.ServerWALEnd >= drainTargetLSN && !inTx {
					logger.Info("Server caught up to drain target LSN %s via keepalive; stopping replication stream", drainTargetLSN)
					// Advance the checkpoint to the target. We stopped on a keepalive
					// (not a data message), so lastLSN still sits at the start when no
					// tracked change reached the target; per the reasoning above the
					// gap holds no tracked changes, so it is safe to checkpoint at the
					// target. Without this the final standby status update and metadata
					// write below would persist the stale start LSN, the slot would
					// never advance, and every subsequent drain would re-scan the same
					// span (and the server would retain that WAL indefinitely).
					if lastLSN < drainTargetLSN {
						lastLSN = drainTargetLSN
						lastLSNVal.Store(uint64(lastLSN))
					}
					reachedTarget = true
					stopStreaming = true
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					logger.Error("ParseXLogData failed: %v", err)
					continue
				}

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					logger.Error("Parse logical replication message failed: %v", err)
					continue
				}

				switch logicalMsg := logicalMsg.(type) {
				case *pglogrepl.BeginMessage:
					currentXID = logicalMsg.Xid
					txChanges[currentXID] = []cdcMsg{}
					inTx = true
				case *pglogrepl.CommitMessage:
					if changes, ok := txChanges[currentXID]; ok {
						if len(changes) > 0 {
							applyChanges(changes)
							logger.Info("Processed %d changes for XID %d", len(changes), currentXID)
						}
						delete(txChanges, currentXID)
					}
					inTx = false
				case *pglogrepl.RelationMessage:
					pkey, err := loadTableKey(processingCtx, pool, logicalMsg)
					if err != nil {
						processingErr = err
						stopStreaming = true
						break
					}
					relations[logicalMsg.RelationID] = logicalMsg
					tableKeys[logicalMsg.RelationID] = pkey
				case *pglogrepl.InsertMessage:
					rel := relations[logicalMsg.RelationID]
					if err := bufferChange("INSERT", rel, logicalMsg.Tuple); err != nil {
						processingErr = err
						stopStreaming = true
					}
				case *pglogrepl.UpdateMessage:
					rel := relations[logicalMsg.RelationID]
					if err := bufferChange("UPDATE", rel, logicalMsg.NewTuple); err != nil {
						processingErr = err
						stopStreaming = true
					}
				case *pglogrepl.DeleteMessage:
					rel := relations[logicalMsg.RelationID]
					if err := bufferChange("DELETE", rel, logicalMsg.OldTuple); err != nil {
						processingErr = err
						stopStreaming = true
					}
				case *pglogrepl.TruncateMessage:
					// The publication is created with the default publish list,
					// which includes truncate, so the server does send these. A
					// cascaded truncate lists every affected published relation
					// here. pgoutput sends the RelationMessages ahead of this
					// message, so the lookups should always hit; treat a miss as
					// terminal rather than skipping a table whose tree would then
					// silently go stale.
					for _, relID := range logicalMsg.RelationIDs {
						rel, ok := relations[relID]
						if !ok {
							processingErr = fmt.Errorf("TRUNCATE references unknown relation OID %d; cannot mark its Merkle tree dirty", relID)
							stopStreaming = true
							break
						}
						if err := markTableTruncated(rel); err != nil {
							processingErr = err
							stopStreaming = true
							break
						}
					}
				}

				// Flush sub-batches of the in-flight transaction so memory stays
				// bounded regardless of transaction size. proto_version '1' buffers a
				// whole transaction until its commit is decoded, so without this a
				// multi-million-row transaction would grow unboundedly here.
				if !continuous && len(txChanges[currentXID]) >= flushBatchSize {
					// Refresh the server's keepalive clock before the synchronous
					// apply. processChanges blocks the receive loop while it does DB
					// work; on a busy node a slow flush could otherwise let
					// wal_sender_timeout elapse with no standby update and the server
					// would drop the replication connection. Reporting lastLSN here is
					// the same conservative position the periodic update sends.
					if conn != nil {
						if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
							logger.Warn("standby status update before CDC flush failed: %v", err)
						} else {
							nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)
						}
					}
					flushActiveTx()
				}

				lastLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				lastLSNVal.Store(uint64(lastLSN))
				if continuous && time.Since(lastFlushTime) >= flushInterval {
					if err := flushMetadata(ctx, pool, publication, slotName, lastLSN, tables); err != nil {
						logger.Warn("failed to flush CDC metadata during streaming: %v", err)
					} else {
						lastFlushedLSN = lastLSN
						lastFlushTime = time.Now()
					}
				}
				// The !inTx guard: lastLSN advances on every data message, so it
				// can cross the target between a transaction's changes and its
				// commit. Keep reading until the commit lands -- it was already
				// decoded and sent, so it is at most a few messages away.
				if !continuous && drainTargetLSN != 0 && lastLSN >= drainTargetLSN && !inTx {
					logger.Info("Reached drain target LSN %s; stopping replication stream", drainTargetLSN)
					reachedTarget = true
					stopStreaming = true
				}
			}
		case *pgproto3.ErrorResponse:
			logger.Error("replication stream aborted by server: severity=%s code=%s message=%q detail=%q hint=%q where=%q routine=%q",
				msg.Severity, msg.Code, msg.Message, msg.Detail, msg.Hint, msg.Where, msg.Routine)
			processingErr = fmt.Errorf("server aborted replication: %s (SQLSTATE %s)", msg.Message, msg.Code)
			stopStreaming = true
		default:
			logger.Info("Received unexpected message: %T", msg)
		}

		if !stopStreaming {
			select {
			case err := <-errCh:
				if err != nil {
					logger.Error("Error processing CDC changes: %v", err)
					processingErr = err
					stopStreaming = true
				}
			default:
			}
		}
	}

	wg.Wait()
	stopMeta()
	metaWg.Wait()

	if processingErr == nil {
		select {
		case err := <-errCh:
			processingErr = err
		default:
		}
	}

	// Data-safety backstop (ACE-190): a bounded drain must not report success
	// unless it actually reached the call-time snapshot. Every legitimate
	// completion above sets reachedTarget; if the loop exited any other way
	// without an error, fail loud rather than checkpoint a stale position and
	// let UpdateMtree/DiffMtree compare a tree that never saw the backlog.
	if !continuous && processingErr == nil && !reachedTarget {
		processingErr = fmt.Errorf("CDC drain ended at LSN %s without reaching target %s on slot %s; "+
			"refusing to report a possibly-stale Merkle tree as current", lastLSN, drainTargetLSN, slotName)
	}

	if processingErr != nil {
		if conn != nil {
			conn.Close(ctx)
		}
		return processingErr
	}

	if conn != nil {
		if err := pglogrepl.SendStandbyStatusUpdate(processingCtx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
			conn.Close(ctx)
			return fmt.Errorf("failed to send final standby status update: %w", err)
		}
	}

	if !continuous {
		tx, err := pool.Begin(processingCtx)
		if err != nil {
			if conn != nil {
				conn.Close(ctx)
			}
			return fmt.Errorf("failed to begin metadata update transaction: %w", err)
		}
		defer tx.Rollback(processingCtx)

		if tables == nil {
			tables = []string{}
		}

		if err := queries.UpdateCDCMetadata(processingCtx, tx, publication, slotName, lastLSN.String(), tables); err != nil {
			if conn != nil {
				conn.Close(ctx)
			}
			return fmt.Errorf("failed to update cdc metadata: %w", err)
		}

		if err := tx.Commit(processingCtx); err != nil {
			if conn != nil {
				conn.Close(ctx)
			}
			return fmt.Errorf("failed to commit cdc metadata update: %w", err)
		}
	}

	if conn != nil {
		conn.Close(ctx)
	}

	if continuous && lastLSN > lastFlushedLSN {
		if err := flushMetadata(ctx, pool, publication, slotName, lastLSN, tables); err != nil {
			logger.Warn("failed to flush CDC metadata on shutdown: %v", err)
		} else {
			lastFlushedLSN = lastLSN
		}
	}

	return nil
}

// getDrainTargetLSN returns the snapshot position a bounded drain must reach
// before it is considered caught up. Two requirements shape it:
//
//   - It must cover every commit visible to readers at call time. The flush
//     LSN alone does not: transactions applied by Spock commit asynchronously
//     (synchronous_commit=off), so a row can be visible while its commit
//     record is still ahead of the flush pointer, and a flush-LSN target
//     would let the drain stop before covering it.
//   - It must be reachable by the walsender, whose send pointer is capped by
//     *flushed* WAL. The raw insert LSN is not always reachable: the WAL
//     writer flushes partial pages only up to the last async commit, so an
//     insert-LSN target behind a non-commit tail record could stall the
//     drain until the next checkpoint.
//
// Both are satisfied by nudging a trivial committing transaction and reading
// the insert LSN in the same statement: every previously visible commit sits
// at or below the captured position, and the nudge's own commit record lands
// just above it, guaranteeing the WAL writer flushes past the target within
// one wal_writer_delay in every synchronous_commit configuration.
func getDrainTargetLSN(ctx context.Context, pool *pgxpool.Pool) (pglogrepl.LSN, error) {
	var lsnStr string
	var xid int64
	err := pool.QueryRow(ctx, "SELECT pg_current_wal_insert_lsn(), txid_current()").Scan(&lsnStr, &xid)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch drain target LSN: %w", err)
	}
	if lsnStr == "" {
		return 0, fmt.Errorf("pg_current_wal_insert_lsn returned empty string")
	}
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse drain target LSN %s: %w", lsnStr, err)
	}
	return lsn, nil
}

// flushMetadata persists drain progress (slot, tracked tables, and the
// last-applied LSN) to ace_cdc_metadata in its own transaction.
func flushMetadata(ctx context.Context, pool *pgxpool.Pool, publication, slotName string, lsn pglogrepl.LSN, tables []string) error {
	if tables == nil {
		tables = []string{}
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin metadata update transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := queries.UpdateCDCMetadata(ctx, tx, publication, slotName, lsn.String(), tables); err != nil {
		return fmt.Errorf("failed to update cdc metadata: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit cdc metadata update: %w", err)
	}

	return nil
}

// mtreeOps maps the operation of a change to the operation of the counters of
// the tree.
var mtreeOps = map[string]string{
	"INSERT": queries.MtreeOpInsert,
	"UPDATE": queries.MtreeOpUpdate,
	"DELETE": queries.MtreeOpDelete,
}

// processChanges applies a batch of buffered CDC changes to the Merkle tree in a
// single transaction: it marks each change's leaf block dirty and bumps its
// insert/delete counters.
func processChanges(ctx context.Context, pool *pgxpool.Pool, changes []cdcMsg, mtreeSchema string) error {
	logger.Debug("processChanges called with %d changes", len(changes))
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for processing changes: %w", err)
	}
	defer tx.Rollback(ctx)

	// The changes of a table are grouped by its primary key as well: a change
	// of the table in the middle of a batch can give it another key.
	type group struct {
		schema string
		table  string
		pkey   *tableKey
	}
	var order []group
	groups := make(map[group][]queries.MtreeKeyChange)
	for _, change := range changes {
		op, ok := mtreeOps[change.operation]
		if !ok {
			return fmt.Errorf("unknown operation %q of a change on %s.%s", change.operation, change.schema, change.table)
		}
		g := group{schema: change.schema, table: change.table, pkey: change.pkey}
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], queries.MtreeKeyChange{Op: op, Key: change.key})
	}
	logger.Debug("Processing changes for %d tables", len(order))

	for _, g := range order {
		// Sanitized: the mtree table and its key type are created quoted.
		mtreeTable := pgx.Identifier{mtreeSchema, fmt.Sprintf("ace_mtree_%s_%s", g.schema, g.table)}.Sanitize()
		keyType := pgx.Identifier{mtreeSchema, fmt.Sprintf("%s_%s_key_type", g.schema, g.table)}.Sanitize()
		tableChanges := groups[g]
		logger.Debug("Processing %d changes for table %s.%s (mtree: %s)", len(tableChanges), g.schema, g.table, mtreeTable)
		if err := queries.UpdateMtreeCounters(ctx, tx, mtreeTable, keyType, g.pkey.castTypes, tableChanges); err != nil {
			return fmt.Errorf("failed to update mtree counters for %s: %w", mtreeTable, err)
		}
		logger.Debug("Successfully updated mtree counters for %s", mtreeTable)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit CDC changes: %w", err)
	}
	logger.Debug("Successfully committed CDC changes")
	return nil
}

// loadTableKey reads the primary key of the table of rel and the cast types
// of its columns from the catalog, and finds the key columns among the
// columns of rel.
//
// The replica identity of the table must have the columns of the primary key
// and no others: REPLICA IDENTITY DEFAULT, or USING INDEX on an index with
// the same columns. A DELETE sends only the replica identity columns, so
// without a key column the leaf of the row cannot be found. Other replica
// identities, such as FULL, are not supported. FULL is checked by its mode:
// with FULL every column is an identity column, so on a table that has only
// key columns the check of the columns passes.
func loadTableKey(ctx context.Context, db queries.DBQuerier, rel *pglogrepl.RelationMessage) (*tableKey, error) {
	if rel.ReplicaIdentity == 'f' {
		return nil, fmt.Errorf("the replica identity of %s.%s is FULL, which is not supported; "+
			"use REPLICA IDENTITY DEFAULT, or USING INDEX on an index with exactly the columns of the primary key",
			rel.Namespace, rel.RelationName)
	}
	pk, err := queries.GetPrimaryKey(ctx, db, rel.Namespace, rel.RelationName)
	if err != nil {
		return nil, fmt.Errorf("primary key of %s.%s: %w", rel.Namespace, rel.RelationName, err)
	}
	if len(pk) == 0 {
		return nil, fmt.Errorf("%s.%s has no primary key", rel.Namespace, rel.RelationName)
	}
	castTypes, err := queries.GetCastTypes(ctx, db, rel.Namespace, rel.RelationName)
	if err != nil {
		return nil, fmt.Errorf("cast types of %s.%s: %w", rel.Namespace, rel.RelationName, err)
	}
	return matchTableKey(rel, pk, castTypes)
}

// matchTableKey finds the primary key columns pk among the columns of rel by
// name. pk and castTypes come from the catalog now; castTypes has the cast
// type of every column.
//
// The identity columns of rel must be exactly the columns of pk, or the
// result is an error. rel describes the table at the commit of the change,
// so the check also fails when the names of the key columns changed after
// that commit.
//
// XXX: rel is trusted to describe the same table as the tree. A change of the
// primary key columns that keeps their names, such as a key column dropped
// and added again, is not seen here.
func matchTableKey(rel *pglogrepl.RelationMessage, pk []string, castTypes map[string]string) (*tableKey, error) {
	var identity []string
	for _, col := range rel.Columns {
		if col.Flags&1 != 0 {
			identity = append(identity, col.Name)
		}
	}
	if !sameColumns(identity, pk) {
		return nil, fmt.Errorf("the replica identity of %s.%s has the columns (%s) in the change, but its primary key has the columns (%s); "+
			"use REPLICA IDENTITY DEFAULT, or USING INDEX on an index with exactly the columns of the primary key; "+
			"after a change of the primary key columns, run mtree teardown, mtree init, and mtree build for every table that had a tree",
			rel.Namespace, rel.RelationName, strings.Join(identity, ", "), strings.Join(pk, ", "))
	}

	k := &tableKey{pos: make([]int, len(pk)), castTypes: make([]string, len(pk))}
	for i, name := range pk {
		typ := castTypes[name]
		if typ == "" {
			return nil, fmt.Errorf("no cast type for the primary key column %s of %s.%s", name, rel.Namespace, rel.RelationName)
		}
		k.pos[i] = slices.IndexFunc(rel.Columns, func(c *pglogrepl.RelationMessageColumn) bool { return c.Name == name })
		k.castTypes[i] = typ
	}
	return k, nil
}

// sameColumns reports whether a and b have the same column names, in any
// order. The names of the columns of a table are unique.
func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for _, name := range a {
		if !slices.Contains(b, name) {
			return false
		}
	}
	return true
}

// keyOfChange returns the text of each primary key column of the row of a
// change, in the order of the primary key. A key column must come in the
// text format: a NULL, an unchanged TOAST value or a binary value is an
// error, because the leaf of the row cannot be found for it.
func keyOfChange(rel *pglogrepl.RelationMessage, pkey *tableKey, tup *pglogrepl.TupleData) ([]string, error) {
	if tup == nil {
		return nil, fmt.Errorf("the change has no row")
	}
	key := make([]string, len(pkey.pos))
	for i, j := range pkey.pos {
		if j >= len(tup.Columns) {
			return nil, fmt.Errorf("the row has %d columns, the relation has %d", len(tup.Columns), len(rel.Columns))
		}
		col := tup.Columns[j]
		name := rel.Columns[j].Name
		switch col.DataType {
		case pglogrepl.TupleDataTypeText:
			key[i] = string(col.Data)
		case pglogrepl.TupleDataTypeNull:
			return nil, fmt.Errorf("the primary key column %s is NULL", name)
		case pglogrepl.TupleDataTypeToast:
			return nil, fmt.Errorf("the primary key column %s is an unchanged TOAST value", name)
		default:
			return nil, fmt.Errorf("the primary key column %s has the data type %q, not text", name, col.DataType)
		}
	}
	return key, nil
}
