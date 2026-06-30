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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/infra/db"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

// Since we're using the native pgoutput plugin, there are some wire protocol
// specifics that need to be used for parsing the messages. pglogrepl helps
// abstract away a lot of the low-level details, but wherever raw codes, flags, OIDs,
// etc. are used, we have provided the necessary context when needed.

type cdcMsg struct {
	operation string
	schema    string
	table     string
	relation  *pglogrepl.RelationMessage
	// pks holds the primary-key value(s) extracted from the change tuple at
	// receive time. The Merkle tree update only needs the PK (to mark the
	// containing leaf block dirty and bump its counters); the rest of the row is
	// recomputed from the live table at hash time. Keeping only the PK -- rather
	// than the whole tuple -- bounds per-change memory to the key, not the row.
	pks []string
}

func ListenForChanges(ctx context.Context, nodeInfo map[string]any) {
	processReplicationStream(ctx, nodeInfo, true)
}

func UpdateFromCDC(ctx context.Context, nodeInfo map[string]any) error {
	return processReplicationStream(ctx, nodeInfo, false)
}

// FlushBatchSize bounds how many un-committed CDC changes a bounded
// (non-continuous) drain accumulates before applying them to the Merkle tree
// and freeing them, rather than holding a whole transaction's worth until its
// commit is decoded (proto_version '1' streams nothing until commit). Flushing
// sub-batches lets a single very large transaction drain with bounded memory.
// Only primary keys are buffered (see cdcMsg.pks), so per-change memory scales
// with the key size, not the row width; this knob mainly trades flush
// round-trips against per-flush array size. The cdc_flush_batch_size config key
// overrides this default. It is a package var so tests can adjust it.
var FlushBatchSize = 10_000

func processReplicationStream(ctx context.Context, nodeInfo map[string]any, continuous bool) error {
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

	targetFlushLSN := pglogrepl.LSN(0)
	if !continuous {
		targetFlushLSN, err = getCurrentWalFlushLSN(ctx, pool)
		if err != nil {
			logger.Error("failed to get current WAL flush LSN: %v", err)
			return fmt.Errorf("failed to get current WAL flush LSN: %w", err)
		}

		if targetFlushLSN == startLSN {
			logger.Info("CDC already up-to-date at LSN %s; exiting", startLSN)
			return nil
		}
	}

	lastLSN := startLSN
	var lastFlushedLSN pglogrepl.LSN = lastLSN
	var lastLSNVal atomic.Uint64
	lastLSNVal.Store(uint64(lastLSN))
	// Bounded-drain (non-continuous) safety state. ACE-190: a bounded drain must
	// actually reach the call-time snapshot (targetFlushLSN) before it may report
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
			return nil, fmt.Errorf("StartReplication failed: %w", err)
		}
		logger.Info("Logical replication started on slot %s from LSN %s", slotName, lastLSN)
		return c, nil
	}

	conn, err = connect()
	if err != nil {
		logger.Error("initial connection failed: %v", err)
	}

	relations := make(map[uint32]*pglogrepl.RelationMessage)
	nextStandbyMessageDeadline := time.Now().Add(10 * time.Second)

	txChanges := make(map[uint32][]cdcMsg)
	var currentXID uint32
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	stopStreaming := false
	var processingErr error
	maxWorkers := int(pool.Config().MaxConns)
	if maxWorkers <= 0 {
		maxWorkers = 4
	}
	workerSem := make(chan struct{}, maxWorkers)

	// typeCache memoizes PK type-name lookups (OID -> typname) for this drain.
	// processChanges runs once per flush, so without this a large transaction
	// would re-query pg_type for the same key type on every sub-batch. Scoped to
	// this stream (one node), so per-node OID differences for custom types can't
	// leak across nodes; a sync.Map because continuous-mode workers run it
	// concurrently.
	var typeCache sync.Map

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
				if err := processChanges(processingCtx, pool, c, mtreeSchema, &typeCache); err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
			}(c)
			return
		}
		if err := processChanges(processingCtx, pool, c, mtreeSchema, &typeCache); err != nil {
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

	var metaWg sync.WaitGroup
	if continuous {
		metaWg.Add(1)
		go func() {
			defer metaWg.Done()
			ticker := time.NewTicker(flushInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					current := pglogrepl.LSN(lastLSNVal.Load())
					if current > lastFlushedLSN {
						if err := flushMetadata(ctx, pool, publication, slotName, current, tables); err != nil {
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
				if lastLSN < targetFlushLSN {
					return fmt.Errorf("CDC drain reached LSN %s of target %s on slot %s but did not "+
						"catch up within cdc_processing_timeout. Progress is durable: re-run "+
						"'ace mtree update' to continue draining, or raise cdc_processing_timeout to "+
						"drain in one pass (required when a single transaction is larger than the "+
						"timeout window): %w", lastLSN, targetFlushLSN, slotName, ctx.Err())
				}
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				if !continuous {
					if lastLSN >= targetFlushLSN {
						// Received every change up to the call-time snapshot.
						logger.Info("Replication stream drained to target LSN %s.", targetFlushLSN)
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
				// targetFlushLSN the decoder has processed every commit at or
				// before the target, and those commits' changes were streamed --
				// and received, in LSN order -- ahead of this keepalive. So the
				// gap between lastLSN and the target holds no further tracked-table
				// changes and the drain is complete. This is the common case where
				// the tracked table did not change but other tables did, so no
				// data message ever advances lastLSN to the target. (Note: this
				// fires only after the decoder reaches the target; while it is
				// still decoding a large in-progress transaction, ServerWALEnd
				// stays below the target and we keep waiting -- see ACE-190.)
				if !continuous && targetFlushLSN != 0 && pkm.ServerWALEnd >= targetFlushLSN {
					logger.Info("Server caught up to target WAL flush LSN %s via keepalive; stopping replication stream", targetFlushLSN)
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
				case *pglogrepl.CommitMessage:
					if changes, ok := txChanges[currentXID]; ok {
						if len(changes) > 0 {
							applyChanges(changes)
							logger.Info("Processed %d changes for XID %d", len(changes), currentXID)
						}
						delete(txChanges, currentXID)
					}
				case *pglogrepl.RelationMessage:
					relations[logicalMsg.RelationID] = logicalMsg
				case *pglogrepl.InsertMessage:
					rel := relations[logicalMsg.RelationID]
					txChanges[currentXID] = append(txChanges[currentXID], cdcMsg{
						operation: "INSERT",
						schema:    rel.Namespace,
						table:     rel.RelationName,
						relation:  rel,
						pks:       getPKs(rel, logicalMsg.Tuple),
					})
				case *pglogrepl.UpdateMessage:
					rel := relations[logicalMsg.RelationID]
					txChanges[currentXID] = append(txChanges[currentXID], cdcMsg{
						operation: "UPDATE",
						schema:    rel.Namespace,
						table:     rel.RelationName,
						relation:  rel,
						pks:       getPKs(rel, logicalMsg.NewTuple),
					})
				case *pglogrepl.DeleteMessage:
					rel := relations[logicalMsg.RelationID]
					txChanges[currentXID] = append(txChanges[currentXID], cdcMsg{
						operation: "DELETE",
						schema:    rel.Namespace,
						table:     rel.RelationName,
						relation:  rel,
						pks:       getPKs(rel, logicalMsg.OldTuple),
					})
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
				if !continuous && targetFlushLSN != 0 && lastLSN >= targetFlushLSN {
					logger.Info("Reached target WAL flush LSN %s; stopping replication stream", targetFlushLSN)
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
			"refusing to report a possibly-stale Merkle tree as current", lastLSN, targetFlushLSN, slotName)
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

func getCurrentWalFlushLSN(ctx context.Context, pool *pgxpool.Pool) (pglogrepl.LSN, error) {
	var lsnStr string
	err := pool.QueryRow(ctx, "SELECT pg_current_wal_flush_lsn()").Scan(&lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch pg_current_wal_flush_lsn: %w", err)
	}
	if lsnStr == "" {
		return 0, fmt.Errorf("pg_current_wal_flush_lsn returned empty string")
	}
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse pg_current_wal_flush_lsn %s: %w", lsnStr, err)
	}
	return lsn, nil
}

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

// A map of PostgreSQL type OIDs that should be quoted in SQL literals.
var quotedOIDs = map[uint32]bool{
	16:   true, // bool
	17:   true, // bytea
	25:   true, // text
	1042: true, // character
	1043: true, // varchar
	1082: true, // date
	1114: true, // timestamp
	1184: true, // timestamptz
	2950: true, // uuid
}

func processChanges(ctx context.Context, pool *pgxpool.Pool, changes []cdcMsg, mtreeSchema string, typeCache *sync.Map) error {
	logger.Debug("processChanges called with %d changes", len(changes))
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction for processing changes: %w", err)
	}
	defer tx.Rollback(ctx)

	tables := make(map[string][]cdcMsg)
	for _, change := range changes {
		key := fmt.Sprintf("%s.%s", change.schema, change.table)
		tables[key] = append(tables[key], change)
	}
	logger.Debug("Processing changes for %d tables", len(tables))

	for _, tableChanges := range tables {
		if len(tableChanges) > 0 {
			firstChange := tableChanges[0]
			schema := firstChange.schema
			table := firstChange.table
			mtreeTable := fmt.Sprintf("%s.ace_mtree_%s_%s", mtreeSchema, schema, table)

			logger.Debug("Processing %d changes for table %s.%s (mtree: %s)", len(tableChanges), schema, table, mtreeTable)

			var inserts, deletes, updates []string
			relation := firstChange.relation
			isComposite := len(getPrimaryKeyColumns(relation)) > 1
			compositeTypeName := fmt.Sprintf("%s.%s_%s_key_type", mtreeSchema, schema, table)
			var pkeyType string
			if !isComposite {
				for _, col := range relation.Columns {
					if col.Flags == 1 {
						if v, ok := typeCache.Load(col.DataType); ok {
							pkeyType = v.(string)
						} else {
							typeName, err := getTypeNameFromOID(ctx, pool, col.DataType)
							if err != nil {
								return fmt.Errorf("failed to get type name for oid %d: %w", col.DataType, err)
							}
							typeCache.Store(col.DataType, typeName)
							pkeyType = typeName
						}
						break
					}
				}
			}

			for _, change := range tableChanges {
				pks := change.pks
				if len(pks) == 0 {
					// No key columns were extractable from the change tuple --
					// pgoutput flags no key column under REPLICA IDENTITY FULL or
					// NOTHING. Without a PK we cannot locate the change's leaf block,
					// so fail loud rather than silently drop it and miss divergence
					// (ACE-190). The operator must give the table a PK-bearing
					// replica identity (DEFAULT, or USING INDEX over the PK).
					return fmt.Errorf("no primary key extracted for a %s on %s.%s; the "+
						"table's REPLICA IDENTITY must expose its primary key (use REPLICA "+
						"IDENTITY DEFAULT or an index covering the PK)", change.operation, schema, table)
				}
				var pkVal string
				if isComposite {
					// Formatting it as a record literal, e.g., "(123, 'abc')"
					// TODO: This is a bit of a hack; need to find a better way
					pkVal = "(" + strings.Join(pks, ",") + ")"
				} else {
					pkVal = pks[0]
				}

				switch change.operation {
				case "INSERT":
					inserts = append(inserts, pkVal)
				case "UPDATE":
					updates = append(updates, pkVal)
				case "DELETE":
					deletes = append(deletes, pkVal)
				}
			}

			logger.Debug("Calling UpdateMtreeCounters: inserts=%d, updates=%d, deletes=%d", len(inserts), len(updates), len(deletes))
			err := queries.UpdateMtreeCounters(ctx, tx, mtreeTable, isComposite, compositeTypeName, pkeyType, inserts, deletes, updates)
			if err != nil {
				return fmt.Errorf("failed to update mtree counters for %s: %w", mtreeTable, err)
			}
			logger.Debug("Successfully updated mtree counters for %s", mtreeTable)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit CDC changes: %w", err)
	}
	logger.Debug("Successfully committed CDC changes")
	return nil
}

func getPKs(rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) []string {
	var pks []string
	if tup == nil || rel == nil {
		return pks
	}

	for i := range rel.Columns {
		if rel.Columns[i].Flags == 1 { // Primary key
			pks = append(pks, tupleValueToString(tup.Columns[i], rel.Columns[i]))
		}
	}
	return pks
}

func getPrimaryKeyColumns(rel *pglogrepl.RelationMessage) []string {
	var pks []string
	if rel == nil {
		return pks
	}

	for _, col := range rel.Columns {
		if col.Flags == 1 { // Primary key
			pks = append(pks, col.Name)
		}
	}
	return pks
}

func tupleValueToString(tupCol *pglogrepl.TupleDataColumn, relCol *pglogrepl.RelationMessageColumn) string {
	switch tupCol.DataType {
	case 'n': // null
		return "NULL"
	case 'u': // unchanged toast
		logger.Warn("unchanged toast value for PK column %s", relCol.Name)
		return ""
	case 't': // text formatted value
		return string(tupCol.Data)
	default:
		return string(tupCol.Data)
	}
}

func getTypeNameFromOID(ctx context.Context, pool *pgxpool.Pool, oid uint32) (string, error) {
	var typeName string
	err := pool.QueryRow(ctx, "SELECT typname FROM pg_type WHERE oid = $1", oid).Scan(&typeName) // nosemgrep
	if err != nil {
		return "", err
	}
	return typeName, nil
}
