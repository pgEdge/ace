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
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// FlushBatchSize is the largest number of changes that the stream passes to
// the applier in one batch. A transaction with more changes is applied in
// several batches before its commit message comes, so memory stays bounded
// however large the transaction is (proto_version '1' sends a transaction
// only after its commit). Only primary keys are buffered, so per-change
// memory scales with the key size, not the row width; this knob mainly
// trades apply round-trips against per-apply array size. The
// cdc_flush_batch_size config key overrides this default. It is a package
// var so tests can adjust it.
var FlushBatchSize = 10_000

// processReplicationStream drains a node's logical-replication slot and applies
// the decoded changes to its Merkle tree. With continuous=true it streams
// until ctx is cancelled. With continuous=false it performs a bounded
// catch-up: it must reach the call-time snapshot (drainTargetLSN) and apply
// everything up to it before reporting success. In both modes one goroutine
// reads the stream and one applier applies the changes in stream order.
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

	// A bounded drain must reach the call-time snapshot (drainTargetLSN)
	// before it may report success; otherwise UpdateMtree/DiffMtree would
	// compare a stale tree and silently miss real divergence.
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

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		},
	}

	connect := func(ctx context.Context, start pglogrepl.LSN) (replConn, time.Duration, error) {
		c, err := auth.GetReplModeConnection(nodeInfo)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to get replication connection: %w", err)
		}
		interval := readStatusInterval(ctx, c)

		if err := pglogrepl.StartReplication(ctx, c, slotName, start, opts); err != nil {
			c.Close(context.Background())
			// If listen acquired the slot in the race window after the up-front
			// check, surface it as ErrSlotBusy so a bounded caller skips rather
			// than fails. (Continuous mode just retries via its reconnect loop.)
			if isSlotBusyErr(err) {
				return nil, 0, fmt.Errorf("%w: %v", ErrSlotBusy, err)
			}
			return nil, 0, fmt.Errorf("StartReplication failed: %w", err)
		}
		logger.Info("Logical replication started on slot %s from LSN %s", slotName, start)
		return pgReplConn{conn: c}, interval, nil
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

	s := &stream{
		continuous: continuous,
		target:     drainTargetLSN,
		batchSize:  flushBatchSize,
		quiet:      time.Second,
		retryDelay: 5 * time.Second,
		esc:        esc,
		connect:    connect,
		loadKey: func(rel *pglogrepl.RelationMessage) (*tableKey, error) {
			return loadTableKey(processingCtx, pool, rel)
		},
		// processingCtx so that an apply in progress is not cancelled with
		// ctx: the drain waits for it to finish.
		apply: func(item applyItem) error {
			return applyItemToTrees(processingCtx, pool, mtreeSchema, item)
		},
	}

	// The metadata flusher of a continuous stream writes the applied
	// position. It stops when the stream ends, not only when ctx is
	// cancelled: on an error the stream ends while ctx is still alive, and
	// the wait for the flusher must not block the return of the error.
	lastFlushedLSN := startLSN
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
					current := s.appliedLSN()
					if current > lastFlushedLSN {
						if err := flushMetadata(metaCtx, pool, publication, slotName, current, tables); err != nil {
							logger.Warn("failed to periodically flush CDC metadata: %v", err)
						} else {
							lastFlushedLSN = current
						}
					}
				}
			}
		}()
	}

	reachedTarget, err := s.run(ctx, startLSN)
	stopMeta()
	metaWg.Wait()
	appliedLSN := s.appliedLSN()

	if continuous {
		// The applied position is safe to write after any end of the
		// stream. ctx may be cancelled here, so the write does not use it.
		if appliedLSN > lastFlushedLSN {
			flushCtx, cancel := context.WithTimeout(processingCtx, 10*time.Second)
			if ferr := flushMetadata(flushCtx, pool, publication, slotName, appliedLSN, tables); ferr != nil {
				logger.Warn("failed to flush CDC metadata on shutdown: %v", ferr)
			}
			cancel()
		}
		if err != nil && ctx.Err() != nil {
			logger.Info("Replication stopping due to context cancellation")
		}
		return err
	}

	if err != nil {
		if ctx.Err() != nil && appliedLSN < drainTargetLSN {
			return fmt.Errorf("CDC drain reached LSN %s of target %s on slot %s but did not "+
				"catch up within cdc_processing_timeout. Progress is durable: re-run "+
				"'ace mtree update' to continue draining, or raise cdc_processing_timeout to "+
				"drain in one pass (required when a single transaction is larger than the "+
				"timeout window): %w", appliedLSN, drainTargetLSN, slotName, ctx.Err())
		}
		return err
	}

	// Data-safety backstop: a bounded drain must not report success unless it
	// actually reached the call-time snapshot. If the stream ended any other
	// way without an error, fail loud rather than checkpoint a stale position
	// and let UpdateMtree/DiffMtree compare a tree that never saw the backlog.
	if !reachedTarget {
		return fmt.Errorf("CDC drain ended at LSN %s without reaching target %s on slot %s; "+
			"refusing to report a possibly-stale Merkle tree as current", appliedLSN, drainTargetLSN, slotName)
	}

	if err := flushMetadata(processingCtx, pool, publication, slotName, appliedLSN, tables); err != nil {
		return err
	}
	return nil
}

// applyItemToTrees does the work of one item of the stream on the Merkle
// trees: it marks all leaves of each table of item.markAll dirty, and then
// applies item.changes in one transaction.
func applyItemToTrees(ctx context.Context, pool *pgxpool.Pool, mtreeSchema string, item applyItem) error {
	for _, t := range item.markAll {
		// Sanitized: the mtree table is created quoted/case-preserved.
		mtreeTable := pgx.Identifier{mtreeSchema, fmt.Sprintf("ace_mtree_%s_%s", t.schema, t.table)}.Sanitize()
		marked, err := queries.MarkAllLeavesDirty(ctx, pool, mtreeTable)
		if err != nil {
			return fmt.Errorf("failed to mark all leaves dirty for %s.%s: %w", t.schema, t.table, err)
		}
		logger.Info("marked all %d blocks of %s.%s dirty for rehash on next tree update", marked, t.schema, t.table)
	}
	if len(item.changes) == 0 {
		return nil
	}
	if err := processChanges(ctx, pool, item.changes, mtreeSchema); err != nil {
		return err
	}
	logger.Debug("Processed %d changes for XID %d", len(item.changes), item.xid)
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
