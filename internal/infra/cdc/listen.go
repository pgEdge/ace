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
	tuple     *pglogrepl.TupleData
}

func ListenForChanges(ctx context.Context, nodeInfo map[string]any) {
	processReplicationStream(ctx, nodeInfo, true)
}

func UpdateFromCDC(ctx context.Context, nodeInfo map[string]any) error {
	return processReplicationStream(ctx, nodeInfo, false)
}

func processReplicationStream(ctx context.Context, nodeInfo map[string]any, continuous bool) error {
	pool, err := auth.GetClusterNodeConnection(ctx, nodeInfo, auth.ConnectionOptions{})
	if err != nil {
		logger.Error("failed to get connection pool: %v", err)
		return fmt.Errorf("failed to get connection pool: %w", err)
	}
	processingCtx := context.WithoutCancel(ctx)

	cfg := config.Cfg.MTree.CDC
	publication := cfg.PublicationName
	slotName := cfg.SlotName
	var startLSNStr string
	var tables []string
	func() {
		tx, err := pool.Begin(ctx)
		if err != nil {
			logger.Error("failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback(ctx)

		metaSlot, metaLSN, metaTables, err := queries.GetCDCMetadata(ctx, tx, publication)
		if err != nil {
			logger.Error("failed to get cdc metadata: %v", err)
			startLSNStr = ""
			return
		}
		startLSNStr = metaLSN
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

	slotConfirmedLSN, err := getSlotConfirmedFlushLSN(ctx, pool, slotName)
	if err != nil {
		logger.Error("failed to get confirmed_flush_lsn for slot %s: %v", slotName, err)
		return fmt.Errorf("failed to get confirmed_flush_lsn for slot %s: %w", slotName, err)
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
	if slotConfirmedLSN != 0 && slotConfirmedLSN < lastLSN {
		lastLSN = slotConfirmedLSN
	}
	var lastFlushedLSN pglogrepl.LSN = lastLSN
	var lastLSNVal atomic.Uint64
	lastLSNVal.Store(uint64(lastLSN))
	flushInterval := 10 * time.Second
	if config.Cfg.MTree.CDC.CDCMetadataFlushSec > 0 {
		flushInterval = time.Duration(config.Cfg.MTree.CDC.CDCMetadataFlushSec) * time.Second
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
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				if !continuous {
					logger.Info("Replication stream drained.")
					stopStreaming = true
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
							wg.Add(1)
							go func(c []cdcMsg) {
								defer wg.Done()
								workerSem <- struct{}{}
								defer func() { <-workerSem }()
								if err := processChanges(processingCtx, pool, c); err != nil {
									select {
									case errCh <- err:
									default:
									}
								}
							}(changes)
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
						tuple:     logicalMsg.Tuple,
					})
				case *pglogrepl.UpdateMessage:
					rel := relations[logicalMsg.RelationID]
					txChanges[currentXID] = append(txChanges[currentXID], cdcMsg{
						operation: "UPDATE",
						schema:    rel.Namespace,
						table:     rel.RelationName,
						relation:  rel,
						tuple:     logicalMsg.NewTuple,
					})
				case *pglogrepl.DeleteMessage:
					rel := relations[logicalMsg.RelationID]
					txChanges[currentXID] = append(txChanges[currentXID], cdcMsg{
						operation: "DELETE",
						schema:    rel.Namespace,
						table:     rel.RelationName,
						relation:  rel,
						tuple:     logicalMsg.OldTuple,
					})
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
					stopStreaming = true
				}
			}
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

func getSlotConfirmedFlushLSN(ctx context.Context, pool *pgxpool.Pool, slotName string) (pglogrepl.LSN, error) {
	var lsnStr string
	err := pool.QueryRow(ctx, "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = $1", slotName).Scan(&lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch confirmed_flush_lsn for slot %s: %w", slotName, err)
	}
	if lsnStr == "" {
		return 0, fmt.Errorf("confirmed_flush_lsn empty for slot %s", slotName)
	}
	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse confirmed_flush_lsn %s for slot %s: %w", lsnStr, slotName, err)
	}
	return lsn, nil
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

func processChanges(ctx context.Context, pool *pgxpool.Pool, changes []cdcMsg) error {
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
			mtreeTable := fmt.Sprintf("%s.ace_mtree_%s_%s", config.Cfg.MTree.Schema, schema, table)

			logger.Debug("Processing %d changes for table %s.%s (mtree: %s)", len(tableChanges), schema, table, mtreeTable)

			var inserts, deletes, updates []string
			relation := firstChange.relation
			isComposite := len(getPrimaryKeyColumns(relation)) > 1
			compositeTypeName := fmt.Sprintf("%s.%s_%s_key_type", config.Cfg.MTree.Schema, schema, table)
			var pkeyType string
			if !isComposite {
				for _, col := range relation.Columns {
					if col.Flags == 1 {
						typeName, err := getTypeNameFromOID(ctx, pool, col.DataType)
						if err != nil {
							return fmt.Errorf("failed to get type name for oid %d: %w", col.DataType, err)
						}
						pkeyType = typeName
						break
					}
				}
			}

			for _, change := range tableChanges {
				pks := getPKs(relation, change.tuple)
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
	err := pool.QueryRow(ctx, "SELECT typname FROM pg_type WHERE oid = $1", oid).Scan(&typeName)
	if err != nil {
		return "", err
	}
	return typeName, nil
}
