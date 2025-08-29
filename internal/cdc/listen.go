// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
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
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
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

func ListenForChanges(nodeInfo map[string]any) {
	processReplicationStream(nodeInfo, true)
}

func UpdateFromCDC(nodeInfo map[string]any) {
	processReplicationStream(nodeInfo, false)
}

func processReplicationStream(nodeInfo map[string]any, continuous bool) {
	pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
	if err != nil {
		logger.Error("failed to get connection pool: %v", err)
	}

	cfg := config.Cfg.MTree.CDC
	publication := cfg.PublicationName
	slotName := cfg.SlotName
	var startLSNStr string
	func() {
		tx, err := pool.Begin(context.Background())
		if err != nil {
			logger.Error("failed to begin transaction: %v", err)
			return
		}
		defer tx.Rollback(context.Background())
		_, startLSNStr, _, err = queries.GetCDCMetadata(context.Background(), tx, publication)
		if err != nil {
			logger.Error("failed to get cdc metadata: %v", err)
			startLSNStr = ""
		}
	}()

	if startLSNStr == "" {
		logger.Error("Could not retrieve LSN. Aborting CDC processing for this node.")
		return
	}

	startLSN, err := pglogrepl.ParseLSN(startLSNStr)
	if err != nil {
		logger.Error("failed to parse lsn: %v", err)
	}

	var lastLSN pglogrepl.LSN = startLSN
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

		if err := pglogrepl.StartReplication(context.Background(), c, slotName, lastLSN, opts); err != nil {
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

	ctx := context.Background()

	relations := make(map[uint32]*pglogrepl.RelationMessage)
	nextStandbyMessageDeadline := time.Now().Add(10 * time.Second)

	txChanges := make(map[uint32][]cdcMsg)
	var currentXID uint32

	for {
		if conn == nil {
			logger.Info("No connection, trying to reconnect...")
			conn, err = connect()
			if err != nil {
				logger.Error("reconnect failed: %v", err)
				if !continuous {
					return
				}
				time.Sleep(5 * time.Second)
			}
			nextStandbyMessageDeadline = time.Now().Add(10 * time.Second)

			if !continuous {
				if conn == nil {
					return
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
			if pgconn.Timeout(err) {
				if !continuous {
					logger.Info("Replication stream drained.")
					if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
						logger.Error("failed to send final standby status update: %v", err)
					}
					conn.Close(ctx)
					return
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
							go processChanges(pool, changes)
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
			}
		default:
			logger.Info("Received unexpected message: %T", msg)
		}
	}
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

func processChanges(pool *pgxpool.Pool, changes []cdcMsg) {
	tx, err := pool.Begin(context.Background())
	if err != nil {
		logger.Error("failed to begin transaction for processing changes: %v", err)
		return
	}
	defer tx.Rollback(context.Background())

	tables := make(map[string][]cdcMsg)
	for _, change := range changes {
		key := fmt.Sprintf("%s.%s", change.schema, change.table)
		tables[key] = append(tables[key], change)
	}

	for _, tableChanges := range tables {
		if len(tableChanges) > 0 {
			firstChange := tableChanges[0]
			schema := firstChange.schema
			table := firstChange.table
			mtreeTable := fmt.Sprintf("ace_mtree_%s_%s", schema, table)

			var inserts, deletes, updates []string
			relation := firstChange.relation
			isComposite := len(getPrimaryKeyColumns(relation)) > 1
			compositeTypeName := fmt.Sprintf("%s_%s_key_type", schema, table)

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

			err := queries.UpdateMtreeCounters(context.Background(), tx, mtreeTable, isComposite, compositeTypeName, inserts, deletes, updates)
			if err != nil {
				logger.Error("failed to update mtree counters for %s: %v", mtreeTable, err)
				return
			}
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		logger.Error("failed to commit CDC changes: %v", err)
	}
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
		val := string(tupCol.Data)
		if quotedOIDs[relCol.DataType] {
			return fmt.Sprintf("'%s'", strings.ReplaceAll(val, "'", "''"))
		}
		return val
	default:
		return string(tupCol.Data)
	}
}
