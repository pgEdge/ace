package cdc

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

func ListenForChanges(nodeInfo map[string]any) {
	pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
	if err != nil {
		logger.Error("failed to get connection pool: %v", err)
	}

	cfg := config.Cfg.MTree.CDC
	publication := cfg.PublicationName
	slotName := cfg.SlotName
	_, startLSNStr, _, err := queries.GetCDCMetadata(context.Background(), pool, publication)
	if err != nil {
		logger.Error("failed to get cdc metadata: %v", err)
	}

	startLSN, err := pglogrepl.ParseLSN(startLSNStr)
	if err != nil {
		logger.Error("failed to parse lsn: %v", err)
	}

	conn, err := auth.GetReplModeConnection(nodeInfo)
	if err != nil {
		logger.Error("failed to get replication connection: %v", err)
	}

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		},
	}
	if err := pglogrepl.StartReplication(context.Background(), conn, slotName, startLSN, opts); err != nil {
		logger.Error("StartReplication failed: %v", err)
	}
	logger.Info("Logical replication started on slot %s", slotName)

	ctx := context.Background()
	defer conn.Close(ctx)

	statusTick := time.NewTicker(10 * time.Second)
	defer statusTick.Stop()

	var lastLSN pglogrepl.LSN = startLSN
	relations := make(map[uint32]*pglogrepl.RelationMessage)

	for {
		select {
		case <-ctx.Done():
			return
		case <-statusTick.C:
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
				logger.Error("SendStandbyStatusUpdate failed: %v", err)
			}
			logger.Info("Sent Standby status update with LSN %s", lastLSN)
		default:
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			msg, err := conn.ReceiveMessage(ctx)
			cancel()
			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}
				logger.Error("ReceiveMessage failed: %v", err)
			}

			switch msg := msg.(type) {
			case *pgproto3.CopyData:
				switch msg.Data[0] {
				case pglogrepl.PrimaryKeepaliveMessageByteID:
					pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
					if err != nil {
						logger.Error("ParsePrimaryKeepaliveMessage failed: %v", err)
					}
					logger.Info("Primary Keepalive Message => ServerWALEnd: %s, ServerTime: %s, ReplyRequested: %t", pkm.ServerWALEnd, pkm.ServerTime, pkm.ReplyRequested)

					if pkm.ReplyRequested {
						if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lastLSN}); err != nil {
							logger.Error("SendStandbyStatusUpdate failed: %v", err)
						}
					}

				case pglogrepl.XLogDataByteID:
					xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
					if err != nil {
						logger.Error("ParseXLogData failed: %v", err)
					}

					logicalMsg, err := pglogrepl.Parse(xld.WALData)
					if err != nil {
						logger.Error("Parse logical replication message failed: %v", err)
					}

					switch logicalMsg := logicalMsg.(type) {
					case *pglogrepl.BeginMessage:
						logger.Info("BEGIN: LSN=%s, XID=%d", xld.WALStart, logicalMsg.Xid)
					case *pglogrepl.CommitMessage:
						logger.Info("COMMIT: LSN=%s", logicalMsg.CommitLSN)
					case *pglogrepl.OriginMessage:
						logger.Info("ORIGIN: Name=%q, LSN=%s", logicalMsg.Name, logicalMsg.CommitLSN)
					case *pglogrepl.RelationMessage:
						relations[logicalMsg.RelationID] = logicalMsg
						logger.Info("RELATION: %s.%s (ID=%d)", logicalMsg.Namespace, logicalMsg.RelationName, logicalMsg.RelationID)
					case *pglogrepl.InsertMessage:
						rel := relations[logicalMsg.RelationID]
						values := tupleKV(rel, logicalMsg.Tuple)
						logger.Info("INSERT: %s.%s -> %s", rel.Namespace, rel.RelationName, values)
					case *pglogrepl.UpdateMessage:
						rel := relations[logicalMsg.RelationID]
						values := tupleKV(rel, logicalMsg.NewTuple)
						logger.Info("UPDATE: %s.%s -> %s", rel.Namespace, rel.RelationName, values)
					case *pglogrepl.DeleteMessage:
						rel := relations[logicalMsg.RelationID]
						values := tupleKV(rel, logicalMsg.OldTuple)
						logger.Info("DELETE: %s.%s -> %s", rel.Namespace, rel.RelationName, values)
					case *pglogrepl.TruncateMessage:
						logger.Info("TRUNCATE: RelationIDs=%v", logicalMsg.RelationIDs)
					default:
						logger.Info("Unknown message type: %T", logicalMsg)
					}

					lastLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				}
			default:
				logger.Info("Received unexpected message: %T", msg)
			}
		}
	}
}

func tupleKV(rel *pglogrepl.RelationMessage, tup *pglogrepl.TupleData) string {
	if tup == nil || rel == nil {
		return "{}"
	}

	var sb strings.Builder
	sb.WriteString("{")

	count := 0
	for i, col := range tup.Columns {
		colName := rel.Columns[i].Name

		if count > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString(fmt.Sprintf("%s: ", colName))

		switch col.DataType {
		case 'n': // null
			sb.WriteString("NULL")
		case 'u': // unchanged toast
			sb.WriteString("[unchanged]")
		case 't': // text
			sb.WriteString(fmt.Sprintf("'%s'", string(col.Data)))
		default:
			sb.WriteString(string(col.Data))
		}
		count++
	}

	sb.WriteString("}")
	return sb.String()
}
