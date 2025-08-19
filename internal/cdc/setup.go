package cdc

import (
	"context"
	"fmt"

	"github.com/jackc/pglogrepl"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

func SetupCDC(nodeInfo map[string]any) (pglogrepl.LSN, error) {
	cfg := config.Cfg.MTree.CDC

	conn, err := auth.GetReplModeConnection(nodeInfo)
	if err != nil {
		return 0, fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close(context.Background())

	// Currently need this for CreatePublication.
	// TODO: Find a cleaner way to do this.
	pool, err := auth.GetClusterNodeConnection(nodeInfo, "")
	if err != nil {
		return 0, fmt.Errorf("failed to get connection pool: %w", err)
	}

	sys, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		conn.Close(context.Background())
		return 0, fmt.Errorf("IdentifySystem failed: %w", err)
	}
	startLSN := sys.XLogPos
	logger.Info("SystemID: %s, Timeline: %d, XLogPos: %s, DBName: %s", sys.SystemID, sys.Timeline, startLSN, sys.DBName)

	slot := cfg.SlotName
	publication := cfg.PublicationName
	logger.Info("Creating publication '%s'", publication)

	err = queries.CreatePublication(context.Background(), pool, publication)
	if err != nil {
		return 0, fmt.Errorf("failed to create publication: %w", err)
	}

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		logger.Error("CreateReplicationSlot failed (slot might already exist): %v", err)
	}
	logger.Info("Created replication slot '%s'", slot)

	opts := pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", publication),
		},
	}
	if err := pglogrepl.StartReplication(context.Background(), conn, slot, startLSN, opts); err != nil {
		conn.Close(context.Background())
		return 0, fmt.Errorf("StartReplication failed: %w", err)
	}
	logger.Info("Logical replication started on slot %s", slot)

	return startLSN, nil
}
