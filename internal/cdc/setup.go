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

	"github.com/jackc/pglogrepl"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/internal/auth"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

func SetupPublication(ctx context.Context, db queries.DBQuerier, publicationName string) error {
	logger.Info("Setting up publication '%s'", publicationName)

	err := queries.DropPublication(ctx, db, publicationName)
	if err != nil {
		return fmt.Errorf("failed to drop publication: %w", err)
	}

	err = queries.CreatePublication(ctx, db, publicationName)
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}
	logger.Info("Created publication '%s'", publicationName)
	return nil
}

func SetupReplicationSlot(ctx context.Context, nodeInfo map[string]any) (pglogrepl.LSN, error) {
	cfg := config.Cfg.MTree.CDC
	slot := cfg.SlotName

	pool, err := auth.GetClusterNodeConnection(ctx, nodeInfo, auth.ConnectionOptions{})
	if err != nil {
		return 0, fmt.Errorf("failed to get connection pool: %w", err)
	}
	defer pool.Close()

	err = queries.DropReplicationSlot(ctx, pool, slot)
	if err != nil {
		return 0, fmt.Errorf("failed to drop replication slot: %w", err)
	}

	conn, err := auth.GetReplModeConnection(nodeInfo)
	if err != nil {
		return 0, fmt.Errorf("failed to get replication connection: %w", err)
	}
	defer conn.Close(context.Background())

	sys, err := pglogrepl.IdentifySystem(context.Background(), conn)
	if err != nil {
		return 0, fmt.Errorf("IdentifySystem failed: %w", err)
	}
	startLSN := sys.XLogPos
	logger.Info("SystemID: %s, Timeline: %d, XLogPos: %s, DBName: %s", sys.SystemID, sys.Timeline, startLSN, sys.DBName)

	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		return 0, fmt.Errorf("CreateReplicationSlot failed: %w", err)
	}
	logger.Info("Created replication slot '%s'", slot)

	return startLSN, nil
}
