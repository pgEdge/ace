/////////////////////////////////////////////////////////////////////////////
//
// ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the pgEdge Community License:
//      https://www.pgedge.com/communitylicense
//
/////////////////////////////////////////////////////////////////////////////

package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func GetClusterNodeConnection(nodeInfo map[string]any, clientRole string) (*pgxpool.Pool, error) {
	user, _ := nodeInfo["DBUser"].(string)
	password, _ := nodeInfo["DBPassword"].(string)
	host, _ := nodeInfo["PublicIP"].(string)
	database, _ := nodeInfo["DBName"].(string)
	port, okPort := nodeInfo["Port"].(string)

	if !okPort {
		port = "5432"
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, database)

	pool, err := SetupDBPool(context.Background(), connStr, fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func SetupDBPool(ctx context.Context, connStr, name string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Minute * 5
	config.MaxConnIdleTime = time.Minute * 2

	pool, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	return pool, nil
}
