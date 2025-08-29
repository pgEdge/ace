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

package auth

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getConnStr(nodeInfo map[string]any) string {
	user, _ := nodeInfo["DBUser"].(string)
	password, _ := nodeInfo["DBPassword"].(string)
	var host string
	if h, ok := nodeInfo["Host"].(string); ok && h != "" {
		host = h
	} else if h, ok := nodeInfo["PublicIP"].(string); ok && h != "" {
		host = h
	}
	database, _ := nodeInfo["DBName"].(string)

	var port string
	if p, ok := nodeInfo["Port"].(string); ok {
		port = p
	} else if p, ok := nodeInfo["Port"].(float64); ok {
		port = fmt.Sprintf("%.0f", p)
	}

	if port == "" {
		port = "5432"
	}

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, database)
}
func GetClusterNodeConnection(nodeInfo map[string]any, clientRole string) (*pgxpool.Pool, error) {
	connStr := getConnStr(nodeInfo)

	pool, err := SetupDBPool(context.Background(), connStr, nodeInfo["Name"].(string))
	if err != nil {
		return nil, err
	}

	return pool, nil
}

func GetReplModeConnection(nodeInfo map[string]any) (*pgconn.PgConn, error) {
	connStr := getConnStr(nodeInfo)
	connStr = connStr + "&replication=database"

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return nil, err
	}
	return conn, nil
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

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	return pool, nil
}
