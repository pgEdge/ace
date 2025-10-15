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
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func toConnectionString(node map[string]any, dbName string) string {
	var parts []string
	var host string
	if h, ok := node["Host"].(string); ok && strings.TrimSpace(h) != "" {
		host = strings.TrimSpace(h)
	} else if h, ok := node["PublicIP"].(string); ok && strings.TrimSpace(h) != "" {
		host = strings.TrimSpace(h)
	} else if h, ok := node["PrivateIP"].(string); ok && strings.TrimSpace(h) != "" {
		host = strings.TrimSpace(h)
	}
	if host != "" {
		parts = append(parts, "host="+host)
	}
	switch v := node["Port"].(type) {
	case float64:
		if v != 0 {
			parts = append(parts, fmt.Sprintf("port=%d", int(v)))
		}
	case int:
		if v != 0 {
			parts = append(parts, fmt.Sprintf("port=%d", v))
		}
	case string:
		if strings.TrimSpace(v) != "" {
			parts = append(parts, "port="+strings.TrimSpace(v))
		}
	}
	if user, ok := node["DBUser"].(string); ok && user != "" {
		parts = append(parts, "user="+user)
	}
	if password, ok := node["DBPassword"].(string); ok && password != "" {
		parts = append(parts, "password="+password)
	}
	dbToUse := dbName
	if dbToUse == "" {
		if db, ok := node["DBName"].(string); ok && db != "" {
			dbToUse = db
		}
	}
	if dbToUse != "" {
		parts = append(parts, "dbname="+dbToUse)
	}

	parts = append(parts, "sslmode=disable")
	return strings.Join(parts, " ")
}

func GetClusterNodeConnection(ctx context.Context, node map[string]any, dbName string) (*pgxpool.Pool, error) {
	connStr := toConnectionString(node, dbName)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	return pool, nil
}

func GetSizedClusterNodeConnection(node map[string]any, dbName string, poolSize int) (*pgxpool.Pool, error) {
	connStr := toConnectionString(node, dbName)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	if poolSize > 0 {
		config.MaxConns = int32(poolSize)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	return pool, nil
}

func GetReplModeConnection(nodeInfo map[string]any) (*pgconn.PgConn, error) {
	connStr := toConnectionString(nodeInfo, "")
	connStr = connStr + " replication=database"

	conn, err := pgconn.Connect(context.Background(), connStr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
