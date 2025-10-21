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
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
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

	if cfg := config.Cfg; cfg != nil {
		pgCfg := cfg.Postgres
		if pgCfg.ConnectionTimeout > 0 {
			parts = append(parts, fmt.Sprintf("connect_timeout=%d", pgCfg.ConnectionTimeout))
		}
		if pgCfg.ApplicationName != "" {
			parts = append(parts, "application_name="+pgCfg.ApplicationName)
		}
		if pgCfg.TCPKeepalivesIdle != nil {
			parts = append(parts, fmt.Sprintf("tcp_keepalives_idle=%d", *pgCfg.TCPKeepalivesIdle))
		}
		if pgCfg.TCPKeepalivesInterval != nil {
			parts = append(parts, fmt.Sprintf("tcp_keepalives_interval=%d", *pgCfg.TCPKeepalivesInterval))
		}
		if pgCfg.TCPKeepalivesCount != nil {
			parts = append(parts, fmt.Sprintf("tcp_keepalives_count=%d", *pgCfg.TCPKeepalivesCount))
		}
	}
	parts = append(parts, "sslmode=disable")
	logger.Debug("connection string: %s", strings.Join(parts, " "))
	return strings.Join(parts, " ")
}

func GetClusterNodeConnection(ctx context.Context, node map[string]any, dbName string) (*pgxpool.Pool, error) {
	connStr := toConnectionString(node, dbName)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	applyPostgresPoolConfig(config)
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
	applyPostgresPoolConfig(config)
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	return pool, nil
}

func GetReplModeConnection(nodeInfo map[string]any) (*pgconn.PgConn, error) {
	connStr := toConnectionString(nodeInfo, "")
	cfg, err := pgconn.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	applyPostgresPgconnConfig(cfg)
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = make(map[string]string)
	}
	cfg.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func applyPostgresPoolConfig(poolCfg *pgxpool.Config) {
	if poolCfg == nil || poolCfg.ConnConfig == nil {
		return
	}
	cfg := config.Cfg
	if cfg == nil {
		return
	}
	runtimeParams := ensureRuntimeParams(poolCfg.ConnConfig.Config.RuntimeParams)
	applyRuntimeParams(runtimeParams, cfg.Postgres)
	poolCfg.ConnConfig.Config.RuntimeParams = runtimeParams
	if cfg.Postgres.ConnectionTimeout > 0 {
		timeout := time.Duration(cfg.Postgres.ConnectionTimeout) * time.Second
		poolCfg.ConnConfig.ConnectTimeout = timeout
		poolCfg.ConnConfig.Config.ConnectTimeout = timeout
	}
}

func applyPostgresPgconnConfig(pgCfg *pgconn.Config) {
	if pgCfg == nil {
		return
	}
	cfg := config.Cfg
	if cfg == nil {
		return
	}
	runtimeParams := ensureRuntimeParams(pgCfg.RuntimeParams)
	applyRuntimeParams(runtimeParams, cfg.Postgres)
	pgCfg.RuntimeParams = runtimeParams
	if cfg.Postgres.ConnectionTimeout > 0 {
		pgCfg.ConnectTimeout = time.Duration(cfg.Postgres.ConnectionTimeout) * time.Second
	}
}

func applyRuntimeParams(params map[string]string, pgCfg config.PostgresConfig) {
	params["statement_timeout"] = strconv.Itoa(pgCfg.StatementTimeout)
	if pgCfg.ApplicationName != "" {
		params["application_name"] = pgCfg.ApplicationName
	}
	if pgCfg.TCPKeepalivesIdle != nil {
		params["tcp_keepalives_idle"] = strconv.Itoa(*pgCfg.TCPKeepalivesIdle)
	}
	if pgCfg.TCPKeepalivesInterval != nil {
		params["tcp_keepalives_interval"] = strconv.Itoa(*pgCfg.TCPKeepalivesInterval)
	}
	if pgCfg.TCPKeepalivesCount != nil {
		params["tcp_keepalives_count"] = strconv.Itoa(*pgCfg.TCPKeepalivesCount)
	}
}

func ensureRuntimeParams(params map[string]string) map[string]string {
	if params == nil {
		return make(map[string]string)
	}
	return params
}
