package auth

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

func GetClusterNodeConnection(nodeInfo map[string]any, clientRole string) (*pgxpool.Pool, error) {
	user, _ := nodeInfo["DBUser"].(string)
	password, _ := nodeInfo["DBPassword"].(string)
	host, _ := nodeInfo["PublicIP"].(string)
	database, _ := nodeInfo["DBName"].(string)
	var port float64
	portVal, ok := nodeInfo["Port"]
	if ok {
		switch v := portVal.(type) {
		case string:
			port, _ = strconv.ParseFloat(v, 64)
		case float64:
			port = v
		}
	}

	if port == 0 {
		port = 5432
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", user, password, host, int(port), database)

	pool, err := SetupDBPool(context.Background(), connStr, fmt.Sprintf("%s:%d", host, int(port)))
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
