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

package auth

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/logger"
)

type ConnectionOptions struct {
	DBName         string
	Role           string
	PoolSize       int
	DropPrivileges bool
}

func toConnectionString(node map[string]any, dbName string) string {
	var parts []string

	stringValue := func(key string) string {
		if node == nil {
			return ""
		}
		val, ok := node[key]
		if !ok || val == nil {
			return ""
		}
		switch v := val.(type) {
		case string:
			return strings.TrimSpace(v)
		case fmt.Stringer:
			return strings.TrimSpace(v.String())
		default:
			return strings.TrimSpace(fmt.Sprintf("%v", v))
		}
	}

	var host string
	if h := stringValue("Host"); h != "" {
		host = h
	} else if h := stringValue("PublicIP"); h != "" {
		host = h
	} else if h := stringValue("PrivateIP"); h != "" {
		host = h
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
		if trimmed := strings.TrimSpace(v); trimmed != "" {
			parts = append(parts, "port="+trimmed)
		}
	}

	if user := stringValue("DBUser"); user != "" {
		parts = append(parts, "user="+user)
	}
	if password := stringValue("DBPassword"); password != "" {
		parts = append(parts, "password="+password)
	}

	dbToUse := dbName
	if dbToUse == "" {
		dbToUse = stringValue("DBName")
	}
	if dbToUse != "" {
		parts = append(parts, "dbname="+dbToUse)
	}

	cfg := config.Get()
	if cfg != nil {
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

	useCertAuth := cfg != nil && cfg.CertAuth.UseCertAuth
	if !useCertAuth {
		parts = append(parts, "sslmode=disable")
	} else {
		sslMode := stringValue("SSLMode")
		if sslMode == "" {
			sslMode = "verify-full"
		}
		sslCert := stringValue("SSLCert")
		if sslCert == "" {
			sslCert = strings.TrimSpace(cfg.CertAuth.ACEUserCertFile)
		}
		sslKey := stringValue("SSLKey")
		if sslKey == "" {
			sslKey = strings.TrimSpace(cfg.CertAuth.ACEUserKeyFile)
		}
		sslRoot := stringValue("SSLRootCert")
		if sslRoot == "" {
			sslRoot = strings.TrimSpace(cfg.CertAuth.CACertFile)
		}

		if sslMode != "" {
			parts = append(parts, "sslmode="+sslMode)
		}
		if sslCert != "" {
			parts = append(parts, "sslcert="+sslCert)
		}
		if sslKey != "" {
			parts = append(parts, "sslkey="+sslKey)
		}
		if sslRoot != "" {
			parts = append(parts, "sslrootcert="+sslRoot)
		}
	}

	connStr := strings.Join(parts, " ")
	logger.Debug("connection string: %s", connStr)
	return connStr
}

func GetClusterNodeConnection(ctx context.Context, node map[string]any, opts ConnectionOptions) (*pgxpool.Pool, error) {
	connStr := toConnectionString(node, opts.DBName)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	applyConnectionOptions(config, opts)
	applyPostgresPoolConfig(config)
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	return pool, nil
}

func GetSizedClusterNodeConnection(node map[string]any, opts ConnectionOptions) (*pgxpool.Pool, error) {
	connStr := toConnectionString(node, opts.DBName)
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, err
	}
	applyConnectionOptions(config, opts)
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

// outputSettings are sent in the startup packet of every ACE connection.
//
// ACE compares rows by the text that the server prints for them: the row
// hash in table-diff and mtree is computed over the text of each column, and
// the walsender of the CDC connection prints every value of a pgoutput tuple
// as text. That text depends on these session settings. If two nodes have
// different defaults (in postgresql.conf, ALTER DATABASE ... SET or
// ALTER ROLE ... SET), equal rows give different hashes, and ACE reports
// differences that do not exist. A timestamptz printed under two time
// zones is the most common case. Some settings also change how input is
// read: with DateStyle = 'DMY' the literal '01/02/2026' is the 1st of
// February, with 'MDY' it is the 2nd of January.
//
// client_encoding is pinned for a different reason. Without it, the server
// sends text in the database encoding, but Go strings and pgx expect UTF-8.
// Logical replication does the same: the subscriber asks the publisher for
// its own encoding.
//
// Logical replication also pins DateStyle, IntervalStyle and
// extra_float_digits on the publisher (libpqwalreceiver.c), but only to make
// each value unambiguous. ACE needs more: it compares the text byte by byte,
// so every node must print a value in exactly the same way.
//
// The values are fixed, not configurable: the only requirement is that all
// nodes use the same values, and fixed values meet it with no check.
// DateStyle keeps the PostgreSQL default order, MDY, so that a date literal
// in a user filter is read as it is on a server with default settings. The
// order does not change the output: ISO always prints YYYY-MM-DD.
// schema-diff pins the same values for its snapshot transaction (it reads
// them with OutputSettings).
//
// The startup packet is used, not SET in AfterConnect, for three reasons.
// It costs no extra round trip. It also covers the replication connection,
// which is a plain pgconn.PgConn with no AfterConnect hook. And a value sent
// in the startup packet has priority over ALTER ROLE and ALTER DATABASE
// defaults (it is a PGC_S_CLIENT source), and over the same setting in the
// "options" parameter (PGOPTIONS), because the server applies "options"
// first.
//
// A connection pooler can drop a startup parameter without an error, for
// example PgBouncer with ignore_startup_parameters. validateOutputSettings
// therefore checks the values that the server reports back after the
// connection is made.
//
// search_path is not pinned here, unlike in schema-diff: a user filter of
// table-diff may call a function by an unqualified name.
//
// Changing a value can change the hashes. Increase queries.CurrentHashVersion
// in the same commit, so that mtree recomputes the stored hashes.
var outputSettings = map[string]string{
	"client_encoding":             "UTF8",
	"datestyle":                   "ISO, MDY",
	"intervalstyle":               "postgres",
	"timezone":                    "UTC",
	"extra_float_digits":          "3",
	"bytea_output":                "hex",
	"lc_monetary":                 "C",
	"standard_conforming_strings": "on",
}

// reportedOutputSettings lists the output settings that the server reports
// in a ParameterStatus message, by the name the server uses in that message.
// The other output settings (extra_float_digits, bytea_output, lc_monetary)
// are not reported, so they cannot be checked without a query.
var reportedOutputSettings = map[string]string{
	"client_encoding":             "client_encoding",
	"datestyle":                   "DateStyle",
	"intervalstyle":               "IntervalStyle",
	"timezone":                    "TimeZone",
	"standard_conforming_strings": "standard_conforming_strings",
}

// OutputSettings returns a copy of the settings that every ACE connection
// gets in its startup packet, keyed by lower-case setting name.
func OutputSettings() map[string]string {
	settings := make(map[string]string, len(outputSettings))
	for name, value := range outputSettings {
		settings[name] = value
	}
	return settings
}

// applyOutputSettings writes outputSettings into params. Setting names are
// case-insensitive on the server, so a key that differs only in case (for
// example "TimeZone" from a connection string, or "timezone" from PGTZ) is
// removed first. Otherwise the startup packet would carry the setting twice,
// and the server would apply the two values in Go map order, which is
// random.
func applyOutputSettings(params map[string]string) {
	for key := range params {
		if _, ok := outputSettings[strings.ToLower(key)]; ok {
			delete(params, key)
		}
	}
	for name, value := range outputSettings {
		params[name] = value
	}
}

// checkReportedOutputSettings compares the reported value of each setting in
// reportedOutputSettings with the value ACE sent. status returns the value
// from the ParameterStatus messages of the connection, or "" if the server
// did not report it.
func checkReportedOutputSettings(status func(name string) string) error {
	var wrong []string
	for name, reported := range reportedOutputSettings {
		want := outputSettings[name]
		if got := status(reported); got != want {
			wrong = append(wrong, fmt.Sprintf("%s is %q, expected %q", reported, got, want))
		}
	}
	if len(wrong) == 0 {
		return nil
	}
	sort.Strings(wrong)
	return fmt.Errorf("the server did not apply the session settings that ACE needs to compare data (%s); "+
		"if a connection pooler is used, make it pass these startup parameters to the server",
		strings.Join(wrong, "; "))
}

// validateOutputSettings wraps the ValidateConnect hook of cfg so that a
// connection fails when the server reports an output setting with a value
// other than the one ACE sent. It reads only the ParameterStatus messages
// that the server sends during the connection start, so it costs no round
// trip. A hook that is already set (for example by target_session_attrs) is
// called first.
func validateOutputSettings(cfg *pgconn.Config) {
	prev := cfg.ValidateConnect
	cfg.ValidateConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		if prev != nil {
			if err := prev(ctx, conn); err != nil {
				return err
			}
		}
		return checkReportedOutputSettings(conn.ParameterStatus)
	}
}

func applyPostgresPoolConfig(poolCfg *pgxpool.Config) {
	if poolCfg == nil || poolCfg.ConnConfig == nil {
		return
	}
	runtimeParams := ensureRuntimeParams(poolCfg.ConnConfig.Config.RuntimeParams)
	applyOutputSettings(runtimeParams)
	poolCfg.ConnConfig.Config.RuntimeParams = runtimeParams
	validateOutputSettings(&poolCfg.ConnConfig.Config)
	cfg := config.Get()
	if ms, ok := clientCheckInterval(cfg); ok {
		setClientCheckInterval(&poolCfg.ConnConfig.Config, ms)
	}
	if cfg == nil {
		return
	}
	applyRuntimeParams(runtimeParams, cfg.Postgres)
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
	runtimeParams := ensureRuntimeParams(pgCfg.RuntimeParams)
	applyOutputSettings(runtimeParams)
	pgCfg.RuntimeParams = runtimeParams
	validateOutputSettings(pgCfg)
	cfg := config.Get()
	if ms, ok := clientCheckInterval(cfg); ok {
		setClientCheckInterval(pgCfg, ms)
	}
	if cfg == nil {
		return
	}
	applyRuntimeParams(runtimeParams, cfg.Postgres)
	if cfg.Postgres.ConnectionTimeout > 0 {
		pgCfg.ConnectTimeout = time.Duration(cfg.Postgres.ConnectionTimeout) * time.Second
	}
}

// defaultClientConnectionCheckInterval is the value of
// client_connection_check_interval, in milliseconds, that ACE sets when
// ace.yaml does not set it.
const defaultClientConnectionCheckInterval = 5000

// clientCheckInterval returns the value of client_connection_check_interval
// in milliseconds that ACE sets, and false if ACE leaves the server setting
// alone.
func clientCheckInterval(cfg *config.Config) (int, bool) {
	ms := defaultClientConnectionCheckInterval
	if cfg != nil && cfg.Postgres.ClientConnectionCheckInterval != nil {
		ms = *cfg.Postgres.ClientConnectionCheckInterval
	}
	return ms, ms >= 0
}

// setClientCheckInterval wraps the AfterConnect hook of cfg so that a new
// connection sets client_connection_check_interval to ms. If ACE stops
// without closing its connections, for example when the kernel kills it, a
// backend in the middle of a long query finds the lost client only with
// this check. Without it, the backend runs the query to the end and holds
// its snapshot all that time.
//
// The value is not sent in the startup packet. A server before PostgreSQL
// 14 does not know the setting, and a server on a platform without the
// check accepts only 0; in both cases the server would refuse the
// connection. If the server refuses the SET, the connection works without
// the check. A hook that is already set is called first.
func setClientCheckInterval(cfg *pgconn.Config, ms int) {
	prev := cfg.AfterConnect
	sql := fmt.Sprintf("SET client_connection_check_interval = %d", ms)
	cfg.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
		if prev != nil {
			if err := prev(ctx, conn); err != nil {
				return err
			}
		}
		_, err := conn.Exec(ctx, sql).ReadAll() // nosemgrep
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			logger.Debug("the server refused client_connection_check_interval: %v", err)
			return nil
		}
		return err
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

	// ACE parallelises its hash queries on the client side, so by default it
	// keeps Postgres from adding parallel workers to each one as well. Without
	// this, every ACE worker could occupy several server cores.
	parallelWorkers := 0
	if pgCfg.MaxParallelWorkersPerGather != nil {
		parallelWorkers = *pgCfg.MaxParallelWorkersPerGather
	}
	if parallelWorkers >= 0 {
		params["max_parallel_workers_per_gather"] = strconv.Itoa(parallelWorkers)
	}

	// Suppress Spock's DDL replication and auto-repset-add behaviour on
	// every ACE connection. ACE only issues DDL against its own pgedge_ace
	// schema and the ace_mtree_pub publication — never against user
	// objects — and all of that DDL is intentionally per-node. Letting
	// Spock replicate it produces cross-node races (e.g. CREATE OR REPLACE
	// FUNCTION racing on pg_proc) and 42704 "publication does not exist"
	// when one node's DROP PUBLICATION + CREATE PUBLICATION lands in
	// another node's pgoutput stream mid-replay.
	//
	// Both GUCs are PGC_USERSET in Spock, so connection-level options
	// override any cluster-level default. Both have dotted names, so on
	// vanilla PG (no Spock loaded) PostgreSQL accepts them as placeholder
	// custom variables and they have no effect — safe in dual-mode.
	params["spock.enable_ddl_replication"] = "off"
	params["spock.include_ddl_repset"] = "off"
}

func ensureRuntimeParams(params map[string]string) map[string]string {
	if params == nil {
		return make(map[string]string)
	}
	return params
}

func applyConnectionOptions(cfg *pgxpool.Config, opts ConnectionOptions) {
	if cfg == nil {
		return
	}
	if opts.PoolSize > 0 {
		cfg.MaxConns = int32(opts.PoolSize)
	}
	role := strings.TrimSpace(opts.Role)

	if role == "" || !opts.DropPrivileges {
		return
	}

	roleSQL := fmt.Sprintf("SET ROLE %s", pgx.Identifier{role}.Sanitize())
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if conn == nil {
			return fmt.Errorf("nil connection when applying role %s", role)
		}
		if _, err := conn.Exec(ctx, roleSQL); err != nil { // nosemgrep
			return fmt.Errorf("failed to set role %q: %w", role, err)
		}
		return nil
	}
}
