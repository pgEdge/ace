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
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
)

// requireOutputSettings checks that params holds every output setting with
// its fixed value, and holds no other key with the same name in another
// case.
func requireOutputSettings(t *testing.T, params map[string]string) {
	t.Helper()
	for name, want := range outputSettings {
		if got, ok := params[name]; !ok || got != want {
			t.Errorf("setting %s: got %q (present: %v), want %q", name, got, ok, want)
		}
	}
	for key := range params {
		lower := strings.ToLower(key)
		if _, ok := outputSettings[lower]; ok && key != lower {
			t.Errorf("key %q duplicates the output setting %q", key, lower)
		}
	}
}

func TestApplyOutputSettingsReplacesKeysInAnyCase(t *testing.T) {
	params := map[string]string{
		"TimeZone":         "Europe/Berlin",
		"DATESTYLE":        "SQL, DMY",
		"timezone":         "America/New_York",
		"application_name": "keep-me",
	}
	applyOutputSettings(params)
	requireOutputSettings(t, params)
	if params["application_name"] != "keep-me" {
		t.Errorf("application_name changed to %q", params["application_name"])
	}
}

func TestApplyOutputSettingsNilSafeThroughEnsure(t *testing.T) {
	params := ensureRuntimeParams(nil)
	applyOutputSettings(params)
	requireOutputSettings(t, params)
}

// The settings must be sent even when no ace.yaml is loaded: they decide
// the row hashes, not a user preference. The PGTZ and PGOPTIONS variables
// must not win over them either.
func TestPoolConfigGetsOutputSettings(t *testing.T) {
	t.Setenv("PGTZ", "Asia/Tokyo")
	t.Setenv("PGOPTIONS", "-c DateStyle=SQL,DMY")
	cfg, err := pgxpool.ParseConfig("host=localhost dbname=x user=x TimeZone=Europe/Berlin sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	applyPostgresPoolConfig(cfg)
	requireOutputSettings(t, cfg.ConnConfig.Config.RuntimeParams)
	// "options" is applied by the server before the separate settings, so
	// it may stay: the value in outputSettings overrides it.
	if cfg.ConnConfig.Config.RuntimeParams["options"] == "" {
		t.Errorf("PGOPTIONS was dropped; only the pinned settings must be replaced")
	}
}

// The replication connection is a plain pgconn connection; its walsender
// prints the pgoutput tuple values, so it needs the same settings.
func TestReplicationConfigGetsOutputSettings(t *testing.T) {
	cfg, err := pgconn.ParseConfig("host=localhost dbname=x user=x timezone=Europe/Berlin sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	applyPostgresPgconnConfig(cfg)
	requireOutputSettings(t, cfg.RuntimeParams)
}

// reportedStatus returns the ParameterStatus values that a server reports
// after it applied every output setting.
func reportedStatus() map[string]string {
	status := make(map[string]string)
	for name, reported := range reportedOutputSettings {
		status[reported] = outputSettings[name]
	}
	return status
}

func TestReportedOutputSettingsAreOutputSettings(t *testing.T) {
	for name := range reportedOutputSettings {
		if _, ok := outputSettings[name]; !ok {
			t.Errorf("reported setting %q is not an output setting", name)
		}
	}
}

func TestCheckReportedOutputSettings(t *testing.T) {
	status := reportedStatus()
	lookup := func(name string) string { return status[name] }
	if err := checkReportedOutputSettings(lookup); err != nil {
		t.Fatalf("all settings applied, got error: %v", err)
	}

	// A pooler that drops TimeZone leaves the server default; one that
	// does not forward client_encoding leaves it unreported.
	status["TimeZone"] = "Europe/Berlin"
	delete(status, "client_encoding")
	err := checkReportedOutputSettings(lookup)
	if err == nil {
		t.Fatal("expected an error for a changed and a missing setting")
	}
	for _, part := range []string{`TimeZone is "Europe/Berlin", expected "UTC"`, `client_encoding is "", expected "UTF8"`} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("error %q does not contain %q", err, part)
		}
	}
}

// The hook set by target_session_attrs must still run, and run first.
func TestValidateOutputSettingsKeepsPreviousHook(t *testing.T) {
	cfg, err := pgconn.ParseConfig("host=localhost dbname=x user=x sslmode=disable target_session_attrs=read-write")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ValidateConnect == nil {
		t.Fatal("target_session_attrs did not set ValidateConnect")
	}
	prevErr := errors.New("previous hook")
	cfg.ValidateConnect = func(context.Context, *pgconn.PgConn) error { return prevErr }
	validateOutputSettings(cfg)
	if err := cfg.ValidateConnect(context.Background(), nil); !errors.Is(err, prevErr) {
		t.Fatalf("got %v, want the error of the previous hook", err)
	}
}

func TestConfigsGetValidateHook(t *testing.T) {
	poolCfg, err := pgxpool.ParseConfig("host=localhost dbname=x user=x sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	applyPostgresPoolConfig(poolCfg)
	if poolCfg.ConnConfig.Config.ValidateConnect == nil {
		t.Error("pool config has no ValidateConnect hook")
	}

	replCfg, err := pgconn.ParseConfig("host=localhost dbname=x user=x sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	applyPostgresPgconnConfig(replCfg)
	if replCfg.ValidateConnect == nil {
		t.Error("replication config has no ValidateConnect hook")
	}
}

func intPtr(v int) *int { return &v }

// ACE already runs its hash queries from several client-side workers, so by
// default it stops Postgres from adding parallel workers on top of that.
func TestRuntimeParamsDisableParallelQueryByDefault(t *testing.T) {
	params := map[string]string{}
	applyRuntimeParams(params, config.PostgresConfig{})
	if got := params["max_parallel_workers_per_gather"]; got != "0" {
		t.Fatalf("max_parallel_workers_per_gather = %q, want \"0\"", got)
	}
}

func TestRuntimeParamsHonourConfiguredParallelWorkers(t *testing.T) {
	params := map[string]string{}
	applyRuntimeParams(params, config.PostgresConfig{MaxParallelWorkersPerGather: intPtr(2)})
	if got := params["max_parallel_workers_per_gather"]; got != "2" {
		t.Fatalf("max_parallel_workers_per_gather = %q, want \"2\"", got)
	}
}

func TestClientCheckInterval(t *testing.T) {
	cases := []struct {
		name   string
		cfg    *config.Config
		wantMS int
		wantOK bool
	}{
		{"no config", nil, 5000, true},
		{"unset", &config.Config{}, 5000, true},
		{"configured", &config.Config{Postgres: config.PostgresConfig{ClientConnectionCheckInterval: intPtr(1000)}}, 1000, true},
		{"zero", &config.Config{Postgres: config.PostgresConfig{ClientConnectionCheckInterval: intPtr(0)}}, 0, true},
		{"negative leaves the server setting", &config.Config{Postgres: config.PostgresConfig{ClientConnectionCheckInterval: intPtr(-1)}}, -1, false},
	}
	for _, tc := range cases {
		ms, ok := clientCheckInterval(tc.cfg)
		if ms != tc.wantMS || ok != tc.wantOK {
			t.Errorf("%s: got (%d, %v), want (%d, %v)", tc.name, ms, ok, tc.wantMS, tc.wantOK)
		}
	}
}

// The setting must not be in the startup packet: an old server, or a
// server on a platform without the check, would refuse the connection.
func TestCheckIntervalNotInStartupPacket(t *testing.T) {
	params := map[string]string{}
	applyRuntimeParams(params, config.PostgresConfig{ClientConnectionCheckInterval: intPtr(1000)})
	if _, ok := params["client_connection_check_interval"]; ok {
		t.Fatalf("client_connection_check_interval is in the startup parameters")
	}
}

func TestConfigsGetAfterConnectHook(t *testing.T) {
	poolCfg, err := pgxpool.ParseConfig("host=localhost dbname=x user=x sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	applyPostgresPoolConfig(poolCfg)
	if poolCfg.ConnConfig.Config.AfterConnect == nil {
		t.Error("pool config has no AfterConnect hook")
	}

	replCfg, err := pgconn.ParseConfig("host=localhost dbname=x user=x sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	applyPostgresPgconnConfig(replCfg)
	if replCfg.AfterConnect == nil {
		t.Error("replication config has no AfterConnect hook")
	}
}

func TestSetClientCheckIntervalKeepsPreviousHook(t *testing.T) {
	cfg, err := pgconn.ParseConfig("host=localhost dbname=x user=x sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	prevErr := errors.New("previous hook")
	cfg.AfterConnect = func(context.Context, *pgconn.PgConn) error { return prevErr }
	setClientCheckInterval(cfg, 5000)
	if err := cfg.AfterConnect(context.Background(), nil); !errors.Is(err, prevErr) {
		t.Fatalf("got %v, want the error of the previous hook", err)
	}
}

// A negative value means "leave the server's own setting alone".
func TestRuntimeParamsNegativeLeavesServerDefault(t *testing.T) {
	params := map[string]string{}
	applyRuntimeParams(params, config.PostgresConfig{MaxParallelWorkersPerGather: intPtr(-1)})
	if _, ok := params["max_parallel_workers_per_gather"]; ok {
		t.Fatalf("expected max_parallel_workers_per_gather to be unset, got %q", params["max_parallel_workers_per_gather"])
	}
}
