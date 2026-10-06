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
