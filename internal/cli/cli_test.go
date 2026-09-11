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

package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pgedge/ace/pkg/config"
	"github.com/urfave/cli/v3"
)

// ---------------------------------------------------------------------------
// resolveClusterArg tests
// ---------------------------------------------------------------------------

func TestResolveClusterArgWithExplicitCluster(t *testing.T) {
	cluster, rest, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, []string{"my-cluster", "public.tbl"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cluster != "my-cluster" {
		t.Fatalf("expected cluster 'my-cluster', got %q", cluster)
	}
	if len(rest) != 1 || rest[0] != "public.tbl" {
		t.Fatalf("unexpected rest args: %v", rest)
	}
}

func TestResolveClusterArgUsesDefault(t *testing.T) {
	t.Cleanup(func() { config.Cfg = nil })
	config.Cfg = &config.Config{DefaultCluster: "default"}

	cluster, rest, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, []string{"public.tbl"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cluster != "default" {
		t.Fatalf("expected default cluster 'default', got %q", cluster)
	}
	if len(rest) != 1 || rest[0] != "public.tbl" {
		t.Fatalf("unexpected rest args: %v", rest)
	}
}

func TestResolveClusterArgErrorsWithoutDefault(t *testing.T) {
	t.Cleanup(func() { config.Cfg = nil })
	config.Cfg = &config.Config{}

	if _, _, err := resolveClusterArg("table-rerun", "", "[cluster]", 0, []string{}); err == nil {
		t.Fatalf("expected error when no cluster and no default configured")
	}
}

// A lone positional is consumed as the required entity (repset), not the
// cluster. The error must name the consumed argument so the user sees why
// "cluster name is required" fired, instead of thinking their argument was
// ignored.
func TestResolveClusterArgSinglePositionalRevealsConsumedArg(t *testing.T) {
	t.Cleanup(func() { config.Cfg = nil })
	config.Cfg = &config.Config{}

	_, _, err := resolveClusterArg("repset-diff", "<repset>", "[cluster] <repset>", 1, []string{"demo"})
	if err == nil {
		t.Fatalf("expected error for a lone positional with no default_cluster set")
	}
	for _, want := range []string{"demo", "<repset>", "repset-diff"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q missing expected substring %q", err.Error(), want)
		}
	}
}

func TestResolveClusterArgUnexpectedArgs(t *testing.T) {
	t.Cleanup(func() { config.Cfg = nil })
	config.Cfg = &config.Config{DefaultCluster: "default"}

	if _, _, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, []string{"c1", "t1", "extra"}); err == nil {
		t.Fatalf("expected error for too many arguments")
	}
}

// ---------------------------------------------------------------------------
// Interspersed flags (urfave/cli v3 handles natively)
// ---------------------------------------------------------------------------

func TestInterspersedFlags(t *testing.T) {
	tests := []struct {
		name     string
		cliArgs  []string
		wantArgs []string
		wantFlag string
	}{
		{
			name:     "flag before positional arg",
			cliArgs:  []string{"ace", "cmd", "-d", "mydb", "public.t1"},
			wantArgs: []string{"public.t1"},
			wantFlag: "mydb",
		},
		{
			name:     "flag after positional arg",
			cliArgs:  []string{"ace", "cmd", "public.t1", "-d", "mydb"},
			wantArgs: []string{"public.t1"},
			wantFlag: "mydb",
		},
		{
			name:     "flag between positional args",
			cliArgs:  []string{"ace", "cmd", "mycluster", "-d", "mydb", "public.t1"},
			wantArgs: []string{"mycluster", "public.t1"},
			wantFlag: "mydb",
		},
		{
			name:     "long flag after positional arg",
			cliArgs:  []string{"ace", "cmd", "public.t1", "--dbname", "mydb"},
			wantArgs: []string{"public.t1"},
			wantFlag: "mydb",
		},
		{
			name:     "equals form after positional arg",
			cliArgs:  []string{"ace", "cmd", "public.t1", "--dbname=mydb"},
			wantArgs: []string{"public.t1"},
			wantFlag: "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotArgs []string
			var gotFlag string

			app := &cli.Command{
				Name: "ace",
				Commands: []*cli.Command{{
					Name: "cmd",
					Flags: []cli.Flag{
						&cli.StringFlag{Name: "dbname", Aliases: []string{"d"}},
					},
					Action: func(_ context.Context, cmd *cli.Command) error {
						gotArgs = cmd.Args().Slice()
						gotFlag = cmd.String("dbname")
						return nil
					},
				}},
			}

			if err := app.Run(context.Background(), tt.cliArgs); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if gotFlag != tt.wantFlag {
				t.Errorf("dbname flag: got %q, want %q", gotFlag, tt.wantFlag)
			}
			if len(gotArgs) != len(tt.wantArgs) {
				t.Fatalf("args length: got %d (%v), want %d (%v)", len(gotArgs), gotArgs, len(tt.wantArgs), tt.wantArgs)
			}
			for i := range tt.wantArgs {
				if gotArgs[i] != tt.wantArgs[i] {
					t.Errorf("args[%d]: got %q, want %q", i, gotArgs[i], tt.wantArgs[i])
				}
			}
		})
	}
}

// `ace cluster init` writes database credentials, so pg_service.conf must end
// up owner-only even when --force overwrites a world-readable file:
// os.WriteFile's mode is umask-masked, and ignored for an existing file.
func TestClusterInitWritesOwnerOnlyServiceFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pg_service.conf")
	if err := os.WriteFile(path, []byte("stale"), 0o644); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("seed mode: %v", err)
	}

	cmd := &cli.Command{
		Name: "init",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "path", Value: path},
			&cli.BoolFlag{Name: "force", Value: true},
			&cli.BoolFlag{Name: "stdout"},
		},
		Action: ClusterInitCLI,
	}
	if err := cmd.Run(context.Background(), []string{"init"}); err != nil {
		t.Fatalf("cluster init: %v", err)
	}

	st, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if st.Mode().Perm()&0o077 != 0 {
		t.Errorf("%s has mode %v, want owner-only for a credentials file", path, st.Mode().Perm())
	}
}
