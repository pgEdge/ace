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
	"strings"
	"testing"

	cli "github.com/urfave/cli/v2"

	"github.com/pgedge/ace/pkg/config"
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

func TestResolveClusterArgUnexpectedArgs(t *testing.T) {
	t.Cleanup(func() { config.Cfg = nil })
	config.Cfg = &config.Config{DefaultCluster: "default"}

	if _, _, err := resolveClusterArg("table-diff", "<table>", "[cluster] <table>", 1, []string{"c1", "t1", "extra"}); err == nil {
		t.Fatalf("expected error for too many arguments")
	}
}

// ---------------------------------------------------------------------------
// applyInterspersedFlags / filteredPositionalArgs tests
// ---------------------------------------------------------------------------

// flagTestResult holds what the action captured during a test run.
type flagTestResult struct {
	positional []string
	strings    map[string]string
	bools      map[string]bool
}

// newFlagTestApp builds a minimal urfave/cli app with a single "cmd" command.
// The Before hook calls applyInterspersedFlags; the action records positional
// args and flag values into result.
func newFlagTestApp(flags []cli.Flag, result *flagTestResult) *cli.App {
	return &cli.App{
		Name:           "ace",
		ExitErrHandler: func(_ *cli.Context, _ error) {},
		Commands: []*cli.Command{{
			Name:  "cmd",
			Flags: flags,
			Before: func(ctx *cli.Context) error {
				return applyInterspersedFlags(ctx)
			},
			Action: func(ctx *cli.Context) error {
				result.positional = filteredPositionalArgs(ctx)
				for _, f := range flags {
					name := f.Names()[0]
					switch f.(type) {
					case *cli.BoolFlag:
						result.bools[name] = ctx.Bool(name)
					default:
						result.strings[name] = ctx.String(name)
					}
				}
				return nil
			},
		}},
	}
}

// runFlags runs the test app with "ace cmd <args...>" and returns the result
// and any error.
func runFlags(flags []cli.Flag, args []string) (*flagTestResult, error) {
	result := &flagTestResult{
		strings: make(map[string]string),
		bools:   make(map[string]bool),
	}
	app := newFlagTestApp(flags, result)
	err := app.Run(append([]string{"ace", "cmd"}, args...))
	return result, err
}

// commonFlags mirrors a realistic set of ACE flags used across many commands.
var commonTestFlags = []cli.Flag{
	&cli.StringFlag{Name: "dbname", Aliases: []string{"d"}},
	&cli.StringFlag{Name: "nodes", Aliases: []string{"n"}},
	&cli.StringFlag{Name: "output", Aliases: []string{"o"}},
	&cli.BoolFlag{Name: "debug", Aliases: []string{"v"}},
	&cli.BoolFlag{Name: "quiet", Aliases: []string{"q"}},
	&cli.IntFlag{Name: "block-size", Aliases: []string{"b"}},
}

// TestInterspersedFlags covers all QA report scenarios for applyInterspersedFlags
// and filteredPositionalArgs.
func TestInterspersedFlags(t *testing.T) {
	tests := []struct {
		tc              string // QA report TC ID
		args            []string
		flags           []cli.Flag
		wantPositional  []string
		wantStrings     map[string]string
		wantBools       map[string]bool
		wantErrContains string // non-empty → expect error containing this
		wantHelp        bool   // true → expect exit code 0 (help output)
	}{
		// --- TC-1: Baseline (flags before args) ---
		{
			tc:             "TC-1a flags-before-arg baseline",
			args:           []string{"--dbname", "testdb", "public.orders"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
		},

		// --- TC-2: String / bool flags after positional arg ---
		{
			tc:             "TC-2a string flag after arg",
			args:           []string{"public.orders", "--dbname", "testdb"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
		},
		{
			tc:             "TC-2b bool flag after arg",
			args:           []string{"public.orders", "--debug"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantBools:      map[string]bool{"debug": true},
		},
		{
			tc:             "TC-2c multiple flags after arg",
			args:           []string{"public.orders", "--dbname", "testdb", "--nodes", "n1,n2", "--output", "json"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb", "nodes": "n1,n2", "output": "json"},
		},

		// --- TC-3: Mixed flags before and after ---
		{
			tc:             "TC-3a mixed some before some after",
			args:           []string{"--dbname", "testdb", "public.orders", "--output", "json", "--debug"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb", "output": "json"},
			wantBools:      map[string]bool{"debug": true},
		},
		{
			tc:             "TC-3b cluster and table with flag after",
			args:           []string{"mycluster", "public.orders", "--dbname", "testdb"},
			flags:          commonTestFlags,
			wantPositional: []string{"mycluster", "public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
		},

		// --- TC-4: --key=value form ---
		{
			tc:             "TC-4a --key=value string flag after arg",
			args:           []string{"public.orders", "--dbname=testdb"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
		},
		{
			tc:             "TC-4b --key=value output flag after arg",
			args:           []string{"public.orders", "--output=json"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"output": "json"},
		},
		{
			tc:             "TC-4c --key=value bool flag after arg",
			args:           []string{"public.orders", "--debug=true"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantBools:      map[string]bool{"debug": true},
		},

		// --- TC-5: Unknown flags ---
		{
			tc:              "TC-5a unknown flag after arg",
			args:            []string{"public.orders", "--foobar"},
			flags:           commonTestFlags,
			wantErrContains: "flag provided but not defined: --foobar",
		},
		{
			tc:              "TC-5b unknown flag with value after arg",
			args:            []string{"public.orders", "--nonexistent", "value"},
			flags:           commonTestFlags,
			wantErrContains: "flag provided but not defined: --nonexistent",
		},

		// --- TC-6: Flag missing its value ---
		{
			tc:              "TC-6a flag with no value at end of args",
			args:            []string{"public.orders", "--dbname"},
			flags:           commonTestFlags,
			wantErrContains: "flag needs an argument: --dbname",
		},
		{
			tc:              "TC-6b flag with no value when next token is a flag",
			args:            []string{"public.orders", "--output", "--debug"},
			flags:           commonTestFlags,
			wantErrContains: "flag needs an argument: --output",
		},

		// --- TC-7: Help flags after positional arg ---
		{
			tc:       "TC-7a --help after positional arg",
			args:     []string{"public.orders", "--help"},
			flags:    commonTestFlags,
			wantHelp: true,
		},
		{
			tc:       "TC-7b -h after positional arg",
			args:     []string{"public.orders", "-h"},
			flags:    commonTestFlags,
			wantHelp: true,
		},

		// --- TC-8: -- separator ---
		{
			// urfave/cli strips -- before args reach our code; tokens after --
			// that look like flags are passed through as positional args by the
			// framework, not re-parsed by applyInterspersedFlags.
			tc:             "TC-8a -- separator preserves subsequent tokens as positional",
			args:           []string{"public.orders", "--", "--dbname", "testdb"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders", "--dbname", "testdb"},
		},

		// --- TC-10: Short flag aliases after positional arg ---
		{
			tc:             "TC-10a short alias -d after arg",
			args:           []string{"public.orders", "-d", "testdb"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
		},
		{
			tc:             "TC-10b short alias -v (bool) after arg",
			args:           []string{"public.orders", "-v"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantBools:      map[string]bool{"debug": true},
		},
		{
			tc:             "TC-10c short alias -o after arg",
			args:           []string{"public.orders", "-o", "json"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"output": "json"},
		},
		{
			tc:             "TC-10d short alias -q (bool) after arg",
			args:           []string{"public.orders", "-q"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantBools:      map[string]bool{"quiet": true},
		},
		{
			tc:             "TC-10e multiple short aliases after arg",
			args:           []string{"public.orders", "-d", "testdb", "-v", "-q"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
			wantBools:      map[string]bool{"debug": true, "quiet": true},
		},

		// --- TC-11: Flag tokens excluded from positional arg count ---
		{
			tc:             "TC-11f flags must not inflate positional arg count",
			args:           []string{"--dbname", "testdb", "public.orders", "--block-size", "1000"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb"},
		},
		{
			tc:             "TC-11 multiple positional args with flags interspersed",
			args:           []string{"mycluster", "--dbname", "testdb", "public.orders", "--output", "json"},
			flags:          commonTestFlags,
			wantPositional: []string{"mycluster", "public.orders"},
			wantStrings:    map[string]string{"dbname": "testdb", "output": "json"},
		},

		// --- Finding #1 (fixed): --diff-file after positional arg now works ---
		// Required: true was removed from --diff-file; validation moved to
		// application layer (Validate()), so the flag is accepted after positional.
		{
			tc:             "Finding-1 diff-file flag after positional arg",
			args:           []string{"public.orders", "--diff-file", "/tmp/diff.json"},
			flags:          []cli.Flag{&cli.StringFlag{Name: "diff-file", Aliases: []string{"f"}}},
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"diff-file": "/tmp/diff.json"},
		},

		// --- Finding #3: Negative numbers treated as flags (known limitation) ---
		// Workaround: use --flag=value form.
		{
			tc:              "Finding-3 negative number as flag value rejected",
			args:            []string{"public.orders", "--block-size", "-100"},
			flags:           commonTestFlags,
			wantErrContains: "flag needs an argument: --block-size",
		},
		{
			tc:             "Finding-3 workaround: --flag=negative-value accepted",
			args:           []string{"public.orders", "--dbname=-special"},
			flags:          commonTestFlags,
			wantPositional: []string{"public.orders"},
			wantStrings:    map[string]string{"dbname": "-special"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.tc, func(t *testing.T) {
			result, err := runFlags(tt.flags, tt.args)

			// --- Check help flag cases (exit code 0) ---
			if tt.wantHelp {
				exitErr, ok := err.(cli.ExitCoder)
				if !ok {
					t.Fatalf("expected exit error with code 0 (help), got: %v", err)
				}
				if exitErr.ExitCode() != 0 {
					t.Errorf("expected exit code 0 (help), got %d", exitErr.ExitCode())
				}
				return
			}

			// --- Check error cases ---
			if tt.wantErrContains != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErrContains)
				}
				if !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Errorf("expected error %q, got %q", tt.wantErrContains, err.Error())
				}
				return
			}

			// --- Check success cases ---
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Check positional args.
			if tt.wantPositional != nil {
				if len(result.positional) != len(tt.wantPositional) {
					t.Errorf("positional args: got %v, want %v", result.positional, tt.wantPositional)
				} else {
					for i, want := range tt.wantPositional {
						if result.positional[i] != want {
							t.Errorf("positional[%d]: got %q, want %q", i, result.positional[i], want)
						}
					}
				}
			}

			// Check string flag values.
			for name, want := range tt.wantStrings {
				if got := result.strings[name]; got != want {
					t.Errorf("flag --%s: got %q, want %q", name, got, want)
				}
			}

			// Check bool flag values.
			for name, want := range tt.wantBools {
				if got := result.bools[name]; got != want {
					t.Errorf("flag --%s: got %v, want %v", name, got, want)
				}
			}
		})
	}
}
