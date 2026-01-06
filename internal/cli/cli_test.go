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
	"testing"

	"github.com/pgedge/ace/pkg/config"
)

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
