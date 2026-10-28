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

package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func parse(t *testing.T, text string) *Config {
	t.Helper()
	var c Config
	if err := yaml.Unmarshal([]byte(text), &c); err != nil {
		t.Fatalf("parse %q: %v", text, err)
	}
	return &c
}

func TestConnectionsFor(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want map[string]int // "<cluster>.<node>" -> N
	}{
		{"nothing set", ``, map[string]int{"demo.n1": DefaultMaxConnections}},
		{"old name", "table_diff:\n  max_connections: 6\n", map[string]int{"demo.n1": 6}},
		{
			"default and node",
			"max_connections:\n  default: 3\n  nodes:\n    demo.n1: 8\n",
			map[string]int{"demo.n1": 8, "demo.n2": 3, "other.n1": 3},
		},
		{
			"default wins over old name",
			"table_diff:\n  max_connections: 6\nmax_connections:\n  default: 2\n",
			map[string]int{"demo.n1": 2},
		},
		{
			"node without default uses old name",
			"table_diff:\n  max_connections: 6\nmax_connections:\n  nodes:\n    demo.n1: 8\n",
			map[string]int{"demo.n1": 8, "demo.n2": 6},
		},
		{"single number", "max_connections: 5\n", map[string]int{"demo.n1": 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := parse(t, tt.yaml)
			for key, want := range tt.want {
				var cluster, node string
				for i := range key {
					if key[i] == '.' {
						cluster, node = key[:i], key[i+1:]
						break
					}
				}
				got, set := c.ConnectionsFor(cluster, node)
				if got != want {
					t.Errorf("ConnectionsFor(%q, %q) = %d, want %d", cluster, node, got, want)
				}
				// Only the case with nothing in the file falls back to the
				// built-in default.
				if wantSet := tt.yaml != ""; set != wantSet {
					t.Errorf("ConnectionsFor(%q, %q): set = %v, want %v", cluster, node, set, wantSet)
				}
			}
		})
	}

	var nilCfg *Config
	if got, set := nilCfg.ConnectionsFor("demo", "n1"); got != DefaultMaxConnections || set {
		t.Errorf("nil config: ConnectionsFor = %d, %v; want %d, false", got, set, DefaultMaxConnections)
	}
}

func TestCheckConnectionLimits(t *testing.T) {
	for _, text := range []string{
		"max_connections:\n  default: -1\n",
		"max_connections:\n  nodes:\n    demo.n1: -2\n",
		"table_diff:\n  max_connections: -1\n",
	} {
		if err := parse(t, text).CheckConnectionLimits(); err == nil {
			t.Errorf("%q: no error for a negative value", text)
		}
	}
	if err := parse(t, "max_connections:\n  default: 4\n  nodes:\n    demo.n1: 0\n").CheckConnectionLimits(); err != nil {
		t.Errorf("valid limits: %v", err)
	}
}
