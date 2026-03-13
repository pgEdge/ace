package server

import (
	"testing"

	"github.com/pgedge/ace/pkg/config"
)

// TestResolversPickUpReloadedConfig verifies that the handler resolver
// functions read from the live global config (via config.Get) rather than
// a stale snapshot, so that a SIGHUP-triggered config.Set takes effect
// for subsequent API requests.
func TestResolversPickUpReloadedConfig(t *testing.T) {
	original := config.Get()
	t.Cleanup(func() {
		if original != nil {
			config.Set(original)
		} else {
			config.Set(&config.Config{})
		}
	})

	// Set initial config.
	config.Set(&config.Config{
		TableDiff: config.DiffConfig{
			DiffBlockSize:     5000,
			ConcurrencyFactor: 0.25,
			CompareUnitSize:   2000,
			MaxDiffRows:       100,
		},
		Server: config.ServerConfig{
			TaskStorePath: "/tmp/old-tasks.db",
		},
	})

	s := &APIServer{}

	// Verify resolvers return initial values (0 = "use config default").
	if got := s.resolveBlockSize(0); got != 5000 {
		t.Errorf("resolveBlockSize: got %d, want 5000", got)
	}
	if got := s.resolveConcurrency(0); got != 0.25 {
		t.Errorf("resolveConcurrency: got %f, want 0.25", got)
	}
	if got := s.resolveCompareUnitSize(0); got != 2000 {
		t.Errorf("resolveCompareUnitSize: got %d, want 2000", got)
	}
	if got := s.resolveMaxDiffRows(0); got != 100 {
		t.Errorf("resolveMaxDiffRows: got %d, want 100", got)
	}
	if got := config.Get().Server.TaskStorePath; got != "/tmp/old-tasks.db" {
		t.Errorf("TaskStorePath: got %q, want /tmp/old-tasks.db", got)
	}

	// Simulate SIGHUP: swap in new config.
	config.Set(&config.Config{
		TableDiff: config.DiffConfig{
			DiffBlockSize:     9999,
			ConcurrencyFactor: 0.75,
			CompareUnitSize:   4000,
			MaxDiffRows:       500,
		},
		Server: config.ServerConfig{
			TaskStorePath: "/tmp/new-tasks.db",
		},
	})

	// Verify resolvers now return the reloaded values.
	if got := s.resolveBlockSize(0); got != 9999 {
		t.Errorf("after reload resolveBlockSize: got %d, want 9999", got)
	}
	if got := s.resolveConcurrency(0); got != 0.75 {
		t.Errorf("after reload resolveConcurrency: got %f, want 0.75", got)
	}
	if got := s.resolveCompareUnitSize(0); got != 4000 {
		t.Errorf("after reload resolveCompareUnitSize: got %d, want 4000", got)
	}
	if got := s.resolveMaxDiffRows(0); got != 500 {
		t.Errorf("after reload resolveMaxDiffRows: got %d, want 500", got)
	}
	if got := config.Get().Server.TaskStorePath; got != "/tmp/new-tasks.db" {
		t.Errorf("after reload TaskStorePath: got %q, want /tmp/new-tasks.db", got)
	}
}
