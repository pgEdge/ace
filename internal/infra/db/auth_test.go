package auth

import (
	"testing"

	"github.com/pgedge/ace/pkg/config"
)

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

// A negative value means "leave the server's own setting alone".
func TestRuntimeParamsNegativeLeavesServerDefault(t *testing.T) {
	params := map[string]string{}
	applyRuntimeParams(params, config.PostgresConfig{MaxParallelWorkersPerGather: intPtr(-1)})
	if _, ok := params["max_parallel_workers_per_gather"]; ok {
		t.Fatalf("expected max_parallel_workers_per_gather to be unset, got %q", params["max_parallel_workers_per_gather"])
	}
}
