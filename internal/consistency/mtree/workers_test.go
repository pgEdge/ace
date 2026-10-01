package mtree

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/pkg/config"
)

// The ratio is a plain share of the host's cores. Before this the build
// path doubled it, so -m 0.1 on 14 cores started 3 workers instead of 1.
// max_connections caps the pool per node, and one connection of that pool
// is held by the transaction that owns the tree, so workers get one fewer.
func TestWorkerCountIsRatioOfCores(t *testing.T) {
	cases := []struct {
		name     string
		cpus     int
		ratio    float64
		maxConns int
		expect   int
	}{
		{"ratio 0.1 on 14 cores gives 1", 14, 0.1, 0, 1},
		{"ratio 0.5 on 16 cores gives 8", 16, 0.5, 0, 8},
		{"ratio 1.0 on 4 cores gives 4", 4, 1.0, 0, 4},
		{"ratio 0 still gives 1 worker", 8, 0, 0, 1},
		{"rounds like table-diff: 0.5 on 3 cores gives 2", 3, 0.5, 0, 2},
		{"rounds down below the half: 0.1 on 14 cores stays 1", 14, 0.1, 0, 1},
		{"max_connections 3 leaves 2 workers", 16, 0.5, 3, 2},
		{"max_connections above the derived count does not raise it", 16, 0.5, 32, 8},
		{"max_connections 2 on a big host gives 1 worker", 64, 1.0, 2, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := workerCount(tc.cpus, tc.ratio, tc.maxConns); got != tc.expect {
				t.Fatalf("workerCount(%d, %v, %d) = %d, want %d", tc.cpus, tc.ratio, tc.maxConns, got, tc.expect)
			}
		})
	}
}

const wantMaxConnsErr = "max_connections must be >= 2 for mtree commands (one connection holds the tree transaction), or 0 to derive from max_cpu_ratio"

func TestValidateRejectsBadMaxConnections(t *testing.T) {
	for _, v := range []int{-1, 1} {
		m := &MerkleTreeTask{MaxCpuRatio: 0.5, MaxConnections: v}
		err := m.validateWorkerLimits(&config.Config{})
		if err == nil || err.Error() != wantMaxConnsErr {
			t.Fatalf("MaxConnections=%d: expected validation error, got %v", v, err)
		}
	}
}

// A flag value of 0 means "use mtree.max_connections from ace.yaml"; an
// explicit flag value wins over the config.
func TestMaxConnectionsFallsBackToConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.MTree.MaxConnections = 3

	m := &MerkleTreeTask{MaxCpuRatio: 0.5}
	if err := m.validateWorkerLimits(cfg); err != nil {
		t.Fatal(err)
	}
	if m.MaxConnections != 3 {
		t.Fatalf("MaxConnections = %d, want 3 from config", m.MaxConnections)
	}

	m = &MerkleTreeTask{MaxCpuRatio: 0.5, MaxConnections: 2}
	if err := m.validateWorkerLimits(cfg); err != nil {
		t.Fatal(err)
	}
	if m.MaxConnections != 2 {
		t.Fatalf("MaxConnections = %d, want explicit 2 to win over config", m.MaxConnections)
	}
}

// A bad mtree.max_connections in ace.yaml is a typo, not a request, and
// must not be silently ignored.
func TestValidateRejectsBadMaxConnectionsFromConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.MTree.MaxConnections = 1
	m := &MerkleTreeTask{MaxCpuRatio: 0.5}
	err := m.validateWorkerLimits(cfg)
	if err == nil || err.Error() != wantMaxConnsErr {
		t.Fatalf("expected validation error from config, got %v", err)
	}
}

// Every pool the mtree commands open, not just the build pool, must honour
// the cap; otherwise update and diff could still exceed it.
func TestConnOptsCarryMaxConnections(t *testing.T) {
	m := &MerkleTreeTask{MaxConnections: 3, ClientRole: "app"}
	if got := m.connOpts().PoolSize; got != 3 {
		t.Fatalf("connOpts().PoolSize = %d, want 3", got)
	}
	if got := m.userConnOpts().PoolSize; got != 3 {
		t.Fatalf("userConnOpts().PoolSize = %d, want 3", got)
	}
	if got := m.userConnOpts().Role; got != "app" {
		t.Fatalf("userConnOpts().Role = %q, want app", got)
	}
}

// The diff opens one pool per node and every phase shares it: traversal,
// range comparison, and the stale-block refresh. So the per-node connection
// count is the cap when one is set, and otherwise one connection per compare
// worker plus one for the transaction that the refresh holds.
func TestDiffPoolSizeIsTheCapOrWorkersPlusOne(t *testing.T) {
	cases := []struct {
		name     string
		workers  int
		maxConns int
		expect   int
	}{
		{"cap set: the pool is the cap", 3, 4, 4},
		{"no cap: one per worker plus one", 8, 0, 9},
		{"one worker and no cap: two connections", 1, 0, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := diffPoolSize(tc.workers, tc.maxConns); got != tc.expect {
				t.Fatalf("diffPoolSize(%d, %d) = %d, want %d", tc.workers, tc.maxConns, got, tc.expect)
			}
		})
	}
}

// Compare workers do not open pools of their own. They use the per-node
// pools the diff opened, so a node without a pool is a lost work item for
// that pair, not a reason to dial the database.
func TestCompareRangesWithoutPoolMarksPairIncomplete(t *testing.T) {
	m := &MerkleTreeTask{Ctx: context.Background(), MaxCpuRatio: 0.1}
	n1 := map[string]any{"Name": "n1"}
	n2 := map[string]any{"Name": "n2"}
	items := []CompareRangesWorkItem{{Node1: n1, Node2: n2, Ranges: [][2][]any{{{int64(1)}, {int64(2)}}}}}

	m.CompareRanges(items, map[string]*pgxpool.Pool{})

	if got := m.incompletePairs(); !slices.Equal(got, []string{"n1/n2"}) {
		t.Fatalf("incompletePairs() = %v, want [n1/n2]", got)
	}
}
