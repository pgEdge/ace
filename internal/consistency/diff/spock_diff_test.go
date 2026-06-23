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

package diff

import (
	"testing"

	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/assert"
)

// spockCfg is a small helper to build a node config for comparison tests.
func spockCfg(nodeName string, subs ...types.SpockSubscription) SpockNodeConfig {
	return SpockNodeConfig{NodeName: nodeName, Subscriptions: subs}
}

// Healthy reciprocal pair with matching repsets. Names are deliberately
// arbitrary: matching must rely on the provider node, not a name convention.
func TestCompareSubscriptions_HealthyReciprocalIgnoresNames(t *testing.T) {
	n1 := spockCfg("n1", types.SpockSubscription{
		SubName: "custom_name_a", ProviderNode: "n2", SubEnabled: true,
		ReplicationSets: []string{"default", "default_insert_only"},
	})
	n2 := spockCfg("n2", types.SpockSubscription{
		SubName: "totally_different", ProviderNode: "n1", SubEnabled: true,
		ReplicationSets: []string{"default_insert_only", "default"},
	})

	d := compareSubscriptions(n1, n2)

	assert.Empty(t, d.MissingOnNode1, "n1 subscribes from n2, nothing missing")
	assert.Empty(t, d.MissingOnNode2, "n2 subscribes from n1, nothing missing")
	assert.Empty(t, d.Different, "replication sets match (order-insensitive)")
}

// n1 doesn't subscribe from n2 (one direction missing); n2 does subscribe from n1.
func TestCompareSubscriptions_MissingOneDirection(t *testing.T) {
	n1 := spockCfg("n1") // no subscriptions at all
	n2 := spockCfg("n2", types.SpockSubscription{
		SubName: "s", ProviderNode: "n1", SubEnabled: true,
		ReplicationSets: []string{"default"},
	})

	d := compareSubscriptions(n1, n2)

	assert.Equal(t, []string{"n2"}, d.MissingOnNode1,
		"n1 should be flagged as not subscribing from n2")
	assert.Empty(t, d.MissingOnNode2,
		"n2 correctly subscribes from n1")
}

// Both directions exist but replication sets differ: reported under Different.
func TestCompareSubscriptions_DifferentReplicationSets(t *testing.T) {
	n1 := spockCfg("n1", types.SpockSubscription{
		SubName: "a", ProviderNode: "n2", SubEnabled: true,
		ReplicationSets: []string{"default"},
	})
	n2 := spockCfg("n2", types.SpockSubscription{
		SubName: "b", ProviderNode: "n1", SubEnabled: true,
		ReplicationSets: []string{"default", "extra"},
	})

	d := compareSubscriptions(n1, n2)

	assert.Empty(t, d.MissingOnNode1)
	assert.Empty(t, d.MissingOnNode2)
	assert.Len(t, d.Different, 1, "replication set difference should be reported")
}

// In a 3-node mesh, comparing n1 and n2 ignores their subscriptions with n3.
func TestCompareSubscriptions_IgnoresUnrelatedPeers(t *testing.T) {
	n1 := spockCfg("n1",
		types.SpockSubscription{SubName: "a", ProviderNode: "n2", SubEnabled: true, ReplicationSets: []string{"default"}},
		types.SpockSubscription{SubName: "c", ProviderNode: "n3", SubEnabled: true, ReplicationSets: []string{"default"}},
	)
	n2 := spockCfg("n2",
		types.SpockSubscription{SubName: "b", ProviderNode: "n1", SubEnabled: true, ReplicationSets: []string{"default"}},
		types.SpockSubscription{SubName: "d", ProviderNode: "n3", SubEnabled: true, ReplicationSets: []string{"default"}},
	)

	d := compareSubscriptions(n1, n2)

	assert.Empty(t, d.MissingOnNode1)
	assert.Empty(t, d.MissingOnNode2)
	assert.Empty(t, d.Different)
}
