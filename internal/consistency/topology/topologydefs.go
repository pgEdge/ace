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

// Package topology answers one question: which node replicates from which,
// over which subscription, carrying which replication sets. Keeping this
// separate from table structure lets structure comparison run without a
// working replication topology, and lets topology be reused by anything
// that needs to know "who talks to whom".
package topology

import "github.com/pgedge/ace/pkg/types"

// NodeConfig aggregates one node's replication topology: its subscriptions,
// the replication sets it knows about, and hints about incomplete
// configuration (unresolved origins, empty replication sets, tables outside
// any replication set).
type NodeConfig struct {
	NodeName      string                    `json:"node_name"`
	Subscriptions []types.SpockSubscription `json:"subscriptions"`
	RepSetInfo    []types.SpockRepSetInfo   `json:"rep_set_info"`
	Hints         []string                  `json:"hints"`
}
