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

package topology

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/types"
)

// FetchSpockNodeConfig reads one node's Spock subscriptions and replication
// set membership, and surfaces hints for common misconfigurations: a
// subscription whose origin node could not be resolved, a subscription with
// no replication sets attached, or replication sets that exist but contain
// no tables (usually because those tables have no primary key).
//
// This is the single place that reads Spock's topology catalogs, so anything
// needing "who replicates from whom" can call it instead of re-deriving it.
func FetchSpockNodeConfig(ctx context.Context, pool *pgxpool.Pool, nodeName string) (NodeConfig, error) {
	config := NodeConfig{NodeName: nodeName, Hints: []string{}}

	nodeInfos, err := queries.GetSpockNodeAndSubInfo(ctx, pool)
	if err != nil {
		return config, fmt.Errorf("querying spock.node and spock.subscription on node %s failed: %w", nodeName, err)
	}

	if len(nodeInfos) > 0 {
		config.NodeName = nodeInfos[0].NodeName
		for _, ni := range nodeInfos {
			sub := types.SpockSubscription{}
			if ni.SubName != "" {
				sub.SubName = ni.SubName
				sub.ProviderNode = ni.SubOriginName
				sub.SubEnabled = ni.SubEnabled
				sub.ReplicationSets = ni.SubReplicationSets
				if ni.SubOriginName == "" {
					hint := fmt.Sprintf("Subscription '%s' has an unresolved origin node; its reciprocal peer cannot be determined and it may be reported below as a missing subscription.", sub.SubName)
					if !utils.Contains(config.Hints, hint) {
						config.Hints = append(config.Hints, hint)
					}
				}
				if len(ni.SubReplicationSets) == 0 {
					hint := fmt.Sprintf("Subscription '%s' has no replication sets.", sub.SubName)
					if !utils.Contains(config.Hints, hint) {
						config.Hints = append(config.Hints, hint)
					}
				}
				config.Subscriptions = append(config.Subscriptions, sub)
			}
			// A row with an empty SubName is a node that has no
			// subscription at all (the query's left join produced one row
			// of nulls rather than none). Appending sub here would add an
			// empty types.SpockSubscription{} to the list for every such
			// node, which is not a subscription to report or match against.
		}
	} else {
		config.Hints = append(config.Hints, "Hint: No subscriptions have been created on this node.")
	}

	repRows, err := queries.GetSpockRepSetInfo(ctx, pool)
	if err != nil {
		return config, fmt.Errorf("querying spock.tables on node %s failed: %w", nodeName, err)
	}
	config.RepSetInfo = repRows

	var tablesInRepSets []string
	for _, rs := range repRows {
		if rs.SetName != "" {
			tablesInRepSets = append(tablesInRepSets, rs.RelName...)
		}
	}
	if len(repRows) > 0 && len(tablesInRepSets) == 0 {
		config.Hints = append(config.Hints, "Hint: Tables not in replication set might not have primary keys, or you need to run repset-add-table.")
	}

	return config, nil
}

// SubscriptionsByProvider indexes subscriptions by the node they replicate
// from. Matching reciprocal subscriptions must go by provider-node identity,
// not by subscription name, because users are free to rename subscriptions.
func SubscriptionsByProvider(subs []types.SpockSubscription) map[string]types.SpockSubscription {
	byProvider := make(map[string]types.SpockSubscription, len(subs))
	for _, s := range subs {
		if s.ProviderNode != "" {
			byProvider[s.ProviderNode] = s
		}
	}
	return byProvider
}
