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
	"strings"
	"testing"
)

// The types belong to a node, not to its address: two nodes can be two
// databases of one server, with the same host and port and other types. A
// numeric port in the description of a node does not matter either.
func TestNodeCastTypesByNodeName(t *testing.T) {
	task := &TableDiffTask{}
	task.ClusterNodes = []map[string]any{
		{"Name": "n1", "PublicIP": "127.0.0.1", "Port": "5432"},
		{"Name": "n2", "PublicIP": "127.0.0.1", "Port": float64(5432)},
	}
	task.castTypes = map[string]map[string]string{
		"n1": {"id": "integer"},
		"n2": {"id": "bigint"},
	}
	for node, want := range map[string]string{"n1": "integer", "n2": "bigint"} {
		got, err := task.nodeCastTypes(node)
		if err != nil {
			t.Fatalf("%s: %v", node, err)
		}
		if got["id"] != want {
			t.Errorf("%s: got %s, want %s", node, got["id"], want)
		}
	}
	if _, err := task.nodeCastTypes("n3"); err == nil || !strings.Contains(err.Error(), "node n3") {
		t.Errorf("an unknown node: got %v, want an error that names the node", err)
	}
}
