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

package auth

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/config"
)

// explainNode is the part of an EXPLAIN (FORMAT JSON) node that the test
// checks.
type explainNode struct {
	NodeType     string        `json:"Node Type"`
	RelationName string        `json:"Relation Name"`
	Plans        []explainNode `json:"Plans"`
}

// hasSeqScan reports whether the plan tree n reads table with a sequential
// scan.
func hasSeqScan(n explainNode, table string) bool {
	if n.NodeType == "Seq Scan" && n.RelationName == table {
		return true
	}
	for _, c := range n.Plans {
		if hasSeqScan(c, table) {
			return true
		}
	}
	return false
}

// TestOpenBlockPlanWithoutStatistics checks the plan of the first and the
// last block of a table with no statistics. These blocks have one bound, and
// with no statistics the planner estimates the range as a third of the table.
// With the session settings of ACE the block is read through an index; with
// the server setting of enable_seqscan the planner reads the whole table. A
// small work_mem makes the bitmap of a third of the table lossy, as on a
// large table, so a bitmap scan costs more than a sequential scan.
func TestOpenBlockPlanWithoutStatistics(t *testing.T) {
	dsn := os.Getenv("ACE_TEST_DSN")
	if dsn == "" {
		t.Skip("ACE_TEST_DSN is not set")
	}
	ctx := context.Background()
	setup, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(setup.Close)
	for _, sql := range []string{
		`DROP SCHEMA IF EXISTS ace_planner_test CASCADE`,
		`CREATE SCHEMA ace_planner_test`,
		`CREATE TABLE ace_planner_test.t (id bigint PRIMARY KEY, a text, b text, n int)
			WITH (autovacuum_enabled = false)`,
		`INSERT INTO ace_planner_test.t
			SELECT g, md5(g::text) || md5((g + 1)::text), md5((g * 7)::text), g % 1000
			FROM generate_series(1, 200000) g`,
	} {
		if _, err := setup.Exec(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	t.Cleanup(func() {
		if _, err := setup.Exec(context.Background(), `DROP SCHEMA IF EXISTS ace_planner_test CASCADE`); err != nil {
			t.Errorf("drop schema ace_planner_test: %v", err)
		}
	})

	key := []string{"id"}
	cols := []string{"id", "a", "b", "n"}
	castTypes, err := queries.GetCastTypes(ctx, setup, "ace_planner_test", "t")
	if err != nil {
		t.Fatal(err)
	}
	blocks := map[string]queries.KeyRange{
		"first block": {Upper: []string{"1000"}},
		"last block":  {Lower: []string{"199000"}},
	}

	for _, tc := range []struct {
		name        string
		cfg         config.PostgresConfig
		wantSeqScan bool
	}{
		{"ACE settings", config.PostgresConfig{}, false},
		{"server setting of enable_seqscan", config.PostgresConfig{EnableSeqscan: true}, true},
	} {
		poolCfg, err := pgxpool.ParseConfig(dsn)
		if err != nil {
			t.Fatal(err)
		}
		// A role or database setting of the test server must not decide
		// the result: the case with the server setting keeps this "on",
		// and the settings of ACE replace it with "off".
		poolCfg.ConnConfig.RuntimeParams["enable_seqscan"] = "on"
		applyRuntimeParams(poolCfg.ConnConfig.RuntimeParams, tc.cfg)
		poolCfg.ConnConfig.RuntimeParams["work_mem"] = "64kB"
		pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
		if err != nil {
			t.Fatal(err)
		}
		for name, r := range blocks {
			for _, mode := range []string{"TD_BLOCK_HASH", "MTREE_LEAF_HASH"} {
				where, args, err := queries.BlockWhere(key, castTypes, r, "")
				if err != nil {
					t.Fatal(err)
				}
				sql, err := queries.BlockHashSQL("ace_planner_test", "t", mode, where, cols)
				if err != nil {
					t.Fatal(err)
				}
				var raw []byte
				if err := pool.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, args...).Scan(&raw); err != nil {
					t.Fatalf("%s: %v", sql, err)
				}
				var out []struct {
					Plan explainNode `json:"Plan"`
				}
				if err := json.Unmarshal(raw, &out); err != nil {
					t.Fatal(err)
				}
				if got := len(out) > 0 && hasSeqScan(out[0].Plan, "t"); got != tc.wantSeqScan {
					t.Errorf("%s, %s, %s: sequential scan of the table = %v, want %v\n%s",
						tc.name, name, mode, got, tc.wantSeqScan, raw)
				}
			}
		}
		pool.Close()
	}
}
