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

package slicer

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newTable creates ace_slicer_test.<name> with the setup statements and
// drops it at the end of the test.
func newTable(t *testing.T, pool *pgxpool.Pool, name string, setup ...string) {
	t.Helper()
	exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.`+name)
	for _, s := range setup {
		exec(t, pool, s)
	}
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), `DROP TABLE IF EXISTS ace_slicer_test.`+name); err != nil {
			t.Errorf("drop table: %v", err)
		}
	})
}

// runSample cuts the table by a sample and returns the blocks in the order
// of arrival, which is key order.
func runSample(t *testing.T, pool *pgxpool.Pool, cfg Config) ([]Block, *Stats) {
	t.Helper()
	out := make(chan Block, 16)
	var blocks []Block
	done := make(chan struct{})
	go func() {
		for b := range out {
			blocks = append(blocks, b)
		}
		close(done)
	}()
	ctx := context.Background()
	s, err := ReadSample(ctx, pool, cfg)
	if err != nil {
		close(out)
		<-done
		t.Fatalf("ReadSample: %v", err)
	}
	st, err := s.Send(ctx, out)
	close(out)
	<-done
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	return blocks, st
}

// checkSampleBlocks checks that the blocks follow each other with no gap
// and no overlap, that the edge blocks are open, that all blocks together
// hold every row, and that the blocks are close to the block size on
// average and never much larger.
func checkSampleBlocks(t *testing.T, pool *pgxpool.Pool, cfg Config, blocks []Block, st *Stats, maxFactor float64) {
	t.Helper()
	if len(blocks) == 0 {
		t.Fatal("no blocks")
	}
	if blocks[0].Start != nil || blocks[len(blocks)-1].End != nil {
		t.Errorf("edge blocks must be open: first starts at %v, last ends at %v", blocks[0].Start, blocks[len(blocks)-1].End)
	}
	var sum, largest int64
	for i, b := range blocks {
		if b.Seq != i {
			t.Errorf("block %d has Seq %d", i, b.Seq)
		}
		if i > 0 && !reflect.DeepEqual(blocks[i-1].End, b.Start) {
			t.Fatalf("block %d ends at %v but block %d starts at %v", i-1, blocks[i-1].End, i, b.Start)
		}
		n := countRange(t, pool, cfg, b)
		sum += n
		largest = max(largest, n)
	}
	total := countRange(t, pool, cfg, Block{})
	if sum != total {
		t.Errorf("blocks hold %d rows, table has %d", sum, total)
	}
	if st.Blocks != int64(len(blocks)) || st.Method != MethodSample {
		t.Errorf("stats %+v, want %d blocks of the sample", st, len(blocks))
	}
	mean := float64(total) / float64(len(blocks))
	if mean < 0.5*float64(cfg.BlockSize) || mean > 2*float64(cfg.BlockSize) {
		t.Errorf("mean block %.0f rows, block size %d", mean, cfg.BlockSize)
	}
	if float64(largest) > maxFactor*float64(cfg.BlockSize) {
		t.Errorf("largest block %d rows, more than %.0f times block size %d", largest, maxFactor, cfg.BlockSize)
	}
}

// TestSampleBlocksOnDB checks the blocks of a sample on a database: they
// cover the table with no gap, are close to the block size, and are the
// same for two samples of an unchanged table.
func TestSampleBlocksOnDB(t *testing.T) {
	pool := testPool(t)

	t.Run("random key", func(t *testing.T) {
		// md5 keys do not follow the insert order, so the heap is not in
		// key order. fillfactor 10 gives a few rows per page, so the 1%
		// sample reads enough pages.
		newTable(t, pool, "s_rand",
			`CREATE TABLE ace_slicer_test.s_rand (id text PRIMARY KEY, v text) WITH (fillfactor = 10, autovacuum_enabled = false)`,
			`INSERT INTO ace_slicer_test.s_rand SELECT md5(g::text), repeat('x', 100) FROM generate_series(1, 200000) g`)
		cfg := Config{Schema: "ace_slicer_test", Table: "s_rand", Key: []string{"id"}, BlockSize: 1000}
		blocks, st := runSample(t, pool, cfg)
		checkSampleBlocks(t, pool, cfg, blocks, st, 5)
		if st.SampleKeys == 0 || st.SamplePercent != 1 {
			t.Errorf("stats %+v", st)
		}

		again, _ := runSample(t, pool, cfg)
		if !reflect.DeepEqual(blocks, again) {
			t.Error("two samples of an unchanged table gave different bounds")
		}
	})

	t.Run("composite key", func(t *testing.T) {
		newTable(t, pool, "s_comp",
			`CREATE TABLE ace_slicer_test.s_comp (a int, b text, v text, PRIMARY KEY (a, b)) WITH (fillfactor = 10, autovacuum_enabled = false)`,
			`INSERT INTO ace_slicer_test.s_comp SELECT g % 7, md5(g::text), repeat('x', 100) FROM generate_series(1, 100000) g`)
		cfg := Config{Schema: "ace_slicer_test", Table: "s_comp", Key: []string{"a", "b"}, BlockSize: 500}
		blocks, st := runSample(t, pool, cfg)
		checkSampleBlocks(t, pool, cfg, blocks, st, 5)
	})

	t.Run("key columns named like the columns of the query", func(t *testing.T) {
		// The sample query adds its own columns, with names that start with
		// _ace_. Key columns with such names must not clash with them.
		newTable(t, pool, "s_names",
			`CREATE TABLE ace_slicer_test.s_names (_ace_rn text, _ace_blk int, v text, PRIMARY KEY (_ace_rn, _ace_blk)) WITH (fillfactor = 10, autovacuum_enabled = false)`,
			`INSERT INTO ace_slicer_test.s_names SELECT md5(g::text), g % 7, repeat('x', 100) FROM generate_series(1, 100000) g`)
		cfg := Config{Schema: "ace_slicer_test", Table: "s_names", Key: []string{"_ace_rn", "_ace_blk"}, BlockSize: 500}
		blocks, st := runSample(t, pool, cfg)
		checkSampleBlocks(t, pool, cfg, blocks, st, 5)
	})

	t.Run("small block size reads the whole table", func(t *testing.T) {
		newTable(t, pool, "s_small",
			`CREATE TABLE ace_slicer_test.s_small (id int PRIMARY KEY)`,
			`INSERT INTO ace_slicer_test.s_small SELECT g FROM generate_series(1, 1000) g`)
		cfg := Config{Schema: "ace_slicer_test", Table: "s_small", Key: []string{"id"}, BlockSize: 10}
		blocks, st := runSample(t, pool, cfg)
		// Every 10th key is a bound, so all blocks but the edges hold 10 rows.
		checkSampleBlocks(t, pool, cfg, blocks, st, 1)
		if st.SampleKeys != 1000 || len(blocks) != 101 {
			t.Errorf("got %d blocks from %d keys, want 101 from 1000", len(blocks), st.SampleKeys)
		}
	})

	t.Run("empty table", func(t *testing.T) {
		newTable(t, pool, "s_empty", `CREATE TABLE ace_slicer_test.s_empty (id int PRIMARY KEY)`)
		cfg := Config{Schema: "ace_slicer_test", Table: "s_empty", Key: []string{"id"}, BlockSize: 1000}
		blocks, _ := runSample(t, pool, cfg)
		if len(blocks) != 1 || blocks[0].Start != nil || blocks[0].End != nil {
			t.Errorf("an empty table must give one open block, got %+v", blocks)
		}
	})

	t.Run("filter is refused", func(t *testing.T) {
		cfg := Config{Schema: "ace_slicer_test", Table: "s_empty", Key: []string{"id"}, BlockSize: 1000, Filter: "id > 1"}
		if _, err := ReadSample(context.Background(), pool, cfg); err == nil {
			t.Error("the sample with a filter must fail")
		}
	})
}

// TestChooseOnDB checks the choice of the auto mode from the real catalog
// of a database, before and after ANALYZE, VACUUM and UPDATE.
func TestChooseOnDB(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()

	newTable(t, pool, "c_rand",
		`CREATE TABLE ace_slicer_test.c_rand (id text PRIMARY KEY, v int) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ace_slicer_test.c_rand SELECT md5(g::text), g FROM generate_series(1, 200000) g`)
	newTable(t, pool, "c_seq",
		`CREATE TABLE ace_slicer_test.c_seq (id bigint PRIMARY KEY, v int) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ace_slicer_test.c_seq SELECT g, g FROM generate_series(1, 200000) g`)
	rnd := Config{Schema: "ace_slicer_test", Table: "c_rand", Key: []string{"id"}, BlockSize: 1000}
	seq := Config{Schema: "ace_slicer_test", Table: "c_seq", Key: []string{"id"}, BlockSize: 1000}

	choose := func(cfg Config, requested string) *Choice {
		t.Helper()
		c, err := Choose(ctx, pool, cfg, requested)
		if err != nil {
			t.Fatalf("Choose: %v", err)
		}
		return c
	}

	// No ANALYZE yet: no statistics, so the cutter.
	if c := choose(rnd, MethodAuto); c.Method != MethodCutter || c.Signals.Correlation != nil {
		t.Errorf("no statistics: got %s, correlation %v", c.Method, c.Signals.Correlation)
	}

	exec(t, pool, `VACUUM ANALYZE ace_slicer_test.c_rand`)
	exec(t, pool, `VACUUM ANALYZE ace_slicer_test.c_seq`)
	c := choose(rnd, MethodAuto)
	if c.Method != MethodCutter || c.Signals.Correlation == nil || *c.Signals.Correlation > 0.1 {
		t.Errorf("random key after VACUUM: got %s (%s)", c.Method, c.Reason)
	}
	if c := choose(seq, MethodAuto); c.Signals.Correlation == nil || *c.Signals.Correlation < 0.9 {
		t.Errorf("sequential key: correlation %v", c.Signals.Correlation)
	}

	// After an UPDATE of every row, the visibility map in pg_class still
	// shows the state of the last VACUUM, but the count of changes shows
	// that most pages are not all-visible.
	exec(t, pool, `UPDATE ace_slicer_test.c_rand SET v = v + 1`)
	exec(t, pool, `UPDATE ace_slicer_test.c_seq SET v = v + 1`)
	exec(t, pool, `SELECT pg_stat_force_next_flush()`)
	c = choose(rnd, MethodAuto)
	u, fromMods := c.Signals.NotAllVisible()
	if c.Method != MethodSample || u < 0.9 || !fromMods {
		t.Errorf("random key after UPDATE: got %s, not all-visible %.2f, from changes %v (%s)", c.Method, u, fromMods, c.Reason)
	}
	if c := choose(seq, MethodAuto); c.Method != MethodCutter || !c.SlowAnchor {
		t.Errorf("sequential key after UPDATE: got %s, slow anchor %v (%s)", c.Method, c.SlowAnchor, c.Reason)
	}
	if c := choose(rnd, MethodCutter); c.Method != MethodCutter || c.Requested != MethodCutter {
		t.Errorf("user cutter: got %s", c.Method)
	}
	if c := choose(seq, MethodSample); c.Method != MethodSample {
		t.Errorf("user sample: got %s", c.Method)
	}

	if _, err := Choose(ctx, pool, Config{Schema: "ace_slicer_test", Table: "no_such", Key: []string{"id"}, BlockSize: 1000}, MethodAuto); err == nil {
		t.Error("a missing table must fail")
	}
}

// TestSamplePageShareOnDB checks the share of sampled keys on the page of
// the key before them: low on a random heap, high when rows with near keys
// share pages, also when the correlation of the key is low.
func TestSamplePageShareOnDB(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	newTable(t, pool, "p_rand",
		`CREATE TABLE ace_slicer_test.p_rand (id text PRIMARY KEY, v text) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ace_slicer_test.p_rand SELECT md5(g::text), repeat('x', 100) FROM generate_series(1, 200000) g`)
	newTable(t, pool, "p_ord",
		`CREATE TABLE ace_slicer_test.p_ord (id bigint PRIMARY KEY, v text) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ace_slicer_test.p_ord SELECT g, repeat('x', 100) FROM generate_series(1, 200000) g`)
	// 32 key ranges written in turn, 100 rows at a time: the correlation of
	// the key is low, but the rows of a block lie on a few pages.
	newTable(t, pool, "p_chunk",
		`CREATE TABLE ace_slicer_test.p_chunk (id bigint PRIMARY KEY, v text) WITH (autovacuum_enabled = false)`,
		`INSERT INTO ace_slicer_test.p_chunk SELECT ((g - 1) / 100 % 32) * 6400 + (g - 1) / 3200 * 100 + (g - 1) % 100, repeat('x', 100) FROM generate_series(1, 200000) g`)
	exec(t, pool, `ANALYZE ace_slicer_test.p_chunk`)

	for _, c := range []struct {
		table     string
		clustered bool
	}{{"p_rand", false}, {"p_ord", true}, {"p_chunk", true}} {
		cfg := Config{Schema: "ace_slicer_test", Table: c.table, Key: []string{"id"}, BlockSize: 1000}
		s, err := ReadSample(ctx, pool, cfg)
		if err != nil {
			t.Fatalf("%s: %v", c.table, err)
		}
		t.Logf("%s: %d keys, %d bounds, same page %.3f", c.table, s.keys, len(s.bounds), s.samePage)
		if s.clustered() != c.clustered {
			t.Errorf("%s: clustered() = %v, want %v (same page %.3f)", c.table, s.clustered(), c.clustered, s.samePage)
		}
		if want := s.keys / sampleKeysPerBlock; int64(len(s.bounds)) != want {
			t.Errorf("%s: %d bounds from %d keys, want %d", c.table, len(s.bounds), s.keys, want)
		}
	}

	var corr float64
	if err := pool.QueryRow(ctx, `SELECT abs(correlation) FROM pg_stats WHERE schemaname = 'ace_slicer_test' AND tablename = 'p_chunk' AND attname = 'id'`).Scan(&corr); err != nil {
		t.Fatal(err)
	}
	if corr >= maxSampleCorrelation {
		t.Errorf("p_chunk must have a low correlation to test the blind spot of the catalog, got %.3f", corr)
	}
}
