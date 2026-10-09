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
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests need a PostgreSQL server. Set ACE_SLICER_TEST_DSN to a
// database where the test may create and drop tables in schema
// ace_slicer_test. The integration suite runs the same checks against the
// docker cluster.

func testPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("ACE_SLICER_TEST_DSN")
	if dsn == "" {
		t.Skip("ACE_SLICER_TEST_DSN is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	exec(t, pool, `CREATE SCHEMA IF NOT EXISTS ace_slicer_test`)
	return pool
}

func exec(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// runSlicer plans and cuts the table and returns the plan and the blocks in
// key order.
func runSlicer(t *testing.T, pool *pgxpool.Pool, cfg Config) (*Plan, []Block, *Stats) {
	t.Helper()
	ctx := context.Background()
	plan, err := PlanParts(ctx, pool, cfg)
	if err != nil {
		t.Fatalf("PlanParts: %v", err)
	}
	out := make(chan Block, 16)
	var blocks []Block
	done := make(chan struct{})
	go func() {
		for b := range out {
			blocks = append(blocks, b)
		}
		close(done)
	}()
	st, err := Cut(ctx, pool, cfg, plan, out)
	close(out)
	<-done
	if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	SortBlocks(blocks)
	return plan, blocks, st
}

// countRange counts the rows of [start, end) that match the filter.
func countRange(t *testing.T, pool *pgxpool.Pool, cfg Config, b Block) int64 {
	t.Helper()
	var where []string
	var args []any
	if b.Start != nil {
		where = append(where, fmt.Sprintf("%s >= %s", cfg.keyExpr(), placeholders(len(args)+1, len(cfg.Key))))
		args = append(args, b.Start...)
	}
	if b.End != nil {
		where = append(where, fmt.Sprintf("%s < %s", cfg.keyExpr(), placeholders(len(args)+1, len(cfg.Key))))
		args = append(args, b.End...)
	}
	if cfg.Filter != "" {
		where = append(where, "("+cfg.Filter+")")
	}
	sql := "SELECT count(*) FROM " + cfg.tableIdent()
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// checkBlocks checks the invariants of a cut: the blocks follow each other
// with no gap and no overlap, the first and the last block are open, every
// block holds at most BlockSize rows, the row count of each block is right
// and all blocks together hold every row of the table.
func checkBlocks(t *testing.T, pool *pgxpool.Pool, cfg Config, blocks []Block, st *Stats) {
	t.Helper()
	if len(blocks) == 0 {
		t.Fatal("no blocks")
	}
	if blocks[0].Start != nil {
		t.Errorf("first block starts at %v, want no lower bound", blocks[0].Start)
	}
	if blocks[len(blocks)-1].End != nil {
		t.Errorf("last block ends at %v, want no upper bound", blocks[len(blocks)-1].End)
	}
	var sum, maxRows int64
	for i, b := range blocks {
		if i > 0 && !reflect.DeepEqual(blocks[i-1].End, b.Start) {
			t.Fatalf("block %d ends at %v but block %d starts at %v", i-1, blocks[i-1].End, i, b.Start)
		}
		n := countRange(t, pool, cfg, b)
		if n != b.Rows {
			t.Errorf("block %d [%v, %v): slicer counted %d rows, table has %d", i, b.Start, b.End, b.Rows, n)
		}
		if n > int64(cfg.BlockSize) {
			t.Errorf("block %d [%v, %v) has %d rows, more than block size %d", i, b.Start, b.End, n, cfg.BlockSize)
		}
		sum += n
		maxRows = max(maxRows, n)
	}
	total := countRange(t, pool, cfg, Block{})
	if sum != total {
		t.Errorf("blocks hold %d rows, table has %d", sum, total)
	}
	if st.Blocks != int64(len(blocks)) || st.Rows != total || st.MaxBlockRows != maxRows {
		t.Errorf("stats %+v, want %d blocks, %d rows, largest %d", st, len(blocks), total, maxRows)
	}
}

func TestSlicerOnDB(t *testing.T) {
	pool := testPool(t)

	type scenario struct {
		name       string
		setup      []string
		key        []string
		filter     string
		blockSize  int
		chunkRows  int
		workers    int
		wantSource string
	}
	icu := "C"
	var hasICU bool
	_ = pool.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_collation WHERE collname = 'und-x-icu')`).Scan(&hasICU)
	if hasICU {
		icu = "und-x-icu"
	}

	scenarios := []scenario{
		{
			name: "int analyzed",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id bigint PRIMARY KEY, v text)`,
				`INSERT INTO ace_slicer_test.t SELECT g, md5(g::text) FROM generate_series(1, 20000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 700, workers: 3, wantSource: SourceHistogram,
		},
		{
			name: "int no statistics",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id bigint PRIMARY KEY, v text) WITH (autovacuum_enabled = false)`,
				`INSERT INTO ace_slicer_test.t SELECT g, md5(g::text) FROM generate_series(1, 20000) g`,
			},
			key: []string{"id"}, blockSize: 700, workers: 2, wantSource: SourceSample,
		},
		{
			name: "int stale statistics",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id bigint PRIMARY KEY, v text) WITH (autovacuum_enabled = false)`,
				`INSERT INTO ace_slicer_test.t SELECT g, md5(g::text) FROM generate_series(1, 5000) g`,
				`ANALYZE ace_slicer_test.t`,
				// Most rows sort above the last histogram bound and go to
				// the last part. The parts are uneven; the blocks are not.
				`INSERT INTO ace_slicer_test.t SELECT g, md5(g::text) FROM generate_series(5001, 20000) g`,
			},
			key: []string{"id"}, blockSize: 700, workers: 2, wantSource: SourceHistogram,
		},
		{
			name: "uuid",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id uuid PRIMARY KEY, v int)`,
				`INSERT INTO ace_slicer_test.t SELECT md5(g::text)::uuid, g FROM generate_series(1, 15000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 500, workers: 4, wantSource: SourceHistogram,
		},
		{
			name: "text with non-C collation",
			setup: []string{
				fmt.Sprintf(`CREATE TABLE ace_slicer_test.t (id text COLLATE %q PRIMARY KEY, v int)`, icu),
				`INSERT INTO ace_slicer_test.t SELECT CASE WHEN g % 2 = 0 THEN 'a' ELSE 'B' END || md5(g::text), g FROM generate_series(1, 15000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 500, workers: 4, wantSource: SourceHistogram,
		},
		{
			name: "composite key with low-cardinality leading column",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (a int, b int, v text, PRIMARY KEY (a, b))`,
				`INSERT INTO ace_slicer_test.t SELECT g % 3, g, 'x' FROM generate_series(1, 15000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"a", "b"}, blockSize: 400, workers: 3, wantSource: SourceSample,
		},
		{
			name: "composite key with histogram",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (a int, b text, v text, PRIMARY KEY (a, b))`,
				`INSERT INTO ace_slicer_test.t SELECT g / 3, 'k' || g, 'x' FROM generate_series(1, 15000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"a", "b"}, blockSize: 400, workers: 3, wantSource: SourceHistogram,
		},
		{
			name: "empty table",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id int PRIMARY KEY)`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 10, workers: 2, wantSource: SourceSample,
		},
		{
			name: "one row",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id int PRIMARY KEY)`,
				`INSERT INTO ace_slicer_test.t VALUES (42)`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 10, workers: 2, wantSource: SourceSample,
		},
		{
			name: "filter",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id int PRIMARY KEY, v int)`,
				`INSERT INTO ace_slicer_test.t SELECT g, g % 7 FROM generate_series(1, 20000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, filter: "v = 3", blockSize: 100, workers: 2, wantSource: SourceHistogram,
		},
		{
			name: "small chunks",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id int PRIMARY KEY)`,
				`INSERT INTO ace_slicer_test.t SELECT g FROM generate_series(1, 3000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 7, chunkRows: 50, workers: 2, wantSource: SourceHistogram,
		},
		{
			name: "block size one",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id int PRIMARY KEY) WITH (autovacuum_enabled = false)`,
				`INSERT INTO ace_slicer_test.t SELECT g FROM generate_series(1, 300) g`,
			},
			key: []string{"id"}, blockSize: 1, chunkRows: 20, workers: 1, wantSource: SourceSample,
		},
		{
			name: "partitioned table",
			setup: []string{
				`CREATE TABLE ace_slicer_test.t (id int PRIMARY KEY, v text) PARTITION BY RANGE (id)`,
				`CREATE TABLE ace_slicer_test.t_1 PARTITION OF ace_slicer_test.t FOR VALUES FROM (MINVALUE) TO (5000)`,
				`CREATE TABLE ace_slicer_test.t_2 PARTITION OF ace_slicer_test.t FOR VALUES FROM (5000) TO (MAXVALUE)`,
				`INSERT INTO ace_slicer_test.t SELECT g, 'x' FROM generate_series(1, 12000) g`,
				`ANALYZE ace_slicer_test.t`,
			},
			key: []string{"id"}, blockSize: 900, workers: 2, wantSource: SourceHistogram,
		},
	}

	for i, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			// A table name of its own for each scenario: pgx caches prepared
			// statements by SQL text, and the key type changes between
			// scenarios.
			table := fmt.Sprintf("t%d", i)
			rename := func(s string) string {
				return strings.ReplaceAll(s, "ace_slicer_test.t", "ace_slicer_test."+table)
			}
			exec(t, pool, rename(`DROP TABLE IF EXISTS ace_slicer_test.t`))
			for _, s := range sc.setup {
				exec(t, pool, rename(s))
			}
			t.Cleanup(func() { exec(t, pool, rename(`DROP TABLE IF EXISTS ace_slicer_test.t`)) })

			cfg := Config{
				Schema: "ace_slicer_test", Table: table, Key: sc.key, Filter: sc.filter,
				BlockSize: sc.blockSize, Workers: sc.workers, ChunkRows: sc.chunkRows,
			}
			plan, blocks, st := runSlicer(t, pool, cfg)
			if plan.Source != sc.wantSource {
				t.Errorf("source %q (%s), want %q", plan.Source, plan.Reason, sc.wantSource)
			}
			checkBlocks(t, pool, cfg, blocks, st)
			t.Logf("%s: %d parts, %d blocks, largest %d rows", plan.Source, len(plan.Parts), len(blocks), st.MaxBlockRows)
		})
	}
}

// TestSlicerRepeatable checks that two cuts of the same data give the same
// blocks, for both part sources.
func TestSlicerRepeatable(t *testing.T) {
	pool := testPool(t)
	for _, analyze := range []bool{true, false} {
		t.Run(fmt.Sprintf("analyze=%v", analyze), func(t *testing.T) {
			exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.r`)
			exec(t, pool, `CREATE TABLE ace_slicer_test.r (id int PRIMARY KEY) WITH (autovacuum_enabled = false)`)
			exec(t, pool, `INSERT INTO ace_slicer_test.r SELECT g FROM generate_series(1, 50000) g`)
			if analyze {
				exec(t, pool, `ANALYZE ace_slicer_test.r`)
			}
			t.Cleanup(func() { exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.r`) })

			cfg := Config{Schema: "ace_slicer_test", Table: "r", Key: []string{"id"}, BlockSize: 1000, Workers: 3}
			_, b1, _ := runSlicer(t, pool, cfg)
			_, b2, _ := runSlicer(t, pool, cfg)
			if !reflect.DeepEqual(b1, b2) {
				t.Fatalf("two cuts differ: %d and %d blocks", len(b1), len(b2))
			}
		})
	}
}

// TestCutConcurrentWrites cuts a table in many small chunks while another
// session deletes and inserts rows in it, also the rows the chunks start
// from. The blocks must still follow each other with no gap, and the first
// and the last block must stay open.
func TestCutConcurrentWrites(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.w`)
	exec(t, pool, `CREATE TABLE ace_slicer_test.w (id int PRIMARY KEY)`)
	exec(t, pool, `INSERT INTO ace_slicer_test.w SELECT g * 2 FROM generate_series(1, 20000) g`)
	exec(t, pool, `ANALYZE ace_slicer_test.w`)
	t.Cleanup(func() { exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.w`) })

	cfg := Config{Schema: "ace_slicer_test", Table: "w", Key: []string{"id"}, BlockSize: 13, ChunkRows: 40, Workers: 4}

	stop := make(chan struct{})
	writerDone := make(chan error, 1)
	go func() {
		var err error
		for i := 0; err == nil; i++ {
			select {
			case <-stop:
				writerDone <- nil
				return
			default:
			}
			// Delete every 13th even key in a moving window, and insert odd
			// keys between them; both hit chunk starts.
			_, err = pool.Exec(ctx, `
				WITH d AS (DELETE FROM ace_slicer_test.w WHERE id % 26 = $1 % 26 AND id % 2 = 0 RETURNING id)
				INSERT INTO ace_slicer_test.w SELECT id + 1 FROM d ON CONFLICT DO NOTHING`, i*2)
		}
		writerDone <- err
	}()

	for run := 0; run < 5; run++ {
		_, blocks, _ := runSlicer(t, pool, cfg)
		if blocks[0].Start != nil || blocks[len(blocks)-1].End != nil {
			t.Fatalf("run %d: first or last block is not open", run)
		}
		for i := 1; i < len(blocks); i++ {
			if !reflect.DeepEqual(blocks[i-1].End, blocks[i].Start) {
				t.Fatalf("run %d: gap between block %d (end %v) and block %d (start %v)", run, i-1, blocks[i-1].End, i, blocks[i].Start)
			}
		}
	}
	close(stop)
	if err := <-writerDone; err != nil {
		t.Fatalf("writer: %v", err)
	}
}

// TestPartitionedWithoutStatistics takes the sample path on a partitioned
// table: TABLESAMPLE on the parent and pages summed over partitions.
func TestPartitionedWithoutStatistics(t *testing.T) {
	pool := testPool(t)
	exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.p`)
	exec(t, pool, `CREATE TABLE ace_slicer_test.p (id int PRIMARY KEY) PARTITION BY RANGE (id)`)
	exec(t, pool, `CREATE TABLE ace_slicer_test.p_1 PARTITION OF ace_slicer_test.p FOR VALUES FROM (MINVALUE) TO (100000) WITH (autovacuum_enabled = false)`)
	exec(t, pool, `CREATE TABLE ace_slicer_test.p_2 PARTITION OF ace_slicer_test.p FOR VALUES FROM (100000) TO (MAXVALUE) WITH (autovacuum_enabled = false)`)
	exec(t, pool, `INSERT INTO ace_slicer_test.p SELECT g FROM generate_series(1, 400000) g`)
	t.Cleanup(func() { exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.p`) })

	cfg := Config{Schema: "ace_slicer_test", Table: "p", Key: []string{"id"}, BlockSize: 10000, Workers: 2}
	plan, blocks, st := runSlicer(t, pool, cfg)
	if plan.Source != SourceSample {
		t.Fatalf("source %q (%s), want sample", plan.Source, plan.Reason)
	}
	if plan.SamplePercent >= 100 {
		t.Fatalf("sample percent %v: pages of the partitions were not counted", plan.SamplePercent)
	}
	checkBlocks(t, pool, cfg, blocks, st)
	t.Logf("%d parts, sample %.3g%%, %d blocks", len(plan.Parts), plan.SamplePercent, len(blocks))
}

// TestCutPlanUsesIndex checks that with the plan guard a cutting query with
// a selective filter reads the primary-key index. Without the guard the
// planner may prefer a sequential scan and a sort for every jump.
func TestCutPlanUsesIndex(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.pl`)
	exec(t, pool, `CREATE TABLE ace_slicer_test.pl (id int PRIMARY KEY, n int)`)
	exec(t, pool, `INSERT INTO ace_slicer_test.pl SELECT g, g % 100 FROM generate_series(1, 200000) g`)
	exec(t, pool, `ANALYZE ace_slicer_test.pl`)
	t.Cleanup(func() { exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.pl`) })

	cfg := &Config{Schema: "ace_slicer_test", Table: "pl", Key: []string{"id"}, Filter: "n = 3", BlockSize: 100}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, chunkPlanGuard); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "EXPLAIN (COSTS OFF) "+chunkSQL(cfg, false), int64(1), int64(100), int64(10_000))
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, line)
	}
	rows.Close()
	text := strings.Join(plan, "\n")
	if !strings.Contains(text, "Index Scan using pl_pkey") || strings.Contains(text, "Seq Scan") {
		t.Fatalf("cutting query does not read the primary key in order:\n%s", text)
	}
}

// TestCutCancel checks that Cut stops when nobody reads its blocks and the
// context is cancelled.
func TestCutCancel(t *testing.T) {
	pool := testPool(t)
	exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.c`)
	exec(t, pool, `CREATE TABLE ace_slicer_test.c (id int PRIMARY KEY)`)
	exec(t, pool, `INSERT INTO ace_slicer_test.c SELECT g FROM generate_series(1, 10000) g`)
	t.Cleanup(func() { exec(t, pool, `DROP TABLE IF EXISTS ace_slicer_test.c`) })

	cfg := Config{Schema: "ace_slicer_test", Table: "c", Key: []string{"id"}, BlockSize: 10, Workers: 2}
	ctx, cancel := context.WithCancel(context.Background())
	plan, err := PlanParts(ctx, pool, cfg)
	if err != nil {
		t.Fatal(err)
	}
	out := make(chan Block) // nobody reads it
	errCh := make(chan error, 1)
	go func() {
		_, err := Cut(ctx, pool, cfg, plan, out)
		errCh <- err
	}()
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("Cut returned no error after cancel")
	}
}
