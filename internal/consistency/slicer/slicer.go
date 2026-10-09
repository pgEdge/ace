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

// Package slicer splits a table into primary-key blocks for table-diff and
// mtree build.
//
// The work has two levels:
//
//   - A part is a share of work for one cutting worker. ACE builds K parts
//     on the anchor node from pg_stats.histogram_bounds of the leading key
//     column, or from a small TABLESAMPLE if there is no histogram. K
//     depends on the number of workers, not on the size of the table.
//     Uneven parts (an old histogram) cost some time but never change the
//     blocks.
//   - A block is the unit that every node hashes. A worker walks its part
//     in key order on the anchor node and starts a new block every
//     block_size rows, so no block holds more than block_size rows there,
//     whatever the key distribution or the physical row order.
//
// Blocks are half-open ranges [Start, End). The first block of the table has
// no lower bound and the last one has no upper bound, so rows that sort
// before the first or after the last key of the anchor node are still
// compared on every node.
//
// Workers send blocks to a channel as soon as they cut them, so hashing
// starts before the whole key space is cut. One cutting query reads a
// bounded number of rows, so it does not hold a snapshot for long.
package slicer

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	// SourceHistogram means part bounds come from pg_stats.histogram_bounds.
	SourceHistogram = "histogram"
	// SourceSample means part bounds come from a TABLESAMPLE of the key.
	SourceSample = "sample"
	// SourceSingle means the whole key space is one part.
	SourceSingle = "single"

	// DefaultChunkRows is the target number of rows that one cutting query
	// reads. It limits how long one query holds its snapshot.
	DefaultChunkRows = 1_000_000

	// The sample reads about samplePagesPerPart pages per part, and never
	// less than minSamplePages pages. A table with fewer pages is read in
	// full.
	samplePagesPerPart = 100
	minSamplePages     = 1000

	// sampleSeed makes the sample, and so the part bounds, repeatable for
	// the same physical table.
	sampleSeed = 1

	maxWorkers = 4
)

// Querier is the part of pgxpool.Pool that the slicer uses.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

// Config describes the table and the limits of one slicing run.
type Config struct {
	Schema string
	Table  string
	Key    []string
	// Filter is an optional SQL boolean expression. Rows that do not match
	// it are not counted toward block_size.
	Filter string

	BlockSize int
	// Parts is the target number of parts. 0 means DefaultParts(Workers).
	Parts int
	// Workers is the number of parts cut at the same time on the anchor.
	Workers int
	// ChunkRows overrides DefaultChunkRows (tests use small values).
	ChunkRows int
}

// DefaultWorkers returns the number of cutting workers for hashWorkers hash
// workers. Cutting a block reads only its keys, which is much cheaper than
// hashing it on every node, so one cutting worker per four hash workers
// keeps them busy.
func DefaultWorkers(hashWorkers int) int {
	w := (hashWorkers + 3) / 4
	if w < 1 {
		w = 1
	}
	if w > maxWorkers {
		w = maxWorkers
	}
	return w
}

// DefaultParts returns the number of parts for the given number of cutting
// workers. Several parts per worker keep the workers busy when parts are
// uneven.
func DefaultParts(workers int) int {
	if workers < 1 {
		workers = 1
	}
	return workers * 4
}

// Part is a range of full primary keys [Lo, Hi). nil means no bound.
type Part struct {
	Lo []any
	Hi []any
}

// Plan is the list of parts and the way ACE built it.
type Plan struct {
	Source string
	// Reason explains why ACE chose Source.
	Reason string
	Parts  []Part
	// SamplePercent is the TABLESAMPLE SYSTEM rate for SourceSample, or 100
	// when ACE read the whole table.
	SamplePercent float64
}

// Block is one range of full primary keys [Start, End). nil means no bound.
type Block struct {
	Part int
	Seq  int
	// Rows is the number of rows in the block on the anchor node when ACE
	// cut it.
	Rows  int64
	Start []any
	End   []any
}

// Less orders blocks by key.
func (b Block) Less(o Block) bool {
	if b.Part != o.Part {
		return b.Part < o.Part
	}
	return b.Seq < o.Seq
}

// SortBlocks sorts blocks by key.
func SortBlocks(blocks []Block) {
	sort.Slice(blocks, func(i, j int) bool { return blocks[i].Less(blocks[j]) })
}

// Stats describes a finished cut.
type Stats struct {
	Blocks int64
	// Rows is the number of rows the anchor node had in the cut ranges.
	Rows int64
	// MaxBlockRows is the size of the largest block on the anchor node.
	MaxBlockRows int64
	// FirstBlock is the time from the start of Cut to the first block.
	FirstBlock time.Duration
	Elapsed    time.Duration
}

// String returns a one-line description of a finished cut for the log.
func (st *Stats) String() string {
	return fmt.Sprintf("%d blocks in %s (first block after %s, largest block %d rows)",
		st.Blocks, st.Elapsed.Round(time.Millisecond), st.FirstBlock.Round(time.Millisecond), st.MaxBlockRows)
}

func (c *Config) validate() error {
	if len(c.Key) == 0 {
		return errors.New("slicer: primary key is empty")
	}
	if c.BlockSize < 1 {
		return fmt.Errorf("slicer: block size must be positive, got %d", c.BlockSize)
	}
	if c.Workers < 1 {
		c.Workers = 1
	}
	if c.Parts < 1 {
		c.Parts = DefaultParts(c.Workers)
	}
	if c.ChunkRows < 1 {
		c.ChunkRows = DefaultChunkRows
	}
	return nil
}

func (c *Config) tableIdent() string {
	return pgx.Identifier{c.Schema}.Sanitize() + "." + pgx.Identifier{c.Table}.Sanitize()
}

func (c *Config) keyList(alias string) string {
	cols := make([]string, len(c.Key))
	for i, k := range c.Key {
		cols[i] = pgx.Identifier{k}.Sanitize()
		if alias != "" {
			cols[i] = alias + "." + cols[i]
		}
	}
	return strings.Join(cols, ", ")
}

// keyExpr is the key as one value: the column for a simple key and ROW(...)
// for a composite key. Comparing ROW values follows the order of the
// primary-key index.
func (c *Config) keyExpr() string {
	if len(c.Key) == 1 {
		return pgx.Identifier{c.Key[0]}.Sanitize()
	}
	return "ROW(" + c.keyList("") + ")"
}

func placeholders(first, n int) string {
	ps := make([]string, n)
	for i := range ps {
		ps[i] = fmt.Sprintf("$%d", first+i)
	}
	if n == 1 {
		return ps[0]
	}
	return "ROW(" + strings.Join(ps, ", ") + ")"
}

// tableStats is what ACE reads from the catalog of the anchor node.
type tableStats struct {
	// typeName is the type of the leading key column.
	typeName string
	// pages is the size of the table, with all its partitions, in pages.
	pages int64
	// bounds is pg_stats.histogram_bounds of the leading key column as text,
	// or nil if there is no histogram.
	bounds []string
}

const statsSQL = `
WITH rel AS (
	SELECT c.oid, c.relkind
	FROM pg_catalog.pg_class c
	JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE n.nspname = $1 AND c.relname = $2
), tree AS (
	SELECT t.relid FROM rel, pg_catalog.pg_partition_tree(rel.oid) t WHERE t.isleaf
	UNION
	SELECT rel.oid FROM rel WHERE rel.relkind <> 'p'
)
SELECT
	(SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)
	 FROM pg_catalog.pg_attribute a, rel
	 WHERE a.attrelid = rel.oid AND a.attname = $3 AND NOT a.attisdropped),
	(SELECT COALESCE(sum(pg_catalog.pg_relation_size(tree.relid)), 0) FROM tree)::bigint
		/ pg_catalog.current_setting('block_size')::bigint,
	(SELECT s.histogram_bounds::text::text[]
	 FROM pg_catalog.pg_stats s
	 WHERE s.schemaname = $1 AND s.tablename = $2 AND s.attname = $3
	 ORDER BY s.inherited DESC
	 LIMIT 1)`

func readStats(ctx context.Context, q Querier, cfg *Config) (*tableStats, error) {
	rows, err := q.Query(ctx, statsSQL, cfg.Schema, cfg.Table, cfg.Key[0])
	if err != nil {
		return nil, fmt.Errorf("slicer: read statistics of %s: %w", cfg.tableIdent(), err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("slicer: read statistics of %s: %w", cfg.tableIdent(), err)
		}
		return nil, fmt.Errorf("slicer: no statistics row for %s", cfg.tableIdent())
	}
	var (
		st       tableStats
		typeName *string
	)
	if err := rows.Scan(&typeName, &st.pages, &st.bounds); err != nil {
		return nil, fmt.Errorf("slicer: read statistics of %s: %w", cfg.tableIdent(), err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("slicer: read statistics of %s: %w", cfg.tableIdent(), err)
	}
	if typeName == nil {
		return nil, fmt.Errorf("slicer: column %q of %s not found", cfg.Key[0], cfg.tableIdent())
	}
	st.typeName = *typeName
	return &st, nil
}

// pickBounds selects up to parts-1 histogram bounds that split the
// histogram into equal shares.
func pickBounds(bounds []string, parts int) []string {
	n := len(bounds)
	if n < 2 || parts < 2 {
		return nil
	}
	out := make([]string, 0, parts-1)
	for i := 1; i < parts; i++ {
		idx := int(math.Round(float64(i) * float64(n-1) / float64(parts)))
		if idx <= 0 {
			idx = 1
		}
		if idx >= n {
			idx = n - 1
		}
		if len(out) > 0 && out[len(out)-1] == bounds[idx] {
			continue
		}
		out = append(out, bounds[idx])
	}
	return out
}

func scanKeys(rows pgx.Rows, width, offset int) ([][]any, error) {
	defer rows.Close()
	var keys [][]any
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		if len(vals) != width+offset {
			return nil, fmt.Errorf("slicer: expected %d columns, got %d", width+offset, len(vals))
		}
		keys = append(keys, append([]any(nil), vals[offset:]...))
	}
	return keys, rows.Err()
}

// probeBounds turns histogram values of the leading key column into full
// keys that exist on the anchor node: for each value it reads the first key
// that is not less than the value. The result is sorted and has no
// duplicates.
func probeBounds(ctx context.Context, q Querier, cfg *Config, typeName string, bounds []string) ([][]any, error) {
	lead := "_ace_t." + pgx.Identifier{cfg.Key[0]}.Sanitize()
	keys := cfg.keyList("_ace_t")
	outKeys := cfg.keyList("_ace_p")
	sql := fmt.Sprintf(`
SELECT DISTINCT %[1]s
FROM unnest($1::text[]) AS _ace_b(v)
CROSS JOIN LATERAL (
	SELECT %[2]s FROM %[3]s AS _ace_t
	WHERE %[4]s >= _ace_b.v::%[5]s
	ORDER BY %[2]s
	LIMIT 1
) _ace_p
ORDER BY %[1]s`, outKeys, keys, cfg.tableIdent(), lead, typeName)

	rows, err := q.Query(ctx, sql, bounds)
	if err != nil {
		return nil, fmt.Errorf("slicer: probe part bounds: %w", err)
	}
	return scanKeys(rows, len(cfg.Key), 0)
}

func samplePercent(pages int64, parts int) float64 {
	target := int64(parts) * samplePagesPerPart
	if target < minSamplePages {
		target = minSamplePages
	}
	if pages <= target {
		return 100
	}
	return 100 * float64(target) / float64(pages)
}

// sampleBounds reads a TABLESAMPLE SYSTEM sample of the key, splits it into
// parts groups and returns the first key of every group but the first one.
func sampleBounds(ctx context.Context, q Querier, cfg *Config, parts int, pct float64) ([][]any, error) {
	sample := ""
	if pct < 100 {
		sample = fmt.Sprintf(" TABLESAMPLE SYSTEM (%g) REPEATABLE (%d)", pct, sampleSeed)
	}
	where := ""
	if f := strings.TrimSpace(cfg.Filter); f != "" {
		where = " WHERE (" + f + ")"
	}
	keys := cfg.keyList("")
	sql := fmt.Sprintf(`
SELECT DISTINCT ON (_ace_bucket) _ace_bucket, %[1]s
FROM (
	SELECT %[1]s, ntile(%[2]d) OVER (ORDER BY %[1]s) AS _ace_bucket
	FROM %[3]s%[4]s%[5]s
) _ace_s
WHERE _ace_bucket > 1
ORDER BY _ace_bucket, %[1]s`, keys, parts, cfg.tableIdent(), sample, where)

	rows, err := q.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("slicer: sample part bounds: %w", err)
	}
	return scanKeys(rows, len(cfg.Key), 1)
}

// partsFromKeys builds len(keys)+1 parts: (nil, k0), [k0, k1), ...,
// [kN, nil). Equal neighbours are merged. keys must be sorted in the order
// of the primary-key index; both callers get them from a query with ORDER
// BY on the key, so Go never compares key values itself. With this order
// the parts do not overlap and cover the whole key space.
func partsFromKeys(keys [][]any) []Part {
	parts := make([]Part, 0, len(keys)+1)
	var lo []any
	for _, k := range keys {
		if lo != nil && reflect.DeepEqual(lo, k) {
			continue
		}
		parts = append(parts, Part{Lo: lo, Hi: k})
		lo = k
	}
	return append(parts, Part{Lo: lo})
}

// PlanParts reads the catalog of the anchor node and builds the parts.
func PlanParts(ctx context.Context, q Querier, cfg Config) (*Plan, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if cfg.Parts < 2 {
		return &Plan{Source: SourceSingle, Reason: "one part requested", Parts: []Part{{}}}, nil
	}

	st, err := readStats(ctx, q, &cfg)
	if err != nil {
		return nil, err
	}

	// No histogram means no ANALYZE yet, or a column whose values are all
	// most common values (often the first column of a composite key).
	if len(st.bounds) >= 2 {
		keys, err := probeBounds(ctx, q, &cfg, st.typeName, pickBounds(st.bounds, cfg.Parts))
		if err != nil {
			return nil, err
		}
		return &Plan{
			Source: SourceHistogram,
			Reason: fmt.Sprintf("%d histogram bounds of column %q", len(st.bounds), cfg.Key[0]),
			Parts:  partsFromKeys(keys),
		}, nil
	}

	pct := samplePercent(st.pages, cfg.Parts)
	keys, err := sampleBounds(ctx, q, &cfg, cfg.Parts, pct)
	if err != nil {
		return nil, err
	}
	return &Plan{
		Source:        SourceSample,
		Reason:        fmt.Sprintf("no histogram for column %q", cfg.Key[0]),
		Parts:         partsFromKeys(keys),
		SamplePercent: pct,
	}, nil
}

// cutQueries holds the SQL that cuts the parts. Each query exists in two
// forms: for a part with an upper bound and for a part without one.
type cutQueries struct {
	first [2]string // the first key of a part with no lower bound
	chunk [2]string // the block starts of one chunk
	tail  [2]string // the rows of the last block of a part
}

func hiIdx(hasHi bool) int {
	if hasHi {
		return 1
	}
	return 0
}

// cutWhere returns the conditions on the key range and the filter. lo and
// hi are the first parameter numbers of the bounds, or 0 for no bound. A
// non-empty alias qualifies the key columns.
func cutWhere(cfg *Config, alias string, lo, hi int) []string {
	keyExpr := cfg.keyExpr()
	if alias != "" {
		keyExpr = cfg.keyList(alias)
		if len(cfg.Key) > 1 {
			keyExpr = "ROW(" + keyExpr + ")"
		}
	}
	nk := len(cfg.Key)
	var where []string
	if lo > 0 {
		where = append(where, fmt.Sprintf("%s >= %s", keyExpr, placeholders(lo, nk)))
	}
	if hi > 0 {
		where = append(where, fmt.Sprintf("%s < %s", keyExpr, placeholders(hi, nk)))
	}
	if f := strings.TrimSpace(cfg.Filter); f != "" {
		where = append(where, "("+f+")")
	}
	return where
}

func whereSQL(conds []string) string {
	if len(conds) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(conds, " AND ")
}

// firstSQL returns the first key of a part that has no lower bound.
// Parameters: the upper bound, if any.
func firstSQL(cfg *Config, hasHi bool) string {
	hi := 0
	if hasHi {
		hi = 1
	}
	keys := cfg.keyList("")
	return fmt.Sprintf(`SELECT %[1]s FROM %[2]s%[3]s ORDER BY %[1]s LIMIT 1`,
		keys, cfg.tableIdent(), whereSQL(cutWhere(cfg, "", 0, hi)))
}

// chunkSQL returns the query that cuts one chunk of a part. It starts at
// the first key that is not less than the lower bound (row 0) and then
// jumps block size rows forward in key order, up to $n times. Each jump is
// one index descent and reads block size index entries, so the query reads
// the chunk once and returns only the block starts.
//
// Parameters: lower bound, upper bound (if any), block size, number of
// jumps.
func chunkSQL(cfg *Config, hasHi bool) string {
	nk := len(cfg.Key)
	hi := 0
	next := 1 + nk
	if hasHi {
		hi = next
		next += nk
	}
	keys := cfg.keyList("")
	tKeys := cfg.keyList("_ace_t")
	bKeys := cfg.keyList("_ace_b")
	tExpr, bExpr := tKeys, bKeys
	if nk > 1 {
		tExpr, bExpr = "ROW("+tKeys+")", "ROW("+bKeys+")"
	}

	startWhere := cutWhere(cfg, "", 1, hi)
	stepWhere := append([]string{fmt.Sprintf("%s >= %s", tExpr, bExpr)}, cutWhere(cfg, "_ace_t", 0, hi)...)

	return fmt.Sprintf(`
WITH RECURSIVE _ace_b(_ace_i, %[1]s) AS (
	(SELECT 0, %[1]s FROM %[2]s%[3]s ORDER BY %[1]s LIMIT 1)
	UNION ALL
	SELECT _ace_b._ace_i + 1, _ace_x.*
	FROM _ace_b CROSS JOIN LATERAL (
		SELECT %[4]s FROM %[2]s AS _ace_t%[5]s
		ORDER BY %[4]s
		OFFSET $%[6]d LIMIT 1
	) _ace_x
	WHERE _ace_b._ace_i < $%[7]d
)
SELECT _ace_i, %[1]s FROM _ace_b ORDER BY _ace_i`,
		keys, cfg.tableIdent(), whereSQL(startWhere), tKeys, whereSQL(stepWhere), next, next+1)
}

// tailSQL counts the rows of the last block of a part, at most block size
// plus one. Parameters: lower bound, upper bound (if any), the limit.
func tailSQL(cfg *Config, hasHi bool) string {
	nk := len(cfg.Key)
	hi, next := 0, 1+nk
	if hasHi {
		hi = next
		next += nk
	}
	return fmt.Sprintf(`SELECT count(*) FROM (SELECT 1 FROM %s%s LIMIT $%d) _ace_s`,
		cfg.tableIdent(), whereSQL(cutWhere(cfg, "", 1, hi)), next)
}

func newCutQueries(cfg *Config) *cutQueries {
	q := &cutQueries{}
	for _, hasHi := range []bool{false, true} {
		q.first[hiIdx(hasHi)] = firstSQL(cfg, hasHi)
		q.chunk[hiIdx(hasHi)] = chunkSQL(cfg, hasHi)
		q.tail[hiIdx(hasHi)] = tailSQL(cfg, hasHi)
	}
	return q
}

type cutter struct {
	cfg       *Config
	q         Querier
	sql       *cutQueries
	out       chan<- Block
	start     time.Time
	blocks    atomic.Int64
	rows      atomic.Int64
	maxRows   atomic.Int64
	firstOnce sync.Once
	first     time.Duration
}

func (c *cutter) send(ctx context.Context, b Block) error {
	select {
	case c.out <- b:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.firstOnce.Do(func() { c.first = time.Since(c.start) })
	c.blocks.Add(1)
	c.rows.Add(b.Rows)
	for {
		cur := c.maxRows.Load()
		if b.Rows <= cur || c.maxRows.CompareAndSwap(cur, b.Rows) {
			break
		}
	}
	return nil
}

// chunkPlanGuard makes the planner read the primary-key index in order.
// Without it, a selective filter can make it choose a sequential scan and a
// sort, and then every jump of a chunk reads the whole table.
const chunkPlanGuard = "SET LOCAL enable_seqscan = off; SET LOCAL enable_sort = off"

// inChunkTx runs fn in a read-only REPEATABLE READ transaction with the
// plan guard, so all queries of one chunk see the same snapshot.
func (c *cutter) inChunkTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := c.q.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return err
	}
	// After Commit, Rollback does nothing.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, chunkPlanGuard); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func keyArgs(lo, hi []any, extra ...any) []any {
	args := make([]any, 0, len(lo)+len(hi)+len(extra))
	args = append(args, lo...)
	args = append(args, hi...)
	return append(args, extra...)
}

// firstKey returns the first key of part p, which has no lower bound, or
// nil if the part has no rows.
func (c *cutter) firstKey(ctx context.Context, p Part) ([]any, error) {
	var key []any
	err := c.inChunkTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, c.sql.first[hiIdx(p.Hi != nil)], p.Hi...)
		if err != nil {
			return err
		}
		keys, err := scanKeys(rows, len(c.cfg.Key), 0)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			key = keys[0]
		}
		return nil
	})
	return key, err
}

// readChunk returns up to n block starts after the block that starts at
// cur, and, when there are fewer than n, the number of rows from the last
// block start to the end of the part.
func (c *cutter) readChunk(ctx context.Context, p Part, cur []any, n int64) (starts [][]any, tail int64, err error) {
	m := int64(c.cfg.BlockSize)
	hi := hiIdx(p.Hi != nil)
	err = c.inChunkTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, c.sql.chunk[hi], keyArgs(cur, p.Hi, m, n)...)
		if err != nil {
			return err
		}
		got, err := scanKeys(rows, len(c.cfg.Key), 1)
		if err != nil {
			return err
		}
		// Row 0 is the first key not less than cur: cur itself, or the
		// next key if cur was deleted. It starts the current block, which
		// is already open.
		if len(got) > 0 {
			starts = got[1:]
		}
		if int64(len(starts)) == n {
			return nil
		}
		last := cur
		if len(starts) > 0 {
			last = starts[len(starts)-1]
		}
		return tx.QueryRow(ctx, c.sql.tail[hi], keyArgs(last, p.Hi, m+1)...).Scan(&tail)
	})
	return starts, tail, err
}

// cutPart cuts one part and sends its blocks in key order.
//
// Invariants:
//
//   - Coverage. The blocks of part [Lo, Hi) are [Lo, b1), [b1, b2), ...,
//     [bn, Hi): each block starts where the previous one ends, the first
//     starts at Lo and the last ends at Hi. This follows from the code
//     structure and holds even if rows change during the cut, so every key
//     of [Lo, Hi) on every node is in exactly one block.
//   - Size. Every block holds at most BlockSize rows on the anchor node in
//     the snapshot of the chunk that cut it. A block never spans two
//     chunks: a chunk ends on a block start. Rows written later can make a
//     block larger; the limit is not a promise about other nodes or other
//     moments.
//
// The size invariant needs the block starts in key order: each one is found
// with ORDER BY on the key, starting from the one before it.
func (c *cutter) cutPart(ctx context.Context, idx int, p Part) error {
	m := int64(c.cfg.BlockSize)
	maxBlocks := int64(c.cfg.ChunkRows) / m
	if maxBlocks < 1 {
		maxBlocks = 1
	}
	// The first chunk of a part cuts one block, so the first block is ready
	// soon. Each next chunk cuts twice as many blocks, up to maxBlocks.
	chunkBlocks := int64(1)

	seq := 0
	cur := p.Lo
	if cur == nil {
		// The part has no lower bound: the first block is (nil, first key),
		// empty on the anchor node. It holds the rows that other nodes have
		// below every key of the anchor.
		first, err := c.firstKey(ctx, p)
		if err != nil {
			return fmt.Errorf("slicer: cut part %d: %w", idx, err)
		}
		if first == nil {
			return c.send(ctx, Block{Part: idx, Seq: 0, Start: nil, End: p.Hi})
		}
		if err := c.send(ctx, Block{Part: idx, Seq: 0, Start: nil, End: first}); err != nil {
			return err
		}
		seq, cur = 1, first
	}

	for {
		starts, tail, err := c.readChunk(ctx, p, cur, chunkBlocks)
		if err != nil {
			return fmt.Errorf("slicer: cut part %d: %w", idx, err)
		}
		full := int64(len(starts)) == chunkBlocks
		chunkBlocks = min(2*chunkBlocks, maxBlocks)

		for _, s := range starts {
			if err := c.send(ctx, Block{Part: idx, Seq: seq, Rows: m, Start: cur, End: s}); err != nil {
				return err
			}
			seq++
			cur = s
		}
		if !full {
			return c.send(ctx, Block{Part: idx, Seq: seq, Rows: tail, Start: cur, End: p.Hi})
		}
		// The chunk ended on a block start. The next chunk starts there.
	}
}

// Describe returns a one-line description of a plan for the log.
func Describe(plan *Plan, cfg Config, anchor string) string {
	s := fmt.Sprintf("%d parts from %s on node %s, %d cutting workers, block size %d (%s)",
		len(plan.Parts), plan.Source, anchor, min(cfg.Workers, len(plan.Parts)), cfg.BlockSize, plan.Reason)
	if plan.Source == SourceSample {
		s += fmt.Sprintf(", sample %.4g%%", plan.SamplePercent)
	}
	return s
}

// Cut cuts every part of the plan on the anchor node and sends the blocks to
// out. It does not close out. Blocks of one part arrive in key order; blocks
// of different parts can interleave. Cut returns when all parts are cut or
// on the first error.
func Cut(ctx context.Context, q Querier, cfg Config, plan *Plan, out chan<- Block) (*Stats, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	c := &cutter{cfg: &cfg, q: q, sql: newCutQueries(&cfg), out: out, start: time.Now()}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan int, len(plan.Parts))
	for i := range plan.Parts {
		jobs <- i
	}
	close(jobs)

	workers := cfg.Workers
	if workers > len(plan.Parts) {
		workers = len(plan.Parts)
	}
	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if ctx.Err() != nil {
					return
				}
				if err := c.cutPart(ctx, i, plan.Parts[i]); err != nil {
					errOnce.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &Stats{
		Blocks:       c.blocks.Load(),
		Rows:         c.rows.Load(),
		MaxBlockRows: c.maxRows.Load(),
		FirstBlock:   c.first,
		Elapsed:      time.Since(c.start),
	}, nil
}
