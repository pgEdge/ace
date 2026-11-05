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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// blockSamplePercent returns the TABLESAMPLE SYSTEM rate that gives about
// sampleKeysPerBlock sampled keys per block of blockSize rows, or 100 when
// the whole table must be read.
func blockSamplePercent(blockSize int) float64 {
	if blockSize <= sampleKeysPerBlock {
		return 100
	}
	return 100 * float64(sampleKeysPerBlock) / float64(blockSize)
}

// sampleKeys returns the key columns renamed to _ace_k0, _ace_k1, ... for
// the first subquery of the sample query, and the list of the new names.
// Above that subquery the query uses only its own names, so a key column
// cannot have the same name as a column that the query adds.
func sampleKeys(cfg *Config) (renamed, names string) {
	r := make([]string, len(cfg.Key))
	n := make([]string, len(cfg.Key))
	for i, k := range cfg.Key {
		n[i] = fmt.Sprintf("_ace_k%d", i)
		r[i] = pgx.Identifier{k}.Sanitize() + " AS " + n[i]
	}
	return strings.Join(r, ", "), strings.Join(n, ", ")
}

// sampleBlocksSQL returns the query that reads the sample and returns every
// sampleKeysPerBlock-th sampled key in key order. The server sorts the keys,
// so the order follows its collations and the comparison of composite keys.
//
// Each row also carries the number of the row in key order and the size of
// the sample. The last sampled key is returned too, so the size is known
// even when the sample holds fewer keys than one block needs.
func sampleBlocksSQL(cfg *Config, pct float64) string {
	sample := ""
	if pct < 100 {
		sample = fmt.Sprintf(" TABLESAMPLE SYSTEM (%g) REPEATABLE (%d)", pct, sampleSeed)
	}
	renamed, keys := sampleKeys(cfg)
	return fmt.Sprintf(`
SELECT _ace_rn, _ace_n, %[1]s
FROM (
	SELECT %[1]s,
		row_number() OVER (ORDER BY %[1]s) AS _ace_rn,
		count(*) OVER () AS _ace_n
	FROM (
		SELECT %[2]s
		FROM %[3]s%[4]s
	) _ace_t
) _ace_s
WHERE _ace_rn %% %[5]d = 0 OR _ace_rn = _ace_n
ORDER BY _ace_rn`, keys, renamed, cfg.tableIdent(), sample, sampleKeysPerBlock)
}

// minSampleKeys is the smallest sample that the auto mode uses. A smaller
// sample means that the catalog overstated the number of rows or that the
// sample missed most of them. Its bounds are few, and one block can hold
// most of the table.
const minSampleKeys = 100

// Sample is the result of the sample query on the anchor node.
type Sample struct {
	// bounds are the block bounds in key order.
	bounds [][]any
	// keys is the number of sampled keys.
	keys int64
	// pct is the TABLESAMPLE SYSTEM rate.
	pct     float64
	elapsed time.Duration
}

// ReadSample runs the sample query on the anchor node. The query ends
// before the function returns, so it does not hold a snapshot while the
// blocks are hashed. A fixed seed gives the same bounds on an unchanged
// table.
func ReadSample(ctx context.Context, q Querier, cfg Config) (*Sample, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cfg.Filter) != "" {
		return nil, errors.New("slicer: the sample cannot be used with a row filter (--table-filter, --against-origin or --until); use the cutter")
	}
	start := time.Now()
	s := &Sample{pct: blockSamplePercent(cfg.BlockSize)}

	// The names are quoted and the numbers are formatted by Go.
	rows, err := q.Query(ctx, sampleBlocksSQL(&cfg, s.pct)) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("slicer: sample block bounds: %w", err)
	}
	defer rows.Close()
	const extra = 2
	nk := len(cfg.Key)
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("slicer: sample block bounds: %w", err)
		}
		if len(vals) != nk+extra {
			return nil, fmt.Errorf("slicer: expected %d columns, got %d", nk+extra, len(vals))
		}
		rn, ok1 := vals[0].(int64)
		n, ok2 := vals[1].(int64)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("slicer: unexpected types of the sample counts: %T, %T", vals[0], vals[1])
		}
		s.keys = n
		// The last sampled key is returned for the counts only.
		if rn%sampleKeysPerBlock == 0 {
			s.bounds = append(s.bounds, append([]any(nil), vals[extra:]...))
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("slicer: sample block bounds: %w", err)
	}
	s.elapsed = time.Since(start)
	return s, nil
}

// Send sends the blocks of the sample to out. It does not close out.
//
// The bounds give the blocks (nil, b1), [b1, b2), ..., [bn, nil), so they
// cover the whole key space with no gap and no overlap, and rows of other
// nodes before the first or after the last bound are compared too. The size
// of a block is close to block_size on average but not exact, and
// Block.Rows is 0.
func (s *Sample) Send(ctx context.Context, out chan<- Block) (*Stats, error) {
	st := &Stats{
		Method:        MethodSample,
		SamplePercent: s.pct,
		SampleKeys:    s.keys,
		FirstBlock:    s.elapsed,
	}
	start := time.Now()
	var lo []any
	for i := 0; i <= len(s.bounds); i++ {
		var hi []any
		if i < len(s.bounds) {
			hi = s.bounds[i]
		}
		select {
		case out <- Block{Seq: i, Start: lo, End: hi}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		st.Blocks++
		lo = hi
	}
	st.Elapsed = s.elapsed + time.Since(start)
	return st, nil
}
