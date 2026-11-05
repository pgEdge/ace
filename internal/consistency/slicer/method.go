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
	"math"
	"strings"
)

// The ways to cut a table into blocks.
//
// The cutter walks the primary-key index of the anchor node and gives exact
// blocks. Its cost depends on the visibility map: on a page that is not
// all-visible it reads the heap for every row, and with a random key this is
// one random page read per row.
//
// The sample reads a fixed share of the heap pages with TABLESAMPLE SYSTEM
// and takes every k-th sampled key as a block bound. Its cost does not depend
// on the visibility map, and the blocks are close to block_size rows only if
// the rows of one block do not share heap pages. If the heap is in key order,
// every sampled page gives a run of neighbour keys, and the blocks become
// very uneven.
//
// The way to cut never changes the result of a comparison: a block is a key
// range, and every node hashes the same ranges. It changes only the time.
const (
	// MethodAuto chooses the cutter or the sample from the catalog of the
	// anchor node.
	MethodAuto = "auto"
	// MethodCutter cuts blocks with the primary-key index.
	MethodCutter = "cutter"
	// MethodSample takes block bounds from a TABLESAMPLE SYSTEM sample.
	MethodSample = "sample"
)

const (
	// sampleKeysPerBlock is the number of sampled keys per block. More keys
	// give more even blocks on a heap that is not in key order, at the cost
	// of a larger sample.
	sampleKeysPerBlock = 10

	// The auto mode uses the sample only when the key does not follow the
	// heap order (|correlation| below maxSampleCorrelation) and the cutter
	// would read the heap often (a share of pages that are not all-visible
	// above minSampleNotAllVisible). On a table where more than
	// slowCutNotAllVisible of the pages are not all-visible, the cutter is
	// slower than the hashing, and the anchor node takes longer than the
	// other nodes.
	maxSampleCorrelation   = 0.1
	minSampleNotAllVisible = 0.05
	slowCutNotAllVisible   = 0.35

	// A table with fewer than minSampleBlocks blocks is always cut with the
	// cutter: the sample gives nothing there.
	minSampleBlocks = 100
)

// ParseMethod checks the name of a way to cut. An empty name means
// MethodAuto.
func ParseMethod(s string) (string, error) {
	switch m := strings.ToLower(strings.TrimSpace(s)); m {
	case "":
		return MethodAuto, nil
	case MethodAuto, MethodCutter, MethodSample:
		return m, nil
	default:
		return "", fmt.Errorf("unknown slicer %q: use %s, %s or %s", s, MethodAuto, MethodCutter, MethodSample)
	}
}

// Signals are the catalog values of the table on the anchor node that the
// auto mode uses.
type Signals struct {
	// Partitioned is true for a partitioned table and for a parent of
	// inheritance children. The catalog values of such a table do not
	// describe the rows of its children.
	Partitioned bool
	// Pages and AllVisiblePages are pg_class.relpages and relallvisible.
	// They are exact only right after VACUUM or ANALYZE.
	Pages           int64
	AllVisiblePages int64
	// Tuples is pg_class.reltuples, or -1 if it is not known.
	Tuples float64
	// ModsSinceAnalyze is the number of rows changed since the last
	// ANALYZE.
	ModsSinceAnalyze int64
	// Correlation is the largest |pg_stats.correlation| over the primary
	// key columns, or nil if a key column has no statistics.
	Correlation *float64
}

// NotAllVisible estimates the share of heap pages that are not all-visible.
// The visibility map share is exact after VACUUM or ANALYZE. An UPDATE or a
// DELETE does not change it, but each changed row can clear the all-visible
// bit of two pages (the old and the new row version), so the count of
// changes since ANALYZE gives a second estimate. The larger of the two is
// used: too high a value costs little, too low a value can make the cut
// very slow. fromMods is true when the count of changes gives the result.
func (s *Signals) NotAllVisible() (share float64, fromMods bool) {
	if s.Pages <= 0 {
		// The table was never vacuumed or analyzed, or it is empty.
		return 1, false
	}
	vm := 1 - float64(s.AllVisiblePages)/float64(s.Pages)
	vm = math.Max(0, math.Min(1, vm))
	mods := math.Min(1, 2*float64(s.ModsSinceAnalyze)/float64(s.Pages))
	if mods > vm {
		return mods, true
	}
	return vm, false
}

const signalsSQL = `
SELECT
	c.relkind = 'p' OR c.relhassubclass,
	c.relpages::bigint,
	c.relallvisible::bigint,
	c.reltuples::float8,
	COALESCE(s.n_mod_since_analyze, 0)::bigint,
	(SELECT count(DISTINCT ps.attname)
	 FROM pg_catalog.pg_stats ps
	 WHERE ps.schemaname = $1 AND ps.tablename = $2
	   AND ps.attname = ANY ($3::text[]) AND NOT ps.inherited
	   AND ps.correlation IS NOT NULL)::int,
	(SELECT max(abs(ps.correlation))
	 FROM pg_catalog.pg_stats ps
	 WHERE ps.schemaname = $1 AND ps.tablename = $2
	   AND ps.attname = ANY ($3::text[]) AND NOT ps.inherited)::float8
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_stat_all_tables s ON s.relid = c.oid
WHERE n.nspname = $1 AND c.relname = $2`

// readSignals reads the catalog values of the table on the anchor node. It
// needs no special privileges.
func readSignals(ctx context.Context, q Querier, cfg Config) (*Signals, error) {
	if len(cfg.Key) == 0 {
		return nil, errors.New("slicer: primary key is empty")
	}
	rows, err := q.Query(ctx, signalsSQL, cfg.Schema, cfg.Table, cfg.Key)
	if err != nil {
		return nil, fmt.Errorf("slicer: read catalog of %s: %w", cfg.tableIdent(), err)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("slicer: read catalog of %s: %w", cfg.tableIdent(), err)
		}
		return nil, fmt.Errorf("slicer: table %s not found", cfg.tableIdent())
	}
	var (
		s        Signals
		withStat int
		corr     *float64
	)
	if err := rows.Scan(&s.Partitioned, &s.Pages, &s.AllVisiblePages, &s.Tuples,
		&s.ModsSinceAnalyze, &withStat, &corr); err != nil {
		return nil, fmt.Errorf("slicer: read catalog of %s: %w", cfg.tableIdent(), err)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("slicer: read catalog of %s: %w", cfg.tableIdent(), err)
	}
	if withStat == len(cfg.Key) && corr != nil {
		s.Correlation = corr
	}
	return &s, nil
}

// Choice is the way ACE cuts the table and why.
type Choice struct {
	// Requested is the way the user asked for: auto, cutter or sample.
	Requested string
	// Method is the way ACE uses: cutter or sample.
	Method string
	Reason string
	// Signals are the catalog values that ACE read on the anchor node.
	Signals *Signals
	// Advice is a short hint for the user, or empty. Several hints are
	// separated by "; ".
	Advice string
	// SlowAnchor is true when ACE expects the cut to make the anchor node
	// slower than the other nodes.
	SlowAnchor bool
	// filtered is true when a row filter is set. The sample cannot be used
	// then, so no message suggests it.
	filtered bool
	// SamplePercent is the TABLESAMPLE SYSTEM rate of the sample method.
	SamplePercent float64
	// cutShare estimates how much the cutter adds to the hashing time of
	// the anchor node, as a share of it. A heap read of the cutter costs
	// about as much as the hash of one row, so this is the share of pages
	// that are not all-visible. It is 0 when the estimate means nothing:
	// for the sample, a small table or a partitioned table.
	cutShare float64
	// SamePage is the share of sampled keys on the same heap page as the
	// key before them, or nil when ACE did not read a sample.
	SamePage *float64
}

// Choose reads the catalog of the anchor node and decides how to cut the
// table. requested is auto, cutter or sample; an explicit way turns the auto
// mode off.
func Choose(ctx context.Context, q Querier, cfg Config, requested string) (*Choice, error) {
	m, err := ParseMethod(requested)
	if err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	s, err := readSignals(ctx, q, cfg)
	if err != nil {
		return nil, err
	}
	return decide(&cfg, m, s)
}

// decide applies the rule of the auto mode to the catalog values.
func decide(cfg *Config, requested string, s *Signals) (*Choice, error) {
	filtered := strings.TrimSpace(cfg.Filter) != ""
	c := &Choice{Requested: requested, Signals: s, filtered: filtered}
	u, _ := s.NotAllVisible()

	switch requested {
	case MethodSample:
		// A sample of the whole table gives too few bounds among the rows
		// that match a selective filter, and the parts of a partitioned
		// table are out of the scope of the sample.
		if filtered {
			return nil, errors.New("slicer: the sample cannot be used with a row filter (--table-filter, --against-origin or --until); use the cutter")
		}
		if s.Partitioned {
			return nil, errors.New("slicer: the sample cannot be used with a partitioned table or a table with child tables; use the cutter")
		}
		c.useSample(cfg, "chosen by the user")
		return c, nil
	case MethodCutter:
		c.useCutter(cfg, "chosen by the user")
		return c, nil
	}

	switch {
	case filtered:
		c.useCutter(cfg, "a row filter is set (--table-filter, --against-origin or --until)")
	case s.Partitioned:
		c.useCutter(cfg, "the table is partitioned or has child tables")
	case s.Tuples >= 0 && small(cfg, s):
		c.useCutter(cfg, fmt.Sprintf("the table is small (about %.0f rows)", s.Tuples))
	case s.Correlation == nil:
		c.addAdvice(analyzeAdvice)
		c.useCutter(cfg, "a primary key column has no statistics")
	case s.Tuples < 0:
		c.addAdvice(analyzeAdvice)
		c.useCutter(cfg, "the number of rows of the table is not known")
	case u <= minSampleNotAllVisible:
		c.useCutter(cfg, "few pages are not all-visible")
	case *s.Correlation >= maxSampleCorrelation:
		c.useCutter(cfg, "the key follows the heap order")
	default:
		c.useSample(cfg, "the key does not follow the heap order, and many pages are not all-visible")
	}
	return c, nil
}

const analyzeAdvice = "run ANALYZE on the table, so that ACE can choose the faster way to cut it"

// small is true for a table with fewer than minSampleBlocks blocks of rows.
// The time to cut it does not matter.
func small(cfg *Config, s *Signals) bool {
	return s.Tuples < float64(minSampleBlocks)*float64(cfg.BlockSize)
}

// useCutter sets the cutter and estimates its cost. On a large table with
// many pages that are not all-visible it also warns that the anchor node
// will be slow. The parent of a partitioned table has no pages of its own,
// so there is no estimate for it.
func (c *Choice) useCutter(cfg *Config, reason string) {
	c.Method = MethodCutter
	c.Reason = reason
	if c.Signals == nil || c.Signals.Partitioned || small(cfg, c.Signals) {
		return
	}
	u, _ := c.Signals.NotAllVisible()
	c.cutShare = u
	if u > slowCutNotAllVisible {
		c.SlowAnchor = true
		c.addAdvice("VACUUM of the table makes the cut faster")
	}
}

// addAdvice adds a hint to Advice.
func (c *Choice) addAdvice(a string) {
	if c.Advice != "" {
		c.Advice += "; "
	}
	c.Advice += a
}

// useSample sets the sample with its rate for the block size of cfg.
func (c *Choice) useSample(cfg *Config, reason string) {
	c.Method = MethodSample
	c.Reason = reason
	c.SamplePercent = blockSamplePercent(cfg.BlockSize)
}

// CheckSample looks at the sample of the anchor node before any block is
// hashed. Two cases make the blocks of a sample bad, and then the auto mode
// takes the cutter. A sample with fewer than minSampleKeys keys gives few
// bounds, so one block can hold most of the table. When rows with near keys
// share heap pages, the blocks are very uneven; the catalog does not show
// this, because a low correlation does not mean a random heap. An explicit
// sample is kept. switched is true when the cutter replaces the sample;
// warning is a line for the log, or empty.
func (c *Choice) CheckSample(cfg Config, s *Sample) (switched bool, warning string) {
	if s.keys < minSampleKeys {
		if c.Requested != MethodAuto {
			return false, ""
		}
		c.SamplePercent = 0
		c.useCutter(&cfg, fmt.Sprintf("the sample has only %d keys", s.keys))
		return true, ""
	}
	share := s.samePage
	c.SamePage = &share
	if !s.clustered() {
		return false, ""
	}
	what := fmt.Sprintf("the sample shows that %s of the sampled keys lie on the same heap page as the key before them", percent(share))
	if c.Requested != MethodAuto {
		return false, what + ", so the blocks will be very uneven; --slicer=cutter gives exact blocks"
	}
	c.SamplePercent = 0
	c.useCutter(&cfg, "rows with near keys share heap pages")
	return true, what + ", so its blocks would be very uneven; the cutter is used instead"
}

// SampleFailed is called when the sample query fails. The auto mode then
// takes the cutter, and SampleFailed returns true. An explicit sample is
// kept, and SampleFailed returns false: the run must fail with the error.
func (c *Choice) SampleFailed(cfg Config) bool {
	if c.Requested != MethodAuto {
		return false
	}
	c.SamplePercent = 0
	c.useCutter(&cfg, "the sample query failed")
	return true
}

// percent formats a share from 0 to 1 as a whole percent.
func percent(share float64) string {
	return fmt.Sprintf("%.0f%%", 100*share)
}

// signalText describes the catalog values for the log and for messages.
func (c *Choice) signalText() string {
	s := c.Signals
	if s == nil || s.Partitioned {
		// The parent of a partitioned table has no pages and no
		// statistics of its own, and the values of an inheritance parent
		// do not describe its children.
		return ""
	}
	corr := "unknown"
	if s.Correlation != nil {
		corr = fmt.Sprintf("%.2f", *s.Correlation)
	}
	u, fromMods := s.NotAllVisible()
	src := "from the visibility map"
	if fromMods {
		src = "from the rows changed since ANALYZE"
	}
	return fmt.Sprintf("|correlation| %s, pages not all-visible %s (%s)", corr, percent(u), src)
}

// Describe returns a one-line description of the choice for the log.
func (c *Choice) Describe(anchor string) string {
	b := &strings.Builder{}
	fmt.Fprintf(b, "slicer %s", c.Method)
	if c.Requested == MethodAuto {
		b.WriteString(" (auto)")
	}
	fmt.Fprintf(b, " on node %s: %s", anchor, c.Reason)
	if st := c.signalText(); st != "" {
		fmt.Fprintf(b, "; %s", st)
	}
	switch c.Method {
	case MethodSample:
		fmt.Fprintf(b, "; the sample reads %.4g%% of the heap pages", c.SamplePercent)
		if c.Signals != nil && c.Signals.Pages > 0 {
			fmt.Fprintf(b, " (about %d pages)", int64(math.Ceil(float64(c.Signals.Pages)*c.SamplePercent/100)))
		}
	case MethodCutter:
		if c.cutShare > minSampleNotAllVisible {
			fmt.Fprintf(b, "; the cut can add up to about %s to the hashing time of the anchor node", percent(c.cutShare))
		}
	}
	if c.SamePage != nil {
		fmt.Fprintf(b, "; sampled keys on the page of the key before them: %s", percent(*c.SamePage))
	}
	return b.String()
}

// sampleRandomKey is true when the catalog shows a key that does not follow
// the heap order, so the sample can give even blocks, and the run has no
// row filter, so the sample can be used.
func (c *Choice) sampleRandomKey() bool {
	if c.filtered {
		return false
	}
	return c.Signals != nil && !c.Signals.Partitioned &&
		c.Signals.Correlation != nil && *c.Signals.Correlation < maxSampleCorrelation &&
		(c.SamePage == nil || *c.SamePage <= maxSamePageShare)
}

// TimeoutAdvice explains a block that hit a time limit after a cut by the
// sample, and tells the user what to do. It is empty for the cutter.
func (c *Choice) TimeoutAdvice() string {
	if c == nil || c.Method != MethodSample {
		return ""
	}
	how := "the user chose the sample"
	if c.Requested == MethodAuto {
		how = "slicer auto chose the sample"
	}
	if st := c.signalText(); st != "" {
		how += ": " + st
	}
	return fmt.Sprintf("a block query hit its time limit after the table was cut by a sample (%s). "+
		"The blocks of a sample can be much larger than block_size if rows with near keys share heap pages. "+
		"Run the diff again with slicer set to cutter (--slicer=cutter): it is slower, but its blocks do not "+
		"depend on the order of rows in the table. VACUUM of the table makes the cutter faster.", how)
}

// SlowCutAdvice is the log line for a cut that was slower than the hashing.
// It advises VACUUM only when the catalog shows many pages that are not
// all-visible.
func (c *Choice) SlowCutAdvice(idleShare float64) string {
	s := fmt.Sprintf("the cut was slower than the hashing: the hash workers of every node waited for blocks at least %s of the time.",
		percent(idleShare))
	vacuum, sample := c.cutShare > slowCutNotAllVisible, c.sampleRandomKey()
	switch {
	case vacuum && sample:
		s += " The table has few all-visible pages. VACUUM of the table can help, or the sample (--slicer=sample)."
	case vacuum:
		s += " The table has few all-visible pages. VACUUM of the table can help."
	case sample:
		s += " The sample (--slicer=sample) can help."
	}
	return s
}
