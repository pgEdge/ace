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

// Package keydiff finds the rows that differ between two nodes in one block
// of a table, after the block hashes of the two nodes did not match.
//
// Each node scans the block once and sends only the key and a short hash of
// every row. The keys and hashes of the node with fewer rows go into a map;
// the rows of the other node are read as a stream and checked against it. The
// full rows are then read by key, only for the keys that differ. The client
// compares bytes for equality only: it never orders key values and never
// converts them to Go types.
package keydiff

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/types"
)

// defaultBatchSize is the number of keys in one row read.
const defaultBatchSize = 2000

// Querier runs a query. Use a *pgxpool.Pool: when the comparison fails, it
// cancels a key query that is not at its end, and pgx then closes the
// connection. A pool replaces it; a single connection or a transaction would
// be lost.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Table describes the compared table.
type Table struct {
	Schema string
	Name   string
	// Key is the list of primary key columns, in the order of the
	// constraint.
	Key []string
	// Cols is the list of all columns, in the order of the diff report.
	Cols []string
	// HashTypes are the column types (format_type) used by the row hash. They
	// must be the same for both nodes and the same as for the block hash, or
	// equal rows get different hashes.
	HashTypes map[string]string
	// ScanChildren tells that the table has inheritance children that the
	// primary key index of the parent does not cover. The row read then lets
	// the planner join the list of keys with the table in one pass, instead
	// of one lookup per key, which would scan such a child once for every
	// key.
	ScanChildren bool
}

// Side is one node of the comparison.
type Side struct {
	Name string
	// Conn must not be nil.
	Conn Querier
	// ColTypes are the column types (format_type) on this node. The row read
	// casts the key texts to them and uses them for its select list.
	ColTypes map[string]string
	// Rows is the number of rows of the block on this node, from the block
	// hash. It is used only if RowsKnown is true.
	Rows      int64
	RowsKnown bool
	// KeyWidth is the average size of the key of one row, in bytes, from
	// the statistics of this node. 0 means it is not known.
	KeyWidth int64
}

// rowsOrUnknown returns Rows, or -1 if it is not known.
func (s Side) rowsOrUnknown() int64 {
	if !s.RowsKnown {
		return -1
	}
	return s.Rows
}

// Block is the part of the table that both nodes hashed.
type Block struct {
	// Where is the WHERE clause of the block, with the same meaning on both
	// nodes. It may use the parameters $1..$n, with values in Args.
	Where string
	Args  []any
	// ReadFilter, if not empty, is added to the WHERE clause of the row read
	// by key. It must not use parameters.
	ReadFilter string
}

// Options limit one comparison.
type Options struct {
	// MaxMemory is the limit, in bytes, of the memory for the keys of the
	// block. 0 means no limit.
	MaxMemory int64
	// BatchSize is the number of keys in one row read. 0 means 2000.
	BatchSize int
}

// Kind is the kind of a differing row.
type Kind int

const (
	// KindOnly1 is a row that exists only on the first node.
	KindOnly1 Kind = iota + 1
	// KindOnly2 is a row that exists only on the second node.
	KindOnly2
	// KindChanged is a row that exists on both nodes with different values.
	KindChanged
)

// Entry is one differing row. Row1 and Hash1 are set for KindOnly1 and
// KindChanged, Row2 and Hash2 for KindOnly2 and KindChanged. Each row has
// the columns commit_ts and node_origin, then the columns of the table.
type Entry struct {
	Kind  Kind
	Row1  types.OrderedMap
	Row2  types.OrderedMap
	Hash1 RowHash
	Hash2 RowHash
}

// Sink receives the differing rows of one row read. Compare calls it once
// for each batch of keys that gives at least one entry, and does not keep
// the entries after the call. An error from the sink stops the comparison
// and is returned by Compare.
type Sink func(entries []Entry) error

// RowFunc changes a differing row of one node before it is written.
type RowFunc func(row types.OrderedMap) types.OrderedMap

// BlockSink returns a sink that writes the entries to out. row1 and row2
// change each row of the first and of the second node before it is written.
func BlockSink(out *utils.DiffBlockWriter, row1, row2 RowFunc) Sink {
	return func(entries []Entry) error {
		for _, e := range entries {
			var err error
			switch e.Kind {
			case KindOnly1:
				err = out.Add(utils.DiffKindNode1Only, row1(e.Row1), nil, e.Hash1[:], nil)
			case KindOnly2:
				err = out.Add(utils.DiffKindNode2Only, nil, row2(e.Row2), nil, e.Hash2[:])
			default:
				err = out.Add(utils.DiffKindChanged, row1(e.Row1), row2(e.Row2), e.Hash1[:], e.Hash2[:])
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
}

// Result counts the rows that differ.
type Result struct {
	// Only1 and Only2 are the numbers of rows that exist only on the first
	// or only on the second node; Changed is the number of rows that exist
	// on both nodes with different values.
	Only1   int
	Only2   int
	Changed int
	// BuildNode is the name of the node whose keys went into the map.
	BuildNode string
	// DiffKeys is the number of keys that the key queries found different.
	// It can be more than the number of rows in the result if rows changed
	// during the comparison.
	DiffKeys int
}

// Count returns the number of differing rows, with a changed row counted
// once.
func (r Result) Count() int {
	return r.Only1 + r.Only2 + r.Changed
}

// Compare finds the rows of block that differ between the nodes s1 and s2
// and gives them to sink, one batch of the row read at a time. A
// *MemoryLimitError means that the keys of the block need more memory than
// Options.MaxMemory.
func Compare(ctx context.Context, t Table, b Block, s1, s2 Side, o Options, sink Sink) (Result, error) {
	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	build, probe := s1, s2
	if s1.RowsKnown && s2.RowsKnown && s2.Rows < s1.Rows {
		build, probe = s2, s1
	}

	if err := checkEstimate(t, build, o); err != nil {
		return Result{}, err
	}

	query, err := t.keyQuery(b.Where)
	if err != nil {
		return Result{}, err
	}
	mem := &memCounter{limit: o.MaxMemory}

	ids, err := runFindDiffKeys(ctx, t, query, b.Args, build, probe, mem)
	if err != nil {
		var me *errMemory
		if errors.As(err, &me) {
			e := &MemoryLimitError{BuildNode: build.Name, BuildRows: build.rowsOrUnknown(), Used: me.used, Limit: o.MaxMemory}
			if !me.build {
				e.ProbeNode = probe.Name
			}
			return Result{}, e
		}
		return Result{}, err
	}
	res := Result{BuildNode: build.Name}
	res.DiffKeys = len(ids)
	if len(ids) == 0 {
		return res, nil
	}

	seen1 := make(map[string]struct{})
	seen2 := make(map[string]struct{})
	var entries []Entry
	for start := 0; start < len(ids); start += o.BatchSize {
		end := min(start+o.BatchSize, len(ids))
		batch := ids[start:end]
		rows1, err := readRows(ctx, t, b.ReadFilter, s1, batch, seen1)
		if err != nil {
			return Result{}, err
		}
		rows2, err := readRows(ctx, t, b.ReadFilter, s2, batch, seen2)
		if err != nil {
			return Result{}, err
		}
		entries = classify(len(batch), rows1, rows2, entries[:0], &res)
		if len(entries) == 0 {
			continue
		}
		if err := sink(entries); err != nil {
			return Result{}, err
		}
		clear(entries)
	}
	return res, nil
}

// KeyWidth returns the average size of the primary key of one row of
// schema.table, in bytes: the sum of avg_width of the key columns in
// pg_stats. It returns 0 if a key column has no statistics.
//
// The result is only a lower bound of the size of the key ID, which holds
// the text of the key: avg_width is the size of the stored value, which is
// smaller than its text for most types (a uuid is 16 bytes stored and 36 as
// text, a bigint 8 and up to 20) and is measured after compression. So the
// check before the build stops only a block that is clearly too large; the
// memory counter during the comparison does not depend on it.
func KeyWidth(ctx context.Context, q Querier, schema, table string, key []string) (int64, error) {
	const query = `SELECT count(*), coalesce(sum(w), 0)::bigint FROM (
		SELECT attname, max(avg_width) AS w FROM pg_catalog.pg_stats
		WHERE schemaname = $1 AND tablename = $2 AND attname = ANY($3::text[])
		GROUP BY attname) AS s`
	rows, err := q.Query(ctx, query, schema, table, key)
	if err != nil {
		return 0, fmt.Errorf("failed to read key statistics of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var cols, width int64
	if rows.Next() {
		if err := rows.Scan(&cols, &width); err != nil {
			return 0, fmt.Errorf("failed to read key statistics of %s.%s: %w", schema, table, err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("failed to read key statistics of %s.%s: %w", schema, table, err)
	}
	if cols < int64(len(key)) {
		return 0, nil
	}
	return width, nil
}

// HasInheritanceChildren reports whether schema.table is a plain table with
// inheritance children. The primary key index of such a table does not cover
// its children, so Table.ScanChildren should be set. The partitions of a
// partitioned table do not count: each of them has the index.
func HasInheritanceChildren(ctx context.Context, q Querier, schema, table string) (bool, error) {
	const query = `SELECT EXISTS (
		SELECT 1 FROM pg_catalog.pg_inherits i
		JOIN pg_catalog.pg_class p ON p.oid = i.inhparent
		JOIN pg_catalog.pg_namespace n ON n.oid = p.relnamespace
		WHERE n.nspname = $1 AND p.relname = $2 AND p.relkind <> 'p')`
	rows, err := q.Query(ctx, query, schema, table)
	if err != nil {
		return false, fmt.Errorf("failed to read the inheritance children of %s.%s: %w", schema, table, err)
	}
	defer rows.Close()
	var has bool
	if rows.Next() {
		if err := rows.Scan(&has); err != nil {
			return false, fmt.Errorf("failed to read the inheritance children of %s.%s: %w", schema, table, err)
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("failed to read the inheritance children of %s.%s: %w", schema, table, err)
	}
	return has, nil
}

// checkEstimate stops the comparison before the build side is read if the
// statistics say that its keys need more memory than the limit.
func checkEstimate(t Table, build Side, o Options) error {
	if o.MaxMemory <= 0 || !build.RowsKnown || build.Rows <= 0 || build.KeyWidth <= 0 {
		return nil
	}
	// One length byte for each key column is the usual size of the length
	// prefix of a key ID.
	perRow := build.KeyWidth + int64(len(t.Key)) + rowHashSize + entryOverhead
	est := build.Rows * perRow
	if est <= o.MaxMemory {
		return nil
	}
	return &MemoryLimitError{BuildNode: build.Name, BuildRows: build.Rows, Used: est, Limit: o.MaxMemory, Estimated: true}
}

// runFindDiffKeys runs the key query on both sides and finds the differing
// keys. A build side that is known to have no rows is not queried.
func runFindDiffKeys(ctx context.Context, t Table, query string, args []any, build, probe Side, mem *memCounter) ([]string, error) {
	var buildSrc keySource
	if !build.RowsKnown || build.Rows != 0 {
		src, err := openKeySource(ctx, t, query, args, build)
		if err != nil {
			return nil, err
		}
		defer src.close()
		buildSrc = src
	}
	probeSrc, err := openKeySource(ctx, t, query, args, probe)
	if err != nil {
		return nil, err
	}
	defer probeSrc.close()

	// A key query is sent on the first call of Next, so the probe query
	// starts only when the whole build side is in the map, and its
	// connection is not busy while the build side is read.
	return findDiffKeys(buildSrc, probeSrc, mem)
}

// keyRows reads the rows of a key query.
type keyRows struct {
	ctx    context.Context
	cancel context.CancelFunc
	conn   Querier
	query  string
	args   []any
	t      Table
	side   string

	rows  pgx.Rows
	cur   keyRow
	texts [][]byte
	err   error
	done  bool
	// complete is true when all rows of the query were read.
	complete bool
}

// openKeySource returns the rows of the key query of side s. The query is
// sent on the first call of Next. It runs under its own context, so that it
// can be cancelled alone.
func openKeySource(ctx context.Context, t Table, query string, args []any, s Side) (*keyRows, error) {
	if s.Conn == nil {
		return nil, fmt.Errorf("no connection for node %s", s.Name)
	}
	qctx, cancel := context.WithCancel(ctx)
	return &keyRows{ctx: qctx, cancel: cancel, conn: s.Conn, query: query, args: args, t: t, side: s.Name,
		texts: make([][]byte, len(t.Key))}, nil
}

// Next reads the next row of the key query and sends the query first if it
// was not sent yet. It returns false after the last row or on an error; Err
// then returns the error, and complete is true only if all rows were read.
func (k *keyRows) Next() bool {
	if k.done {
		return false
	}
	if k.rows == nil {
		qargs := make([]any, 0, len(k.args)+1)
		qargs = append(qargs, k.t.keyQueryFormats())
		qargs = append(qargs, k.args...)
		rows, err := k.conn.Query(k.ctx, k.query, qargs...) // nosemgrep
		if err != nil {
			k.err = fmt.Errorf("key query failed on node %s: %w", k.side, err)
			k.done = true
			return false
		}
		k.rows = rows
	}
	if !k.rows.Next() {
		k.done = true
		if err := k.rows.Err(); err != nil {
			k.err = fmt.Errorf("key query failed on node %s: %w", k.side, err)
		} else {
			k.complete = true
		}
		return false
	}
	raw := k.rows.RawValues()
	n := len(k.t.Key)
	if len(raw) != n+1 {
		k.err = fmt.Errorf("key query on node %s returned %d columns, want %d", k.side, len(raw), n+1)
		k.done = true
		return false
	}
	for i := 0; i < n; i++ {
		k.texts[i] = raw[i]
	}
	id, err := encodeKeyID(k.texts)
	if err != nil {
		k.err = fmt.Errorf("key query on node %s: %w", k.side, err)
		k.done = true
		return false
	}
	h := raw[n]
	if len(h) != rowHashSize {
		k.err = fmt.Errorf("key query on node %s returned a row hash of %d bytes, want %d", k.side, len(h), rowHashSize)
		k.done = true
		return false
	}
	k.cur.id = id
	copy(k.cur.hash[:], h)
	return true
}

func (k *keyRows) Row() keyRow { return k.cur }

func (k *keyRows) Err() error { return k.err }

// close ends the query. If the rows were not all read, the query is
// cancelled on the server first, so that closing it does not read the rest
// of the block. The cancel closes the connection.
func (k *keyRows) close() {
	if k.rows != nil {
		if !k.complete {
			k.cancel()
		}
		k.rows.Close()
	}
	k.cancel()
}

// readRow is one row of the row read.
type readRow struct {
	hash RowHash
	row  types.OrderedMap
}

// readRows reads the full rows for the keys ids on one node. The result is
// indexed by the position of the key in ids. A key can have no row (the row
// was deleted) or more than one row (an inheritance tree). seen holds the
// rows (table OID and ctid) already returned for other keys of the same
// comparison; such a row is not returned again.
func readRows(ctx context.Context, t Table, filter string, s Side, ids []string, seen map[string]struct{}) ([][]readRow, error) {
	query, err := t.ReadQuery(s.ColTypes, filter)
	if err != nil {
		return nil, err
	}
	n := len(t.Key)
	cols := make([][]string, n)
	for i := range cols {
		cols[i] = make([]string, len(ids))
	}
	for j, id := range ids {
		texts, err := decodeKeyID(id, n)
		if err != nil {
			return nil, err
		}
		for i := range texts {
			cols[i][j] = texts[i]
		}
	}
	args := make([]any, n)
	for i := range cols {
		args[i] = cols[i]
	}

	rows, err := s.Conn.Query(ctx, query, args...) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("row read by key failed on node %s: %w", s.Name, err)
	}
	defer rows.Close()

	const first = 4 // position, row hash, table OID, ctid
	out := make([][]readRow, len(ids))
	for rows.Next() {
		// The row is read in the same form as every other row read for a
		// diff report. The service columns keep their Go types in this form.
		row, err := utils.ScanRow(rows)
		if err != nil {
			return nil, fmt.Errorf("row read by key on node %s: %w", s.Name, err)
		}
		if len(row) < first {
			return nil, fmt.Errorf("row read by key on node %s returned %d columns", s.Name, len(row))
		}
		pos, ok := row[0].Value.(int64)
		if !ok || pos < 1 || pos > int64(len(ids)) {
			return nil, fmt.Errorf("row read by key on node %s returned a bad key position %v", s.Name, row[0].Value)
		}
		hb, ok := row[1].Value.([]byte)
		if !ok || len(hb) != rowHashSize {
			return nil, fmt.Errorf("row read by key on node %s returned a bad row hash", s.Name)
		}
		where := fmt.Sprintf("%v/%v", row[2].Value, row[3].Value)
		if _, dup := seen[where]; dup {
			continue
		}
		seen[where] = struct{}{}
		var r readRow
		copy(r.hash[:], hb)
		// A copy, so that the service columns are not kept with the row.
		r.row = append(make(types.OrderedMap, 0, len(row)-first), row[first:]...)
		out[pos-1] = append(out[pos-1], r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row read by key failed on node %s: %w", s.Name, err)
	}
	return out, nil
}

// classify appends the differing rows of one batch to entries and counts
// them in res. For each key, the rows with the same hash on both nodes
// cancel out. Of the rows left, as many as possible are paired as changed
// rows, and the rest exist on one node only. A key whose rows all cancel out
// (the row changed back, or was deleted on both nodes, during the
// comparison) adds nothing.
func classify(n int, rows1, rows2 [][]readRow, entries []Entry, res *Result) []Entry {
	for i := 0; i < n; i++ {
		left1, left2 := cancelEqual(rows1[i], rows2[i])
		for len(left1) > 0 && len(left2) > 0 {
			entries = append(entries, Entry{Kind: KindChanged, Row1: left1[0].row, Row2: left2[0].row,
				Hash1: left1[0].hash, Hash2: left2[0].hash})
			res.Changed++
			left1, left2 = left1[1:], left2[1:]
		}
		for _, r := range left1 {
			entries = append(entries, Entry{Kind: KindOnly1, Row1: r.row, Hash1: r.hash})
			res.Only1++
		}
		for _, r := range left2 {
			entries = append(entries, Entry{Kind: KindOnly2, Row2: r.row, Hash2: r.hash})
			res.Only2++
		}
	}
	return entries
}

// cancelEqual removes the pairs of rows with the same hash from a and b.
func cancelEqual(a, b []readRow) ([]readRow, []readRow) {
	if len(a) == 0 || len(b) == 0 {
		return a, b
	}
	used := make([]bool, len(b))
	var left1 []readRow
	for _, ra := range a {
		matched := false
		for j, rb := range b {
			if !used[j] && rb.hash == ra.hash {
				used[j] = true
				matched = true
				break
			}
		}
		if !matched {
			left1 = append(left1, ra)
		}
	}
	var left2 []readRow
	for j, rb := range b {
		if !used[j] {
			left2 = append(left2, rb)
		}
	}
	return left1, left2
}
