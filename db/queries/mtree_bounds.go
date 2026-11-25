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

package queries

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/pgedge/ace/pkg/types"
)

// The functions in this file read and write key values of a table and the
// bounds of the leaves of its Merkle tree. A key value is the text of each
// key column, the output of its type: a query reads it in the text format and
// sends it back as $n::type. Do not decode a key value into a Go type
// or compare two of them in Go: the driver cannot decode every type, and Go
// does not know the order of a type and its collation.

// textResult asks for every column of a result in the text format, so that a
// key comes as the output of its type.
var textResult = pgx.QueryResultFormats{pgx.TextFormatCode}

// boundAttrs returns the select lists of range_start and range_end of a
// leaf: the column itself for a simple key, and every attribute of the
// composite for a composite key. A query that joins the tree table with the
// table must give the alias of the tree table: the table can have a column
// named range_start, and then an unqualified name is ambiguous.
func boundAttrs(alias string, simple bool, key []string) (start, end string) {
	if simple {
		return "range_start", "range_end"
	}
	prefix := ""
	if alias != "" {
		prefix = alias + "."
	}
	starts := make([]string, len(key))
	ends := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		starts[i] = "(" + prefix + "range_start)." + attr
		ends[i] = "(" + prefix + "range_end)." + attr
	}
	return strings.Join(starts, ", "), strings.Join(ends, ", ")
}

// boundValue returns the value of a bound written to a leaf, with parameters
// from $first, and its arguments: NULL for an open bound, the key value for a
// simple key and ROW(...) for a composite key. The ROW(...) needs no cast:
// assignment to the composite column of the tree converts it.
func boundValue(first int, key []string, castTypes map[string]string, b []string) (string, []any, error) {
	if b == nil {
		return "NULL", nil, nil
	}
	if len(b) != len(key) {
		return "", nil, fmt.Errorf("a key bound has %d values for %d key columns", len(b), len(key))
	}
	expr, err := KeyParams(first, key, castTypes)
	if err != nil {
		return "", nil, err
	}
	args := make([]any, len(b))
	for i, v := range b {
		args[i] = v
	}
	return expr, args, nil
}

// parseInt reads an integer column of a text-format result; what names the
// column in the error.
func parseInt(raw []byte, what string) (int64, error) {
	v, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", what, err)
	}
	return v, nil
}

// queryBlockRanges runs sql, whose rows are node_position, the n columns of
// range_start, the n columns of range_end and, with withCount, a row count.
// The first leaf has no lower bound.
func queryBlockRanges(ctx context.Context, db DBQuerier, sql string, args []any, n int, withCount bool) ([]types.BlockRangeWithCount, error) {
	rows, err := db.Query(ctx, sql, append([]any{textResult}, args...)...) // nosemgrep
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	width := 1 + 2*n
	if withCount {
		width++
	}
	var out []types.BlockRangeWithCount
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != width {
			return nil, fmt.Errorf("expected %d columns, got %d", width, len(raw))
		}
		pos, err := parseInt(raw[0], "node position")
		if err != nil {
			return nil, err
		}
		br := types.BlockRangeWithCount{
			NodePosition: pos,
			RangeStart:   OpenFirstBlockStart(pos, textBound(raw[1:1+n])),
			RangeEnd:     textBound(raw[1+n : 1+2*n]),
		}
		if withCount {
			if br.Count, err = parseInt(raw[1+2*n], "row count"); err != nil {
				return nil, err
			}
		}
		out = append(out, br)
	}
	return out, rows.Err()
}

// blockRanges returns the leaves of in without their row counts.
func blockRanges(in []types.BlockRangeWithCount) []types.BlockRange {
	out := make([]types.BlockRange, len(in))
	for i, b := range in {
		out[i] = types.BlockRange{NodePosition: b.NodePosition, RangeStart: b.RangeStart, RangeEnd: b.RangeEnd}
	}
	return out
}

// queryKeys runs sql, whose rows are the n key columns, and returns the keys.
func queryKeys(ctx context.Context, db DBQuerier, sql string, args []any, n int) ([][]string, error) {
	rows, err := db.Query(ctx, sql, append([]any{textResult}, args...)...) // nosemgrep
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]string
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) != n {
			return nil, fmt.Errorf("expected %d columns, got %d", n, len(raw))
		}
		out = append(out, textBound(raw))
	}
	return out, rows.Err()
}

// keyList returns the quoted key columns, each followed by suffix, separated
// by commas. suffix is SQL text, such as " DESC".
func keyList(key []string, suffix string) string {
	cols := make([]string, len(key))
	for i, k := range key {
		cols[i] = pgx.Identifier{k}.Sanitize() + suffix
	}
	return strings.Join(cols, ", ")
}

func tableIdent(schema, table string) string {
	return pgx.Identifier{schema, table}.Sanitize()
}

// GetDirtyAndNewBlocks returns the leaves that are dirty or have no hash.
func GetDirtyAndNewBlocks(ctx context.Context, db DBQuerier, mtreeTable string, simplePrimaryKey bool, key []string) ([]types.BlockRange, error) {
	start, end := boundAttrs("", simplePrimaryKey, key)
	sql, err := RenderSQL(SQLTemplates.GetDirtyAndNewBlocksExpanded, map[string]any{
		"MtreeTable": mtreeTable,
		"StartAttrs": start,
		"EndAttrs":   end,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to render GetDirtyAndNewBlocks SQL: %w", err)
	}
	blocks, err := queryBlockRanges(ctx, db, sql, nil, len(key), false)
	if err != nil {
		return nil, fmt.Errorf("query to get dirty and new blocks for '%s' failed: %w", mtreeTable, err)
	}
	return blockRanges(blocks), nil
}

// FindBlocksToSplit returns the leaves at nodePositions that got more than
// insertsSinceUpdate inserts.
func FindBlocksToSplit(ctx context.Context, db DBQuerier, mtreeTable string, insertsSinceUpdate int, nodePositions []int64, simplePrimaryKey bool, key []string) ([]types.BlockRange, error) {
	start, end := boundAttrs("", simplePrimaryKey, key)
	sql, err := RenderSQL(SQLTemplates.FindBlocksToSplitExpanded, map[string]any{
		"MtreeTable": mtreeTable,
		"StartAttrs": start,
		"EndAttrs":   end,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToSplit SQL: %w", err)
	}
	blocks, err := queryBlockRanges(ctx, db, sql, []any{insertsSinceUpdate, nodePositions}, len(key), false)
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTable, err)
	}
	return blockRanges(blocks), nil
}

// FindBlocksToMerge returns the leaves with fewer rows than mergeThreshold
// times the block size. With blockPositions, only these leaves and their
// neighbours are candidates.
func FindBlocksToMerge(ctx context.Context, db DBQuerier, mtreeTable string, simplePrimaryKey bool, schema string, table string, key []string, mergeThreshold float64, blockPositions []int64) ([]types.BlockRange, error) {
	var args []any
	usePositionFilter := len(blockPositions) > 0
	if usePositionFilter {
		posSet := make(map[int64]struct{}, len(blockPositions)*3)
		for _, p := range blockPositions {
			posSet[p] = struct{}{}
			if p > 0 {
				posSet[p-1] = struct{}{}
			}
			posSet[p+1] = struct{}{}
		}
		expanded := make([]int64, 0, len(posSet))
		for p := range posSet {
			expanded = append(expanded, p)
		}
		args = append(args, expanded)
	}

	blockSize, err := GetBlockSizeFromMetadata(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}
	args = append(args, float64(blockSize)*mergeThreshold)

	sanitizedKeys := make([]string, len(key))
	for i, k := range key {
		sanitizedKeys[i] = pgx.Identifier{k}.Sanitize()
	}
	// The templates differ in the comparison of a row with a leaf bound: the
	// column for a simple key, ROW(...) for a composite key.
	start, end := boundAttrs("", simplePrimaryKey, key)
	tmpl := SQLTemplates.FindBlocksToMergeExpanded
	if simplePrimaryKey {
		tmpl = SQLTemplates.FindBlocksToMerge
	}
	sql, err := RenderSQL(tmpl, map[string]any{
		"MtreeTable":          mtreeTable,
		"SimplePrimaryKey":    simplePrimaryKey,
		"SchemaIdent":         pgx.Identifier{schema}.Sanitize(),
		"TableIdent":          pgx.Identifier{table}.Sanitize(),
		"Key":                 sanitizedKeys,
		"StartAttrs":          start,
		"EndAttrs":            end,
		"UsePositionFilter":   usePositionFilter,
		"PositionPlaceholder": "$1",
		"MergeValPlaceholder": fmt.Sprintf("$%d", len(args)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToMerge SQL: %w", err)
	}
	blocks, err := queryBlockRanges(ctx, db, sql, args, len(key), false)
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTable, err)
	}
	return blockRanges(blocks), nil
}

// GetBlockWithCount returns the leaf at position and the number of its rows,
// or nil if there is no such leaf.
func GetBlockWithCount(ctx context.Context, db DBQuerier, mtreeTable, schema, table string, key []string, isComposite bool, position int64) (*types.BlockRangeWithCount, error) {
	sanitizedKeys := make([]string, len(key))
	for i, k := range key {
		sanitizedKeys[i] = pgx.Identifier{k}.Sanitize()
	}
	start, end := boundAttrs("t1", !isComposite, key)
	tmpl := SQLTemplates.GetBlockWithCountExpanded
	if !isComposite {
		tmpl = SQLTemplates.GetBlockWithCount
	}
	sql, err := RenderSQL(tmpl, map[string]any{
		"MtreeTable":  mtreeTable,
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"IsComposite": isComposite,
		"Key":         sanitizedKeys,
		"StartAttrs":  start,
		"EndAttrs":    end,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockWithCount SQL: %w", err)
	}
	blocks, err := queryBlockRanges(ctx, db, sql, []any{position}, len(key), true)
	if err != nil {
		return nil, fmt.Errorf("query to get block %d of '%s' failed: %w", position, mtreeTable, err)
	}
	if len(blocks) == 0 {
		return nil, nil
	}
	return &blocks[0], nil
}

// InsertBlockRange adds a leaf at nodePosition with the bounds start and
// end; nil is an open bound.
func InsertBlockRange(ctx context.Context, db DBQuerier, mtreeTable string, key []string, castTypes map[string]string, nodePosition int64, start, end []string) error {
	return InsertBlockRanges(ctx, db, mtreeTable, key, castTypes, []types.BlockRange{
		{NodePosition: nodePosition, RangeStart: start, RangeEnd: end},
	})
}

// InsertBlockRanges adds the ranges of the leaves, several of them in one
// statement.
func InsertBlockRanges(ctx context.Context, db DBQuerier, mtreeTable string, key []string, castTypes map[string]string, ranges []types.BlockRange) error {
	// A statement takes at most 65535 parameters.
	const maxParams = 60000
	chunk := max(maxParams/(1+2*len(key)), 1)
	for from := 0; from < len(ranges); from += chunk {
		to := min(from+chunk, len(ranges))
		values := make([]string, 0, to-from)
		var args []any
		for _, r := range ranges[from:to] {
			args = append(args, r.NodePosition)
			pos := len(args)
			start, sargs, err := boundValue(len(args)+1, key, castTypes, r.RangeStart)
			if err != nil {
				return err
			}
			args = append(args, sargs...)
			end, eargs, err := boundValue(len(args)+1, key, castTypes, r.RangeEnd)
			if err != nil {
				return err
			}
			args = append(args, eargs...)
			values = append(values, fmt.Sprintf("(0, $%d, %s, %s, current_timestamp)", pos, start, end))
		}
		sql := fmt.Sprintf("INSERT INTO %s (node_level, node_position, range_start, range_end, last_modified) VALUES %s",
			mtreeTable, strings.Join(values, ", "))
		if _, err := db.Exec(ctx, sql, args...); err != nil { // nosemgrep
			return fmt.Errorf("insert block ranges into '%s' failed: %w", mtreeTable, err)
		}
	}
	return nil
}

// updateBound sets one bound of the leaf at nodePosition and marks the leaf
// dirty.
func updateBound(ctx context.Context, db DBQuerier, mtreeTable, column string, key []string, castTypes map[string]string, b []string, nodePosition int64) error {
	val, args, err := boundValue(2, key, castTypes, b)
	if err != nil {
		return err
	}
	sql := fmt.Sprintf(`UPDATE %s SET %s = %s, dirty = true, last_modified = current_timestamp
		WHERE node_level = 0 AND node_position = $1`, mtreeTable, column, val)
	if _, err := db.Exec(ctx, sql, append([]any{nodePosition}, args...)...); err != nil { // nosemgrep
		return fmt.Errorf("update of %s of block %d in '%s' failed: %w", column, nodePosition, mtreeTable, err)
	}
	return nil
}

// UpdateBlockRangeEnd sets the upper bound of the leaf at nodePosition; nil
// is no bound.
func UpdateBlockRangeEnd(ctx context.Context, db DBQuerier, mtreeTable string, key []string, castTypes map[string]string, end []string, nodePosition int64) error {
	return updateBound(ctx, db, mtreeTable, "range_end", key, castTypes, end, nodePosition)
}

// UpdateBlockRangeStart sets the lower bound of the leaf at nodePosition.
func UpdateBlockRangeStart(ctx context.Context, db DBQuerier, mtreeTable string, key []string, castTypes map[string]string, start []string, nodePosition int64) error {
	return updateBound(ctx, db, mtreeTable, "range_start", key, castTypes, start, nodePosition)
}

// GetMaxKey returns the largest key not less than start, or the largest key
// of the table if start is nil. It returns nil if there is no such key.
func GetMaxKey(ctx context.Context, db DBQuerier, schema, table string, key []string, castTypes map[string]string, start []string) ([]string, error) {
	where, args, err := KeyRange{Lower: start}.Cond("", key, castTypes, 1)
	if err != nil {
		return nil, err
	}
	// Each key column DESC, not the ROW: the scan walks the primary key index
	// backward.
	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT 1",
		keyList(key, ""), tableIdent(schema, table), where, keyList(key, " DESC"))
	keys, err := queryKeys(ctx, db, sql, args, len(key))
	if err != nil {
		return nil, fmt.Errorf("query to get the largest key of '%s.%s' failed: %w", schema, table, err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return keys[0], nil
}

// GetMinKey returns the smallest key of the table, or nil if it is empty.
func GetMinKey(ctx context.Context, db DBQuerier, schema, table string, key []string) ([]string, error) {
	sql := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT 1",
		keyList(key, ""), tableIdent(schema, table), keyList(key, ""))
	keys, err := queryKeys(ctx, db, sql, nil, len(key))
	if err != nil {
		return nil, fmt.Errorf("query to get the smallest key of '%s.%s' failed: %w", schema, table, err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return keys[0], nil
}

// GetBlockRowCount returns the number of rows in r.
func GetBlockRowCount(ctx context.Context, db DBQuerier, schema, table string, key []string, castTypes map[string]string, r KeyRange) (int64, error) {
	where, args, err := r.Cond("", key, castTypes, 1)
	if err != nil {
		return 0, err
	}
	sql := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s", tableIdent(schema, table), where)
	var count int64
	if err := db.QueryRow(ctx, sql, args...).Scan(&count); err != nil { // nosemgrep
		return 0, fmt.Errorf("query to get block row count for '%s.%s' failed: %w", schema, table, err)
	}
	return count, nil
}

// GetBulkSplitPoints returns every blockSize-th key in r without the first
// key: the starts of the new leaves when r is cut into leaves of blockSize
// rows.
func GetBulkSplitPoints(ctx context.Context, db DBQuerier, schema, table string, key []string, castTypes map[string]string, r KeyRange, blockSize int) ([][]string, error) {
	where, args, err := r.Cond("", key, castTypes, 1)
	if err != nil {
		return nil, err
	}
	args = append(args, blockSize)
	sql, err := RenderSQL(SQLTemplates.GetBulkSplitPoints, map[string]any{
		"SchemaIdent":          pgx.Identifier{schema}.Sanitize(),
		"TableIdent":           pgx.Identifier{table}.Sanitize(),
		"PkeyColsStr":          keyList(key, ""),
		"WhereClause":          where,
		"BlockSizePlaceholder": fmt.Sprintf("$%d", len(args)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBulkSplitPoints SQL: %w", err)
	}
	points, err := queryKeys(ctx, db, sql, args, len(key))
	if err != nil {
		return nil, fmt.Errorf("query to get split points of '%s.%s' failed: %w", schema, table, err)
	}
	return points, nil
}

// ComputeLeafHashes returns the hash of the rows in [start, end) as an mtree
// leaf stores it, computed in the hash form of the node. nil is an open
// side.
func ComputeLeafHashes(ctx context.Context, db DBQuerier, schema, table string, key []string, castTypes map[string]string, start, end []string, allCols []string, form HashForm) ([]byte, error) {
	where, args, err := BlockWhere(key, castTypes, KeyRange{Lower: start, Upper: end}, "")
	if err != nil {
		return nil, err
	}
	sql, err := BlockHashSQL(schema, table, "MTREE_LEAF_HASH", where, allCols, form)
	if err != nil {
		return nil, err
	}
	var leafHash []byte
	if err := db.QueryRow(ctx, sql, args...).Scan(&leafHash); err != nil { // nosemgrep
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("query to compute leaf hashes for '%s.%s' returned no row", schema, table)
		}
		return nil, fmt.Errorf("query to compute leaf hashes for '%s.%s' failed: %w", schema, table, err)
	}
	return leafHash, nil
}
