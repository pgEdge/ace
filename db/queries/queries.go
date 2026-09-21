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
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgedge/ace/pkg/types"
)

// CurrentHashVersion is the version of the hash algorithm used by this build.
// Increment when the SQL hash computation changes (e.g., switching from
// whole-row ::text to per-column concat_ws with trim_scale).
const CurrentHashVersion = 2

type DBQuerier interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
	Query(context.Context, string, ...interface{}) (pgx.Rows, error)
	QueryRow(context.Context, string, ...interface{}) pgx.Row
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
}

var validIdentifierRegex = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func SanitiseIdentifier(ident string) error {
	if !validIdentifierRegex.MatchString(ident) {
		return fmt.Errorf("invalid identifier: %s", ident)
	}
	return nil
}

// CommitTimestampFilter returns a SQL predicate that restricts rows to those
// committed at or before the given timestamp. Frozen rows (where
// pg_xact_commit_timestamp returns NULL after VACUUM FREEZE) are always
// included. Returns an empty string when t is nil.
func CommitTimestampFilter(t *time.Time) string {
	if t == nil {
		return ""
	}
	return fmt.Sprintf("(pg_xact_commit_timestamp(xmin) IS NULL OR pg_xact_commit_timestamp(xmin) <= '%s'::timestamptz)", t.Format(time.RFC3339Nano))
}

func RenderSQL(t *template.Template, data any) (string, error) {
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to render SQL: %w", err)
	}
	return buf.String(), nil
}

func MaxColumnSize(ctx context.Context, db DBQuerier, schema, table, column string) (int64, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"ColumnIdent": pgx.Identifier{column}.Sanitize(),
	}

	query, err := RenderSQL(SQLTemplates.GetMaxColumnSize, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render MaxColumnSize SQL: %w", err)
	}

	var maxSize int64
	if err := db.QueryRow(ctx, query).Scan(&maxSize); err != nil {
		return 0, fmt.Errorf(
			"MaxColumnSize query failed for %s.%s.%s: %w",
			schema,
			table,
			column,
			err,
		)
	}

	return maxSize, nil
}

func GeneratePkeyOffsetsQuery(
	schema, table string,
	keyColumns []string,
	tableSampleMethod string,
	samplePercent float64,
	ntileCount int,
	filter string,
) (string, error) {
	if len(keyColumns) == 0 {
		return "", fmt.Errorf("keyColumns cannot be empty")
	}
	for _, ident := range append([]string{schema, table}, keyColumns...) {
		if err := SanitiseIdentifier(ident); err != nil {
			return "", fmt.Errorf("invalid identifier %q: %w", ident, err)
		}
	}
	schemaIdent := pgx.Identifier{schema}.Sanitize()
	tableIdent := pgx.Identifier{table}.Sanitize()

	quotedKeyColsOriginal := make([]string, len(keyColumns))
	for i, c := range keyColumns {
		quotedKeyColsOriginal[i] = pgx.Identifier{c}.Sanitize()
	}

	keyColsSelect := strings.Join(quotedKeyColsOriginal, ",\n        ")
	keyColsOrder := strings.Join(quotedKeyColsOriginal, ", ")

	var descs []string
	for _, c := range keyColumns {
		descs = append(descs, fmt.Sprintf("%s DESC", pgx.Identifier{c}.Sanitize()))
	}
	keyColsOrderDesc := strings.Join(descs, ", ")

	var firstSelects, lastSelects, firstTuples []string
	for _, c := range keyColumns {
		quotedCol := pgx.Identifier{c}.Sanitize()
		firstSelects = append(firstSelects,
			fmt.Sprintf(`(SELECT %s FROM first_row) AS %s`, quotedCol, quotedCol))
		lastSelects = append(lastSelects,
			fmt.Sprintf(`(SELECT %s FROM last_row) AS %s`, quotedCol, quotedCol))
		firstTuples = append(firstTuples,
			fmt.Sprintf(`(SELECT %s FROM first_row)`, quotedCol))
	}

	var rangeStarts, rangeEnds []string
	for _, c := range keyColumns {
		quotedCol := pgx.Identifier{c}.Sanitize()
		aliasStart := fmt.Sprintf(`range_start_%s`, c)
		quotedAliasStart := pgx.Identifier{aliasStart}.Sanitize()

		aliasEnd := fmt.Sprintf(`range_end_%s`, c)
		quotedAliasEnd := pgx.Identifier{aliasEnd}.Sanitize()

		rangeStarts = append(rangeStarts, fmt.Sprintf(`%s AS %s`, quotedCol, quotedAliasStart))
		rangeEnds = append(rangeEnds, fmt.Sprintf(
			`LEAD(%s) OVER (ORDER BY seq, %s) AS %s`,
			quotedCol, keyColsOrder, quotedAliasEnd,
		))
	}

	var startComponentCols []string
	var endComponentCols []string
	for _, c := range keyColumns {
		aliasStart := fmt.Sprintf(`range_start_%s`, c)
		quotedAliasStart := pgx.Identifier{aliasStart}.Sanitize()
		startComponentCols = append(startComponentCols, quotedAliasStart)

		aliasEnd := fmt.Sprintf(`range_end_%s`, c)
		quotedAliasEnd := pgx.Identifier{aliasEnd}.Sanitize()
		endComponentCols = append(endComponentCols, quotedAliasEnd)
	}
	selectOutputCols := append(startComponentCols, endComponentCols...)

	data := map[string]any{
		"SchemaIdent":          schemaIdent,
		"TableIdent":           tableIdent,
		"TableSampleMethod":    tableSampleMethod,
		"SamplePercent":        samplePercent,
		"NtileCount":           ntileCount,
		"KeyColumnsSelect":     keyColsSelect,
		"KeyColumnsOrder":      keyColsOrder,
		"KeyColumnsOrderDesc":  keyColsOrderDesc,
		"FirstRowSelects":      strings.Join(firstSelects, ",\n        "),
		"LastRowSelects":       strings.Join(lastSelects, ",\n        "),
		"FirstRowTupleSelects": fmt.Sprintf("ROW(%s)", strings.Join(firstTuples, ",\n        ")),
		"RangeStartColumns":    strings.Join(rangeStarts, ",\n        "),
		"RangeEndColumns":      strings.Join(rangeEnds, ",\n        "),
		"RangeOutputColumns":   strings.Join(selectOutputCols, ",\n    "),
		"HasFilter":            strings.TrimSpace(filter) != "",
		"Filter":               strings.TrimSpace(filter),
	}

	return RenderSQL(SQLTemplates.GetPkeyOffsets, data)
}

func CreateXORFunction(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.CreateXORFunction, nil)
	if err != nil {
		return fmt.Errorf("failed to render CreateXORFunction SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create xor function failed: %w", err)
	}

	return nil
}

func CreateMetadataTable(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.CreateMetadataTable, nil)
	if err != nil {
		return fmt.Errorf("failed to render CreateMetadataTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create metadata table failed: %w", err)
	}

	return nil
}

func CreateCDCMetadataTable(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.CreateCDCMetadataTable, nil)
	if err != nil {
		return fmt.Errorf("failed to render CreateCDCMetadataTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create cdc metadata table failed: %w", err)
	}

	return nil
}

func CreateSimpleMtreeTable(ctx context.Context, db DBQuerier, mtreeTable, pkeyType string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
		"PkeyType":   pkeyType,
	}

	sql, err := RenderSQL(SQLTemplates.CreateSimpleMtreeTable, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateSimpleMtreeTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create simple mtree table for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func DropCompositeType(ctx context.Context, db DBQuerier, compositeTypeName string) error {
	data := map[string]interface{}{
		"CompositeTypeName": compositeTypeName,
	}

	sql, err := RenderSQL(SQLTemplates.DropCompositeType, data)
	if err != nil {
		return fmt.Errorf("failed to render DropCompositeType SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop composite type '%s' failed: %w", compositeTypeName, err)
	}

	return nil
}

func CreateCompositeType(ctx context.Context, db DBQuerier, compositeTypeName, keyTypeColumns string) error {
	data := map[string]interface{}{
		"CompositeTypeName": compositeTypeName,
		"KeyTypeColumns":    keyTypeColumns,
	}

	sql, err := RenderSQL(SQLTemplates.CreateCompositeType, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateCompositeType SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create composite type '%s' failed: %w", compositeTypeName, err)
	}

	return nil
}

func CreateCompositeMtreeTable(ctx context.Context, db DBQuerier, mtreeTable, compositeTypeName string) error {
	data := map[string]interface{}{
		"MtreeTable":        mtreeTable,
		"CompositeTypeName": compositeTypeName,
	}

	sql, err := RenderSQL(SQLTemplates.CreateCompositeMtreeTable, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateCompositeMtreeTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create composite mtree table for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func InsertBlockRanges(ctx context.Context, db DBQuerier, mtreeTable string, nodePosition int64, rangeStart, rangeEnd interface{}) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.InsertBlockRanges, data)
	if err != nil {
		return fmt.Errorf("failed to render InsertBlockRanges SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, nodePosition, rangeStart, rangeEnd)
	if err != nil {
		return fmt.Errorf("query to insert block ranges for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func InsertCompositeBlockRanges(ctx context.Context, db DBQuerier, mtreeTable string, nodePosition int64, startVals, endVals []any) error {
	startPh := make([]string, len(startVals))
	args := make([]any, 0, 1+len(startVals)+len(endVals))
	args = append(args, nodePosition)
	argIdx := 2
	for i := range startVals {
		startPh[i] = fmt.Sprintf("$%d", argIdx)
		args = append(args, startVals[i])
		argIdx++
	}

	var endExpr string
	if endVals == nil {
		endExpr = "NULL"
	} else {
		endPh := make([]string, len(endVals))
		for i := range endVals {
			endPh[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, endVals[i])
			argIdx++
		}
		endExpr = fmt.Sprintf("ROW(%s)", strings.Join(endPh, ", "))
	}
	startExpr := fmt.Sprintf("ROW(%s)", strings.Join(startPh, ", "))

	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
		"StartExpr":  startExpr,
		"EndExpr":    endExpr,
	}

	stmt, err := RenderSQL(SQLTemplates.InsertCompositeBlockRanges, data)
	if err != nil {
		return fmt.Errorf("failed to render InsertCompositeBlockRanges SQL: %w", err)
	}

	if _, err := db.Exec(ctx, stmt, args...); err != nil { // nosemgrep
		return fmt.Errorf("query to insert composite block ranges for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func InsertBlockRangesBatchSimple(ctx context.Context, db DBQuerier, mtreeTable string, ranges []types.BlockRange) error {
	if len(ranges) == 0 {
		return nil
	}

	const maxParams = 60000
	const paramsPerRow = 3
	chunkSize := maxParams / paramsPerRow
	if chunkSize < 1 {
		chunkSize = 1
	}

	for start := 0; start < len(ranges); start += chunkSize {
		end := start + chunkSize
		if end > len(ranges) {
			end = len(ranges)
		}

		type rowPlaceholders struct {
			NodePos string
			Start   string
			End     string
		}

		rowsMeta := make([]rowPlaceholders, 0, end-start)
		args := make([]any, 0, (end-start)*paramsPerRow)
		paramIdx := 1
		for i := start; i < end; i++ {
			r := ranges[i]
			rowsMeta = append(rowsMeta, rowPlaceholders{
				NodePos: fmt.Sprintf("$%d", paramIdx),
				Start:   fmt.Sprintf("$%d", paramIdx+1),
				End:     fmt.Sprintf("$%d", paramIdx+2),
			})
			paramIdx += 3

			args = append(args, r.NodePosition)
			var rs any
			var re any
			if len(r.RangeStart) > 0 {
				rs = r.RangeStart[0]
			}
			if len(r.RangeEnd) > 0 {
				re = r.RangeEnd[0]
			}
			args = append(args, rs, re)
		}

		data := map[string]any{
			"MtreeTable": mtreeTable,
			"Rows":       rowsMeta,
		}

		sql, err := RenderSQL(SQLTemplates.InsertBlockRangesBatchSimple, data)
		if err != nil {
			return fmt.Errorf("failed to render InsertBlockRangesBatchSimple SQL: %w", err)
		}

		if _, err := db.Exec(ctx, sql, args...); err != nil { // nosemgrep
			return fmt.Errorf("batch insert block ranges for '%s' failed: %w", mtreeTable, err)
		}
	}

	return nil
}

func InsertBlockRangesBatchComposite(ctx context.Context, db DBQuerier, mtreeTable string, ranges []types.BlockRange, keyLen int) error {
	if len(ranges) == 0 {
		return nil
	}

	if keyLen <= 0 {
		return fmt.Errorf("invalid keyLen")
	}

	const maxParams = 60000
	paramsPerRow := 1 + 2*keyLen
	if paramsPerRow <= 0 {
		paramsPerRow = 1
	}
	chunkSize := maxParams / paramsPerRow
	if chunkSize < 1 {
		chunkSize = 1
	}

	for start := 0; start < len(ranges); start += chunkSize {
		end := start + chunkSize
		if end > len(ranges) {
			end = len(ranges)
		}

		type rowPlaceholders struct {
			NodePos   string
			StartList string
			EndList   string
		}

		rowsMeta := make([]rowPlaceholders, 0, end-start)
		args := make([]any, 0, (end-start)*paramsPerRow)
		paramIdx := 1
		for i := start; i < end; i++ {
			r := ranges[i]
			nodePos := fmt.Sprintf("$%d", paramIdx)
			args = append(args, r.NodePosition)
			paramIdx++

			startPh := make([]string, keyLen)
			for k := 0; k < keyLen; k++ {
				startPh[k] = fmt.Sprintf("$%d", paramIdx)
				var v any
				if k < len(r.RangeStart) {
					v = r.RangeStart[k]
				}
				args = append(args, v)
				paramIdx++
			}

			endPh := make([]string, keyLen)
			for k := 0; k < keyLen; k++ {
				endPh[k] = fmt.Sprintf("$%d", paramIdx)
				var v any
				if k < len(r.RangeEnd) {
					v = r.RangeEnd[k]
				}
				args = append(args, v)
				paramIdx++
			}

			rowsMeta = append(rowsMeta, rowPlaceholders{
				NodePos:   nodePos,
				StartList: strings.Join(startPh, ", "),
				EndList:   strings.Join(endPh, ", "),
			})
		}

		data := map[string]any{
			"MtreeTable": mtreeTable,
			"Rows":       rowsMeta,
		}

		sql, err := RenderSQL(SQLTemplates.InsertBlockRangesBatchComposite, data)
		if err != nil {
			return fmt.Errorf("failed to render InsertBlockRangesBatchComposite SQL: %w", err)
		}

		if _, err := db.Exec(ctx, sql, args...); err != nil { // nosemgrep
			return fmt.Errorf("batch insert composite block ranges for '%s' failed: %w", mtreeTable, err)
		}
	}

	return nil
}

func GetPkeyOffsets(ctx context.Context, db DBQuerier, schema, table string, keyColumns []string, tableSampleMethod string, samplePercent float64, ntileCount int) ([]types.PkeyOffset, error) {
	sql, err := GeneratePkeyOffsetsQuery(schema, table, keyColumns, tableSampleMethod, samplePercent, ntileCount, "")
	if err != nil {
		return nil, fmt.Errorf("failed to generate GetPkeyOffsets SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to get pkey offsets for '%s.%s' failed: %w", schema, table, err)
	}
	defer rows.Close()

	var offsets []types.PkeyOffset
	numKeyCols := len(keyColumns)
	for rows.Next() {
		values := make([]interface{}, numKeyCols*2)
		valuePtrs := make([]interface{}, numKeyCols*2)
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan pkey offset: %w", err)
		}

		offset := types.PkeyOffset{
			RangeStart: values[:numKeyCols],
			RangeEnd:   values[numKeyCols:],
		}
		offsets = append(offsets, offset)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over pkey offsets: %w", err)
	}

	return offsets, nil
}

// isNumericType returns true if a PostgreSQL type string represents a numeric/decimal type.
func isNumericType(colType string) bool {
	lower := strings.ToLower(colType)
	return strings.HasPrefix(lower, "numeric") || strings.HasPrefix(lower, "decimal")
}

// buildRowTextExpr builds a SQL expression that converts a single row to text.
// When allCols is provided, it returns concat_ws('|', col1_expr, col2_expr, ...)
// with numeric/decimal columns wrapped in trim_scale() to normalize trailing zeros.
// If allCols is nil/empty, falls back to the table-alias::text whole-row cast.
//
// PostgreSQL limits functions to 100 arguments. Since concat_ws uses 1 argument
// for the separator, at most 99 column expressions fit per call. For wider
// tables, the expressions are batched into nested concat_ws calls.
func buildRowTextExpr(tableAlias string, allCols []string, colTypes map[string]string) string {
	if len(allCols) == 0 {
		return tableAlias + "::text"
	}

	exprs := make([]string, len(allCols))
	for i, col := range allCols {
		quoted := pgx.Identifier{col}.Sanitize()
		qualifiedCol := tableAlias + "." + quoted
		if colTypes != nil && isNumericType(colTypes[col]) {
			exprs[i] = fmt.Sprintf("COALESCE(trim_scale(%s)::text, '')", qualifiedCol)
		} else {
			exprs[i] = fmt.Sprintf("COALESCE(%s::text, '')", qualifiedCol)
		}
	}
	return concatWSBatched(exprs)
}

// concatWSBatched produces a concat_ws('|', ...) expression. When len(exprs)
// exceeds 99 (the max value-arguments per concat_ws call, since the separator
// takes one slot), it splits the expressions into batches and nests the calls.
func concatWSBatched(exprs []string) string {
	const maxArgs = 99 // 100 total - 1 for the separator

	if len(exprs) <= maxArgs {
		return fmt.Sprintf("concat_ws('|', %s)", strings.Join(exprs, ", "))
	}

	// Split into batches, wrap each in its own concat_ws, then combine.
	var batches []string
	for i := 0; i < len(exprs); i += maxArgs {
		end := i + maxArgs
		if end > len(exprs) {
			end = len(exprs)
		}
		batches = append(batches, fmt.Sprintf("concat_ws('|', %s)", strings.Join(exprs[i:end], ", ")))
	}
	return fmt.Sprintf("concat_ws('|', %s)", strings.Join(batches, ", "))
}

func BlockHashSQL(schema, table string, primaryKeyCols []string, mode string, includeLower, includeUpper bool, filter string, allCols []string, colTypes map[string]string) (string, error) {
	if len(primaryKeyCols) == 0 {
		return "", fmt.Errorf("primaryKeyCols cannot be empty")
	}
	if err := SanitiseIdentifier(schema); err != nil {
		return "", err
	}
	if err := SanitiseIdentifier(table); err != nil {
		return "", err
	}

	for _, pkCol := range primaryKeyCols {
		if pkCol == "" {
			return "", fmt.Errorf("primary key column identifier cannot be empty")
		}
		if err := SanitiseIdentifier(pkCol); err != nil {
			return "", fmt.Errorf("invalid primary key column identifier %q: %w", pkCol, err)
		}
	}

	schemaIdent := pgx.Identifier{schema}.Sanitize()
	tableIdent := pgx.Identifier{table}.Sanitize()
	tableAlias := "_tbl_"

	quotedPKColIdents := make([]string, len(primaryKeyCols))
	for i, pkCol := range primaryKeyCols {
		quotedPKColIdents[i] = pgx.Identifier{pkCol}.Sanitize()
	}
	pkOrderByStr := strings.Join(quotedPKColIdents, ", ")

	pkComparisonExpression := ""
	if len(primaryKeyCols) == 1 {
		pkComparisonExpression = quotedPKColIdents[0]
	} else {
		pkComparisonExpression = fmt.Sprintf("ROW(%s)", strings.Join(quotedPKColIdents, ", "))
	}

	paramIndex := 1
	whereParts := make([]string, 0, 2)

	if includeLower {
		startPlaceholders := make([]string, len(primaryKeyCols))
		for i := range primaryKeyCols {
			startPlaceholders[i] = fmt.Sprintf("$%d", paramIndex)
			paramIndex++
		}
		var lowerExpr string
		if len(primaryKeyCols) == 1 {
			lowerExpr = fmt.Sprintf("%s >= %s", pkComparisonExpression, startPlaceholders[0])
		} else {
			lowerExpr = fmt.Sprintf("%s >= ROW(%s)", pkComparisonExpression, strings.Join(startPlaceholders, ", "))
		}
		whereParts = append(whereParts, lowerExpr)
	}

	if includeUpper {
		endPlaceholders := make([]string, len(primaryKeyCols))
		for i := range primaryKeyCols {
			endPlaceholders[i] = fmt.Sprintf("$%d", paramIndex)
			paramIndex++
		}
		// Upper bound is exclusive: range_end is the next block's range_start
		// (from LEAD in the build offsets and from split points), so each row
		// belongs to exactly one leaf.
		operator := "<"
		var upperExpr string
		if len(primaryKeyCols) == 1 {
			upperExpr = fmt.Sprintf("%s %s %s", pkComparisonExpression, operator, endPlaceholders[0])
		} else {
			upperExpr = fmt.Sprintf("%s %s ROW(%s)", pkComparisonExpression, operator, strings.Join(endPlaceholders, ", "))
		}
		whereParts = append(whereParts, upperExpr)
	}

	if trimmed := strings.TrimSpace(filter); trimmed != "" {
		whereParts = append(whereParts, fmt.Sprintf("(%s)", trimmed))
	}

	if len(whereParts) == 0 {
		whereParts = append(whereParts, "TRUE")
	}

	var tmpl *template.Template
	switch mode {
	case "TD_BLOCK_HASH":
		tmpl = SQLTemplates.TDBlockHashSQL
	case "MTREE_LEAF_HASH":
		tmpl = SQLTemplates.MtreeLeafHashSQL
	default:
		return "", fmt.Errorf("invalid mode: %s", mode)
	}

	rowTextExpr := buildRowTextExpr(tableAlias, allCols, colTypes)

	data := map[string]any{
		"SchemaIdent":  schemaIdent,
		"TableIdent":   tableIdent,
		"TableAlias":   tableAlias,
		"PkOrderByStr": pkOrderByStr,
		"WhereClause":  strings.Join(whereParts, " AND "),
		"RowTextExpr":  rowTextExpr,
	}
	return RenderSQL(tmpl, data)
}

// GetColumns retrieves the column names for a given table.
func GetColumns(ctx context.Context, db DBQuerier, schema, table string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetColumns, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetColumns SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query to get columns failed for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan column name: %w", err)
		}
		columns = append(columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns: %w", err)
	}

	if len(columns) == 0 {
		return nil, nil
	}

	return columns, nil
}

func GetPrimaryKey(ctx context.Context, db DBQuerier, schema, table string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPrimaryKey, nil)
	if err != nil {
		return nil, err
	}
	rows, err := db.Query(ctx, sql, schema, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(keys) == 0 {
		return nil, nil
	}

	return keys, nil
}

func GetSimplePrimaryKey(ctx context.Context, db DBQuerier, schema, table string) (bool, error) {
	keys, err := GetPrimaryKey(ctx, db, schema, table)
	if err != nil {
		return false, err
	}
	return len(keys) == 1, nil
}

func GetColumnTypes(ctx context.Context, db DBQuerier, schema, table string) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetColumnTypes, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetColumnTypes SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, table)
	if err != nil {
		return nil, fmt.Errorf("query to get column types failed for %s.%s: %w", schema, table, err)
	}
	defer rows.Close()

	types := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return nil, fmt.Errorf("failed to scan column type: %w", err)
		}
		types[columnName] = dataType
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over column types: %w", err)
	}

	if len(types) == 0 {
		return nil, fmt.Errorf("could not fetch column types for %s.%s", schema, table)
	}

	return types, nil
}

// TODO: add Spock privilege checks.
func CheckUserPrivileges(ctx context.Context, db DBQuerier, username, schema, table string) (*types.UserPrivileges, error) {
	sql, err := RenderSQL(SQLTemplates.CheckUserPrivileges, nil)
	if err != nil {
		return nil, err
	}

	var privileges types.UserPrivileges
	err = db.QueryRow(ctx, sql, username, schema, table).Scan(
		&privileges.TableSelect,
		&privileges.TableCreate,
		&privileges.TableInsert,
		&privileges.TableUpdate,
		&privileges.TableDelete,
		&privileges.ColumnsSelect,
		&privileges.TableConstraintsSelect,
		&privileges.KeyColumnUsageSelect,
	)
	if err != nil {
		return nil, err
	}

	return &privileges, nil
}

func GetSpockNodeAndSubInfo(ctx context.Context, db DBQuerier) ([]types.SpockNodeAndSubInfo, error) {
	sql, err := RenderSQL(SQLTemplates.SpockNodeAndSubInfo, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var infos []types.SpockNodeAndSubInfo
	for rows.Next() {
		var info types.SpockNodeAndSubInfo
		if err := rows.Scan(
			&info.NodeID,
			&info.NodeName,
			&info.Location,
			&info.Country,
			&info.SubID,
			&info.SubName,
			&info.SubEnabled,
			&info.SubReplicationSets,
			&info.SubOriginName,
		); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return infos, nil
}

func GetSpockNodeNames(ctx context.Context, db DBQuerier) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetSpockNodeNames, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make(map[string]string)
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

func GetSpockOriginLSNForNode(ctx context.Context, db DBQuerier, originNodeName string) (*string, error) {
	sql, err := RenderSQL(SQLTemplates.GetSpockOriginLSNForNode, nil)
	if err != nil {
		return nil, err
	}
	var lsn *string
	if err := db.QueryRow(ctx, sql, originNodeName).Scan(&lsn); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch spock origin lsn: %w", err)
	}
	return lsn, nil
}

func GetSpockSlotLSNForNode(ctx context.Context, db DBQuerier, failedNode string) (*string, error) {
	sql, err := RenderSQL(SQLTemplates.GetSpockSlotLSNForNode, nil)
	if err != nil {
		return nil, err
	}
	var lsn *string
	if err := db.QueryRow(ctx, sql, failedNode).Scan(&lsn); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch spock slot lsn: %w", err)
	}
	return lsn, nil
}

func GetNativeOriginLSNForNode(ctx context.Context, db DBQuerier, originNodeName string) (*string, error) {
	sql, err := RenderSQL(SQLTemplates.GetNativeOriginLSNForNode, nil)
	if err != nil {
		return nil, err
	}
	var lsn *string
	if err := db.QueryRow(ctx, sql, originNodeName).Scan(&lsn); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch native origin lsn: %w", err)
	}
	return lsn, nil
}

func GetNativeSlotLSNForNode(ctx context.Context, db DBQuerier, failedNode string) (*string, error) {
	sql, err := RenderSQL(SQLTemplates.GetNativeSlotLSNForNode, nil)
	if err != nil {
		return nil, err
	}
	var lsn *string
	if err := db.QueryRow(ctx, sql, failedNode).Scan(&lsn); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to fetch native slot lsn: %w", err)
	}
	return lsn, nil
}

func GetReplicationOriginNames(ctx context.Context, db DBQuerier) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetReplicationOriginNames, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make(map[string]string)
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

// GetNativeNodeOriginNames maps replication origin IDs to subscription names
// for native PG logical replication (no spock). This is the native PG
// equivalent of GetSpockNodeNames.
func GetNativeNodeOriginNames(ctx context.Context, db DBQuerier) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetNativeNodeOriginNames, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make(map[string]string)
	for rows.Next() {
		var id, name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		names[id] = name
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return names, nil
}

func GetNodeOriginNames(ctx context.Context, db DBQuerier) (map[string]string, error) {
	var spockAvailable bool
	err := db.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'spock')").Scan(&spockAvailable)
	if err != nil {
		return nil, fmt.Errorf("detecting spock extension: %w", err)
	}
	if spockAvailable {
		return GetSpockNodeNames(ctx, db)
	}
	return GetNativeNodeOriginNames(ctx, db)
}

func CheckSpockInstalled(ctx context.Context, db DBQuerier) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'spock')").Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("detecting spock extension: %w", err)
	}
	return exists, nil
}

func GetSpockRepSetInfo(ctx context.Context, db DBQuerier) ([]types.SpockRepSetInfo, error) {
	sql, err := RenderSQL(SQLTemplates.SpockRepSetInfo, nil)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var infos []types.SpockRepSetInfo
	for rows.Next() {
		var info types.SpockRepSetInfo
		if err := rows.Scan(
			&info.SetName,
			&info.RelName,
		); err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return infos, nil
}

func EnsurePgcrypto(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.EnsurePgcrypto, nil)
	if err != nil {
		return fmt.Errorf("failed to render ensure-pgcrypto SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql); err != nil {
		return fmt.Errorf("failed to ensure pgcrypto extension: %w", err)
	}
	return nil
}

func CheckSchemaExists(ctx context.Context, db DBQuerier, schema string) (bool, error) {
	sql, err := RenderSQL(SQLTemplates.CheckSchemaExists, nil)
	if err != nil {
		return false, fmt.Errorf("failed to render CheckSchemaExists SQL: %w", err)
	}

	var exists bool
	err = db.QueryRow(ctx, sql, schema).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query to check if schema '%s' exists failed: %w", schema, err)
	}

	return exists, nil
}

func GetTablesInSchema(ctx context.Context, db DBQuerier, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetTablesInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetTablesInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get tables in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over tables: %w", err)
	}

	return tables, nil
}

// GetForeignTablesInSchema lists the foreign tables in a schema so callers
// can say which tables a schema diff skipped.
func GetForeignTablesInSchema(ctx context.Context, db DBQuerier, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetForeignTablesInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetForeignTablesInSchema SQL: %w", err)
	}
	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get foreign tables in schema %s failed: %w", schema, err)
	}
	defer rows.Close()
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan foreign table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

func GetViewsInSchema(ctx context.Context, db DBQuerier, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetViewsInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetViewsInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get views in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var views []string
	for rows.Next() {
		var viewName string
		if err := rows.Scan(&viewName); err != nil {
			return nil, fmt.Errorf("failed to scan view name: %w", err)
		}
		views = append(views, viewName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over views: %w", err)
	}

	return views, nil
}

func GetFunctionsInSchema(ctx context.Context, db DBQuerier, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetFunctionsInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetFunctionsInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get functions in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var functions []string
	for rows.Next() {
		var funcName string
		if err := rows.Scan(&funcName); err != nil {
			return nil, fmt.Errorf("failed to scan function name: %w", err)
		}
		functions = append(functions, funcName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over functions: %w", err)
	}

	return functions, nil
}

func GetIndicesInSchema(ctx context.Context, db DBQuerier, schema string) ([]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetIndicesInSchema, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetIndicesInSchema SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema)
	if err != nil {
		return nil, fmt.Errorf("query to get indices in schema '%s' failed: %w", schema, err)
	}
	defer rows.Close()

	var indices []string
	for rows.Next() {
		var indexName string
		if err := rows.Scan(&indexName); err != nil {
			return nil, fmt.Errorf("failed to scan index name: %w", err)
		}
		indices = append(indices, indexName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over indices: %w", err)
	}

	return indices, nil
}

func CheckRepSetExists(ctx context.Context, db DBQuerier, repSet string) (bool, error) {
	sql, err := RenderSQL(SQLTemplates.CheckRepSetExists, nil)
	if err != nil {
		return false, fmt.Errorf("failed to render CheckRepSetExists SQL: %w", err)
	}

	var exists bool
	err = db.QueryRow(ctx, sql, repSet).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("query to check if rep set '%s' exists failed: %w", repSet, err)
	}

	return exists, nil
}

// QualifiedName is a relation's schema and name kept apart, so a schema or
// name containing a dot cannot be mistaken for the separator.
type QualifiedName struct {
	Schema string
	Name   string
}

// String renders schema.name without quoting, the form the rest of ACE
// passes around as a qualified table name.
func (q QualifiedName) String() string {
	return q.Schema + "." + q.Name
}

// GetTablesInRepSet lists the relations Spock has in a replication set.
func GetTablesInRepSet(ctx context.Context, db DBQuerier, repSet string) ([]QualifiedName, error) {
	sql, err := RenderSQL(SQLTemplates.GetTablesInRepSet, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetTablesInRepSet SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, repSet)
	if err != nil {
		return nil, fmt.Errorf("query to get tables in rep set '%s' failed: %w", repSet, err)
	}
	defer rows.Close()

	var tables []QualifiedName
	for rows.Next() {
		var q QualifiedName
		if err := rows.Scan(&q.Schema, &q.Name); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, q)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over tables: %w", err)
	}

	return tables, nil
}

func GetRowCountEstimate(ctx context.Context, db DBQuerier, schema, table string) (int64, error) {
	sql, err := RenderSQL(SQLTemplates.EstimateRowCount, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to render EstimateRowCount SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get row count estimate for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetPkeyColumnTypes(ctx context.Context, db DBQuerier, schema, table string, pkeys []string) (map[string]string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPkeyColumnTypes, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetPkeyColumnTypes SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, table, pkeys)
	if err != nil {
		return nil, fmt.Errorf("query to get pkey column types for '%s.%s' failed: %w", schema, table, err)
	}
	defer rows.Close()

	types := make(map[string]string)
	for rows.Next() {
		var colName, colType string
		if err := rows.Scan(&colName, &colType); err != nil {
			return nil, fmt.Errorf("failed to scan pkey column type: %w", err)
		}
		types[colName] = colType
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over pkey column types: %w", err)
	}

	return types, nil
}

// ColumnDescriptor is one column's structural properties for schema
// structure comparison. It excludes attnum: column order is not part of
// structural identity, and callers must not rely on row order for anything
// but display.
type ColumnDescriptor struct {
	Table    string
	Name     string
	TypeOID  uint32 // this node's own type OID; local-only lookup key for domain/range/composite/enum descriptors, see GetColumnDescriptors
	TypeMod  int32
	TypeText string
	// TypeNamespace/TypeName/TypeKind are the portable identity of the
	// column's type (namespace.typname plus typtype); structural comparison
	// keys on these fields.
	TypeNamespace string
	TypeName      string
	TypeKind      string // 'b' base | 'd' domain | 'e' enum | 'r' range | 'c' composite
	NotNull       bool
	Identity      string // '' | 'a' (always) | 'd' (by default)
	Generated     string // '' | 's' (stored)
	Options       string // attoptions, sorted and comma-joined so apply order cannot differ; opaque to this layer
	CollNamespace string
	CollName      string
	CollProvider  string
	// CollVersion is the version the catalog recorded for this collation,
	// not the version of the collation library in use now; see
	// GetColumnDescriptors' SQL and GetDatabaseLocale.
	CollVersion string
	DefaultExpr string
}

// GetColumnDescriptors reads the structural properties of every live column
// of the given tables in one round trip. See the GetColumnDescriptors SQL
// template for exactly which catalog fields are read and why.
func GetColumnDescriptors(ctx context.Context, db DBQuerier, schema string, tables []string) (map[string][]ColumnDescriptor, error) {
	sql, err := RenderSQL(SQLTemplates.GetColumnDescriptors, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetColumnDescriptors SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, tables)
	if err != nil {
		return nil, fmt.Errorf("query to get column descriptors for schema %q failed: %w", schema, err)
	}
	defer rows.Close()

	result := make(map[string][]ColumnDescriptor)
	for rows.Next() {
		var c ColumnDescriptor
		if err := rows.Scan(&c.Table, &c.Name, &c.TypeOID, &c.TypeMod, &c.TypeText,
			&c.TypeNamespace, &c.TypeName, &c.TypeKind,
			&c.NotNull, &c.Identity, &c.Generated, &c.Options,
			&c.CollNamespace, &c.CollName, &c.CollProvider, &c.CollVersion,
			&c.DefaultExpr); err != nil {
			return nil, fmt.Errorf("failed to scan column descriptor: %w", err)
		}
		result[c.Table] = append(result[c.Table], c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over column descriptors: %w", err)
	}
	return result, nil
}

// ReplicaIdentityDescriptor is the table's row-identity mode together with
// the key the mode resolves to. KeyColumns/KeyOpclasses describe the primary
// key when ReplicaIdentity is "d" and the designated index when it is "i";
// both are empty for "f" (the whole row is the identity) and "n" (there is
// none), which ReplicaIdentity itself already says.
type ReplicaIdentityDescriptor struct {
	Table           string
	ReplicaIdentity string // "d" default | "n" nothing | "f" full | "i" specific index
	KeyColumns      []string
	KeyOpclasses    []string
	// KeyLength is the designated index's indnkeyatts. It must equal
	// len(KeyColumns): the query joins each key column to pg_attribute and
	// pg_opclass, and a join that fails would drop a column from the key
	// silently, turning a real key difference into an apparent match.
	KeyLength int
}

// GetReplicaIdentityKey reads each table's replica identity mode and, when
// it names an explicit index, that index's key columns and operator
// classes, in index-column order.
func GetReplicaIdentityKey(ctx context.Context, db DBQuerier, schema string, tables []string) (map[string]ReplicaIdentityDescriptor, error) {
	sql, err := RenderSQL(SQLTemplates.GetReplicaIdentityKey, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetReplicaIdentityKey SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, tables)
	if err != nil {
		return nil, fmt.Errorf("query to get replica identity keys for schema %q failed: %w", schema, err)
	}
	defer rows.Close()

	result := make(map[string]ReplicaIdentityDescriptor)
	for rows.Next() {
		var d ReplicaIdentityDescriptor
		if err := rows.Scan(&d.Table, &d.ReplicaIdentity, &d.KeyColumns, &d.KeyOpclasses, &d.KeyLength); err != nil {
			return nil, fmt.Errorf("failed to scan replica identity descriptor: %w", err)
		}
		// A short key is a wrong key, and a wrong key that still compares
		// equal to the other node's is worse than an error. See KeyLength.
		if len(d.KeyColumns) != d.KeyLength || len(d.KeyOpclasses) != d.KeyLength {
			return nil, fmt.Errorf(
				"replica identity key for table %q resolved to %d column(s) and %d operator class(es), but the index declares %d key column(s)",
				d.Table, len(d.KeyColumns), len(d.KeyOpclasses), d.KeyLength)
		}
		result[d.Table] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over replica identity descriptors: %w", err)
	}
	return result, nil
}

// ConstraintDescriptor is one PRIMARY KEY, UNIQUE, CHECK, FOREIGN KEY or
// EXCLUDE constraint. It deliberately has no Name field: PostgreSQL invents
// names for unnamed constraints, so identical constraints on two nodes can
// carry different names with no structural difference at all. Comparison
// must go by Definition.
type ConstraintDescriptor struct {
	Table      string
	Type       string // "p" | "u" | "c" | "f" | "x"
	Definition string
	Deferrable bool
	Validated  bool
}

// GetConstraintDescriptors reads every PRIMARY KEY, UNIQUE, CHECK, FOREIGN
// KEY and EXCLUDE constraint on the given tables.
func GetConstraintDescriptors(ctx context.Context, db DBQuerier, schema string, tables []string) (map[string][]ConstraintDescriptor, error) {
	sql, err := RenderSQL(SQLTemplates.GetConstraintDescriptors, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetConstraintDescriptors SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, tables)
	if err != nil {
		return nil, fmt.Errorf("query to get constraint descriptors for schema %q failed: %w", schema, err)
	}
	defer rows.Close()

	result := make(map[string][]ConstraintDescriptor)
	for rows.Next() {
		var c ConstraintDescriptor
		if err := rows.Scan(&c.Table, &c.Type, &c.Definition, &c.Deferrable, &c.Validated); err != nil {
			return nil, fmt.Errorf("failed to scan constraint descriptor: %w", err)
		}
		result[c.Table] = append(result[c.Table], c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over constraint descriptors: %w", err)
	}
	return result, nil
}

// PartitionDescriptor says whether a table is itself a partition (and of
// what bound) and/or is itself partitioned (and by what key). Empty strings
// mean "not applicable", not "unknown".
type PartitionDescriptor struct {
	Table          string
	PartitionBound string
	PartitionKey   string
}

// GetPartitionDescriptors reads partition bound and partition key
// information for the given tables.
func GetPartitionDescriptors(ctx context.Context, db DBQuerier, schema string, tables []string) (map[string]PartitionDescriptor, error) {
	sql, err := RenderSQL(SQLTemplates.GetPartitionDescriptors, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetPartitionDescriptors SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, schema, tables)
	if err != nil {
		return nil, fmt.Errorf("query to get partition descriptors for schema %q failed: %w", schema, err)
	}
	defer rows.Close()

	result := make(map[string]PartitionDescriptor)
	for rows.Next() {
		var d PartitionDescriptor
		if err := rows.Scan(&d.Table, &d.PartitionBound, &d.PartitionKey); err != nil {
			return nil, fmt.Errorf("failed to scan partition descriptor: %w", err)
		}
		result[d.Table] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over partition descriptors: %w", err)
	}
	return result, nil
}

// DomainDescriptor is what a domain (pg_type.typtype = 'd') constrains,
// resolved one level of typbasetype deep. BaseTypeNamespace/BaseTypeName/
// BaseTypeMod carry the same portable (namespace, name, typmod) shape as a
// column's own type, so the same comparison logic applies to both. OID
// appears only as the map key (see GetDomainDescriptors); it is local to
// this node.
type DomainDescriptor struct {
	Namespace         string
	Name              string
	BaseTypeNamespace string
	BaseTypeName      string
	// BaseTypeKind is the base type's typtype, part of its identity: see
	// GetDomainDescriptors' base_kind.
	BaseTypeKind string
	BaseTypeMod  int32
	// BaseTypeText is the base type as format_type prints it, modifier
	// included, for display (e.g. "character varying(20)"); nothing
	// compares it (see GetDomainDescriptors).
	BaseTypeText string
	NotNull      bool
	Default      string
	Checks       []string // CHECK definitions, sorted by definition text (see GetDomainDescriptors)
}

// GetDomainDescriptors resolves every domain named by oids, keyed by that
// OID. oids must come from this node's own GetColumnDescriptors result
// (or a recursive domain-of-domain lookup on this node); the map keys are
// local to this node — DomainDescriptor's fields are what get compared
// across nodes.
func GetDomainDescriptors(ctx context.Context, db DBQuerier, oids []uint32) (map[uint32]DomainDescriptor, error) {
	result := make(map[uint32]DomainDescriptor)
	if len(oids) == 0 {
		return result, nil
	}

	sql, err := RenderSQL(SQLTemplates.GetDomainDescriptors, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetDomainDescriptors SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, oids)
	if err != nil {
		return nil, fmt.Errorf("query to get domain descriptors failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid uint32
		var d DomainDescriptor
		if err := rows.Scan(&oid, &d.Namespace, &d.Name, &d.BaseTypeNamespace, &d.BaseTypeName,
			&d.BaseTypeKind, &d.BaseTypeMod, &d.BaseTypeText, &d.NotNull, &d.Default,
			&d.Checks); err != nil {
			return nil, fmt.Errorf("failed to scan domain descriptor: %w", err)
		}
		result[oid] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over domain descriptors: %w", err)
	}
	return result, nil
}

// RangeDescriptor is what makes a range type (pg_type.typtype = 'r') mean
// what it means: its element type and the collation/opclass/functions that
// order and canonicalise it. Canonical/SubtypeDiff are already portable
// text (see GetRangeDescriptors — printed via ::regprocedure::text), not
// OIDs.
type RangeDescriptor struct {
	Namespace        string
	Name             string
	SubtypeNamespace string
	SubtypeName      string
	// SubtypeKind is the subtype's typtype, part of its identity: see
	// GetDomainDescriptors' base_kind.
	SubtypeKind string
	// Collation and Opclass are schema-qualified, or empty when the range
	// has none.
	Collation   string
	Opclass     string
	Canonical   string
	SubtypeDiff string
}

// GetRangeDescriptors resolves every range type named by oids, keyed by
// that OID (this node's own — see GetDomainDescriptors' same caveat).
func GetRangeDescriptors(ctx context.Context, db DBQuerier, oids []uint32) (map[uint32]RangeDescriptor, error) {
	result := make(map[uint32]RangeDescriptor)
	if len(oids) == 0 {
		return result, nil
	}

	sql, err := RenderSQL(SQLTemplates.GetRangeDescriptors, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetRangeDescriptors SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, oids)
	if err != nil {
		return nil, fmt.Errorf("query to get range descriptors failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid uint32
		var d RangeDescriptor
		if err := rows.Scan(&oid, &d.Namespace, &d.Name, &d.SubtypeNamespace, &d.SubtypeName,
			&d.SubtypeKind, &d.Collation, &d.Opclass, &d.Canonical, &d.SubtypeDiff); err != nil {
			return nil, fmt.Errorf("failed to scan range descriptor: %w", err)
		}
		result[oid] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over range descriptors: %w", err)
	}
	return result, nil
}

// CompositeAttribute is one field of a composite type (pg_type.typtype =
// 'c'), in attnum order: attribute order is part of a composite type's
// structural identity (see GetCompositeAttributes).
type CompositeAttribute struct {
	// AttNum is the attribute's own pg_attribute.attnum, not its position
	// in Attributes: DROP ATTRIBUTE leaves the surviving attributes'
	// attnums alone, so this is the ordinal that stays comparable across
	// nodes. See GetCompositeAttributes.
	AttNum        int16
	Name          string
	TypeNamespace string
	TypeName      string
	// TypeKind is the attribute type's typtype, part of its identity: see
	// GetDomainDescriptors' base_kind.
	TypeKind string
	TypeMod  int32
	// TypeText is the attribute's type as format_type prints it. Display
	// only, never compared (see BaseTypeText).
	TypeText string
	// Collation is schema-qualified, or empty when the attribute has none.
	Collation string
}

// CompositeDescriptor is one composite type's own portable identity plus
// its attributes, in declaration order.
type CompositeDescriptor struct {
	Namespace  string
	Name       string
	Attributes []CompositeAttribute
}

// GetCompositeAttributes resolves every composite type named by oids,
// keyed by that OID (this node's own), each with its attributes in
// declaration order.
func GetCompositeAttributes(ctx context.Context, db DBQuerier, oids []uint32) (map[uint32]CompositeDescriptor, error) {
	result := make(map[uint32]CompositeDescriptor)
	if len(oids) == 0 {
		return result, nil
	}

	sql, err := RenderSQL(SQLTemplates.GetCompositeAttributes, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetCompositeAttributes SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, oids)
	if err != nil {
		return nil, fmt.Errorf("query to get composite attributes failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid uint32
		var namespace, name string
		var a CompositeAttribute
		if err := rows.Scan(&oid, &namespace, &name, &a.AttNum, &a.Name,
			&a.TypeNamespace, &a.TypeName, &a.TypeKind, &a.TypeMod, &a.TypeText,
			&a.Collation); err != nil {
			return nil, fmt.Errorf("failed to scan composite attribute: %w", err)
		}
		d := result[oid]
		d.Namespace, d.Name = namespace, name
		d.Attributes = append(d.Attributes, a)
		result[oid] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over composite attributes: %w", err)
	}
	return result, nil
}

// EnumDescriptor is one enum type's own portable identity plus its labels,
// in enumsortorder.
type EnumDescriptor struct {
	Namespace string
	Name      string
	Labels    []string
}

// GetEnumLabels resolves every enum type named by oids, keyed by that OID
// (this node's own), each with its labels in enumsortorder — order is the
// entire point of an enum.
func GetEnumLabels(ctx context.Context, db DBQuerier, oids []uint32) (map[uint32]EnumDescriptor, error) {
	result := make(map[uint32]EnumDescriptor)
	if len(oids) == 0 {
		return result, nil
	}

	sql, err := RenderSQL(SQLTemplates.GetEnumLabels, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetEnumLabels SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, oids)
	if err != nil {
		return nil, fmt.Errorf("query to get enum labels failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var oid uint32
		var namespace, name, label string
		if err := rows.Scan(&oid, &namespace, &name, &label); err != nil {
			return nil, fmt.Errorf("failed to scan enum label: %w", err)
		}
		d := result[oid]
		d.Namespace, d.Name = namespace, name
		d.Labels = append(d.Labels, label)
		result[oid] = d
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over enum labels: %w", err)
	}
	return result, nil
}

// TypeReference is one type's kind plus the OIDs of the types it is built
// out of. Every OID here is local to the node it was read from.
type TypeReference struct {
	OID  uint32
	Kind string // typtype: 'b' base | 'd' domain | 'e' enum | 'r' range | 'm' multirange | 'c' composite | 'p' pseudo
	// Refs holds each referenced type, with zeroes dropped: an array's
	// element type, a domain's base type, a range's subtype, a
	// multirange's range, and a composite's attribute types.
	Refs []uint32
}

// GetTypeReferences reads the kind of every type in oids and the types each
// of them refers to, for a caller resolving the full set of types a schema
// depends on. See the GetTypeReferences SQL template for which catalog
// edges are followed and why a single pass over a column's own typtype is
// not enough.
func GetTypeReferences(ctx context.Context, db DBQuerier, oids []uint32) (map[uint32]TypeReference, error) {
	result := make(map[uint32]TypeReference)
	if len(oids) == 0 {
		return result, nil
	}

	sql, err := RenderSQL(SQLTemplates.GetTypeReferences, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetTypeReferences SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, oids)
	if err != nil {
		return nil, fmt.Errorf("query to get type references failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			ref                                          TypeReference
			element, base, rangeSubtype, multirangeRange uint32
			attributes                                   []uint32
		)
		if err := rows.Scan(&ref.OID, &ref.Kind, &element, &base,
			&rangeSubtype, &multirangeRange, &attributes); err != nil {
			return nil, fmt.Errorf("failed to scan type reference: %w", err)
		}
		for _, candidate := range append([]uint32{element, base, rangeSubtype, multirangeRange}, attributes...) {
			if candidate != 0 {
				ref.Refs = append(ref.Refs, candidate)
			}
		}
		result[ref.OID] = ref
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over type references: %w", err)
	}
	return result, nil
}

// DatabaseLocale is the collation configuration of one database.
type DatabaseLocale struct {
	Name     string // datname, for a report that has to say which database
	Collate  string // datcollate
	Ctype    string // datctype
	Provider string // datlocprovider: 'c' libc | 'i' icu | 'b' builtin; empty before PostgreSQL 15
	Locale   string // datlocale, or daticulocale before PostgreSQL 17; empty when unset
}

// GetDatabaseLocale reads the connected database's collation settings.
//
// A column that does not name a collation of its own inherits these, and
// inherits them invisibly: pg_attribute records the "default" collation on
// every node whatever the database was created with. Comparing this is the
// only way to notice that two nodes holding the same rows disagree about
// how those rows sort. See the GetDatabaseLocale SQL template.
func GetDatabaseLocale(ctx context.Context, db DBQuerier) (DatabaseLocale, error) {
	var locale DatabaseLocale

	sql, err := RenderSQL(SQLTemplates.GetDatabaseLocale, nil)
	if err != nil {
		return locale, fmt.Errorf("failed to render GetDatabaseLocale SQL: %w", err)
	}

	row := db.QueryRow(ctx, sql)
	if err := row.Scan(&locale.Name, &locale.Collate, &locale.Ctype,
		&locale.Provider, &locale.Locale); err != nil {
		return locale, fmt.Errorf("failed to scan database locale: %w", err)
	}
	return locale, nil
}

// QuoteIdentifiers renders every distinct name in names the way this node's
// own PostgreSQL would write it back via quote_ident(), so identifiers
// round-trip unambiguously.
//
// The returned map has one entry per distinct input name. A name that does
// not come back is absent; callers should treat that as "print the raw
// name" for display.
func QuoteIdentifiers(ctx context.Context, db DBQuerier, names []string) (map[string]string, error) {
	result := make(map[string]string, len(names))
	if len(names) == 0 {
		return result, nil
	}

	sql, err := RenderSQL(SQLTemplates.QuoteIdentifiers, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render QuoteIdentifiers SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, names)
	if err != nil {
		return nil, fmt.Errorf("query to quote identifiers failed: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var raw, quoted string
		if err := rows.Scan(&raw, &quoted); err != nil {
			return nil, fmt.Errorf("failed to scan quoted identifier: %w", err)
		}
		result[raw] = quoted
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over quoted identifiers: %w", err)
	}
	return result, nil
}

func GetPkeyType(ctx context.Context, db DBQuerier, schema, table, pkey string) (string, error) {
	sql, err := RenderSQL(SQLTemplates.GetPkeyType, nil)
	if err != nil {
		return "", fmt.Errorf("failed to render GetPkeyType SQL: %w", err)
	}

	var pkeyType string
	if err := db.QueryRow(ctx, sql, schema, table, pkey).Scan(&pkeyType); err != nil {
		return "", fmt.Errorf("query to get pkey type for '%s.%s.%s' failed: %w", schema, table, pkey, err)
	}
	return pkeyType, nil
}

func UpdateMetadata(ctx context.Context, db DBQuerier, schema, table string, totalRows int64, blockSize, numBlocks int, isComposite bool, hashVersion int) error {
	sql, err := RenderSQL(SQLTemplates.UpdateMetadata, nil)
	if err != nil {
		return fmt.Errorf("failed to render UpdateMetadata SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, schema, table, totalRows, blockSize, numBlocks, isComposite, hashVersion)
	if err != nil {
		return fmt.Errorf("query to update metadata for '%s.%s' failed: %w", schema, table, err)
	}

	return nil
}

func ComputeLeafHashes(ctx context.Context, db DBQuerier, schema, table string, _ bool, key []string, start []any, end []any, allCols []string, colTypes map[string]string) ([]byte, error) {
	hasLower := len(start) > 0 && !sliceAllNil(start)
	hasUpper := len(end) > 0 && !sliceAllNil(end)

	sql, err := BlockHashSQL(schema, table, key, "MTREE_LEAF_HASH", hasLower, hasUpper, "", allCols, colTypes)
	if err != nil {
		return nil, err
	}

	args := make([]any, 0, len(key)*2)
	if hasLower {
		args = append(args, start...)
	}
	if hasUpper {
		args = append(args, end...)
	}

	var leafHash []byte
	if err := db.QueryRow(ctx, sql, args...).Scan(&leafHash); err != nil { // nosemgrep
		return nil, fmt.Errorf("query to compute leaf hashes for '%s.%s' failed: %w", schema, table, err)
	}
	return leafHash, nil
}

func sliceAllNil(vals []any) bool {
	if len(vals) == 0 {
		return true
	}
	for _, v := range vals {
		if v != nil {
			return false
		}
	}
	return true
}

func UpdateLeafHashes(ctx context.Context, db DBQuerier, mtreeTable string, leafHash []byte, nodePosition int64) (int64, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.UpdateLeafHashes, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render UpdateLeafHashes SQL: %w", err)
	}

	var updatedNodePosition int64
	err = db.QueryRow(ctx, sql, leafHash, nodePosition).Scan(&updatedNodePosition)
	if err != nil {
		return 0, fmt.Errorf("query to update leaf hashes for '%s' failed: %w", mtreeTable, err)
	}

	return updatedNodePosition, nil
}

func UpdateLeafHashesBatch(ctx context.Context, db DBQuerier, mtreeTable string, leafHashes map[int64][]byte) error {
	tx, ok := db.(pgx.Tx)
	if !ok {
		return fmt.Errorf("UpdateLeafHashesBatch expects a pgx.Tx transaction object")
	}

	batch := &pgx.Batch{}
	updateQuery, err := RenderSQL(SQLTemplates.UpdateLeafHashesBatch, map[string]interface{}{"MtreeTable": mtreeTable})
	if err != nil {
		return fmt.Errorf("failed to render UpdateLeafHashesBatch SQL: %w", err)
	}

	for blockID, hash := range leafHashes {
		batch.Queue(updateQuery, hash, blockID)
	}

	br := tx.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(leafHashes); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("failed to execute update in batch: %w", err)
		}
	}

	return nil
}

// MarkLeavesDirtyByPositions flags the given leaf blocks for rehash. Used to
// refresh leaves whose stored hash is stale relative to the live table data
// (e.g. mismatches resolved as false positives during a diff).
func MarkLeavesDirtyByPositions(ctx context.Context, db DBQuerier, mtreeTable string, nodePositions []int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.MarkLeavesDirtyByPositions, data)
	if err != nil {
		return fmt.Errorf("failed to render MarkLeavesDirtyByPositions SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, nodePositions)
	if err != nil {
		return fmt.Errorf("query to mark leaves dirty for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func ClearDirtyFlags(ctx context.Context, db DBQuerier, mtreeTable string, nodePositions []int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.ClearDirtyFlags, data)
	if err != nil {
		return fmt.Errorf("failed to render ClearDirtyFlags SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, nodePositions)
	if err != nil {
		return fmt.Errorf("query to clear dirty flags for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func BuildParentNodes(ctx context.Context, db DBQuerier, mtreeTable string, nodeLevel int) (int, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.BuildParentNodes, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render BuildParentNodes SQL: %w", err)
	}

	var count int
	err = db.QueryRow(ctx, sql, nodeLevel).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to build parent nodes for '%s' failed: %w", mtreeTable, err)
	}

	return count, nil
}

func GetRootNode(ctx context.Context, db DBQuerier, mtreeTable string) (*types.RootNode, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetRootNode, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetRootNode SQL: %w", err)
	}

	var rootNode types.RootNode
	err = db.QueryRow(ctx, sql).Scan(&rootNode.NodePosition, &rootNode.NodeHash)
	if err != nil {
		return nil, fmt.Errorf("query to get root node for '%s' failed: %w", mtreeTable, err)
	}

	return &rootNode, nil
}

func GetNodeChildren(ctx context.Context, db DBQuerier, mtreeTable string, nodeLevel, nodePosition int) ([]types.NodeChild, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetNodeChildren, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetNodeChildren SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, nodeLevel, nodePosition)
	if err != nil {
		return nil, fmt.Errorf("query to get node children for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var children []types.NodeChild
	for rows.Next() {
		var child types.NodeChild
		if err := rows.Scan(&child.NodeLevel, &child.NodePosition, &child.NodeHash); err != nil {
			return nil, fmt.Errorf("failed to scan node child: %w", err)
		}
		children = append(children, child)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over node children: %w", err)
	}

	return children, nil
}

func GetLeafRanges(ctx context.Context, db DBQuerier, mtreeTable string, nodePositions []int64, simplePrimaryKey bool, key []string) ([]types.LeafRange, error) {
	if simplePrimaryKey {
		data := map[string]interface{}{
			"MtreeTable": mtreeTable,
		}

		sql, err := RenderSQL(SQLTemplates.GetLeafRanges, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render GetLeafRanges SQL: %w", err)
		}

		rows, err := db.Query(ctx, sql, nodePositions) // nosemgrep
		if err != nil {
			return nil, fmt.Errorf("query to get leaf ranges for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()

		var ranges []types.LeafRange
		for rows.Next() {
			var r types.LeafRange
			var start, end any
			if err := rows.Scan(&start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan leaf range: %w", err)
			}
			r.RangeStart = []any{start}
			if end != nil {
				r.RangeEnd = []any{end}
			}
			ranges = append(ranges, r)
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over leaf ranges: %w", err)
		}

		return ranges, nil
	}

	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable": mtreeTable,
		"StartAttrs": strings.Join(startAttrs, ", "),
		"EndAttrs":   strings.Join(endAttrs, ", "),
	}
	sql, err := RenderSQL(SQLTemplates.GetLeafRangesExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetLeafRangesExpanded SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, nodePositions) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to get expanded leaf ranges for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var ranges []types.LeafRange
	numKeyCols := len(key)
	for rows.Next() {
		dest := make([]any, numKeyCols*2)
		destPtrs := make([]any, numKeyCols*2)
		for i := range dest {
			destPtrs[i] = &dest[i]
		}

		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded leaf range: %w", err)
		}

		startVals := make([]any, numKeyCols)
		copy(startVals, dest[:numKeyCols])
		endVals := make([]any, numKeyCols)
		copy(endVals, dest[numKeyCols:])

		ranges = append(ranges, types.LeafRange{
			RangeStart: startVals,
			RangeEnd:   endVals,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded leaf ranges: %w", err)
	}

	return ranges, nil
}

func GetRowCountEstimateFromMetadata(ctx context.Context, db DBQuerier, schema, table string) (int64, error) {
	sql, err := RenderSQL(SQLTemplates.GetRowCountEstimate, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetRowCountEstimate SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, schema, table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get row count estimate from metadata for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetMaxValComposite(ctx context.Context, db DBQuerier, schema, table string, pkeyCols []string, pkeyValues []any) ([]interface{}, error) {
	cols := make([]string, len(pkeyCols))
	for i, c := range pkeyCols {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	colsStr := strings.Join(cols, ", ")

	valsPh := make([]string, len(pkeyValues))
	args := make([]any, len(pkeyValues))
	for i, v := range pkeyValues {
		valsPh[i] = fmt.Sprintf("$%d", i+1)
		args[i] = v
	}
	valsStr := strings.Join(valsPh, ", ")

	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    colsStr,
		"PkeyValues":  fmt.Sprintf("ROW(%s)", valsStr),
	}
	sql, err := RenderSQL(SQLTemplates.GetMaxValComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMaxValComposite SQL: %w", err)
	}
	dest := make([]interface{}, len(pkeyCols))
	destPtrs := make([]interface{}, len(pkeyCols))
	for i := range destPtrs {
		destPtrs[i] = &dest[i]
	}
	if err := db.QueryRow(ctx, sql, args...).Scan(destPtrs...); err != nil { // nosemgrep
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get max val composite for '%s.%s' failed: %w", schema, table, err)
	}
	return dest, nil
}

func UpdateMaxVal(ctx context.Context, db DBQuerier, mtreeTable string, rangeEnd interface{}, nodePosition int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.UpdateMaxVal, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateMaxVal SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, rangeEnd, nodePosition)
	if err != nil {
		return fmt.Errorf("query to update max val for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func GetMaxValSimple(ctx context.Context, db DBQuerier, schema, table, key string, rangeStart interface{}) (interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}

	sql, err := RenderSQL(SQLTemplates.GetMaxValSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMaxValSimple SQL: %w", err)
	}

	var maxVal interface{}
	err = db.QueryRow(ctx, sql, rangeStart).Scan(&maxVal)
	if err != nil {
		return nil, fmt.Errorf("query to get max val simple for '%s.%s' failed: %w", schema, table, err)
	}

	return maxVal, nil
}

func GetCountComposite(ctx context.Context, db DBQuerier, schema, table, whereClause string) (int64, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"WhereClause": whereClause,
	}

	sql, err := RenderSQL(SQLTemplates.GetCountComposite, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetCountComposite SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get count composite for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetCountSimple(ctx context.Context, db DBQuerier, schema, table, key, pkeyType string, rangeStart, rangeEnd interface{}) (int64, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
		"PkeyType":    pkeyType,
	}

	sql, err := RenderSQL(SQLTemplates.GetCountSimple, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetCountSimple SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, rangeStart, rangeEnd).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query to get count simple for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetBlockRowCount(ctx context.Context, db DBQuerier, schema string, table string, keyColumns []string, isComposite bool, start, end []any) (int64, error) {
	var whereClause string
	var args []any

	if isComposite {
		var conditions []string
		var startPlaceholders, endPlaceholders []string

		if len(start) > 0 {
			for i := range start {
				startPlaceholders = append(startPlaceholders, fmt.Sprintf("$%d", len(args)+i+1))
			}
			conditions = append(conditions, fmt.Sprintf("ROW(%s) >= ROW(%s)", strings.Join(keyColumns, ", "), strings.Join(startPlaceholders, ", ")))
			args = append(args, start...)
		}

		if len(end) > 0 && end[0] != nil {
			for i := range end {
				endPlaceholders = append(endPlaceholders, fmt.Sprintf("$%d", len(args)+i+1))
			}
			conditions = append(conditions, fmt.Sprintf("ROW(%s) <= ROW(%s)", strings.Join(keyColumns, ", "), strings.Join(endPlaceholders, ", ")))
			args = append(args, end...)
		}
		whereClause = strings.Join(conditions, " AND ")
	} else {
		var conditions []string
		if len(start) > 0 && start[0] != nil {
			conditions = append(conditions, fmt.Sprintf("%s >= $1", keyColumns[0]))
			args = append(args, start[0])
		}
		if len(end) > 0 && end[0] != nil {
			conditions = append(conditions, fmt.Sprintf("%s <= $%d", keyColumns[0], len(args)+1))
			args = append(args, end[0])
		}
		whereClause = strings.Join(conditions, " AND ")
	}

	if whereClause == "" {
		whereClause = "TRUE"
	}

	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"WhereClause": whereClause,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockRowCount, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetBlockRowCount SQL: %w", err)
	}

	var count int64
	err = db.QueryRow(ctx, sql, args...).Scan(&count) // nosemgrep
	if err != nil {
		return 0, fmt.Errorf("query to get block row count for '%s.%s' failed: %w", schema, table, err)
	}

	return count, nil
}

func GetDirtyAndNewBlocks(ctx context.Context, db DBQuerier, mtreeTable string, simplePrimaryKey bool, key []string) ([]types.BlockRange, error) {
	if simplePrimaryKey {
		data := map[string]interface{}{
			"MtreeTable": mtreeTable,
		}
		sql, err := RenderSQL(SQLTemplates.GetDirtyAndNewBlocks, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render GetDirtyAndNewBlocks SQL: %w", err)
		}

		rows, err := db.Query(ctx, sql) // nosemgrep
		if err != nil {
			return nil, fmt.Errorf("query to get dirty and new blocks for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()

		var blocks []types.BlockRange
		for rows.Next() {
			var br types.BlockRange
			var start any
			var end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block range row: %w", err)
			}
			if start != nil {
				br.RangeStart = []any{start}
			}
			if end != nil {
				br.RangeEnd = []any{end}
			}
			blocks = append(blocks, br)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over block ranges: %w", err)
		}
		return blocks, nil
	}

	// Composite primary key: expand attributes using template to avoid binary composite decoding
	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable": mtreeTable,
		"StartAttrs": strings.Join(startAttrs, ", "),
		"EndAttrs":   strings.Join(endAttrs, ", "),
	}
	sql, err := RenderSQL(SQLTemplates.GetDirtyAndNewBlocksExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetDirtyAndNewBlocksExpanded SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to get dirty and new blocks for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var blocks []types.BlockRange
	numCols := 1 + len(key) + len(key)
	for rows.Next() {
		dest := make([]any, numCols)
		destPtrs := make([]any, numCols)
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded block range row: %w", err)
		}
		var br types.BlockRange
		if v, ok := dest[0].(int64); ok {
			br.NodePosition = v
		} else {
			// allow numeric types that can be cast to int64
			switch t := dest[0].(type) {
			case int32:
				br.NodePosition = int64(t)
			case int:
				br.NodePosition = int64(t)
			default:
				return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
			}
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):])
		br.RangeStart = startVals
		br.RangeEnd = endVals
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded block ranges: %w", err)
	}
	return blocks, nil
}

func FindBlocksToSplit(ctx context.Context, db DBQuerier, mtreeTable string, insertsSinceUpdate int, nodePositions []int64, simplePrimaryKey bool, key []string) ([]types.BlockRange, error) {
	if simplePrimaryKey {
		data := map[string]interface{}{
			"MtreeTable": mtreeTable,
		}
		sql, err := RenderSQL(SQLTemplates.FindBlocksToSplit, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render FindBlocksToSplit SQL: %w", err)
		}
		rows, err := db.Query(ctx, sql, insertsSinceUpdate, nodePositions) // nosemgrep
		if err != nil {
			return nil, fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()

		var blocks []types.BlockRange
		for rows.Next() {
			var br types.BlockRange
			var start any
			var end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block to split: %w", err)
			}
			if start != nil {
				br.RangeStart = []any{start}
			}
			if end != nil {
				br.RangeEnd = []any{end}
			}
			blocks = append(blocks, br)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over blocks to split: %w", err)
		}
		return blocks, nil
	}

	// Composite primary key: expand composite attributes using template
	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable": mtreeTable,
		"StartAttrs": strings.Join(startAttrs, ", "),
		"EndAttrs":   strings.Join(endAttrs, ", "),
	}
	sql, err := RenderSQL(SQLTemplates.FindBlocksToSplitExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToSplitExpanded SQL: %w", err)
	}

	rows, err := db.Query(ctx, sql, insertsSinceUpdate, nodePositions) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to split for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()

	var blocks []types.BlockRange
	numCols := 1 + len(key) + len(key)
	for rows.Next() {
		dest := make([]any, numCols)
		destPtrs := make([]any, numCols)
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded block to split: %w", err)
		}
		var br types.BlockRange
		if v, ok := dest[0].(int64); ok {
			br.NodePosition = v
		} else {
			switch t := dest[0].(type) {
			case int32:
				br.NodePosition = int64(t)
			case int:
				br.NodePosition = int64(t)
			default:
				return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
			}
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):])
		br.RangeStart = startVals
		br.RangeEnd = endVals
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded blocks to split: %w", err)
	}
	return blocks, nil
}

func GetMaxNodePosition(ctx context.Context, db DBQuerier, mtreeTable string) (int64, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.GetMaxNodePosition, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetMaxNodePosition SQL: %w", err)
	}
	var pos int64
	if err := db.QueryRow(ctx, sql).Scan(&pos); err != nil {
		return 0, fmt.Errorf("query to get max node position for '%s' failed: %w", mtreeTable, err)
	}
	return pos, nil
}

func UpdateBlockRangeEnd(ctx context.Context, db DBQuerier, mtreeTable string, rangeEnd any, nodePosition int64) error {
	data := map[string]interface{}{
		"MtreeTable":   mtreeTable,
		"RangeEndExpr": "$1",
		"NodePosition": "$2",
	}
	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeEnd, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeEnd SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql, rangeEnd, nodePosition); err != nil {
		return fmt.Errorf("query to update block range end for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func UpdateBlockRangeEndComposite(ctx context.Context, db DBQuerier, mtreeTable string, compositeTypeName string, endVals []any, pos int64) error {
	args := []any{}
	isNull := len(endVals) == 0

	data := map[string]any{
		"MtreeTable":              mtreeTable,
		"IsNull":                  isNull,
		"CompositeTypeName":       compositeTypeName,
		"NodePositionPlaceholder": fmt.Sprintf("$%d", len(endVals)+1),
	}

	if !isNull {
		placeholders := make([]string, len(endVals))
		for i, val := range endVals {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, val)
		}
		data["Placeholders"] = strings.Join(placeholders, ", ")
	}
	args = append(args, pos)

	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeEndComposite, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeEndCompositeTx SQL: %w", err)
	}

	if _, err := db.Exec(ctx, sql, args...); err != nil { // nosemgrep
		return fmt.Errorf("query to update composite block range end for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func UpdateBlockRangeStart(ctx context.Context, db DBQuerier, mtreeTable string, rangeStart any, nodePosition int64) error {
	data := map[string]interface{}{
		"MtreeTable":     mtreeTable,
		"RangeStartExpr": "$1",
		"NodePosition":   "$2",
	}
	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeStart, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeStart SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql, rangeStart, nodePosition); err != nil {
		return fmt.Errorf("query to update block range start for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func UpdateBlockRangeStartComposite(ctx context.Context, db DBQuerier, mtreeTable string, compositeTypeName string, startVals []any, pos int64) error {
	args := []any{}
	isNull := len(startVals) == 0

	data := map[string]any{
		"MtreeTable":              mtreeTable,
		"IsNull":                  isNull,
		"CompositeTypeName":       compositeTypeName,
		"NodePositionPlaceholder": fmt.Sprintf("$%d", len(startVals)+1),
	}

	if !isNull {
		placeholders := make([]string, len(startVals))
		for i, val := range startVals {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args = append(args, val)
		}
		data["Placeholders"] = strings.Join(placeholders, ", ")
	}
	args = append(args, pos)

	sql, err := RenderSQL(SQLTemplates.UpdateBlockRangeStartComposite, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateBlockRangeStartCompositeTx SQL: %w", err)
	}

	if _, err := db.Exec(ctx, sql, args...); err != nil { // nosemgrep
		return fmt.Errorf("query to update composite block range start for '%s' failed: %w", mtreeTable, err)
	}
	return nil
}

func GetMinValComposite(ctx context.Context, db DBQuerier, schema, table string, pkeyCols []string) ([]interface{}, error) {
	cols := make([]string, len(pkeyCols))
	for i, c := range pkeyCols {
		cols[i] = pgx.Identifier{c}.Sanitize()
	}
	colsStr := strings.Join(cols, ", ")

	data := map[string]any{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    colsStr,
	}
	sql, err := RenderSQL(SQLTemplates.GetMinValComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMinValComposite SQL: %w", err)
	}

	dest := make([]interface{}, len(pkeyCols))
	destPtrs := make([]interface{}, len(pkeyCols))
	for i := range destPtrs {
		destPtrs[i] = &dest[i]
	}
	if err := db.QueryRow(ctx, sql).Scan(destPtrs...); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get min val composite for '%s.%s' failed: %w", schema, table, err)
	}
	return dest, nil
}

func GetMinValSimple(ctx context.Context, db DBQuerier, schema, table, key string) (interface{}, error) {
	data := map[string]interface{}{
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}
	sql, err := RenderSQL(SQLTemplates.GetMinValSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetMinValSimple SQL: %w", err)
	}
	var minVal interface{}
	if err := db.QueryRow(ctx, sql).Scan(&minVal); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get min val simple for '%s.%s' failed: %w", schema, table, err)
	}
	return minVal, nil
}

func FindBlocksToMergeComposite(ctx context.Context, db DBQuerier, mtreeTable, schema, table string, keyColumns []string, nodePositions []int64, mergeThreshold float64) ([]types.BlockRange, error) {
	return findBlocksToMerge(ctx, db, mtreeTable, schema, table, keyColumns, false, nodePositions, mergeThreshold)
}

func FindBlocksToMergeSimple(ctx context.Context, db DBQuerier, mtreeTable, schema, table, key string, nodePositions []int64, mergeThreshold float64) ([]types.BlockRange, error) {
	return findBlocksToMerge(ctx, db, mtreeTable, schema, table, []string{key}, true, nodePositions, mergeThreshold)
}

func findBlocksToMerge(ctx context.Context, db DBQuerier, mtreeTable, schema, table string, key []string, simplePrimaryKey bool, nodePositions []int64, mergeThreshold float64) ([]types.BlockRange, error) {
	var queryArgs []any
	usePositionFilter := len(nodePositions) > 0

	if usePositionFilter {
		// Expand candidate positions to include adjacent neighbours
		posSet := make(map[int64]struct{}, len(nodePositions)*3)
		for _, p := range nodePositions {
			posSet[p] = struct{}{}
			if p > 0 {
				posSet[p-1] = struct{}{}
			}
			posSet[p+1] = struct{}{}
		}
		expandedPositions := make([]int64, 0, len(posSet))
		for p := range posSet {
			expandedPositions = append(expandedPositions, p)
		}
		queryArgs = append(queryArgs, expandedPositions)
	}

	sanitizedKeys := make([]string, len(key))
	for i, k := range key {
		sanitizedKeys[i] = pgx.Identifier{k}.Sanitize()
	}

	blockSize, err := GetBlockSizeFromMetadata(ctx, db, schema, table)
	if err != nil {
		return nil, err
	}
	mergeThresholdValue := float64(blockSize) * mergeThreshold
	queryArgs = append(queryArgs, mergeThresholdValue)

	if simplePrimaryKey {
		data := map[string]any{
			"MtreeTable":          mtreeTable,
			"SchemaIdent":         pgx.Identifier{schema}.Sanitize(),
			"TableIdent":          pgx.Identifier{table}.Sanitize(),
			"SimplePrimaryKey":    simplePrimaryKey,
			"Key":                 sanitizedKeys,
			"UsePositionFilter":   usePositionFilter,
			"PositionPlaceholder": "$1",
			"MergeValPlaceholder": fmt.Sprintf("$%d", len(queryArgs)),
		}
		sql, err := RenderSQL(SQLTemplates.FindBlocksToMerge, data)
		if err != nil {
			return nil, fmt.Errorf("failed to render FindBlocksToMerge SQL: %w", err)
		}
		rows, err := db.Query(ctx, sql, queryArgs...) // nosemgrep
		if err != nil {
			return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTable, err)
		}
		defer rows.Close()
		var blocks []types.BlockRange
		for rows.Next() {
			var br types.BlockRange
			var start, end any
			if err := rows.Scan(&br.NodePosition, &start, &end); err != nil {
				return nil, fmt.Errorf("failed to scan block to merge: %w", err)
			}
			br.RangeStart = []any{start}
			br.RangeEnd = []any{end}
			blocks = append(blocks, br)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating over blocks to merge: %w", err)
		}
		return blocks, nil
	}

	// Composite key path: expand attributes to avoid binary composite decoding
	startAttrs := make([]string, len(key))
	endAttrs := make([]string, len(key))
	for i, k := range key {
		attr := pgx.Identifier{k}.Sanitize()
		startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
		endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
	}
	data := map[string]any{
		"MtreeTable":          mtreeTable,
		"SchemaIdent":         pgx.Identifier{schema}.Sanitize(),
		"TableIdent":          pgx.Identifier{table}.Sanitize(),
		"Key":                 sanitizedKeys,
		"StartAttrs":          strings.Join(startAttrs, ", "),
		"EndAttrs":            strings.Join(endAttrs, ", "),
		"UsePositionFilter":   usePositionFilter,
		"PositionPlaceholder": "$1",
		"MergeValPlaceholder": fmt.Sprintf("$%d", len(queryArgs)),
	}
	sql, err := RenderSQL(SQLTemplates.FindBlocksToMergeExpanded, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render FindBlocksToMergeExpanded SQL: %w", err)
	}
	rows, err := db.Query(ctx, sql, queryArgs...) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("query to find blocks to merge for '%s' failed: %w", mtreeTable, err)
	}
	defer rows.Close()
	var blocks []types.BlockRange
	for rows.Next() {
		dest := make([]any, 1+len(key)+len(key))
		destPtrs := make([]any, len(dest))
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan expanded block to merge: %w", err)
		}
		var br types.BlockRange
		switch v := dest[0].(type) {
		case int64:
			br.NodePosition = v
		case int32:
			br.NodePosition = int64(v)
		case int:
			br.NodePosition = int64(v)
		default:
			return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):])
		br.RangeStart = startVals
		br.RangeEnd = endVals
		blocks = append(blocks, br)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over expanded blocks to merge: %w", err)
	}
	return blocks, nil
}

func GetBlockCountComposite(ctx context.Context, db DBQuerier, mtreeTable, schema, table, pkeyCols string, nodePosition int64) (*types.BlockCountComposite, error) {
	data := map[string]interface{}{
		"MtreeTable":  mtreeTable,
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"PkeyCols":    pkeyCols,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockCountComposite, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockCountComposite SQL: %w", err)
	}

	var blockCount types.BlockCountComposite
	err = db.QueryRow(ctx, sql, nodePosition).Scan(&blockCount.NodePosition, &blockCount.RangeStart, &blockCount.RangeEnd, &blockCount.Count)
	if err != nil {
		return nil, fmt.Errorf("query to get block count composite for '%s' failed: %w", mtreeTable, err)
	}

	return &blockCount, nil
}

func GetBlockCountSimple(ctx context.Context, db DBQuerier, mtreeTable, schema, table, key string, nodePosition int64) (*types.BlockCountSimple, error) {
	data := map[string]interface{}{
		"MtreeTable":  mtreeTable,
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"Key":         key,
	}

	sql, err := RenderSQL(SQLTemplates.GetBlockCountSimple, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockCountSimple SQL: %w", err)
	}

	var blockCount types.BlockCountSimple
	err = db.QueryRow(ctx, sql, nodePosition).Scan(&blockCount.NodePosition, &blockCount.RangeStart, &blockCount.RangeEnd, &blockCount.Count)
	if err != nil {
		return nil, fmt.Errorf("query to get block count simple for '%s' failed: %w", mtreeTable, err)
	}

	return &blockCount, nil
}

func GetBlockSizeFromMetadata(ctx context.Context, db DBQuerier, schema, table string) (int, error) {
	data := map[string]interface{}{}
	query, err := RenderSQL(SQLTemplates.GetBlockSizeFromMetadata, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetBlockSizeFromMetadata SQL: %w", err)
	}
	var blockSize int
	err = db.QueryRow(ctx, query, schema, table).Scan(&blockSize)
	if err != nil {
		return 0, fmt.Errorf("query to get block size from metadata for '%s.%s' failed: %w", schema, table, err)
	}
	return blockSize, nil
}

func EnsureHashVersionColumn(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.EnsureHashVersionColumn, nil)
	if err != nil {
		return fmt.Errorf("failed to render EnsureHashVersionColumn SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to add hash_version column to metadata table: %w", err)
	}
	return nil
}

func GetHashVersion(ctx context.Context, db DBQuerier, schema, table string) (int, error) {
	sql, err := RenderSQL(SQLTemplates.GetHashVersion, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetHashVersion SQL: %w", err)
	}

	var version int
	err = db.QueryRow(ctx, sql, schema, table).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("query to get hash version for '%s.%s' failed: %w", schema, table, err)
	}
	return version, nil
}

func MarkAllLeavesDirty(ctx context.Context, db DBQuerier, mtreeTable string) (int64, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.MarkAllLeavesDirty, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render MarkAllLeavesDirty SQL: %w", err)
	}

	tag, err := db.Exec(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("query to mark all leaves dirty for '%s' failed: %w", mtreeTable, err)
	}
	return tag.RowsAffected(), nil
}

func UpdateHashVersion(ctx context.Context, db DBQuerier, schema, table string, version int) error {
	sql, err := RenderSQL(SQLTemplates.UpdateHashVersion, nil)
	if err != nil {
		return fmt.Errorf("failed to render UpdateHashVersion SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, version, schema, table)
	if err != nil {
		return fmt.Errorf("query to update hash version for '%s.%s' failed: %w", schema, table, err)
	}
	return nil
}

func GetMaxNodeLevel(ctx context.Context, db DBQuerier, mtreeTable string) (int, error) {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.GetMaxNodeLevel, data)
	if err != nil {
		return 0, fmt.Errorf("failed to render GetMaxNodeLevel SQL: %w", err)
	}

	var maxLevel int
	err = db.QueryRow(ctx, sql).Scan(&maxLevel)
	if err != nil {
		return 0, fmt.Errorf("query to get max node level for '%s' failed: %w", mtreeTable, err)
	}

	return maxLevel, nil
}

func DropXORFunction(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.DropXORFunction, nil)
	if err != nil {
		return fmt.Errorf("failed to render DropXORFunction SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop xor function failed: %w", err)
	}

	return nil
}

func DropMetadataTable(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.DropMetadataTable, nil)
	if err != nil {
		return fmt.Errorf("failed to render DropMetadataTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop metadata table failed: %w", err)
	}

	return nil
}

func DropMtreeTable(ctx context.Context, db DBQuerier, mtreeTable string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.DropMtreeTable, data)
	if err != nil {
		return fmt.Errorf("failed to render DropMtreeTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop mtree table for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func DeleteParentNodes(ctx context.Context, db DBQuerier, mtreeTable string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}

	sql, err := RenderSQL(SQLTemplates.DeleteParentNodes, data)
	if err != nil {
		return fmt.Errorf("failed to render DeleteParentNodes SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to delete parent nodes for '%s' failed: %w", mtreeTable, err)
	}

	return nil
}

func FindBlocksToMerge(ctx context.Context, db DBQuerier, mtreeTableName string, simplePrimaryKey bool, schema string, table string, key []string, mergeThreshold float64, blockPositions []int64) ([]types.BlockRange, error) {
	return findBlocksToMerge(ctx, db, mtreeTableName, schema, table, key, simplePrimaryKey, blockPositions, mergeThreshold)
}

func GetBlockWithCount(ctx context.Context, db DBQuerier, mtreeTable, schema, table string, key []string, isComposite bool, position int64) (*types.BlockRangeWithCount, error) {
	sanitizedKeys := make([]string, len(key))
	for i, k := range key {
		sanitizedKeys[i] = pgx.Identifier{k}.Sanitize()
	}

	data := map[string]interface{}{
		"MtreeTable":  mtreeTable,
		"SchemaIdent": pgx.Identifier{schema}.Sanitize(),
		"TableIdent":  pgx.Identifier{table}.Sanitize(),
		"IsComposite": isComposite,
		"Key":         sanitizedKeys,
	}

	var query string
	var err error
	if isComposite {
		// Expand attributes to avoid composite binary scan
		startAttrs := make([]string, len(key))
		endAttrs := make([]string, len(key))
		for i, k := range key {
			attr := pgx.Identifier{k}.Sanitize()
			startAttrs[i] = fmt.Sprintf("(range_start).%s", attr)
			endAttrs[i] = fmt.Sprintf("(range_end).%s", attr)
		}
		data["StartAttrs"] = strings.Join(startAttrs, ", ")
		data["EndAttrs"] = strings.Join(endAttrs, ", ")
		query, err = RenderSQL(SQLTemplates.GetBlockWithCountExpanded, data)
	} else {
		query, err = RenderSQL(SQLTemplates.GetBlockWithCount, data)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBlockWithCount SQL: %w", err)
	}

	var block types.BlockRangeWithCount
	var count int64
	var start, end any

	row := db.QueryRow(ctx, query, position) // nosemgrep, position is int64

	if isComposite {
		// node_position, start attrs..., end attrs..., count
		dest := make([]any, 1+len(key)+len(key)+1)
		destPtrs := make([]any, len(dest))
		for i := range destPtrs {
			destPtrs[i] = &dest[i]
		}
		if err := row.Scan(destPtrs...); err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		if v, ok := dest[0].(int64); ok {
			block.NodePosition = v
		} else if v2, ok := dest[0].(int32); ok {
			block.NodePosition = int64(v2)
		} else if v3, ok := dest[0].(int); ok {
			block.NodePosition = int64(v3)
		} else {
			return nil, fmt.Errorf("unexpected type for node_position: %T", dest[0])
		}
		startVals := make([]any, len(key))
		endVals := make([]any, len(key))
		copy(startVals, dest[1:1+len(key)])
		copy(endVals, dest[1+len(key):1+len(key)+len(key)])
		if c, ok := dest[len(dest)-1].(int64); ok {
			count = c
		} else if c2, ok := dest[len(dest)-1].(int32); ok {
			count = int64(c2)
		} else if c3, ok := dest[len(dest)-1].(int); ok {
			count = int64(c3)
		} else {
			return nil, fmt.Errorf("unexpected type for count: %T", dest[len(dest)-1])
		}
		block.RangeStart = startVals
		block.RangeEnd = endVals
	} else {
		err := row.Scan(&block.NodePosition, &start, &end, &count)
		if err != nil {
			if err == pgx.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		block.RangeStart = []any{start}
		block.RangeEnd = []any{end}
	}
	block.Count = count
	return &block, nil
}

func UpdateNodePosition(ctx context.Context, db DBQuerier, mtreeTable string, oldPosition, newPosition int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	query, err := RenderSQL(SQLTemplates.UpdateNodePosition, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateNodePosition SQL: %w", err)
	}
	_, err = db.Exec(ctx, query, newPosition, oldPosition)
	return err
}

func DeleteBlock(ctx context.Context, db DBQuerier, mtreeTable string, position int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	query, err := RenderSQL(SQLTemplates.DeleteBlock, data)
	if err != nil {
		return fmt.Errorf("failed to render DeleteBlock SQL: %w", err)
	}
	_, err = db.Exec(ctx, query, position)
	return err
}

func UpdateNodePositionsSequential(ctx context.Context, db DBQuerier, mtreeTable string, startPosition int64) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
	}
	query, err := RenderSQL(SQLTemplates.UpdateNodePositionsSequential, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateNodePositionsSequential SQL: %w", err)
	}
	_, err = db.Exec(ctx, query, startPosition, startPosition)
	return err
}

func ResetPositionsByStart(ctx context.Context, db DBQuerier, mtreeTable string, key []string, isComposite bool) error {
	data := map[string]any{
		"MtreeTable": mtreeTable,
	}
	var query string
	var err error
	if isComposite {
		query, err = RenderSQL(SQLTemplates.ResetPositionsByStartExpanded, data)
	} else {
		query, err = RenderSQL(SQLTemplates.ResetPositionsByStart, data)
	}
	if err != nil {
		return fmt.Errorf("failed to render ResetPositionsByStart SQL: %w", err)
	}
	if _, err := db.Exec(ctx, query); err != nil {
		return fmt.Errorf("query to reset positions failed: %w", err)
	}
	return nil
}

func GetBulkSplitPoints(ctx context.Context, db DBQuerier, schema, table string, key []string, pkeyType string, isComposite bool, start, end []any, blockSize int) ([][]any, error) {
	args := []any{}
	paramIndex := 1

	sanitisedKeyCols := make([]string, len(key))
	for i, k := range key {
		sanitisedKeyCols[i] = pgx.Identifier{k}.Sanitize()
	}
	pkeyColsStr := strings.Join(sanitisedKeyCols, ", ")

	var conditions []string
	if start != nil {
		if isComposite {
			placeholders := make([]string, len(key))
			for i := 0; i < len(key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i)
			}
			conditions = append(conditions, fmt.Sprintf("(%s) >= (%s)", pkeyColsStr, strings.Join(placeholders, ", ")))
			args = append(args, start...)
			paramIndex += len(key)
		} else {
			conditions = append(conditions, fmt.Sprintf("%s >= $%d", sanitisedKeyCols[0], paramIndex))
			args = append(args, start[0])
			paramIndex++
		}
	}
	if end != nil {
		if isComposite {
			placeholders := make([]string, len(key))
			for i := 0; i < len(key); i++ {
				placeholders[i] = fmt.Sprintf("$%d", paramIndex+i)
			}
			conditions = append(conditions, fmt.Sprintf("(%s) <= (%s)", pkeyColsStr, strings.Join(placeholders, ", ")))
			args = append(args, end...)
			paramIndex += len(key)
		} else {
			conditions = append(conditions, fmt.Sprintf("%s <= $%d", sanitisedKeyCols[0], paramIndex))
			args = append(args, end[0])
			paramIndex++
		}
	}

	whereClause := ""
	if len(conditions) > 0 {
		whereClause = strings.Join(conditions, " AND ")
	}

	blockSizePlaceholder := fmt.Sprintf("$%d", paramIndex)
	args = append(args, blockSize)

	data := map[string]any{
		"SchemaIdent":          pgx.Identifier{schema}.Sanitize(),
		"TableIdent":           pgx.Identifier{table}.Sanitize(),
		"PkeyColsStr":          pkeyColsStr,
		"WhereClause":          whereClause,
		"BlockSizePlaceholder": blockSizePlaceholder,
	}

	query, err := RenderSQL(SQLTemplates.GetBulkSplitPoints, data)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetBulkSplitPoints SQL: %w", err)
	}

	rows, err := db.Query(ctx, query, args...) // nosemgrep
	if err != nil {
		return nil, fmt.Errorf("failed to execute bulk split points query: %w", err)
	}
	defer rows.Close()

	var splitPoints [][]any
	for rows.Next() {
		dest := make([]any, len(key))
		destPtrs := make([]any, len(key))
		for i := range dest {
			destPtrs[i] = &dest[i]
		}
		if err := rows.Scan(destPtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan split point row: %w", err)
		}
		splitPoints = append(splitPoints, dest)
	}

	if rows.Err() != nil {
		return nil, fmt.Errorf("error iterating over split point rows: %w", rows.Err())
	}

	return splitPoints, nil
}

func CreatePublication(ctx context.Context, db DBQuerier, publicationName string) error {
	data := map[string]interface{}{
		"PublicationName": publicationName,
	}
	sql, err := RenderSQL(SQLTemplates.CreatePublication, data)
	if err != nil {
		return fmt.Errorf("failed to render CreatePublication SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create publication failed: %w", err)
	}

	return nil
}

func CreateReplicationSlot(ctx context.Context, db DBQuerier, slotName string) error {
	data := map[string]interface{}{
		"SlotName": slotName,
	}
	sql, err := RenderSQL(SQLTemplates.CreateReplicationSlot, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateReplicationSlot SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create replication slot failed: %w", err)
	}

	return nil
}

func UpdateCDCMetadata(ctx context.Context, db DBQuerier, publicationName, slotName, startLSN string, tables []string) error {
	sql, err := RenderSQL(SQLTemplates.UpdateCDCMetadata, nil)
	if err != nil {
		return fmt.Errorf("failed to render UpdateCDCMetadata SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, publicationName, slotName, startLSN, tables)
	if err != nil {
		return fmt.Errorf("query to update cdc metadata failed: %w", err)
	}

	return nil
}

func AlterPublicationAddTable(ctx context.Context, db DBQuerier, publicationName, tableName string) error {
	data := map[string]interface{}{
		"PublicationName": publicationName,
		"TableName":       tableName,
	}
	sql, err := RenderSQL(SQLTemplates.AlterPublicationAddTable, data)
	if err != nil {
		return fmt.Errorf("failed to render AlterPublicationAddTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to alter publication failed: %w", err)
	}

	return nil
}

func MarkBlockDirty(ctx context.Context, db DBQuerier, mtreeTable, pkeyValue string) error {
	data := map[string]interface{}{
		"MtreeTable": mtreeTable,
		"PkeyValue":  pkeyValue,
	}
	sql, err := RenderSQL(SQLTemplates.MarkBlockDirty, data)
	if err != nil {
		return fmt.Errorf("failed to render MarkBlockDirty SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to mark block dirty failed: %w", err)
	}

	return nil
}

func DropPublication(ctx context.Context, db DBQuerier, publicationName string) error {
	data := map[string]interface{}{
		"PublicationName": publicationName,
	}
	sql, err := RenderSQL(SQLTemplates.DropPublication, data)
	if err != nil {
		return fmt.Errorf("failed to render DropPublication SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop publication failed: %w", err)
	}

	return nil
}

// GetActiveSlotPID returns the PID of the active consumer holding the named
// logical replication slot, or nil if the slot exists but is not currently
// active (or does not exist). Used to detect a running `mtree listen` before a
// bounded CDC drain attempts to attach to the same slot.
func GetActiveSlotPID(ctx context.Context, db DBQuerier, slotName string) (*int32, error) {
	var pid *int32
	pidSQL, err := RenderSQL(SQLTemplates.GetReplicationSlotPID, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetReplicationSlotPID SQL: %w", err)
	}

	err = db.QueryRow(ctx, pidSQL, slotName).Scan(&pid)
	if err != nil && err != pgx.ErrNoRows {
		return nil, fmt.Errorf("query to get replication slot PID failed for slot %s: %w", slotName, err)
	}
	return pid, nil
}

func DropReplicationSlot(ctx context.Context, db DBQuerier, slotName string) error {
	pid, err := GetActiveSlotPID(ctx, db, slotName)
	if err != nil {
		return err
	}

	if pid != nil {
		terminateSQL, err := RenderSQL(SQLTemplates.TerminateBackend, nil)
		if err != nil {
			return fmt.Errorf("failed to render TerminateBackend SQL: %w", err)
		}
		_, err = db.Exec(ctx, terminateSQL, *pid)
		if err != nil {
			return fmt.Errorf("failed to terminate backend (pid: %d) for replication slot %s: %w", *pid, slotName, err)
		}

		checkPidSQL, err := RenderSQL(SQLTemplates.CheckPIDExists, nil)
		if err != nil {
			return fmt.Errorf("failed to render CheckPIDExists SQL: %w", err)
		}

		for i := 0; i < 20; i++ {
			var checkPid int32
			err := db.QueryRow(ctx, checkPidSQL, *pid).Scan(&checkPid)
			if err == pgx.ErrNoRows {
				pid = nil // PID is gone.
				break
			}
			if err != nil {
				return fmt.Errorf("failed to check for PID %d: %w", *pid, err)
			}
			time.Sleep(100 * time.Millisecond)
		}

		if pid != nil {
			return fmt.Errorf("timed out waiting for backend (pid: %d) to terminate", *pid)
		}
	}

	data := map[string]interface{}{
		"SlotName": slotName,
	}

	sql, err := RenderSQL(SQLTemplates.DropReplicationSlot, data)
	if err != nil {
		return fmt.Errorf("failed to render DropReplicationSlot SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop replication slot failed: %w", err)
	}

	return nil
}

func DropCDCMetadataTable(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.DropCDCMetadataTable, nil)
	if err != nil {
		return fmt.Errorf("failed to render DropCDCMetadataTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to drop cdc metadata table failed: %w", err)
	}

	return nil
}

// GetCDCMetadata reads cdc metadata for a publication. pubCommitLSN is
// empty for legacy rows that pre-date that column; callers must treat
// empty as uncheckable and skip the publication-commit guard with a
// warning.
func GetCDCMetadata(ctx context.Context, db DBQuerier, publicationName string) (slotName, startLSN string, tables []string, pubCommitLSN string, err error) {
	sql, err := RenderSQL(SQLTemplates.GetCDCMetadata, nil)
	if err != nil {
		return "", "", nil, "", err
	}
	err = db.QueryRow(ctx, sql, publicationName).Scan(&slotName, &startLSN, &tables, &pubCommitLSN)
	if err != nil {
		return "", "", nil, "", err
	}
	return slotName, startLSN, tables, pubCommitLSN, nil
}

// InitCDCMetadata sets cdc metadata at init time, including pub_commit_lsn.
// Ongoing flushes use UpdateCDCMetadata, which leaves pub_commit_lsn
// untouched so it always reflects the LSN captured at init.
func InitCDCMetadata(ctx context.Context, db DBQuerier, publicationName, slotName, startLSN, pubCommitLSN string, tables []string) error {
	sql, err := RenderSQL(SQLTemplates.InitCDCMetadata, nil)
	if err != nil {
		return fmt.Errorf("failed to render InitCDCMetadata SQL: %w", err)
	}
	if tables == nil {
		tables = []string{}
	}
	_, err = db.Exec(ctx, sql, publicationName, slotName, startLSN, pubCommitLSN, tables)
	if err != nil {
		return fmt.Errorf("query to init cdc metadata failed: %w", err)
	}
	return nil
}

// CurrentWalInsertLSN returns the current WAL insert LSN. Called mid Phase
// A, after CREATE PUBLICATION, so the value is strictly less than Phase
// A's commit LSN; since Phase B's slot has consistent_point >= that commit
// LSN, the returned value is a safe lower bound for any valid replication
// start LSN.
func CurrentWalInsertLSN(ctx context.Context, db DBQuerier) (string, error) {
	sql, err := RenderSQL(SQLTemplates.CurrentWalInsertLSN, nil)
	if err != nil {
		return "", fmt.Errorf("failed to render CurrentWalInsertLSN SQL: %w", err)
	}
	var lsn string
	if err := db.QueryRow(ctx, sql).Scan(&lsn); err != nil {
		return "", fmt.Errorf("failed to fetch current WAL insert LSN: %w", err)
	}
	return lsn, nil
}

func UpdateMtreeCounters(ctx context.Context, db DBQuerier, mtreeTable string, isComposite bool, compositeTypeName string, pkeyType string, inserts, deletes, updates []string) error {
	sql, err := RenderSQL(SQLTemplates.UpdateMtreeCounters, struct {
		MtreeTable        string
		IsComposite       bool
		CompositeTypeName string
		PkeyType          string
	}{
		MtreeTable:        mtreeTable,
		IsComposite:       isComposite,
		CompositeTypeName: compositeTypeName,
		PkeyType:          pkeyType,
	})
	if err != nil {
		return err
	}

	args := pgx.NamedArgs{
		"inserts": inserts,
		"deletes": deletes,
		"updates": updates,
	}

	_, err = db.Exec(ctx, sql, args)
	return err
}

func CreateSchema(ctx context.Context, db DBQuerier, schemaName string) error {
	data := map[string]interface{}{
		"SchemaName": schemaName,
	}
	sql, err := RenderSQL(SQLTemplates.CreateSchema, data)
	if err != nil {
		return fmt.Errorf("failed to render CreateSchema SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to create schema failed: %w", err)
	}

	return nil
}

func ResetPositionsByStartFromTemp(ctx context.Context, db DBQuerier, mtreeTable string, offset int64) error {
	data := map[string]any{
		"MtreeTable": mtreeTable,
	}
	query, err := RenderSQL(SQLTemplates.ResetPositionsByStartFromTemp, data)
	if err != nil {
		return fmt.Errorf("failed to render ResetPositionsByStartFromTemp SQL: %w", err)
	}
	if _, err := db.Exec(ctx, query, offset); err != nil {
		return fmt.Errorf("query to reset positions from temp failed: %w", err)
	}
	return nil
}

func UpdateAllLeafNodePositionsToTemp(ctx context.Context, db DBQuerier, mtreeTable string, offset int64) error {
	data := map[string]any{
		"MtreeTable": mtreeTable,
	}
	sql, err := RenderSQL(SQLTemplates.UpdateAllLeafNodePositionsToTemp, data)
	if err != nil {
		return fmt.Errorf("failed to render UpdateAllLeafNodePositionsToTemp SQL: %w", err)
	}
	if _, err := db.Exec(ctx, sql, offset); err != nil {
		return fmt.Errorf("query to update all leaf node positions to temp failed: %w", err)
	}
	return nil
}

func AlterPublicationDropTable(ctx context.Context, db DBQuerier, publicationName, tableName string) error {
	data := map[string]interface{}{
		"PublicationName": publicationName,
		"TableName":       tableName,
	}
	sql, err := RenderSQL(SQLTemplates.AlterPublicationDropTable, data)
	if err != nil {
		return fmt.Errorf("failed to render AlterPublicationDropTable SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to alter publication failed: %w", err)
	}

	return nil
}

func DeleteMetadata(ctx context.Context, db DBQuerier, schema, table string) error {
	sql, err := RenderSQL(SQLTemplates.DeleteMetadata, nil)
	if err != nil {
		return fmt.Errorf("failed to render DeleteMetadata SQL: %w", err)
	}
	_, err = db.Exec(ctx, sql, schema, table)
	if err != nil {
		return fmt.Errorf("query to delete metadata for '%s.%s' failed: %w", schema, table, err)
	}
	return nil
}

func RemoveTableFromCDCMetadata(ctx context.Context, db DBQuerier, tableName, publicationName string) error {
	sql, err := RenderSQL(SQLTemplates.RemoveTableFromCDCMetadata, nil)
	if err != nil {
		return fmt.Errorf("failed to render RemoveTableFromCDCMetadata SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, tableName, publicationName)
	if err != nil {
		return fmt.Errorf("query to remove table '%s' from cdc metadata for publication '%s' failed: %w", tableName, publicationName, err)
	}

	return nil
}

func GetReplicationOriginByName(ctx context.Context, db DBQuerier, originName string) (*uint32, error) {
	sql, err := RenderSQL(SQLTemplates.GetReplicationOriginByName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to render GetReplicationOriginByName SQL: %w", err)
	}

	var originID uint32
	err = db.QueryRow(ctx, sql, originName).Scan(&originID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("query to get replication origin by name '%s' failed: %w", originName, err)
	}

	return &originID, nil
}

func CreateReplicationOrigin(ctx context.Context, db DBQuerier, originName string) (uint32, error) {
	sql, err := RenderSQL(SQLTemplates.CreateReplicationOrigin, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to render CreateReplicationOrigin SQL: %w", err)
	}

	var originID uint32
	err = db.QueryRow(ctx, sql, originName).Scan(&originID)
	if err != nil {
		return 0, fmt.Errorf("query to create replication origin '%s' failed: %w", originName, err)
	}

	return originID, nil
}

func SetupReplicationOriginSession(ctx context.Context, db DBQuerier, originName string) error {
	sql, err := RenderSQL(SQLTemplates.SetupReplicationOriginSession, nil)
	if err != nil {
		return fmt.Errorf("failed to render SetupReplicationOriginSession SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, originName)
	if err != nil {
		return fmt.Errorf("query to setup replication origin session for origin '%s' failed: %w", originName, err)
	}

	return nil
}

func ResetReplicationOriginSession(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.ResetReplicationOriginSession, nil)
	if err != nil {
		return fmt.Errorf("failed to render ResetReplicationOriginSession SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to reset replication origin session failed: %w", err)
	}

	return nil
}

func SetupReplicationOriginXact(ctx context.Context, db DBQuerier, originLSN string, originTimestamp *time.Time) error {
	if originTimestamp == nil {
		return fmt.Errorf("origin timestamp is required for pg_replication_origin_xact_setup (LSN %s)", originLSN)
	}

	sql, err := RenderSQL(SQLTemplates.SetupReplicationOriginXact, nil)
	if err != nil {
		return fmt.Errorf("failed to render SetupReplicationOriginXact SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql, originLSN, *originTimestamp)
	if err != nil {
		return fmt.Errorf("query to setup replication origin xact with LSN %s failed: %w", originLSN, err)
	}

	return nil
}

func ResetReplicationOriginXact(ctx context.Context, db DBQuerier) error {
	sql, err := RenderSQL(SQLTemplates.ResetReplicationOriginXact, nil)
	if err != nil {
		return fmt.Errorf("failed to render ResetReplicationOriginXact SQL: %w", err)
	}

	_, err = db.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("query to reset replication origin xact failed: %w", err)
	}

	return nil
}
