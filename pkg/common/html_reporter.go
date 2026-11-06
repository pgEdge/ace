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

package common

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pgedge/ace/pkg/logger"
	"github.com/pgedge/ace/pkg/types"
)

//go:embed templates/diff_report.html
var htmlDiffTemplate string

//go:embed templates/diff_report.css
var htmlDiffCSS string

//go:embed templates/diff_report.js
var htmlDiffJS string

// DefaultMaxHTMLRows is how many rows the HTML report shows for each node pair
// when neither the command line nor the config file sets max_html_rows. A row
// here is one primary key, whether it differs in value or is missing on one
// node. One row takes several kilobytes of markup, so without a limit a large
// diff gives a file of gigabytes that no browser can open.
const DefaultMaxHTMLRows int64 = 10000

// DefaultMaxHTMLValueLength is the number of characters of a value that the
// HTML report shows when neither the command line nor the config file sets
// max_html_value_length. A longer value is cut at the right side.
const DefaultMaxHTMLValueLength int64 = 1024

// htmlWriteBufferSize is the size of the buffer in front of the report file.
// It is a variable only so that tests can make every write reach the writer.
var htmlWriteBufferSize = 256 * 1024

// htmlPairCount captures the diff counts grouped by node pair for the report summary.
type htmlPairCount struct {
	Name  string
	Count string
}

type htmlSummaryItem struct {
	Label string
	Value string
}

type htmlSummaryData struct {
	Items     []htmlSummaryItem
	Breakdown []htmlPairCount
}

// htmlTruncation is shown at the top of a report that does not show every
// row of the diff.
type htmlTruncation struct {
	Shown    string
	Total    string
	Limit    string
	DiffFile string
}

type htmlReportHead struct {
	CSS        template.CSS
	Truncation *htmlTruncation
	Summary    htmlSummaryData
}

type htmlReportTail struct {
	JS template.JS
}

// htmlPairHead is the data for the "pair_head" and "pair_tail" blocks.
// Hidden is empty when the section shows every row of the pair. NotRendered
// is empty unless table-diff counted rows that the report leaves out because
// they show no difference.
type htmlPairHead struct {
	NodeA           string
	NodeB           string
	DiffCount       string
	Shown           string
	Total           string
	Hidden          string
	HiddenBreakdown string
	NotRendered     string
	DiffFile        string
	HasDiffs        bool
}

type htmlCell struct {
	Column     string
	IsKey      bool
	NodeAHTML  template.HTML
	NodeAClass string
	NodeBHTML  template.HTML
	NodeBClass string
	HasDiff    bool
}

// htmlRow is the data for one "value_row" or "missing_row" block.
type htmlRow struct {
	NodeA     string
	NodeB     string
	First     bool
	PKey      string
	RowType   string // "value_diff", "missing_in_a", "missing_in_b"
	Cells     []htmlCell
	NodeAJSON string
}

// htmlReportInfo goes into the embedded diff data, so that the page script
// knows when the repair plan it builds covers only part of the diff. The
// list of rows follows it in the page.
type htmlReportInfo struct {
	Truncated bool           `json:"truncated"`
	MaxRows   int64          `json:"max_html_rows"`
	DiffFile  string         `json:"diff_file"`
	IntegerPK bool           `json:"integer_pk"`
	Pairs     []htmlPairInfo `json:"pairs"`
}

// htmlPlanRow is what the page script needs to write a repair-plan rule for
// one shown row. Key is the display key, as in data-pk. Type uses the names
// of the repair executor. PK holds each primary key value as the JSON text
// that the diff file has for it. The script copies that text into the YAML
// as it is and never turns it into a JavaScript number, so a bigint keeps
// every digit and a text key such as "007" stays text.
type htmlPlanRow struct {
	Pair  string   `json:"pair"`
	NodeA string   `json:"node_a"`
	NodeB string   `json:"node_b"`
	Key   string   `json:"key"`
	Type  string   `json:"type"`
	PK    []string `json:"pk"`
}

type htmlPairInfo struct {
	Pair  string `json:"pair"`
	Shown int64  `json:"shown"`
	Total int64  `json:"total"`
}

// The kinds of a report row, as the page shows them.
const (
	htmlKindValue    = iota // a value difference
	htmlKindMissingB        // the row exists on node A only
	htmlKindMissingA        // the row exists on node B only
	htmlKinds
)

// htmlEntry is one shown row of the report: its markup data, with every
// long value already cut, and its primary key as JSON text.
type htmlEntry struct {
	kind    int
	row     htmlRow
	pk      []string
	display string
	id      string
}

// htmlPairPlan holds the shown rows of one node pair, in report order: the
// blocks of the diff in block order, and the rows of a block sorted by their
// key. Only these rows are in memory, with their values cut to maxLen; the
// other rows of the pair are only counted.
type htmlPairPlan struct {
	pairKey string
	nodeA   string
	nodeB   string
	columns []string
	pkSet   map[string]struct{}
	maxLen  int64
	entries []htmlEntry

	// all and shown count the rows of each kind in the diff and in the
	// report. notRendered counts the value differences of the read blocks
	// that show no difference after stringifyCellValue.
	all         [htmlKinds]int64
	shown       [htmlKinds]int64
	notRendered int64
}

// total is the number of rows that the report would show without a limit,
// as far as the read blocks tell.
func (p *htmlPairPlan) total() int64 {
	var n int64
	for _, c := range p.all {
		n += c
	}
	return n - p.notRendered
}

func (p *htmlPairPlan) shownCount() int64 {
	var n int64
	for _, c := range p.shown {
		n += c
	}
	return n
}

// hidden returns the number of rows of one kind that the report does not
// show.
func (p *htmlPairPlan) hidden(kind int) int64 {
	h := p.all[kind] - p.shown[kind]
	if kind == htmlKindValue {
		h -= p.notRendered
	}
	return h
}

// hiddenBreakdown describes the rows that the report does not show, for
// example "400,000 value differences, 50,000 missing in n3".
func (p *htmlPairPlan) hiddenBreakdown() string {
	var parts []string
	add := func(one, many string, hidden int64) {
		if hidden <= 0 {
			return
		}
		label := many
		if hidden == 1 {
			label = one
		}
		parts = append(parts, formatInt64WithCommas(hidden)+" "+label)
	}
	add("value difference", "value differences", p.hidden(htmlKindValue))
	add("missing in "+p.nodeB, "missing in "+p.nodeB, p.hidden(htmlKindMissingB))
	add("missing in "+p.nodeA, "missing in "+p.nodeA, p.hidden(htmlKindMissingA))
	return strings.Join(parts, ", ")
}

// planType is the repair executor's name for the kind of difference of a row
// in this pair: row_mismatch, missing_on_n2 or missing_on_n1, where n1 and n2
// are node A and node B of the pair.
const (
	planTypeMismatch  = "row_mismatch"
	planTypeMissingN2 = "missing_on_n2"
	planTypeMissingN1 = "missing_on_n1"
)

// pkLiterals returns each primary key value of the row as JSON text, the
// same text the diff file has for it.
func pkLiterals(row types.OrderedMap, primaryKey []string) []string {
	m := OrderedMapToMap(row)
	out := make([]string, len(primaryKey))
	for i, col := range primaryKey {
		b, err := json.Marshal(m[col])
		if err != nil {
			b = []byte("null")
		}
		out[i] = string(b)
	}
	return out
}

// errStopBlock ends the read of a block file early.
var errStopBlock = errors.New("stop reading the block")

// countLines returns the number of lines of a file.
func countLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	buf := make([]byte, 256*1024)
	var n int64
	for {
		k, err := f.Read(buf)
		n += int64(bytes.Count(buf[:k], []byte{'\n'}))
		if err == io.EOF {
			return n, nil
		}
		if err != nil {
			return 0, err
		}
	}
}

// buildHTMLPairPlan reads the blocks of one pair in block order until about
// maxRows rows are shown. The limit is applied at a block boundary: a block
// that would pass it is not read, unless it is the first block, which then
// gives the first maxRows rows of its file, and no other block is read. A
// value difference that shows no difference after stringifyCellValue (for
// example 1 against "1", or a difference only in _spock_metadata_) is left
// out and counted.
func buildHTMLPairPlan(ctx context.Context, r *StoredDiff, pair DiffResultPair, columns, primaryKey []string, maxRows, maxLen int64) (*htmlPairPlan, error) {
	nodeA, nodeB := pair.Node1, pair.Node2
	if names := strings.Split(pair.Pair, "/"); len(names) == 2 && names[0] == pair.Node2 && names[1] == pair.Node1 {
		nodeA, nodeB = pair.Node2, pair.Node1
	}
	swap := nodeA != pair.Node1
	p := &htmlPairPlan{
		pairKey: pair.Pair,
		nodeA:   nodeA,
		nodeB:   nodeB,
		columns: columns,
		pkSet:   make(map[string]struct{}, len(primaryKey)),
		maxLen:  maxLen,
	}
	for _, col := range primaryKey {
		p.pkSet[col] = struct{}{}
	}
	p.all[htmlKindValue] = pair.Changed
	p.all[htmlKindMissingB] = pair.Node1Only
	p.all[htmlKindMissingA] = pair.Node2Only
	if swap {
		p.all[htmlKindMissingB], p.all[htmlKindMissingA] = pair.Node2Only, pair.Node1Only
	}

	files, err := r.BlockFiles(pair)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		taken := p.shownCount()
		if taken >= maxRows {
			break
		}
		lines, err := countLines(file)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", file, err)
		}
		limit := lines
		if taken+lines > maxRows {
			if taken > 0 {
				break
			}
			limit = maxRows
		}
		block, err := p.readBlock(file, limit, swap, primaryKey)
		if err != nil {
			return nil, err
		}
		sort.SliceStable(block, func(i, j int) bool {
			if c := comparePKKey(block[i].display, block[j].display); c != 0 {
				return c < 0
			}
			return block[i].id < block[j].id
		})
		p.entries = append(p.entries, block...)
		if limit < lines {
			break
		}
	}
	return p, nil
}

// readBlock reads at most limit entries of one block file and returns the
// rows to show.
func (p *htmlPairPlan) readBlock(file string, limit int64, swap bool, primaryKey []string) ([]htmlEntry, error) {
	var block []htmlEntry
	var read int64
	err := ReadDiffBlock(file, func(e *DiffEntryLine) error {
		if read >= limit {
			return errStopBlock
		}
		read++
		var ent htmlEntry
		var rows [2]types.OrderedMap
		for i, raw := range []json.RawMessage{e.Row1, e.Row2} {
			if len(raw) == 0 {
				continue
			}
			if err := rows[i].UnmarshalJSON(raw); err != nil {
				return fmt.Errorf("%s: %w", file, err)
			}
		}
		rowA, rowB := rows[0], rows[1]
		if swap {
			rowA, rowB = rowB, rowA
		}
		switch {
		case e.Kind == DiffKindChanged:
			ent.kind = htmlKindValue
			if !rowsDiffer(rowA, rowB, p.columns) {
				p.notRendered++
				return nil
			}
		case rowB == nil:
			ent.kind = htmlKindMissingB
		default:
			ent.kind = htmlKindMissingA
		}
		row := rowA
		if row == nil {
			row = rowB
		}
		idx := int(p.shownCount())
		ent.id = buildRowKey(row, primaryKey, idx)
		ent.display = buildRowDisplayKey(row, primaryKey, idx)
		ent.pk = pkLiterals(row, primaryKey)
		if ent.kind == htmlKindValue {
			ent.row = p.valueRow(rowA, rowB, ent.display)
		} else {
			ent.row = p.missingRow(ent.kind, row, ent.display)
		}
		p.shown[ent.kind]++
		block = append(block, ent)
		return nil
	})
	if err != nil && !errors.Is(err, errStopBlock) {
		return nil, err
	}
	return block, nil
}

// cutValue cuts s at the right side to max characters. It returns the text
// to show and the length of s in characters if s was cut, or 0.
func cutValue(s string, max int64) (string, int) {
	if max <= 0 || int64(len(s)) <= max {
		return s, 0
	}
	n := utf8.RuneCountInString(s)
	if int64(n) <= max {
		return s, 0
	}
	i := 0
	for k := int64(0); k < max; k++ {
		_, size := utf8.DecodeRuneInString(s[i:])
		i += size
	}
	return s[:i], n
}

// cutMark is the markup after a value that the report shows cut. hidden
// says that the two values differ only in the part that is not shown.
func cutMark(full int, hidden bool) template.HTML {
	note := ""
	if hidden {
		note = ", differs after the shown part"
	}
	return template.HTML(`<span class="value-cut">… (` + formatInt64WithCommas(int64(full)) + ` characters` + note + `)</span>`)
}

// rowJSON returns the row as JSON for the custom row editor, or an empty
// string when a value of the row is cut on the page: an editor filled with
// cut values would write cut data in a repair.
func (p *htmlPairPlan) rowJSON(row types.OrderedMap, cut bool) string {
	if cut {
		return ""
	}
	return buildRowJSONPretty(row, p.columns)
}

func (p *htmlPairPlan) valueRow(a, b types.OrderedMap, display string) htmlRow {
	rowA := OrderedMapToMap(a)
	rowB := OrderedMapToMap(b)

	cut := false
	cells := make([]htmlCell, 0, len(p.columns))
	for _, col := range p.columns {
		valA := stringifyCellValue(rowA[col])
		valB := stringifyCellValue(rowB[col])
		_, isPK := p.pkSet[col]

		shownA, fullA := valA, 0
		shownB, fullB := valB, 0
		if !isPK {
			shownA, fullA = cutValue(valA, p.maxLen)
			shownB, fullB = cutValue(valB, p.maxLen)
		}
		hidden := valA != valB && shownA == shownB
		htmlA, htmlB := highlightDifference(shownA, shownB)
		if fullA > 0 {
			htmlA += cutMark(fullA, hidden)
			cut = true
		}
		if fullB > 0 {
			htmlB += cutMark(fullB, hidden)
			cut = true
		}
		c := htmlCell{
			Column:    col,
			IsKey:     isPK,
			NodeAHTML: htmlA,
			NodeBHTML: htmlB,
			HasDiff:   valA != valB,
		}
		if c.HasDiff {
			c.NodeAClass = "value-diff"
			c.NodeBClass = "value-diff"
		}
		cells = append(cells, c)
	}
	return htmlRow{
		NodeA:     p.nodeA,
		NodeB:     p.nodeB,
		PKey:      display,
		RowType:   "value_diff",
		Cells:     cells,
		NodeAJSON: p.rowJSON(a, cut),
	}
}

// missingRow builds a row that exists on one node only: on node A for
// htmlKindMissingB, on node B for htmlKindMissingA.
func (p *htmlPairPlan) missingRow(kind int, present types.OrderedMap, display string) htmlRow {
	missingInB := kind == htmlKindMissingB
	row := htmlRow{
		NodeA: p.nodeA,
		NodeB: p.nodeB,
		PKey:  display,
	}
	values := OrderedMapToMap(present)
	if missingInB {
		row.RowType = "missing_in_b"
	} else {
		row.RowType = "missing_in_a"
	}

	cut := false
	row.Cells = make([]htmlCell, 0, len(p.columns))
	for _, col := range p.columns {
		_, isPK := p.pkSet[col]
		text, full := stringifyCellValue(values[col]), 0
		if !isPK {
			text, full = cutValue(text, p.maxLen)
		}
		val := plainHTML(text)
		if full > 0 {
			val += cutMark(full, false)
			cut = true
		}
		c := htmlCell{Column: col, IsKey: isPK}
		if missingInB {
			c.NodeAHTML = val
			c.NodeBHTML = plainHTML("MISSING")
			c.NodeBClass = "missing"
		} else {
			c.NodeAHTML = plainHTML("MISSING")
			c.NodeAClass = "missing"
			c.NodeBHTML = val
		}
		row.Cells = append(row.Cells, c)
	}
	if missingInB {
		row.NodeAJSON = p.rowJSON(present, cut)
	}
	return row
}

// buildHTMLSummaryItems returns the label and value pairs of the summary at
// the top of the HTML report. shown and total are the numbers of rows that
// the report shows and that the diff found; truncated says that the report
// shows fewer rows. Items with an empty or zero value are left out.
func buildHTMLSummaryItems(summary types.DiffSummary, shown, total int64, truncated bool) []htmlSummaryItem {
	items := []htmlSummaryItem{
		{Label: "Table", Value: fmt.Sprintf("%s.%s", summary.Schema, summary.Table)},
		{Label: "Nodes", Value: strings.Join(summary.Nodes, ", ")},
		{Label: "Primary Key", Value: formatPrimaryKey(summary.PrimaryKey)},
		{Label: "Total Differences", Value: formatInt64WithCommas(totalDiffs(summary.DiffRowsCount))},
	}
	if truncated {
		items = append(items, htmlSummaryItem{
			Label: "Rows Shown in Report",
			Value: formatInt64WithCommas(shown) + " of " + formatInt64WithCommas(total),
		})
	}
	items = append(items,
		htmlSummaryItem{Label: "Total Rows Checked", Value: formatInt64WithCommas(summary.TotalRowsChecked)},
		htmlSummaryItem{Label: "Initial Ranges", Value: formatInt64WithCommas(int64(summary.InitialRangesCount))},
		htmlSummaryItem{Label: "Mismatched Ranges", Value: formatInt64WithCommas(int64(summary.MismatchedRangesCount))},
		htmlSummaryItem{Label: "Block Size", Value: formatInt64WithCommas(int64(summary.BlockSize))},
		htmlSummaryItem{Label: "Concurrency Factor", Value: strconv.FormatFloat(summary.ConcurrencyFactor, 'f', -1, 64)},
		htmlSummaryItem{Label: "Time Taken", Value: formatDurationHuman(summary.TimeTaken)},
		htmlSummaryItem{Label: "Start Time", Value: formatTimestampHuman(summary.StartTime)},
		htmlSummaryItem{Label: "End Time", Value: formatTimestampHuman(summary.EndTime)},
	)
	items = append(items, summaryLimitItems(summary)...)

	var filtered []htmlSummaryItem
	for _, item := range items {
		if item.Value != "" && item.Value != "0" {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// summaryLimitItems returns the summary items that tell the reader the diff
// may be incomplete.
func summaryLimitItems(summary types.DiffSummary) []htmlSummaryItem {
	var items []htmlSummaryItem
	if summary.MaxDiffRows > 0 {
		items = append(items, htmlSummaryItem{
			Label: "Max Diff Rows",
			Value: formatInt64WithCommas(summary.MaxDiffRows),
		})
		if summary.DiffRowLimitReached {
			items = append(items, htmlSummaryItem{
				Label: "Stopped Early",
				Value: "yes (max_diff_rows limit)",
			})
		}
	}
	return items
}

// WriteHTMLDiffReport writes the HTML report of a diff result next to the
// result directory, with the same base name and the extension .html, and
// returns its path. It shows about maxRows rows for each node pair and cuts
// each shown value to maxValueLen characters; a value of 0 or less means
// DefaultMaxHTMLRows or DefaultMaxHTMLValueLength.
func WriteHTMLDiffReport(ctx context.Context, r *StoredDiff, maxRows, maxValueLen int64) (htmlPath string, err error) {
	path := strings.TrimRight(r.Dir, string(os.PathSeparator)) + ".html"
	// Each run has its own temporary file, so two runs on one result do not
	// write into the same file.
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("failed to create HTML diff report: %w", err)
	}
	tmp := f.Name()
	if err := f.Chmod(SecureFileMode); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("restrict permissions on %s: %w", tmp, err)
	}
	// A report cut off in the middle looks complete in a browser up to the
	// point where it stops, so do not leave one behind.
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()

	diffFile := filepath.Base(strings.TrimRight(r.Dir, string(os.PathSeparator))) + ".json"
	if err = renderHTMLDiffReport(ctx, f, r, diffFile, maxRows, maxValueLen); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", fmt.Errorf("failed to close HTML diff report: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("failed to rename HTML diff report: %w", err)
	}
	return path, nil
}

// htmlBlockWriter runs template blocks and other writes, and keeps the first
// error. After an error it does nothing more, so a long run of writes needs a
// single check at the end.
type htmlBlockWriter struct {
	tmpl *template.Template
	w    *bufio.Writer
	err  error
}

func (b *htmlBlockWriter) do(fn func() error) {
	if b.err == nil {
		b.err = fn()
	}
}

func (b *htmlBlockWriter) block(name string, data any) {
	b.do(func() error { return b.tmpl.ExecuteTemplate(b.w, name, data) })
}

// renderHTMLDiffReport writes the report to out. diffFile is the name of the
// JSON report, which the page refers to for the full diff.
//
// The report goes out block by block through a buffered writer. The shown
// rows of every pair are held in memory, with their values cut to
// maxValueLen, before the first byte is written, because the page head says
// how many rows are shown.
func renderHTMLDiffReport(ctx context.Context, out io.Writer, r *StoredDiff, diffFile string, maxRows, maxValueLen int64) error {
	if maxRows <= 0 {
		maxRows = DefaultMaxHTMLRows
	}
	if maxValueLen <= 0 {
		maxValueLen = DefaultMaxHTMLValueLength
	}
	tmpl, err := template.New("tableDiffReport").Parse(htmlDiffTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse HTML template: %w", err)
	}

	m := r.Manifest
	summary := m.Summary
	columnNames := make([]string, 0, len(m.Columns))
	for _, c := range m.Columns {
		columnNames = append(columnNames, c.Name)
	}
	columns := reorderColumnsWithPriority(columnNames, m.PrimaryKey)

	// The page header says whether the report is truncated, so every pair
	// must be planned before the first byte is written.
	var plans []*htmlPairPlan
	for _, pair := range m.Pairs {
		p, err := buildHTMLPairPlan(ctx, r, pair, columns, m.PrimaryKey, maxRows, maxValueLen)
		if err != nil {
			return err
		}
		plans = append(plans, p)
	}
	head, info := buildHTMLReportHead(summary, plans, diffFile, maxRows, m.IntegerKey)

	w := bufio.NewWriterSize(out, htmlWriteBufferSize)
	bw := &htmlBlockWriter{tmpl: tmpl, w: w}
	bw.block("report_head", head)
	if len(plans) == 0 {
		bw.block("no_pairs", nil)
	}
	for _, p := range plans {
		writeHTMLPair(bw, p, summary, diffFile)
	}
	bw.do(func() error { return writeHTMLDiffData(w, summary, plans, info) })
	bw.block("report_tail", htmlReportTail{JS: template.JS(htmlDiffJS)})
	bw.do(w.Flush)
	if bw.err != nil {
		return fmt.Errorf("failed to write HTML diff report: %w", bw.err)
	}
	return nil
}

// buildHTMLReportHead returns the data of the page head and the report info
// for the page script. Both say whether the report is truncated.
func buildHTMLReportHead(summary types.DiffSummary, plans []*htmlPairPlan, diffFile string, maxRows int64, integerPK bool) (htmlReportHead, htmlReportInfo) {
	info := htmlReportInfo{
		MaxRows:   maxRows,
		DiffFile:  diffFile,
		IntegerPK: integerPK && len(summary.PrimaryKey) == 1,
		Pairs:     make([]htmlPairInfo, 0, len(plans)),
	}
	var shownAll, totalAll int64
	for _, p := range plans {
		shownAll += p.shownCount()
		totalAll += p.total()
		info.Pairs = append(info.Pairs, htmlPairInfo{Pair: p.pairKey, Shown: p.shownCount(), Total: p.total()})
	}
	info.Truncated = shownAll < totalAll

	head := htmlReportHead{
		CSS: template.CSS(htmlDiffCSS),
		Summary: htmlSummaryData{
			Items:     buildHTMLSummaryItems(summary, shownAll, totalAll, info.Truncated),
			Breakdown: buildDiffBreakdown(summary.DiffRowsCount),
		},
	}
	if info.Truncated {
		head.Truncation = &htmlTruncation{
			Shown:    formatInt64WithCommas(shownAll),
			Total:    formatInt64WithCommas(totalAll),
			Limit:    formatInt64WithCommas(maxRows),
			DiffFile: diffFile,
		}
		logger.Warn("HTML report shows %d of %d rows (max_html_rows=%d per node pair); the full diff is in %s",
			shownAll, totalAll, maxRows, diffFile)
	}
	return head, info
}

// writeHTMLPair writes the section of one node pair, one row at a time.
func writeHTMLPair(bw *htmlBlockWriter, p *htmlPairPlan, summary types.DiffSummary, diffFile string) {
	head := p.pairHead(summary, diffFile)
	bw.block("pair_head", head)
	for i, e := range p.entries {
		if bw.err != nil {
			return
		}
		row := e.row
		row.First = i == 0
		if e.kind == htmlKindValue {
			bw.block("value_row", row)
		} else {
			bw.block("missing_row", row)
		}
	}
	bw.block("pair_tail", head)
}

// pairHead returns the data of the pair head and tail.
func (p *htmlPairPlan) pairHead(summary types.DiffSummary, diffFile string) htmlPairHead {
	counted := summary.DiffRowsCount[p.pairKey]
	head := htmlPairHead{
		NodeA:     p.nodeA,
		NodeB:     p.nodeB,
		DiffCount: formatInt64WithCommas(int64(counted)),
		Total:     formatInt64WithCommas(p.total()),
		DiffFile:  diffFile,
		HasDiffs:  p.total() > 0,
	}
	if hidden := p.total() - p.shownCount(); hidden > 0 {
		head.Shown = formatInt64WithCommas(p.shownCount())
		head.Hidden = formatInt64WithCommas(hidden)
		head.HiddenBreakdown = p.hiddenBreakdown()
	}
	if p.notRendered > 0 {
		head.NotRendered = formatInt64WithCommas(p.notRendered)
	}
	return head
}

// writeHTMLDiffData embeds the data that the page script needs to build a
// repair plan: the diff summary, htmlReportInfo, and one htmlPlanRow for each
// shown row, in report order. The rows are written one at a time.
//
// json.Marshal escapes '<', '>' and '&', so no value can close the script
// element early.
func writeHTMLDiffData(w *bufio.Writer, summary types.DiffSummary, plans []*htmlPairPlan, info htmlReportInfo) error {
	w.WriteString(`<script id="diff-data" type="application/json">{"summary":`)
	if err := writeJSONTo(w, summary); err != nil {
		return err
	}
	w.WriteString(`,"report_info":`)
	if err := writeJSONTo(w, info); err != nil {
		return err
	}
	w.WriteString(`,"rows":[`)
	first := true
	for _, p := range plans {
		for _, e := range p.entries {
			if !first {
				w.WriteByte(',')
			}
			first = false
			row := htmlPlanRow{
				Pair:  p.pairKey,
				NodeA: p.nodeA,
				NodeB: p.nodeB,
				Key:   e.display,
				PK:    e.pk,
			}
			switch e.kind {
			case htmlKindValue:
				row.Type = planTypeMismatch
			case htmlKindMissingB:
				row.Type = planTypeMissingN2
			default:
				row.Type = planTypeMissingN1
			}
			if err := writeJSONTo(w, row); err != nil {
				return err
			}
		}
	}
	// bufio.Writer keeps the first write error and returns it from every
	// later call, so a single check at the end covers all the writes above.
	_, err := w.WriteString("]}</script>\n")
	return err
}

func writeJSONTo(w *bufio.Writer, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func highlightDifference(a, b string) (template.HTML, template.HTML) {
	if a == b {
		esc := template.HTMLEscapeString(a)
		return template.HTML(esc), template.HTML(esc)
	}

	runesA := []rune(a)
	runesB := []rune(b)

	prefix := 0
	maxPrefix := len(runesA)
	if len(runesB) < maxPrefix {
		maxPrefix = len(runesB)
	}
	for prefix < maxPrefix && runesA[prefix] == runesB[prefix] {
		prefix++
	}

	suffix := 0
	for suffix < len(runesA)-prefix && suffix < len(runesB)-prefix && runesA[len(runesA)-suffix-1] == runesB[len(runesB)-suffix-1] {
		suffix++
	}

	prefixA, suffixA := prefix, suffix
	prefixB, suffixB := prefix, suffix

	if len(runesA)-prefixA-suffixA <= 0 {
		prefixA = 0
		suffixA = 0
	}
	if len(runesB)-prefixB-suffixB <= 0 {
		prefixB = 0
		suffixB = 0
	}

	highlightedA := renderHighlighted(runesA, prefixA, suffixA)
	highlightedB := renderHighlighted(runesB, prefixB, suffixB)
	return highlightedA, highlightedB
}

func renderHighlighted(value []rune, prefix, suffix int) template.HTML {
	var builder strings.Builder

	if prefix > 0 {
		builder.WriteString(template.HTMLEscapeString(string(value[:prefix])))
	}

	middleLen := len(value) - prefix - suffix
	if middleLen > 0 {
		builder.WriteString(`<span class="diff-chunk">`)
		builder.WriteString(template.HTMLEscapeString(string(value[prefix : prefix+middleLen])))
		builder.WriteString(`</span>`)
	}

	if suffix > 0 {
		builder.WriteString(template.HTMLEscapeString(string(value[len(value)-suffix:])))
	}

	return template.HTML(builder.String())
}

func plainHTML(value string) template.HTML {
	return template.HTML(template.HTMLEscapeString(value))
}

func totalDiffs(diffCounts map[string]int) int64 {
	var total int64
	for _, count := range diffCounts {
		total += int64(count)
	}
	return total
}

func buildDiffBreakdown(diffCounts map[string]int) []htmlPairCount {
	if len(diffCounts) == 0 {
		return nil
	}

	pairs := make([]htmlPairCount, 0, len(diffCounts))
	for pair, count := range diffCounts {
		pairs = append(pairs, htmlPairCount{Name: pair, Count: formatInt64WithCommas(int64(count))})
	}
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].Name < pairs[j].Name })
	return pairs
}

// buildRowKey returns the identity a row is matched by across the two nodes.
// It falls back to the row's position when there is no usable primary key.
// That keeps distinct rows of one node distinct, but it pairs rows of the two
// nodes by position only: __row_0 on A meets __row_0 on B, whatever they hold.
func buildRowKey(row types.OrderedMap, primaryKey []string, index int) string {
	if len(primaryKey) == 0 {
		return fmt.Sprintf("__row_%d", index)
	}

	key, err := StringifyOrderedMapKey(row, primaryKey)
	if err != nil || key == "" {
		return fmt.Sprintf("__row_%d", index)
	}
	return key
}

// buildRowDisplayKey renders the same primary key for human eyes: the raw
// values joined by a pipe, with no quoting. Two rows whose pkey values differ
// only in where the pipes fall share a display key; they still have distinct
// identities, so neither is dropped from the report.
func buildRowDisplayKey(row types.OrderedMap, primaryKey []string, index int) string {
	if len(primaryKey) == 0 {
		return fmt.Sprintf("__row_%d", index)
	}

	parts := make([]string, len(primaryKey))
	for i, col := range primaryKey {
		val, ok := row.Get(col)
		if !ok {
			return fmt.Sprintf("__row_%d", index)
		}
		parts[i] = fmt.Sprintf("%v", val)
	}
	return strings.Join(parts, "|")
}

func reorderColumnsWithPriority(columns, priority []string) []string {
	ordered := make([]string, 0, len(columns))
	seen := make(map[string]struct{}, len(columns))

	for _, col := range priority {
		for _, candidate := range columns {
			if candidate == col {
				if _, ok := seen[candidate]; !ok {
					ordered = append(ordered, candidate)
					seen[candidate] = struct{}{}
				}
				break
			}
		}
	}

	for _, col := range columns {
		if _, ok := seen[col]; ok {
			continue
		}
		ordered = append(ordered, col)
		seen[col] = struct{}{}
	}

	return ordered
}

func rowsDiffer(rowA, rowB types.OrderedMap, columns []string) bool {
	mapA := OrderedMapToMap(rowA)
	mapB := OrderedMapToMap(rowB)

	for _, col := range columns {
		valA, okA := mapA[col]
		valB, okB := mapB[col]

		if !okA && !okB {
			continue
		}
		if stringifyCellValue(valA) != stringifyCellValue(valB) {
			return true
		}
	}

	return false
}

func stringifyCellValue(value any) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case string:
		return v
	case fmt.Stringer:
		return v.String()
	case json.Number:
		return v.String()
	default:
		if v == nil {
			return "NULL"
		}
		if b, err := json.Marshal(v); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}

func formatInt64WithCommas(value int64) string {
	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}

	s := strconv.FormatInt(value, 10)
	n := len(s)
	if n <= 3 {
		return sign + s
	}

	var builder strings.Builder
	builder.Grow(len(s) + len(s)/3)

	remainder := n % 3
	if remainder == 0 {
		remainder = 3
	}
	builder.WriteString(s[:remainder])
	for i := remainder; i < n; i += 3 {
		builder.WriteString(",")
		builder.WriteString(s[i : i+3])
	}

	return sign + builder.String()
}

func formatDurationHuman(durationStr string) string {
	if durationStr == "" {
		return ""
	}

	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		return durationStr
	}

	if dur < time.Millisecond {
		return fmt.Sprintf("%dµs", dur/time.Microsecond)
	}
	if dur < time.Second {
		return fmt.Sprintf("%.2f ms", float64(dur)/float64(time.Millisecond))
	}
	if dur < time.Minute {
		return fmt.Sprintf("%.2f s", dur.Seconds())
	}
	minutes := int(dur.Minutes())
	seconds := int(dur.Seconds()) % 60
	if dur < time.Hour {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	hours := minutes / 60
	minutes = minutes % 60
	return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
}

func formatTimestampHuman(ts string) string {
	if ts == "" {
		return ""
	}

	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return ts
	}
	return t.Format("02 Jan 2006 15:04:05 MST")
}

func formatPrimaryKey(pk []string) string {
	if len(pk) == 0 {
		return "N/A"
	}
	return strings.Join(pk, ", ")
}

// buildRowJSONPretty returns the row as an indented JSON object with the
// given columns, with the keys in sorted order.
func buildRowJSONPretty(row types.OrderedMap, columns []string) string {
	if len(row) == 0 || len(columns) == 0 {
		return ""
	}
	rowMap := OrderedMapToMap(row)
	ordered := make(map[string]any, len(columns))
	for _, col := range columns {
		if val, ok := rowMap[col]; ok {
			ordered[col] = val
		}
	}
	b, err := json.MarshalIndent(ordered, "", "  ")
	if err != nil {
		return ""
	}
	return string(b)
}

func comparePKKey(a, b string) int {
	partsA := strings.Split(a, "|")
	partsB := strings.Split(b, "|")
	max := len(partsA)
	if len(partsB) < max {
		max = len(partsB)
	}
	for idx := 0; idx < max; idx++ {
		if cmp := comparePKComponent(partsA[idx], partsB[idx]); cmp != 0 {
			return cmp
		}
	}
	switch {
	case len(partsA) < len(partsB):
		return -1
	case len(partsA) > len(partsB):
		return 1
	default:
		return 0
	}
}

// comparePKComponent orders numbers by value and before all other strings,
// and other strings byte-wise. The order must be total: the HTML report shows
// a prefix of the sorted rows, and with an order that is not transitive (as
// when a number and a string compared as strings, so that "1a" < "9" < "10"
// but "10" < "1a") the shown rows could change from one run to the next.
func comparePKComponent(a, b string) int {
	numA, okA := parseNumeric(a)
	numB, okB := parseNumeric(b)
	switch {
	case okA && okB:
		switch {
		case numA < numB:
			return -1
		case numA > numB:
			return 1
		default:
			return 0
		}
	case okA:
		return -1
	case okB:
		return 1
	}
	return strings.Compare(a, b)
}

func parseNumeric(val string) (float64, bool) {
	if val == "" {
		return 0, false
	}
	num, err := strconv.ParseFloat(val, 64)
	// NaN is not equal to anything, itself included, and would break the
	// order; treat it and the infinities as text.
	if err != nil || math.IsNaN(num) || math.IsInf(num, 0) {
		return 0, false
	}
	return num, true
}
