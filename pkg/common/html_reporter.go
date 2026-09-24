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
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

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
// they show no difference (see buildHTMLPairPlan).
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

type htmlGroupHead struct {
	Title         string
	DividerBefore bool
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
// list of rows follows it in the page (see writeHTMLDiffData).
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
	Shown int    `json:"shown"`
	Total int    `json:"total"`
}

// htmlPairPlan lists the report rows of one node pair, in report order:
// value differences, then rows missing on node B, then rows missing on node A.
// It holds only row keys. The row data stays in the diff result, and the
// markup for a row exists only while that row is written.
type htmlPairPlan struct {
	pairKey    string
	nodeA      string
	nodeB      string
	columns    []string
	rowMapA    map[string]types.OrderedMap
	rowMapB    map[string]types.OrderedMap
	display    map[string]string
	valueKeys  []string
	missingInB []string
	missingInA []string

	// How many rows of each list the report shows.
	shownValue    int
	shownMissingB int
	shownMissingA int
}

func (p *htmlPairPlan) total() int {
	return len(p.valueKeys) + len(p.missingInB) + len(p.missingInA)
}

func (p *htmlPairPlan) shown() int {
	return p.shownValue + p.shownMissingB + p.shownMissingA
}

// applyLimit shows at most limit rows of the pair and takes them in report
// order. So when value differences alone reach the limit, no missing rows
// are shown; the section note and footer then give the count of each kind.
func (p *htmlPairPlan) applyLimit(limit int64) {
	remaining := limit
	take := func(n int) int {
		if int64(n) > remaining {
			n = int(remaining)
		}
		remaining -= int64(n)
		return n
	}
	p.shownValue = take(len(p.valueKeys))
	p.shownMissingB = take(len(p.missingInB))
	p.shownMissingA = take(len(p.missingInA))
}

// hiddenBreakdown describes the rows that the report does not show, for
// example "400,000 value differences, 50,000 missing in n3".
func (p *htmlPairPlan) hiddenBreakdown() string {
	var parts []string
	add := func(one, many string, all, shown int) {
		hidden := all - shown
		if hidden <= 0 {
			return
		}
		label := many
		if hidden == 1 {
			label = one
		}
		parts = append(parts, formatInt64WithCommas(int64(hidden))+" "+label)
	}
	add("value difference", "value differences", len(p.valueKeys), p.shownValue)
	add("missing in "+p.nodeB, "missing in "+p.nodeB, len(p.missingInB), p.shownMissingB)
	add("missing in "+p.nodeA, "missing in "+p.nodeA, len(p.missingInA), p.shownMissingA)
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

var integerLiteralRe = regexp.MustCompile(`^-?[0-9]+$`)

// htmlIntegerPK tells whether the primary key is a single column and every
// row of the diff, in every pair, has a whole number in it. Only then can the
// page script write a range rule: a range over the shown keys 1 and 2 would
// also match a key 1.5 that the page did not show.
func htmlIntegerPK(plans []*htmlPairPlan, primaryKey []string) bool {
	if len(primaryKey) != 1 {
		return false
	}
	isInt := func(rows map[string]types.OrderedMap) bool {
		for _, row := range rows {
			if !integerLiteralRe.MatchString(pkLiterals(row, primaryKey)[0]) {
				return false
			}
		}
		return true
	}
	for _, p := range plans {
		if !isInt(p.rowMapA) || !isInt(p.rowMapB) {
			return false
		}
	}
	return true
}

// buildHTMLPairPlan pairs the rows of the two nodes and sorts them. A row
// that exists on both nodes but shows no difference after stringifyCellValue
// (for example 1 against "1", or a difference only in _spock_metadata_) is
// left out, as before; writeHTMLPair reports how many. It returns nil when
// the pair has no rows to show.
func buildHTMLPairPlan(pairKey string, nodeDiff types.DiffByNodePair, primaryKey []string) *htmlPairPlan {
	nodeNames := strings.Split(pairKey, "/")
	if len(nodeNames) != 2 {
		nodeNames = nodeNames[:0]
		for name := range nodeDiff.Rows {
			nodeNames = append(nodeNames, name)
		}
		sort.Strings(nodeNames)
		if len(nodeNames) < 2 {
			return nil
		}
	}

	p := &htmlPairPlan{
		pairKey: pairKey,
		nodeA:   nodeNames[0],
		nodeB:   nodeNames[1],
	}
	rowsA := nodeDiff.Rows[p.nodeA]
	rowsB := nodeDiff.Rows[p.nodeB]
	if len(rowsA) == 0 && len(rowsB) == 0 {
		return nil
	}

	p.columns = collectColumnsInOrder(primaryKey, rowsA, rowsB)

	// Two keys per row, and they are not interchangeable. buildRowKey is
	// the collision-proof identity used to pair a row on A with the same
	// row on B; buildRowDisplayKey is the plain rendering shown in the
	// report and embedded in data-pk, which the report's own JavaScript
	// interpolates into a CSS attribute selector and so cannot carry the
	// quotes the identity encoding adds.
	p.display = make(map[string]string, len(rowsA)+len(rowsB))

	p.rowMapA = make(map[string]types.OrderedMap, len(rowsA))
	for idx, row := range rowsA {
		key := buildRowKey(row, primaryKey, idx)
		p.rowMapA[key] = row
		p.display[key] = buildRowDisplayKey(row, primaryKey, idx)
	}

	p.rowMapB = make(map[string]types.OrderedMap, len(rowsB))
	for idx, row := range rowsB {
		key := buildRowKey(row, primaryKey, idx)
		p.rowMapB[key] = row
		p.display[key] = buildRowDisplayKey(row, primaryKey, idx)
	}

	for key, rowA := range p.rowMapA {
		if rowB, ok := p.rowMapB[key]; ok {
			if rowsDiffer(rowA, rowB, p.columns) {
				p.valueKeys = append(p.valueKeys, key)
			}
		} else {
			p.missingInB = append(p.missingInB, key)
		}
	}
	for key := range p.rowMapB {
		if _, ok := p.rowMapA[key]; !ok {
			p.missingInA = append(p.missingInA, key)
		}
	}

	sortPKKeys(p.valueKeys, p.display)
	sortPKKeys(p.missingInA, p.display)
	sortPKKeys(p.missingInB, p.display)
	return p
}

func (p *htmlPairPlan) valueRow(key string, pkSet map[string]struct{}) htmlRow {
	rowA := OrderedMapToMap(p.rowMapA[key])
	rowB := OrderedMapToMap(p.rowMapB[key])

	cells := make([]htmlCell, 0, len(p.columns))
	for _, col := range p.columns {
		valA := stringifyCellValue(rowA[col])
		valB := stringifyCellValue(rowB[col])
		_, isPK := pkSet[col]

		htmlA, htmlB := highlightDifference(valA, valB)
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
		PKey:      p.display[key],
		RowType:   "value_diff",
		Cells:     cells,
		NodeAJSON: buildRowJSONPretty(p.rowMapA[key], p.columns),
	}
}

// missingRow builds a row that exists on one node only. missingInB tells
// which node lacks it.
func (p *htmlPairPlan) missingRow(key string, missingInB bool, pkSet map[string]struct{}) htmlRow {
	row := htmlRow{
		NodeA: p.nodeA,
		NodeB: p.nodeB,
		PKey:  p.display[key],
	}
	var present map[string]any
	if missingInB {
		present = OrderedMapToMap(p.rowMapA[key])
		row.RowType = "missing_in_b"
		row.NodeAJSON = buildRowJSONPretty(p.rowMapA[key], p.columns)
	} else {
		present = OrderedMapToMap(p.rowMapB[key])
		row.RowType = "missing_in_a"
	}

	row.Cells = make([]htmlCell, 0, len(p.columns))
	for _, col := range p.columns {
		val := plainHTML(stringifyCellValue(present[col]))
		_, isPK := pkSet[col]
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
	return row
}

func buildHTMLSummaryItems(summary types.DiffSummary, shown, total int, truncated bool) []htmlSummaryItem {
	items := []htmlSummaryItem{
		{Label: "Table", Value: fmt.Sprintf("%s.%s", summary.Schema, summary.Table)},
		{Label: "Nodes", Value: strings.Join(summary.Nodes, ", ")},
		{Label: "Primary Key", Value: formatPrimaryKey(summary.PrimaryKey)},
		{Label: "Total Differences", Value: formatInt64WithCommas(totalDiffs(summary.DiffRowsCount))},
	}
	if truncated {
		items = append(items, htmlSummaryItem{
			Label: "Rows Shown in Report",
			Value: formatInt64WithCommas(int64(shown)) + " of " + formatInt64WithCommas(int64(total)),
		})
	}
	items = append(items,
		htmlSummaryItem{Label: "Total Rows Checked", Value: formatInt64WithCommas(summary.TotalRowsChecked)},
		htmlSummaryItem{Label: "Initial Ranges", Value: formatInt64WithCommas(int64(summary.InitialRangesCount))},
		htmlSummaryItem{Label: "Mismatched Ranges", Value: formatInt64WithCommas(int64(summary.MismatchedRangesCount))},
		htmlSummaryItem{Label: "Block Size", Value: formatInt64WithCommas(int64(summary.BlockSize))},
		htmlSummaryItem{Label: "Compare Unit Size", Value: formatInt64WithCommas(int64(summary.CompareUnitSize))},
		htmlSummaryItem{Label: "Concurrency Factor", Value: strconv.FormatFloat(summary.ConcurrencyFactor, 'f', -1, 64)},
		htmlSummaryItem{Label: "Time Taken", Value: formatDurationHuman(summary.TimeTaken)},
		htmlSummaryItem{Label: "Start Time", Value: formatTimestampHuman(summary.StartTime)},
		htmlSummaryItem{Label: "End Time", Value: formatTimestampHuman(summary.EndTime)},
	)

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

	// Without this, an incomplete run produces an HTML report that looks just
	// like a clean one. People who read the report never see the worker
	// errors, which only went to the log.
	if len(summary.IncompletePairs) > 0 {
		items = append(items, htmlSummaryItem{
			Label: "Comparison Incomplete",
			Value: strings.Join(summary.IncompletePairs, ", ") + " (counts are lower bounds)",
		})
	}

	var filtered []htmlSummaryItem
	for _, item := range items {
		if item.Value != "" && item.Value != "0" {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// writeHTMLDiffReport writes the HTML report next to the JSON report. It shows
// at most maxRows rows for each node pair; a value <= 0 means
// DefaultMaxHTMLRows.
func writeHTMLDiffReport(diffResult types.DiffOutput, jsonFilePath string, maxRows int64) (htmlPath string, err error) {
	if jsonFilePath == "" {
		return "", nil
	}

	path := strings.TrimSuffix(jsonFilePath, filepath.Ext(jsonFilePath)) + ".html"
	f, err := CreateFileSecure(path)
	if err != nil {
		return "", fmt.Errorf("failed to create HTML diff report: %w", err)
	}
	// A report cut off in the middle looks complete in a browser up to the
	// point where it stops, so do not leave one behind. The deferred call
	// uses its own copy of the path: by the time it runs, "return "", err"
	// has already cleared htmlPath.
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(path)
		}
	}()

	if err = renderHTMLDiffReport(f, diffResult, filepath.Base(jsonFilePath), maxRows); err != nil {
		return "", err
	}
	if err = f.Close(); err != nil {
		return "", fmt.Errorf("failed to close HTML diff report: %w", err)
	}
	return path, nil
}

// renderHTMLDiffReport writes the report to out. diffFile is the name of the
// JSON report, which the page refers to for the full diff.
//
// The report goes out block by block through a buffered writer. Only the row
// keys of the whole diff are held in memory, never the markup: at several
// kilobytes of markup per row, a document built in memory for a diff of half a
// million rows needs gigabytes.
func renderHTMLDiffReport(out io.Writer, diffResult types.DiffOutput, diffFile string, maxRows int64) error {
	if maxRows <= 0 {
		maxRows = DefaultMaxHTMLRows
	}

	tmpl, err := template.New("tableDiffReport").Parse(htmlDiffTemplate)
	if err != nil {
		return fmt.Errorf("failed to parse HTML template: %w", err)
	}

	summary := diffResult.Summary
	pkSet := make(map[string]struct{}, len(summary.PrimaryKey))
	for _, col := range summary.PrimaryKey {
		pkSet[col] = struct{}{}
	}

	pairKeys := make([]string, 0, len(diffResult.NodeDiffs))
	for key := range diffResult.NodeDiffs {
		pairKeys = append(pairKeys, key)
	}
	sort.Strings(pairKeys)

	// The page header says whether the report is truncated, so every pair
	// must be counted before the first byte is written.
	var plans []*htmlPairPlan
	var shownAll, totalAll int
	for _, pairKey := range pairKeys {
		p := buildHTMLPairPlan(pairKey, diffResult.NodeDiffs[pairKey], summary.PrimaryKey)
		if p == nil {
			continue
		}
		p.applyLimit(maxRows)
		shownAll += p.shown()
		totalAll += p.total()
		plans = append(plans, p)
	}
	truncated := shownAll < totalAll

	head := htmlReportHead{
		CSS: template.CSS(htmlDiffCSS),
		Summary: htmlSummaryData{
			Items:     buildHTMLSummaryItems(summary, shownAll, totalAll, truncated),
			Breakdown: buildDiffBreakdown(summary.DiffRowsCount),
		},
	}
	if truncated {
		head.Truncation = &htmlTruncation{
			Shown:    formatInt64WithCommas(int64(shownAll)),
			Total:    formatInt64WithCommas(int64(totalAll)),
			Limit:    formatInt64WithCommas(maxRows),
			DiffFile: diffFile,
		}
		logger.Warn("HTML report shows %d of %d rows (max_html_rows=%d per node pair); the full diff is in %s",
			shownAll, totalAll, maxRows, diffFile)
	}

	w := bufio.NewWriterSize(out, htmlWriteBufferSize)
	if err := tmpl.ExecuteTemplate(w, "report_head", head); err != nil {
		return fmt.Errorf("failed to render HTML diff report: %w", err)
	}
	if len(plans) == 0 {
		if err := tmpl.ExecuteTemplate(w, "no_pairs", nil); err != nil {
			return fmt.Errorf("failed to render HTML diff report: %w", err)
		}
	}
	for _, p := range plans {
		if err := writeHTMLPair(tmpl, w, p, summary, pkSet, diffFile); err != nil {
			return fmt.Errorf("failed to render HTML diff report: %w", err)
		}
	}

	info := htmlReportInfo{
		Truncated: truncated,
		MaxRows:   maxRows,
		DiffFile:  diffFile,
		IntegerPK: htmlIntegerPK(plans, summary.PrimaryKey),
		Pairs:     make([]htmlPairInfo, 0, len(plans)),
	}
	for _, p := range plans {
		info.Pairs = append(info.Pairs, htmlPairInfo{Pair: p.pairKey, Shown: p.shown(), Total: p.total()})
	}
	if err := writeHTMLDiffData(w, summary, plans, info, summary.PrimaryKey); err != nil {
		return fmt.Errorf("failed to embed diff data in HTML report: %w", err)
	}

	if err := tmpl.ExecuteTemplate(w, "report_tail", htmlReportTail{JS: template.JS(htmlDiffJS)}); err != nil {
		return fmt.Errorf("failed to render HTML diff report: %w", err)
	}
	if err := w.Flush(); err != nil {
		return fmt.Errorf("failed to write HTML diff report: %w", err)
	}
	return nil
}

// writeHTMLPair writes the section of one node pair, one row at a time.
func writeHTMLPair(tmpl *template.Template, w io.Writer, p *htmlPairPlan, summary types.DiffSummary, pkSet map[string]struct{}, diffFile string) error {
	// Every count in the section comes from the plan, so the numbers on the
	// page always add up. The engine's own count can be higher; the note
	// below says why.
	counted := summary.DiffRowsCount[p.pairKey]
	head := htmlPairHead{
		NodeA:     p.nodeA,
		NodeB:     p.nodeB,
		DiffCount: formatInt64WithCommas(int64(counted)),
		Total:     formatInt64WithCommas(int64(p.total())),
		DiffFile:  diffFile,
		HasDiffs:  p.total() > 0,
	}
	if hidden := p.total() - p.shown(); hidden > 0 {
		head.Shown = formatInt64WithCommas(int64(p.shown()))
		head.Hidden = formatInt64WithCommas(int64(hidden))
		head.HiddenBreakdown = p.hiddenBreakdown()
	}
	if notRendered := counted - p.total(); notRendered > 0 {
		head.NotRendered = formatInt64WithCommas(int64(notRendered))
	}

	if err := tmpl.ExecuteTemplate(w, "pair_head", head); err != nil {
		return err
	}

	if p.shownValue > 0 {
		if err := tmpl.ExecuteTemplate(w, "separator", "Value Differences"); err != nil {
			return err
		}
		for i, key := range p.valueKeys[:p.shownValue] {
			row := p.valueRow(key, pkSet)
			row.First = i == 0
			if err := tmpl.ExecuteTemplate(w, "value_row", row); err != nil {
				return err
			}
		}
	}

	if p.shownMissingB > 0 || p.shownMissingA > 0 {
		if err := tmpl.ExecuteTemplate(w, "separator", "Missing Rows"); err != nil {
			return err
		}
		groups := []struct {
			keys       []string
			missingInB bool
			title      string
		}{
			{p.missingInB[:p.shownMissingB], true, "Missing in " + p.nodeB},
			{p.missingInA[:p.shownMissingA], false, "Missing in " + p.nodeA},
		}
		groupWritten := false
		for _, g := range groups {
			if len(g.keys) == 0 {
				continue
			}
			if err := tmpl.ExecuteTemplate(w, "group_head", htmlGroupHead{Title: g.title, DividerBefore: groupWritten}); err != nil {
				return err
			}
			groupWritten = true
			for i, key := range g.keys {
				row := p.missingRow(key, g.missingInB, pkSet)
				row.First = i == 0
				if err := tmpl.ExecuteTemplate(w, "missing_row", row); err != nil {
					return err
				}
			}
		}
	}

	return tmpl.ExecuteTemplate(w, "pair_tail", head)
}

// writeHTMLDiffData embeds the data that the page script needs to build a
// repair plan: the diff summary, htmlReportInfo, and one htmlPlanRow for each
// shown row, in report order. The rows are written one at a time, so the
// list never exists in memory as a whole.
//
// json.Marshal escapes '<', '>' and '&', so no value can close the script
// element early.
func writeHTMLDiffData(w *bufio.Writer, summary types.DiffSummary, plans []*htmlPairPlan, info htmlReportInfo, primaryKey []string) error {
	writeJSON := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		w.Write(b)
		return nil
	}

	w.WriteString(`<script id="diff-data" type="application/json">{"summary":`)
	if err := writeJSON(summary); err != nil {
		return err
	}
	w.WriteString(`,"html_report":`)
	if err := writeJSON(info); err != nil {
		return err
	}
	w.WriteString(`,"rows":[`)
	first := true
	for _, p := range plans {
		lists := []struct {
			keys     []string
			rows     map[string]types.OrderedMap
			planType string
		}{
			{p.valueKeys[:p.shownValue], p.rowMapA, planTypeMismatch},
			{p.missingInB[:p.shownMissingB], p.rowMapA, planTypeMissingN2},
			{p.missingInA[:p.shownMissingA], p.rowMapB, planTypeMissingN1},
		}
		for _, l := range lists {
			for _, key := range l.keys {
				if !first {
					w.WriteByte(',')
				}
				first = false
				if err := writeJSON(htmlPlanRow{
					Pair:  p.pairKey,
					NodeA: p.nodeA,
					NodeB: p.nodeB,
					Key:   p.display[key],
					Type:  l.planType,
					PK:    pkLiterals(l.rows[key], primaryKey),
				}); err != nil {
					return err
				}
			}
		}
	}
	// bufio.Writer keeps the first write error and returns it from every
	// later call, so a single check at the end covers all the writes above.
	_, err := w.WriteString("]}</script>\n")
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

func collectColumnsInOrder(primaryKey []string, rowSets ...[]types.OrderedMap) []string {
	seen := make(map[string]struct{})
	var columns []string

	for _, rows := range rowSets {
		for _, row := range rows {
			for _, kv := range row {
				if kv.Key == "_spock_metadata_" {
					continue
				}
				if _, ok := seen[kv.Key]; !ok {
					seen[kv.Key] = struct{}{}
					columns = append(columns, kv.Key)
				}
			}
		}
	}

	return reorderColumnsWithPriority(columns, primaryKey)
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

// buildRowJSON returns a deterministic JSON object string for the given row using the provided column order.
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

// sortPKKeys orders row identity keys by their displayed primary key, using
// numeric-aware comparison so 2 sorts before 10, and falls back to the
// identity key itself when two displays compare equal. The identity keys are a
// quoted encoding that comparePKKey cannot read numerically, hence the lookup
// through display.
func sortPKKeys(keys []string, display map[string]string) {
	// A key with no display entry would compare as the empty string against
	// every other one, which is not an ordering; fall back to the key itself
	// so the report stays deterministic.
	shown := func(key string) string {
		if d, ok := display[key]; ok {
			return d
		}
		return key
	}
	sort.Slice(keys, func(i, j int) bool {
		if c := comparePKKey(shown(keys[i]), shown(keys[j])); c != 0 {
			return c < 0
		}
		// Distinct rows can share a display key -- that is the whole reason
		// identity and display are separate -- and comparePKComponent also
		// ties values that differ only in spelling, such as 1 and 1.0.
		// sort.Slice is not stable and the input order comes from map
		// iteration, so without a tie-break on the identity the report
		// reorders those rows from one run to the next.
		return keys[i] < keys[j]
	})
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
// but "10" < "1a") the prefix changed from one run to the next.
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
