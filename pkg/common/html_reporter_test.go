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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/pgedge/ace/pkg/types"
)

// htmlTestDiff builds a two-node diff with the given number of value
// differences, rows missing on n2 and rows missing on n1. Primary keys are
// 1..N in that order, so the report order is easy to predict.
func htmlTestDiff(valueDiffs, missingOnB, missingOnA int, note string) types.DiffOutput {
	row := func(id int, val string) types.OrderedMap {
		return types.OrderedMap{
			{Key: "id", Value: id},
			{Key: "val", Value: val},
			{Key: "note", Value: note},
		}
	}
	var a, b []types.OrderedMap
	id := 0
	for i := 0; i < valueDiffs; i++ {
		id++
		a = append(a, row(id, "on-n1"))
		b = append(b, row(id, "on-n2"))
	}
	for i := 0; i < missingOnB; i++ {
		id++
		a = append(a, row(id, "only-n1"))
	}
	for i := 0; i < missingOnA; i++ {
		id++
		b = append(b, row(id, "only-n2"))
	}
	return types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{
			"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": a, "n2": b}},
		},
		Summary: types.DiffSummary{
			Schema:        "public",
			Table:         "t",
			Nodes:         []string{"n1", "n2"},
			PrimaryKey:    []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": valueDiffs + missingOnB + missingOnA},
		},
	}
}

type htmlTestData struct {
	Summary    types.DiffSummary `json:"summary"`
	HTMLReport htmlReportInfo    `json:"html_report"`
	Rows       []htmlPlanRow     `json:"rows"`
}

// typeCounts counts the embedded plan rows of each type for one pair.
func (d htmlTestData) typeCounts(pair string) map[string]int {
	c := map[string]int{}
	for _, r := range d.Rows {
		if r.Pair == pair {
			c[r.Type]++
		}
	}
	return c
}

var diffDataRe = regexp.MustCompile(`(?s)<script id="diff-data" type="application/json">(.*?)</script>`)

// renderHTMLTestReport writes the report and returns the page and the parsed
// embedded diff data.
func renderHTMLTestReport(t *testing.T, diff types.DiffOutput, maxRows int64) (string, htmlTestData) {
	t.Helper()
	jsonPath := filepath.Join(t.TempDir(), "public_t_diffs-20260101000000.json")
	htmlPath, err := writeHTMLDiffReport(diff, jsonPath, maxRows)
	if err != nil {
		t.Fatalf("writeHTMLDiffReport: %v", err)
	}
	raw, err := os.ReadFile(htmlPath)
	if err != nil {
		t.Fatalf("read report: %v", err)
	}
	page := string(raw)
	if !strings.HasSuffix(strings.TrimSpace(page), "</html>") {
		t.Fatal("report does not end with </html>")
	}

	m := diffDataRe.FindStringSubmatch(page)
	if m == nil {
		t.Fatal("report has no diff-data script")
	}
	var data htmlTestData
	if err := json.Unmarshal([]byte(m[1]), &data); err != nil {
		t.Fatalf("embedded diff data is not valid JSON: %v", err)
	}
	return page, data
}

// countRows counts rendered rows of one type ("value_diff", "missing_in_a",
// "missing_in_b"). Every rendered row has exactly one select checkbox.
func countRows(page, rowType string) int {
	re := regexp.MustCompile(`class="row-select" data-pk="[^"]*" data-type="` + rowType + `"`)
	return len(re.FindAllStringIndex(page, -1))
}

func TestHTMLReportNotTruncated(t *testing.T) {
	page, data := renderHTMLTestReport(t, htmlTestDiff(4, 2, 1, "x"), 0)

	// Look for the elements, not the bare class names: the embedded CSS
	// always has those.
	for _, marker := range []string{`class="truncation-banner"`, `class="truncation-note"`, `class="truncation-row"`, "Rows Shown in Report"} {
		if strings.Contains(page, marker) {
			t.Errorf("report that shows every entry contains %q", marker)
		}
	}
	if !strings.Contains(page, ">7 rows<") {
		t.Error("section pill does not show the plain row count")
	}
	if got := countRows(page, "value_diff"); got != 4 {
		t.Errorf("value rows: got %d, want 4", got)
	}
	if got := countRows(page, "missing_in_b"); got != 2 {
		t.Errorf("missing_in_b rows: got %d, want 2", got)
	}
	if got := countRows(page, "missing_in_a"); got != 1 {
		t.Errorf("missing_in_a rows: got %d, want 1", got)
	}

	if data.HTMLReport.Truncated {
		t.Error("html_report.truncated is true for a complete report")
	}
	if data.HTMLReport.MaxRows != DefaultMaxHTMLRows {
		t.Errorf("max_html_rows: got %d, want the default %d", data.HTMLReport.MaxRows, DefaultMaxHTMLRows)
	}
	want := map[string]int{"row_mismatch": 4, "missing_on_n2": 2, "missing_on_n1": 1}
	if got := data.typeCounts("n1/n2"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("embedded rows: got %v, want %v", got, want)
	}
	if data.Summary.Table != "t" || len(data.Summary.PrimaryKey) != 1 {
		t.Errorf("embedded summary is wrong: %+v", data.Summary)
	}
}

// The limit takes entries in report order: value differences first, then
// rows missing on n2, then rows missing on n1.
func TestHTMLReportTruncated(t *testing.T) {
	// 25 value differences, 10 missing on n2, 5 missing on n1; limit 30.
	page, data := renderHTMLTestReport(t, htmlTestDiff(25, 10, 5, "x"), 30)

	if got := countRows(page, "value_diff"); got != 25 {
		t.Errorf("value rows: got %d, want 25", got)
	}
	if got := countRows(page, "missing_in_b"); got != 5 {
		t.Errorf("missing_in_b rows: got %d, want 5", got)
	}
	if got := countRows(page, "missing_in_a"); got != 0 {
		t.Errorf("missing_in_a rows: got %d, want 0", got)
	}
	if !strings.Contains(page, ">Missing in n2<") {
		t.Error("group of rows missing on n2 is not shown")
	}
	if strings.Contains(page, ">Missing in n1<") {
		t.Error("group header for rows missing on n1 is shown, but none of its rows are")
	}

	for _, want := range []string{
		"This report shows 30 of 40 rows",
		"public_t_diffs-20260101000000.json",
		"Its default action is <code>skip</code>",
		"Rows Shown in Report",
		"30 of 40 rows shown",
		"Not shown: 5 missing in n2, 5 missing in n1.",
		"table-repair skips the other rows",
		"they stay different",
		"--max-html-rows",
		"10 more rows are not shown (5 missing in n2, 5 missing in n1)",
		"Default: insert from n1",
		`data-truncated="true"`,
	} {
		if !strings.Contains(page, want) {
			t.Errorf("truncated report does not contain %q", want)
		}
	}

	// The page script builds repair plans from the embedded data, so it
	// must hold exactly the rows on the page and say that it is partial.
	want := map[string]int{"row_mismatch": 25, "missing_on_n2": 5}
	if got := data.typeCounts("n1/n2"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("embedded rows: got %v, want %v", got, want)
	}
	info := data.HTMLReport
	if !info.Truncated || info.MaxRows != 30 || info.DiffFile != "public_t_diffs-20260101000000.json" {
		t.Errorf("html_report: got %+v", info)
	}
	if len(info.Pairs) != 1 || info.Pairs[0] != (htmlPairInfo{Pair: "n1/n2", Shown: 30, Total: 40}) {
		t.Errorf("html_report.pairs: got %+v", info.Pairs)
	}
}

func TestHTMLReportTruncatedInsideValueDiffs(t *testing.T) {
	page, data := renderHTMLTestReport(t, htmlTestDiff(25, 10, 5, "x"), 20)

	if got := countRows(page, "value_diff"); got != 20 {
		t.Errorf("value rows: got %d, want 20", got)
	}
	if strings.Contains(page, ">Missing Rows<") {
		t.Error("missing rows separator is shown, but no missing row is")
	}
	want := "20 more rows are not shown (5 value differences, 10 missing in n2, 5 missing in n1)"
	if !strings.Contains(page, want) {
		t.Errorf("footer does not contain %q", want)
	}

	// The shown rows are the first 20 primary keys.
	if len(data.Rows) != 20 {
		t.Fatalf("embedded rows: got %d, want 20", len(data.Rows))
	}
	for i, r := range data.Rows {
		if r.PK[0] != fmt.Sprint(i+1) {
			t.Fatalf("embedded row %d has pk %v, want %d", i, r.PK, i+1)
		}
	}
}

// Primary key values go into the embedded JSON inside a <script> element,
// twice: as the display key and as a JSON literal. A value must not be able
// to close that element.
func TestHTMLReportEmbeddedDataCannotCloseScript(t *testing.T) {
	evil := `</script><script>alert(1)</script><!--`
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{
			"n1": {{{Key: "id", Value: evil}, {Key: "v", Value: "a"}}},
			"n2": {{{Key: "id", Value: evil}, {Key: "v", Value: "b"}}},
		}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"}, DiffRowsCount: map[string]int{"n1/n2": 1}},
	}
	page, data := renderHTMLTestReport(t, diff, 0)

	if strings.Contains(page, evil) {
		t.Fatal("raw value with </script> appears in the page")
	}
	if got := strings.Count(page, "<script"); got != 2 {
		t.Errorf("page has %d <script> tags, want 2", got)
	}
	if len(data.Rows) != 1 || data.Rows[0].Key != evil {
		t.Fatalf("embedded key after decoding: got %+v", data.Rows)
	}
	var lit string
	if err := json.Unmarshal([]byte(data.Rows[0].PK[0]), &lit); err != nil || lit != evil {
		t.Errorf("embedded pk literal %q does not decode to the value (%v)", data.Rows[0].PK[0], err)
	}
}

func TestHTMLReportNoRows(t *testing.T) {
	diff := htmlTestDiff(0, 0, 0, "")
	diff.Summary.IncompletePairs = []string{"n1/n2"}
	page, data := renderHTMLTestReport(t, diff, 0)

	if !strings.Contains(page, "No row-level differences were recorded.") {
		t.Error("empty report has no empty message")
	}
	if len(data.Rows) != 0 {
		t.Errorf("embedded rows: got %d, want 0", len(data.Rows))
	}
}

func TestHTMLPairPlanApplyLimit(t *testing.T) {
	p := &htmlPairPlan{
		valueKeys:  make([]string, 3),
		missingInB: make([]string, 4),
		missingInA: make([]string, 5),
	}
	for _, tc := range []struct {
		limit            int64
		value, mb, ma, n int
	}{
		{limit: 1, value: 1, n: 1},
		{limit: 3, value: 3, n: 3},
		{limit: 5, value: 3, mb: 2, n: 5},
		{limit: 9, value: 3, mb: 4, ma: 2, n: 9},
		{limit: 12, value: 3, mb: 4, ma: 5, n: 12},
		{limit: 1000, value: 3, mb: 4, ma: 5, n: 12},
	} {
		p.applyLimit(tc.limit)
		if p.shownValue != tc.value || p.shownMissingB != tc.mb || p.shownMissingA != tc.ma || p.shown() != tc.n {
			t.Errorf("limit %d: got %d/%d/%d (%d), want %d/%d/%d (%d)", tc.limit,
				p.shownValue, p.shownMissingB, p.shownMissingA, p.shown(), tc.value, tc.mb, tc.ma, tc.n)
		}
	}
}

// threeNodeDiff has two node pairs with different row sets, so a mix-up
// between pairs or nodes shows in the key checks.
func threeNodeDiff() types.DiffOutput {
	d := htmlTestDiff(12, 6, 4, "x")
	pair := d.NodeDiffs["n1/n2"]
	var n1, n3 []types.OrderedMap
	for i := 101; i <= 110; i++ {
		r := types.OrderedMap{{Key: "id", Value: i}, {Key: "val", Value: "a"}, {Key: "note", Value: "y"}}
		n1 = append(n1, r)
		if i%2 == 0 {
			n3 = append(n3, types.OrderedMap{{Key: "id", Value: i}, {Key: "val", Value: "b"}, {Key: "note", Value: "y"}})
		}
	}
	n3 = append(n3, types.OrderedMap{{Key: "id", Value: 200}, {Key: "val", Value: "c"}, {Key: "note", Value: "y"}})
	d.NodeDiffs = map[string]types.DiffByNodePair{
		"n1/n2": pair,
		"n1/n3": {Rows: map[string][]types.OrderedMap{"n1": n1, "n3": n3}},
	}
	d.Summary.Nodes = []string{"n1", "n2", "n3"}
	d.Summary.DiffRowsCount = map[string]int{"n1/n2": 22, "n1/n3": 11}
	return d
}

// pageKeys returns, for each section (in page order), the primary keys of the
// rendered rows by row type.
func pageKeys(t *testing.T, page string) []map[string][]string {
	t.Helper()
	var sections []map[string][]string
	for _, sec := range strings.Split(page, `<div class="diff-section" `)[1:] {
		keys := map[string][]string{}
		re := regexp.MustCompile(`class="row-select" data-pk="([^"]*)" data-type="([^"]*)"`)
		for _, m := range re.FindAllStringSubmatch(sec, -1) {
			keys[m[2]] = append(keys[m[2]], m[1])
		}
		sections = append(sections, keys)
	}
	return sections
}

// The page script builds repair plans from the embedded rows and picks
// user choices by data-pk, so the rendered rows and the embedded rows must be
// the same rows, of the same type, in the same order, for every pair.
func TestHTMLReportPageAndDataHoldTheSameRows(t *testing.T) {
	page, data := renderHTMLTestReport(t, threeNodeDiff(), 8)
	sections := pageKeys(t, page)
	if len(sections) != 2 {
		t.Fatalf("got %d sections, want 2", len(sections))
	}
	pageType := map[string]string{"row_mismatch": "value_diff", "missing_on_n2": "missing_in_b", "missing_on_n1": "missing_in_a"}

	for i, pc := range []struct{ pair, a, b string }{{"n1/n2", "n1", "n2"}, {"n1/n3", "n1", "n3"}} {
		got := map[string][]string{}
		for _, r := range data.Rows {
			if r.Pair != pc.pair {
				continue
			}
			if r.NodeA != pc.a || r.NodeB != pc.b {
				t.Errorf("%s: row %s has nodes %s/%s", pc.pair, r.Key, r.NodeA, r.NodeB)
			}
			if r.PK[0] != r.Key {
				t.Errorf("%s: pk literal %s does not match key %s", pc.pair, r.PK[0], r.Key)
			}
			got[pageType[r.Type]] = append(got[pageType[r.Type]], r.Key)
		}
		if fmt.Sprint(got) != fmt.Sprint(sections[i]) {
			t.Errorf("%s: embedded rows %v, page shows %v", pc.pair, got, sections[i])
		}
		n := len(got["value_diff"]) + len(got["missing_in_b"]) + len(got["missing_in_a"])
		if n != 8 {
			t.Errorf("%s: %d rows, want the limit 8", pc.pair, n)
		}
		if info := data.HTMLReport.Pairs[i]; info.Pair != pc.pair || info.Shown != n {
			t.Errorf("%s: html_report says %+v, page shows %d rows", pc.pair, info, n)
		}
	}
	if data.HTMLReport.Pairs[0].Total != 22 || data.HTMLReport.Pairs[1].Total != 11 {
		t.Errorf("pair totals: got %+v", data.HTMLReport.Pairs)
	}
}

// Keys go into the plan as the JSON text of the diff file, so a bigint keeps
// every digit and a text key that looks like a number stays text. Ranges are
// allowed only when every key of the diff is a whole number.
func TestHTMLReportPKLiterals(t *testing.T) {
	one := func(v1, v2 any) types.DiffOutput {
		return types.DiffOutput{
			NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{
				"n1": {{{Key: "id", Value: v1}, {Key: "v", Value: "a"}}, {{Key: "id", Value: v2}, {Key: "v", Value: "a"}}},
				"n2": {{{Key: "id", Value: v1}, {Key: "v", Value: "b"}}},
			}}},
			Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"}, DiffRowsCount: map[string]int{"n1/n2": 2}},
		}
	}
	for _, tc := range []struct {
		name      string
		v1, v2    any
		lit1      string
		integerPK bool
	}{
		{"bigint", int64(9007199254740995), int64(9007199254740996), "9007199254740995", true},
		{"text that looks like a number", "007", "008", `"007"`, false},
		{"fraction in another row", int64(1), 1.5, "1", false},
		{"json number", json.Number("12345678901234567890"), json.Number("1"), "12345678901234567890", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, data := renderHTMLTestReport(t, one(tc.v1, tc.v2), 0)
			if data.Rows[0].PK[0] != tc.lit1 {
				t.Errorf("pk literal: got %s, want %s", data.Rows[0].PK[0], tc.lit1)
			}
			if data.HTMLReport.IntegerPK != tc.integerPK {
				t.Errorf("integer_pk: got %v, want %v", data.HTMLReport.IntegerPK, tc.integerPK)
			}
		})
	}
}

// The report shows a prefix of the sorted rows, so the sort must not depend
// on map order. Keys that mix numbers and text used to break that.
func TestHTMLReportIsDeterministic(t *testing.T) {
	var a, b []types.OrderedMap
	for i := 1; i <= 60; i++ {
		for _, id := range []string{fmt.Sprint(i), fmt.Sprintf("%da", i)} {
			a = append(a, types.OrderedMap{{Key: "id", Value: id}, {Key: "val", Value: "a"}})
			b = append(b, types.OrderedMap{{Key: "id", Value: id}, {Key: "val", Value: "b"}})
		}
	}
	a = append(a, types.OrderedMap{{Key: "id", Value: "NaN"}, {Key: "val", Value: "a"}})
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": a, "n2": b}}},
		Summary:   types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"}, DiffRowsCount: map[string]int{"n1/n2": 121}},
	}

	var first string
	for run := 0; run < 20; run++ {
		var buf bytes.Buffer
		if err := renderHTMLDiffReport(&buf, diff, "d.json", 20); err != nil {
			t.Fatal(err)
		}
		if run == 0 {
			first = buf.String()
			continue
		}
		if buf.String() != first {
			t.Fatalf("run %d rendered a different report", run)
		}
	}
	// Numbers come first, in numeric order.
	keys := pageKeys(t, first)[0]["value_diff"]
	if keys[0] != "1" || keys[1] != "2" || keys[19] != "20" {
		t.Errorf("first shown keys: %v", keys)
	}
}

func TestComparePKComponentIsATotalOrder(t *testing.T) {
	vals := []string{"1", "1.0", "2", "9", "10", "1a", "10a", "9a", "", "NaN", "Inf", "-1", "abc", "a"}
	sign := func(x int) int {
		switch {
		case x < 0:
			return -1
		case x > 0:
			return 1
		}
		return 0
	}
	for _, a := range vals {
		for _, b := range vals {
			if sign(comparePKComponent(a, b)) != -sign(comparePKComponent(b, a)) {
				t.Errorf("not antisymmetric: %q, %q", a, b)
			}
			for _, c := range vals {
				if comparePKComponent(a, b) < 0 && comparePKComponent(b, c) < 0 && comparePKComponent(a, c) >= 0 {
					t.Errorf("not transitive: %q < %q < %q but not %q < %q", a, b, c, a, c)
				}
			}
		}
	}
}

// table-diff can count a row that the report leaves out because its values
// look the same (1 against "1"). The section counts must still add up, and
// the page must say why the numbers differ.
func TestHTMLReportRowsWithoutVisibleDifference(t *testing.T) {
	d := htmlTestDiff(2, 0, 0, "x")
	pair := d.NodeDiffs["n1/n2"]
	pair.Rows["n1"] = append(pair.Rows["n1"], types.OrderedMap{{Key: "id", Value: 3}, {Key: "val", Value: 1}, {Key: "note", Value: "x"}})
	pair.Rows["n2"] = append(pair.Rows["n2"], types.OrderedMap{{Key: "id", Value: 3}, {Key: "val", Value: "1"}, {Key: "note", Value: "x"}})
	d.Summary.DiffRowsCount["n1/n2"] = 3

	page, data := renderHTMLTestReport(t, d, 2)
	if strings.Contains(page, `class="truncation-banner"`) {
		t.Error("report shows a truncation banner, but every row with a difference is shown")
	}
	if !strings.Contains(page, ">2 rows<") {
		t.Error("section pill does not count the rendered rows")
	}
	if !strings.Contains(page, "1 of the 3 rows that table-diff found for this pair have no visible difference") {
		t.Error("section does not explain the row left out")
	}
	if data.HTMLReport.Truncated {
		t.Error("html_report.truncated is true")
	}
}

type failingWriter struct{ left int }

func (w *failingWriter) Write(p []byte) (int, error) {
	if len(p) > w.left {
		n := w.left
		w.left = 0
		return n, errors.New("disk full")
	}
	w.left -= len(p)
	return len(p), nil
}

func TestRenderHTMLDiffReportWriteError(t *testing.T) {
	// A small buffer makes every block reach the writer at once, so the
	// failure lands where the test puts it.
	orig := htmlWriteBufferSize
	htmlWriteBufferSize = 16
	t.Cleanup(func() { htmlWriteBufferSize = orig })

	diff := htmlTestDiff(4, 2, 2, "x")
	var full bytes.Buffer
	if err := renderHTMLDiffReport(&full, diff, "d.json", 0); err != nil {
		t.Fatal(err)
	}
	page := full.String()
	rowsAt := strings.Index(page, `class="diff-row"`)
	dataAt := strings.Index(page, `<script id="diff-data"`)
	tailAt := strings.LastIndex(page, "<script>")
	if rowsAt < 0 || dataAt < 0 || tailAt < 0 {
		t.Fatal("could not find the parts of the page")
	}
	for name, at := range map[string]int{"head": 10, "rows": rowsAt + 10, "embedded data": dataAt + 60, "tail": tailAt + 10} {
		err := renderHTMLDiffReport(&failingWriter{left: at}, diff, "d.json", 0)
		if err == nil || !strings.Contains(err.Error(), "disk full") {
			t.Errorf("write error in the %s (after %d bytes): got %v", name, at, err)
		}
	}
}

// writeHTMLDiffReport must not leave a file behind when it fails.
func TestWriteHTMLDiffReportRemovesFileOnError(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "r.json")
	htmlPath := filepath.Join(dir, "r.html")
	orig := htmlDiffTemplate
	htmlDiffTemplate = `{{define "report_head"}}{{.NoSuchField}}{{end}}`
	t.Cleanup(func() { htmlDiffTemplate = orig })

	got, err := writeHTMLDiffReport(htmlTestDiff(1, 0, 0, "x"), jsonPath, 0)
	if err == nil {
		t.Fatal("expected an error from a broken template")
	}
	if got != "" {
		t.Errorf("returned path %q on error", got)
	}
	if _, statErr := os.Stat(htmlPath); !os.IsNotExist(statErr) {
		t.Errorf("HTML file left behind after error (stat: %v)", statErr)
	}
}
