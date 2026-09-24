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
func renderHTMLTestReport(t *testing.T, diff types.DiffOutput) (string, htmlTestData) {
	t.Helper()
	jsonPath := filepath.Join(t.TempDir(), "public_t_diffs-20260101000000.json")
	htmlPath, err := writeHTMLDiffReport(diff, jsonPath)
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

func TestHTMLReportRowsAndData(t *testing.T) {
	page, data := renderHTMLTestReport(t, htmlTestDiff(4, 2, 1, "x"))

	if !strings.Contains(page, ">7 entries<") {
		t.Error("section pill does not show the entry count")
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

	want := map[string]int{"row_mismatch": 4, "missing_on_n2": 2, "missing_on_n1": 1}
	if got := data.typeCounts("n1/n2"); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("embedded rows: got %v, want %v", got, want)
	}
	if data.Summary.Table != "t" || len(data.Summary.PrimaryKey) != 1 {
		t.Errorf("embedded summary is wrong: %+v", data.Summary)
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
	page, data := renderHTMLTestReport(t, diff)

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
	page, data := renderHTMLTestReport(t, diff)

	if !strings.Contains(page, "No row-level differences were recorded.") {
		t.Error("empty report has no empty message")
	}
	if len(data.Rows) != 0 {
		t.Errorf("embedded rows: got %d, want 0", len(data.Rows))
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
	page, data := renderHTMLTestReport(t, threeNodeDiff())
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
	}
	if len(data.Rows) != 22+11 {
		t.Errorf("embedded rows: got %d, want 33", len(data.Rows))
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
			_, data := renderHTMLTestReport(t, one(tc.v1, tc.v2))
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
		if err := renderHTMLDiffReport(&buf, diff); err != nil {
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
	if err := renderHTMLDiffReport(&full, diff); err != nil {
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
		err := renderHTMLDiffReport(&failingWriter{left: at}, diff)
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

	got, err := writeHTMLDiffReport(htmlTestDiff(1, 0, 0, "x"), jsonPath)
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
