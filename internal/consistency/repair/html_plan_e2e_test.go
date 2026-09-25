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

package repair

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	planner "github.com/pgedge/ace/internal/consistency/repair/plan"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/types"
)

// These tests check the whole path of a repair plan built in the HTML
// report: table-diff writes the JSON and HTML reports, the page script
// (run in Node.js, with no user choices) builds the YAML plan, and the
// repair executor resolves that plan against the full JSON diff. They skip
// when node is not installed.

// planScript runs buildPlanYaml from the page on the page's embedded data.
// It takes the script and the data out of the page itself, so it tests what
// a browser would run. An optional third argument is a JSON selection:
// {"keys": [row ids as rowId builds them], "total": rows on the page}.
const planScript = `
const fs = require('fs');
const [htmlPath, outPath, selArg] = process.argv.slice(2);
const page = fs.readFileSync(htmlPath, 'utf8');
const data = page.match(/<script id="diff-data" type="application\/json">([\s\S]*?)<\/script>/)[1];
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)];
let src = scripts[scripts.length - 1][1].trim();
src = src.replace(/^\(function \(\) \{/, '').replace(/\}\)\(\);?$/, '');
const doc = {
    getElementById: () => ({ textContent: data }),
    querySelectorAll: () => [],
    querySelector: () => null,
};
const build = new Function('document', src + '; return buildPlanYaml;')(doc);
let selection = { usingSelection: false };
if (selArg) {
    const sel = JSON.parse(selArg);
    selection = { selectedKeys: new Set(sel.keys), selectedCount: sel.keys.length, totalRows: sel.total, usingSelection: true };
}
fs.writeFileSync(outPath, build(JSON.parse(data), selection));
`

func e2eRow(id any, v string) types.OrderedMap {
	return types.OrderedMap{{Key: "id", Value: id}, {Key: "v", Value: v}}
}

// runHTMLPlan writes the reports for diff with the given limit, builds the
// plan on the page, and resolves it. It returns the upserted primary keys per
// node, the plan, and the resolver error.
func runHTMLPlan(t *testing.T, diff types.DiffOutput, limit int64) (map[string][]string, string, error) {
	t.Helper()
	return runHTMLPlanSelected(t, diff, limit, nil, 0)
}

// runHTMLPlanSelected is runHTMLPlan with the given rows selected on the
// page, out of total shown rows. Row ids have the form "n1/n2|key".
func runHTMLPlanSelected(t *testing.T, diff types.DiffOutput, limit int64, selected []string, total int) (map[string][]string, string, error) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	dir := t.TempDir()
	t.Chdir(dir) // WriteDiffReport writes into the current directory

	jsonPath, htmlPath, err := utils.WriteDiffReport(diff, "public", "t", "html", limit)
	if err != nil {
		t.Fatalf("WriteDiffReport: %v", err)
	}
	scriptPath := filepath.Join(dir, "plan.js")
	planPath := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(scriptPath, []byte(planScript), 0o600); err != nil {
		t.Fatal(err)
	}
	args := []string{scriptPath, htmlPath, planPath}
	if selected != nil {
		sel, err := json.Marshal(map[string]any{"keys": selected, "total": total})
		if err != nil {
			t.Fatal(err)
		}
		args = append(args, string(sel))
	}
	if out, err := exec.Command(node, args...).CombinedOutput(); err != nil {
		t.Fatalf("page script failed: %v\n%s", err, out)
	}
	planText, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}

	plan, err := planner.LoadRepairPlanFile(planPath)
	if err != nil {
		t.Fatalf("plan does not load: %v\n%s", err, planText)
	}
	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	task := &TableRepairTask{}
	task.Schema, task.Table = "public", "t"
	task.Key = []string{"id"}
	task.SimplePrimaryKey = true
	task.RepairPlan = plan
	if err := json.Unmarshal(raw, &task.RawDiffs); err != nil {
		t.Fatal(err)
	}

	upserts, _, err := CalculatePlanRepairSets(task)
	got := map[string][]string{}
	for nodeName, rows := range upserts {
		for _, row := range rows {
			got[nodeName] = append(got[nodeName], fmt.Sprint(row["id"]))
		}
		sort.Strings(got[nodeName])
	}
	return got, string(planText), err
}

func idRange(from, to int) []string {
	var out []string
	for i := from; i <= to; i++ {
		out = append(out, fmt.Sprint(i))
	}
	sort.Strings(out)
	return out
}

// twoNodeDiff: 30 value differences (1..30), 15 rows missing on n2
// (31..45), 8 rows missing on n1 (46..53).
func twoNodeDiff() types.DiffOutput {
	var a, b []types.OrderedMap
	for id := 1; id <= 30; id++ {
		a = append(a, e2eRow(id, "a"))
		b = append(b, e2eRow(id, "b"))
	}
	for id := 31; id <= 45; id++ {
		a = append(a, e2eRow(id, "a"))
	}
	for id := 46; id <= 53; id++ {
		b = append(b, e2eRow(id, "b"))
	}
	return types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": a, "n2": b}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", Nodes: []string{"n1", "n2"},
			PrimaryKey: []string{"id"}, DiffRowsCount: map[string]int{"n1/n2": 53}},
	}
}

// Before the fix, the hidden rows missing on n1 got keep_n1 and the whole
// plan was rejected.
func TestHTMLPlanTruncatedTwoNodes(t *testing.T) {
	got, plan, err := runHTMLPlan(t, twoNodeDiff(), 25)
	if err != nil {
		t.Fatalf("plan from a truncated report does not resolve: %v\n%s", err, plan)
	}
	if want := idRange(1, 25); fmt.Sprint(got["n2"]) != fmt.Sprint(want) || len(got["n1"]) != 0 {
		t.Errorf("upserts: got %v, want n2=%v and nothing on n1\n%s", got, want, plan)
	}
	if !strings.Contains(plan, "type: skip") || !strings.Contains(plan, "# WARNING") {
		t.Errorf("plan has no skip default or no warning:\n%s", plan)
	}
}

func TestHTMLPlanCompleteReport(t *testing.T) {
	got, plan, err := runHTMLPlan(t, twoNodeDiff(), 0)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	if len(got["n2"]) != 45 || len(got["n1"]) != 8 {
		t.Errorf("upserts: got n1=%d n2=%d, want n1=8 n2=45", len(got["n1"]), len(got["n2"]))
	}
	if strings.Contains(plan, "# WARNING") || !strings.Contains(plan, "type: keep_n1") {
		t.Errorf("complete report must give the old plan:\n%s", plan)
	}
}

// JSON.parse would round these keys to the same float, so a plan built from
// parsed numbers names the wrong rows. The keys exist on one node only, so
// the plan must have explicit rules for them.
func TestHTMLPlanBigintKeys(t *testing.T) {
	a := []types.OrderedMap{e2eRow(int64(9007199254740995), "a"), e2eRow(int64(9007199254740996), "a")}
	b := []types.OrderedMap{e2eRow(int64(9007199254740997), "b")}
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": a, "n2": b}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": 3}},
	}
	got, plan, err := runHTMLPlan(t, diff, 0)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	want := map[string][]string{
		"n2": {"9007199254740995", "9007199254740996"},
		"n1": {"9007199254740997"},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %v, want %v\n%s", got, want, plan)
	}
}

// Text keys that look like numbers must stay text in the plan. If they turn
// into numbers, no rule matches these rows missing on n1, the plan default
// keep_n1 applies to them, and table-repair rejects the plan.
func TestHTMLPlanTextKeysThatLookNumeric(t *testing.T) {
	var b []types.OrderedMap
	for _, id := range []string{"001", "002", "003"} {
		b = append(b, e2eRow(id, "b"))
	}
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": nil, "n2": b}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": 3}},
	}
	got, plan, err := runHTMLPlan(t, diff, 0)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	want := map[string][]string{"n1": {"001", "002", "003"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %v, want %v\n%s", got, want, plan)
	}
	if regexp.MustCompile(`range:`).MatchString(plan) {
		t.Errorf("plan uses a range for text keys:\n%s", plan)
	}
}

// threeNodeDiff builds a diff with the given rows per pair and node.
func threeNodeDiff(pairs map[string]map[string][]types.OrderedMap) types.DiffOutput {
	d := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{},
		Summary: types.DiffSummary{Schema: "public", Table: "t", Nodes: []string{"n1", "n2", "n3"},
			PrimaryKey: []string{"id"}, DiffRowsCount: map[string]int{}},
	}
	for pair, rows := range pairs {
		d.NodeDiffs[pair] = types.DiffByNodePair{Rows: rows}
		count := 0
		for _, r := range rows {
			count += len(r)
		}
		d.Summary.DiffRowsCount[pair] = count
	}
	return d
}

// Key 3 differs in n1/n2, where the limit hides it, and in n1/n3, where it is
// shown. A plan rule cannot name a pair, so a rule for the shown row would
// also repair the hidden one; a range over the shown keys 1, 2 (n1/n2) and 3
// (n1/n3) did exactly that. The shown row must be left out instead.
func TestHTMLPlanThreeNodesHiddenTwin(t *testing.T) {
	diff := threeNodeDiff(map[string]map[string][]types.OrderedMap{
		"n1/n2": {
			"n1": {e2eRow(1, "a"), e2eRow(2, "a"), e2eRow(3, "a")},
			"n2": {e2eRow(1, "b"), e2eRow(2, "b"), e2eRow(3, "b")},
		},
		"n1/n3": {
			"n1": {e2eRow(3, "a")},
			"n3": {e2eRow(3, "c")},
		},
	})
	got, plan, err := runHTMLPlan(t, diff, 2)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	want := map[string][]string{"n2": {"1", "2"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %v, want %v\n%s", got, want, plan)
	}
	if !strings.Contains(plan, "1 shown row has no rule") || !strings.Contains(plan, `#   "3" (row_mismatch in n1/n3)`) {
		t.Errorf("plan does not list the row it leaves out:\n%s", plan)
	}
}

// Key 5 is missing on n1, and n2 and n3 disagree on it. So it is a row in all
// three pairs: missing_on_n1 in n1/n2 and n1/n3, row_mismatch in n2/n3. Key 6
// is missing on n3 only. The page used to write one plan entry per key, taken
// from the first pair, as a row_override; that override also matched the
// n2/n3 row, and table-repair rejected the plan. It also wrote node names in
// apply_from, where table-repair wants n1 or n2 (the pair's first or second
// node), so "from: n3" was rejected too.
func TestHTMLPlanThreeNodesSameKeyInSeveralPairs(t *testing.T) {
	diff := threeNodeDiff(map[string]map[string][]types.OrderedMap{
		"n1/n2": {
			"n2": {e2eRow(5, "b")},
		},
		"n1/n3": {
			"n1": {e2eRow(6, "a")},
			"n3": {e2eRow(5, "c")},
		},
		"n2/n3": {
			"n2": {e2eRow(5, "b"), e2eRow(6, "a")},
			"n3": {e2eRow(5, "c")},
		},
	})
	got, plan, err := runHTMLPlan(t, diff, 0)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	// n1 gets key 5 in both n1/n2 and n1/n3. That is two different rows, n2's
	// and n3's, and table-repair keeps whichever pair it handles last, in Go
	// map order. This is a known table-repair problem, not something the plan
	// can fix, so the test checks only which keys each node gets. n3 gets key
	// 5 from n2 (keep_n1 in n2/n3) and key 6.
	want := map[string][]string{"n1": {"5"}, "n3": {"5", "6"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %v, want %v\n%s", got, want, plan)
	}
	if strings.Contains(plan, "row_overrides") || strings.Contains(plan, "# WARNING") {
		t.Errorf("plan for a complete multi-pair report must use rules only and leave nothing out:\n%s", plan)
	}
}

// Node names other than n1 and n2 must not reach apply_from.
func TestHTMLPlanNodeNamesAreNotPlanNames(t *testing.T) {
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"east/west": {Rows: map[string][]types.OrderedMap{
			"east": {e2eRow(1, "a")},
			"west": {e2eRow(2, "b")},
		}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"east/west": 2}},
	}
	got, plan, err := runHTMLPlan(t, diff, 0)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	want := map[string][]string{"east": {"2"}, "west": {"1"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %v, want %v\n%s", got, want, plan)
	}
}

// With some rows selected, the rows not selected must be left alone. The
// plan used to keep default_action keep_n1, so they were repaired anyway,
// and the unselected rows missing on n1 made table-repair reject the plan.
func TestHTMLPlanPartialSelection(t *testing.T) {
	got, plan, err := runHTMLPlanSelected(t, twoNodeDiff(), 0, []string{"n1/n2|1", "n1/n2|31"}, 53)
	if err != nil {
		t.Fatalf("plan from a partial selection does not resolve: %v\n%s", err, plan)
	}
	want := map[string][]string{"n2": {"1", "31"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %v, want %v\n%s", got, want, plan)
	}
	if !strings.Contains(plan, "type: skip") || !strings.Contains(plan, "rules only for the 2 rows selected") {
		t.Errorf("plan has no skip default or no comment:\n%s", plan)
	}
}

// YAML reads U+0085, U+2028 and U+2029 as line breaks, and neither
// JSON.stringify nor Go's json.Marshal escapes all of them. In a quoted key
// the break turned into a space, so the rule named another key, the rows
// fell through to keep_n1, and table-repair rejected the plan.
func TestHTMLPlanKeysWithYAMLLineBreaks(t *testing.T) {
	keys := []string{"a\u0085b", "c d", "e f", "g\nh"}
	var b []types.OrderedMap
	for _, id := range keys {
		b = append(b, e2eRow(id, "b"))
	}
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": nil, "n2": b}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": len(keys)}},
	}
	got, plan, err := runHTMLPlan(t, diff, 0)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	want := append([]string(nil), keys...)
	sort.Strings(want)
	if fmt.Sprint(got["n1"]) != fmt.Sprint(want) || len(got) != 1 {
		t.Errorf("upserts: got %q, want n1=%q\n%s", got, want, plan)
	}
}

// A left-out row's key goes into a YAML comment. A line break in it must not
// end the comment, or the rest of the key becomes part of the plan.
func TestHTMLPlanLeftOutKeyWithLineBreak(t *testing.T) {
	x := "x default_action: {type: keep_n1}"
	y := "y\ndefault_action: {type: keep_n1}"
	diff := threeNodeDiff(map[string]map[string][]types.OrderedMap{
		"n1/n2": {
			"n1": {e2eRow("a", "1"), e2eRow("b", "1"), e2eRow(x, "1"), e2eRow(y, "1")},
			"n2": {e2eRow("a", "2"), e2eRow("b", "2"), e2eRow(x, "2"), e2eRow(y, "2")},
		},
		"n1/n3": {
			"n1": {e2eRow(x, "1"), e2eRow(y, "1")},
			"n3": {e2eRow(x, "3"), e2eRow(y, "3")},
		},
	})
	got, plan, err := runHTMLPlan(t, diff, 2)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	want := map[string][]string{"n2": {"a", "b"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %q, want %q\n%s", got, want, plan)
	}
	if !strings.Contains(plan, "2 shown rows have no rule") {
		t.Errorf("plan does not list the rows it leaves out:\n%s", plan)
	}
}

// Keys of the kinds NormalizeScannedValue puts in a diff file: timestamps
// (time.Time, written as RFC 3339 text), numerics (text), bytea (base64 text)
// and UUIDs (text). The plan must copy the diff file's text exactly, as
// quoted strings, so table-repair compares string with string. An unquoted
// timestamp or number in the YAML would be read as another type and match
// nothing.
func TestHTMLPlanNormalizedKeyTypes(t *testing.T) {
	india := time.FixedZone("IST", 5*3600+30*60)
	cases := []struct {
		name string
		ids  []any
	}{
		{"timestamptz", []any{
			time.Date(2026, 1, 2, 3, 4, 5, 123456000, time.UTC),
			time.Date(2026, 1, 2, 3, 4, 5, 0, india),
		}},
		{"date", []any{time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)}},
		{"numeric", []any{"12.50", "1e-7", "123456789012345678901234567890"}},
		{"bytea", []any{[]byte{0, 1, 2, 255}}},
		{"uuid", []any{"0b9e3f1e-5c1a-4d0e-9d7f-3a2b1c0d9e8f"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var b []types.OrderedMap
			for _, id := range tc.ids {
				b = append(b, e2eRow(id, "b"))
			}
			diff := types.DiffOutput{
				NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": nil, "n2": b}}},
				Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
					DiffRowsCount: map[string]int{"n1/n2": len(tc.ids)}},
			}
			// The rows are missing on n1, so if a rule matches nothing,
			// keep_n1 applies and table-repair rejects the plan.
			got, plan, err := runHTMLPlan(t, diff, 0)
			if err != nil {
				t.Fatalf("plan does not resolve: %v\n%s", err, plan)
			}
			if len(got["n1"]) != len(tc.ids) || len(got) != 1 {
				t.Errorf("upserts: got %q, want %d rows on n1\n%s", got, len(tc.ids), plan)
			}
		})
	}
}
