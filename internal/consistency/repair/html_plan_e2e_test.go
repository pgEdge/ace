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
// a browser would run. An optional third argument is a JSON htmlPageState.
const planScript = `
const fs = require('fs');
const [htmlPath, outPath, stateArg] = process.argv.slice(2);
const state = stateArg ? JSON.parse(stateArg) : {};
const page = fs.readFileSync(htmlPath, 'utf8');
const data = page.match(/<script id="diff-data" type="application\/json">([\s\S]*?)<\/script>/)[1];
const scripts = [...page.matchAll(/<script>([\s\S]*?)<\/script>/g)];
let src = scripts[scripts.length - 1][1].trim();
src = src.replace(/^\(function \(\) \{/, '').replace(/\}\)\(\);?$/, '');
// CSS is a browser global that Node.js lacks. The stand-in marks what it
// escapes, and the stub document has a control only for a selector built
// with it, so a key that skips CSS.escape finds no control.
const css = { escape: s => '\u0000' + s + '\u0000' };
const controls = new Map(Object.entries(state.actions || {}).map(([key, value]) =>
    ['.plan-action[data-pk="' + css.escape(key) + '"]', { value, dataset: {} }]));
const doc = {
    getElementById: () => ({ textContent: data }),
    querySelectorAll: () => [],
    querySelector: sel => controls.get(sel) || null,
};
const build = new Function('document', 'CSS', src + '; return buildPlanYaml;')(doc, css);
let selection = { usingSelection: false };
if (state.keys) {
    selection = { selectedKeys: new Set(state.keys), selectedCount: state.keys.length, totalRows: state.total, usingSelection: true };
}
fs.writeFileSync(outPath, build(JSON.parse(data), selection));
`

func e2eRow(id any, v string) types.OrderedMap {
	return types.OrderedMap{{Key: "id", Value: id}, {Key: "v", Value: v}}
}

// htmlPageState is what the user has done on the page: Keys are the selected
// rows (as data-pk holds them) out of Total shown rows, and Actions maps a
// row's key to the value chosen in its action control, such as "delete".
type htmlPageState struct {
	Keys    []string          `json:"keys,omitempty"`
	Total   int               `json:"total,omitempty"`
	Actions map[string]string `json:"actions,omitempty"`
}

// runHTMLPlan writes the reports for diff with the given limit, builds the
// plan on the page, and resolves it. It returns the upserted primary keys per
// node, the plan, and the resolver error.
func runHTMLPlan(t *testing.T, diff types.DiffOutput, limit int64) (map[string][]string, string, error) {
	t.Helper()
	return runHTMLPlanOnPage(t, diff, limit, nil)
}

// runHTMLPlanOnPage is runHTMLPlan with the given page state.
func runHTMLPlanOnPage(t *testing.T, diff types.DiffOutput, limit int64, page *htmlPageState) (map[string][]string, string, error) {
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
	if page != nil {
		state, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		args = append(args, string(state))
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
	got, plan, err := runHTMLPlanOnPage(t, twoNodeDiff(), 0, &htmlPageState{Keys: []string{"1", "31"}, Total: 53})
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

// The page finds a row's action control with its key in a CSS selector. A
// key with a quote made building the plan fail, and a key with a backslash
// matched no control, so the plan used the default action instead.
func TestHTMLPlanChosenActionForKeysWithQuotes(t *testing.T) {
	var a, b []types.OrderedMap
	for _, id := range []string{`a"b`, `c\d`, "e"} {
		a = append(a, e2eRow(id, "a"))
		b = append(b, e2eRow(id, "b"))
	}
	diff := types.DiffOutput{
		NodeDiffs: map[string]types.DiffByNodePair{"n1/n2": {Rows: map[string][]types.OrderedMap{"n1": a, "n2": b}}},
		Summary: types.DiffSummary{Schema: "public", Table: "t", PrimaryKey: []string{"id"},
			DiffRowsCount: map[string]int{"n1/n2": 3}},
	}
	page := &htmlPageState{Actions: map[string]string{`a"b`: "delete", `c\d`: "keep_n2"}}
	got, plan, err := runHTMLPlanOnPage(t, diff, 0, page)
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	// a"b is deleted, so it is not upserted; c\d takes n2's row; e keeps
	// the default keep_n1.
	want := map[string][]string{"n1": {`c\d`}, "n2": {"e"}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("upserts: got %q, want %q\n%s", got, want, plan)
	}
}
