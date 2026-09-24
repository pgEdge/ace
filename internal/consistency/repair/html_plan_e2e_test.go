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
// a browser would run.
const planScript = `
const fs = require('fs');
const [htmlPath, outPath] = process.argv.slice(2);
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
fs.writeFileSync(outPath, build(JSON.parse(data), { usingSelection: false }));
`

func e2eRow(id any, v string) types.OrderedMap {
	return types.OrderedMap{{Key: "id", Value: id}, {Key: "v", Value: v}}
}

// runHTMLPlan writes the reports for diff, builds the plan on the page, and
// resolves it. It returns the upserted primary keys per
// node, the plan, and the resolver error.
func runHTMLPlan(t *testing.T, diff types.DiffOutput) (map[string][]string, string, error) {
	t.Helper()
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	dir := t.TempDir()
	t.Chdir(dir) // WriteDiffReport writes into the current directory

	jsonPath, htmlPath, err := utils.WriteDiffReport(diff, "public", "t", "html")
	if err != nil {
		t.Fatalf("WriteDiffReport: %v", err)
	}
	scriptPath := filepath.Join(dir, "plan.js")
	planPath := filepath.Join(dir, "plan.yaml")
	if err := os.WriteFile(scriptPath, []byte(planScript), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(node, scriptPath, htmlPath, planPath).CombinedOutput(); err != nil {
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

// Every row is on the page, so the plan covers every row: the default
// keep_n1 for value differences, and explicit inserts for missing rows.
func TestHTMLPlanCompleteReport(t *testing.T) {
	got, plan, err := runHTMLPlan(t, twoNodeDiff())
	if err != nil {
		t.Fatalf("plan does not resolve: %v\n%s", err, plan)
	}
	if len(got["n2"]) != 45 || len(got["n1"]) != 8 {
		t.Errorf("upserts: got n1=%d n2=%d, want n1=8 n2=45", len(got["n1"]), len(got["n2"]))
	}
	if !strings.Contains(plan, "type: keep_n1") {
		t.Errorf("plan has no keep_n1 default:\n%s", plan)
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
	got, plan, err := runHTMLPlan(t, diff)
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
	got, plan, err := runHTMLPlan(t, diff)
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
