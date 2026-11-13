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
	"fmt"
	"reflect"
	"strings"

	"github.com/pgedge/ace/internal/consistency/repair/plan"
	"github.com/pgedge/ace/internal/consistency/repair/plan/parser"
	utils "github.com/pgedge/ace/pkg/common"
	"github.com/pgedge/ace/pkg/types"
)

type planDiffRow struct {
	pkStr          string
	pkMap          map[string]any
	node1Name      string
	node2Name      string
	n1Row          map[string]any
	n2Row          map[string]any
	n1Meta         map[string]any
	n2Meta         map[string]any
	diffType       string   // missing_on_n1, missing_on_n2, row_mismatch
	columnsChanged []string // only set for mismatches
}

type staleActionSummary struct {
	Type planner.RepairActionType `json:"type"`
	From string                  `json:"from,omitempty"`
	Mode planner.RepairApplyMode  `json:"mode,omitempty"`
}

type planStaleCheck struct {
	DiffCommitRaw any
	RuleName      string
	Action        staleActionSummary
	DiffType      string
}

// calculatePlanRepairSets returns the row operations that the repair plan
// gives for the rows of the diff report.
func calculatePlanRepairSets(task *TableRepairTask) (*repairSets, error) {
	if task.RepairPlan == nil {
		return nil, fmt.Errorf("repair plan is nil")
	}

	tableKey := fmt.Sprintf("%s.%s", task.Schema, task.Table)
	tablePlan, ok := task.RepairPlan.Tables[tableKey]
	if !ok {
		return nil, fmt.Errorf("repair plan does not contain table %s", tableKey)
	}

	sets := newRepairSets()
	task.planRuleMatches = make(map[string]map[string]string)
	task.planStaleChecks = make(map[string]map[string]planStaleCheck)

	for _, p := range task.diff.pairs {
		sets.pair = p.name
		diffRows, err := buildPlanDiffRows(task, p)
		if err != nil {
			return nil, err
		}
		cmp := task.newComparer(p)

		// The rules compare values on the server: all comparisons that the
		// rules can need for the rows of the pair are computed first.
		for _, d := range diffRows {
			if err := addRuleComparisons(task, tablePlan, d, cmp); err != nil {
				return nil, err
			}
		}
		if err := cmp.flush(); err != nil {
			return nil, err
		}

		type resolved struct {
			action   *planner.RepairPlanAction
			ruleName string
		}
		actions := make([]resolved, len(diffRows))
		for i, d := range diffRows {
			action, ruleName, err := resolvePlanAction(task, tablePlan, d, cmp)
			if err != nil {
				return nil, err
			}
			if action == nil {
				return nil, fmt.Errorf("no action resolved for row %s", d.pkStr)
			}
			if err := ensureActionCompatible(d, action); err != nil {
				return nil, err
			}
			if err := addFreshestComparisons(action, d, cmp); err != nil {
				return nil, err
			}
			actions[i] = resolved{action, ruleName}
		}
		if err := cmp.flush(); err != nil {
			return nil, err
		}

		for i, d := range diffRows {
			if err := applyPlanAction(d, actions[i].action, actions[i].ruleName, sets, task.planRuleMatches, task.planStaleChecks, cmp); err != nil {
				return nil, err
			}
		}
	}

	return sets, nil
}

// buildPlanDiffRows returns a plan row for each entry of the pair. The kind
// of the entry gives the diff type.
func buildPlanDiffRows(task *TableRepairTask, p *diffPair) ([]planDiffRow, error) {
	diffRows := make([]planDiffRow, 0, len(p.entries))
	for _, e := range p.entries {
		d := planDiffRow{pkStr: e.key, node1Name: p.node1, node2Name: p.node2}
		for _, side := range []struct {
			node string
			raw  types.OrderedMap
			row  *map[string]any
			meta *map[string]any
		}{{p.node1, e.row1, &d.n1Row, &d.n1Meta}, {p.node2, e.row2, &d.n2Row, &d.n2Meta}} {
			if side.raw == nil {
				continue
			}
			raw := utils.OrderedMapToMap(side.raw)
			*side.meta = extractSpockMeta(raw)
			*side.row = utils.StripSpockMetadata(raw)
			if d.pkMap == nil {
				pkMap, err := extractPkMap(task.Key, *side.row)
				if err != nil {
					return nil, fmt.Errorf("node %s row %s: %w", side.node, e.key, err)
				}
				d.pkMap = pkMap
			}
		}
		switch {
		case d.n1Row == nil:
			d.diffType = "missing_on_n1"
		case d.n2Row == nil:
			d.diffType = "missing_on_n2"
		default:
			d.diffType = "row_mismatch"
		}
		d.columnsChanged = computeChangedColumns(d.n1Row, d.n2Row)
		diffRows = append(diffRows, d)
	}
	return diffRows, nil
}

func extractPkMap(keys []string, row map[string]any) (map[string]any, error) {
	pk := make(map[string]any)
	for _, k := range keys {
		val, ok := row[k]
		if !ok {
			return nil, fmt.Errorf("missing primary key column %s", k)
		}
		pk[k] = val
	}
	return pk, nil
}

func computeChangedColumns(n1, n2 map[string]any) []string {
	if n1 == nil || n2 == nil {
		return nil
	}
	colSet := make(map[string]struct{})
	for k := range n1 {
		colSet[k] = struct{}{}
	}
	for k := range n2 {
		colSet[k] = struct{}{}
	}

	var changed []string
	for col := range colSet {
		if !reflect.DeepEqual(n1[col], n2[col]) {
			changed = append(changed, col)
		}
	}
	return changed
}

// addRuleComparisons queues the comparisons that the rules of tablePlan can
// need for row: the pk_in ranges and the comparisons of the when
// expressions.
func addRuleComparisons(task *TableRepairTask, tablePlan planner.RepairTablePlan, row planDiffRow, cmp *comparer) error {
	for _, rule := range tablePlan.Rules {
		for _, c := range rangeComparisons(rule.PKIn, row.pkMap, task.Key, task.SimplePrimaryKey) {
			if err := cmp.add(c); err != nil {
				return err
			}
		}
		if w := rule.CompiledWhen(); w != nil {
			for _, c := range w.Comparisons(rowValues(row)) {
				if err := cmp.add(c); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// rowValues returns the value provider of a when expression for row.
func rowValues(row planDiffRow) parser.ValueProvider {
	return func(source, column string) parser.Value {
		return lookupValue(source, column, row)
	}
}

func resolvePlanAction(task *TableRepairTask, tablePlan planner.RepairTablePlan, row planDiffRow, cmp *comparer) (*planner.RepairPlanAction, string, error) {
	// Row overrides
	for idx, override := range tablePlan.RowOverrides {
		if pkMatchesOverride(task.Key, row.pkMap, override.PK) {
			name := override.Name
			if strings.TrimSpace(name) == "" {
				name = fmt.Sprintf("row_override_%d", idx)
			}
			if err := validateActionCompatibility(&override.Action, row); err != nil {
				return nil, "", err
			}
			return &override.Action, name, nil
		}
	}

	// Rules in order
	for idx, rule := range tablePlan.Rules {
		matched, err := matchPKIn(rule.PKIn, row.pkMap, task.Key, task.SimplePrimaryKey, cmp)
		if err != nil {
			return nil, "", err
		}
		if !matched {
			continue
		}
		if len(rule.DiffTypes) > 0 && !containsString(rule.DiffTypes, row.diffType) {
			continue
		}
		if len(rule.ColumnsChanged) > 0 && !columnsIntersect(rule.ColumnsChanged, row.columnsChanged) {
			continue
		}
		if rule.CompiledWhen() != nil {
			ok, err := rule.CompiledWhen().Eval(rowValues(row), cmp.compare)
			if err != nil {
				return nil, "", err
			}
			if !ok {
				continue
			}
		}
		if err := validateActionCompatibility(&rule.Action, row); err != nil {
			return nil, "", err
		}
		name := rule.Name
		if strings.TrimSpace(name) == "" {
			name = fmt.Sprintf("rule_%d", idx)
		}
		return &rule.Action, name, nil
	}

	if tablePlan.DefaultAction != nil {
		if err := validateActionCompatibility(tablePlan.DefaultAction, row); err != nil {
			return nil, "", err
		}
		return tablePlan.DefaultAction, "table_default", nil
	}
	if task.RepairPlan.DefaultAction != nil {
		if err := validateActionCompatibility(task.RepairPlan.DefaultAction, row); err != nil {
			return nil, "", err
		}
		return task.RepairPlan.DefaultAction, "global_default", nil
	}

	return nil, "", fmt.Errorf("no default action configured in repair plan")
}

// pkMatchesOverride reports whether the key of a row is the key of an
// override: the text of every key column is the same, byte for byte.
func pkMatchesOverride(pkOrder []string, rowPk map[string]any, overridePk map[string]planner.PlanValue) bool {
	if len(overridePk) != len(pkOrder) {
		return false
	}
	for _, col := range pkOrder {
		ov, ok := overridePk[col]
		if !ok || !ov.Valid {
			return false
		}
		text, ok := valueText(rowPk[col])
		if !ok || text != ov.Text {
			return false
		}
	}
	return true
}

// matchPKIn reports whether the key of a row matches one of the matchers. An
// equals key matches when its text is the key text, byte for byte. A range
// of a one-column key matches when from <= key <= to with the type of the
// key column; the comparer must have computed these comparisons before.
func matchPKIn(matchers []planner.RepairPKMatcher, rowPk map[string]any, pkOrder []string, simple bool, cmp *comparer) (bool, error) {
	if len(matchers) == 0 {
		return true, nil
	}
	for _, m := range matchers {
		for _, eq := range m.Equals {
			if simple == eq.List || len(eq.Values) != len(pkOrder) {
				continue
			}
			all := true
			for i, col := range pkOrder {
				text, ok := valueText(rowPk[col])
				if !ok || !eq.Values[i].Valid || text != eq.Values[i].Text {
					all = false
					break
				}
			}
			if all {
				return true, nil
			}
		}
		if simple && m.Range != nil {
			in := true
			for _, c := range rangeComparisons([]planner.RepairPKMatcher{m}, rowPk, pkOrder, simple) {
				ok, err := cmp.compare(c)
				if err != nil {
					return false, err
				}
				in = in && ok
			}
			if in {
				return true, nil
			}
		}
	}
	return false, nil
}

// rangeComparisons returns the comparisons from <= key and key <= to of the
// ranges of matchers, for a one-column key.
func rangeComparisons(matchers []planner.RepairPKMatcher, rowPk map[string]any, pkOrder []string, simple bool) []parser.Comparison {
	if !simple || len(pkOrder) != 1 {
		return nil
	}
	col := pkOrder[0]
	key, ok := valueText(rowPk[col])
	if !ok {
		return nil
	}
	var out []parser.Comparison
	for _, m := range matchers {
		if m.Range == nil || m.Range.From == nil || m.Range.To == nil {
			continue
		}
		out = append(out,
			parser.Comparison{Op: "<=", Left: m.Range.From.Text, Right: key, RightColumn: col},
			parser.Comparison{Op: "<=", Left: key, LeftColumn: col, Right: m.Range.To.Text})
	}
	return out
}

func columnsIntersect(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	set := make(map[string]struct{})
	for _, col := range b {
		set[col] = struct{}{}
	}
	for _, col := range a {
		if _, ok := set[col]; ok {
			return true
		}
	}
	return false
}

func containsString(list []string, target string) bool {
	for _, v := range list {
		if v == target {
			return true
		}
	}
	return false
}

func allowStaleRepairs(action *planner.RepairPlanAction) bool {
	if action == nil || action.AllowStaleRepairs == nil {
		return true
	}
	return *action.AllowStaleRepairs
}

// recordPlanStaleCheck records for the row operation of row on nodeName the
// commit timestamp that the diff saw, so that the repair can skip the row if
// it changed on the node after the diff. Actions that allow stale repairs
// record nothing.
func recordPlanStaleCheck(staleChecks map[string]map[string]planStaleCheck, nodeName, pkStr string, row planDiffRow, action *planner.RepairPlanAction, ruleName string) {
	if staleChecks == nil || action == nil {
		return
	}
	if allowStaleRepairs(action) {
		return
	}
	var meta map[string]any
	switch nodeName {
	case row.node1Name:
		meta = row.n1Meta
	case row.node2Name:
		meta = row.n2Meta
	}
	var diffCommit any
	if meta != nil {
		if v, ok := meta["commit_ts"]; ok {
			diffCommit = v
		}
	}
	if staleChecks[nodeName] == nil {
		staleChecks[nodeName] = make(map[string]planStaleCheck)
	}
	staleChecks[nodeName][pkStr] = planStaleCheck{
		DiffCommitRaw: diffCommit,
		RuleName:      ruleName,
		Action: staleActionSummary{
			Type: action.Type,
			From: strings.TrimSpace(action.From),
			Mode: action.Mode,
		},
		DiffType: row.diffType,
	}
}

// applyPlanAction adds the row operations of action for one diff row to
// sets. A row goes to a node as an UPDATE if the node had the row at diff
// time, and as an INSERT if it did not.
func applyPlanAction(row planDiffRow, action *planner.RepairPlanAction, ruleName string, sets *repairSets, matches map[string]map[string]string, staleChecks map[string]map[string]planStaleCheck, cmp *comparer) error {
	switch action.Type {
	case planner.RepairActionKeepN1:
		if row.n1Row == nil {
			return fmt.Errorf("keep_n1 specified but row missing on %s (pk %s)", row.node1Name, row.pkStr)
		}
		sets.write(row.node2Name, row.pkStr, copyMap(row.n1Row), row.n2Row != nil)
		recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
		recordMatch(matches, row.node2Name, row.pkStr, ruleName)
	case planner.RepairActionKeepN2:
		if row.n2Row == nil {
			return fmt.Errorf("keep_n2 specified but row missing on %s (pk %s)", row.node2Name, row.pkStr)
		}
		sets.write(row.node1Name, row.pkStr, copyMap(row.n2Row), row.n1Row != nil)
		recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
		recordMatch(matches, row.node1Name, row.pkStr, ruleName)
	case planner.RepairActionApplyFrom:
		from := strings.ToLower(strings.TrimSpace(action.From))
		mode := action.Mode
		if mode == "" {
			mode = planner.RepairApplyModeReplace
		}
		switch from {
		case "n1":
			if row.n1Row == nil {
				return fmt.Errorf("apply_from n1 specified but row missing on %s (pk %s)", row.node1Name, row.pkStr)
			}
			if mode == planner.RepairApplyModeInsert && row.n2Row != nil {
				return nil
			}
			sets.write(row.node2Name, row.pkStr, copyMap(row.n1Row), row.n2Row != nil)
			recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node2Name, row.pkStr, ruleName)
		case "n2":
			if row.n2Row == nil {
				return fmt.Errorf("apply_from n2 specified but row missing on %s (pk %s)", row.node2Name, row.pkStr)
			}
			if mode == planner.RepairApplyModeInsert && row.n1Row != nil {
				return nil
			}
			sets.write(row.node1Name, row.pkStr, copyMap(row.n2Row), row.n1Row != nil)
			recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		default:
			return fmt.Errorf("apply_from requires from to be n1 or n2 (pk %s)", row.pkStr)
		}
	case planner.RepairActionBidirectional:
		// A row that only one node has is inserted on the other node.
		switch {
		case row.n1Row != nil && row.n2Row == nil:
			sets.write(row.node2Name, row.pkStr, copyMap(row.n1Row), false)
			recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node2Name, row.pkStr, ruleName)
		case row.n2Row != nil && row.n1Row == nil:
			sets.write(row.node1Name, row.pkStr, copyMap(row.n2Row), false)
			recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		default:
			return fmt.Errorf("bidirectional needs a row on one node only (pk %s)", row.pkStr)
		}
	case planner.RepairActionCustom:
		customRow, err := buildCustomRow(action, row, cmp)
		if err != nil {
			return err
		}
		sets.write(row.node1Name, row.pkStr, copyMap(customRow), row.n1Row != nil)
		sets.write(row.node2Name, row.pkStr, copyMap(customRow), row.n2Row != nil)
		recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
		recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
		recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		recordMatch(matches, row.node2Name, row.pkStr, ruleName)
	case planner.RepairActionSkip:
		return nil
	case planner.RepairActionDelete:
		if row.n1Row != nil {
			sets.remove(row.node1Name, row.pkStr, row.n1Row)
			recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		}
		if row.n2Row != nil {
			sets.remove(row.node2Name, row.pkStr, row.n2Row)
			recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node2Name, row.pkStr, ruleName)
		}
	default:
		return fmt.Errorf("action %s not supported in execution", action.Type)
	}
	return nil
}

func recordMatch(matches map[string]map[string]string, node, pk, rule string) {
	if matches == nil {
		return
	}
	if matches[node] == nil {
		matches[node] = make(map[string]string)
	}
	matches[node][pk] = rule
}

func copyMap(src map[string]any) map[string]any {
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func extractSpockMeta(row map[string]any) map[string]any {
	meta := make(map[string]any)
	if rawMeta, ok := row["_spock_metadata_"].(map[string]any); ok {
		for k, v := range rawMeta {
			meta[k] = v
		}
	}
	if val, ok := row["commit_ts"]; ok {
		meta["commit_ts"] = val
	}
	if val, ok := row["node_origin"]; ok {
		meta["node_origin"] = val
	}
	return meta
}

// lookupValue returns the value of a column of the row of source ("n1" or
// "n2"): a column of the table, or a column of the spock metadata of the
// row. A column that the row does not have is NULL.
func lookupValue(source, column string, row planDiffRow) parser.Value {
	var data, meta map[string]any
	switch source {
	case "n1":
		data, meta = row.n1Row, row.n1Meta
	case "n2":
		data, meta = row.n2Row, row.n2Meta
	default:
		return parser.Value{Null: true}
	}
	v, ok := data[column]
	if !ok {
		v = meta[column]
	}
	text, ok := valueText(v)
	if !ok {
		return parser.Value{Null: true}
	}
	return parser.Value{Text: text}
}

// lookupTemplate returns the value of a template {{n1.col}} or {{n2.col}},
// and false if inner is not such a template.
func lookupTemplate(inner string, row planDiffRow) (any, bool) {
	parts := strings.Split(inner, ".")
	if len(parts) != 2 {
		return nil, false
	}
	source := strings.ToLower(strings.TrimSpace(parts[0]))
	if source != "n1" && source != "n2" {
		return nil, false
	}
	v := lookupValue(source, strings.TrimSpace(parts[1]), row)
	if v.Null {
		return nil, true
	}
	return v.Text, true
}

func validateActionCompatibility(action *planner.RepairPlanAction, row planDiffRow) error {
	switch action.Type {
	case planner.RepairActionKeepN1:
		if row.n1Row == nil {
			return fmt.Errorf("keep_n1 used on row missing from n1 (pk %s)", row.pkStr)
		}
	case planner.RepairActionKeepN2:
		if row.n2Row == nil {
			return fmt.Errorf("keep_n2 used on row missing from n2 (pk %s)", row.pkStr)
		}
	case planner.RepairActionApplyFrom:
		mode := action.Mode
		if mode == planner.RepairApplyModeInsert && row.diffType == "row_mismatch" {
			return fmt.Errorf("apply_from insert mode cannot be used on mismatched rows (pk %s)", row.pkStr)
		}
		if strings.TrimSpace(action.From) == "n1" && row.n1Row == nil {
			return fmt.Errorf("apply_from n1 used on row missing from n1 (pk %s)", row.pkStr)
		}
		if strings.TrimSpace(action.From) == "n2" && row.n2Row == nil {
			return fmt.Errorf("apply_from n2 used on row missing from n2 (pk %s)", row.pkStr)
		}
	case planner.RepairActionCustom:
		// allow; buildCustomRow will enforce availability
	case planner.RepairActionBidirectional:
		if row.diffType == "row_mismatch" {
			return fmt.Errorf("bidirectional cannot be used on mismatched rows (pk %s)", row.pkStr)
		}
	case planner.RepairActionSkip, planner.RepairActionDelete:
		return nil
	default:
		return fmt.Errorf("unsupported action type %s", action.Type)
	}
	return nil
}

// buildCustomRow returns the row that a custom action writes: the values of
// custom_row, then the columns that coalesce_priority and pick_freshest
// fill, then the key of the row. A value of custom_row is its text, NULL, or
// the value of a template {{n1.col}} or {{n2.col}}.
func buildCustomRow(action *planner.RepairPlanAction, row planDiffRow, cmp *comparer) (map[string]any, error) {
	if action.CustomRow == nil && action.CustomHelpers == nil {
		return nil, fmt.Errorf("custom action missing custom_row or helpers (pk %s)", row.pkStr)
	}

	result := make(map[string]any)
	// Start from explicit custom_row with template substitutions.
	for k, v := range action.CustomRow {
		result[k] = substituteValue(v, row)
	}

	// Coalesce helper fills missing columns from priority order.
	if action.CustomHelpers != nil && len(action.CustomHelpers.CoalescePriority) > 0 {
		cols := unionKeys(row.n1Row, row.n2Row)
		for col := range cols {
			if _, exists := result[col]; exists {
				continue
			}
			for _, src := range action.CustomHelpers.CoalescePriority {
				var data map[string]any
				switch src {
				case "n1":
					data = row.n1Row
				case "n2":
					data = row.n2Row
				}
				if val, ok := data[col]; ok && val != nil {
					result[col] = val
					break
				}
			}
		}
	}

	// pick_freshest helper backfills remaining columns from the chosen side.
	if action.CustomHelpers != nil && action.CustomHelpers.PickFreshest != nil {
		pick := action.CustomHelpers.PickFreshest
		winner, err := freshestSide(pick.Key, pick.Tie, row, cmp)
		if err != nil {
			return nil, err
		}
		var source map[string]any
		switch winner {
		case "n1":
			source = row.n1Row
		case "n2":
			source = row.n2Row
		}
		for col, val := range source {
			if _, exists := result[col]; !exists {
				result[col] = val
			}
		}
	}

	// Ensure PK columns are present using existing PK map.
	for k, v := range row.pkMap {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}

	return result, nil
}

// substituteValue returns the value of one column of custom_row: NULL, the
// value of a template, or the text of the plan.
func substituteValue(v planner.PlanValue, row planDiffRow) any {
	if !v.Valid {
		return nil
	}
	str := strings.TrimSpace(v.Text)
	if strings.HasPrefix(str, "{{") && strings.HasSuffix(str, "}}") {
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(str, "{{"), "}}"))
		if val, ok := lookupTemplate(inner, row); ok {
			return val
		}
	}
	return v.Text
}

func unionKeys(maps ...map[string]any) map[string]struct{} {
	out := make(map[string]struct{})
	for _, m := range maps {
		for k := range m {
			out[k] = struct{}{}
		}
	}
	return out
}

// freshestComparisons returns the comparisons that pick_freshest needs for
// row: whether the key value of n1 is greater, and whether it is less, than
// the one of n2. With a NULL side no comparison is needed.
func freshestComparisons(key string, row planDiffRow) []parser.Comparison {
	k1 := lookupValue("n1", key, row)
	k2 := lookupValue("n2", key, row)
	if k1.Null || k2.Null {
		return nil
	}
	return []parser.Comparison{
		{Op: ">", Left: k1.Text, LeftColumn: key, Right: k2.Text, RightColumn: key},
		{Op: "<", Left: k1.Text, LeftColumn: key, Right: k2.Text, RightColumn: key},
	}
}

// addFreshestComparisons queues the comparisons of pick_freshest, if action
// has it.
func addFreshestComparisons(action *planner.RepairPlanAction, row planDiffRow, cmp *comparer) error {
	if action.Type != planner.RepairActionCustom || action.CustomHelpers == nil || action.CustomHelpers.PickFreshest == nil {
		return nil
	}
	for _, c := range freshestComparisons(action.CustomHelpers.PickFreshest.Key, row) {
		if err := cmp.add(c); err != nil {
			return err
		}
	}
	return nil
}

// freshestSide returns the side whose value of the key column is greater, with
// the type of the column. The key can be a column of the table, such as an
// application timestamp, or commit_ts of the spock metadata. A side with a
// value wins over a side with NULL; with two NULLs or two equal values the
// tie side wins.
func freshestSide(key string, tie string, row planDiffRow, cmp *comparer) (string, error) {
	tie = strings.TrimSpace(strings.ToLower(tie))
	k1 := lookupValue("n1", key, row)
	k2 := lookupValue("n2", key, row)
	switch {
	case k1.Null && k2.Null:
		return tie, nil
	case k1.Null:
		return "n2", nil
	case k2.Null:
		return "n1", nil
	}
	cs := freshestComparisons(key, row)
	gt, err := cmp.compare(cs[0])
	if err != nil {
		return "", err
	}
	if gt {
		return "n1", nil
	}
	lt, err := cmp.compare(cs[1])
	if err != nil {
		return "", err
	}
	if lt {
		return "n2", nil
	}
	return tie, nil
}

func ensureActionCompatible(row planDiffRow, action *planner.RepairPlanAction) error {
	switch row.diffType {
	case "missing_on_n2":
		if action.Type == planner.RepairActionKeepN2 || (action.Type == planner.RepairActionApplyFrom && strings.TrimSpace(strings.ToLower(action.From)) == "n2") {
			return fmt.Errorf("action %s requires n2 row but diff is missing_on_n2 (pk %s)", action.Type, row.pkStr)
		}
	case "missing_on_n1":
		if action.Type == planner.RepairActionKeepN1 || (action.Type == planner.RepairActionApplyFrom && strings.TrimSpace(strings.ToLower(action.From)) == "n1") {
			return fmt.Errorf("action %s requires n1 row but diff is missing_on_n1 (pk %s)", action.Type, row.pkStr)
		}
	}

	if action.Type == planner.RepairActionApplyFrom && action.Mode == planner.RepairApplyModeInsert && row.diffType == "row_mismatch" {
		return fmt.Errorf("apply_from insert mode cannot be used on mismatched rows (pk %s)", row.pkStr)
	}
	if action.Type == planner.RepairActionBidirectional && row.diffType == "row_mismatch" {
		return fmt.Errorf("bidirectional cannot be used on mismatched rows (pk %s)", row.pkStr)
	}

	if action.Type == planner.RepairActionCustom && action.CustomRow == nil && action.CustomHelpers == nil {
		return fmt.Errorf("custom action missing custom_row/helpers (pk %s)", row.pkStr)
	}

	return nil
}
