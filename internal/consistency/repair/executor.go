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
	"time"

	"github.com/pgedge/ace/internal/consistency/repair/plan"
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

func CalculatePlanRepairSets(task *TableRepairTask) (map[string]map[string]map[string]any, map[string]map[string]map[string]any, error) {
	if task.RepairPlan == nil {
		return nil, nil, fmt.Errorf("repair plan is nil")
	}

	tableKey := fmt.Sprintf("%s.%s", task.Schema, task.Table)
	tablePlan, ok := task.RepairPlan.Tables[tableKey]
	if !ok {
		return nil, nil, fmt.Errorf("repair plan does not contain table %s", tableKey)
	}

	fullRowsToUpsert := make(map[string]map[string]map[string]any) // nodeName -> pkey -> rowData
	fullRowsToDelete := make(map[string]map[string]map[string]any) // nodeName -> pkey -> rowData
	task.planRuleMatches = make(map[string]map[string]string)
	task.planStaleChecks = make(map[string]map[string]planStaleCheck)

	for pairKey, diffs := range task.RawDiffs.NodeDiffs {
		nodes := strings.Split(pairKey, "/")
		if len(nodes) != 2 {
			continue
		}
		node1Name, node2Name := nodes[0], nodes[1]
		diffRows, err := buildPlanDiffRows(task, node1Name, node2Name, diffs)
		if err != nil {
			return nil, nil, err
		}

		for _, d := range diffRows {
			action, ruleName, err := resolvePlanAction(task, tablePlan, d)
			if err != nil {
				return nil, nil, err
			}
			if action == nil {
				return nil, nil, fmt.Errorf("no action resolved for row %s", d.pkStr)
			}
			if err := ensureActionCompatible(d, action); err != nil {
				return nil, nil, err
			}
			if err := applyPlanAction(d, action, ruleName, fullRowsToUpsert, fullRowsToDelete, task.planRuleMatches, task.planStaleChecks); err != nil {
				return nil, nil, err
			}
		}
	}

	return fullRowsToUpsert, fullRowsToDelete, nil
}

func buildPlanDiffRows(task *TableRepairTask, node1Name, node2Name string, diffs types.DiffByNodePair) ([]planDiffRow, error) {
	node1Rows := diffs.Rows[node1Name]
	node2Rows := diffs.Rows[node2Name]

	node1RowsByPKey := make(map[string]map[string]any)
	node1PkMaps := make(map[string]map[string]any)
	node1MetaByPKey := make(map[string]map[string]any)
	for _, row := range node1Rows {
		pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
		if err != nil {
			return nil, fmt.Errorf("error stringifying pkey for row on %s: %w", node1Name, err)
		}
		raw := utils.OrderedMapToMap(row)
		meta := extractSpockMeta(raw)
		cleanRow := utils.StripSpockMetadata(raw)
		node1RowsByPKey[pkeyStr] = cleanRow
		node1MetaByPKey[pkeyStr] = meta
		pkMap, err := extractPkMap(task.Key, cleanRow)
		if err != nil {
			return nil, fmt.Errorf("node %s row %s: %w", node1Name, pkeyStr, err)
		}
		node1PkMaps[pkeyStr] = pkMap
	}

	node2RowsByPKey := make(map[string]map[string]any)
	node2PkMaps := make(map[string]map[string]any)
	node2MetaByPKey := make(map[string]map[string]any)
	for _, row := range node2Rows {
		pkeyStr, err := utils.StringifyOrderedMapKey(row, task.Key)
		if err != nil {
			return nil, fmt.Errorf("error stringifying pkey for row on %s: %w", node2Name, err)
		}
		raw := utils.OrderedMapToMap(row)
		meta := extractSpockMeta(raw)
		cleanRow := utils.StripSpockMetadata(raw)
		node2RowsByPKey[pkeyStr] = cleanRow
		node2MetaByPKey[pkeyStr] = meta
		pkMap, err := extractPkMap(task.Key, cleanRow)
		if err != nil {
			return nil, fmt.Errorf("node %s row %s: %w", node2Name, pkeyStr, err)
		}
		node2PkMaps[pkeyStr] = pkMap
	}

	seen := make(map[string]struct{})
	var diffRows []planDiffRow

	for pkStr, row := range node1RowsByPKey {
		diffRows = append(diffRows, planDiffRow{
			pkStr:     pkStr,
			pkMap:     node1PkMaps[pkStr],
			node1Name: node1Name,
			node2Name: node2Name,
			n1Row:     row,
			n2Row:     node2RowsByPKey[pkStr],
			n1Meta:    node1MetaByPKey[pkStr],
			n2Meta:    node2MetaByPKey[pkStr],
			diffType: func() string {
				if _, ok := node2RowsByPKey[pkStr]; ok {
					return "row_mismatch"
				}
				return "missing_on_n2"
			}(),
			columnsChanged: computeChangedColumns(row, node2RowsByPKey[pkStr]),
		})
		seen[pkStr] = struct{}{}
	}

	for pkStr, row := range node2RowsByPKey {
		if _, ok := seen[pkStr]; ok {
			continue
		}
		diffRows = append(diffRows, planDiffRow{
			pkStr:          pkStr,
			pkMap:          node2PkMaps[pkStr],
			node1Name:      node1Name,
			node2Name:      node2Name,
			n1Row:          node1RowsByPKey[pkStr],
			n2Row:          row,
			n1Meta:         node1MetaByPKey[pkStr],
			n2Meta:         node2MetaByPKey[pkStr],
			diffType:       "missing_on_n1",
			columnsChanged: computeChangedColumns(node1RowsByPKey[pkStr], row),
		})
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

func resolvePlanAction(task *TableRepairTask, tablePlan planner.RepairTablePlan, row planDiffRow) (*planner.RepairPlanAction, string, error) {
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
		if !matchPKIn(rule.PKIn, row.pkMap, task.Key, task.SimplePrimaryKey) {
			continue
		}
		if len(rule.DiffTypes) > 0 && !containsString(rule.DiffTypes, row.diffType) {
			continue
		}
		if len(rule.ColumnsChanged) > 0 && !columnsIntersect(rule.ColumnsChanged, row.columnsChanged) {
			continue
		}
		if rule.CompiledWhen() != nil {
			ok, err := rule.CompiledWhen().Eval(func(source, column string) (any, bool) {
				return lookupValue(source, column, row)
			})
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

func pkMatchesOverride(pkOrder []string, rowPk map[string]any, overridePk map[string]any) bool {
	if len(overridePk) != len(pkOrder) {
		return false
	}

	equal := func(a, b any) bool {
		if af, ok := asFloat(a); ok {
			if bf, ok2 := asFloat(b); ok2 {
				return af == bf
			}
		}
		return reflect.DeepEqual(a, b)
	}

	for _, col := range pkOrder {
		if !equal(rowPk[col], overridePk[col]) {
			return false
		}
	}
	return true
}

func matchPKIn(matchers []planner.RepairPKMatcher, rowPk map[string]any, pkOrder []string, simple bool) bool {
	if len(matchers) == 0 {
		return true
	}

	equal := func(a, b any) bool {
		if af, ok := asFloat(a); ok {
			if bf, ok2 := asFloat(b); ok2 {
				return af == bf
			}
		}
		return reflect.DeepEqual(a, b)
	}

	for _, m := range matchers {
		if simple {
			val := rowPk[pkOrder[0]]
			for _, eq := range m.Equals {
				if equal(eq, val) {
					return true
				}
			}
			if m.Range != nil {
				if compareRange(val, m.Range.From, m.Range.To) {
					return true
				}
			}
		} else {
			// Composite PK: support equals with tuple order
			for _, eq := range m.Equals {
				tuple, ok := eq.([]any)
				if !ok || len(tuple) != len(pkOrder) {
					continue
				}
				all := true
				for i, col := range pkOrder {
					if !equal(rowPk[col], tuple[i]) {
						all = false
						break
					}
				}
				if all {
					return true
				}
			}
		}
	}
	return false
}

func compareRange(val, from, to any) bool {
	ln, lok := asFloat(val)
	fn, fok := asFloat(from)
	tn, tok := asFloat(to)
	if lok && fok && tok {
		return ln >= fn && ln <= tn
	}

	ls, lsok := val.(string)
	fs, fsok := from.(string)
	ts, tsok := to.(string)
	if lsok && fsok && tsok {
		return ls >= fs && ls <= ts
	}
	return false
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
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

func applyPlanAction(row planDiffRow, action *planner.RepairPlanAction, ruleName string, upserts, deletes map[string]map[string]map[string]any, matches map[string]map[string]string, staleChecks map[string]map[string]planStaleCheck) error {
	switch action.Type {
	case planner.RepairActionKeepN1:
		if row.n1Row == nil {
			return fmt.Errorf("keep_n1 specified but row missing on %s (pk %s)", row.node1Name, row.pkStr)
		}
		addRow(upserts, row.node2Name, row.pkStr, copyMap(row.n1Row))
		recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
		recordMatch(matches, row.node2Name, row.pkStr, ruleName)
	case planner.RepairActionKeepN2:
		if row.n2Row == nil {
			return fmt.Errorf("keep_n2 specified but row missing on %s (pk %s)", row.node2Name, row.pkStr)
		}
		addRow(upserts, row.node1Name, row.pkStr, copyMap(row.n2Row))
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
			addRow(upserts, row.node2Name, row.pkStr, copyMap(row.n1Row))
			recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node2Name, row.pkStr, ruleName)
		case "n2":
			if row.n2Row == nil {
				return fmt.Errorf("apply_from n2 specified but row missing on %s (pk %s)", row.node2Name, row.pkStr)
			}
			if mode == planner.RepairApplyModeInsert && row.n1Row != nil {
				return nil
			}
			addRow(upserts, row.node1Name, row.pkStr, copyMap(row.n2Row))
			recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		default:
			return fmt.Errorf("apply_from requires from to be n1 or n2 (pk %s)", row.pkStr)
		}
	case planner.RepairActionBidirectional:
		if row.n1Row != nil {
			addRow(upserts, row.node2Name, row.pkStr, copyMap(row.n1Row))
			recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node2Name, row.pkStr, ruleName)
		}
		if row.n2Row != nil {
			addRow(upserts, row.node1Name, row.pkStr, copyMap(row.n2Row))
			recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		}
	case planner.RepairActionCustom:
		customRow, err := buildCustomRow(action, row)
		if err != nil {
			return err
		}
		addRow(upserts, row.node1Name, row.pkStr, copyMap(customRow))
		addRow(upserts, row.node2Name, row.pkStr, copyMap(customRow))
		recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
		recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
		recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		recordMatch(matches, row.node2Name, row.pkStr, ruleName)
	case planner.RepairActionSkip:
		return nil
	case planner.RepairActionDelete:
		if row.n1Row != nil {
			addRow(deletes, row.node1Name, row.pkStr, row.n1Row)
			recordPlanStaleCheck(staleChecks, row.node1Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node1Name, row.pkStr, ruleName)
		}
		if row.n2Row != nil {
			addRow(deletes, row.node2Name, row.pkStr, row.n2Row)
			recordPlanStaleCheck(staleChecks, row.node2Name, row.pkStr, row, action, ruleName)
			recordMatch(matches, row.node2Name, row.pkStr, ruleName)
		}
	default:
		return fmt.Errorf("action %s not supported in execution", action.Type)
	}
	return nil
}

func addRow(target map[string]map[string]map[string]any, node, pk string, row map[string]any) {
	if target[node] == nil {
		target[node] = make(map[string]map[string]any)
	}
	target[node][pk] = row
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

func lookupValue(source, column string, row planDiffRow) (any, bool) {
	switch source {
	case "n1":
		if row.n1Row != nil {
			if v, ok := row.n1Row[column]; ok {
				return v, true
			}
		}
		if row.n1Meta != nil {
			if v, ok := row.n1Meta[column]; ok {
				return v, true
			}
		}
	case "n2":
		if row.n2Row != nil {
			if v, ok := row.n2Row[column]; ok {
				return v, true
			}
		}
		if row.n2Meta != nil {
			if v, ok := row.n2Meta[column]; ok {
				return v, true
			}
		}
	}
	return nil, false
}

func lookupTemplate(inner string, row planDiffRow) (any, bool) {
	parts := strings.Split(inner, ".")
	if len(parts) != 2 {
		return nil, false
	}
	source := strings.ToLower(strings.TrimSpace(parts[0]))
	col := strings.TrimSpace(parts[1])
	return lookupValue(source, col, row)
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
	case planner.RepairActionBidirectional, planner.RepairActionSkip, planner.RepairActionDelete:
		return nil
	default:
		return fmt.Errorf("unsupported action type %s", action.Type)
	}
	return nil
}

func buildCustomRow(action *planner.RepairPlanAction, row planDiffRow) (map[string]any, error) {
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
				switch src {
				case "n1":
					if row.n1Row != nil {
						if val, ok := row.n1Row[col]; ok && val != nil {
							result[col] = val
							break
						}
					}
				case "n2":
					if row.n2Row != nil {
						if val, ok := row.n2Row[col]; ok && val != nil {
							result[col] = val
							break
						}
					}
				}
			}
		}
	}

	// pick_freshest helper backfills remaining columns from the chosen side.
	if action.CustomHelpers != nil && action.CustomHelpers.PickFreshest != nil {
		pick := action.CustomHelpers.PickFreshest
		winner := freshestSide(pick.Key, pick.Tie, row)
		var source map[string]any
		switch winner {
		case "n1":
			source = row.n1Row
		case "n2":
			source = row.n2Row
		}
		if source != nil {
			for col, val := range source {
				if _, exists := result[col]; !exists {
					result[col] = val
				}
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

func substituteValue(val any, row planDiffRow) any {
	s, ok := val.(string)
	if !ok {
		return val
	}
	str := strings.TrimSpace(s)
	if strings.HasPrefix(str, "{{") && strings.HasSuffix(str, "}}") {
		inner := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(str, "{{"), "}}"))
		if val, ok := lookupTemplate(inner, row); ok {
			return val
		}
	}
	return val
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

func freshestSide(key string, tie string, row planDiffRow) string {
	val1 := row.n1Row
	val2 := row.n2Row
	var k1, k2 any
	if val1 != nil {
		k1 = val1[key]
	}
	if val2 != nil {
		k2 = val2[key]
	}
	n1Ok := k1 != nil
	n2Ok := k2 != nil
	if !n1Ok && n2Ok {
		return "n2"
	}
	if n1Ok && !n2Ok {
		return "n1"
	}
	if !n1Ok && !n2Ok {
		return strings.TrimSpace(strings.ToLower(tie))
	}

	if f1, ok := asFloat(k1); ok {
		if f2, ok2 := asFloat(k2); ok2 {
			if f1 > f2 {
				return "n1"
			} else if f2 > f1 {
				return "n2"
			}
			return strings.TrimSpace(strings.ToLower(tie))
		}
	}

	if s1, ok := k1.(string); ok {
		if s2, ok2 := k2.(string); ok2 {
			if s1 > s2 {
				return "n1"
			} else if s2 > s1 {
				return "n2"
			}
			return strings.TrimSpace(strings.ToLower(tie))
		}
	}

	// Try parsing timestamps if values are strings
	if s1, ok := k1.(string); ok {
		if s2, ok2 := k2.(string); ok2 {
			if t1, err1 := time.Parse(time.RFC3339, s1); err1 == nil {
				if t2, err2 := time.Parse(time.RFC3339, s2); err2 == nil {
					if t1.After(t2) {
						return "n1"
					} else if t2.After(t1) {
						return "n2"
					}
					return strings.TrimSpace(strings.ToLower(tie))
				}
			}
		}
	}

	return strings.TrimSpace(strings.ToLower(tie))
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

	if action.Type == planner.RepairActionCustom && action.CustomRow == nil && action.CustomHelpers == nil {
		return fmt.Errorf("custom action missing custom_row/helpers (pk %s)", row.pkStr)
	}

	return nil
}
