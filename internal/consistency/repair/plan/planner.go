// ///////////////////////////////////////////////////////////////////////////
//
// # ACE - Active Consistency Engine
//
// Copyright (C) 2023 - 2025, pgEdge (https://www.pgedge.com/)
//
// This software is released under the PostgreSQL License:
// https://opensource.org/license/postgresql
//
// ///////////////////////////////////////////////////////////////////////////

package planner

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pgedge/ace/internal/consistency/repair/plan/parser"
	"gopkg.in/yaml.v3"
)

// Advanced repair-file schema notes:
//   - Versioned YAML/JSON document describing fallbacks, ordered rules, and explicit row overrides.
//   - Precedence: row_overrides > first matching rule > table.default_action > global default_action.
//   - Selectors: pk_in (values or ranges for simple PKs), diff_type, columns_changed, when (restricted expr over n1./n2.).
//   - Actions: keep_n1/keep_n2, apply_from {from: n1|n2, mode: replace|upsert|insert}, bidirectional, custom {custom_row or helpers},
//     skip, delete (optional).
//   - Rules must declare at least one selector to avoid match-all accidents; tables can inherit the global default.
//
// This scaffolding parses and validates the file; execution wiring comes next.
const RepairPlanSchemaVersion = 1

type RepairActionType string

const (
	RepairActionKeepN1        RepairActionType = "keep_n1"
	RepairActionKeepN2        RepairActionType = "keep_n2"
	RepairActionApplyFrom     RepairActionType = "apply_from"
	RepairActionBidirectional RepairActionType = "bidirectional"
	RepairActionCustom        RepairActionType = "custom"
	RepairActionSkip          RepairActionType = "skip"
	RepairActionDelete        RepairActionType = "delete"
)

type RepairApplyMode string

const (
	RepairApplyModeReplace RepairApplyMode = "replace"
	RepairApplyModeUpsert  RepairApplyMode = "upsert"
	RepairApplyModeInsert  RepairApplyMode = "insert"
)

// RepairPlanFile represents a versioned repair-file describing defaults, rules, and per-row overrides.
type RepairPlanFile struct {
	Version       int                        `json:"version" yaml:"version"`
	DefaultAction *RepairPlanAction          `json:"default_action,omitempty" yaml:"default_action,omitempty"`
	Tables        map[string]RepairTablePlan `json:"tables,omitempty" yaml:"tables,omitempty"` // key: qualified table name
	Metadata      map[string]any             `json:"metadata,omitempty" yaml:"metadata,omitempty"`
	Notes         string                     `json:"notes,omitempty" yaml:"notes,omitempty"`
}

type RepairTablePlan struct {
	DefaultAction *RepairPlanAction   `json:"default_action,omitempty" yaml:"default_action,omitempty"`
	Rules         []RepairRule        `json:"rules,omitempty" yaml:"rules,omitempty"`
	RowOverrides  []RepairRowOverride `json:"row_overrides,omitempty" yaml:"row_overrides,omitempty"`
}

// RepairRule is evaluated in order; the first matching rule wins.
type RepairRule struct {
	Name           string            `json:"name,omitempty" yaml:"name,omitempty"`
	PKIn           []RepairPKMatcher `json:"pk_in,omitempty" yaml:"pk_in,omitempty"`
	DiffTypes      []string          `json:"diff_type,omitempty" yaml:"diff_type,omitempty"`
	ColumnsChanged []string          `json:"columns_changed,omitempty" yaml:"columns_changed,omitempty"`
	When           string            `json:"when,omitempty" yaml:"when,omitempty"` // restricted expression over n1./n2. values
	Action         RepairPlanAction  `json:"action" yaml:"action"`

	compiledWhen *parser.WhenExpr `json:"-" yaml:"-"`
}

// RepairRowOverride matches an exact PK (map for composite keys) and wins before any rule.
type RepairRowOverride struct {
	Name   string           `json:"name,omitempty" yaml:"name,omitempty"`
	PK     map[string]any   `json:"pk" yaml:"pk"`
	Action RepairPlanAction `json:"action" yaml:"action"`
}

// RepairPKMatcher supports exact PKs and ranges (range only for simple PKs).
type RepairPKMatcher struct {
	Equals []any          `json:"equals,omitempty" yaml:"equals,omitempty"` // []any for simple PK, []any slice for composite PK values
	Range  *RepairPKRange `json:"range,omitempty" yaml:"range,omitempty"`
}

type RepairPKRange struct {
	From any `json:"from" yaml:"from"`
	To   any `json:"to" yaml:"to"`
}

// RepairPlanAction captures the small, memorable action set.
type RepairPlanAction struct {
	Type          RepairActionType  `json:"type" yaml:"type"`
	From          string            `json:"from,omitempty" yaml:"from,omitempty"` // n1 or n2 for apply_from
	Mode          RepairApplyMode   `json:"mode,omitempty" yaml:"mode,omitempty"` // replace|upsert|insert for apply_from
	CustomRow     map[string]any    `json:"custom_row,omitempty" yaml:"custom_row,omitempty"`
	CustomHelpers *CustomHelperSpec `json:"helpers,omitempty" yaml:"helpers,omitempty"`
}

type CustomHelperSpec struct {
	CoalescePriority []string          `json:"coalesce_priority,omitempty" yaml:"coalesce_priority,omitempty"` // e.g., ["n1","n2"]
	PickFreshest     *PickFreshestSpec `json:"pick_freshest,omitempty" yaml:"pick_freshest,omitempty"`
}

type PickFreshestSpec struct {
	Key string `json:"key" yaml:"key"`           // column name used for freshness comparison
	Tie string `json:"tie,omitempty" yaml:"tie"` // n1 or n2
}

// LoadRepairPlanFile parses YAML or JSON into a RepairPlanFile and performs lightweight validation.
func LoadRepairPlanFile(planPath string) (*RepairPlanFile, error) {
	raw, err := os.ReadFile(planPath)
	if err != nil {
		return nil, fmt.Errorf("read repair plan %s: %w", planPath, err)
	}

	plan := &RepairPlanFile{}

	// Try YAML first (superset), then JSON for clearer errors when input is strictly JSON.
	yamlErr := yaml.Unmarshal(raw, plan)
	if yamlErr != nil {
		if jsonErr := json.Unmarshal(raw, plan); jsonErr != nil {
			return nil, fmt.Errorf("parse repair plan %s as yaml (%v) or json (%v)", planPath, yamlErr, jsonErr)
		}
	}

	if plan.Version == 0 {
		plan.Version = RepairPlanSchemaVersion
	}

	if err := plan.Validate(); err != nil {
		return nil, fmt.Errorf("invalid repair plan %s: %w", planPath, err)
	}

	return plan, nil
}

// Validate performs static checks. It intentionally keeps defaults permissive; execution-time checks will enforce column existence.
func (plan *RepairPlanFile) Validate() error {
	if plan == nil {
		return fmt.Errorf("repair plan is nil")
	}
	if plan.Version <= 0 {
		return fmt.Errorf("version must be greater than zero")
	}
	if plan.DefaultAction != nil {
		if err := validateRepairAction(plan.DefaultAction, "global", "default_action"); err != nil {
			return err
		}
	}
	if len(plan.Tables) == 0 {
		return fmt.Errorf("at least one table entry is required")
	}

	validDiffTypes := map[string]struct{}{
		"row_mismatch":  {},
		"missing_on_n1": {},
		"missing_on_n2": {},
		"deleted_on_n1": {},
		"deleted_on_n2": {},
	}

	for tableName, tablePlan := range plan.Tables {
		if tablePlan.DefaultAction != nil {
			if err := validateRepairAction(tablePlan.DefaultAction, tableName, "table.default_action"); err != nil {
				return err
			}
		}

		for i := range tablePlan.RowOverrides {
			override := &tablePlan.RowOverrides[i]
			if len(override.PK) == 0 {
				return fmt.Errorf("table %s row_override[%d] must provide pk map", tableName, i)
			}
			if err := validateRepairAction(&override.Action, tableName, fmt.Sprintf("row_override[%d]", i)); err != nil {
				return err
			}
		}

		for i := range tablePlan.Rules {
			rule := &tablePlan.Rules[i]
			if !ruleHasSelector(rule) {
				return fmt.Errorf("table %s rule[%d]%s must specify at least one selector (pk_in, diff_type, columns_changed, when)",
					tableName, i, ruleLabel(rule))
			}
			if err := validatePKMatchers(rule.PKIn, tableName, ruleLabel(rule)); err != nil {
				return err
			}
			if err := validateDiffTypes(rule.DiffTypes, validDiffTypes, tableName, ruleLabel(rule)); err != nil {
				return err
			}
			if err := validateRepairAction(&rule.Action, tableName, fmt.Sprintf("rule%s", ruleLabel(rule))); err != nil {
				return err
			}
			if len(rule.DiffTypes) > 0 {
				if err := validateActionCompatibility(&rule.Action, rule.DiffTypes, tableName, ruleLabel(rule)); err != nil {
					return err
				}
			}
			if strings.TrimSpace(rule.When) != "" {
				expr, err := parser.CompileWhenExpression(rule.When)
				if err != nil {
					return fmt.Errorf("table %s rule%s: invalid when expression: %w", tableName, ruleLabel(rule), err)
				}
				rule.compiledWhen = expr
			}
		}
	}

	return nil
}

func ruleLabel(rule *RepairRule) string {
	if rule == nil || strings.TrimSpace(rule.Name) == "" {
		return ""
	}
	return fmt.Sprintf(" (%s)", strings.TrimSpace(rule.Name))
}

func ruleHasSelector(rule *RepairRule) bool {
	if rule == nil {
		return false
	}
	return len(rule.PKIn) > 0 ||
		len(rule.DiffTypes) > 0 ||
		len(rule.ColumnsChanged) > 0 ||
		strings.TrimSpace(rule.When) != ""
}

// CompiledWhen returns the parsed predicate, if present.
func (r *RepairRule) CompiledWhen() *parser.WhenExpr {
	return r.compiledWhen
}

func validatePKMatchers(matchers []RepairPKMatcher, tableName, label string) error {
	for i := range matchers {
		matcher := matchers[i]
		if len(matcher.Equals) == 0 && matcher.Range == nil {
			return fmt.Errorf("table %s rule%s pk_in[%d] must specify equals or range", tableName, label, i)
		}
		if matcher.Range != nil {
			if matcher.Range.From == nil || matcher.Range.To == nil {
				return fmt.Errorf("table %s rule%s pk_in[%d] range requires both from and to", tableName, label, i)
			}
		}
	}
	return nil
}

func validateDiffTypes(diffTypes []string, allowed map[string]struct{}, tableName, label string) error {
	for i, dt := range diffTypes {
		dt = strings.TrimSpace(dt)
		if dt == "" {
			return fmt.Errorf("table %s rule%s diff_type[%d] cannot be empty", tableName, label, i)
		}
		if _, ok := allowed[dt]; !ok {
			return fmt.Errorf("table %s rule%s diff_type[%d] must be one of %v", tableName, label, i, keys(allowed))
		}
	}
	return nil
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func validateRepairAction(action *RepairPlanAction, tableName, location string) error {
	if action == nil {
		return fmt.Errorf("table %s %s action cannot be nil", tableName, location)
	}

	switch action.Type {
	case RepairActionKeepN1, RepairActionKeepN2, RepairActionBidirectional, RepairActionSkip, RepairActionDelete:
		return nil
	case RepairActionApplyFrom:
		if action.From == "" {
			return fmt.Errorf("table %s %s: apply_from requires 'from' (n1 or n2)", tableName, location)
		}
		if action.Mode != "" &&
			action.Mode != RepairApplyModeReplace &&
			action.Mode != RepairApplyModeUpsert &&
			action.Mode != RepairApplyModeInsert {
			return fmt.Errorf("table %s %s: apply_from mode must be one of replace|upsert|insert", tableName, location)
		}
		return nil
	case RepairActionCustom:
		if len(action.CustomRow) == 0 && action.CustomHelpers == nil {
			return fmt.Errorf("table %s %s: custom requires custom_row or helpers", tableName, location)
		}
		if action.CustomHelpers != nil {
			if len(action.CustomHelpers.CoalescePriority) > 0 {
				for _, src := range action.CustomHelpers.CoalescePriority {
					if src != "n1" && src != "n2" {
						return fmt.Errorf("table %s %s: coalesce_priority entries must be n1 or n2", tableName, location)
					}
				}
			}
			if action.CustomHelpers.PickFreshest != nil {
				if strings.TrimSpace(action.CustomHelpers.PickFreshest.Key) == "" {
					return fmt.Errorf("table %s %s: pick_freshest requires key", tableName, location)
				}
				if tie := strings.TrimSpace(action.CustomHelpers.PickFreshest.Tie); tie != "" && tie != "n1" && tie != "n2" {
					return fmt.Errorf("table %s %s: pick_freshest.tie must be n1 or n2", tableName, location)
				}
			}
		}
		return nil
	default:
		return fmt.Errorf("table %s %s: unsupported action type %q", tableName, location, action.Type)
	}
}

func validateActionCompatibility(action *RepairPlanAction, diffTypes []string, tableName, label string) error {
	for _, dt := range diffTypes {
		normalized := strings.TrimSpace(strings.ToLower(dt))
		switch normalized {
		case "missing_on_n1", "deleted_on_n1":
			if action.Type == RepairActionKeepN1 {
				return fmt.Errorf("table %s rule%s: keep_n1 incompatible with diff_type %s", tableName, label, dt)
			}
			if action.Type == RepairActionApplyFrom && strings.TrimSpace(strings.ToLower(action.From)) == "n1" {
				return fmt.Errorf("table %s rule%s: apply_from n1 incompatible with diff_type %s", tableName, label, dt)
			}
		case "missing_on_n2", "deleted_on_n2":
			if action.Type == RepairActionKeepN2 {
				return fmt.Errorf("table %s rule%s: keep_n2 incompatible with diff_type %s", tableName, label, dt)
			}
			if action.Type == RepairActionApplyFrom && strings.TrimSpace(strings.ToLower(action.From)) == "n2" {
				return fmt.Errorf("table %s rule%s: apply_from n2 incompatible with diff_type %s", tableName, label, dt)
			}
		case "row_mismatch":
			if action.Type == RepairActionApplyFrom && action.Mode == RepairApplyModeInsert {
				return fmt.Errorf("table %s rule%s: apply_from insert incompatible with diff_type %s", tableName, label, dt)
			}
		}
	}
	return nil
}
