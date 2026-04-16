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

package diff

import (
	"strings"
	"testing"
)

func TestResolveAgainstOrigin_EmptyInput(t *testing.T) {
	task := &TableDiffTask{AgainstOrigin: ""}
	if err := task.resolveAgainstOrigin(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.resolvedAgainstOrigin != "" {
		t.Fatalf("expected empty resolvedAgainstOrigin, got %q", task.resolvedAgainstOrigin)
	}
}

func TestResolveAgainstOrigin_NoNodeOriginNames(t *testing.T) {
	task := &TableDiffTask{AgainstOrigin: "n1"}
	err := task.resolveAgainstOrigin()
	if err == nil {
		t.Fatal("expected error when NodeOriginNames is empty")
	}
	if !strings.Contains(err.Error(), "no node origin names available") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveAgainstOrigin_MatchByID(t *testing.T) {
	task := &TableDiffTask{
		AgainstOrigin:   "3",
		NodeOriginNames: map[string]string{"3": "n1", "4": "n2"},
	}
	if err := task.resolveAgainstOrigin(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.resolvedAgainstOrigin != "3" {
		t.Fatalf("expected resolvedAgainstOrigin=3, got %q", task.resolvedAgainstOrigin)
	}
}

func TestResolveAgainstOrigin_MatchByName_SpockNodeName(t *testing.T) {
	task := &TableDiffTask{
		AgainstOrigin:   "n1",
		NodeOriginNames: map[string]string{"3": "n1", "4": "n2"},
	}
	if err := task.resolveAgainstOrigin(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.resolvedAgainstOrigin != "3" {
		t.Fatalf("expected resolvedAgainstOrigin=3, got %q", task.resolvedAgainstOrigin)
	}
}

func TestResolveAgainstOrigin_MatchByName_SubscriptionName(t *testing.T) {
	// Native PG: NodeOriginNames maps roident -> subscription name
	task := &TableDiffTask{
		AgainstOrigin:   "sub_n1_to_n2",
		NodeOriginNames: map[string]string{"5": "sub_n1_to_n2", "6": "sub_n3_to_n2"},
	}
	if err := task.resolveAgainstOrigin(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if task.resolvedAgainstOrigin != "5" {
		t.Fatalf("expected resolvedAgainstOrigin=5, got %q", task.resolvedAgainstOrigin)
	}
}

func TestResolveAgainstOrigin_NoMatch(t *testing.T) {
	task := &TableDiffTask{
		AgainstOrigin:   "nonexistent",
		NodeOriginNames: map[string]string{"3": "n1", "4": "n2"},
	}
	err := task.resolveAgainstOrigin()
	if err == nil {
		t.Fatal("expected error for unresolvable origin")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Fatalf("error should mention the unresolved name: %v", err)
	}
}

func TestBuildEffectiveFilter_AgainstOrigin(t *testing.T) {
	task := &TableDiffTask{
		resolvedAgainstOrigin: "3",
	}
	filter, err := task.buildEffectiveFilter()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(filter, "pg_xact_commit_timestamp_origin") {
		t.Fatalf("expected pg_xact_commit_timestamp_origin in filter, got: %s", filter)
	}
	if !strings.Contains(filter, "'3'") {
		t.Fatalf("expected roident=3 in filter, got: %s", filter)
	}
	if strings.Contains(filter, "spock") {
		t.Fatalf("filter should not reference spock: %s", filter)
	}
}

func TestBuildEffectiveFilter_NonNumericOrigin(t *testing.T) {
	task := &TableDiffTask{
		resolvedAgainstOrigin: "not_a_number",
	}
	_, err := task.buildEffectiveFilter()
	if err == nil {
		t.Fatal("expected error for non-numeric resolved origin")
	}
	if !strings.Contains(err.Error(), "not a valid numeric node ID") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildEffectiveFilter_Empty(t *testing.T) {
	task := &TableDiffTask{}
	filter, err := task.buildEffectiveFilter()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if filter != "" {
		t.Fatalf("expected empty filter, got: %s", filter)
	}
}
