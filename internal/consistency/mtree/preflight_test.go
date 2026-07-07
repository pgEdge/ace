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

package mtree

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsMissingTreeErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"no rows", pgx.ErrNoRows, true},
		{"wrapped no rows", fmt.Errorf("query failed: %w", pgx.ErrNoRows), true},
		{"undefined table", &pgconn.PgError{Code: "42P01"}, true},
		{"wrapped undefined table", fmt.Errorf("migrate: %w", &pgconn.PgError{Code: "42P01"}), true},
		{"invalid schema name", &pgconn.PgError{Code: "3F000"}, true},
		{"wrapped invalid schema name", fmt.Errorf("migrate: %w", &pgconn.PgError{Code: "3F000"}), true},
		{"other pg error", &pgconn.PgError{Code: "42501"}, false},
		{"unrelated error", errors.New("connection refused"), false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isMissingTreeErr(tc.err); got != tc.want {
				t.Errorf("isMissingTreeErr(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestMissingTreeErrorMessage(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ClusterName = "demo"
	m.QualifiedTableName = "public.time_repair_test2"
	m.DBName = "postgres"

	err := m.missingTreeError("n1", pgx.ErrNoRows)
	if !errors.Is(err, ErrMtreeNotFound) {
		t.Fatalf("expected error to wrap ErrMtreeNotFound, got %v", err)
	}
	want := "no merkle tree found for public.time_repair_test2 on node n1: " +
		"run 'ace mtree build demo public.time_repair_test2 --dbname postgres' first"
	if err.Error() != want {
		t.Errorf("message mismatch:\n got: %s\nwant: %s", err.Error(), want)
	}
}

func TestMissingTreeErrorMessageOmitsEmptyDBName(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ClusterName = "demo"
	m.QualifiedTableName = "public.t"

	err := m.missingTreeError("n2", pgx.ErrNoRows)
	want := "no merkle tree found for public.t on node n2: " +
		"run 'ace mtree build demo public.t' first"
	if err.Error() != want {
		t.Errorf("message mismatch:\n got: %s\nwant: %s", err.Error(), want)
	}
}

func TestMissingTreeErrorMessageSchemaMissingWithDBName(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ClusterName = "demo"
	m.QualifiedTableName = "public.time_repair_test2"
	m.DBName = "postgres"

	err := m.missingTreeError("n1", &pgconn.PgError{Code: "3F000"})
	if !errors.Is(err, ErrMtreeNotFound) {
		t.Fatalf("expected error to wrap ErrMtreeNotFound, got %v", err)
	}
	want := "no merkle tree found for public.time_repair_test2 on node n1: " +
		"run 'ace mtree init demo --dbname postgres' and then " +
		"'ace mtree build demo public.time_repair_test2 --dbname postgres' first"
	if err.Error() != want {
		t.Errorf("message mismatch:\n got: %s\nwant: %s", err.Error(), want)
	}
}

func TestMissingTreeErrorMessageSchemaMissingOmitsEmptyDBName(t *testing.T) {
	m := &MerkleTreeTask{}
	m.ClusterName = "demo"
	m.QualifiedTableName = "public.t"

	err := m.missingTreeError("n2", &pgconn.PgError{Code: "3F000"})
	want := "no merkle tree found for public.t on node n2: " +
		"run 'ace mtree init demo' and then 'ace mtree build demo public.t' first"
	if err.Error() != want {
		t.Errorf("message mismatch:\n got: %s\nwant: %s", err.Error(), want)
	}
}
