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

package cdc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsSlotBusyErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "direct 55006 PgError",
			err:  &pgconn.PgError{Code: "55006", Message: "replication slot is active"},
			want: true,
		},
		{
			name: "wrapped 55006 PgError",
			err:  fmt.Errorf("StartReplication failed: %w", &pgconn.PgError{Code: "55006"}),
			want: true,
		},
		{
			name: "different SQLSTATE",
			err:  &pgconn.PgError{Code: "42704"},
			want: false,
		},
		{
			name: "plain error",
			err:  errors.New("some other failure"),
			want: false,
		},
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSlotBusyErr(tt.err); got != tt.want {
				t.Errorf("isSlotBusyErr() = %v, want %v", got, tt.want)
			}
		})
	}
}
