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
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgedge/ace/pkg/types"
	"github.com/stretchr/testify/require"
)

// nodeFailureTask returns a task without node info and connection pools, so
// that repair cannot start on any node. On n1/n2, n1 has keys 1 and 2, n2 has
// keys 2 (with another value) and 3.
func nodeFailureTask() *TableRepairTask {
	row := func(id int, v string) types.OrderedMap {
		return types.OrderedMap{{Key: "id", Value: id}, {Key: "v", Value: v}}
	}
	task := NewTableRepairTask()
	task.Schema, task.Table = "public", "t"
	task.Key = []string{"id"}
	task.RawDiffs = types.DiffOutput{NodeDiffs: map[string]types.DiffByNodePair{
		"n1/n2": {Rows: map[string][]types.OrderedMap{
			"n1": {row(1, "a"), row(2, "b")},
			"n2": {row(2, "x"), row(3, "z")},
		}},
	}}
	return task
}

// An error in the setup of a node does not come from a row: repair stops with
// this error, the task fails, and no row is recorded as failed.
func TestRepairStopsOnSetupError(t *testing.T) {
	t.Run("source_of_truth", func(t *testing.T) {
		task := nodeFailureTask()
		task.SourceOfTruth = "n1"
		err := task.runUnidirectionalRepair(time.Now())
		require.ErrorContains(t, err, "no node info for n2")
		require.Equal(t, "FAILED", task.TaskStatus)
		require.Empty(t, task.FailedOps)
	})
	t.Run("bidirectional", func(t *testing.T) {
		task := nodeFailureTask()
		task.Bidirectional = true
		err := task.runBidirectionalRepair()
		require.ErrorContains(t, err, "no connection pool for node n1")
		require.Equal(t, "FAILED", task.TaskStatus)
		require.Empty(t, task.FailedOps)
	})
}

// Only an answer of the server makes a failed statement or COMMIT a failure
// of the rows. A broken connection or a cancelled context does not, and
// during COMMIT it leaves the result of the command unknown.
func TestRejectedByServer(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"unique violation", &pgconn.PgError{Code: "23505"}, true},
		{"wrapped deferred constraint", fmt.Errorf("commit: %w", &pgconn.PgError{Code: "23503"}), true},
		{"commit turned into rollback", pgx.ErrTxCommitRollback, true},
		{"connection closed", io.ErrUnexpectedEOF, false},
		{"network error", &net.OpError{Op: "read", Err: io.EOF}, false},
		{"cancelled context", context.Canceled, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, rejectedByServer(tc.err))
		})
	}
}
