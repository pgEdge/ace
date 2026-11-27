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
	"context"
	"testing"
	"time"
)

// The retry of busy leaf indexes stops when no tree is busy any more.
func TestRetryUntilDoneStopsWhenDone(t *testing.T) {
	calls := 0
	retryUntilDone(context.Background(), time.Millisecond, func() bool {
		calls++
		return calls < 3
	})
	if calls != 3 {
		t.Fatalf("try was called %d times, want 3", calls)
	}
}

// The retry stops when the drain ends, even if a tree stays busy.
func TestRetryUntilDoneStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		retryUntilDone(ctx, time.Millisecond, func() bool { return true })
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retry did not stop after the context ended")
	}
}
