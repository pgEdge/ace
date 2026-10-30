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

package cli

import (
	"context"
	"errors"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestSignalContextStopsOnSignal(t *testing.T) {
	ctx, stop := signalContext(context.Background())
	defer stop()

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("context is not done after SIGTERM")
	}
	cause := context.Cause(ctx)
	if cause == nil || !strings.Contains(cause.Error(), "interrupted by signal") {
		t.Fatalf("cause = %v, want an error that names the signal", cause)
	}
}

func TestSignalContextStopWithoutSignal(t *testing.T) {
	ctx, stop := signalContext(context.Background())
	stop()
	stop()
	if !errors.Is(context.Cause(ctx), context.Canceled) {
		t.Fatalf("cause = %v, want context.Canceled", context.Cause(ctx))
	}
}

func TestWithStopCause(t *testing.T) {
	queryErr := errors.New("query cancelled")
	if err := withStopCause(context.Background(), queryErr); err != queryErr {
		t.Fatalf("running context: got %v, want %v", err, queryErr)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	if err := withStopCause(ctx, nil); err != nil {
		t.Fatalf("nil error: got %v", err)
	}
	signalErr := errors.New("interrupted by signal")
	cancel(signalErr)
	if err := withStopCause(ctx, nil); err != nil {
		t.Fatalf("nil error after stop: got %v", err)
	}
	if err := withStopCause(ctx, queryErr); err != signalErr {
		t.Fatalf("stopped context: got %v, want %v", err, signalErr)
	}
}
