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
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

// stopSignals are the signals that stop a diff command in a correct way.
var stopSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

// signalContext returns a context that is cancelled on the first SIGINT or
// SIGTERM. Its cause names the signal, so the command can report why it
// stopped. After the first signal the process gets the default action back,
// so a second signal stops it at once. The caller must call stop when the
// command ends.
func signalContext(parent context.Context) (ctx context.Context, stop func()) {
	ctx, cancel := context.WithCancelCause(parent)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, stopSignals...)
	done := make(chan struct{})
	go func() {
		select {
		case sig := <-ch:
			signal.Stop(ch)
			cancel(fmt.Errorf("interrupted by signal %q", sig))
		case <-done:
		}
	}()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			close(done)
			signal.Stop(ch)
			cancel(nil)
		})
	}
	return ctx, stop
}

// withStopCause returns the cause of the stop of ctx in place of err. A
// command that a signal stops then reports the signal, not the error of a
// query that the stop cancelled.
func withStopCause(ctx context.Context, err error) error {
	if err == nil || ctx.Err() == nil {
		return err
	}
	return context.Cause(ctx)
}
