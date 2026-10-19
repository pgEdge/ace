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
	"testing"
	"time"

	"github.com/pgedge/ace/internal/consistency/slicer"
)

// A reader of the block log does not depend on other readers: one reader
// can read every block while another reader has read none. This is what
// lets a fast node hash ahead of a slow one during mtree build.
func TestBlockLogReadersAreIndependent(t *testing.T) {
	l := newBlockLog()
	for i := 0; i < 100; i++ {
		if got := l.add(slicer.Block{Seq: i}); got != i {
			t.Fatalf("add returned index %d, want %d", got, i)
		}
	}

	// The fast reader reads all blocks; the slow reader has not started.
	for i := 0; i < 100; i++ {
		b, ok := l.get(i)
		if !ok || b.Seq != i {
			t.Fatalf("get(%d) = %v, %v; want block %d", i, b, ok, i)
		}
	}
	b, ok := l.get(0)
	if !ok || b.Seq != 0 {
		t.Fatalf("slow reader: get(0) = %v, %v; want block 0", b, ok)
	}
}

// get waits for a block that is not there yet and returns false when the
// log is closed without it.
func TestBlockLogGetWaitsAndEnds(t *testing.T) {
	l := newBlockLog()

	got := make(chan slicer.Block, 1)
	go func() {
		b, ok := l.get(0)
		if ok {
			got <- b
		}
		close(got)
	}()

	select {
	case <-got:
		t.Fatal("get returned before the block was added")
	case <-time.After(50 * time.Millisecond):
	}
	l.add(slicer.Block{Seq: 7})
	select {
	case b := <-got:
		if b.Seq != 7 {
			t.Fatalf("get returned block %d, want 7", b.Seq)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("get did not return after the block was added")
	}

	done := make(chan bool, 1)
	go func() {
		_, ok := l.get(1)
		done <- ok
	}()
	l.close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("get(1) returned a block after close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("get did not return after close")
	}
}
