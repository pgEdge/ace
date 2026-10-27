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

package slicer

import (
	"sync"
	"testing"
	"time"
)

// Two cursors do not depend on each other: one cursor can read every block
// while the other has read none. This is what lets a fast node hash ahead of
// a slow one.
func TestBlockListCursorsAreIndependent(t *testing.T) {
	l := NewBlockList()
	fast, slow := l.NewCursor(), l.NewCursor()
	for i := 0; i < 100; i++ {
		if got := l.Add(Block{Seq: i}); got != i {
			t.Fatalf("Add returned index %d, want %d", got, i)
		}
	}
	for i := 0; i < 100; i++ {
		idx, b, ok := fast.Next()
		if !ok || idx != i || b.Seq != i {
			t.Fatalf("fast cursor: Next = %d, %v, %v; want block %d", idx, b, ok, i)
		}
	}
	idx, b, ok := slow.Next()
	if !ok || idx != 0 || b.Seq != 0 {
		t.Fatalf("slow cursor: Next = %d, %v, %v; want block 0", idx, b, ok)
	}
}

// Next waits for a block that is not there yet, and returns false when the
// list is closed without it.
func TestBlockListNextWaitsAndEnds(t *testing.T) {
	l := NewBlockList()
	c := l.NewCursor()

	got := make(chan Block, 1)
	go func() {
		_, b, ok := c.Next()
		if ok {
			got <- b
		}
		close(got)
	}()

	select {
	case <-got:
		t.Fatal("Next returned before the block was added")
	case <-time.After(50 * time.Millisecond):
	}
	l.Add(Block{Seq: 7})
	select {
	case b := <-got:
		if b.Seq != 7 {
			t.Fatalf("Next returned block %d, want 7", b.Seq)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not return after the block was added")
	}

	done := make(chan bool, 1)
	go func() {
		_, _, ok := c.Next()
		done <- ok
	}()
	l.Close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("Next returned a block after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not return after Close")
	}
}

// After Close a cursor still returns the blocks that it has not read. After
// Stop it returns none, and Add does not change the list.
func TestBlockListCloseAndStop(t *testing.T) {
	l := NewBlockList()
	c := l.NewCursor()
	l.Add(Block{Seq: 0})
	l.Add(Block{Seq: 1})
	l.Close()
	for i := 0; i < 2; i++ {
		if _, b, ok := c.Next(); !ok || b.Seq != i {
			t.Fatalf("Next after Close = %v, %v; want block %d", b, ok, i)
		}
	}
	if _, _, ok := c.Next(); ok {
		t.Fatal("Next returned a block after the end of a closed list")
	}

	l = NewBlockList()
	c = l.NewCursor()
	l.Add(Block{Seq: 0})
	l.Stop()
	if _, _, ok := c.Next(); ok {
		t.Fatal("Next returned a block after Stop")
	}
	if got := l.Add(Block{Seq: 1}); got != -1 {
		t.Fatalf("Add after Stop returned %d, want -1", got)
	}
	if l.Len() != 1 {
		t.Fatalf("Len after Stop = %d, want 1", l.Len())
	}
}

// Workers that share one cursor get every block exactly once.
func TestBlockListSharedCursor(t *testing.T) {
	const blocks, workers = 1000, 8
	l := NewBlockList()
	c := l.NewCursor()

	var (
		mu   sync.Mutex
		seen = make(map[int]int)
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx, b, ok := c.Next()
				if !ok {
					return
				}
				if b.Seq != idx {
					t.Errorf("block at index %d has Seq %d", idx, b.Seq)
				}
				mu.Lock()
				seen[idx]++
				mu.Unlock()
			}
		}()
	}
	for i := 0; i < blocks; i++ {
		l.Add(Block{Seq: i})
	}
	l.Close()
	wg.Wait()

	if len(seen) != blocks {
		t.Fatalf("workers read %d different blocks, want %d", len(seen), blocks)
	}
	for idx, n := range seen {
		if n != 1 {
			t.Fatalf("block %d was read %d times", idx, n)
		}
	}
}
