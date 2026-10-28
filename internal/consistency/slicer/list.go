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

import "sync"

// BlockList is the list of the blocks of one run, in the order in which they
// were added. The producer adds blocks and never waits for the readers.
// Every node reads the list through its own Cursor, so a slow node does not
// hold back a fast one: each node has its own position in the list.
//
// The list has no size limit. A block holds only its bounds, so even a large
// table gives a small list.
type BlockList struct {
	mu      sync.Mutex
	cond    *sync.Cond
	blocks  []Block
	closed  bool
	stopped bool
}

// NewBlockList returns an empty open list.
func NewBlockList() *BlockList {
	l := &BlockList{}
	l.cond = sync.NewCond(&l.mu)
	return l
}

// Add appends b and returns its index in the list. After Stop, Add does not
// change the list and returns -1.
func (l *BlockList) Add(b Block) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return -1
	}
	l.blocks = append(l.blocks, b)
	l.cond.Broadcast()
	return len(l.blocks) - 1
}

// Close tells the readers that no more blocks will come. A cursor still
// returns the blocks that it has not read yet.
func (l *BlockList) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.cond.Broadcast()
}

// Stop closes the list and makes every cursor stop at once, also when it has
// not read every block. A run calls it after an error.
func (l *BlockList) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	l.stopped = true
	l.cond.Broadcast()
}

// Len returns the number of blocks in the list.
func (l *BlockList) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.blocks)
}

// Blocks returns the blocks in the order in which they were added. The
// caller must not change the result, and must call Blocks only after Close
// or Stop.
func (l *BlockList) Blocks() []Block {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.blocks
}

// Cursor is the position of one node in a BlockList. Several workers of the
// node can share one cursor: each block goes to only one of them.
type Cursor struct {
	l    *BlockList
	next int
}

// NewCursor returns a cursor at the start of the list.
func (l *BlockList) NewCursor() *Cursor {
	return &Cursor{l: l}
}

// Next waits for the next block of the cursor and returns it with its index
// in the list. ok is false when the list is closed and the cursor has
// returned every block, or when the list is stopped.
func (c *Cursor) Next() (idx int, b Block, ok bool) {
	l := c.l
	l.mu.Lock()
	defer l.mu.Unlock()
	for c.next >= len(l.blocks) && !l.closed {
		l.cond.Wait()
	}
	if l.stopped || c.next >= len(l.blocks) {
		return 0, Block{}, false
	}
	idx = c.next
	c.next++
	return idx, l.blocks[idx], true
}
