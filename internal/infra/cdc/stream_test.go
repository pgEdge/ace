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
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The helpers below build pgoutput messages (protocol version 1) as the
// walsender sends them.

func appendCString(b []byte, s string) []byte {
	return append(append(b, s...), 0)
}

func xlogData(walStart pglogrepl.LSN, data []byte) *pgproto3.CopyData {
	b := []byte{pglogrepl.XLogDataByteID}
	b = binary.BigEndian.AppendUint64(b, uint64(walStart))
	b = binary.BigEndian.AppendUint64(b, uint64(walStart))
	b = binary.BigEndian.AppendUint64(b, 0)
	return &pgproto3.CopyData{Data: append(b, data...)}
}

func keepalive(walEnd pglogrepl.LSN, reply bool) *pgproto3.CopyData {
	b := []byte{pglogrepl.PrimaryKeepaliveMessageByteID}
	b = binary.BigEndian.AppendUint64(b, uint64(walEnd))
	b = binary.BigEndian.AppendUint64(b, 0)
	if reply {
		b = append(b, 1)
	} else {
		b = append(b, 0)
	}
	return &pgproto3.CopyData{Data: b}
}

func beginMsg(xid uint32, finalLSN pglogrepl.LSN) *pgproto3.CopyData {
	b := []byte{'B'}
	b = binary.BigEndian.AppendUint64(b, uint64(finalLSN))
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint32(b, xid)
	return xlogData(finalLSN, b)
}

// commitMsg is the commit of a transaction whose commit record starts at
// commitLSN and ends at endLSN. The walsender sends it with endLSN as the
// start of the XLogData message.
func commitMsg(commitLSN, endLSN pglogrepl.LSN) *pgproto3.CopyData {
	b := []byte{'C', 0}
	b = binary.BigEndian.AppendUint64(b, uint64(commitLSN))
	b = binary.BigEndian.AppendUint64(b, uint64(endLSN))
	b = binary.BigEndian.AppendUint64(b, 0)
	return xlogData(endLSN, b)
}

// relationMsg is a relation with an int4 key column "id" and a text
// column "v".
func relationMsg(relID uint32, name string) *pgproto3.CopyData {
	b := []byte{'R'}
	b = binary.BigEndian.AppendUint32(b, relID)
	b = appendCString(b, "public")
	b = appendCString(b, name)
	b = append(b, 'd')
	b = binary.BigEndian.AppendUint16(b, 2)
	b = append(b, 1)
	b = appendCString(b, "id")
	b = binary.BigEndian.AppendUint32(b, 23)
	b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
	b = append(b, 0)
	b = appendCString(b, "v")
	b = binary.BigEndian.AppendUint32(b, 25)
	b = binary.BigEndian.AppendUint32(b, 0xFFFFFFFF)
	return xlogData(0, b)
}

func tuple(b []byte, id, v string) []byte {
	b = binary.BigEndian.AppendUint16(b, 2)
	for _, s := range []string{id, v} {
		b = append(b, 't')
		b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
		b = append(b, s...)
	}
	return b
}

func insertMsg(relID uint32, at pglogrepl.LSN, id string) *pgproto3.CopyData {
	b := []byte{'I'}
	b = binary.BigEndian.AppendUint32(b, relID)
	b = append(b, 'N')
	return xlogData(at, tuple(b, id, "x"))
}

func updateMsg(relID uint32, at pglogrepl.LSN, id string) *pgproto3.CopyData {
	b := []byte{'U'}
	b = binary.BigEndian.AppendUint32(b, relID)
	b = append(b, 'N')
	return xlogData(at, tuple(b, id, "y"))
}

func truncateMsg(at pglogrepl.LSN, relIDs ...uint32) *pgproto3.CopyData {
	b := []byte{'T'}
	b = binary.BigEndian.AppendUint32(b, uint32(len(relIDs)))
	b = append(b, 0)
	for _, id := range relIDs {
		b = binary.BigEndian.AppendUint32(b, id)
	}
	return xlogData(at, b)
}

// txn returns the messages of one transaction with an INSERT of each id.
func txn(xid uint32, relID uint32, commitLSN, endLSN pglogrepl.LSN, ids ...string) []pgproto3.BackendMessage {
	msgs := []pgproto3.BackendMessage{beginMsg(xid, commitLSN)}
	for _, id := range ids {
		msgs = append(msgs, insertMsg(relID, commitLSN-1, id))
	}
	return append(msgs, commitMsg(commitLSN, endLSN))
}

// fakeConn is a replConn that returns the messages fed to it and records
// the positions of the status updates.
type fakeConn struct {
	msgs     chan pgproto3.BackendMessage
	mu       sync.Mutex
	statuses []pglogrepl.LSN
}

func newFakeConn() *fakeConn {
	return &fakeConn{msgs: make(chan pgproto3.BackendMessage, 1000)}
}

func (c *fakeConn) feed(msgs ...pgproto3.BackendMessage) {
	for _, m := range msgs {
		c.msgs <- m
	}
}

// lose makes the connection fail after the messages fed so far.
func (c *fakeConn) lose() {
	close(c.msgs)
}

func (c *fakeConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	select {
	case m, ok := <-c.msgs:
		if !ok {
			return nil, errors.New("connection lost")
		}
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *fakeConn) SendStatus(_ context.Context, pos pglogrepl.LSN) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statuses = append(c.statuses, pos)
	return nil
}

func (c *fakeConn) Close(context.Context) error {
	return nil
}

func (c *fakeConn) sentStatuses() []pglogrepl.LSN {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]pglogrepl.LSN(nil), c.statuses...)
}

// harness runs a stream on fake connections, which connect returns in
// order, and records the applied items.
type harness struct {
	s      *stream
	mu     sync.Mutex
	starts []pglogrepl.LSN
	items  []applyItem
	// hook, if set, runs in the applier for each item before it is
	// recorded.
	hook func(applyItem) error
}

func newHarness(continuous bool, target pglogrepl.LSN, interval time.Duration, conns ...*fakeConn) *harness {
	h := &harness{}
	h.s = &stream{
		continuous: continuous,
		target:     target,
		batchSize:  1000,
		quiet:      20 * time.Millisecond,
		retryDelay: 10 * time.Millisecond,
		connect: func(_ context.Context, start pglogrepl.LSN) (replConn, time.Duration, error) {
			h.mu.Lock()
			defer h.mu.Unlock()
			n := len(h.starts)
			h.starts = append(h.starts, start)
			if n >= len(conns) {
				return nil, 0, errors.New("no more connections")
			}
			return conns[n], interval, nil
		},
		loadKey: func(*pglogrepl.RelationMessage) (*tableKey, error) {
			return &tableKey{pos: []int{0}, castTypes: []string{"int4"}}, nil
		},
		apply: func(item applyItem) error {
			if h.hook != nil {
				if err := h.hook(item); err != nil {
					return err
				}
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			h.items = append(h.items, item)
			return nil
		},
	}
	return h
}

type runResult struct {
	reached bool
	err     error
}

func (h *harness) start(ctx context.Context, start pglogrepl.LSN) <-chan runResult {
	res := make(chan runResult, 1)
	go func() {
		reached, err := h.s.run(ctx, start)
		res <- runResult{reached, err}
	}()
	return res
}

func (h *harness) appliedItems() []applyItem {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]applyItem(nil), h.items...)
}

func (h *harness) connectStarts() []pglogrepl.LSN {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]pglogrepl.LSN(nil), h.starts...)
}

func wait(t *testing.T, res <-chan runResult) runResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end")
		return runResult{}
	}
}

func TestStreamSendsStatusWhileApplyWaits(t *testing.T) {
	gate := make(chan struct{})
	c := newFakeConn()
	h := newHarness(true, 0, 10*time.Millisecond, c)
	h.hook = func(item applyItem) error {
		if item.end == 0x100 {
			<-gate
		}
		return nil
	}
	c.feed(relationMsg(1, "t"))
	c.feed(txn(1, 1, 0xF0, 0x100, "1")...)
	c.feed(txn(2, 1, 0x1F0, 0x200, "2")...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := h.start(ctx, 0x10)

	// The apply of the first transaction waits. The stream keeps sending
	// status updates, and they do not confirm the waiting transaction.
	time.Sleep(200 * time.Millisecond)
	st := c.sentStatuses()
	require.GreaterOrEqual(t, len(st), 5, "status updates while the apply waits")
	for _, pos := range st {
		require.Equal(t, pglogrepl.LSN(0x10), pos)
	}

	close(gate)
	require.Eventually(t, func() bool {
		st := c.sentStatuses()
		return st[len(st)-1] == 0x200
	}, 2*time.Second, 5*time.Millisecond)
	cancel()
	r := wait(t, res)
	require.ErrorIs(t, r.err, context.Canceled)

	// The confirmed positions only grow, and each is the end of a commit
	// record or the start position.
	st = c.sentStatuses()
	for i, pos := range st {
		require.Contains(t, []pglogrepl.LSN{0x10, 0x100, 0x200}, pos)
		if i > 0 {
			require.GreaterOrEqual(t, pos, st[i-1])
		}
	}
}

func TestStreamRepliesToKeepaliveWhileApplyWaits(t *testing.T) {
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	c := newFakeConn()
	h := newHarness(true, 0, time.Hour, c)
	h.hook = func(applyItem) error {
		<-gate
		return nil
	}
	c.feed(relationMsg(1, "t"))
	c.feed(txn(1, 1, 0xF0, 0x100, "1")...)
	c.feed(keepalive(0x100, true))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := h.start(ctx, 0x10)

	require.Eventually(t, func() bool { return len(c.sentStatuses()) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, pglogrepl.LSN(0x10), c.sentStatuses()[0], "the reply does not confirm the waiting transaction")

	close(gate)
	cancel()
	wait(t, res)
}

func TestStreamBoundedStopsAtCommitAfterTarget(t *testing.T) {
	c := newFakeConn()
	h := newHarness(false, 0x150, time.Hour, c)
	c.feed(relationMsg(1, "t"))
	c.feed(txn(1, 1, 0xF0, 0x100, "1")...)
	c.feed(txn(2, 1, 0x1F0, 0x200, "2")...)
	c.feed(txn(3, 1, 0x2F0, 0x300, "3")...)

	r := wait(t, h.start(context.Background(), 0x10))
	require.NoError(t, r.err)
	require.True(t, r.reached)
	require.Equal(t, pglogrepl.LSN(0x200), h.s.appliedLSN())

	items := h.appliedItems()
	require.Len(t, items, 2)
	assert.Equal(t, pglogrepl.LSN(0x100), items[0].end)
	assert.Equal(t, pglogrepl.LSN(0x200), items[1].end)
	st := c.sentStatuses()
	assert.Equal(t, pglogrepl.LSN(0x200), st[len(st)-1])
}

func TestStreamBoundedStopsOnKeepalive(t *testing.T) {
	c := newFakeConn()
	h := newHarness(false, 0x150, time.Hour, c)
	c.feed(keepalive(0x100, false), keepalive(0x150, false))

	r := wait(t, h.start(context.Background(), 0x10))
	require.NoError(t, r.err)
	require.True(t, r.reached)
	require.Equal(t, pglogrepl.LSN(0x150), h.s.appliedLSN())
	items := h.appliedItems()
	require.Len(t, items, 1)
	assert.Empty(t, items[0].changes)
}

func TestStreamBoundedDoesNotStopInsideTransaction(t *testing.T) {
	c := newFakeConn()
	h := newHarness(false, 0x150, time.Hour, c)
	c.feed(relationMsg(1, "t"), beginMsg(1, 0x1F0), insertMsg(1, 0x120, "1"), keepalive(0x300, false))

	res := h.start(context.Background(), 0x10)
	// A quiet interval inside the transaction does not end the drain.
	time.Sleep(100 * time.Millisecond)
	require.Empty(t, h.appliedItems())
	c.feed(commitMsg(0x1F0, 0x200))

	r := wait(t, res)
	require.NoError(t, r.err)
	require.True(t, r.reached)
	require.Equal(t, pglogrepl.LSN(0x200), h.s.appliedLSN())
	items := h.appliedItems()
	require.Len(t, items, 1)
	assert.Len(t, items[0].changes, 1)
}

func TestStreamReconnectAppliesTransactionOnce(t *testing.T) {
	c1, c2 := newFakeConn(), newFakeConn()
	h := newHarness(true, 0, time.Hour, c1, c2)
	h.hook = func(item applyItem) error {
		if item.end == 0x100 {
			time.Sleep(100 * time.Millisecond)
		}
		return nil
	}
	// The first connection is lost inside the second transaction, while
	// the first one is still in the queue or being applied.
	c1.feed(relationMsg(1, "t"))
	c1.feed(txn(1, 1, 0xF0, 0x100, "1")...)
	c1.feed(beginMsg(2, 0x1F0), insertMsg(1, 0x150, "2"))
	c1.lose()
	// The server sends the second transaction again from its start.
	c2.feed(relationMsg(1, "t"))
	c2.feed(txn(2, 1, 0x1F0, 0x200, "2")...)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := h.start(ctx, 0x10)
	require.Eventually(t, func() bool { return h.s.appliedLSN() == 0x200 }, 2*time.Second, 5*time.Millisecond)
	cancel()
	wait(t, res)

	// The stream connects again only after the first transaction is applied,
	// so the server does not send it again.
	assert.Equal(t, []pglogrepl.LSN{0x10, 0x100}, h.connectStarts())
	var ends []pglogrepl.LSN
	changes := 0
	for _, item := range h.appliedItems() {
		ends = append(ends, item.end)
		changes += len(item.changes)
	}
	assert.Equal(t, []pglogrepl.LSN{0x100, 0x200}, ends)
	assert.Equal(t, 2, changes)
}

func TestStreamBatchesLargeTransaction(t *testing.T) {
	c := newFakeConn()
	h := newHarness(false, 0x100, time.Hour, c)
	h.s.batchSize = 2
	c.feed(relationMsg(1, "t"))
	c.feed(txn(1, 1, 0xF0, 0x100, "1", "2", "3", "4", "5")...)

	r := wait(t, h.start(context.Background(), 0x10))
	require.NoError(t, r.err)
	items := h.appliedItems()
	require.Len(t, items, 3)
	for i, want := range []int{2, 2, 1} {
		assert.Len(t, items[i].changes, want)
	}
	assert.Equal(t, []pglogrepl.LSN{0, 0, 0x100}, []pglogrepl.LSN{items[0].end, items[1].end, items[2].end})
}

func TestStreamTruncateMarksAllInApplier(t *testing.T) {
	c := newFakeConn()
	h := newHarness(false, 0x100, time.Hour, c)
	c.feed(relationMsg(1, "t"), beginMsg(1, 0xF0), insertMsg(1, 0x20, "1"), truncateMsg(0x30, 1), insertMsg(1, 0x40, "2"), commitMsg(0xF0, 0x100))

	r := wait(t, h.start(context.Background(), 0x10))
	require.NoError(t, r.err)
	items := h.appliedItems()
	require.Len(t, items, 2)
	assert.Equal(t, []tableRef{{schema: "public", table: "t"}}, items[0].markAll)
	require.Len(t, items[0].changes, 1)
	assert.Equal(t, []string{"1"}, items[0].changes[0].key)
	assert.Empty(t, items[1].markAll)
	require.Len(t, items[1].changes, 1)
	assert.Equal(t, []string{"2"}, items[1].changes[0].key)
	assert.Equal(t, pglogrepl.LSN(0x100), items[1].end)
}

func TestStreamEscalationMarksAllInApplier(t *testing.T) {
	c := newFakeConn()
	h := newHarness(false, 0x100, time.Hour, c)
	h.s.esc = newEscalator(2, 0, fixedEstimate(0))
	c.feed(relationMsg(1, "t"), beginMsg(1, 0xF0),
		updateMsg(1, 0x20, "1"), insertMsg(1, 0x30, "2"), updateMsg(1, 0x40, "3"), updateMsg(1, 0x50, "4"),
		commitMsg(0xF0, 0x100))

	r := wait(t, h.start(context.Background(), 0x10))
	require.NoError(t, r.err)
	items := h.appliedItems()
	require.Len(t, items, 2)
	// The UPDATE before the mark is dropped, the INSERT is kept, and the
	// UPDATE after the mark is not tracked.
	assert.Equal(t, []tableRef{{schema: "public", table: "t"}}, items[0].markAll)
	require.Len(t, items[0].changes, 1)
	assert.Equal(t, "INSERT", items[0].changes[0].operation)
	assert.Empty(t, items[1].changes)
	assert.Equal(t, pglogrepl.LSN(0x100), items[1].end)
}

func TestStreamApplyErrorStopsStream(t *testing.T) {
	c := newFakeConn()
	h := newHarness(true, 0, time.Hour, c)
	applyErr := errors.New("apply failed")
	h.hook = func(applyItem) error { return applyErr }
	c.feed(relationMsg(1, "t"))
	c.feed(txn(1, 1, 0xF0, 0x100, "1")...)
	c.feed(txn(2, 1, 0x1F0, 0x200, "2")...)

	r := wait(t, h.start(context.Background(), 0x10))
	require.ErrorIs(t, r.err, applyErr)
	assert.Equal(t, pglogrepl.LSN(0x10), h.s.appliedLSN())
	for _, pos := range c.sentStatuses() {
		assert.Equal(t, pglogrepl.LSN(0x10), pos)
	}
}

func TestParseGUCDuration(t *testing.T) {
	for text, want := range map[string]time.Duration{
		"0":      0,
		"1min":   time.Minute,
		"60s":    time.Minute,
		"1500ms": 1500 * time.Millisecond,
		"500":    500 * time.Millisecond,
		"2h":     2 * time.Hour,
		"1d":     24 * time.Hour,
	} {
		got, err := parseGUCDuration(text)
		require.NoError(t, err, text)
		assert.Equal(t, want, got, text)
	}
	for _, text := range []string{"", "1 week", "-1", "abc"} {
		_, err := parseGUCDuration(text)
		assert.Error(t, err, text)
	}
}

func TestStatusInterval(t *testing.T) {
	assert.Equal(t, maxStatusInterval, statusInterval(0))
	assert.Equal(t, maxStatusInterval, statusInterval(time.Minute))
	assert.Equal(t, time.Second, statusInterval(2*time.Second))
	assert.Equal(t, 5*time.Second, statusInterval(10*time.Second))
}
