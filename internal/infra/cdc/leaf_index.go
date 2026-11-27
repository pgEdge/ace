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
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgedge/ace/db/queries"
	"github.com/pgedge/ace/pkg/logger"
)

// leafIndexLockTimeout bounds the wait for the lock on a tree table that a
// long tree update holds. The index is then left for a later try.
const leafIndexLockTimeout = time.Second

// ensureLeafIndexes gives every Merkle tree of the node the leaf index that
// the CDC lookup of a key needs. A tree built by an older version has no such
// index, and without it each applied transaction reads all leaves of the
// tree. A drain applies the changes of every table in the publication, so all
// trees are checked, not only the tree of the command. A failure here does not
// stop the drain: the lookup gives the same leaves without the index, only
// slower. It reports whether a tree was left without the index because its
// table was in use, so that a long drain can try again.
func ensureLeafIndexes(ctx context.Context, pool *pgxpool.Pool, mtreeSchema string) (busy bool) {
	trees, err := queries.ListTreesWithoutLeafIndex(ctx, pool, mtreeSchema)
	if err != nil {
		if ctx.Err() == nil {
			logger.Warn("cannot check the leaf indexes of the Merkle trees: %v", err)
		}
		return false
	}
	for _, tree := range trees {
		mtreeTable := pgx.Identifier{mtreeSchema, fmt.Sprintf("ace_mtree_%s_%s", tree.Schema, tree.Table)}.Sanitize()
		created, err := queries.EnsureMtreeLeafIndex(ctx, pool, mtreeTable, leafIndexLockTimeout)
		if ctx.Err() != nil {
			return false
		}
		if err == nil {
			if created {
				logger.Info("created the leaf index of Merkle tree %s", mtreeTable)
			}
			continue
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
			logger.Info("Merkle tree %s is in use; its leaf index is left for a later try", mtreeTable)
			busy = true
			continue
		}
		ddl, ddlErr := queries.MtreeLeafIndexDDL(mtreeTable)
		if ddlErr != nil {
			ddl = "(" + ddlErr.Error() + ")"
		}
		logger.Warn("cannot create the leaf index of Merkle tree %s, so CDC on this tree reads all its leaves for each transaction: %v. The owner of the table can create it with: %s", mtreeTable, err, ddl)
	}
	return busy
}

// startLeafIndexes gives the trees of the node their leaf index before a
// drain starts to apply changes. A continuous drain holds the slot, so no
// other drain can create the index of a tree that is busy now; for it the
// tries go on in the background. The returned function stops them; call it
// before the pool is closed.
func startLeafIndexes(ctx context.Context, pool *pgxpool.Pool, mtreeSchema string, continuous bool, interval time.Duration) (stop func()) {
	if !ensureLeafIndexes(ctx, pool, mtreeSchema) || !continuous {
		return func() {}
	}
	return retryLeafIndexesInBackground(ctx, pool, mtreeSchema, interval)
}

// retryLeafIndexesInBackground tries again every interval to give the busy
// trees their leaf index, until no tree is busy, in a goroutine beside the
// replication receive loop: a busy tree must not delay the reading of the
// stream or the replies to the server. The returned function stops the
// goroutine and waits for it; call it before the pool is closed.
func retryLeafIndexesInBackground(ctx context.Context, pool *pgxpool.Pool, mtreeSchema string, interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		retryUntilDone(ctx, interval, func() bool {
			return ensureLeafIndexes(ctx, pool, mtreeSchema)
		})
	}()
	return func() {
		cancel()
		<-done
	}
}

// retryUntilDone calls try every interval while it returns true, until ctx
// ends.
func retryUntilDone(ctx context.Context, interval time.Duration, try func() (again bool)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !try() {
				return
			}
		}
	}
}
