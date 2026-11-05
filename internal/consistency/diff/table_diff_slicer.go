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

package diff

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pgedge/ace/internal/consistency/slicer"
	"github.com/pgedge/ace/pkg/config"
	"github.com/pgedge/ace/pkg/types"
)

// configuredSlicer checks the way to cut s and returns its name. An empty s
// takes table_diff.slicer from cfg, so a wrong value in the configuration
// file fails too. cfg can be nil.
func configuredSlicer(s string, cfg *config.Config) (string, error) {
	if strings.TrimSpace(s) == "" && cfg != nil {
		s = cfg.TableDiff.Slicer
	}
	return slicer.ParseMethod(s)
}

const (
	// A cut that lasts at least minSlowCut and leaves the hash workers idle
	// for more than maxHashIdleShare of that time is reported as slower
	// than the hashing.
	minSlowCut       = 10 * time.Second
	maxHashIdleShare = 0.5
)

// slicerSummary turns the choice of the way to cut into the part of the
// diff summary that the report shows.
func slicerSummary(c *slicer.Choice) *types.SlicerSummary {
	if c == nil {
		return nil
	}
	sum := &types.SlicerSummary{
		Requested:     c.Requested,
		Method:        c.Method,
		Reason:        c.Reason,
		SamplePercent: c.SamplePercent,
	}
	if c.SamePage != nil {
		share := *c.SamePage
		sum.SamplePageShare = &share
	}
	if s := c.Signals; s != nil && !s.Partitioned {
		if s.Correlation != nil {
			corr := *s.Correlation
			sum.KeyCorrelation = &corr
		}
		u, _ := s.NotAllVisible()
		sum.NotAllVisible = &u
	}
	return sum
}

// isQueryTimeout reports whether err comes from a query on one block that
// hit a time limit: the block timeout or the statement_timeout of the
// server. The server gives the same error code when an administrator
// cancels the query; then the advice is not needed, but it does no harm.
// The run timeout is not a limit on one block, so it does not count.
func isQueryTimeout(err error) bool {
	if isBlockTimeout(err) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "57014"
}

// sampleReadError is an error of the sample query. The table is not cut
// yet then, so the advice about the blocks of a sample does not apply.
type sampleReadError struct {
	err error
}

// Error returns the text of the error of the sample query.
func (e *sampleReadError) Error() string { return e.err.Error() }

// Unwrap returns the error of the sample query.
func (e *sampleReadError) Unwrap() error { return e.err }

// withSlicerAdvice adds advice to an error of a run that cut the table by a
// sample and then hit a time limit on a block. Such a sample can give very
// large blocks, and the cutter does not have this problem. Other errors are
// returned as they are, nil too.
func (t *TableDiffTask) withSlicerAdvice(err error) error {
	if err == nil || t.slicerChoice == nil || t.slicerChoice.Method != slicer.MethodSample || !isQueryTimeout(err) {
		return err
	}
	var sre *sampleReadError
	if errors.As(err, &sre) {
		return err
	}
	return fmt.Errorf("table-diff stopped: %w; %s", err, t.slicerChoice.TimeoutAdvice())
}

// hashIdleShare returns the share of time that the hash workers waited for
// blocks while the cut was running, and true if the cut was long enough and
// the share large enough to say that the cut was slower than the hashing.
// idle and busy are the sums over the workers of one node of the time that
// they waited for a block and the time that they hashed; cut is the time
// from the first block to the end of the cut.
func hashIdleShare(idle, busy, cut time.Duration) (float64, bool) {
	if cut < minSlowCut || idle < 0 || busy < 0 || idle+busy <= 0 {
		return 0, false
	}
	share := float64(idle) / float64(idle+busy)
	return share, share > maxHashIdleShare
}
