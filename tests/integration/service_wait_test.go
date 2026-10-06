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

package integration

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// serviceStartTimeout is how long a test waits for a restarted node, and
// for Spock replication to settle after it.
const serviceStartTimeout = 90 * time.Second

// poolForService returns the shared test pool of a cluster service.
func poolForService(service string) (*pgxpool.Pool, error) {
	switch service {
	case serviceN1:
		return pgCluster.Node1Pool, nil
	case serviceN2:
		return pgCluster.Node2Pool, nil
	case serviceN3:
		return pgCluster.Node3Pool, nil
	}
	return nil, fmt.Errorf("unknown service %q", service)
}

// startServiceAndWait starts the container of service and waits until
// PostgreSQL in it accepts queries.
//
// startService returns as soon as the container runs, but PostgreSQL in it
// still needs a few seconds to start. A test that connects in this window
// gets "connection reset by peer". Before this wait existed, every test that
// ran after TestCatastrophicSingleNodeFailure failed in this way.
func startServiceAndWait(ctx context.Context, service string) error {
	if err := startService(ctx, service); err != nil {
		return err
	}
	return waitForService(ctx, service, serviceStartTimeout)
}

// waitForService waits until the shared pool of service runs a query. A
// failed query also removes a broken connection from the pool, so the
// pool is usable again after a restart.
func waitForService(ctx context.Context, service string, timeout time.Duration) error {
	pool, err := poolForService(service)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		var one int
		err = pool.QueryRow(ctx, "SELECT 1").Scan(&one)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s did not accept queries within %s: %w", service, timeout, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// waitForSpockSettled waits until replication between all three nodes is
// working and has no backlog: every Spock subscription is replicating, and
// on every node each Spock slot has confirmed the WAL position that the node
// had when the wait started.
//
// A test that stops a node or disables a subscription leaves a backlog of
// changes. If the test then drops its table while an apply worker still
// replays changes to it, the apply worker fails, and replication between
// the nodes is broken for the rest of the run.
func waitForSpockSettled(ctx context.Context, timeout time.Duration) error {
	services := []string{serviceN1, serviceN2, serviceN3}
	deadline := time.Now().Add(timeout)

	targets := make(map[string]string, len(services))
	for _, service := range services {
		pool, err := poolForService(service)
		if err != nil {
			return err
		}
		var lsn string
		if err := pool.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&lsn); err != nil {
			return fmt.Errorf("read WAL position on %s: %w", service, err)
		}
		targets[service] = lsn
	}

	for _, service := range services {
		pool, _ := poolForService(service)
		for {
			err := spockSettledOn(ctx, pool, targets[service])
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("replication on %s did not settle within %s: %w", service, timeout, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	return nil
}

// spockSettledOn returns nil if every Spock subscription of the node is
// replicating and every Spock slot of the node has confirmed target.
func spockSettledOn(ctx context.Context, pool *pgxpool.Pool, target string) error {
	var notReplicating int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM spock.sub_show_status() WHERE status <> 'replicating'`,
	).Scan(&notReplicating); err != nil {
		return err
	}
	if notReplicating > 0 {
		return fmt.Errorf("%d subscription(s) are not replicating", notReplicating)
	}

	// The count of all Spock slots guards the filter: if the plugin name
	// did not match, the check would pass with no slot checked.
	var slots, behind int
	if err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE confirmed_flush_lsn IS NULL
		                           OR confirmed_flush_lsn < $1::pg_lsn)
		FROM pg_catalog.pg_replication_slots
		WHERE plugin = 'spock_output'`,
		target,
	).Scan(&slots, &behind); err != nil {
		return err
	}
	if slots == 0 {
		return fmt.Errorf("no Spock replication slots found")
	}
	if behind > 0 {
		return fmt.Errorf("%d Spock slot(s) have not confirmed %s", behind, target)
	}
	return nil
}
