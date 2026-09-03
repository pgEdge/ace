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

package queries

// DefaultMaxUnionBranches caps the heap relations a UNION ALL table source
// may combine. Measured on PG 17: a left-deep UNION ALL fails past ~8,000
// branches with the default 2 MB max_stack_depth; the balanced form used here
// reaches 65,000 but planning takes over a minute there.
const DefaultMaxUnionBranches = 4000
