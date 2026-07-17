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

package repair

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// commit_ts is carried in the spock metadata map (_spock_metadata_), which the
// executor keeps in n1Meta/n2Meta while stripping it from n1Row/n2Row. These
// tests ensure pick_freshest's freshestSide resolves the key from metadata too,
// so `key: commit_ts` actually compares commit timestamps instead of always
// falling through to the tie node.
func TestFreshestSide_CommitTsFromMeta(t *testing.T) {
	older := "2026-01-15T12:20:07.805412-08:00"
	newer := "2026-01-15T12:21:39.662530-08:00"

	t.Run("n2 newer", func(t *testing.T) {
		row := planDiffRow{
			n1Row:  map[string]any{"id": 1, "v": "a"},
			n2Row:  map[string]any{"id": 1, "v": "b"},
			n1Meta: map[string]any{"commit_ts": older},
			n2Meta: map[string]any{"commit_ts": newer},
		}
		require.Equal(t, "n2", freshestSide("commit_ts", "n1", row))
	})

	t.Run("n1 newer", func(t *testing.T) {
		row := planDiffRow{
			n1Row:  map[string]any{"id": 1, "v": "a"},
			n2Row:  map[string]any{"id": 1, "v": "b"},
			n1Meta: map[string]any{"commit_ts": newer},
			n2Meta: map[string]any{"commit_ts": older},
		}
		require.Equal(t, "n1", freshestSide("commit_ts", "n2", row))
	})
}

// When only one side has a commit_ts (e.g. the other was written with
// track_commit_timestamp off), the side that has one wins; when neither has
// one, the tie node is chosen.
func TestFreshestSide_CommitTsNullHandling(t *testing.T) {
	ts := "2026-01-15T12:21:39.662530-08:00"

	t.Run("only n2 has commit_ts -> n2", func(t *testing.T) {
		row := planDiffRow{
			n1Meta: map[string]any{},
			n2Meta: map[string]any{"commit_ts": ts},
		}
		require.Equal(t, "n2", freshestSide("commit_ts", "n1", row))
	})

	t.Run("only n1 has commit_ts -> n1", func(t *testing.T) {
		row := planDiffRow{
			n1Meta: map[string]any{"commit_ts": ts},
			n2Meta: map[string]any{},
		}
		require.Equal(t, "n1", freshestSide("commit_ts", "n2", row))
	})

	t.Run("neither has commit_ts -> tie", func(t *testing.T) {
		row := planDiffRow{
			n1Meta: map[string]any{},
			n2Meta: map[string]any{},
		}
		require.Equal(t, "n2", freshestSide("commit_ts", "n2", row))
	})
}

// An application-level timestamp column lives in the row itself and must keep
// working after the metadata fallback is added.
func TestFreshestSide_AppColumnFromRow(t *testing.T) {
	row := planDiffRow{
		n1Row: map[string]any{"updated_at": "2026-01-15T12:00:00-08:00"},
		n2Row: map[string]any{"updated_at": "2026-01-15T13:00:00-08:00"},
	}
	require.Equal(t, "n2", freshestSide("updated_at", "n1", row))
}
