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

package types

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestDiffOutputCheckFormatVersion(t *testing.T) {
	tests := []struct {
		version int
		ok      bool
	}{
		{0, true}, // written before the field existed
		{1, true},
		{DiffFormatVersion, true},
		{DiffFormatVersion + 1, false},
		{-1, false},
	}
	for _, tt := range tests {
		err := DiffOutput{FormatVersion: tt.version}.CheckFormatVersion()
		if tt.ok && err != nil {
			t.Errorf("version %d: unexpected error %v", tt.version, err)
		}
		if !tt.ok && (err == nil || !strings.Contains(err.Error(), "unsupported diff report format version")) {
			t.Errorf("version %d: want an unsupported-version error, got %v", tt.version, err)
		}
	}
}

// The field is always written, and a report without it reads as version 0,
// which CheckFormatVersion accepts.
func TestDiffOutputFormatVersionJSON(t *testing.T) {
	b, err := json.Marshal(DiffOutput{FormatVersion: DiffFormatVersion})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"format_version":1`) {
		t.Errorf("marshalled report %s has no format_version", b)
	}

	var old DiffOutput
	if err := json.Unmarshal([]byte(`{"diffs":{},"summary":{}}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.FormatVersion != 0 {
		t.Errorf("a report without the field read as version %d, want 0", old.FormatVersion)
	}
	if err := old.CheckFormatVersion(); err != nil {
		t.Errorf("a report without the field must be accepted: %v", err)
	}
}
