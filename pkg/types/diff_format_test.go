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
	"fmt"
	"strings"
	"testing"
)

func TestDiffOutputCheckFormatVersion(t *testing.T) {
	tests := []struct {
		version int
		want    string // part of the error, or "" for no error
	}{
		{0, "run the diff again"}, // written before the field existed
		{1, "run the diff again"},
		{-1, "run the diff again"},
		{DiffFormatVersion, ""},
		{DiffFormatVersion + 1, "unsupported diff report format version"},
	}
	for _, tt := range tests {
		err := DiffOutput{FormatVersion: tt.version}.CheckFormatVersion()
		if tt.want == "" && err != nil {
			t.Errorf("version %d: unexpected error %v", tt.version, err)
		}
		if tt.want != "" && (err == nil || !strings.Contains(err.Error(), tt.want)) {
			t.Errorf("version %d: want an error with %q, got %v", tt.version, tt.want, err)
		}
	}
}

// The field is always written. A report without it reads as version 0, which
// CheckFormatVersion refuses.
func TestDiffOutputFormatVersionJSON(t *testing.T) {
	b, err := json.Marshal(DiffOutput{FormatVersion: DiffFormatVersion})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), fmt.Sprintf(`"format_version":%d`, DiffFormatVersion)) {
		t.Errorf("marshalled report %s has no format_version", b)
	}

	var old DiffOutput
	if err := json.Unmarshal([]byte(`{"diffs":{},"summary":{}}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.FormatVersion != 0 {
		t.Errorf("a report without the field read as version %d, want 0", old.FormatVersion)
	}
	if err := old.CheckFormatVersion(); err == nil {
		t.Error("a report without the field must be refused")
	}
}
