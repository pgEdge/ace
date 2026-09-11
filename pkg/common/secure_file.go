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

package common

import (
	"fmt"
	"os"
)

// ACE writes row data copied straight out of the compared tables: diff JSON,
// HTML reports, repair reports, stale-skip logs. os.Create and os.WriteFile
// only *request* a mode, which the umask then masks off — under the usual 0022
// that lands at 0644, readable by every local user. These helpers set the mode
// explicitly so it does not depend on the operator's umask.
const (
	SecureFileMode os.FileMode = 0o600
	SecureDirMode  os.FileMode = 0o700
)

// CreateFileSecure creates or truncates path for writing, owner-only. The
// Chmod is not redundant: O_CREATE's mode is umask-masked, and ignored
// altogether when the file already exists.
func CreateFileSecure(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, SecureFileMode)
	if err != nil {
		return nil, err
	}
	if err := f.Chmod(SecureFileMode); err != nil {
		f.Close()
		return nil, fmt.Errorf("restrict permissions on %s: %w", path, err)
	}
	return f, nil
}

// WriteFileSecure is os.WriteFile for files that may contain table data.
func WriteFileSecure(path string, data []byte) error {
	f, err := CreateFileSecure(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// MkdirAllSecure creates path and any missing parents, restricting path itself
// to the owner. Only the leaf is tightened, so an existing reports/ stays as
// the operator set it; the files written underneath are owner-only anyway.
func MkdirAllSecure(path string) error {
	if err := os.MkdirAll(path, SecureDirMode); err != nil {
		return err
	}
	if err := os.Chmod(path, SecureDirMode); err != nil {
		return fmt.Errorf("restrict permissions on %s: %w", path, err)
	}
	return nil
}
