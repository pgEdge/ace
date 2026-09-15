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

// ExitCodeError wraps an error with a specific process exit code, letting a
// caller distinguish how severe a failure was without parsing stdout.
//
// This lives in pkg/common because the value is most naturally constructed
// where the divergence is found (inside internal/consistency/diff), and
// internal/cli already depends on that package.
type ExitCodeError struct {
	Code int
	Err  error
}

// Error returns the wrapped error's message, so that carrying an exit code
// changes nothing about how the failure reads. A value built without a
// wrapped error still has to say something, since a caller is free to print
// it before it looks at the code.
func (e *ExitCodeError) Error() string {
	if e.Err == nil {
		return "exit code error"
	}
	return e.Err.Error()
}

// Unwrap returns the wrapped error, so errors.Is and errors.As see past the
// exit code to the failure underneath it.
func (e *ExitCodeError) Unwrap() error { return e.Err }

// ExitCode is the method main.go looks for (via errors.As) to pick a
// process exit code other than the generic 1 a plain error gets.
func (e *ExitCodeError) ExitCode() int { return e.Code }
