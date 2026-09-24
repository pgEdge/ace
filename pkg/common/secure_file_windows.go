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

//go:build windows

package common

// ACE does not support Windows. The helpers in secure_file.go protect
// output files with POSIX mode bits, and Windows does not use them for
// access control: other local users could read reports that contain
// table rows. This reference to an undefined name stops the build on
// Windows, so nobody gets an unsafe binary by accident.
var _ = ACE_does_not_support_Windows
