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

package logger

import (
	"fmt"
	"os"

	"github.com/charmbracelet/log"
)

var (
	Log = log.NewWithOptions(os.Stderr, log.Options{
		ReportTimestamp: true,
	})
)

func SetLevel(level log.Level) {
	Log.SetLevel(level)
}

func SetOutput(w *os.File) {
	Log.SetOutput(w)
}

func Info(format string, args ...any) {
	Log.Infof(format, args...)
}

func Debug(format string, args ...any) {
	Log.Debugf(format, args...)
}

func Warn(format string, args ...any) {
	Log.Warnf(format, args...)
}

func Error(format string, args ...any) error {
	Log.Errorf(format, args...)
	return fmt.Errorf(format, args...)
}

func Fatal(msg any, args ...any) {
	Log.Fatal(msg, args...)
}
