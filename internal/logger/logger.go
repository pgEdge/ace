package logger

import (
	"os"

	"github.com/charmbracelet/log"
)

var (
	// Log is the global logger.
	Log = log.NewWithOptions(os.Stderr, log.Options{
		ReportTimestamp: true,
	})
)

// SetLevel sets the log level for the global logger.
func SetLevel(level log.Level) {
	Log.SetLevel(level)
}

// Info logs a formatted string at the info level.
func Info(format string, args ...any) {
	Log.Infof(format, args...)
}

// Debug logs a formatted string at the debug level.
func Debug(format string, args ...any) {
	Log.Debugf(format, args...)
}

// Error logs a formatted string at the error level.
func Error(format string, args ...any) {
	Log.Errorf(format, args...)
}
