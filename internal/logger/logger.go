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
