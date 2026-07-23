// Package clog provides a structured log/slog handler and initializer.
package clog

import (
	"log/slog"
	"os"
	"strings"
)

// Init initializes the global logger. When formatStr is "json", it sets up
// a standard JSON handler. Otherwise, it uses the beautified consoleHandler.
func Init(levelStr, formatStr string) {
	var level slog.Level
	switch strings.ToLower(levelStr) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	case "info":
		fallthrough
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: level,
	}

	var baseHandler slog.Handler
	if strings.ToLower(formatStr) == "json" {
		baseHandler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		baseHandler = newConsoleHandler(os.Stdout, opts)
	}

	logger := slog.New(baseHandler)
	slog.SetDefault(logger)
}
