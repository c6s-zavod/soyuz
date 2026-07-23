package clog

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestConsoleHandler(t *testing.T) {
	var buf bytes.Buffer
	handler := newConsoleHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(handler)

	logger.Info("test message", slog.String("key", "value"))

	out := buf.String()
	if !strings.Contains(out, "test message") {
		t.Fatalf("expected log output to contain 'test message', got: %s", out)
	}
	if !strings.Contains(out, "key") || !strings.Contains(out, "value") {
		t.Fatalf("expected log output to contain key and value, got: %s", out)
	}
}
