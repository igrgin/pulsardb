package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

func NewLogger(minLevel string) *slog.Logger {
	level, err := ParseLevel(minLevel)
	if err != nil {
		slog.Error("failed to parse log level:", err)
		os.Exit(-1)
	}

	h := NewPlainHandler(os.Stdout, level)
	return slog.New(h)
}

type PlainHandler struct {
	mu        sync.Mutex
	w         io.Writer
	minLevel  slog.Leveler
	withAttrs []slog.Attr
}

func NewPlainHandler(w io.Writer, minLevel slog.Leveler) *PlainHandler {
	return &PlainHandler{
		w:        w,
		minLevel: minLevel,
	}
}

func (h *PlainHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	if h.minLevel == nil {
		return true
	}
	return lvl >= h.minLevel.Level()
}

func (h *PlainHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	ts := r.Time.Format("2006-01-02 15:04:05.000")

	levelText := strings.ToUpper(r.Level.String())
	levelText = colorLevel(levelText)

	src := ""
	if s := r.Source(); s != nil {
		file := filepath.Base(s.File)
		src = fmt.Sprintf("%s:%d", file, s.Line)
	}

	msg := r.Message

	line := fmt.Sprintf("%s %s %s %s\n", ts, levelText, src, msg)
	_, err := io.WriteString(h.w, line)
	return err
}

func (h *PlainHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	cp := *h
	cp.withAttrs = append(cp.withAttrs, attrs...)
	return &cp
}

func (h *PlainHandler) WithGroup(name string) slog.Handler {
	return h
}

func colorLevel(level string) string {
	const reset = "\x1b[0m"

	switch level {
	case "DEBUG":
		return "\x1b[36m" + level + reset
	case "INFO":
		return "\x1b[32m" + level + reset
	case "WARN":
		return "\x1b[33m" + level + reset
	case "ERROR":
		return "\x1b[31m" + level + reset
	default:
		return level
	}
}

func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return slog.LevelDebug, nil
	case "INFO":
		return slog.LevelInfo, nil
	case "WARN", "WARNING":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, errors.New("unknown log level")
	}
}
