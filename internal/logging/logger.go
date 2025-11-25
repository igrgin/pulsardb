package logging

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

type prettyHandler struct {
	out    io.Writer
	level  slog.Leveler
	source bool
}

func NewPrettyHandler(out io.Writer, opts *slog.HandlerOptions) slog.Handler {
	if out == nil {
		out = os.Stdout
	}
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &prettyHandler{
		out:    out,
		level:  opts.Level,
		source: opts.AddSource,
	}
}

var logger *slog.Logger

func Init(levelName string) {
	level := parseLogLevel(levelName)

	handler := NewPrettyHandler(os.Stdout, &slog.HandlerOptions{
		Level:     level,
		AddSource: true,
	})

	logger = slog.New(handler)
	slog.SetDefault(logger)
}

func (h *prettyHandler) Enabled(_ context.Context, lvl slog.Level) bool {
	if h.level == nil {
		return true
	}
	return lvl >= h.level.Level()
}

func (h *prettyHandler) Handle(_ context.Context, r slog.Record) error {
	if !h.Enabled(nil, r.Level) {
		return nil
	}

	var buf bytes.Buffer

	// time: fixed layout, always same width
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	fmt.Fprintf(&buf, "%s ", ts)

	// level: 5 chars, colorized, then single space
	level := levelToUpper(r.Level)
	color := colorForLevel(r.Level)
	reset := "\033[0m"
	fmt.Fprintf(&buf, "%s%-5s%s ", color, level, reset)

	// file:line: left-padded/truncated to a fixed width, then space
	if h.source {
		if file, line := resolveCaller(); file != "" {
			loc := fmt.Sprintf("%s:%d", filepath.Base(file), line)
			fmt.Fprintf(&buf, "%-25s ", loc)
		}
	}

	// message
	buf.WriteString(r.Message)

	// attributes key=value
	var errVal error
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "error" {
			if e, ok := a.Value.Any().(error); ok {
				errVal = e
				fmt.Fprintf(&buf, " %s=%v", a.Key, e)
				return true
			}
		}
		fmt.Fprintf(&buf, " %s=%v", a.Key, a.Value.Any())
		return true
	})

	buf.WriteByte('\n')

	// error + stack trace
	if errVal != nil {
		fmt.Fprintf(&buf, "ERROR: %v\n", errVal)
		buf.Write(debug.Stack())
		buf.WriteByte('\n')
	}

	_, err := h.out.Write(buf.Bytes())
	return err
}

func (h *prettyHandler) WithAttrs(_ []slog.Attr) slog.Handler { return h }
func (h *prettyHandler) WithGroup(_ string) slog.Handler      { return h }

func levelToUpper(l slog.Level) string {
	switch {
	case l <= slog.LevelDebug:
		return "DEBUG"
	case l == slog.LevelInfo:
		return "INFO"
	case l == slog.LevelWarn:
		return "WARN"
	default:
		return "ERROR"
	}
}

func parseLogLevel(l string) slog.Level {
	switch strings.ToLower(l) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func colorForLevel(l slog.Level) string {
	switch {
	case l <= slog.LevelDebug:
		return "\033[36m" // cyan
	case l == slog.LevelInfo:
		return "\033[32m" // green
	case l == slog.LevelWarn:
		return "\033[33m" // yellow
	default:
		return "\033[31m" // red
	}
}

// resolveCaller walks the stack and returns the first frame outside `internal/logging`.
func resolveCaller() (string, int) {
	const maxDepth = 32
	var pcs [maxDepth]uintptr

	n := runtime.Callers(5, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])

	for {
		f, more := frames.Next()
		if !more {
			break
		}

		if strings.Contains(
			f.File,
			string(os.PathSeparator)+"internal"+string(os.PathSeparator)+"logging"+string(os.PathSeparator),
		) {
			continue
		}

		return f.File, f.Line
	}

	return "", 0
}
