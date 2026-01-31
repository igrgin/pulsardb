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

func (h *prettyHandler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}

	var buf bytes.Buffer
	ts := time.Now().Format("2006-01-02 15:04:05.000")
	_, _ = fmt.Fprintf(&buf, "%s ", ts)

	level := levelToUpper(r.Level)
	color := colorForLevel(r.Level)
	reset := "\033[0m"
	_, _ = fmt.Fprintf(&buf, "%s%-5s%s ", color, level, reset)

	if h.source {
		if file, line, topPkg := resolveCaller(); file != "" {
			loc := fmt.Sprintf("%s:%d", filepath.Base(file), line)
			pkgName := filepath.Base(filepath.Dir(file))
			if pkgName == "." || pkgName == "" {
				pkgName = topPkg
			}
			// Increase widths so package and location have more space between them
			_, _ = fmt.Fprintf(&buf, "%-16s %-22s ", pkgName, loc)
		}
	}

	buf.WriteString(r.Message)

	r.Attrs(func(a slog.Attr) bool {
		_, _ = fmt.Fprintf(&buf, " %s=%v", a.Key, a.Value.Any())
		return true
	})

	buf.WriteByte('\n')
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
		return "\033[36m"
	case l == slog.LevelInfo:
		return "\033[32m"
	case l == slog.LevelWarn:
		return "\033[33m"
	default:
		return "\033[31m"
	}
}

func resolveCaller() (string, int, string) {
	const maxDepth = 32
	var pcs [maxDepth]uintptr

	n := runtime.Callers(4, pcs[:])
	frames := runtime.CallersFrames(pcs[:n])

	for {
		f, more := frames.Next()
		if !more {
			break
		}

		if strings.Contains(f.File, "logger") {
			continue
		}

		pkg := extractPackageName(f.Function)
		return f.File, f.Line, pkg
	}

	return "", 0, ""
}

func extractPackageName(fullFunc string) string {
	if fullFunc == "" {
		return ""
	}

	lastSlash := strings.LastIndex(fullFunc, "/")
	if lastSlash == -1 {

		if dot := strings.Index(fullFunc, "."); dot != -1 {
			return fullFunc[:dot]
		}
		return fullFunc
	}

	pathAndFunc := fullFunc[:lastSlash]
	if idx := strings.LastIndex(pathAndFunc, "/"); idx != -1 {
		return pathAndFunc[idx+1:]
	}
	return pathAndFunc
}
