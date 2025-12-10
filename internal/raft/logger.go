package raft

import (
	"fmt"
	"log/slog"
	"os"
)

type Logger struct {
	l *slog.Logger
}

func NewSlogRaftLogger() *Logger {
	// Force raft logger to debug level, independent of global level.
	// If your global logger is already set on slog.Default(), you can just use it.
	return &Logger{
		l: slog.Default().WithGroup("raft"),
	}
}

func (l *Logger) Debug(v ...interface{}) {
	l.l.Debug(fmt.Sprint(v...))
}

func (l *Logger) Info(v ...interface{}) {
	l.l.Info(fmt.Sprint(v...))
}

func (l *Logger) Warning(v ...interface{}) {
	l.l.Warn(fmt.Sprint(v...))
}

func (l *Logger) Error(v ...interface{}) {
	l.l.Error(fmt.Sprint(v...))
}

func (l *Logger) Fatal(v ...interface{}) {
	// etcd/raft expects this to terminate the process
	l.l.Error(fmt.Sprint(v...))
	os.Exit(1)
}

func (l *Logger) Panic(v ...interface{}) {
	l.l.Error(fmt.Sprint(v...))
	panic(fmt.Sprint(v...))
}

func (l *Logger) Debugf(format string, v ...interface{}) {
	l.l.Debug(fmt.Sprintf(format, v...))
}

func (l *Logger) Infof(format string, v ...interface{}) {
	l.l.Info(fmt.Sprintf(format, v...))
}

func (l *Logger) Warningf(format string, v ...interface{}) {
	l.l.Warn(fmt.Sprintf(format, v...))
}

func (l *Logger) Errorf(format string, v ...interface{}) {
	l.l.Error(fmt.Sprintf(format, v...))
}

func (l *Logger) Fatalf(format string, v ...interface{}) {
	l.l.Error(fmt.Sprintf(format, v...))
	os.Exit(1)
}

func (l *Logger) Panicf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	l.l.Error(msg)
	panic(msg)
}
