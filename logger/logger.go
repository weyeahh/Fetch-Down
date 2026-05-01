package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var (
	defaultLogger *Logger
	levelNames    = map[Level]string{
		LevelDebug: "DEBUG",
		LevelInfo:  "INFO",
		LevelWarn:  "WARN",
		LevelError: "ERROR",
	}
)

type Logger struct {
	level  Level
	logger *log.Logger
}

func Init(levelStr string) {
	defaultLogger = New(levelStr)
}

func New(levelStr string) *Logger {
	level := parseLevel(levelStr)
	return &Logger{
		level:  level,
		logger: log.New(os.Stdout, "", 0),
	}
}

func parseLevel(s string) Level {
	switch strings.ToLower(s) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	default:
		return LevelInfo
	}
}

func (l *Logger) logf(level Level, format string, args ...interface{}) {
	if level < l.level {
		return
	}
	ts := time.Now().Format("2006-01-02 15:04:05")
	prefix := fmt.Sprintf("[%s] [%s] ", ts, levelNames[level])
	l.logger.Printf(prefix+format, args...)
}

func Debug(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.logf(LevelDebug, format, args...)
	}
}

func Info(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.logf(LevelInfo, format, args...)
	}
}

func Warn(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.logf(LevelWarn, format, args...)
	}
}

func Error(format string, args ...interface{}) {
	if defaultLogger != nil {
		defaultLogger.logf(LevelError, format, args...)
	}
}
