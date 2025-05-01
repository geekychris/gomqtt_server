package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// LogLevel defines the severity of log messages
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

// Logger provides a simple logging facility for the MQTT server
type Logger struct {
	level     LogLevel
	logger    *log.Logger
	mu        sync.Mutex
}

// NewLogger creates a new logger instance
func NewLogger(level LogLevel) *Logger {
	return &Logger{
		level:  level,
		logger: log.New(os.Stdout, "", 0),
	}
}

// SetLevel changes the current logging level
func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// Debug logs messages at DEBUG level
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.level <= DEBUG {
		l.log("DEBUG", format, args...)
	}
}

// Info logs messages at INFO level
func (l *Logger) Info(format string, args ...interface{}) {
	if l.level <= INFO {
		l.log("INFO", format, args...)
	}
}

// Warn logs messages at WARN level
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.level <= WARN {
		l.log("WARN", format, args...)
	}
}

// Error logs messages at ERROR level
func (l *Logger) Error(format string, args ...interface{}) {
	if l.level <= ERROR {
		l.log("ERROR", format, args...)
	}
}

// Fatal logs messages at FATAL level and exits the program
func (l *Logger) Fatal(format string, args ...interface{}) {
	if l.level <= FATAL {
		l.log("FATAL", format, args...)
	}
	os.Exit(1)
}

// log handles the actual logging
func (l *Logger) log(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	message := fmt.Sprintf(format, args...)
	l.logger.Printf("[%s] %s: %s", timestamp, level, message)
}
