// Package utils provides a thread-safe, broadcast-capable logger for the distributed system.
// Every component uses this logger to emit structured LogEntry events to the dashboard.
package utils

import (
	"fmt"
	"sync"
	"time"

	"distributed-health-system/models"
)

// Logger is a thread-safe logger that writes to stdout and broadcasts
// log entries to a shared channel consumed by the dashboard.
type Logger struct {
	mu          sync.Mutex
	entries     []*models.LogEntry
	maxEntries  int
	subscribers []chan *models.LogEntry
}

// Global singleton logger shared by all packages.
var GlobalLogger = NewLogger(500)

// NewLogger creates a Logger that keeps the last `maxEntries` log lines.
func NewLogger(maxEntries int) *Logger {
	return &Logger{
		maxEntries:  maxEntries,
		subscribers: make([]chan *models.LogEntry, 0),
	}
}

// Subscribe returns a channel that receives all new log entries.
// Used by the dashboard WebSocket hub to forward logs to browsers.
func (l *Logger) Subscribe() chan *models.LogEntry {
	ch := make(chan *models.LogEntry, 100)
	l.mu.Lock()
	l.subscribers = append(l.subscribers, ch)
	l.mu.Unlock()
	return ch
}

// log is the internal emit function.
func (l *Logger) log(level models.LogLevel, source, message string) {
	entry := &models.LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Source:    source,
		Message:   message,
	}

	// Color codes for terminal output.
	colorMap := map[models.LogLevel]string{
		models.LogInfo:    "\033[36m",    // Cyan
		models.LogWarn:    "\033[33m",    // Yellow
		models.LogError:   "\033[31m",    // Red
		models.LogSuccess: "\033[32m",    // Green
		models.LogEvent:   "\033[35m",    // Magenta
	}
	reset := "\033[0m"
	color := colorMap[level]

	fmt.Printf("%s[%s] [%-7s] [%-15s] %s%s\n",
		color,
		entry.Timestamp.Format("15:04:05.000"),
		string(level),
		source,
		message,
		reset,
	)

	l.mu.Lock()
	l.entries = append(l.entries, entry)
	if len(l.entries) > l.maxEntries {
		l.entries = l.entries[len(l.entries)-l.maxEntries:]
	}
	subs := make([]chan *models.LogEntry, len(l.subscribers))
	copy(subs, l.subscribers)
	l.mu.Unlock()

	// Non-blocking broadcast to all subscribers.
	for _, ch := range subs {
		select {
		case ch <- entry:
		default:
		}
	}
}

// Info logs an informational message.
func (l *Logger) Info(source, message string) {
	l.log(models.LogInfo, source, message)
}

// Warn logs a warning message.
func (l *Logger) Warn(source, message string) {
	l.log(models.LogWarn, source, message)
}

// Error logs an error message.
func (l *Logger) Error(source, message string) {
	l.log(models.LogError, source, message)
}

// Success logs a success message.
func (l *Logger) Success(source, message string) {
	l.log(models.LogSuccess, source, message)
}

// Event logs a significant system event (leader election, node failure, etc.).
func (l *Logger) Event(source, message string) {
	l.log(models.LogEvent, source, message)
}

// GetAll returns a copy of all stored log entries.
func (l *Logger) GetAll() []*models.LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]*models.LogEntry, len(l.entries))
	copy(result, l.entries)
	return result
}

// GetRecent returns the last n log entries.
func (l *Logger) GetRecent(n int) []*models.LogEntry {
	all := l.GetAll()
	if len(all) <= n {
		return all
	}
	return all[len(all)-n:]
}
