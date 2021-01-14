package parse

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/fatih/color"
)

const (
	// PeriodicFileLoggerDelimiter is the log file delimiter
	PeriodicFileLoggerDelimiter = "\n"
)

// PeriodicFileLogger ..
type PeriodicFileLogger struct {
	period      time.Duration
	buffer      []string
	bufferMutex sync.Mutex
	filePath    string
	done        chan bool
}

// Log a JSON marshallable struct to one line on a file (periodically)
func (l *PeriodicFileLogger) Log(dat interface{}) error {
	str, err := json.Marshal(dat)
	if err != nil {
		return err
	}

	l.bufferMutex.Lock()
	defer l.bufferMutex.Unlock()
	l.buffer = append(l.buffer, string(str))
	return nil
}

// StopFileLogger ..
func (l *PeriodicFileLogger) StopFileLogger() {
	l.done <- true
}

// StartFileLogger ..
func (l *PeriodicFileLogger) StartFileLogger(ctx context.Context) {
	go func() {
		tc := time.NewTicker(l.period)
		defer tc.Stop()
		for {
			select {
			case <-ctx.Done():
				l.write()
				return
			case <-tc.C:
				l.write()
				return
			case <-l.done:
				l.write()
				return
			}
		}
	}()
}

// write is a thread safe call to write the buffer to the file
func (l *PeriodicFileLogger) write() {
	l.bufferMutex.Lock()
	defer l.bufferMutex.Unlock()

	f, err := os.OpenFile(l.filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf(err.Error())
	} else {
		for _, str := range l.buffer {
			if _, err := f.WriteString(str + PeriodicFileLoggerDelimiter); err != nil {
				fmt.Printf(err.Error())
				break
			}
		}
		if len(l.buffer) > 0 {
			color.Blue("wrote %v new entries to log file at %v", len(l.buffer), l.filePath)
		}
	}
	l.buffer = nil
}

// NewPeriodicFileLogger ..
func NewPeriodicFileLogger(filePath string, period time.Duration) *PeriodicFileLogger {
	lgr := &PeriodicFileLogger{
		period:   period,
		buffer:   []string{},
		filePath: filePath,
		done:     make(chan bool, 1),
	}
	return lgr
}
