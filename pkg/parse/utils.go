package parse

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/fatih/color"
)

const (
	// PeriodicFileLoggerDelimiter is the log file delimiter
	PeriodicFileLoggerDelimiter = "\n"
	PeriodicFileChangeValue     = 2.4e+7 // ~25MB
)

// PeriodicFileLogger ..
type PeriodicFileLogger struct {
	period       time.Duration
	buffer       []string
	bufferMutex  sync.Mutex
	filePath     string
	currFilePath string
	filePage     int64
	done         chan bool
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
			case <-l.done:
				l.write()
				return
			case <-tc.C:
				l.write()
			}
		}
	}()
}

// write is a thread safe call to write the buffer to the file
func (l *PeriodicFileLogger) write() {
	l.bufferMutex.Lock()
	defer l.bufferMutex.Unlock()
	defer func() {
		l.buffer = nil
	}()

	f, err := os.OpenFile(l.currFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		color.Red(err.Error())
		return
	}

	for _, str := range l.buffer {
		if _, err := f.WriteString(str + PeriodicFileLoggerDelimiter); err != nil {
			color.Red(err.Error())
			break
		}
		if stat, err := f.Stat(); err != nil {
			return
		} else if stat.Size() > PeriodicFileChangeValue {
			l.filePage++
			l.currFilePath = fmt.Sprintf("%v%v", l.filePath, l.filePage)
			f, err = os.OpenFile(l.currFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				color.Red(err.Error())
				return
			}
		}
	}
	if len(l.buffer) > 0 {
		color.Blue("wrote %v new entries to log file(s)", len(l.buffer))
	}
	if err := f.Close(); err != nil {
		color.Red("trouble closing file: %v", err.Error())
	}
}

// NewPeriodicFileLogger ..
func NewPeriodicFileLogger(filePath string, period time.Duration) *PeriodicFileLogger {
	lgr := &PeriodicFileLogger{
		period:   period,
		buffer:   []string{},
		filePath: filePath,
		done:     make(chan bool, 1),
	}
	lgr.currFilePath = fmt.Sprintf("%v%v", lgr.filePath, lgr.filePage)
	return lgr
}

// ThreadSafeCounter thread safe Count
type ThreadSafeCounter struct {
	counterChan chan *big.Int
	Count       *big.Int
}

func (c *ThreadSafeCounter) countLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case val := <-c.counterChan:
			c.Count = new(big.Int).Add(c.Count, val)
		}
	}
}

func (c *ThreadSafeCounter) Add(val *big.Int) {
	c.counterChan <- val
}

func NewAtomicCounter(ctx context.Context) *ThreadSafeCounter {
	ctr := &ThreadSafeCounter{
		counterChan: make(chan *big.Int),
		Count:       new(big.Int),
	}
	go ctr.countLoop(ctx)
	return ctr
}
