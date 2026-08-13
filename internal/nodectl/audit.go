package nodectl

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const auditQueueDepth = 1024

// Auditor asynchronously appends significant controller events to a file.
// Path is typically /run/node-ctl/audit.log (tmpfs — host reboot wipes).
//
// Logf only formats and enqueues a line. The resource RPC goroutine never
// performs file I/O or waits for the writer; a full queue drops the best-effort
// audit record. Close drains accepted records before closing the writer.
type Auditor struct {
	mu        sync.Mutex
	writer    io.WriteCloser
	lines     chan string
	done      chan struct{}
	closed    bool
	writeErr  error
	closeErr  error
	closeOnce sync.Once
}

// NewAuditor opens the audit file in append mode, creating it (and
// any missing parent directories) if needed. Returns nil when path
// is empty (audit disabled).
func NewAuditor(path string) (*Auditor, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("audit: mkdir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("audit: open: %w", err)
	}
	return newAuditor(f, auditQueueDepth), nil
}

func newAuditor(writer io.WriteCloser, queueDepth int) *Auditor {
	if queueDepth < 1 {
		queueDepth = 1
	}
	a := &Auditor{
		writer: writer,
		lines:  make(chan string, queueDepth),
		done:   make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *Auditor) run() {
	defer close(a.done)
	for line := range a.lines {
		if _, err := io.WriteString(a.writer, line); err != nil {
			a.mu.Lock()
			if a.writeErr == nil {
				a.writeErr = err
			}
			a.mu.Unlock()
		}
	}
}

// Logf enqueues one timestamped line without waiting for file I/O. No-op on a
// nil or closed receiver; drops the record if the bounded queue is full.
func (a *Auditor) Logf(format string, args ...any) {
	if a == nil {
		return
	}
	line := fmt.Sprintf("%s "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339Nano)}, args...)...)
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return
	}
	select {
	case a.lines <- line:
	default:
	}
}

// Close flushes and closes the audit file.
func (a *Auditor) Close() error {
	if a == nil {
		return nil
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		close(a.lines)
		a.mu.Unlock()
		<-a.done
		closeErr := a.writer.Close()

		a.mu.Lock()
		defer a.mu.Unlock()
		if a.writeErr != nil {
			a.closeErr = a.writeErr
		} else {
			a.closeErr = closeErr
		}
		a.writer = nil
	})
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closeErr
}
