package nodectl

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Auditor appends one line per significant controller event to a file.
// Path is typically /run/node-ctl/audit.log (tmpfs — host reboot wipes).
//
// Concurrent safe; serialized via mutex. Writes are best-effort: if the
// file is unreachable, the call returns the error to the caller and
// the call site decides whether to log or ignore.
type Auditor struct {
	mu   sync.Mutex
	f    *os.File
	path string
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
	return &Auditor{f: f, path: path}, nil
}

// Logf writes one timestamped line. No-op on nil receiver.
func (a *Auditor) Logf(format string, args ...any) {
	if a == nil || a.f == nil {
		return
	}
	line := fmt.Sprintf("%s "+format+"\n",
		append([]any{time.Now().UTC().Format(time.RFC3339Nano)}, args...)...)
	a.mu.Lock()
	defer a.mu.Unlock()
	_, _ = a.f.WriteString(line)
}

// Close flushes and closes the audit file.
func (a *Auditor) Close() error {
	if a == nil || a.f == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.f.Close()
	a.f = nil
	return err
}
