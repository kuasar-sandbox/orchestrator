package nodectl

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestAuditor_AppendsLines(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "audit.log")
	a, err := NewAuditor(path)
	if err != nil {
		t.Fatal(err)
	}
	a.Logf("admit token=%s", "abc")
	a.Logf("release token=%s reason=%s", "abc", "normal")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	content := string(data)
	if !strings.Contains(content, "admit token=abc") {
		t.Errorf("missing admit line; got: %s", content)
	}
	if !strings.Contains(content, "release token=abc reason=normal") {
		t.Errorf("missing release line; got: %s", content)
	}
	// Two lines.
	if c := strings.Count(content, "\n"); c != 2 {
		t.Errorf("expected 2 lines, got %d", c)
	}
}

func TestAuditor_NilSafe(t *testing.T) {
	var a *Auditor
	a.Logf("noop") // must not panic
	if err := a.Close(); err != nil {
		t.Errorf("nil close should be no-op, got: %v", err)
	}
}

func TestAuditor_EmptyPathDisabled(t *testing.T) {
	a, err := NewAuditor("")
	if err != nil {
		t.Fatal(err)
	}
	if a != nil {
		t.Errorf("empty path should yield nil auditor, got %+v", a)
	}
}

type blockingAuditWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingAuditWriter) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (*blockingAuditWriter) Close() error { return nil }

func TestAuditorLogfDoesNotWaitForFileIO(t *testing.T) {
	w := &blockingAuditWriter{started: make(chan struct{}), release: make(chan struct{})}
	a := newAuditor(w, 1)
	a.Logf("first")
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("audit writer did not start")
	}

	returned := make(chan struct{})
	go func() {
		// The first queued record is blocked in Write. One record may enter the
		// queue and all remaining records must be dropped without blocking.
		for n := 0; n < 100; n++ {
			a.Logf("queued %d", n)
		}
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Logf waited for the blocked file writer")
	}
	close(w.release)
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
}
