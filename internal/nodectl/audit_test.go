package nodectl

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
