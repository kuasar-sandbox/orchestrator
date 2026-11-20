package builder

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestHostCommandCooperativeCancellation(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "tool")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'echo cleanup-diagnostic >&2; exit 23' TERM\necho ready > \"$READY\"\nwhile :; do :; done\n"), 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	p := &buildPipeline{ctx: ctx}
	done := make(chan struct{})
	var out []byte
	var err error
	go func() {
		out, err = p.hostCmdEnv(map[string]string{"READY": filepath.Join(dir, "ready")}, script, "arg")
		close(done)
	}()
	deadline := time.After(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			break
		}
		select {
		case <-done:
			t.Fatalf("early exit: %v", err)
		case <-deadline:
			t.Fatal("no readiness")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
		if err == nil || !strings.Contains(string(out), "cleanup-diagnostic") {
			t.Fatalf("result: %q %v", out, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper did not exit")
	}
}
