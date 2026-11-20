package orch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/workspacedisk"
)

// Run as sandbox-ctl through the real promote command. Delayed cleanup makes
// cancellation observable without mistaking a sent signal for process exit.
func TestPublishCancellationProcess(t *testing.T) {
	dir := os.Getenv("ORCH_PUBLISH_CANCEL_DIR")
	if dir == "" {
		return
	}
	// Bound the whole child lifetime, including waiting for its first signal,
	// so a parent killed outside Go's test cleanup cannot orphan the helper.
	lifetime, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ctx, stop := signal.NotifyContext(lifetime, syscall.SIGTERM)
	defer stop()
	if os.Getenv("ORCH_PUBLISH_IGNORE_TERM") == "1" {
		signal.Ignore(syscall.SIGTERM)
	}
	if os.Getenv("ORCH_PUBLISH_DEFAULT_TERM") == "1" {
		signal.Reset(syscall.SIGTERM)
	}
	func() {
		for _, name := range []string{"materialize", "incomplete.sandbox"} {
			path := filepath.Join(dir, name)
			if err := os.WriteFile(path, []byte("unfinished"), 0o600); err != nil {
				t.Fatal(err)
			}
			defer os.Remove(path)
		}
		if err := os.WriteFile(filepath.Join(dir, "ready"), []byte(fmt.Sprint(os.Getpid())), 0o600); err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		if err := os.WriteFile(filepath.Join(dir, "canceling"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := os.Stat(filepath.Join(dir, "release")); err == nil {
				return
			}
			select {
			case <-lifetime.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	os.Exit(1)
}

func installCancelingPublisher(t *testing.T, f exportResumeFixture) string {
	t.Helper()
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("ORCH_PUBLISH_TEST_BINARY", binary)
	t.Setenv("ORCH_PUBLISH_CANCEL_DIR", dir)
	script := "#!/bin/sh\nexec \"$ORCH_PUBLISH_TEST_BINARY\" -test.run='^TestPublishCancellationProcess$'\n"
	if err := os.WriteFile(filepath.Join(dir, "sandbox-ctl"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	f.o.executables = configresolve.ExecutablesForNodeCtl(filepath.Join(dir, "node-ctl"))
	return dir
}

func waitPublishFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("publisher did not create %s", path)
}

func TestExportCancellationWaitsForPublisherCleanup(t *testing.T) {
	for _, cause := range []string{"resume", "request", "lifecycle"} {
		t.Run(cause, func(t *testing.T) {
			f := newExportResumeFixture(t)
			dir := installCancelingPublisher(t, f)
			// Release the helper even if an assertion fails before normal cleanup.
			t.Cleanup(func() { _ = os.WriteFile(filepath.Join(dir, "release"), nil, 0o600) })
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			lifecycle, stop := context.WithCancel(f.ctx)
			defer stop()
			f.o.SetLifecycleContext(lifecycle)
			done := startExport(f.o, ctx, f.apiKey, f.sb.ID, false, true)
			waitPublishFile(t, filepath.Join(dir, "ready"))
			switch cause {
			case "resume":
				if _, err := f.o.Connect(f.ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{}); err != nil {
					t.Fatal(err)
				}
			case "request":
				cancel()
			case "lifecycle":
				stop()
			}
			waitPublishFile(t, filepath.Join(dir, "canceling"))
			select {
			case result := <-done:
				t.Fatalf("export returned before publisher cleanup: %+v", result)
			default:
			}
			if f.o.exports.ActiveDone(f.sb.ID) == nil {
				t.Fatal("publisher lost export ownership before exit")
			}
			assertSourceOperationBusy(t, f.o, f.sb)
			if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			result := waitExportResult(t, done)
			if result.err == nil || (cause == "resume" && !errors.Is(result.err, api.ErrExportPreempted)) {
				t.Fatalf("canceled export = %+v", result)
			}
			for _, name := range []string{"materialize", "incomplete.sandbox"} {
				if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("publisher left %s: %v", name, err)
				}
			}
			awaitSourceOperationAvailable(t, f.o, f.sb)
			if cause == "resume" {
				assertLocalResumeWon(t, f)
			}
		})
	}
}

func TestPublishCancellationKillsUnresponsiveProcess(t *testing.T) {
	f := newExportResumeFixture(t)
	dir := installCancelingPublisher(t, f)
	t.Setenv("ORCH_PUBLISH_IGNORE_TERM", "1")
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.o.promote(ctx, f.sb, f.sb.ResumeSource)
		done <- err
	}()
	waitPublishFile(t, filepath.Join(dir, "ready"))
	started := time.Now()
	cancel()
	select {
	case err := <-done:
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("unresponsive publisher error = %v, want ExitError", err)
		}
		if status, ok := exit.Sys().(syscall.WaitStatus); !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
			t.Fatalf("unresponsive publisher error = %v, want SIGKILL", err)
		}
		if elapsed := time.Since(started); elapsed < 5*time.Second || elapsed > 15*time.Second {
			t.Fatalf("publisher grace period = %v, want 5 seconds", elapsed)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("unresponsive publisher was not killed")
	}
}

func TestPublishCancellationDiagnostics(t *testing.T) {
	for _, defaultTerm := range []bool{false, true} {
		t.Run(fmt.Sprintf("default_term=%t", defaultTerm), func(t *testing.T) {
			f := newExportResumeFixture(t)
			dir := installCancelingPublisher(t, f)
			if defaultTerm {
				t.Setenv("ORCH_PUBLISH_DEFAULT_TERM", "1")
			}
			var logs bytes.Buffer
			f.o.log = slog.New(slog.NewTextHandler(&logs, nil))
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := f.o.promote(ctx, f.sb, f.sb.ResumeSource)
				done <- err
			}()
			waitPublishFile(t, filepath.Join(dir, "ready"))
			if !defaultTerm {
				if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			select {
			case err := <-done:
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatalf("canceled publisher = %v, want ExitError", err)
				}
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || status.Signaled() != defaultTerm {
					t.Fatalf("unexpected child exit: %v", err)
				}
				if defaultTerm {
					if status.Signal() != syscall.SIGTERM || !strings.Contains(logs.String(), "level=WARN") || !strings.Contains(logs.String(), "files may remain") {
						t.Fatalf("signal death needs residual-file warning: %s", logs.String())
					}
				} else if !strings.Contains(logs.String(), "level=INFO") || strings.Contains(logs.String(), "files may remain") {
					t.Fatalf("cooperative exit must remain Info: %s", logs.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("publisher not reaped")
			}
		})
	}
}

// Workspace storage must wait for cooperative cleanup too, rather than close
// the child's output pipes immediately using the short admission probe path.
func TestWorkspaceStorageCancellationWaitsForCleanup(t *testing.T) {
	f := newExportResumeFixture(t)
	dir := installCancelingPublisher(t, f)
	script, err := os.ReadFile(filepath.Join(dir, "sandbox-ctl"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "node-ctl"), script, 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.WriteFile(filepath.Join(dir, "release"), nil, 0600) })
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := f.o.callWorkspaceStorage(ctx, f.sb.ManifestKey, workspacedisk.Request{Operation: "list"})
		done <- err
	}()
	waitPublishFile(t, filepath.Join(dir, "ready"))
	cancel()
	waitPublishFile(t, filepath.Join(dir, "canceling"))
	select {
	case err := <-done:
		t.Fatalf("returned before cleanup: %v", err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("result: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("helper not reaped")
	}
	for _, name := range []string{"materialize", "incomplete.sandbox"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("uncleaned %s: %v", name, err)
		}
	}
}
