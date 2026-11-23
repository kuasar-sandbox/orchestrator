package helperexec

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestHelperProcess(t *testing.T) {
	dir := os.Getenv("HELPER_EXEC_TEST_DIR")
	if dir == "" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	mode := os.Getenv("HELPER_EXEC_TEST_MODE")
	if mode == "ignore" {
		signal.Ignore(syscall.SIGTERM)
	}
	if mode == "success" {
		fmt.Print("done")
		return
	}
	if mode == "error" {
		fmt.Fprint(os.Stderr, "child diagnostic")
		os.Exit(23)
	}
	if err := os.WriteFile(filepath.Join(dir, "ready"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	fmt.Fprint(os.Stderr, "cleanup diagnostic")
	if err := os.WriteFile(filepath.Join(dir, "cleaned"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	os.Exit(1)
}

func TestCommandCancellation(t *testing.T) {
	for _, mode := range []string{"cooperative", "ignore"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			cmd := CommandContext(ctx, os.Args[0], "-test.run=^TestHelperProcess$")
			cmd.Env = append(os.Environ(), "HELPER_EXEC_TEST_DIR="+dir, "HELPER_EXEC_TEST_MODE="+mode)
			var stderr []byte
			done := make(chan error, 1)
			go func() { var err error; stderr, err = cmd.CombinedOutput(); done <- err }()
			deadline := time.After(10 * time.Second)
			for {
				if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("early exit: %v %s", err, stderr)
				case <-deadline:
					t.Fatal("no readiness")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			started := time.Now()
			cancel()
			select {
			case err := <-done:
				var exit *exec.ExitError
				if !errors.As(err, &exit) {
					t.Fatalf("exit: %v", err)
				}
				if mode == "ignore" {
					status, ok := exit.Sys().(syscall.WaitStatus)
					if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL || time.Since(started) < 5*time.Second {
						t.Fatalf("fallback: %v after %v", err, time.Since(started))
					}
				} else {
					if exit.ExitCode() != 1 {
						t.Fatalf("exit: %v %s", err, stderr)
					}
					if _, err := os.Stat(filepath.Join(dir, "cleaned")); err != nil {
						t.Fatal(err)
					}
					if string(stderr) != "cleanup diagnostic" {
						t.Fatalf("lost diagnostics: %q", stderr)
					}
				}
			case <-time.After(15 * time.Second):
				t.Fatal("child not reaped")
			}
		})
	}
}

func TestCommandResults(t *testing.T) {
	for _, mode := range []string{"success", "error"} {
		t.Run(mode, func(t *testing.T) {
			cmd := CommandContext(t.Context(), os.Args[0], "-test.run=^TestHelperProcess$")
			cmd.Env = append(os.Environ(), "HELPER_EXEC_TEST_DIR="+t.TempDir(), "HELPER_EXEC_TEST_MODE="+mode)
			out, err := cmd.CombinedOutput()
			if mode == "success" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 23 || string(out) != "child diagnostic" {
				t.Fatalf("result: %q %v", out, err)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := CommandContext(ctx, "/does-not-exist").Run(); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled before start: %v", err)
	}
	if err := CommandContext(t.Context(), "/does-not-exist").Run(); err == nil {
		t.Fatal("missing executable succeeded")
	}
}
