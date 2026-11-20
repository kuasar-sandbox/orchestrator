package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

type workspaceReadyReader struct {
	io.Reader
	path string
}

func (r workspaceReadyReader) Read(p []byte) (int, error) {
	if err := os.WriteFile(r.path, nil, 0600); err != nil {
		return 0, err
	}
	return r.Reader.Read(p)
}

// The real entry installs its signal context before reading the request. Hold
// input there, signal the child, and require cancellation to reach Serve.
func TestWorkspaceStorageCommandSignals(t *testing.T) {
	if path := os.Getenv("WORKSPACE_SIGNAL_READY"); path != "" {
		err := workspaceStorageCmd(workspaceReadyReader{os.Stdin, path}, os.Stdout)
		if errors.Is(err, context.Canceled) {
			os.Exit(75)
		}
		os.Exit(76)
	}
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			ready := filepath.Join(t.TempDir(), "ready")
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWorkspaceStorageCommandSignals$")
			cmd.Env = append(os.Environ(), "WORKSPACE_SIGNAL_READY="+ready)
			input, err := cmd.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = cmd.Process.Kill(); _ = input.Close() }()
			for {
				if _, err := os.Stat(ready); err == nil {
					break
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				time.Sleep(time.Millisecond)
			}
			if err := cmd.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
			_, _ = io.WriteString(input, `{"Operation":"list"}`)
			_ = input.Close()
			var exit *exec.ExitError
			if err := cmd.Wait(); !errors.As(err, &exit) || exit.ExitCode() != 75 {
				t.Fatalf("cancellation did not reach Serve: %v %s", err, stderr.String())
			}
		})
	}
}
