package orch

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type snapshotCheckWriterFunc func([]byte) (int, error)

func (f snapshotCheckWriterFunc) Write(p []byte) (int, error) { return f(p) }

func TestSnapshotRuntimeHelperAllowsOutputDrainWithinDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "/bin/sh", "-c", "printf '%s\\n' '{\"status\":\"compatible\"}'")
	cmd.WaitDelay = 250 * time.Millisecond

	entered, release := make(chan struct{}), make(chan struct{})
	releaseOutput := sync.OnceFunc(func() { close(release) })
	defer releaseOutput()
	var out bytes.Buffer
	writer := snapshotCheckWriterFunc(func(p []byte) (int, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-release
		return out.Write(p)
	})
	done := make(chan error, 1)
	go func() { done <- runSnapshotRuntimeHelper(ctx, cmd, nil, writer, io.Discard) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("helper did not produce its response")
	}
	// The child must have been reaped before delaying output delivery, so this
	// exercises normal post-exit drain rather than a slowly running helper.
	for {
		err := cmd.Process.Signal(syscall.Signal(0))
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("helper did not exit while its response was buffered")
		case <-time.After(5 * time.Millisecond):
		}
	}
	time.Sleep(500 * time.Millisecond)
	releaseOutput()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("complete response within admission deadline rejected: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("helper did not finish draining its response")
	}
	if got, want := out.String(), "{\"status\":\"compatible\"}\n"; got != want {
		t.Fatalf("helper response = %q, want %q", got, want)
	}
}

func TestImportAdmissionDeadlineClosesInheritedHelperPipes(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	pidFile := filepath.Join(t.TempDir(), "pipe-holder.pid")
	quote := "'" + strings.ReplaceAll(pidFile, "'", "'\"'\"'") + "'"
	installSnapshotRuntimeHelperScript(t, fixture.o,
		"/bin/cat >/dev/null\n/bin/sleep 30 &\nprintf '%s\\n' \"$!\" > "+quote+
			"\nprintf '%s\\n' '{\"status\":\"compatible\"}'\n")
	t.Cleanup(func() {
		raw, err := os.ReadFile(pidFile)
		if errors.Is(err, os.ErrNotExist) {
			return
		}
		if err != nil {
			t.Error(err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
		if err != nil {
			t.Error(err)
			return
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			t.Error(err)
			return
		}
		defer process.Release()
		if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			t.Error(err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	cmd := fixture.command("inherited-pipe-target", fixture.token)
	_, err := fixture.o.prepareClusterConnect(ctx, cmd, 0)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("inherited pipes did not use admission deadline: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("inherited helper pipes blocked admission beyond its deadline")
	}
	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("pipe holder never started: %v", err)
	}
	if row, _ := fixture.o.st.Get(context.Background(), cmd.SID); row != nil {
		t.Fatal("incomplete helper response inserted a sandbox")
	}
	next := fixture.command("after-inherited-pipe", fixture.token)
	next.Memory = resumeMemoryOption(false)
	if ack := fixture.o.HandleCommand(context.Background(), next); ack.Status != routesync.AckAccepted {
		t.Fatalf("subsequent E-only command was blocked: %+v", ack)
	}
}
