package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"golang.org/x/sys/unix"
)

func checkpointCleanupTool(t *testing.T, o *Orchestrator, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "node-ctl")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nset -eu\n[ \"$1\" = checkpoint-cleanup ]\n"+body), 0700); err != nil {
		t.Fatal(err)
	}
	o.executables = configresolve.ExecutablesForNodeCtl(path)
	return path
}

func localPausedCheckpoint(t *testing.T, id string) sandboxFinalizerFixture {
	t.Helper()
	f := newSandboxFinalizerFixture(t, id)
	f.sb.State = types.StatePaused
	f.sb.RunID = ""
	f.sb.VswitchPort = ""
	f.sb.FloatingIP = ""
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://" + strings.Repeat("a", 64) + ".sandbox@digest:" + strings.Repeat("a", 64)}
	if err := os.MkdirAll(filepath.Join(f.sb.BaseDir, "checkpoint"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	f.o.cache(f.sb)
	return f
}

func TestCheckpointCleanupFailureRetainsMarkerAndRestartRetries(t *testing.T) {
	f := localPausedCheckpoint(t, "checkpoint-retry")
	checkpointCleanupTool(t, f.o, "echo injected checkpoint I/O failure >&2\nexit 1\n")
	if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err == nil {
		t.Fatal("failed tool reported cleanup success")
	}
	row, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || row.RunDir != f.sb.RunDir || row.ResumeSource != f.sb.ResumeSource {
		t.Fatalf("last marker/source lost: %+v %v", row, err)
	}
	if _, err := os.Stat(f.sb.RunDir); err != nil {
		t.Fatalf("RunDir removed before checkpoint cleanup: %v", err)
	}
	restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
	restarted.lc = f.lc
	restarted.vs = f.vs
	proof := filepath.Join(t.TempDir(), "cleanup.args")
	checkpointCleanupTool(t, restarted, "printf '%s\\n' \"$@\" > '"+proof+"'\n")
	restarted.removeSandboxRunDir = func(path string) error {
		if _, err := os.Stat(proof); err != nil {
			return errors.New("RunDir removal preceded checkpoint cleanup")
		}
		return os.RemoveAll(path)
	}
	if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatal(err)
	}
	row, err = restarted.st.Get(context.Background(), f.sb.ID)
	if err != nil || row.RunDir != "" || row.ResumeSource != f.sb.ResumeSource || row.State != types.StatePaused {
		t.Fatalf("restart did not finish exact owner: %+v %v", row, err)
	}
	args, err := os.ReadFile(proof)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(args), "--from\n"+f.sb.ResumeSource.Ref+"\n") || !strings.Contains(string(args), "--sandbox-id\n"+f.sb.ID+"\n") {
		t.Fatalf("tool lost source/producer: %s", args)
	}
}

func TestCheckpointCleanupExportFenceAndStaleSource(t *testing.T) {
	f := localPausedCheckpoint(t, "checkpoint-readers")
	proof := filepath.Join(t.TempDir(), "tool-ran")
	checkpointCleanupTool(t, f.o, "touch '"+proof+"'\n")
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	attempt, ok := f.o.exports.Begin(f.sb.ID, f.sb.ResumeSource, true, cancel)
	if !ok {
		t.Fatal("claim export")
	}
	if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err == nil || !strings.Contains(err.Error(), "export still owns source") {
		t.Fatalf("active export not fenced: %v", err)
	}
	// Detached template publishers retain the same source until Finish closes the
	// fence. No waiting under the lifecycle lock is permitted.
	f.o.exports.ResumeAccepted(f.sb.ID, errors.New("resume accepted"))
	if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err == nil {
		t.Fatal("detached reader was not fenced")
	}
	if _, err := os.Stat(proof); !os.IsNotExist(err) {
		t.Fatal("cleanup ran while publisher read source")
	}
	f.o.exports.Finish(attempt)
	stale := cloneSandbox(f.sb)
	f.sb.ResumeSource.Ref = "file://" + strings.Repeat("b", 64) + ".sandbox@digest:" + strings.Repeat("b", 64)
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	if err := f.o.cleanupPausedOwnership(context.Background(), stale); err == nil {
		t.Fatal("stale owner accepted cleanup")
	}
	if _, err := os.Stat(proof); !os.IsNotExist(err) {
		t.Fatal("stale owner started cleanup")
	}
	if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err != nil {
		t.Fatal(err)
	}
	row, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || row.ResumeSource != f.sb.ResumeSource || row.RunDir != "" {
		t.Fatalf("fresh retry lost source: %+v %v", row, err)
	}
}

func TestCheckpointCleanupSerializesResumeAndDelete(t *testing.T) {
	for _, action := range []string{"resume", "delete"} {
		t.Run(action, func(t *testing.T) {
			f := localPausedCheckpoint(t, "checkpoint-"+action)
			dir := t.TempDir()
			started, release := filepath.Join(dir, "started"), filepath.Join(dir, "release")
			for _, path := range []string{started, release} {
				if err := unix.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
			}
			notify, err := os.OpenFile(started, os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			permit, err := os.OpenFile(release, os.O_RDWR, 0)
			if err != nil {
				notify.Close()
				t.Fatal(err)
			}
			var once sync.Once
			unblock := func() { once.Do(func() { _, _ = permit.Write([]byte("go\n")) }) }
			var workers sync.WaitGroup
			t.Cleanup(func() { unblock(); notify.Close(); workers.Wait(); permit.Close() })
			checkpointCleanupTool(t, f.o, "printf x > '"+started+"'\nread answer < '"+release+"'\n")
			cleaned := make(chan error, 1)
			workers.Add(1)
			go func() { defer workers.Done(); cleaned <- f.o.finalizePausedCleanupOnce(f.sb.ID) }()
			entered := make(chan error, 1)
			go func() { b := make([]byte, 1); _, err := notify.Read(b); entered <- err }()
			select {
			case err := <-entered:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("cleanup tool did not start")
			}
			row, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil || row.RunDir == "" {
				t.Fatalf("marker cleared during cleanup: %+v %v", row, err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			admitted := make(chan error, 1)
			workers.Add(1)
			go func() {
				defer workers.Done()
				if action == "delete" {
					_, err := f.o.Kill(ctx, f.sb.ID, f.apiKey)
					admitted <- err
				} else {
					_, err := f.o.Connect(ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
					admitted <- err
				}
			}()
			// Observe real keyed-lock contention, never assume a scheduling delay.
			deadline := time.Now().Add(3 * time.Second)
			for {
				f.o.lifecycle.mu.Lock()
				lock := f.o.lifecycle.locks[f.sb.ID]
				waiting := lock != nil && lock.refs == 2
				f.o.lifecycle.mu.Unlock()
				if waiting {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("admission did not contend for cleanup lock")
				}
				runtime.Gosched()
			}
			if _, found := f.o.launches.Lookup(f.sb.ID); found {
				t.Fatal("Resume acquired runner while tool read source")
			}
			if action == "resume" {
				cancel()
			}
			unblock()
			if err := <-cleaned; err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-admitted:
				if action == "delete" && err != nil || action == "resume" && !errors.Is(err, context.Canceled) {
					t.Fatalf("%s admission: %v", action, err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("admission deadlocked behind cleanup")
			}
			if action == "delete" {
				waitForExportDeletion(t, f.o, f.sb.ID)
			} else {
				row, err := f.o.st.Get(context.Background(), f.sb.ID)
				if err != nil || row.State != types.StatePaused || row.ResumeSource != f.sb.ResumeSource || row.RunDir != "" {
					t.Fatalf("cancelled Resume changed source: %+v %v", row, err)
				}
			}
		})
	}
}
