//go:build linux

package orch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configresolve"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/artifact"
	"golang.org/x/sys/unix"
)

// sourceOperationTry exercises the production lock ordering in a fresh
// admission, without sharing an earlier admission's private context.
func sourceOperationTry(o *Orchestrator, sb *types.Sandbox) (context.Context, func(), error) {
	unlock := o.lifecycle.Lock(sb.ID)
	defer unlock()
	return o.trySourceOperation(context.Background(), sb)
}

func assertSourceOperationBusy(t *testing.T, o *Orchestrator, sb *types.Sandbox) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, release, err := sourceOperationTry(o, sb)
		if release != nil {
			release()
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errSourceOperationBusy) {
			t.Fatalf("fresh source operation = %v, want busy", err)
		}
	case <-time.After(time.Second):
		t.Fatal("source operation blocked while holding the SID lifecycle lock")
	}
}

func TestSourceOperationNestedGrantAndRetainedReference(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "source-nested")
	ctx, closeParent, err := sourceOperationTry(f.o, f.sb)
	if err != nil {
		t.Fatal(err)
	}
	defer closeParent()
	grant := sourceGrant(ctx)
	if grant == nil {
		t.Fatal("canonical existing BaseDir received no source grant")
	}
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	nested, closeNested, err := f.o.trySourceOperation(ctx, f.sb)
	unlock()
	if err != nil || sourceGrant(nested) != grant {
		t.Fatalf("nested operation did not reuse its admission: %v", err)
	}
	closeNested()
	assertSourceOperationBusy(t, f.o, f.sb)

	successor := cloneSandbox(f.sb)
	successor.CreatedUnix++
	unlock = f.o.lifecycle.Lock(f.sb.ID)
	_, release, err := f.o.trySourceOperation(ctx, successor)
	unlock()
	if release != nil {
		release()
	}
	if !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("stale context borrowed another incarnation's grant: %v", err)
	}

	retained, closeRetained, err := retainSourceOperation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeRetained()
	closeParent()
	if sourceGrant(ctx) != nil || sourceGrant(retained) == nil {
		t.Fatal("closing admission did not isolate the retained reference")
	}
	assertSourceOperationBusy(t, f.o, f.sb)
	closeRetained()
	_, closeNext, err := sourceOperationTry(f.o, f.sb)
	if err != nil {
		t.Fatalf("last reference did not release ownership: %v", err)
	}
	closeNext()
}

func TestSourceOperationRejectsNonprivateLockPaths(t *testing.T) {
	for _, kind := range []string{"invalid-id", "base-symlink", "lock-symlink", "lock-hardlink", "lock-directory"} {
		t.Run(kind, func(t *testing.T) {
			f := newSandboxFinalizerFixture(t, "source-path")
			lockPath := filepath.Join(f.sb.BaseDir, sourceOperationLockName)
			outside := filepath.Join(t.TempDir(), "outside")
			switch kind {
			case "invalid-id":
				f.sb.ID = ".."
				f.sb.BaseDir = filepath.Clean(filepath.Join(f.o.cfg.Paths.BaseRoot, "sandboxes", f.sb.ID))
			case "base-symlink":
				if err := os.Rename(f.sb.BaseDir, outside); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, f.sb.BaseDir); err != nil {
					t.Fatal(err)
				}
			case "lock-symlink":
				if err := os.WriteFile(outside, nil, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, lockPath); err != nil {
					t.Fatal(err)
				}
			case "lock-hardlink":
				if err := os.WriteFile(outside, nil, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(outside, lockPath); err != nil {
					t.Fatal(err)
				}
			case "lock-directory":
				if err := os.Mkdir(lockPath, 0700); err != nil {
					t.Fatal(err)
				}
			}
			_, release, err := sourceOperationTry(f.o, f.sb)
			if release != nil {
				release()
			}
			if err == nil || errors.Is(err, errSourceOperationBusy) {
				t.Fatalf("unsafe %s accepted or reported as retryable busy: %v", kind, err)
			}
		})
	}
}

type sourceOperationCommandBarrier struct {
	dir, readyPath, releasePath string
	ready, permit               *os.File
	once                        sync.Once
}

func newSourceOperationCommandBarrier(t *testing.T) *sourceOperationCommandBarrier {
	t.Helper()
	b := &sourceOperationCommandBarrier{dir: t.TempDir()}
	b.readyPath, b.releasePath = filepath.Join(b.dir, "ready"), filepath.Join(b.dir, "release")
	for _, path := range []string{b.readyPath, b.releasePath} {
		if err := unix.Mkfifo(path, 0600); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	b.ready, err = os.OpenFile(b.readyPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b.permit, err = os.OpenFile(b.releasePath, os.O_RDWR, 0)
	if err != nil {
		b.ready.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { b.release(); b.ready.Close(); b.permit.Close() })
	return b
}

func (b *sourceOperationCommandBarrier) release() {
	b.once.Do(func() { _, _ = b.permit.Write([]byte("done\n")) })
}

func (b *sourceOperationCommandBarrier) script(t *testing.T, name, command string, lockFD int) string {
	t.Helper()
	path := filepath.Join(b.dir, name)
	body := "#!/bin/sh\nset -eu\n"
	if command != "" {
		body += "[ \"$1\" = '" + command + "' ]\n"
	}
	// Report even a missing FD, so a missing production inheritance call is a
	// direct assertion failure instead of a timeout waiting for the subprocess.
	body += fmt.Sprintf("target=$(readlink /proc/self/fd/%d || true)\nprintf '%%s %%s\\n' \"$$\" \"$target\" > '%s'\nread answer < '%s'\n", lockFD, b.readyPath, b.releasePath)
	if err := os.WriteFile(path, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	return path
}

func (b *sourceOperationCommandBarrier) started(t *testing.T, wantLock string) int {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	ready := make(chan result, 1)
	go func() {
		line, err := bufio.NewReader(b.ready).ReadString('\n')
		ready <- result{line, err}
	}()
	select {
	case result := <-ready:
		if result.err != nil {
			t.Fatal(result.err)
		}
		fields := strings.SplitN(strings.TrimSuffix(result.line, "\n"), " ", 2)
		if len(fields) != 2 {
			t.Fatalf("invalid child readiness: %q", result.line)
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			t.Fatal(err)
		}
		if fields[1] != wantLock {
			t.Fatalf("command inherited lock = %q, want %q (pid %d)", fields[1], wantLock, pid)
		}
		return pid
	case <-time.After(5 * time.Second):
		t.Fatal("protected command did not reach its readiness barrier")
		return 0
	}
}

func awaitSourceOperationAvailable(t *testing.T, o *Orchestrator, sb *types.Sandbox) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		_, release, err := sourceOperationTry(o, sb)
		if err == nil {
			release()
			return
		}
		if !errors.Is(err, errSourceOperationBusy) {
			t.Fatal(err)
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("actual command exit did not release source ownership")
		}
	}
}

func TestSourceOperationClosingParentKeepsInheritedChildLocked(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "source-parent-close")
	ctx, closeParent, err := sourceOperationTry(f.o, f.sb)
	if err != nil {
		t.Fatal(err)
	}
	defer closeParent()
	barrier := newSourceOperationCommandBarrier(t)
	// Existing ExtraFiles must survive and determine the inherited lock's FD.
	other, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	cmd := exec.Command(barrier.script(t, "hold", "", 4))
	cmd.ExtraFiles = []*os.File{other}
	inheritSourceOperation(ctx, cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		if !waited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	barrier.started(t, filepath.Join(f.sb.BaseDir, sourceOperationLockName))
	closeParent()
	assertSourceOperationBusy(t, f.o, f.sb)
	barrier.release()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	waited = true
	awaitSourceOperationAvailable(t, f.o, f.sb)
}

type sourceOperationConductorInput struct {
	BaseRoot, RunRoot string
	Sandbox           *types.Sandbox
	DBPath            string
	Tool              string
	Command           string
}

// This helper is a separate conductor process. The parent test kills it while
// its real production exec.Cmd is blocked inside a separately exec'd command.
// No Go goroutine or test-only lock model can substitute for that FD lifetime.
func TestSourceOperationConductorProcess(t *testing.T) {
	path := os.Getenv("ORCH_SOURCE_OPERATION_CONDUCTOR_INPUT")
	if path == "" {
		return
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var in sourceOperationConductorInput
	if err := json.Unmarshal(data, &in); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Paths.BaseRoot, cfg.Paths.RunRoot = in.BaseRoot, in.RunRoot
	cfg.Checkpoint.Mode = config.CheckpointLocal
	o := testOrchCfgAt(t, cfg, in.DBPath)
	o.executables = configresolve.ExecutablesForNodeCtl(in.Tool)
	ctx, release, err := sourceOperationTry(o, in.Sandbox)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	switch in.Command {
	case "publish":
		_, err = o.promote(ctx, in.Sandbox, in.Sandbox.ResumeSource)
	case "snapshot":
		_, err = o.snapshotLocal(ctx, in.Sandbox, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot})
	case "export":
		_, err = o.snapshotLocal(ctx, in.Sandbox, sandboxcfg.CaptureRequest{Kind: types.CaptureSandbox})
	case "checkpoint-cleanup":
		err = o.cleanupCheckpoint(ctx, in.Sandbox)
	default:
		t.Fatalf("unknown command %q", in.Command)
	}
	t.Fatalf("conductor command unexpectedly returned before SIGKILL: %v", err)
}

func TestSourceOperationCommandsSurviveConductorSIGKILL(t *testing.T) {
	for _, command := range []string{"publish", "snapshot", "export", "checkpoint-cleanup"} {
		t.Run(command, func(t *testing.T) {
			f := localPausedCheckpoint(t, "source-orphan")
			barrier := newSourceOperationCommandBarrier(t)
			toolName := "sandbox-ctl"
			if command == "checkpoint-cleanup" {
				toolName = "node-ctl"
			}
			tool := barrier.script(t, toolName, command, 3)
			input := sourceOperationConductorInput{
				BaseRoot: f.o.cfg.Paths.BaseRoot, RunRoot: f.o.cfg.Paths.RunRoot, Sandbox: f.sb, DBPath: f.dbPath,
				Tool: filepath.Join(filepath.Dir(tool), "node-ctl"), Command: command,
			}
			data, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			inputPath := filepath.Join(barrier.dir, "conductor.json")
			if err := os.WriteFile(inputPath, data, 0600); err != nil {
				t.Fatal(err)
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(binary, "-test.run=^TestSourceOperationConductorProcess$", "-test.v")
			cmd.Env = append(os.Environ(), "ORCH_SOURCE_OPERATION_CONDUCTOR_INPUT="+inputPath)
			var output bytes.Buffer
			cmd.Stdout, cmd.Stderr = &output, &output
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			waited := false
			t.Cleanup(func() {
				if !waited {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
				if t.Failed() {
					t.Logf("conductor output:\n%s", output.String())
				}
			})
			childPID := barrier.started(t, filepath.Join(f.sb.BaseDir, sourceOperationLockName))
			// A pidfd safely names this exact child even if an assertion fails.
			pidfd, err := unix.PidfdOpen(childPID, 0)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = unix.PidfdSendSignal(pidfd, unix.SIGKILL, nil, 0)
				_ = unix.Close(pidfd)
			})
			assertSourceOperationBusy(t, f.o, f.sb)
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err = cmd.Wait()
			waited = true
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != unix.SIGKILL {
				t.Fatalf("conductor termination = %v, want SIGKILL", err)
			}
			// The new conductor has an empty export registry. Only the actual
			// child's inherited open file description can protect the source.
			restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
			assertSourceOperationBusy(t, restarted, f.sb)
			barrier.release()
			awaitSourceOperationAvailable(t, restarted, f.sb)
		})
	}
}

func TestSourceOperationReopensAfterAuthorizedTerminalRebuild(t *testing.T) {
	f := newSandboxFinalizerFixture(t, "source-rebuild")
	staleCtx, closeSource, err := sourceOperationTry(f.o, f.sb)
	if err != nil {
		t.Fatal(err)
	}
	oldFile, err := os.Open(filepath.Join(f.sb.BaseDir, sourceOperationLockName))
	if err != nil {
		t.Fatal(err)
	}
	defer oldFile.Close() // Keep the old inode allocated, without another flock.
	oldStat, err := oldFile.Stat()
	if err != nil {
		t.Fatal(err)
	}
	closeSource()
	beginDeletingForTest(t, f)
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(f.sb.BaseDir); !os.IsNotExist(err) {
		t.Fatalf("terminal finalizer did not remove old BaseDir: %v", err)
	}
	_, releaseMissing, err := sourceOperationTry(f.o, f.sb)
	if err != nil {
		t.Fatal(err)
	}
	releaseMissing()
	if _, err := os.Stat(f.sb.BaseDir); !os.IsNotExist(err) {
		t.Fatal("source admission recreated a missing BaseDir")
	}
	successor := cloneSandbox(f.sb)
	successor.CreatedUnix++
	if err := os.MkdirAll(successor.BaseDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.o.st.Put(context.Background(), successor); err != nil {
		t.Fatal(err)
	}
	unlock := f.o.lifecycle.Lock(successor.ID)
	nextCtx, closeNext, err := f.o.trySourceOperation(staleCtx, successor)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	defer closeNext()
	nextStat, err := sourceGrant(nextCtx).file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if os.SameFile(oldStat, nextStat) {
		t.Fatal("rebuilt SID reused an unlinked old lock inode")
	}
	assertSourceOperationBusy(t, f.o, successor)
}

func TestSourceOperationExportRetainsGrantUntilPublisherActuallyReturns(t *testing.T) {
	for _, toTemplate := range []bool{false, true} {
		t.Run(fmt.Sprintf("template=%t", toTemplate), func(t *testing.T) {
			f := newExportResumeFixture(t)
			if err := os.MkdirAll(f.sb.BaseDir, 0700); err != nil {
				t.Fatal(err)
			}
			started := make(chan *sourceOperationGrant, 1)
			canceled := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			finish := func() { once.Do(func() { close(release) }) }
			t.Cleanup(finish)
			f.o.artifactPublisher = func(ctx context.Context, _ *types.Sandbox, source types.ResumeSource) (artifact.PublishReport, error) {
				started <- sourceGrant(ctx)
				if !toTemplate {
					<-ctx.Done()
					close(canceled)
				}
				<-release // Cancellation is not proof the real publisher exited.
				if !toTemplate {
					return artifact.PublishReport{}, context.Cause(ctx)
				}
				return testPublicationReport(source.Kind, "manifest://"+strings.Repeat("c", 64)), nil
			}
			done := startExport(f.o, f.ctx, f.apiKey, f.sb.ID, toTemplate, true)
			var grant *sourceOperationGrant
			select {
			case grant = <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("publisher not admitted")
			}
			if grant == nil {
				t.Fatal("export admission released its source before publisher began")
			}
			accepted, err := f.o.Connect(f.ctx, f.sb.ID, f.apiKey, "", api.ConnectOptions{})
			if err != nil || accepted == nil || accepted.State != types.StateStarting {
				t.Fatalf("resume could not overtake publisher: %+v, %v", accepted, err)
			}
			if !toTemplate {
				select {
				case <-canceled:
				case <-time.After(3 * time.Second):
					t.Fatal("KMT publisher was not canceled")
				}
			}
			assertLocalResumeWon(t, f)
			assertSourceOperationBusy(t, f.o, f.sb)
			if grant.closed.Load() || f.o.exports.ActiveDone(f.sb.ID) == nil {
				t.Fatal("resume/cancellation retired a still-running publisher")
			}
			select {
			case result := <-done:
				t.Fatalf("export returned before actual publisher completion: %+v", result)
			default:
			}
			finish()
			result := waitExportResult(t, done)
			if toTemplate && result.err != nil {
				t.Fatal(result.err)
			}
			if !toTemplate && !errors.Is(result.err, api.ErrExportPreempted) {
				t.Fatalf("KMT result = %v, want preempted", result.err)
			}
			if !grant.closed.Load() || f.o.exports.ActiveDone(f.sb.ID) != nil {
				t.Fatal("completed publisher retained admission ownership")
			}
		})
	}
}

func sourceOperationMissingDirectoryFixture(t *testing.T, portable bool) exportResumeFixture {
	t.Helper()
	f := newExportResumeFixture(t)
	// Model an imported or historical paused row whose canonical persistent
	// path is recorded but has never been materialized on this node.
	if err := os.RemoveAll(f.sb.BaseDir); err != nil {
		t.Fatal(err)
	}
	if portable {
		f.sb.ResumeSource.Ref = "manifest://" + strings.Repeat("b", 64)
	}
	if err := f.o.st.Put(f.ctx, f.sb); err != nil {
		t.Fatal(err)
	}
	f.o.cache(f.sb)
	return f
}

func TestSourceOperationPortableMissingDirectoryCanFinalizeOwnExport(t *testing.T) {
	for _, toTemplate := range []bool{false, true} {
		t.Run(fmt.Sprintf("template=%t", toTemplate), func(t *testing.T) {
			f := sourceOperationMissingDirectoryFixture(t, true)
			f.o.artifactPublisher = func(context.Context, *types.Sandbox, types.ResumeSource) (artifact.PublishReport, error) {
				t.Error("portable export invoked publisher")
				return artifact.PublishReport{}, errors.New("unexpected publication")
			}
			result, err := f.o.ExportSandbox(f.ctx, f.apiKey, f.sb.ID, toTemplate, false, "")
			if err != nil || result.Validate() != nil {
				t.Fatalf("portable source could not finalize its own export: %+v, %v", result, err)
			}
			waitForExportDeletion(t, f.o, f.sb.ID)
			if _, err := os.Stat(f.sb.BaseDir); !os.IsNotExist(err) {
				t.Fatalf("portable export materialized missing BaseDir: %v", err)
			}
		})
	}
}

func TestSourceOperationLogicalGrantDoesNotAuthorizeAnotherDelete(t *testing.T) {
	f := sourceOperationMissingDirectoryFixture(t, true)
	admission, closeAdmission, err := sourceOperationTry(f.o, f.sb)
	if err != nil {
		t.Fatal(err)
	}
	defer closeAdmission()
	if g := sourceGrant(admission); g == nil || g.file != nil {
		t.Fatalf("missing directory grant = %+v, want scoped ownership without FD", g)
	}
	retained, closeRetained, err := retainSourceOperation(admission)
	if err != nil {
		t.Fatal(err)
	}
	defer closeRetained()
	_, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	attempt, started := f.o.exports.Begin(f.sb.ID, f.sb.ResumeSource, true, "", cancel)
	if !started {
		t.Fatal("export admission failed")
	}
	defer f.o.exports.Finish(attempt)
	closeAdmission()
	if sourceGrant(admission) != nil || sourceGrant(retained) == nil {
		t.Fatal("logical retained grant did not outlive its admission")
	}
	assertSourceOperationBusy(t, f.o, f.sb)
	unlock := f.o.lifecycle.Lock(f.sb.ID)
	accepted, err := f.o.acceptSandboxDeleteLocked(context.Background(), f.sb)
	unlock()
	if accepted || !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("unrelated delete borrowed logical export ownership: %t, %v", accepted, err)
	}
	current, err := f.o.st.Get(f.ctx, f.sb.ID)
	if err != nil || current == nil || current.State != types.StatePaused {
		t.Fatalf("unrelated delete mutated paused source: %+v, %v", current, err)
	}
	cmd := exec.Command("true")
	inheritSourceOperation(retained, cmd)
	if err := cmd.Run(); err != nil {
		t.Fatalf("logical grant supplied an invalid inherited FD: %v", err)
	}
	if f.o.exports.BeginFinalize(attempt) != exportFinalizing {
		t.Fatal("export did not win its own finalization")
	}
	unlock = f.o.lifecycle.Lock(f.sb.ID)
	accepted, err = f.o.acceptSandboxDeleteLocked(retained, f.sb)
	unlock()
	if err != nil || !accepted {
		t.Fatalf("own retained grant could not accept source deletion: %t, %v", accepted, err)
	}
	if err := f.o.finalizeSandboxDeleteOnce(context.Background(), f.sb.ID); !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("physical finalizer overtook unfinished export: %v", err)
	}
	closeRetained()
	f.o.exports.Finish(attempt)
	waitForExportDeletion(t, f.o, f.sb.ID)
	if _, err := os.Stat(f.sb.BaseDir); !os.IsNotExist(err) {
		t.Fatalf("logical grant created a lock directory: %v", err)
	}
}

func TestSourceOperationInvalidExportOwnerRejectedBeforePublisher(t *testing.T) {
	for _, owner := range []string{"local-missing-directory", "portable-empty-base-field"} {
		t.Run(owner, func(t *testing.T) {
			f := sourceOperationMissingDirectoryFixture(t, owner == "portable-empty-base-field")
			canonical := f.sb.BaseDir
			if owner == "portable-empty-base-field" {
				// Preserve the existing cleanup validator: a ResumeSource does
				// not authorize a row with an empty canonical BaseDir field.
				f.sb.BaseDir = ""
				if err := f.o.st.Put(f.ctx, f.sb); err != nil {
					t.Fatal(err)
				}
				f.o.cache(f.sb)
			}
			called := false
			f.o.artifactPublisher = func(context.Context, *types.Sandbox, types.ResumeSource) (artifact.PublishReport, error) {
				called = true
				return artifact.PublishReport{}, errors.New("unexpected publication")
			}
			if _, err := f.o.ExportSandbox(f.ctx, f.apiKey, f.sb.ID, true, false, ""); err == nil {
				t.Fatal("invalid export source was admitted")
			}
			if called || f.o.exports.ActiveDone(f.sb.ID) != nil {
				t.Fatal("invalid source registered or started publication")
			}
			current, err := f.o.st.Get(f.ctx, f.sb.ID)
			if err != nil || current == nil || current.State != types.StatePaused ||
				current.BaseDir != f.sb.BaseDir || current.ResumeSource != f.sb.ResumeSource {
				t.Fatalf("invalid export mutated its source: %+v, %v", current, err)
			}
			if _, err := os.Stat(canonical); !os.IsNotExist(err) {
				t.Fatalf("invalid export recreated missing BaseDir: %v", err)
			}
		})
	}
}
