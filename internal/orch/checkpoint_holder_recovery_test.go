package orch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/accelerator/pkg/manifest/fetch"
	"github.com/kuasar-sandbox/accelerator/pkg/sparse"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"golang.org/x/sys/unix"
)

type holderProcessSpec struct {
	Directory string
	RunDir    string
	Record    memorycheckpoint.CaptureRecord
	Retired   bool
}

// Use the actual SDK holder in another kernel process, including its record
// writer, directory lock, immutable memfd and RAM lifetime pin. This is a host
// integration fixture, not a VMM or a replacement holder implementation.
func TestCheckpointHolderProcessHelper(t *testing.T) {
	raw := os.Getenv("ORCH_CHECKPOINT_HOLDER_SPEC")
	if raw == "" {
		return
	}
	var spec holderProcessSpec
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		t.Fatal(err)
	}
	m, err := memory.Create("orch-holder-recovery", 4096)
	if err != nil {
		t.Fatal(err)
	}
	copy(m.Bytes(), bytes.Repeat([]byte{0x6d}, 4096))
	fd, err := memorycheckpoint.ReadOnlyMemfd(m.FD())
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	h, err := memorycheckpoint.PrepareHolder(spec.Directory, spec.RunDir, spec.Record, fd, 4096, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Record().Update(func(r *memorycheckpoint.CaptureRecord) {
		r.Committed, r.RuntimeRetired = true, spec.Retired
		r.SnapshotRef, r.SandboxRef = checkpointSnapshotRef, checkpointSandboxRef
	}); err != nil {
		t.Fatal(err)
	}
	_, _ = fmt.Fprintln(os.Stdout, "ready")
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		if scan.Text() != "file-ready" {
			t.Fatalf("unexpected holder command %q", scan.Text())
		}
		if err := os.WriteFile(filepath.Join(spec.Directory, "payload.memory"), bytes.Repeat([]byte{0x6d}, 4096), 0600); err != nil {
			t.Fatal(err)
		}
		if err := h.Record().Update(func(r *memorycheckpoint.CaptureRecord) {
			r.FileReady, r.PayloadRef = true, "payload.memory"
		}); err != nil {
			t.Fatal(err)
		}
		_, _ = fmt.Fprintln(os.Stdout, "file-ready")
	}
}

type checkpointHolderProcess struct {
	cmd   *exec.Cmd
	in    io.WriteCloser
	lines chan string
	path  string
	spec  holderProcessSpec
}

func startCheckpointHolder(t *testing.T, f sandboxFinalizerFixture, owner store.RunPoolOwnership, retired bool) *checkpointHolderProcess {
	t.Helper()
	spec := holderProcessSpec{Directory: filepath.Join(owner.RetirementBaseDir, "checkpoint"), RunDir: f.sb.RunDir, Retired: retired,
		Record: memorycheckpoint.CaptureRecord{Version: memorycheckpoint.Version,
			Producer:     memorycheckpoint.RunIdentity{SandboxID: owner.TaskID, InstanceID: owner.RetirementInstanceID, RunID: owner.RunID},
			CheckpointID: owner.RetirementCheckpointID, HostOperationID: owner.RetirementOperationID,
			MemoryID: owner.MemoryID, Epoch: owner.MemoryEpoch, SaveError: "injected interrupted save"}}
	if err := os.MkdirAll(spec.Directory, 0700); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestCheckpointHolderProcessHelper$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "ORCH_CHECKPOINT_HOLDER_SPEC="+string(raw), "GORACE=atexit_sleep_ms=0")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	path, err := memorycheckpoint.RecordPath(spec.Directory, spec.Record.CheckpointID)
	if err != nil {
		t.Fatal(err)
	}
	p := &checkpointHolderProcess{cmd: cmd, in: in, lines: make(chan string, 4), path: path, spec: spec}
	go func() {
		scanner := bufio.NewScanner(out)
		for scanner.Scan() {
			p.lines <- scanner.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() {
		_ = in.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	p.expect(t, "ready")
	return p
}

func (p *checkpointHolderProcess) expect(t *testing.T, want string) {
	t.Helper()
	select {
	case line := <-p.lines:
		if line != want {
			t.Fatalf("holder response %q, want %q", line, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("holder response timed out")
	}
}

func (p *checkpointHolderProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Wait(); err == nil {
		t.Fatal("holder did not exit from SIGKILL")
	}
}

func (p *checkpointHolderProcess) record(t *testing.T) memorycheckpoint.CaptureRecord {
	t.Helper()
	r, err := memorycheckpoint.ReadCaptureRecord(p.path)
	if err != nil {
		t.Fatal(err)
	}
	return *r
}

type holderTestStream struct{ sparse.Source }

func (*holderTestStream) Close() error { return nil }

func (p *checkpointHolderProcess) open(t *testing.T) *memorycheckpoint.TopBacking {
	t.Helper()
	encoded, err := memorycheckpoint.EncodeMap(&memory.CaptureView{Size: 4096, Materialized: []byte{1}, Dirty: []byte{1}, Released: []byte{0}})
	if err != nil {
		t.Fatal(err)
	}
	mapRef := "file://map.image@digest:" + strings.Repeat("1", 64)
	options := memorycheckpoint.OpenOptions{Directory: p.spec.Directory, OpenFile: func(_ context.Context, ref string) (fetch.Stream, error) {
		data := encoded
		if ref != mapRef {
			var err error
			data, err = os.ReadFile(filepath.Join(p.spec.Directory, ref))
			if err != nil {
				return nil, err
			}
		}
		source, err := sparse.NewSource(bytes.NewReader(data), uint64(len(data)), nil)
		return &holderTestStream{Source: source}, err
	}}
	top, err := memorycheckpoint.OpenTop(context.Background(), memorycheckpoint.Descriptor{Version: memorycheckpoint.Version,
		MemoryID: p.spec.Record.MemoryID, CheckpointID: p.spec.Record.CheckpointID, Size: 4096, MapRef: mapRef}, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = top.Close() })
	return top
}

func TestCheckpointHolderRemainsReadableAfterFullRunDirCleanup(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	m.ParentRetired = true
	if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	p := startCheckpointHolder(t, f, m, true)
	f.sb.State = types.StatePaused
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: checkpointSnapshotRef, SandboxRef: checkpointSandboxRef}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	checkpointCleanupTool(t, f.o, fmt.Sprintf("exit %d\n", CheckpointMemoryReadersBusyExitCode))
	if err := f.o.finalizePausedCleanupOnce(f.sb.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(f.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("RunDir inode survived ordinary cleanup: %v", err)
	}
	r := p.record(t)
	if filepath.Dir(r.Holder.Directory.Path) != filepath.Dir(f.sb.RunDir) || r.FileReady || r.RAMReleased {
		t.Fatal("fixture did not preserve a pending independent sibling holder")
	}
	top := p.open(t)
	buf := make([]byte, 4096)
	if n, err := top.Reader.ReadAt(buf, 0); err != nil || n != len(buf) || !bytes.Equal(buf, bytes.Repeat([]byte{0x6d}, 4096)) {
		t.Fatalf("RAM after RunDir removal: %d %v", n, err)
	}
}

func TestFailedSuccessorCleanupLeavesIndependentHolderReadable(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	p := startCheckpointHolder(t, f, m, true)
	successor := cloneSandbox(f.sb)
	successor.RunID = "failed-successor"
	// Resume owns no new BaseDir. Its existing launch cleanup must remove only
	// this attempt's ordinary runtime paths, while the old producer stays live.
	progress := &launchCleanupProgress{runID: successor.RunID, runDir: successor.RunDir}
	if err := progress.step(context.Background(), f.o, successor, true); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(successor.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed successor retained RunDir: %v", err)
	}
	top := p.open(t)
	buf := make([]byte, 4096)
	if n, err := top.Reader.ReadAt(buf, 0); err != nil || n != len(buf) || !bytes.Equal(buf, bytes.Repeat([]byte{0x6d}, 4096)) {
		t.Fatalf("failed successor damaged predecessor RAM: %d %v", n, err)
	}
	if r := p.record(t); r.FileReady || r.RAMReleased {
		t.Fatal("launch cleanup consumed the predecessor save")
	}
	if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
		t.Fatalf("failed successor forgot predecessor holder owner: %v %v", found, err)
	}
}

func TestLostHolderRecoveryChangesOnlyExactPausingRun(t *testing.T) {
	for _, state := range []string{"pausing", "pausing-restart", "paused", "successor"} {
		t.Run(state, func(t *testing.T) {
			f := pendingPauseFixture(t)
			stopRetirementWorkers(t, f.o)
			m := retiredTestOwnership(t, f)
			p := startCheckpointHolder(t, f, m, true)
			before := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: checkpointSandboxRef}
			f.sb.ResumeSource = before
			switch state {
			case "pausing", "pausing-restart":
				f.sb.State = types.StatePausing
				m.Role, m.MemoryID, m.MemoryEpoch = "", "", 0
				if err := f.o.saveRunOwnership(context.Background(), m); err != nil {
					t.Fatal(err)
				}
			case "paused":
				f.sb.State, f.sb.RunID = types.StatePaused, ""
				checkpointCleanupTool(t, f.o, "exit 0\n")
			case "successor":
				f.sb.RunID = "successor-run"
			}
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(f.sb.RunDir)
			if err != nil {
				t.Fatal(err)
			}
			p.kill(t)
			if state == "pausing-restart" {
				restarted := testOrchCfgAt(t, f.o.cfg, f.dbPath)
				restarted.lc, restarted.vs = f.lc, f.vs
				ctx, cancel := context.WithCancel(context.Background())
				restarted.SetLifecycleContext(ctx)
				t.Cleanup(func() { cancel(); drainRunEnds(t, restarted) })
				if err := restarted.ReconcileSandboxes(ctx); err != nil {
					t.Fatal(err)
				}
				drainRunEnds(t, restarted)
			} else if err := f.o.reconcileRetiredRun(context.Background(), m); err != nil {
				t.Fatal(err)
			}
			r := p.record(t)
			if !r.Lost() || !r.RAMReleased || r.FileReady || r.Discarded {
				t.Fatalf("unexpected loss facts: %+v", r)
			}
			current, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil || current.ResumeSource != before {
				t.Fatalf("recovery changed historical source: %+v %v", current, err)
			}
			want := f.sb.State
			if state == "pausing" || state == "pausing-restart" {
				want = types.StateDead
			}
			if current.State != want || state == "successor" && current.RunID != f.sb.RunID {
				t.Fatalf("old loss changed wrong business state: %+v", current)
			}
			if _, err := os.Stat(f.sb.BaseDir); err != nil {
				t.Fatalf("loss removed historical checkpoint directory: %v", err)
			}
			if state == "successor" {
				latest, err := os.Stat(f.sb.RunDir)
				if err != nil || !os.SameFile(info, latest) {
					t.Fatalf("old cleanup replaced successor RunDir: %v", err)
				}
			}
			if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || found {
				t.Fatalf("released generation owner remained: %v %v", found, err)
			}
		})
	}
}

func TestDeadHolderDefaultPauseWaitFailsUntilLastReaderCloses(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	p := startCheckpointHolder(t, f, m, true)
	top := p.open(t)
	f.sb.RunID = "restored-from-older-checkpoint"
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	p.kill(t)
	f.o.captureStateReader = func(ctx context.Context, _ string, query ctl.CaptureStateQuery) (ctl.CaptureState, error) {
		r, err := memorycheckpoint.WaitCurrent(ctx, p.spec.Directory, query.SandboxID, query.NoWaitForPreviousMemorySave)
		if errors.Is(err, memorycheckpoint.ErrSavePending) {
			return captureStateFor(query, true), nil
		}
		if err != nil {
			return ctl.CaptureState{}, err
		}
		result := captureStateFor(query, false)
		if r != nil {
			result.MemoryID, result.Epoch = r.MemoryID, r.Epoch
		}
		return result, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := f.o.Pause(ctx, f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot}); !errors.Is(err, memorycheckpoint.ErrMemoryUnavailable) {
		t.Fatalf("default wait after producer loss: %v", err)
	}
	current, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || current.State != types.StateRunning || p.record(t).RAMReleased {
		t.Fatalf("unavailable wait changed running state or released reader RAM: %+v %v", current, err)
	}
	if _, err := f.o.Pause(ctx, f.sb.ID, f.apiKey, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, NoWaitForPreviousMemorySave: true}); !errors.Is(err, api.ErrPreviousMemorySavePending) {
		t.Fatalf("no-wait physical obligation: %v", err)
	}
	if err := top.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.o.reconcileRetiredRun(ctx, m); err != nil {
		t.Fatal(err)
	}
	if r := p.record(t); !r.Lost() || r.SaveError == "" {
		t.Fatalf("fixture lost historical error or physical completion: %+v", r)
	}
	_, unlock, admitted, err := f.o.lockPauseAdmission(ctx, f.sb.ID, sandboxcfg.CaptureRequest{}, nil)
	if err != nil {
		t.Fatalf("completed historical loss poisoned pause admission: %v", err)
	}
	defer unlock()
	if admitted.RunID != f.sb.RunID || admitted.State != types.StateRunning {
		t.Fatal("admission used a different running generation")
	}
}

func TestUnknownHolderProofCannotReleaseHostOwnership(t *testing.T) {
	for _, fault := range []string{"missing-pin", "replacement-pin", "corrupt-record", "not-retired"} {
		t.Run(fault, func(t *testing.T) {
			f := pendingPauseFixture(t)
			stopRetirementWorkers(t, f.o)
			m := retiredTestOwnership(t, f)
			p := startCheckpointHolder(t, f, m, fault != "not-retired")
			p.kill(t)
			switch fault {
			case "missing-pin", "replacement-pin":
				if err := os.Rename(p.path+".ram.lock", p.path+".old-pin"); err != nil {
					t.Fatal(err)
				}
				if fault == "replacement-pin" {
					if err := os.WriteFile(p.path+".ram.lock", nil, 0600); err != nil {
						t.Fatal(err)
					}
				}
			case "corrupt-record":
				if err := os.WriteFile(p.path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.o.reconcileRetiredRun(context.Background(), m); err == nil {
				t.Fatal("unknown proof authorized cleanup")
			}
			if fault != "corrupt-record" && p.record(t).RAMReleased {
				t.Fatal("unknown proof synthesized physical release")
			}
			if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
				t.Fatalf("unknown proof discarded durable owner: %v %v", found, err)
			}
			if _, stops := f.lc.stopSnapshot(); stops != 0 {
				t.Fatal("unknown proof authorized unit fencing")
			}
		})
	}
}

func TestReleasedHolderDoesNotRemoveLockedEmptySuccessorDirectory(t *testing.T) {
	f := pendingPauseFixture(t)
	stopRetirementWorkers(t, f.o)
	m := retiredTestOwnership(t, f)
	p := startCheckpointHolder(t, f, m, true)
	p.kill(t)
	if err := os.RemoveAll(f.sb.RunDir); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(f.sb.RunDir, 0700); err != nil {
		t.Fatal(err)
	}
	dir, err := os.Open(f.sb.RunDir)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	if err := unix.Flock(int(dir.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	before, err := dir.Stat()
	if err != nil {
		t.Fatal(err)
	}
	f.sb.RunID = "successor-before-pid"
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	if err := f.o.reconcileRetiredRun(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	other, err := os.Open(f.sb.RunDir)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	after, err := other.Stat()
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("delayed holder cleanup changed successor directory inode: %v", err)
	}
	if err := unix.Flock(int(other.Fd()), unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) {
		t.Fatalf("second owner entered locked successor RunDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(f.sb.RunDir, "sandbox.pid"), []byte("successor"), 0600); err != nil {
		t.Fatalf("successor could not publish PID after old holder cleanup: %v", err)
	}
}

func TestExitedSuccessorCleansRunDirBeforeHistoricalHolderFinishes(t *testing.T) {
	for _, tail := range []bool{false, true} {
		name := "borrowed-ram"
		if tail {
			name = "directory-only-retry"
		}
		t.Run(name, func(t *testing.T) {
			f := pendingPauseFixture(t)
			stopRetirementWorkers(t, f.o)
			m := retiredTestOwnership(t, f)
			p := startCheckpointHolder(t, f, m, true)
			var reader *memorycheckpoint.TopBacking
			unknown := filepath.Join(p.record(t).Holder.Directory.Path, "unowned-content")
			if tail {
				if err := os.WriteFile(unknown, []byte("must be preserved"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				reader = p.open(t)
			}
			p.kill(t)
			f.sb.RunID = "exited-successor"
			if err := f.o.st.Put(context.Background(), f.sb); err != nil {
				t.Fatal(err)
			}
			finish := func() error {
				unlock := f.o.lifecycle.Lock(f.sb.ID)
				defer unlock()
				current, err := f.o.st.Get(context.Background(), f.sb.ID)
				if err != nil {
					return err
				}
				return f.o.finalizeRunningSandboxDeadLocked(context.Background(), current)
			}
			err := finish()
			if tail && !errors.Is(err, errRetirementPending) || !tail && !errors.Is(err, api.ErrPreviousMemorySavePending) {
				t.Fatalf("unexpected cleanup responsibility: %v", err)
			}
			if _, err := os.Lstat(f.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("historical holder retained exited successor RunDir: %v", err)
			}
			if _, found, err := f.o.st.RunPoolOwnership(context.Background(), m.RunID); err != nil || !found {
				t.Fatalf("pending historical responsibility lost owner: %v %v", found, err)
			}
			pending, err := f.o.retainedMemoryPending(context.Background(), f.sb.ID)
			if err != nil || pending == tail {
				t.Fatalf("directory-only retry recreated RAM obligation: pending=%v err=%v", pending, err)
			}
			if tail {
				if b, err := os.ReadFile(unknown); err != nil || string(b) != "must be preserved" {
					t.Fatalf("cleanup removed unknown holder content: %q %v", b, err)
				}
				if err := os.Remove(unknown); err != nil {
					t.Fatal(err)
				}
			} else if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if err := finish(); err != nil {
				t.Fatalf("terminal retry did not converge: %v", err)
			}
			current, err := f.o.st.Get(context.Background(), f.sb.ID)
			if err != nil || current.State != types.StateDead || current.RunDir != "" || current.BaseDir != f.sb.BaseDir {
				t.Fatalf("terminal retry lost retained source or current state: %+v %v", current, err)
			}
		})
	}
}
