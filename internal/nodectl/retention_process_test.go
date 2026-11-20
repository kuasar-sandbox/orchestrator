package nodectl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"gopkg.in/yaml.v3"

	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// This helper exercises the production resctl adapter, lifecycle lease and RPC
// as a different kernel PID from both the node controller and its successor.
func TestRetainedMemoryClientProcessHelper(t *testing.T) {
	role := os.Getenv("NODECTL_RETENTION_CLIENT")
	if role == "" {
		return
	}
	dir, socket := os.Getenv("NODECTL_RETENTION_DIR"), os.Getenv("NODECTL_SOCKET")
	cgroup := filepath.Join(dir, "cgroups", role)
	writeFakeCgroup(t, cgroup, "536870912", os.Getpid())
	if err := os.WriteFile(filepath.Join(cgroup, "memory.high"), []byte("max\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := recoverySandboxConfig(socket, cgroup)
	if managed := os.Getenv("NODECTL_RETENTION_MANAGED"); managed != "" && role == "successor" {
		dir := filepath.Join(managed, "retained-sid")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		pidfile, err := os.OpenFile(filepath.Join(dir, "retained-sid.pid"), os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			t.Fatal(err)
		}
		defer pidfile.Close()
		lock := unix.Flock_t{Type: unix.F_WRLCK, Whence: 0, Start: 0, Len: 0}
		if err := unix.FcntlFlock(pidfile.Fd(), unix.F_SETLK, &lock); err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintln(pidfile, os.Getpid()); err != nil {
			t.Fatal(err)
		}
		raw, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "retained-sid.yaml"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	path, err := memorycheckpoint.RecordPath(filepath.Join(dir, "checkpoint"), "checkpoint-one", "memory-one")
	if err != nil {
		t.Fatal(err)
	}
	capture := resource.CaptureRetention{CapturePath: path, MemoryID: "memory-one", Epoch: 1, OperationID: "checkpoint-one"}
	options := resctl.ControllerHookOptions{SocketPath: socket, SandboxID: "retained-sid", CgroupPath: cgroup, Context: context.Background()}
	if role == "successor" {
		options.Retained = &capture
	}
	hooks, err := resctl.NewControllerHooks(options, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer hooks.CloseRetained()
	var writer *memorycheckpoint.RecordWriter
	if role == "source" {
		if _, err := hooks.Admit("retained-sid", 0); err != nil {
			t.Fatal(err)
		}
		if !hooks.SupportsRetainedMemory() {
			t.Fatal("handoff capability was not negotiated")
		}
		start, err := retentionProcessStartTime(os.Getpid())
		if err != nil {
			t.Fatal(err)
		}
		writer, err = newTestRetentionRecord(t, filepath.Join(dir, "checkpoint"), memorycheckpoint.CaptureRecord{
			Version: memorycheckpoint.Version, Producer: memorycheckpoint.RunIdentity{SandboxID: "retained-sid", InstanceID: "instance-one", RunID: "run-one"},
			CheckpointID: "checkpoint-one", MemoryID: "memory-one", Epoch: 1,
			Holder: memorycheckpoint.HolderIdentity{PID: os.Getpid(), StartTime: start, SocketPath: filepath.Join(dir, "holder.sock")}})
		if err != nil {
			t.Fatal(err)
		}
		defer writer.Close()
	}
	_, _ = fmt.Fprintln(os.Stdout, "ready")
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		switch scan.Text() {
		case "prepare_next":
			start, startErr := retentionProcessStartTime(os.Getpid())
			if startErr != nil {
				t.Fatal(startErr)
			}
			writer, err = newTestRetentionRecord(t, filepath.Join(dir, "checkpoint"), memorycheckpoint.CaptureRecord{
				Version: memorycheckpoint.Version, Producer: memorycheckpoint.RunIdentity{SandboxID: "retained-sid", InstanceID: "instance-two", RunID: "run-two"},
				CheckpointID: "checkpoint-two", MemoryID: "memory-two", Epoch: 1,
				Holder: memorycheckpoint.HolderIdentity{PID: os.Getpid(), StartTime: start, SocketPath: filepath.Join(dir, "holder-two.sock")}})
			if err == nil {
				defer writer.Close()
				capture = resource.CaptureRetention{CapturePath: writer.Path(), MemoryID: "memory-two", Epoch: 1, OperationID: "checkpoint-two"}
				_, err = hooks.PrepareRetained(ctx, capture)
			}
		case "prepare":
			_, err = hooks.PrepareRetained(ctx, capture)
		case "commit_capture":
			err = writer.Update(func(r *memorycheckpoint.CaptureRecord) {
				r.Committed = true
				r.SnapshotRef = "snapshot-test"
				r.SandboxRef = "sandbox-test"
			})
		case "close_runtime":
			hooks.Release("runtime cleanup precedes RuntimeRetired fact")
		case "retire":
			err = writer.Update(func(r *memorycheckpoint.CaptureRecord) { r.RuntimeRetired = true })
			if err == nil {
				err = hooks.CommitRetained(ctx, capture)
			}
		case "admit":
			_, err = hooks.Admit("retained-sid", 256<<20)
			if err == nil {
				hooks.StartHeartbeat(context.Background(), 20*time.Millisecond)
			}
		case "ram_released":
			err = writer.Update(func(r *memorycheckpoint.CaptureRecord) {
				r.FileReady, r.RAMReleased = true, true
				r.PayloadRef = "file-test"
			})
		case "release_retained":
			err = hooks.ReleaseRetained(ctx, capture)
		case "drop_lease":
			err = hooks.CloseRetained()
		case "close":
			cancel()
			if err := hooks.CloseRetained(); err != nil {
				t.Fatal(err)
			}
			_, _ = fmt.Fprintln(os.Stdout, "closed")
			return
		default:
			err = fmt.Errorf("unknown helper command")
		}
		cancel()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stdout, "error:"+err.Error())
			err = nil
		} else {
			_, _ = fmt.Fprintln(os.Stdout, "ok:"+scan.Text())
		}
	}
}

type retentionClientProcess struct {
	cmd   *exec.Cmd
	in    io.WriteCloser
	lines chan string
}

func startRetentionClient(t *testing.T, role, dir, socket string) *retentionClientProcess {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestRetainedMemoryClientProcessHelper$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "NODECTL_RETENTION_CLIENT="+role, "NODECTL_RETENTION_DIR="+dir, "NODECTL_SOCKET="+socket, "GORACE=atexit_sleep_ms=0")
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
	p := &retentionClientProcess{cmd: cmd, in: in, lines: make(chan string, 16)}
	go func() {
		scan := bufio.NewScanner(out)
		for scan.Scan() {
			p.lines <- scan.Text()
		}
		close(p.lines)
	}()
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	if got := p.next(t); got != "ready" {
		t.Fatalf("client startup: %s", got)
	}
	return p
}

func (p *retentionClientProcess) next(t *testing.T) string {
	t.Helper()
	select {
	case line, ok := <-p.lines:
		if !ok {
			t.Fatal("retention helper output closed")
		}
		return line
	case <-time.After(15 * time.Second):
		t.Fatal("retention helper timed out")
		return ""
	}
}

func (p *retentionClientProcess) send(t *testing.T, command string) {
	t.Helper()
	if _, err := fmt.Fprintln(p.in, command); err != nil {
		t.Fatal(err)
	}
}

func (p *retentionClientProcess) run(t *testing.T, command string) {
	t.Helper()
	p.send(t, command)
	want := "ok:" + command
	if command == "close" {
		want = "closed"
	}
	if got := p.next(t); got != want {
		t.Fatalf("%s: %s", command, got)
	}
	if command == "close" {
		if err := p.cmd.Wait(); err != nil {
			t.Fatal(err)
		}
	}
}

func retainedAdminStatus(t *testing.T, socket string) *resource.AdminStatusResult {
	t.Helper()
	c := &resource.Client{SocketPath: socket}
	if err := c.Connect(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.AdminStatus()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func waitRetainedCharge(t *testing.T, socket string, memory uint64, count int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		got := retainedAdminStatus(t, socket)
		if got.NodeAllocated == memory && got.ReservationCount == count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained charge: %+v; want memory=%d active=%d", got, memory, count)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRetainedMemoryRPCProcessHandoff(t *testing.T) {
	t.Setenv("NODECTL_TEST_PHYSICAL", "4294967296")
	for _, tc := range []struct {
		name                                string
		releaseBeforeAdmit, loseACK, queued bool
	}{
		{name: "pending"}, {name: "released-after-candidate", releaseBeforeAdmit: true}, {name: "controller-dies-after-publication", loseACK: true},
		{name: "existing-admission-queue", queued: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.queued {
				t.Setenv("NODECTL_TEST_PHYSICAL", "555745280")
			}
			dir := shortTestDir(t)
			root, socket := filepath.Join(dir, "cgroups"), filepath.Join(dir, "ctl.sock")
			writeFakeCgroupRoot(t, root)
			phase := ""
			if tc.loseACK {
				phase = "retention_published"
			}
			controller := startRecoveryController(t, socket, root, phase)
			source := startRetentionClient(t, "source", dir, socket)
			source.run(t, "prepare")
			source.run(t, "commit_capture")
			source.run(t, "close_runtime")
			waitRetainedCharge(t, socket, 256<<20, 1)
			if pid, locked, err := resource.LeaseLockOwner(resource.LeasePath(socket, "retained-sid")); err != nil || !locked || pid != source.cmd.Process.Pid {
				t.Fatalf("prepared cleanup dropped source lease: %d %v %v", pid, locked, err)
			}
			source.run(t, "retire")
			waitRetainedCharge(t, socket, 256<<20, 0)
			if high, err := os.ReadFile(filepath.Join(root, "source", "memory.high")); err != nil || string(high) != "max\n" {
				t.Fatalf("B -> R changed cgroup high: %q %v", high, err)
			}
			if !tc.loseACK {
				controller.kill(t)
				controller = startRecoveryController(t, socket, root, "")
				waitRetainedCharge(t, socket, 256<<20, 0)
			}
			successor := startRetentionClient(t, "successor", dir, socket)
			if tc.releaseBeforeAdmit {
				source.run(t, "ram_released")
				source.run(t, "release_retained")
				waitRetainedCharge(t, socket, 0, 0)
			}
			if tc.queued {
				successor.send(t, "admit")
				select {
				case got := <-successor.lines:
					t.Fatalf("admission bypassed retained budget: %s", got)
				case <-time.After(100 * time.Millisecond):
				}
				source.run(t, "ram_released")
				source.run(t, "release_retained")
				if got := successor.next(t); got != "ok:admit" {
					t.Fatalf("queued handoff: %s", got)
				}
			} else if tc.loseACK {
				successor.send(t, "admit")
				if got := controller.next(t); got != "phase:retention_published" {
					t.Fatalf("lost ACK phase: %s", got)
				}
				controller.kill(t)
				controller = startRecoveryController(t, socket, root, "")
				if got := successor.next(t); got != "ok:admit" {
					t.Fatalf("adopt replay: %s", got)
				}
			} else {
				successor.run(t, "admit")
			}
			want := uint64(512 << 20)
			if tc.releaseBeforeAdmit || tc.queued {
				want = 256 << 20
			}
			waitRetainedCharge(t, socket, want, 1)
			source.run(t, "drop_lease")
			if pid, locked, err := resource.LeaseLockOwner(resource.LeasePath(socket, "retained-sid")); err != nil || !locked || pid != successor.cmd.Process.Pid {
				t.Fatalf("old Close removed successor lease: %d %v %v", pid, locked, err)
			}
			if !tc.releaseBeforeAdmit && !tc.queued {
				source.run(t, "ram_released")
			}
			if tc.name == "pending" {
				// Data readiness may precede the source's resource ACK. The
				// successor settles that precise old proof before preparing R2.
				successor.run(t, "prepare_next")
				waitRetainedCharge(t, socket, 256<<20, 1)
				successor.run(t, "commit_capture")
				successor.run(t, "close_runtime")
				successor.run(t, "retire")
				waitRetainedCharge(t, socket, 256<<20, 0)
				// The late producer receives its ACK and can exit, but its
				// distinct memory ID cannot clear R2 even with equal epochs.
				source.run(t, "release_retained")
				waitRetainedCharge(t, socket, 256<<20, 0)
				successor.run(t, "ram_released")
				successor.run(t, "release_retained")
				waitRetainedCharge(t, socket, 0, 0)
			} else {
				// An exact original producer can report RAMReleased after
				// transfer, without touching the successor's active B.
				source.run(t, "release_retained")
				waitRetainedCharge(t, socket, 256<<20, 1)
				successor.run(t, "release_retained")
				waitRetainedCharge(t, socket, 256<<20, 1)
			}
			source.run(t, "close")
			successor.run(t, "close")
			waitRetainedCharge(t, socket, 0, 0)
			controller.stop(t)
		})
	}
}
