package nodectl

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
	"github.com/kuasar-sandbox/sandboxer/pkg/memory"
	"github.com/kuasar-sandbox/sandboxer/pkg/memorycheckpoint"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
)

// This producer owns a real lease, record writer, frozen memfd, RAM pin and
// holder socket. The parent uses the production accounting handlers and actual
// POSIX lease handoff. Commands are ordering barriers, not release proofs.
func TestRetentionHolderRecoveryProducer(t *testing.T) {
	root := os.Getenv("NODECTL_HOLDER_RECOVERY_ROOT")
	if root == "" {
		return
	}
	fail := func(err error) { _, _ = fmt.Fprintln(os.Stderr, err); os.Exit(2) }
	socket := filepath.Join(root, "ctl.sock")
	lease, err := resource.CreateLease(resource.Lease{Version: resource.LeaseVersion, SandboxID: "sid", PID: os.Getpid(),
		ControllerSocket: socket, CgroupPath: filepath.Join(root, "cgroups", "source"),
		CapacityMemory: 1 << 30, CapacityCPUMilli: 1000, FloorMemory: 128 << 20, FloorCPUMilli: 500,
		StartupMemory: 256 << 20, ClientFeatures: []string{resource.FeatureStateSyncV1, resource.FeatureRetainedMemoryV2}})
	if err != nil {
		fail(err)
	}
	defer lease.Close()
	m, err := memory.Create("resource-holder-recovery", 8192)
	if err != nil {
		fail(err)
	}
	copy(m.Bytes(), bytes.Repeat([]byte{0x35}, 8192))
	fd, err := memorycheckpoint.ReadOnlyMemfd(m.FD())
	if err != nil {
		fail(err)
	}
	h, err := memorycheckpoint.PrepareHolder(filepath.Join(root, "checkpoint"), filepath.Join(root, "runs", "runtime"),
		memorycheckpoint.CaptureRecord{Version: memorycheckpoint.Version,
			Producer:     memorycheckpoint.RunIdentity{SandboxID: "sid", InstanceID: "old-instance", RunID: "run-sid"},
			CheckpointID: "checkpoint", HostOperationID: "checkpoint", MemoryID: "memory", Epoch: 1}, fd, 8192, nil)
	if err != nil {
		fail(err)
	}
	if err := m.Close(); err != nil {
		fail(err)
	}
	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(h.Record().Read()); err != nil {
		fail(err)
	}
	scan := bufio.NewScanner(os.Stdin)
	for scan.Scan() {
		switch scan.Text() {
		case "retire":
			err = h.Record().Update(func(r *memorycheckpoint.CaptureRecord) {
				r.Committed, r.RuntimeRetired = true, true
				r.SnapshotRef, r.SandboxRef = "snapshot", "sandbox"
			})
		case "file-ready":
			err = h.Record().Update(func(r *memorycheckpoint.CaptureRecord) {
				r.FileReady, r.PayloadRef = true, "payload"
			})
		default:
			err = errors.New("unknown holder recovery command")
		}
		if err != nil {
			fail(err)
		}
		if err := enc.Encode(h.Record().Read()); err != nil {
			fail(err)
		}
	}
	os.Exit(0)
}

type retentionHolderProcess struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *json.Decoder
	record memorycheckpoint.CaptureRecord
	path   string
	dead   bool
}

func startRetentionHolderProcess(t *testing.T, root string) *retentionHolderProcess {
	t.Helper()
	for _, directory := range []string{filepath.Join(root, "runs", "runtime"), filepath.Join(root, "checkpoint")} {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRetentionHolderRecoveryProducer$", "-test.timeout=30s")
	cmd.Env = append(os.Environ(), "NODECTL_HOLDER_RECOVERY_ROOT="+root, "GORACE=atexit_sleep_ms=0")
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &retentionHolderProcess{cmd: cmd, in: in, out: json.NewDecoder(out)}
	t.Cleanup(func() {
		_ = in.Close()
		if !p.dead {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	if err := p.out.Decode(&p.record); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		p.dead = true
		t.Fatalf("holder startup: %v; %s", err, stderr.String())
	}
	p.path, err = memorycheckpoint.RecordPath(filepath.Join(root, "checkpoint"), p.record.CheckpointID, p.record.MemoryID)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (p *retentionHolderProcess) request(t *testing.T, command string) {
	t.Helper()
	if _, err := fmt.Fprintln(p.in, command); err != nil {
		t.Fatal(err)
	}
	if err := p.out.Decode(&p.record); err != nil {
		t.Fatal(err)
	}
}

func (p *retentionHolderProcess) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	err := p.cmd.Wait()
	p.dead = true
	var exited *exec.ExitError
	if !errors.As(err, &exited) {
		t.Fatalf("producer was not killed: %v", err)
	}
}

type retentionHolderFixture struct {
	srv      *Server
	producer *retentionHolderProcess
	capture  resource.CaptureRetention
	claim    Placement
	request  *Message
	token    string
	owner    resource.LeaseFileIdentity
	root     string
}

func newRetentionHolderFixture(t *testing.T) *retentionHolderFixture {
	t.Helper()
	root := shortTestDir(t)
	p := startRetentionHolderProcess(t, root)
	socket := filepath.Join(root, "ctl.sock")
	l, _, _, err := resource.InspectLeaseIdentity(resource.LeasePath(socket, "sid"))
	if err != nil {
		t.Fatal(err)
	}
	s := twoPoolState(t)
	capacity := Resources{MemoryBytes: 1 << 30, CPUMilli: 1000}
	placePoolTest(t, s, "numa0", "sid", capacity)
	if _, _, err := s.Admit(AdmitSpec{SandboxID: "sid", Token: "source-token", PeerPID: p.cmd.Process.Pid,
		CgroupPath: l.CgroupPath, LeasePath: resource.LeasePath(socket, "sid"), Capacity: capacity,
		ConfiguredAllocatable: Resources{MemoryBytes: 128 << 20, CPUMilli: 500}, InitialBudget: 256 << 20, ClientFeatures: l.ClientFeatures}); err != nil {
		t.Fatal(err)
	}
	srv := &Server{Identity: socket, State: s, Inventory: &Inventory{ControllerSocket: socket,
		CgroupScanPaths: []string{filepath.Join(root, "cgroups")}, Pool: s.AllocatablePool, Logf: t.Logf}}
	srv.Admission = NewAdmissionController(AdmissionPolicy{Rate: 100, Burst: 100, StartupTTL: time.Minute})
	srv.Admission.SetWiring(s, nil, srv.BuildAdmitOKFromQueue)
	capture := resource.CaptureRetention{CapturePath: p.path, MemoryID: p.record.MemoryID, Epoch: p.record.Epoch, OperationID: "checkpoint"}
	call := func(typ string) {
		t.Helper()
		if r := srv.handleRetained(p.cmd.Process.Pid, &Message{Type: typ, SandboxID: "sid", Retention: &capture}, "source-token"); r.Type != TypeAck {
			t.Fatalf("%s: %s", typ, r.Msg)
		}
	}
	call(resource.TypePrepareRetained)
	p.request(t, "retire")
	call(resource.TypeCommitRetained)
	if err := s.RetireExecution("sid", "run-sid"); err != nil {
		t.Fatal(err)
	}
	claim, err := s.ReservePlacement("sid", "successor-run", capacity, OperationRecovery, "", func(name string) bool { return name == "numa1" })
	if err != nil {
		t.Fatal(err)
	}
	h, req := adoptionCandidate(t, srv, capture, "successor")
	var token string
	response, err := srv.buildRetainedAdmitOK(nil, os.Getpid(), req, &token)
	if err != nil || response.Status != StatusAdmitted || token == "" {
		t.Fatalf("successor handoff: %v", err)
	}
	if published, err := h.ConfirmPublished(); err != nil || !published {
		t.Fatalf("successor lease publication: %v", err)
	}
	_, owner, inode, err := resource.InspectLeaseIdentity(resource.LeasePath(socket, "sid"))
	if err != nil || owner != os.Getpid() {
		t.Fatalf("successor lease owner: %v", err)
	}
	f := &retentionHolderFixture{srv: srv, producer: p, capture: capture, claim: claim, request: req, token: token, owner: inode, root: root}
	f.check(t, true, 256<<20)
	return f
}

func (f *retentionHolderFixture) check(t *testing.T, pending bool, activeBytes uint64) {
	t.Helper()
	s := f.srv.State
	r, found := s.RetainedMemory("sid")
	if found != pending || found && (r.Bytes != 256<<20 || r.SourcePool != "numa0" || r.MemoryID != f.capture.MemoryID) {
		t.Fatalf("original R identity or pending state changed: %+v %v", r, found)
	}
	wantR := uint64(0)
	if pending {
		wantR = 256 << 20
	}
	if got := s.PoolStates()["numa0"].ResourceSnapshot().Reserved.MemoryBytes; got != wantR {
		t.Fatalf("original pool R: got %d want %d", got, wantR)
	}
	if got := s.PoolStates()["numa1"].ResourceSnapshot().Reserved.MemoryBytes; got != activeBytes {
		t.Fatalf("successor B: got %d want %d", got, activeBytes)
	}
	active, found := s.retentionReservation("sid")
	if !found || active.PeerPID != os.Getpid() || active.CgroupPath != f.request.CgroupPath || active.ReservationMemory != activeBytes {
		t.Fatalf("successor active identity changed: found=%v", found)
	}
	if !active.Provisional && (active.Token != f.token || active.CacheReservation != 4<<20) {
		t.Fatal("old recovery changed successor token or cache")
	}
	p := s.PoolStates()["numa1"]
	p.mu.Lock()
	claim := p.placements["sid"]
	p.mu.Unlock()
	if claim != f.claim || s.SandboxPool("sid") != "numa1" {
		t.Fatal("old recovery changed successor placement")
	}
	_, owner, inode, err := resource.InspectLeaseIdentity(resource.LeasePath(f.srv.Identity, "sid"))
	if err != nil || owner != os.Getpid() || inode != f.owner {
		t.Fatalf("old recovery changed successor lease inode/owner: %v", err)
	}
	receipt, err := readRetainedReceipt(retentionPath(f.srv.Identity, "sid"))
	if err != nil || (!receipt.pending()) == pending || receipt.OwnerToken != f.token || receipt.OwnerFile != f.owner {
		t.Fatalf("resource receipt did not durably preserve settlement and successor identity: %v", err)
	}
}

func (f *retentionHolderFixture) restart(t *testing.T) {
	t.Helper()
	s := twoPoolState(t)
	if err := s.RestorePlacement(f.claim); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.Inventory.Recover(s); err != nil {
		t.Fatal(err)
	}
	f.srv = recoveredPoolServer(f.srv, s)
}

func (f *retentionHolderFixture) replaySuccessor(t *testing.T) {
	t.Helper()
	response, err := f.srv.buildRetainedAdmitOK(nil, os.Getpid(), f.request, nil)
	if err != nil || response.Token != f.token || response.Status != StatusAdmitted {
		t.Fatalf("successor replay after inventory recovery: %v", err)
	}
}

type retentionRecoveryStream struct{ sparse.Source }

func (*retentionRecoveryStream) Close() error { return nil }

func openRetentionRecoveryReader(t *testing.T, f *retentionHolderFixture, allowFile <-chan struct{}) *memorycheckpoint.TopBacking {
	t.Helper()
	encoded, err := memorycheckpoint.EncodeMap(&memory.CaptureView{Size: 8192, Materialized: []byte{3}, Dirty: []byte{3}, Released: []byte{0}})
	if err != nil {
		t.Fatal(err)
	}
	mapRef := "file://map.image@digest:" + strings.Repeat("1", 64)
	d := memorycheckpoint.Descriptor{Version: memorycheckpoint.Version, CheckpointID: f.producer.record.CheckpointID,
		MemoryID: f.capture.MemoryID, Size: 8192, MapRef: mapRef}
	top, err := memorycheckpoint.OpenTop(context.Background(), d, memorycheckpoint.OpenOptions{Directory: filepath.Join(f.root, "checkpoint"),
		OpenFile: func(ctx context.Context, ref string) (fetch.Stream, error) {
			var data []byte
			switch ref {
			case mapRef:
				data = encoded
			case "payload":
				select {
				case <-allowFile:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				data = bytes.Repeat([]byte{0x35}, 8192)
			default:
				return nil, errors.New("unexpected recovery payload reference")
			}
			source, err := sparse.NewSource(bytes.NewReader(data), uint64(len(data)), nil)
			return &retentionRecoveryStream{source}, err
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = top.Close() })
	var b [1]byte
	if _, err := top.Reader.ReadAt(b[:], 4096); err != nil || b[0] != 0x35 {
		t.Fatalf("borrowed frozen memory: byte=%x error=%v", b[0], err)
	}
	return top
}

func TestRetainedMemoryHolderExitSettlesOnlyOriginalR(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, fileReady := range []bool{false, true} {
			for _, reader := range []bool{false, true} {
				t.Run(fmt.Sprintf("restart-%v/file-%v/reader-%v", restart, fileReady, reader), func(t *testing.T) {
					f := newRetentionHolderFixture(t)
					if err := f.srv.State.ReconcileRetainedMemory("sid"); err != nil {
						t.Fatal(err)
					}
					f.check(t, true, 256<<20) // A live writer/producer is never sufficient proof.
					allowFile := make(chan struct{})
					var top *memorycheckpoint.TopBacking
					if reader {
						top = openRetentionRecoveryReader(t, f, allowFile)
					}
					if fileReady {
						f.producer.request(t, "file-ready")
					}
					f.producer.kill(t)
					if restart {
						f.restart(t)
						f.check(t, reader, 1<<30) // Ordinary live-lease recovery conservatively restores B at Capacity.
						f.replaySuccessor(t)
					} else if err := f.srv.State.ReconcileRetainedMemory("sid"); err != nil {
						t.Fatal(err)
					}
					f.check(t, reader, 256<<20)
					if reader {
						proof, err := memorycheckpoint.ReadCaptureRecord(f.capture.CapturePath)
						if err != nil || proof.RAMReleased {
							t.Fatalf("reader-held RAM was settled after producer death: %v", err)
						}
						if fileReady {
							close(allowFile) // The active reader switches while keeping its separate GC pin.
						} else if err := top.Close(); err != nil {
							t.Fatal(err)
						}
					}
					deadline := time.Now().Add(3 * time.Second)
					for {
						if err := f.srv.State.ReconcileRetainedMemory("sid"); err != nil {
							t.Fatal(err)
						}
						if _, pending := f.srv.State.RetainedMemory("sid"); !pending {
							break
						}
						if time.Now().After(deadline) {
							t.Fatal("last RAM-pin close did not permit resource convergence")
						}
						time.Sleep(10 * time.Millisecond)
					}
					f.check(t, false, 256<<20)
					proof, err := memorycheckpoint.ReadCaptureRecord(f.capture.CapturePath)
					if err != nil || !proof.RAMReleased || proof.FileReady != fileReady || proof.Lost() == fileReady {
						t.Fatalf("physical recovery facts: %+v %v", proof, err)
					}
					if reader && fileReady {
						pin, err := memorycheckpoint.ExclusiveDirectory(filepath.Join(f.root, "checkpoint"))
						if err == nil {
							_ = pin.Close()
							t.Fatal("disk-only reader did not retain its separate GC pin")
						}
					}
					previous, err := memorycheckpoint.ObservePreviousSave(context.Background(), filepath.Join(f.root, "checkpoint"), *proof)
					if err != nil || !previous.Record.RAMReleased {
						t.Fatalf("historical lost source permanently blocked SID readiness: %v", err)
					}
					for range 2 {
						if err := f.srv.State.ReconcileRetainedMemory("sid"); err != nil {
							t.Fatal(err)
						}
					}
					if err := f.srv.State.RetireExecution("sid", "run-sid"); err != nil {
						t.Fatal(err)
					}
					if _, _, err := f.srv.State.Release("source-token"); err != nil {
						t.Fatal(err)
					}
					f.check(t, false, 256<<20)
				})
			}
		}
	}
}

func TestRetainedMemoryHolderRecoveryRejectsUnknownProof(t *testing.T) {
	for _, restart := range []bool{false, true} {
		for _, fault := range []string{"capture-operation", "producer-run", "missing-ram-inode", "replaced-ram-inode"} {
			t.Run(fmt.Sprintf("restart-%v/%s", restart, fault), func(t *testing.T) {
				f := newRetentionHolderFixture(t)
				f.producer.kill(t)
				proof, err := memorycheckpoint.ReadCaptureRecord(f.capture.CapturePath)
				if err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "capture-operation", "producer-run":
					if fault == "capture-operation" {
						proof.HostOperationID = "different-operation"
					} else {
						proof.Producer.RunID = "different-run"
					}
					b, err := json.Marshal(proof)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(f.capture.CapturePath, b, 0600); err != nil {
						t.Fatal(err)
					}
				case "missing-ram-inode", "replaced-ram-inode":
					path := f.capture.CapturePath + ".ram.lock"
					// Rename preserves the old inode, preventing immediate inode
					// reuse from accidentally making the negative case positive.
					if err := os.Rename(path, path+".original"); err != nil {
						t.Fatal(err)
					}
					if fault == "replaced-ram-inode" {
						if err := os.WriteFile(path, nil, 0600); err != nil {
							t.Fatal(err)
						}
					}
				}
				if restart {
					f.restart(t)
					f.check(t, true, 1<<30)
				} else {
					if err := f.srv.State.ReconcileRetainedMemory("sid"); err != nil {
						t.Fatal(err)
					}
					f.check(t, true, 256<<20)
				}
				got, err := memorycheckpoint.ReadCaptureRecord(f.capture.CapturePath)
				if err != nil || got.RAMReleased {
					t.Fatalf("unknown/mismatched proof was mutated before validation: %v", err)
				}
			})
		}
	}
}
