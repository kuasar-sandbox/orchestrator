package orch

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/sandboxer/pkg/ctl"
)

// A real ctl socket exercises framing, complete-list validation, chunking and
// ambiguous connection failures rather than substituting the client API.
type diskCheckpointPeer struct {
	mu                            sync.Mutex
	refs                          []ctl.DiffCheckpointRef
	lists                         int
	deletes                       [][]ctl.DiffCheckpointRef
	truncateList, loseDeleteReply bool
	listError, deleteError        *ctl.Response
	beforeListReply               func()
}

func newDiskCheckpointFixture(t *testing.T, sid string) sandboxFinalizerFixture {
	f := newSandboxFinalizerFixture(t, sid)
	f.o.cfg.Paths.RunRoot = shortOrchestratorTestDir(t)
	f.sb.RunDir = nodepath.SandboxRunDir(f.o.cfg.Paths.RunRoot, sid)
	if err := os.MkdirAll(f.sb.RunDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	return f
}

func (p *diskCheckpointPeer) setup(refs []ctl.DiffCheckpointRef, truncate, lose bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refs, p.truncateList, p.loseDeleteReply = refs, truncate, lose
}

func serveDiskCheckpointPeer(t *testing.T, sb *types.Sandbox) *diskCheckpointPeer {
	t.Helper()
	ln, err := net.Listen("unix", filepath.Join(sb.RunDir, "ctl.sock"))
	if err != nil {
		t.Fatal(err)
	}
	p := &diskCheckpointPeer{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			func() {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				var req ctl.Request
				if err := ctl.ReadMessage(c, &req); err != nil {
					return
				}
				p.mu.Lock()
				defer p.mu.Unlock()
				switch req.Type {
				case ctl.TypeCheckpointListRequest:
					p.lists++
					listError := p.listError
					if p.beforeListReply != nil {
						p.beforeListReply()
					}
					if listError != nil {
						_ = ctl.WriteMessage(c, *listError)
						return
					}
					if p.truncateList {
						_ = ctl.WriteMessage(c, ctl.Response{Type: ctl.TypeCheckpointListChunk, Checkpoints: p.refs})
						return
					}
					_ = ctl.WriteCheckpointList(c, p.refs)
				case ctl.TypeCheckpointDeleteRequest:
					if req.CheckpointOwner != "" {
						t.Error("online release supplied an offline owner")
					}
					p.deletes = append(p.deletes, append([]ctl.DiffCheckpointRef(nil), req.CheckpointRefs...))
					if p.deleteError != nil {
						_ = ctl.WriteMessage(c, *p.deleteError)
						return
					}
					var remaining []ctl.DiffCheckpointRef
					for _, ref := range p.refs {
						removed := false
						for _, selected := range req.CheckpointRefs {
							removed = removed || ref == selected
						}
						if !removed {
							remaining = append(remaining, ref)
						}
					}
					p.refs = remaining
					if p.loseDeleteReply {
						p.loseDeleteReply = false
						return
					}
					_ = ctl.WriteMessage(c, ctl.Response{Type: ctl.TypeCheckpointDeleteDone})
				default:
					t.Errorf("unexpected ctl request %q", req.Type)
				}
			}()
		}
	}()
	t.Cleanup(func() { ln.Close(); <-done })
	return p
}

func diskCheckpointReleaseLifecycle(t *testing.T, o *Orchestrator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	o.SetLifecycleContext(ctx)
	t.Cleanup(func() {
		cancel()
		drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := o.acceptedOps.Drain(drain); err != nil {
			t.Error(err)
		}
	})
}

func waitDiskCheckpointRelease(t *testing.T, o *Orchestrator, sid string) {
	t.Helper()
	// Wait for the accepted worker to finish without closing admission or
	// cancelling its lifecycle; a later independent trigger must still work.
	o.acceptedOps.mu.Lock()
	idle := o.acceptedOps.idle
	o.acceptedOps.mu.Unlock()
	select {
	case <-idle:
	case <-time.After(3 * time.Second):
		t.Fatal("disk checkpoint release worker did not finish")
	}
	o.diskReleaseMu.Lock()
	active := o.diskReleaseActive[sid] != nil
	o.diskReleaseMu.Unlock()
	if active {
		t.Fatal("finished worker retained its SID")
	}
}

func TestDiskCheckpointReleaseUnsupportedStopsWorker(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-unsupported")
	p := serveDiskCheckpointPeer(t, f.sb)
	diskCheckpointReleaseLifecycle(t, f.o)
	p.mu.Lock()
	p.listError = &ctl.Response{Type: ctl.TypeError, Code: ctl.CheckpointUnsupportedCode, Msg: "checkpoint disabled"}
	p.mu.Unlock()
	f.o.requestDiskCheckpointRelease(f.sb.ID)
	waitDiskCheckpointRelease(t, f.o, f.sb.ID)
	p.mu.Lock()
	lists, deletes := p.lists, len(p.deletes)
	p.mu.Unlock()
	if lists != 1 || deletes != 0 {
		t.Fatalf("unsupported List: lists=%d deletes=%d", lists, deletes)
	}
	got, err := f.o.st.Get(context.Background(), f.sb.ID)
	if err != nil || got == nil || got.State != types.StateRunning || got.RunID != f.sb.RunID {
		t.Fatalf("unsupported check changed running ownership: %+v, %v", got, err)
	}

	// Unsupported is not cached by SID. An independent later trigger still
	// discovers and deletes versions when the runtime supports Checkpoint.
	refs := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "later"}}
	p.mu.Lock()
	p.listError, p.refs = nil, refs
	p.mu.Unlock()
	f.o.requestDiskCheckpointRelease(f.sb.ID)
	waitDiskCheckpointRelease(t, f.o, f.sb.ID)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 2 || len(p.deletes) != 1 || !reflect.DeepEqual(p.deletes[0], refs) {
		t.Fatalf("later trigger was skipped: lists=%d deletes=%+v", p.lists, p.deletes)
	}
}

func TestDiskCheckpointReleasePreservesRetainedCaptureUntilConsumption(t *testing.T) {
	f := newDiskCheckpointFixture(t, "retained-disk")
	ctx := context.Background()
	source := types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "file://retained.sandbox"}
	if ok, err := f.o.st.SetPausing(ctx, f.sb.ID, f.sb.RunID, true); err != nil || !ok {
		t.Fatalf("gate=%v %v", ok, err)
	}
	if ok, err := f.o.st.CommitPaused(ctx, f.sb.ID, f.sb.RunID, source, "retained"); err != nil || !ok {
		t.Fatalf("retain=%v %v", ok, err)
	}
	if err := f.o.st.Put(ctx, f.sb); err != nil {
		t.Fatal(err)
	}
	peer := serveDiskCheckpointPeer(t, f.sb)
	retained := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "retained"}, {DiffID: "data", ID: "retained"}}
	ordinary := ctl.DiffCheckpointRef{DiffID: "root", ID: "ordinary"}
	peer.setup(append(append([]ctl.DiffCheckpointRef{}, retained...), ordinary), false, false)
	if err := f.o.releaseDiskCheckpointsOnce(ctx, f.sb.ID, &diskCheckpointSelection{}); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	kept := append([]ctl.DiffCheckpointRef{}, peer.refs...)
	peer.mu.Unlock()
	if !reflect.DeepEqual(kept, retained) {
		t.Fatalf("retained versions deleted: %+v", kept)
	}
	if ok, err := f.o.st.DeleteCheckpoint(ctx, f.sb.ID, "retained"); err != nil || !ok {
		t.Fatalf("consume=%v %v", ok, err)
	}
	if err := f.o.releaseDiskCheckpointsOnce(ctx, f.sb.ID, &diskCheckpointSelection{}); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peer.refs) != 0 {
		t.Fatalf("consumed capture remains: %+v", peer.refs)
	}
}

func TestDiskCheckpointReleaseUnsupportedPreservesConcurrentTrigger(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-unsupported-trigger")
	p := serveDiskCheckpointPeer(t, f.sb)
	diskCheckpointReleaseLifecycle(t, f.o)
	refs := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "concurrent"}}
	p.mu.Lock()
	p.listError = &ctl.Response{Type: ctl.TypeError, Code: ctl.CheckpointUnsupportedCode, Msg: "checkpoint disabled"}
	// The peer has selected the unsupported response but has not sent it yet.
	// Trigger another check while the original worker is still in List.
	p.beforeListReply = func() {
		p.beforeListReply, p.listError, p.refs = nil, nil, refs
		f.o.requestDiskCheckpointRelease(f.sb.ID)
	}
	p.mu.Unlock()
	f.o.requestDiskCheckpointRelease(f.sb.ID)
	waitDiskCheckpointRelease(t, f.o, f.sb.ID)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 2 || len(p.deletes) != 1 || !reflect.DeepEqual(p.deletes[0], refs) {
		t.Fatalf("concurrent trigger was lost: lists=%d deletes=%+v", p.lists, p.deletes)
	}
}

func TestDiskCheckpointReleaseListErrorsRemainRetryable(t *testing.T) {
	for _, message := range []string{"temporary storage failure", ctl.CheckpointUnsupportedCode, "unknown type: " + ctl.TypeCheckpointListRequest} {
		t.Run(message, func(t *testing.T) {
			f := newDiskCheckpointFixture(t, "disk-list-error")
			p := serveDiskCheckpointPeer(t, f.sb)
			diskCheckpointReleaseLifecycle(t, f.o)
			refs := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "retry"}}
			p.mu.Lock()
			p.refs = refs
			p.listError = &ctl.Response{Type: ctl.TypeError, Msg: message}
			p.mu.Unlock()
			selected := &diskCheckpointSelection{}
			err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selected)
			if err == nil || errors.Is(err, ctl.ErrCheckpointUnsupported) || selected.listed {
				t.Fatalf("ordinary List error accepted: selected=%+v err=%v", selected, err)
			}
			p.mu.Lock()
			if len(p.deletes) != 0 {
				t.Error("failed List issued Delete")
			}
			// The worker's first attempt still fails. Only its automatic retry
			// can observe a complete list and release the selected version.
			p.beforeListReply = func() { p.beforeListReply, p.listError = nil, nil }
			p.mu.Unlock()
			f.o.requestDiskCheckpointRelease(f.sb.ID)
			waitDiskCheckpointRelease(t, f.o, f.sb.ID)
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.lists != 3 || len(p.deletes) != 1 || !reflect.DeepEqual(p.deletes[0], refs) {
				t.Fatalf("ordinary error was not retried: lists=%d deletes=%+v", p.lists, p.deletes)
			}
		})
	}
}

func TestDiskCheckpointReleaseDeleteUnsupportedRemainsRetryable(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-delete-unsupported")
	p := serveDiskCheckpointPeer(t, f.sb)
	refs := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "selected"}}
	p.setup(refs, false, false)
	p.mu.Lock()
	p.deleteError = &ctl.Response{Type: ctl.TypeError, Code: ctl.CheckpointUnsupportedCode, Msg: "delete unsupported"}
	p.mu.Unlock()
	selected := &diskCheckpointSelection{}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selected); !errors.Is(err, ctl.ErrCheckpointUnsupported) {
		t.Fatalf("Delete error was swallowed: %v", err)
	}
	p.mu.Lock()
	p.deleteError = nil
	p.refs = append(p.refs, ctl.DiffCheckpointRef{DiffID: "root", ID: "new"})
	p.mu.Unlock()
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selected); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 1 || len(p.deletes) != 2 || !reflect.DeepEqual(p.deletes[0], refs) || !reflect.DeepEqual(p.deletes[1], refs) {
		t.Fatalf("Delete error changed retry selection: lists=%d deletes=%+v", p.lists, p.deletes)
	}
}

func TestDiskCheckpointReleaseBothCompletionOrders(t *testing.T) {
	for _, exportFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "export-first", false: "running-first"}[exportFirst], func(t *testing.T) {
			f := newDiskCheckpointFixture(t, "disk-event-order")
			ctx, cancel := context.WithCancel(context.Background())
			f.o.SetLifecycleContext(ctx)
			t.Cleanup(func() {
				cancel()
				drain, stop := context.WithTimeout(context.Background(), 10*time.Second)
				defer stop()
				if err := f.o.acceptedOps.Drain(drain); err != nil {
					t.Error(err)
				}
			})
			p := serveDiskCheckpointPeer(t, f.sb)
			p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, false, false)
			release := holdTerminalSource(t, f.o, f.sb)
			_, cancelUpload := context.WithCancelCause(context.Background())
			defer cancelUpload(nil)
			attempt, ok := f.o.exports.Begin(f.sb.ID, f.sb.ResumeSource, true, "", cancelUpload)
			if !ok {
				t.Fatal("export admission")
			}
			defer f.o.exports.Finish(attempt)
			f.sb.State, f.sb.LaunchMode = types.StateStarting, types.LaunchCold
			f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
			if err := f.o.st.Put(ctx, f.sb); err != nil {
				t.Fatal(err)
			}
			finishExport := func() { release(); f.o.exports.Finish(attempt); f.o.requestDiskCheckpointRelease(f.sb.ID) }
			finishRunning := func() {
				f.sb.State, f.sb.LaunchMode = types.StateRunning, ""
				if err := f.o.st.Put(ctx, f.sb); err != nil {
					t.Fatal(err)
				}
				f.o.requestDiskCheckpointRelease(f.sb.ID)
			}
			if exportFirst {
				finishExport()
			} else {
				finishRunning()
			}
			// The first event alone never admits runtime deletion. The other
			// event must trigger or wake the coalesced release worker.
			time.Sleep(50 * time.Millisecond)
			p.mu.Lock()
			early := p.lists
			p.mu.Unlock()
			if early != 0 {
				t.Fatal("released before both completion boundaries")
			}
			if exportFirst {
				finishRunning()
			} else {
				finishExport()
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				p.mu.Lock()
				released := len(p.deletes) > 0 && len(p.refs) == 0
				p.mu.Unlock()
				if released {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("completion event did not release versions")
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestDiskCheckpointReleaseRetriesOnlySelectedVersions(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-release-fixed")
	p := serveDiskCheckpointPeer(t, f.sb)
	old := []ctl.DiffCheckpointRef{{DiffID: "root", ID: "old-root"}, {DiffID: "data", ID: "old-data"}}
	p.setup(old, false, true)
	selection := &diskCheckpointSelection{}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err == nil {
		t.Fatal("lost Delete reply was accepted")
	}
	p.mu.Lock()
	p.refs = append(append([]ctl.DiffCheckpointRef(nil), old...), ctl.DiffCheckpointRef{DiffID: "root", ID: "new-version"})
	p.mu.Unlock()
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 1 || len(p.deletes) != 2 || !reflect.DeepEqual(p.deletes[0], old) || !reflect.DeepEqual(p.deletes[1], old) {
		t.Fatalf("retry widened selection: lists=%d deletes=%+v", p.lists, p.deletes)
	}
}

func TestDiskCheckpointReleaseIncompleteListNeverDeletes(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-release-partial")
	p := serveDiskCheckpointPeer(t, f.sb)
	p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, true, false)
	selection := &diskCheckpointSelection{}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err == nil {
		t.Fatal("partial List accepted")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if selection.listed || len(p.deletes) != 0 {
		t.Fatal("partial List selected or deleted versions")
	}
}

func TestDiskCheckpointReleaseRechecksStateAndExportOwnership(t *testing.T) {
	f := newDiskCheckpointFixture(t, "disk-release-state")
	p := serveDiskCheckpointPeer(t, f.sb)
	p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, false, false)
	release := holdTerminalSource(t, f.o, f.sb)
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, &diskCheckpointSelection{}); !errors.Is(err, errSourceOperationBusy) {
		t.Fatalf("live child = %v", err)
	}
	release()
	for _, state := range []types.State{types.StateStarting, types.StatePaused, types.StateDeleting, types.StateDead} {
		f.sb.State = state
		f.sb.LaunchMode = ""
		if state == types.StateStarting {
			f.sb.LaunchMode = types.LaunchImage
		}
		if state == types.StatePaused {
			f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
		}
		row := cloneSandbox(f.sb)
		if state == types.StateDead {
			row.RunID, row.RunDir, row.BaseDir, row.VswitchPort, row.FloatingIP, row.InnerIP, row.PortMAC = "", "", "", "", "", "", ""
			row.ResumeSource = types.ResumeSource{}
		}
		if err := f.o.st.Put(context.Background(), row); err != nil {
			t.Fatal(err)
		}
		if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, &diskCheckpointSelection{}); err != nil {
			t.Fatal(err)
		}
	}
	p.mu.Lock()
	if p.lists != 0 || len(p.deletes) != 0 {
		t.Error("non-running state issued runtime commands")
	}
	p.mu.Unlock()
	f.sb.State = types.StateRunning
	// A retained ResumeSource does not make versions live once running without
	// an active publisher. This is the normal successful restore state.
	f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	if err := f.o.st.Put(context.Background(), f.sb); err != nil {
		t.Fatal(err)
	}
	if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, &diskCheckpointSelection{}); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lists != 1 || len(p.deletes) != 1 {
		t.Fatal("retained ResumeSource blocked release")
	}
}

func TestDiskCheckpointReleaseRetryStopsAfterNewPauseOrRun(t *testing.T) {
	for _, successor := range []string{"paused", "new-run", "accepted-result"} {
		t.Run(successor, func(t *testing.T) {
			f := newDiskCheckpointFixture(t, "disk-release-recheck")
			p := serveDiskCheckpointPeer(t, f.sb)
			p.setup([]ctl.DiffCheckpointRef{{DiffID: "root", ID: "old"}}, false, true)
			selection := &diskCheckpointSelection{}
			if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err == nil {
				t.Fatal("expected uncertain delete")
			}
			switch successor {
			case "paused":
				f.sb.State = types.StatePaused
				f.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSandbox, Ref: "manifest://aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
			case "new-run":
				f.sb.RunID = "sr-00000000-0000-7000-8000-000000000999"
			case "accepted-result":
				addRunEndResult(t, f, f.sb.ID, f.sb.RunID)
			}
			if successor != "accepted-result" {
				if err := f.o.st.Put(context.Background(), f.sb); err != nil {
					t.Fatal(err)
				}
			}
			if err := f.o.releaseDiskCheckpointsOnce(context.Background(), f.sb.ID, selection); err != nil {
				t.Fatal(err)
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.lists != 1 || len(p.deletes) != 1 {
				t.Fatal("stale retry crossed successor state")
			}
		})
	}
}
