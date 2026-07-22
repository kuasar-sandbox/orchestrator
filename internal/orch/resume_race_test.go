package orch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// countingLauncher records how many times a unit was started — the launch count.
type countingLauncher struct {
	starts                atomic.Int64
	orch                  *Orchestrator
	startEntered          chan struct{}
	startRelease          <-chan struct{}
	startOnce             sync.Once
	blockBeforeAssignment bool
	stopped               sync.Map
}

func (l *countingLauncher) Start(ctx context.Context, unit string) error {
	l.starts.Add(1)
	if l.blockBeforeAssignment {
		if l.startEntered != nil {
			l.startOnce.Do(func() { close(l.startEntered) })
		}
		if l.startRelease != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-l.startRelease:
			}
		}
	}
	if l.orch != nil {
		prefix := strings.TrimSuffix(l.orch.cfg.Units.Runner, ".service")
		if strings.HasPrefix(unit, prefix) {
			runID := l.orch.unitToRunID(unit)
			// The systemd StartUnit call context only bounds the D-Bus job. The
			// launched process has its own lifetime and keeps waiting afterward.
			go func() { _, _, _ = l.orch.WaitAssignment(context.Background(), runKindSandbox, runID) }()
		}
	}
	if l.startEntered != nil && !l.blockBeforeAssignment {
		l.startOnce.Do(func() { close(l.startEntered) })
	}
	if l.startRelease != nil && !l.blockBeforeAssignment {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.startRelease:
		}
	}
	return nil
}
func (l *countingLauncher) Stop(_ context.Context, unit string) error {
	l.stopped.Store(unit, struct{}{})
	return nil
}
func (l *countingLauncher) ResetFailed(context.Context, string) error { return nil }
func (l *countingLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	return nil, nil
}
func (l *countingLauncher) Reload(context.Context) error { return nil }
func (l *countingLauncher) Close() error                 { return nil }

// stubVS is a no-op vswitch that hands back a fixed port (satisfies vsClient).
type stubVS struct{}

func (stubVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return &vswitch.Port{Port: "1", FloatingIP: "169.254.1.2", MAC: "02:00:00:00:00:01", InnerIP: "169.254.1.1"}, nil
}
func (stubVS) Detach(context.Context, string) error { return nil }
func (stubVS) TapFD(port string) vswitch.TapFD      { return vswitch.TapFD{Exec: []string{"true", port}} }

type releaseAdmissionFake struct {
	wake     chan struct{}
	released []string
}

func sandboxTestEvent(update nodeexec.EventUpdate) nodeexec.EventUpdate {
	presentation := clusterstate.SandboxPresentationV1{
		CPUCount: 1, MemoryMB: 512, DiskSizeMB: 64, EnvdVersion: "0.1.0", StartedAt: 1, EndAt: 2,
	}
	update.Presentation = &presentation
	return update
}

func (f *releaseAdmissionFake) GetAdmission(id, digest string) (nodectl.PreparedAdmissionResult, error) {
	return nodectl.PreparedAdmissionResult{SandboxID: id, DemandDigest: digest, State: nodectl.PreparedClaimed}, nil
}
func (f *releaseAdmissionFake) PrepareAdmission(id, digest string, _ nodectl.SandboxAdmissionDemand) (nodectl.PreparedAdmissionResult, error) {
	return f.GetAdmission(id, digest)
}
func (f *releaseAdmissionFake) ClaimAdmission(id, digest string) (nodectl.PreparedAdmissionResult, error) {
	return f.GetAdmission(id, digest)
}
func (f *releaseAdmissionFake) ReleaseAdmission(id, digest, reason string) (nodectl.PreparedAdmissionResult, error) {
	f.released = append(f.released, id+":"+reason)
	return nodectl.PreparedAdmissionResult{
		SandboxID: id, DemandDigest: digest, State: nodectl.PreparedReleased, Reason: reason,
	}, nil
}

func TestStartupTerminalizesDeadRunningClusterSandbox(t *testing.T) {
	ctx := context.Background()
	o := testOrch(t)
	sid := "sandbox-dead-after-restart"
	templateRef := "bare-img-" + strings.Repeat("b", 64)
	dispatch := clusterResumeDispatch(t, sid, templateRef)
	sandbox := &types.Sandbox{
		ID: sid, TemplateID: templateRef, State: types.StateStarting,
		AuthKey: strings.Repeat("a", 64), ManifestKey: strings.Repeat("b", 64),
		CreatedUnix: 1,
	}
	if _, err := o.st.RecordSandboxWorkflow(ctx, dispatch, nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-dead",
	}, sandbox); err != nil {
		t.Fatal(err)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil {
		t.Fatalf("stored Sandbox = %+v, %v", stored, err)
	}
	if _, err := o.st.CommitSandboxEvent(ctx, stored, sandboxTestEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: 49983,
	})); err != nil {
		t.Fatal(err)
	}
	if err := o.st.SetState(ctx, sid, types.StateDead); err != nil {
		t.Fatal(err)
	}
	admission := &releaseAdmissionFake{wake: make(chan struct{})}
	authority, err := nodeexec.NewAuthority(
		o.st, admission,
		func(context.Context) (nodeexec.LocalSessionIdentity, error) {
			return nodeexec.LocalSessionIdentity{
				NodeID: "node-1", NodeEpoch: 7, SessionSeq: 1, DataEndpoint: "node-1:8443",
			}, nil
		},
		func(context.Context) (nodeexec.BuildCapacity, string, error) {
			return nodeexec.BuildCapacity{Slots: 1, QueueLimit: 1}, "", nil
		},
		func(context.Context, nodeexec.DispatchRecord) (*types.Build, error) { return nil, nil },
		func(context.Context, nodeexec.DispatchRecord) (*types.Sandbox, error) { return nil, nil },
		func(context.Context, nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
			return nodectl.SandboxAdmissionDemand{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	node := &FinalClusterNode{
		store: o.st, authority: authority,
		session: &ClusterSession{nodeID: "node-1", nodeEpoch: 7},
	}
	if err := node.reconcileDeadRunningSandboxes(ctx, 10); err != nil {
		t.Fatal(err)
	}
	record, err := o.st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sid)
	if err != nil || record == nil || record.AdmissionState != nodeexec.AdmissionTerminal ||
		record.ResourceClaimed || record.LatestEvent == nil || record.LatestEvent.State != "ERROR" {
		t.Fatalf("terminal workflow = %+v, %v", record, err)
	}
	if len(admission.released) != 1 || !strings.Contains(admission.released[0], "runner is absent") {
		t.Fatalf("Admission release = %v", admission.released)
	}
	events, _, err := o.st.PendingExecutionEvents(
		ctx, "node-1", 7, routesync.EventCursor{}, 10, routesync.MaxExecutionEventBytes,
	)
	if err != nil || len(events) != 1 || events[0].State != "ERROR" {
		t.Fatalf("durable terminal outbox = %+v, %v", events, err)
	}
}
func (*releaseAdmissionFake) FinalizeAdmission(string, string) error { return nil }
func (*releaseAdmissionFake) PromoteQueued() ([]nodectl.PreparedAdmissionResult, error) {
	return nil, nil
}
func (f *releaseAdmissionFake) Wake() <-chan struct{} { return f.wake }

// TestResumeRace_ConnectAndRouteSingleLaunch is the regression guard for the
// control-plane resume race: a paused sandbox hit concurrently by /connect
// (control plane) and proxy traffic (data plane) must resume exactly once.
//
// Before the fix, Connect called resume directly while Route/OnWake went through
// the per-sid single-flight, so the two paths could both launch — double port
// attach + double unit start — and they mutated the shared cached *Sandbox
// without synchronization (a data race). With Connect routed through the same
// single-flight and resume publishing an immutable copy, this is one launch and
// `go test -race` is clean.
func TestResumeRace_ConnectAndRouteSingleLaunch(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Sandbox.Resources.VCPU, cfg.Sandbox.Resources.Memory = 1, "512MiB"
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "lib")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31" // allocInnerIP needs a valid CIDR

	lc := &countingLauncher{}
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc.orch = o
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}

	// bare-img with no snapshot ref → launch reaches lc.Start without the e2b
	// readiness wait or the snapshot-probe exec (RestoreRefFor returns "").
	mk := strings.Repeat("a", 64)
	raw, _ := hex.DecodeString(mk)
	apiKey, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	sid := "sbx-race-1"
	sb := &types.Sandbox{
		ID: sid, TemplateID: "bare-img-" + strings.Repeat("b", 64), State: types.StatePaused,
		AuthKey: mk, ManifestKey: mk,
		RunDir:      cfg.Paths.RunRoot + "/" + sid,
		BaseDir:     cfg.Paths.BaseRoot + "/" + sid,
		CreatedUnix: 1,
	}
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	// Fire /connect, several data-plane Route calls, and a couple of read-only
	// MMDS lookups at the same paused sandbox simultaneously (released together
	// for maximal contention on the cached pointer).
	var wg sync.WaitGroup
	release := make(chan struct{})
	launchG := func(fn func()) { wg.Add(1); go func() { defer wg.Done(); <-release; fn() }() }

	launchG(func() { _, _ = o.Connect(ctx, sid, apiKey, "", 60) })
	for i := 0; i < 8; i++ {
		launchG(func() { _, _ = o.Route(ctx, proxy.RouteRequest{SandboxID: sid, Port: 49983}) })
	}
	for i := 0; i < 4; i++ {
		launchG(func() { _, _ = o.ByFloatingIP("169.254.1.2"); _, _, _ = o.SandboxInfo(sid) })
	}
	close(release)
	wg.Wait()

	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launch (lc.Start) called %d times; want exactly 1 — concurrent connect+route must collapse to one resume", got)
	}
	got, err := st.Get(ctx, sid)
	if err != nil || got == nil || got.State != types.StateRunning {
		t.Fatalf("sandbox should be running after resume: %+v (err=%v)", got, err)
	}
}

func TestResumeRace_RegistryRetriesShareDataPlaneSingleFlight(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Sandbox.Resources.VCPU, cfg.Sandbox.Resources.Memory = 1, "512MiB"
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "lib")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	startEntered := make(chan struct{})
	startRelease := make(chan struct{})
	lc := &countingLauncher{startEntered: startEntered, startRelease: startRelease, blockBeforeAssignment: true}
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc.orch = o
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}

	sid := "sbx-cluster-resume-race"
	templateRef := "bare-img-" + strings.Repeat("b", 64)
	dispatch := clusterResumeDispatch(t, sid, templateRef)
	sandbox := &types.Sandbox{
		ID: sid, TemplateID: templateRef, State: types.StatePaused,
		AuthKey: strings.Repeat("a", 64), ManifestKey: strings.Repeat("b", 64),
		RunDir: cfg.Paths.RunRoot + "/" + sid, BaseDir: cfg.Paths.BaseRoot + "/" + sid,
		CreatedUnix: 1,
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-1",
	}, sandbox); err != nil {
		t.Fatal(err)
	}
	stored, err := st.Get(ctx, sid)
	if err != nil || stored == nil {
		t.Fatalf("stored cluster sandbox = %+v, %v", stored, err)
	}
	if _, err := st.CommitSandboxEvent(ctx, stored, sandboxTestEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: 49983,
	})); err != nil {
		t.Fatal(err)
	}
	stored, _ = st.Get(ctx, sid)
	if _, err := st.CommitSandboxEvent(ctx, stored, sandboxTestEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRoutePaused), TargetPort: 49983,
	})); err != nil {
		t.Fatal(err)
	}
	stored, _ = st.Get(ctx, sid)
	o.cache(stored)

	node := &FinalClusterNode{core: o, store: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	command := &routesync.Command{SID: sid}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); node.resumeSandbox(ctx, command) }()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("first cluster resume did not reach launcher")
	}
	wg.Add(1)
	go func() { defer wg.Done(); node.resumeSandbox(ctx, command) }()
	time.Sleep(25 * time.Millisecond)
	close(startRelease)
	wg.Wait()

	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("Registry resume retries launched %d sandbox units, want 1", got)
	}
	stored, err = st.Get(ctx, sid)
	if err != nil || stored == nil || stored.State != types.StateRunning {
		t.Fatalf("sandbox after deduplicated cluster resume = %+v, %v", stored, err)
	}
}

func TestResumeRace_RegistryDeleteWaitsForInFlightResume(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Sandbox.Resources.VCPU, cfg.Sandbox.Resources.Memory = 1, "512MiB"
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "lib")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	startEntered := make(chan struct{})
	startRelease := make(chan struct{})
	lc := &countingLauncher{startEntered: startEntered, startRelease: startRelease, blockBeforeAssignment: true}
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc.orch = o
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}

	sid := "sbx-cluster-delete-race"
	templateRef := "bare-img-" + strings.Repeat("b", 64)
	dispatch := clusterResumeDispatch(t, sid, templateRef)
	sandbox := &types.Sandbox{
		ID: sid, TemplateID: templateRef, State: types.StatePaused,
		AuthKey: strings.Repeat("a", 64), ManifestKey: strings.Repeat("b", 64),
		RunDir: cfg.Paths.RunRoot + "/" + sid, BaseDir: cfg.Paths.BaseRoot + "/" + sid,
		CreatedUnix: 1,
	}
	if _, err := st.RecordSandboxWorkflow(ctx, dispatch, nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-1",
	}, sandbox); err != nil {
		t.Fatal(err)
	}
	stored, _ := st.Get(ctx, sid)
	if _, err := st.CommitSandboxEvent(ctx, stored, sandboxTestEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: 49983,
	})); err != nil {
		t.Fatal(err)
	}
	stored, _ = st.Get(ctx, sid)
	if _, err := st.CommitSandboxEvent(ctx, stored, sandboxTestEvent(nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRoutePaused), TargetPort: 49983,
	})); err != nil {
		t.Fatal(err)
	}

	authority, err := nodeexec.NewAuthority(
		st, &releaseAdmissionFake{wake: make(chan struct{})},
		func(context.Context) (nodeexec.LocalSessionIdentity, error) {
			return nodeexec.LocalSessionIdentity{NodeID: "node-1", NodeEpoch: 7, SessionSeq: 1, DataEndpoint: "node-1:8443"}, nil
		},
		func(context.Context) (nodeexec.BuildCapacity, string, error) {
			return nodeexec.BuildCapacity{Slots: 1, QueueLimit: 1}, "", nil
		},
		func(context.Context, nodeexec.DispatchRecord) (*types.Build, error) { return nil, nil },
		func(context.Context, nodeexec.DispatchRecord) (*types.Sandbox, error) { return nil, nil },
		func(context.Context, nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
			return nodectl.SandboxAdmissionDemand{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	node := &FinalClusterNode{
		core: o, store: st, authority: authority,
		log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	command := &routesync.Command{SID: sid}
	resumeDone := make(chan struct{})
	go func() { node.resumeSandbox(ctx, command); close(resumeDone) }()
	select {
	case <-startEntered:
	case <-time.After(time.Second):
		t.Fatal("resume did not reach launcher")
	}
	deleteDone := make(chan struct{})
	go func() { node.deleteSandbox(ctx, command); close(deleteDone) }()
	select {
	case <-deleteDone:
		t.Fatal("delete bypassed the in-flight resume lifecycle lock")
	case <-time.After(25 * time.Millisecond):
	}
	close(startRelease)
	<-resumeDone
	<-deleteDone

	stored, err = st.Get(ctx, sid)
	if err != nil || stored != nil {
		t.Fatalf("sandbox after serialized delete = %+v, %v", stored, err)
	}
	if exists, err := st.ExecutionObjectExists(ctx, clusterstate.ExecutionKindSandbox, sid); err != nil || exists {
		t.Fatalf("deleted Sandbox remains occupied = %v, %v", exists, err)
	}
	record, err := st.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, sid)
	if err != nil || record == nil || record.ObjectState != "DELETED" {
		t.Fatalf("workflow after serialized delete = %+v, %v", record, err)
	}
}

func TestFailedResumeStopsDurablyAssignedRuntimeBeforeResourceRelease(t *testing.T) {
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := &config.Config{}
	cfg.Sandbox.Resources.VCPU, cfg.Sandbox.Resources.Memory = 1, "512MiB"
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "lib")
	cfg.Units.Runner = "sandbox-runner@.service"
	lc := &countingLauncher{}
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sid := "sbx-failed-resume-cleanup"
	dispatch := clusterResumeDispatch(t, sid, "bare-img-"+strings.Repeat("b", 64))
	paused := &types.Sandbox{
		ID: sid, TemplateID: "bare-img-" + strings.Repeat("b", 64), State: types.StatePaused,
		AuthKey: strings.Repeat("a", 64), ManifestKey: strings.Repeat("b", 64),
		RunDir: cfg.Paths.RunRoot + "/" + sid, BaseDir: cfg.Paths.BaseRoot + "/" + sid,
		CreatedUnix: 1,
	}
	record, err := st.RecordSandboxWorkflow(context.Background(), dispatch, nodeexec.AdmissionDecision{
		State: nodeexec.AdmissionAdmitted, Result: clusterstate.DispatchAcceptedAdmitted,
		ReservationToken: "reservation-1",
	}, paused)
	if err != nil {
		t.Fatal(err)
	}
	actual := *paused
	actual.State = types.StateRunning
	actual.RunID = "run-after-resume"
	actual.VswitchPort = "new-port"
	if err := st.Put(context.Background(), &actual); err != nil {
		t.Fatal(err)
	}
	authority, err := nodeexec.NewAuthority(
		st, &releaseAdmissionFake{wake: make(chan struct{})},
		func(context.Context) (nodeexec.LocalSessionIdentity, error) {
			return nodeexec.LocalSessionIdentity{
				NodeID: "node-1", NodeEpoch: 7, SessionSeq: 1, DataEndpoint: "node-1:8443",
			}, nil
		},
		func(context.Context) (nodeexec.BuildCapacity, string, error) {
			return nodeexec.BuildCapacity{Slots: 1, QueueLimit: 1}, "", nil
		},
		func(context.Context, nodeexec.DispatchRecord) (*types.Build, error) { return nil, nil },
		func(context.Context, nodeexec.DispatchRecord) (*types.Sandbox, error) { return nil, nil },
		func(context.Context, nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
			return nodectl.SandboxAdmissionDemand{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	node := &FinalClusterNode{core: o, store: st, authority: authority}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := node.failResumedSandbox(ctx, record, paused, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("failed resume cleanup error = %v", err)
	}
	if _, stopped := lc.stopped.Load(o.runnerUnit(actual.RunID)); !stopped {
		t.Fatalf("durably assigned runtime %q was not stopped", actual.RunID)
	}
	stored, err := st.Get(context.Background(), sid)
	if err != nil || stored == nil || stored.State != types.StateDead {
		t.Fatalf("Sandbox after failed resume = %+v, %v", stored, err)
	}
	workflow, err := st.GetNodeWorkflow(context.Background(), clusterstate.ExecutionKindSandbox, sid)
	if err != nil || workflow == nil || workflow.ObjectState != "ERROR" || workflow.ResourceClaimed {
		t.Fatalf("workflow after failed resume = %+v, %v", workflow, err)
	}
}

func clusterResumeDispatch(t *testing.T, sid, templateRef string) nodeexec.DispatchRecord {
	t.Helper()
	normalized, err := placement.NormalizeSandboxDemand(placement.SandboxDemand{
		SlotUnits: 1, FloorMemory: 512 << 20, StartupBudgetMemory: 512 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	spec, err := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "access-token", TargetPort: 49983,
		Request: clusterstate.NodeRequestEnvelopeV1{
			Version: clusterstate.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes",
			Body: []byte(`{"metadata":null,"templateID":"` + templateRef + `","timeout":0}`),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	demandDigest := sha256.Sum256(normalized)
	specDigest := sha256.Sum256(spec)
	binding, err := clusterstate.EncodeExecutionBinding(clusterstate.ExecutionBinding{
		RegistryGeneration: "generation-1", Kind: clusterstate.ExecutionKindSandbox,
		ObjectID: sid, Group: "/group", RouteKey: "route-1", NodeID: "node-1", NodeEpoch: 7,
		DemandDigest: demandDigest, DispatchSpecDigest: specDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	bindingDigest, err := clusterstate.ExecutionBindingDigest(binding)
	if err != nil {
		t.Fatal(err)
	}
	return nodeexec.DispatchRecord{
		Kind: clusterstate.ExecutionKindSandbox, ObjectID: sid, Group: "/group", RouteKey: "route-1",
		NodeID: "node-1", NodeEpoch: 7, SessionSeq: 1, DataEndpoint: "node-1:8443",
		NormalizedDemand: normalized, DemandDigest: hex.EncodeToString(demandDigest[:]),
		DispatchSpec: spec, DispatchSpecDigest: hex.EncodeToString(specDigest[:]),
		ProviderPolicyVersion: "provider-v1", OpaqueBinding: binding, BindingDigest: bindingDigest,
	}
}

func TestCacheIfAbsentDoesNotPublishStaleStoreRead(t *testing.T) {
	o := &Orchestrator{reg: make(map[string]*types.Sandbox)}
	running := &types.Sandbox{ID: "sbx-race-1", State: types.StateRunning}
	stale := &types.Sandbox{ID: running.ID, State: types.StatePaused}
	o.cache(running)

	if got := o.cacheIfAbsent(stale); got != running {
		t.Fatalf("cacheIfAbsent returned stale store row: %+v", got)
	}
	if got := o.lookup(running.ID); got != running {
		t.Fatalf("cache was overwritten by stale store row: %+v", got)
	}
}
