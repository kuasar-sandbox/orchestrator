package orch

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type blockedRestoreVS struct {
	entered    chan struct{}
	release    chan struct{}
	returned   chan struct{}
	once       sync.Once
	returnOnce sync.Once
	detaches   atomic.Int64
	detached   chan string
}

func newBlockedRestoreVS() *blockedRestoreVS {
	return &blockedRestoreVS{
		entered: make(chan struct{}), release: make(chan struct{}), returned: make(chan struct{}),
		detached: make(chan string, 4),
	}
}

func (v *blockedRestoreVS) Attach(ctx context.Context, _ vswitch.AttachReq) (*vswitch.Port, error) {
	v.once.Do(func() { close(v.entered) })
	select {
	case <-v.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	v.returnOnce.Do(func() { close(v.returned) })
	return &vswitch.Port{Port: "restore-worker-port", FloatingIP: "169.254.1.2", MAC: "02:00:00:00:00:91"}, nil
}

func (v *blockedRestoreVS) Detach(_ context.Context, port string) error {
	v.detaches.Add(1)
	select {
	case v.detached <- port:
	default:
	}
	return nil
}

func (*blockedRestoreVS) TapFD(port string) vswitch.TapFD {
	return vswitch.TapFD{Exec: []string{"true", port}}
}

type canceledRestoreVS struct {
	attaches atomic.Int64
}

func (v *canceledRestoreVS) Attach(ctx context.Context, _ vswitch.AttachReq) (*vswitch.Port, error) {
	v.attaches.Add(1)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &vswitch.Port{Port: "unexpected-port"}, nil
}

func (*canceledRestoreVS) Detach(context.Context, string) error { return nil }
func (*canceledRestoreVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{} }

type trackedRestoreVS struct {
	attaches atomic.Int64
	detaches atomic.Int64
}

func (v *trackedRestoreVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	v.attaches.Add(1)
	return &vswitch.Port{
		Port: "tracked-restore-port", FloatingIP: "169.254.1.2", MAC: "02:00:00:00:00:92",
	}, nil
}

func (v *trackedRestoreVS) Detach(context.Context, string) error {
	v.detaches.Add(1)
	return nil
}

func (*trackedRestoreVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func TestRestoreCancelAfterSummaryBeforeAttachHasNoNetworkOwnership(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &canceledRestoreVS{}
	o.vs = vs
	// Hold the existing allocation fence so the worker cannot enter Attach after
	// accepting the task summary.
	o.networkAllocationMu.Lock()
	locked := true
	defer func() {
		if locked {
			o.networkAllocationMu.Unlock()
		}
	}()
	req := createRequestFixture(t, o, "1")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("1", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	attempt := waitForAcceptedPrepare(t, o, created.ID)
	attempt.cancel()
	o.networkAllocationMu.Unlock()
	locked = false
	dead := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "dead after pre-attach cancel")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("canceled restore retained ownership: %+v", dead)
	}
	if got := vs.attaches.Load(); got != 1 {
		t.Fatalf("canceled Attach calls = %d, want one context-rejected call", got)
	}
}

func TestRestoreCancelAfterAssignmentBeforeBootstrap(t *testing.T) {
	cfg := &config.Config{}
	assigned := make(chan string, 1)
	gate := make(chan struct{})
	lc := &countingLauncher{assigned: assigned, connectGate: gate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	req := createRequestFixture(t, o, "0")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("0", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case sid := <-assigned:
		if sid != created.ID {
			t.Fatalf("assigned sandbox = %q, want %q", sid, created.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("restore runner was not assigned")
	}
	attempt, found := o.launches.Lookup(created.ID)
	if !found || attempt.RunID() == "" {
		t.Fatalf("assigned restore attempt = %+v, found=%t", attempt, found)
	}
	attempt.mu.Lock()
	prepared := attempt.prepare != nil
	attempt.mu.Unlock()
	if prepared {
		t.Fatal("task prepared snapshot before the bootstrap gate opened")
	}
	if killed, err := o.Kill(ctx, created.ID, req.APIKey); err != nil || !killed {
		t.Fatalf("Kill = %t, %v", killed, err)
	}
	close(gate)
	if stored, err := o.st.Get(ctx, created.ID); err != nil || stored != nil {
		t.Fatalf("canceled assigned restore = %+v, %v", stored, err)
	}
}

func TestRestoreCancelAfterAttachBeforeCASDetachesOwnedPort(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := newBlockedRestoreVS()
	o.vs = vs
	req := createRequestFixture(t, o, "2")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("2", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-vs.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("restore did not reach network attach")
	}
	// Keep the worker between a successful Attach return and its exact-run CAS.
	unlock := o.lifecycle.Lock(created.ID)
	close(vs.release)
	select {
	case <-vs.returned:
	case <-time.After(time.Second):
		unlock()
		t.Fatal("network Attach did not return")
	}
	attempt, found := o.launches.Lookup(created.ID)
	if !found {
		unlock()
		t.Fatal("restore attempt disappeared before cancellation")
	}
	attempt.cancel()
	unlock()
	dead := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "dead after pre-CAS cancel")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("canceled restore retained ownership: %+v", dead)
	}
	if got := vs.detaches.Load(); got != 1 {
		t.Fatalf("attached worker port detaches = %d, want 1", got)
	}
}

func TestRestoreTaskExitAfterFinalResponseRollsBackCommittedResources(t *testing.T) {
	cfg := &config.Config{}
	// Empty (non-nil) wire means the fake received its final spec and then
	// exited before exec could emit runtime readiness.
	lc := &countingLauncher{readinessWire: []byte{}}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &trackedRestoreVS{}
	o.vs = vs
	req := createRequestFixture(t, o, "3")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("3", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	dead := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "dead after post-final task exit")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("post-final task exit retained ownership: %+v", dead)
	}
	if vs.attaches.Load() != 1 || vs.detaches.Load() != 1 || lc.stops.Load() == 0 {
		t.Fatalf("post-final cleanup attaches=%d detaches=%d stops=%d", vs.attaches.Load(), vs.detaches.Load(), lc.stops.Load())
	}
}

func waitForAcceptedPrepare(t *testing.T, o *Orchestrator, sid string) *launchAttempt {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if attempt, found := o.launches.Lookup(sid); found {
			attempt.mu.Lock()
			accepted := attempt.prepare != nil
			attempt.mu.Unlock()
			if accepted {
				return attempt
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("snapshot prepare summary was not accepted")
	return nil
}

func TestRestoreExactRunNetworkCASMissDetachesUncommittedPort(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := newBlockedRestoreVS()
	o.vs = vs
	req := createRequestFixture(t, o, "4")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("4", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-vs.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("restore did not reach network attach")
	}
	bound := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.RunID != "" }, "exact runner binding")
	foreign := store.StartingResources{VswitchPort: "foreign-port", FloatingIP: "192.0.2.99", InnerIP: "169.254.1.1/31", PortMAC: "02:00:00:00:00:99"}
	changed, err := o.st.SetStartingResourcesForRun(ctx, created.ID, bound.RunID, foreign)
	if err != nil || !changed {
		t.Fatalf("install competing exact-run ownership = %t, %v", changed, err)
	}
	close(vs.release)
	dead := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "dead after network CAS miss")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("rollback retained ownership: %+v", dead)
	}
	select {
	case port := <-vs.detached:
		if port != "restore-worker-port" {
			t.Fatalf("detached port = %q", port)
		}
	case <-time.After(time.Second):
		t.Fatal("uncommitted worker port was not detached")
	}
}

func TestRestoreYAMLFailureAfterNetworkCommitRollsBackExactOwnership(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := newBlockedRestoreVS()
	o.vs = vs
	req := createRequestFixture(t, o, "5")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("5", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-vs.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("restore did not reach network attach")
	}
	// A directory at the exact YAML pathname makes the post-CAS config write
	// fail deterministically without changing production code.
	if err := os.MkdirAll(o.sandboxConfigPath(created), 0o700); err != nil {
		t.Fatal(err)
	}
	close(vs.release)
	dead := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateDead }, "dead after YAML failure")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("rollback retained ownership: %+v", dead)
	}
	if got := vs.detaches.Load(); got != 1 {
		t.Fatalf("network detaches = %d, want 1", got)
	}
	for _, dir := range []string{created.RunDir, created.BaseDir} {
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("rollback retained %s: %v", dir, err)
		}
	}
}

func TestRestoreMalformedInheritedNetworkIsBestEffort(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{snapshotSummary: &configsock.SnapshotPrepareSummary{
		SchemaVersion:      configsock.SnapshotPrepareSchemaVersion,
		Capacity:           configsock.SnapshotCapacity{CPU: 2, Memory: "2GiB"},
		RawNetworkMetadata: "{malformed",
		ResolutionDigest:   strings.Repeat("a", 64),
		RequiredRefCount:   1,
	}}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	req := createRequestFixture(t, o, "6")
	req.TemplateID = types.TemplateID{
		Profile: types.ProfileBare, Kind: types.KindSnp,
		Ref: "manifest://" + strings.Repeat("6", 64),
	}.String()
	created, err := o.Create(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	running := waitForSandbox(t, o, ctx, created.ID, func(sb *types.Sandbox) bool { return sb.State == types.StateRunning }, "running with malformed inherited network")
	if running.VswitchPort == "" {
		t.Fatalf("running sandbox has no network ownership: %+v", running)
	}
}

func TestValidateSandboxPrepareSummaryDiscardsPartiallyParsedNetwork(t *testing.T) {
	summary := configsock.SnapshotPrepareSummary{
		SchemaVersion:      configsock.SnapshotPrepareSchemaVersion,
		Capacity:           configsock.SnapshotCapacity{CPU: 2, Memory: "2GiB"},
		RawNetworkMetadata: `{"hostname":"must-not-survive","dns":`,
		ResolutionDigest:   strings.Repeat("a", 64),
		RequiredRefCount:   1,
	}
	_, network, err := validateSandboxPrepareSummary(summary)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(network, sandboxcfg.NetworkSpec{}) {
		t.Fatalf("partially parsed malformed network survived: %+v", network)
	}
}

func TestSandboxTaskBootstrapUsesExactRunAndDoesNoArtifactIO(t *testing.T) {
	cfg := &config.Config{ManifestConfig: "manifest.yaml"}
	o := testOrchCfg(t, cfg)
	manifestKey := strings.Repeat("7", 64)
	rootRef := "manifest://" + strings.Repeat("8", 64)
	sb := &types.Sandbox{
		ID: "task-bootstrap", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: rootRef}.String(),
		State:      types.StateStarting, RunID: "run-current",
		RunDir: filepath.Join(t.TempDir(), "run", "task-bootstrap"), BaseDir: filepath.Join(t.TempDir(), "base", "task-bootstrap"),
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.InsertSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	attempt, err := o.launches.Claim(context.Background(), sb.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	defer o.launches.Finish(attempt, nil)
	attempt.SetRunID(sb.RunID)
	deadline := time.Now().Add(time.Minute)
	attempt.SetDeadline(deadline)

	if _, found, err := o.SandboxTaskAuth(context.Background(), sb.ID, "stale-run"); err != nil || found {
		t.Fatalf("stale auth = %t, %v", found, err)
	}
	auth, found, err := o.SandboxTaskAuth(context.Background(), sb.ID, sb.RunID)
	if err != nil || !found || auth.PidFile != sb.PidFile() {
		t.Fatalf("exact auth = %+v, %t, %v", auth, found, err)
	}
	task, found, err := o.SandboxTaskSpecFor(context.Background(), sb.ID, sb.RunID)
	if err != nil || !found || task.Prepare == nil || task.Final != nil {
		t.Fatalf("task bootstrap = %+v, %t, %v", task, found, err)
	}
	if task.Prepare.RootRef != rootRef || task.Env["MANIFEST_KEY"] != manifestKey || task.Prepare.AbsoluteDeadlineUnixNano != deadline.UnixNano() {
		t.Fatalf("task bootstrap content = %+v", task)
	}
}

func TestSandboxTaskBootstrapNormalizesRawLocalRoot(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "run", "local-root")
	got, err := normalizeSandboxTaskRootRef("checkpoint.snapshot", runDir)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(runDir, "checkpoint.snapshot")
	if got != want {
		t.Fatalf("normalized root = %q, want %q", got, want)
	}
	for _, portable := range []string{
		"manifest://" + strings.Repeat("a", 64),
		"file://relative.snapshot@sha256:" + strings.Repeat("b", 64),
	} {
		if got, err := normalizeSandboxTaskRootRef(portable, runDir); err != nil || got != portable {
			t.Fatalf("portable root %q normalized to %q, %v", portable, got, err)
		}
	}
}
