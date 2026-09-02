package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type sandboxFinalizerLauncher struct {
	mu       sync.Mutex
	unit     string
	state    string
	stopErr  error
	resetErr error
	listErr  error
	stops    int
	resets   int
}

func (*sandboxFinalizerLauncher) Start(context.Context, string) error { return nil }
func (l *sandboxFinalizerLauncher) Stop(_ context.Context, unit string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stops++
	if l.stopErr != nil {
		return l.stopErr
	}
	if unit == l.unit {
		l.state = "inactive"
	}
	return nil
}
func (l *sandboxFinalizerLauncher) ResetFailed(context.Context, string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.resets++
	return l.resetErr
}
func (l *sandboxFinalizerLauncher) List(_ context.Context, pattern string) ([]launcher.Unit, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.listErr != nil {
		return nil, l.listErr
	}
	if l.unit == "" {
		return nil, nil
	}
	matched, _ := filepath.Match(pattern, l.unit)
	if !matched {
		return nil, nil
	}
	return []launcher.Unit{{Name: l.unit, ActiveState: l.state}}, nil
}
func (*sandboxFinalizerLauncher) Reload(context.Context) error { return nil }
func (*sandboxFinalizerLauncher) SetResources(context.Context, string, launcher.ResourceProperties) error {
	return nil
}
func (*sandboxFinalizerLauncher) Resources(context.Context, string, string) (launcher.ResourceProperties, error) {
	return launcher.ResourceProperties{}, nil
}
func (*sandboxFinalizerLauncher) Close() error { return nil }

func (l *sandboxFinalizerLauncher) clearFaults() {
	l.mu.Lock()
	l.stopErr, l.resetErr, l.listErr = nil, nil, nil
	l.mu.Unlock()
}

type sandboxFinalizerVS struct {
	mu        sync.Mutex
	detachErr error
	detached  bool
	calls     int
}

func (*sandboxFinalizerVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return nil, nil
}
func (v *sandboxFinalizerVS) Detach(context.Context, string) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.calls++
	if v.detachErr != nil {
		return v.detachErr
	}
	if v.detached {
		return vswitch.ErrPortNotAttached
	}
	v.detached = true
	return nil
}
func (*sandboxFinalizerVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func (v *sandboxFinalizerVS) clearFault() {
	v.mu.Lock()
	v.detachErr = nil
	v.mu.Unlock()
}

type sandboxFinalizerFixture struct {
	o      *Orchestrator
	sb     *types.Sandbox
	apiKey string
	dbPath string
	lc     *sandboxFinalizerLauncher
	vs     *sandboxFinalizerVS
}

func newSandboxFinalizerFixture(t *testing.T, id string) sandboxFinalizerFixture {
	t.Helper()
	root := t.TempDir()
	cfg := &config.Config{}
	cfg.Paths.RunRoot = filepath.Join(root, "run")
	cfg.Paths.BaseRoot = filepath.Join(root, "base")
	cfg.Units.Runner = "sandbox-runner@.service"
	runID := "sr-00000000-0000-7000-8000-000000000287"
	unit := "sandbox-runner@" + runID + ".service"
	lc := &sandboxFinalizerLauncher{unit: unit, state: "active"}
	vs := &sandboxFinalizerVS{}
	dbPath := filepath.Join(root, "node.db")
	o := testOrchCfgAt(t, cfg, dbPath)
	o.lc, o.vs = lc, vs
	manifestKey := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: id, Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}.String(),
		State:      types.StateRunning, RunID: runID, VswitchPort: "port-287", FloatingIP: "192.0.2.87",
		RunDir: nodepath.SandboxRunDir(cfg.Paths.RunRoot, id), BaseDir: nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, id),
		APISecret: apiSecret, ManifestKey: manifestKey, CreatedUnix: 287,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "owned"), []byte("owned"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	o.cache(sb)
	return sandboxFinalizerFixture{o: o, sb: sb, apiKey: apiKey, dbPath: dbPath, lc: lc, vs: vs}
}

func beginDeletingForTest(t *testing.T, fixture sandboxFinalizerFixture) {
	t.Helper()
	changed, err := fixture.o.st.BeginSandboxDelete(context.Background(), fixture.sb)
	if err != nil || !changed {
		t.Fatalf("BeginSandboxDelete = %t, %v", changed, err)
	}
}

func assertDeletingOwnership(t *testing.T, fixture sandboxFinalizerFixture) *types.Sandbox {
	t.Helper()
	got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || got == nil {
		t.Fatalf("deleting owner = %+v, %v", got, err)
	}
	if got.State != types.StateDeleting || got.RunID != fixture.sb.RunID || got.VswitchPort != fixture.sb.VswitchPort ||
		got.RunDir != fixture.sb.RunDir || got.BaseDir != fixture.sb.BaseDir {
		t.Fatalf("cleanup failure lost deleting ownership: %+v", got)
	}
	return got
}

func TestSandboxDeleteFinalizerRetainsOwnershipAtEveryFailure(t *testing.T) {
	fault := errors.New("injected finalizer failure")
	for _, test := range []struct {
		name          string
		inject        func(*sandboxFinalizerFixture)
		clear         func(*sandboxFinalizerFixture)
		wantRunExists bool
		wantBaseExist bool
	}{
		{
			name:          "stop",
			inject:        func(f *sandboxFinalizerFixture) { f.lc.stopErr = fault },
			clear:         func(f *sandboxFinalizerFixture) { f.lc.clearFaults() },
			wantRunExists: true, wantBaseExist: true,
		},
		{
			name:          "reset",
			inject:        func(f *sandboxFinalizerFixture) { f.lc.resetErr = fault },
			clear:         func(f *sandboxFinalizerFixture) { f.lc.clearFaults() },
			wantRunExists: true, wantBaseExist: true,
		},
		{
			name:          "detach",
			inject:        func(f *sandboxFinalizerFixture) { f.vs.detachErr = fault },
			clear:         func(f *sandboxFinalizerFixture) { f.vs.clearFault() },
			wantRunExists: true, wantBaseExist: true,
		},
		{
			name: "run-dir",
			inject: func(f *sandboxFinalizerFixture) {
				f.o.removeSandboxRunDir = func(string) error { return fault }
			},
			clear:         func(f *sandboxFinalizerFixture) { f.o.removeSandboxRunDir = os.RemoveAll },
			wantRunExists: true, wantBaseExist: true,
		},
		{
			name: "base-dir",
			inject: func(f *sandboxFinalizerFixture) {
				f.o.removeSandboxBaseDir = func(string) error { return fault }
			},
			clear:         func(f *sandboxFinalizerFixture) { f.o.removeSandboxBaseDir = os.RemoveAll },
			wantRunExists: false, wantBaseExist: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSandboxFinalizerFixture(t, "delete-fault-"+test.name)
			test.inject(&fixture)
			beginDeletingForTest(t, fixture)
			if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); !errors.Is(err, fault) {
				t.Fatalf("first finalizer error = %v", err)
			}
			assertDeletingOwnership(t, fixture)
			for path, wantExists := range map[string]bool{
				fixture.sb.RunDir: test.wantRunExists, fixture.sb.BaseDir: test.wantBaseExist,
			} {
				_, err := os.Stat(path)
				if wantExists && err != nil {
					t.Fatalf("owned path %s removed after failure: %v", path, err)
				}
				if !wantExists && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("completed cleanup path %s still exists: %v", path, err)
				}
			}
			test.clear(&fixture)
			if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); err != nil {
				t.Fatalf("retry finalizer: %v", err)
			}
			if got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID); err != nil || got != nil {
				t.Fatalf("finalized row = %+v, %v", got, err)
			}
			for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
				if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("finalizer retained %s: %v", path, err)
				}
			}
		})
	}
}

func TestKillDurablyAcceptsDeletingAndRetriesToTerminalObservation(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-live-retry")
	recorder := &objectObserverRecorder{}
	fixture.o.SetExtensionObserver(recorder)
	events, cancel := fixture.o.Subscribe()
	defer cancel()
	var failRunDir atomic.Bool
	failRunDir.Store(true)
	var removeCalls atomic.Int32
	fixture.o.removeSandboxRunDir = func(path string) error {
		removeCalls.Add(1)
		if failRunDir.Load() {
			return errors.New("transient RunDir failure")
		}
		return os.RemoveAll(path)
	}

	if killed, err := fixture.o.Kill(context.Background(), fixture.sb.ID, fixture.apiKey); err != nil || !killed {
		t.Fatalf("Kill = %t, %v", killed, err)
	}
	deadline := time.Now().Add(time.Second)
	for removeCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if removeCalls.Load() == 0 {
		t.Fatal("delete finalizer did not reach injected RunDir failure")
	}
	deleting := assertDeletingOwnership(t, fixture)
	if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
		t.Fatalf("terminal observer published before cleanup: %v", kinds)
	}
	var routed []routesync.RouteEntry
	if err := fixture.o.Range(context.Background(), func(entry routesync.RouteEntry) error {
		routed = append(routed, entry)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range routed {
		if entry.SandboxID == deleting.ID {
			t.Fatalf("deleting sandbox remained in route snapshot: %+v", entry)
		}
	}
	if _, err := fixture.o.Connect(context.Background(), deleting.ID, fixture.apiKey, "", api.ConnectOptions{}); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("Connect deleting sandbox = %v", err)
	}
	if _, err := fixture.o.ExecSession(context.Background(), deleting.ID, fixture.apiKey, "", 0, nil); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("ExecSession deleting sandbox = %v", err)
	}
	fixture.o.OnWake(context.Background(), deleting.ID)
	if _, found := fixture.o.launches.Lookup(deleting.ID); found {
		t.Fatal("Wake activated a deleting sandbox")
	}
	select {
	case event := <-events:
		t.Fatalf("terminal route event published before cleanup: %+v", event)
	default:
	}
	if err := fixture.o.Pause(context.Background(), deleting.ID, fixture.apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("Pause deleting sandbox = %v", err)
	}
	oldDeadline := deleting.DeadlineUnix
	if changed, err := fixture.o.SetTimeout(context.Background(), deleting.ID, fixture.apiKey, 60); err != nil || changed {
		t.Fatalf("SetTimeout deleting sandbox = %t, %v", changed, err)
	}
	stillDeleting, err := fixture.o.st.Get(context.Background(), deleting.ID)
	if err != nil || stillDeleting == nil || stillDeleting.State != types.StateDeleting || stillDeleting.DeadlineUnix != oldDeadline {
		t.Fatalf("SetTimeout changed deleting history = %+v, %v", stillDeleting, err)
	}
	if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
		t.Fatalf("SetTimeout re-observed deleting sandbox: %v", kinds)
	}
	if killed, err := fixture.o.Kill(context.Background(), deleting.ID, fixture.apiKey); err != nil || !killed {
		t.Fatalf("repeated Kill = %t, %v", killed, err)
	}

	failRunDir.Store(false)
	waitForSandboxAbsent(t, fixture.o, context.Background(), fixture.sb.ID, "live finalizer retry")
	if err := fixture.o.DrainSandboxDeletes(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		if event.Kind != routesync.TypeDelete || event.SID != fixture.sb.ID {
			t.Fatalf("terminal route event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("completed delete did not publish its terminal route event")
	}
	if kinds := recorder.sandboxKinds(); len(kinds) != 1 || kinds[0] != "delete" {
		t.Fatalf("terminal observer events = %v", kinds)
	}
}

func TestSandboxDeleteFinalizerRejectsNonCanonicalOwnershipPaths(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-noncanonical-path")
	nodeFile := filepath.Join(fixture.o.cfg.Paths.RunRoot, "node.sock")
	if err := os.WriteFile(nodeFile, []byte("node-level"), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.sb.RunDir = fixture.o.cfg.Paths.RunRoot
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	beginDeletingForTest(t, fixture)
	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); err == nil {
		t.Fatal("non-canonical cleanup path was accepted")
	}
	assertDeletingOwnership(t, fixture)
	if fixture.lc.stops != 0 || fixture.vs.calls != 0 {
		t.Fatalf("invalid path cleanup touched unit/network: stops=%d detaches=%d", fixture.lc.stops, fixture.vs.calls)
	}
	if _, err := os.Stat(nodeFile); err != nil {
		t.Fatalf("invalid cleanup path removed node-level file: %v", err)
	}
}

func TestReconcileResumesDeletingFinalizerAfterRestart(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-restart")
	beginDeletingForTest(t, fixture)
	baseErr := errors.New("injected pre-restart BaseDir failure")
	fixture.o.removeSandboxBaseDir = func(string) error { return baseErr }
	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); !errors.Is(err, baseErr) {
		t.Fatalf("pre-restart finalizer = %v", err)
	}
	assertDeletingOwnership(t, fixture)
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-restart RunDir cleanup = %v", err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
		t.Fatalf("pre-restart BaseDir ownership lost: %v", err)
	}
	runNodeFile := filepath.Join(fixture.o.cfg.Paths.RunRoot, "node.sock")
	baseNodeFile := filepath.Join(fixture.o.cfg.Paths.BaseRoot, "node.db")
	for _, path := range []string{runNodeFile, baseNodeFile} {
		if err := os.WriteFile(path, []byte("node-level"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	lc := &sandboxFinalizerLauncher{unit: fixture.lc.unit, state: "inactive"}
	vs := &sandboxFinalizerVS{detached: true}
	restarted := New(fixture.o.cfg, fixture.o.st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID); err != nil || got != nil {
		t.Fatalf("restarted finalizer row = %+v, %v", got, err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restarted finalizer retained BaseDir: %v", err)
	}
	for _, path := range []string{runNodeFile, baseNodeFile} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("sandbox finalizer removed node-level file %s: %v", path, err)
		}
	}
}

func TestSandboxDeleteWorkerConsumesSameIDSuccessorBeforeRetiring(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-worker-successor")
	beginDeletingForTest(t, fixture)
	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); err != nil {
		t.Fatal(err)
	}

	successor := cloneSandbox(fixture.sb)
	successor.State = types.StateRunning
	successor.CreatedUnix++
	successor.RunID = "sr-00000000-0000-7000-8000-000000000288"
	successor.VswitchPort = "port-288"
	successor.FloatingIP = "192.0.2.88"
	fixture.lc.mu.Lock()
	fixture.lc.unit = fixture.o.runnerUnit(successor.RunID)
	fixture.lc.state = "active"
	fixture.lc.mu.Unlock()
	fixture.vs.mu.Lock()
	fixture.vs.detached = false
	fixture.vs.mu.Unlock()
	for _, dir := range []string{successor.RunDir, successor.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.o.st.InsertSandbox(context.Background(), successor); err != nil {
		t.Fatal(err)
	}
	if changed, err := fixture.o.st.BeginSandboxDelete(context.Background(), successor); err != nil || !changed {
		t.Fatalf("begin successor delete = %t, %v", changed, err)
	}

	fixture.o.deleteMu.Lock()
	fixture.o.deleteActive[successor.ID] = struct{}{}
	fixture.o.deleteMu.Unlock()
	if retired, err := fixture.o.retireSandboxDeleteWorker(successor.ID); err != nil || retired {
		t.Fatalf("retire with deleting successor = %t, %v", retired, err)
	}
	fixture.o.deleteMu.Lock()
	_, active := fixture.o.deleteActive[successor.ID]
	fixture.o.deleteMu.Unlock()
	if !active {
		t.Fatal("worker retired while same-ID successor still needed finalization")
	}

	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), successor.ID); err != nil {
		t.Fatal(err)
	}
	if retired, err := fixture.o.retireSandboxDeleteWorker(successor.ID); err != nil || !retired {
		t.Fatalf("retire after successor finalization = %t, %v", retired, err)
	}
	fixture.o.deleteMu.Lock()
	_, active = fixture.o.deleteActive[successor.ID]
	fixture.o.deleteMu.Unlock()
	if active {
		t.Fatal("completed successor retained a finalizer worker entry")
	}
}

func TestPausedCleanupFailureBlocksResumeUntilRunDirIsRemoved(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "paused-run-dir-gate")
	fixture.sb.State = types.StatePaused
	fixture.sb.RunID, fixture.sb.VswitchPort, fixture.sb.FloatingIP = "", "", ""
	fixture.sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	runErr := errors.New("injected paused RunDir failure")
	fixture.o.removeSandboxRunDir = func(string) error { return runErr }
	if _, err := fixture.o.Connect(context.Background(), fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{}); !errors.Is(err, runErr) {
		t.Fatalf("Resume cleanup gate = %v", err)
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" || stored.VswitchPort != "" {
		t.Fatalf("paused row after RunDir failure = %+v, %v", stored, err)
	}
	if _, found := fixture.o.launches.Lookup(fixture.sb.ID); found {
		t.Fatal("RunDir cleanup failure admitted a new launch owner")
	}
	if _, err := os.Stat(fixture.sb.RunDir); err != nil {
		t.Fatalf("failed paused cleanup removed RunDir: %v", err)
	}
	fixture.o.removeSandboxRunDir = os.RemoveAll
	if err := fixture.o.cleanupPausedOwnership(context.Background(), stored); err != nil {
		t.Fatalf("paused RunDir retry: %v", err)
	}
	stored, err = fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.RunDir != "" || stored.EnvdUDS != "" || stored.CiUDS != "" {
		t.Fatalf("successful paused RunDir cleanup retained ownership: %+v, %v", stored, err)
	}
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paused RunDir retry retained directory: %v", err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
		t.Fatalf("paused cleanup removed BaseDir/checkpoint owner: %v", err)
	}
}

func TestPausedNetworkStoreFailureRetriesWithoutAnotherAdmission(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "paused-network-store-retry")
	fixture.sb.State = types.StatePaused
	fixture.sb.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSnapshot,
		Ref:  "manifest://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	fixture.o.cache(fixture.sb)
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(lifecycleCtx)
	t.Cleanup(func() {
		cancelLifecycle()
		drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = fixture.o.DrainPauses(drainCtx)
	})

	installStoreTrigger(t, fixture.dbPath, `CREATE TRIGGER fail_paused_network_clear BEFORE UPDATE OF vswitch_port ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced paused network clear failure'); END`)
	if _, err := fixture.o.Connect(context.Background(), fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{}); err == nil ||
		!strings.Contains(err.Error(), "forced paused network clear failure") {
		t.Fatalf("Resume cleanup failure = %v", err)
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" ||
		stored.VswitchPort != fixture.sb.VswitchPort || stored.RunDir != fixture.sb.RunDir {
		t.Fatalf("partial paused cleanup owner = %+v, %v", stored, err)
	}
	fixture.o.networkAllocationMu.Lock()
	_, fenced := fixture.o.detachedPortsPending[fixture.sb.VswitchPort]
	fixture.o.networkAllocationMu.Unlock()
	if !fenced {
		t.Fatal("detached paused port was not fenced while its durable clear failed")
	}
	fixture.o.pausedCleanupMu.Lock()
	_, retrying := fixture.o.pausedCleanupActive[fixture.sb.ID]
	fixture.o.pausedCleanupMu.Unlock()
	if !retrying {
		t.Fatal("failed paused cleanup did not start a live retry worker")
	}

	installStoreTrigger(t, fixture.dbPath, `DROP TRIGGER fail_paused_network_clear`)
	cleaned := waitForSandbox(t, fixture.o, context.Background(), fixture.sb.ID, func(sb *types.Sandbox) bool {
		return sb.State == types.StatePaused && sb.RunID == "" && sb.VswitchPort == "" && sb.RunDir == ""
	}, "fully cleaned paused ownership without another admission")
	if cleaned.BaseDir != fixture.sb.BaseDir || cleaned.ResumeSource != fixture.sb.ResumeSource {
		t.Fatalf("paused retry changed portable owner = %+v", cleaned)
	}
	deadline := time.Now().Add(time.Second)
	for {
		fixture.o.pausedCleanupMu.Lock()
		_, retrying = fixture.o.pausedCleanupActive[fixture.sb.ID]
		fixture.o.pausedCleanupMu.Unlock()
		if !retrying {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("successful paused cleanup retained retry worker")
		}
		time.Sleep(time.Millisecond)
	}
	fixture.o.networkAllocationMu.Lock()
	_, fenced = fixture.o.detachedPortsPending[fixture.sb.VswitchPort]
	fixture.o.networkAllocationMu.Unlock()
	if fenced {
		t.Fatal("successful paused cleanup retained detached port fence")
	}
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paused cleanup retry retained RunDir: %v", err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
		t.Fatalf("paused cleanup retry removed BaseDir: %v", err)
	}

	cancelLifecycle()
	if err := fixture.o.DrainPauses(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPauseStartsLiveRetryForRetainedRunDir(t *testing.T) {
	cfg := checkpointOrchestratorConfig(t, config.CheckpointLocal)
	o, sb, apiKey, _, _, _ := newCheckpointPauseFixture(t, cfg, "")
	for _, dir := range []string{sb.RunDir, sb.BaseDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(sb.BaseDir, "checkpoint-owner"), []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	o.SetLifecycleContext(lifecycleCtx)
	t.Cleanup(func() {
		cancelLifecycle()
		drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = o.DrainPauses(drainCtx)
	})

	var failRunDir atomic.Bool
	failRunDir.Store(true)
	var removeCalls atomic.Int32
	o.removeSandboxRunDir = func(path string) error {
		removeCalls.Add(1)
		if failRunDir.Load() {
			return errors.New("forced paused RunDir cleanup failure")
		}
		return os.RemoveAll(path)
	}
	if err := o.Pause(context.Background(), sb.ID, apiKey, orchSnapshotCapture(sandboxcfg.SnapshotPolicy{})); err != nil {
		t.Fatal(err)
	}
	paused, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || paused == nil || paused.State != types.StatePaused || paused.RunID != "" ||
		paused.VswitchPort != "" || paused.RunDir != sb.RunDir {
		t.Fatalf("paused row did not retain failed RunDir owner = %+v, %v", paused, err)
	}
	o.pausedCleanupMu.Lock()
	_, retrying := o.pausedCleanupActive[sb.ID]
	o.pausedCleanupMu.Unlock()
	if !retrying {
		t.Fatal("successful Pause did not retain a live cleanup retry owner")
	}

	failRunDir.Store(false)
	cleaned := waitForSandbox(t, o, context.Background(), sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StatePaused && current.RunDir == ""
	}, "paused RunDir cleanup without Resume")
	if cleaned.BaseDir != sb.BaseDir || !cleaned.ResumeSource.Valid() {
		t.Fatalf("Pause retry changed checkpoint owner = %+v", cleaned)
	}
	if removeCalls.Load() < 2 {
		t.Fatalf("RunDir removal calls = %d, want failed Pause cleanup plus live retry", removeCalls.Load())
	}
	if _, err := os.Stat(sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Pause retry retained RunDir: %v", err)
	}
	if _, err := os.Stat(sb.BaseDir); err != nil {
		t.Fatalf("Pause retry removed BaseDir: %v", err)
	}

	cancelLifecycle()
	if err := o.DrainPauses(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestKillFinalizesFullyCleanedPausedSandbox(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-clean-paused")
	runDir := fixture.sb.RunDir
	fixture.sb.State = types.StatePaused
	fixture.sb.RunID = ""
	fixture.sb.VswitchPort, fixture.sb.FloatingIP, fixture.sb.InnerIP, fixture.sb.PortMAC = "", "", "", ""
	fixture.sb.RunDir, fixture.sb.EnvdUDS, fixture.sb.CiUDS = "", "", ""
	fixture.sb.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSnapshot,
		Ref:  "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
	}
	fixture.lc.state = "inactive"
	if err := os.RemoveAll(runDir); err != nil {
		t.Fatal(err)
	}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}

	if killed, err := fixture.o.Kill(context.Background(), fixture.sb.ID, fixture.apiKey); err != nil || !killed {
		t.Fatalf("Kill clean paused sandbox = %t, %v", killed, err)
	}
	waitForSandboxAbsent(t, fixture.o, context.Background(), fixture.sb.ID, "clean paused finalizer")
	if err := fixture.o.DrainSandboxDeletes(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("clean paused finalizer retained BaseDir: %v", err)
	}
}

func TestRejectedResumePublishesCompletedPausedCleanup(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "paused-cleanup-hook-reject")
	fixture.sb.State = types.StatePaused
	fixture.sb.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSnapshot,
		Ref:  "manifest://ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	fixture.o.cache(fixture.sb)
	fixture.o.SetExtensionHooks(sandboxHookFunc(func(context.Context, *conductorextension.SandboxOperation) error {
		return fmtRejected("reject resume after cleanup")
	}), nil)

	if _, err := fixture.o.Connect(context.Background(), fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{}); !errors.Is(err, conductorextension.ErrRejected) {
		t.Fatalf("rejected Resume = %v", err)
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" ||
		stored.VswitchPort != "" || stored.RunDir != "" {
		t.Fatalf("durable paused cleanup after rejected Resume = %+v, %v", stored, err)
	}
	cached := fixture.o.lookup(fixture.sb.ID)
	if cached == nil || cached.State != types.StatePaused || cached.RunID != "" ||
		cached.VswitchPort != "" || cached.RunDir != "" {
		t.Fatalf("cached paused cleanup after rejected Resume = %+v", cached)
	}
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected Resume retained cleaned RunDir: %v", err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
		t.Fatalf("rejected Resume removed paused BaseDir: %v", err)
	}
}

func TestPausedCleanupSerializesConcurrentResumeAdmission(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "paused-cleanup-resume-race")
	fixture.sb.State = types.StatePaused
	fixture.sb.ResumeSource = types.ResumeSource{
		Kind: types.ResumeSourceSnapshot,
		Ref:  "manifest://dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}

	removeEntered := make(chan struct{})
	releaseRemove := make(chan struct{})
	var removeOnce sync.Once
	fixture.o.removeSandboxRunDir = func(path string) error {
		removeOnce.Do(func() {
			close(removeEntered)
			<-releaseRemove
		})
		return os.RemoveAll(path)
	}
	cleanupDone := make(chan error, 1)
	go func() {
		unlock := fixture.o.lifecycle.Lock(fixture.sb.ID)
		err := fixture.o.cleanupPausedOwnership(context.Background(), fixture.sb)
		unlock()
		cleanupDone <- err
	}()
	select {
	case <-removeEntered:
	case <-time.After(time.Second):
		t.Fatal("paused cleanup did not reach RunDir removal")
	}

	resumeCtx, cancelResume := context.WithCancel(context.Background())
	resumeDone := make(chan error, 1)
	go func() {
		_, err := fixture.o.Connect(resumeCtx, fixture.sb.ID, fixture.apiKey, "", api.ConnectOptions{})
		resumeDone <- err
	}()
	select {
	case err := <-resumeDone:
		t.Fatalf("Resume admission escaped paused cleanup fence: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if _, found := fixture.o.launches.Lookup(fixture.sb.ID); found {
		t.Fatal("concurrent Resume claimed a runner before paused cleanup completed")
	}

	cancelResume()
	close(releaseRemove)
	if err := <-cleanupDone; err != nil {
		t.Fatalf("paused cleanup: %v", err)
	}
	if err := <-resumeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Resume after cleanup fence = %v", err)
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" || stored.VswitchPort != "" || stored.RunDir != "" {
		t.Fatalf("paused row after serialized cleanup = %+v, %v", stored, err)
	}
	if _, found := fixture.o.launches.Lookup(fixture.sb.ID); found {
		t.Fatal("canceled concurrent Resume acquired new runtime ownership")
	}
	if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("paused cleanup retained RunDir: %v", err)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
		t.Fatalf("paused cleanup removed BaseDir: %v", err)
	}
}
