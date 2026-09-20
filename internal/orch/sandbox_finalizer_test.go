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
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyshm"
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
func (*sandboxFinalizerLauncher) Close() error                 { return nil }

func (l *sandboxFinalizerLauncher) clearFaults() {
	l.mu.Lock()
	l.stopErr, l.resetErr, l.listErr = nil, nil, nil
	l.mu.Unlock()
}

func (l *sandboxFinalizerLauncher) stopSnapshot() (string, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.state, l.stops
}

type sandboxFinalizerVS struct {
	mu         sync.Mutex
	detachErr  error
	detachHook func(string)
	detached   bool
	calls      int
}

func (*sandboxFinalizerVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return nil, nil
}
func (v *sandboxFinalizerVS) Detach(_ context.Context, port string) error {
	v.mu.Lock()
	v.calls++
	err := v.detachErr
	if err == nil {
		if v.detached {
			err = vswitch.ErrPortNotAttached
		} else {
			v.detached = true
		}
	}
	hook := v.detachHook
	v.mu.Unlock()
	if hook != nil {
		hook(port)
	}
	return err
}
func (*sandboxFinalizerVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func (v *sandboxFinalizerVS) clearFault() {
	v.mu.Lock()
	v.detachErr = nil
	v.mu.Unlock()
}

type serializedSandboxDeleteVS struct {
	detachStarted chan struct{}
	allowDetach   chan struct{}
	attachStarted chan struct{}
}

func (v *serializedSandboxDeleteVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	close(v.attachStarted)
	return &vswitch.Port{Port: "port-new"}, nil
}

func (v *serializedSandboxDeleteVS) Detach(context.Context, string) error {
	close(v.detachStarted)
	<-v.allowDetach
	return nil
}

func (*serializedSandboxDeleteVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

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
	return assertDeletingOwnershipState(t, fixture, true)
}

func assertDeletingOwnershipState(t *testing.T, fixture sandboxFinalizerFixture, wantNetwork bool) *types.Sandbox {
	t.Helper()
	got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || got == nil {
		t.Fatalf("deleting owner = %+v, %v", got, err)
	}
	wantPort, wantFloatingIP, wantInnerIP, wantPortMAC := "", "", "", ""
	if wantNetwork {
		wantPort, wantFloatingIP, wantInnerIP, wantPortMAC = fixture.sb.VswitchPort, fixture.sb.FloatingIP, fixture.sb.InnerIP, fixture.sb.PortMAC
	}
	if got.State != types.StateDeleting || got.RunID != fixture.sb.RunID || got.VswitchPort != wantPort ||
		got.FloatingIP != wantFloatingIP || got.InnerIP != wantInnerIP || got.PortMAC != wantPortMAC ||
		got.RunDir != fixture.sb.RunDir || got.BaseDir != fixture.sb.BaseDir {
		t.Fatalf("cleanup failure lost deleting ownership: %+v", got)
	}
	return got
}

func TestSandboxDeleteFinalizerRetainsOnlyPendingOwnershipAtEveryFailure(t *testing.T) {
	fault := errors.New("injected finalizer failure")
	for _, test := range []struct {
		name          string
		inject        func(*sandboxFinalizerFixture)
		clear         func(*sandboxFinalizerFixture)
		wantNetwork   bool
		wantRunExists bool
		wantBaseExist bool
		wantDetaches  int
	}{
		{
			name:          "stop",
			inject:        func(f *sandboxFinalizerFixture) { f.lc.stopErr = fault },
			clear:         func(f *sandboxFinalizerFixture) { f.lc.clearFaults() },
			wantNetwork:   true,
			wantRunExists: true, wantBaseExist: true,
			wantDetaches: 1,
		},
		{
			name:          "reset",
			inject:        func(f *sandboxFinalizerFixture) { f.lc.resetErr = fault },
			clear:         func(f *sandboxFinalizerFixture) { f.lc.clearFaults() },
			wantNetwork:   true,
			wantRunExists: true, wantBaseExist: true,
			wantDetaches: 1,
		},
		{
			name:          "detach",
			inject:        func(f *sandboxFinalizerFixture) { f.vs.detachErr = fault },
			clear:         func(f *sandboxFinalizerFixture) { f.vs.clearFault() },
			wantNetwork:   true,
			wantRunExists: true, wantBaseExist: true,
			wantDetaches: 2,
		},
		{
			name: "run-dir",
			inject: func(f *sandboxFinalizerFixture) {
				f.o.removeSandboxRunDir = func(string) error { return fault }
			},
			clear:         func(f *sandboxFinalizerFixture) { f.o.removeSandboxRunDir = os.RemoveAll },
			wantRunExists: true, wantBaseExist: true,
			wantDetaches: 1,
		},
		{
			name: "base-dir",
			inject: func(f *sandboxFinalizerFixture) {
				f.o.removeSandboxBaseDir = func(string) error { return fault }
			},
			clear:         func(f *sandboxFinalizerFixture) { f.o.removeSandboxBaseDir = os.RemoveAll },
			wantRunExists: false, wantBaseExist: true,
			wantDetaches: 1,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSandboxFinalizerFixture(t, "delete-fault-"+test.name)
			test.inject(&fixture)
			beginDeletingForTest(t, fixture)
			if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); !errors.Is(err, fault) {
				t.Fatalf("first finalizer error = %v", err)
			}
			assertDeletingOwnershipState(t, fixture, test.wantNetwork)
			if !test.wantNetwork {
				fixture.o.networkAllocationMu.Lock()
				_, fenced := fixture.o.detachedPortsPending[fixture.sb.VswitchPort]
				fixture.o.networkAllocationMu.Unlock()
				if fenced {
					t.Fatal("durably cleared deleting port remained allocation-fenced")
				}
				if _, err := fixture.o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"}); err != nil {
					t.Fatalf("new allocation after durable network clear: %v", err)
				}
			}
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
			if fixture.vs.calls != test.wantDetaches {
				t.Fatalf("Detach calls = %d, want %d", fixture.vs.calls, test.wantDetaches)
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

func TestSandboxDeleteDetachAndDurableClearFenceNewAllocation(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-allocation-fence")
	vs := &serializedSandboxDeleteVS{
		detachStarted: make(chan struct{}),
		allowDetach:   make(chan struct{}),
		attachStarted: make(chan struct{}),
	}
	fixture.o.vs = vs
	beginDeletingForTest(t, fixture)
	runErr := errors.New("injected RunDir failure after network clear")
	fixture.o.removeSandboxRunDir = func(string) error { return runErr }

	finalizeDone := make(chan error, 1)
	go func() {
		finalizeDone <- fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID)
	}()
	select {
	case <-vs.detachStarted:
	case <-time.After(time.Second):
		t.Fatal("delete finalizer did not start Detach")
	}
	attachDone := make(chan error, 1)
	go func() {
		_, err := fixture.o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"})
		attachDone <- err
	}()
	select {
	case <-vs.attachStarted:
		t.Fatal("new allocation crossed the Detach-to-durable-clear fence")
	case <-time.After(50 * time.Millisecond):
	}

	close(vs.allowDetach)
	select {
	case err := <-finalizeDone:
		if !errors.Is(err, runErr) {
			t.Fatalf("finalizer error = %v, want RunDir failure", err)
		}
	case <-time.After(time.Second):
		t.Fatal("delete finalizer did not finish network clear")
	}
	select {
	case <-vs.attachStarted:
	case <-time.After(time.Second):
		t.Fatal("new allocation did not resume after durable network clear")
	}
	if err := <-attachDone; err != nil {
		t.Fatalf("attachNetwork after durable clear: %v", err)
	}
	assertDeletingOwnershipState(t, fixture, false)
}

func TestSandboxDeleteNetworkClearFailureKeepsFenceUntilLiveRetry(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-network-clear-retry")
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(lifecycleCtx)
	t.Cleanup(func() {
		cancelLifecycle()
		drainCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = fixture.o.DrainSandboxDeletes(drainCtx)
	})
	installStoreTrigger(t, fixture.dbPath, `CREATE TRIGGER fail_deleting_network_clear BEFORE UPDATE OF vswitch_port ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced deleting network clear failure'); END`)

	if killed, err := fixture.o.Kill(context.Background(), fixture.sb.ID, fixture.apiKey); err != nil || !killed {
		t.Fatalf("Kill = %t, %v", killed, err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		fixture.o.networkAllocationMu.Lock()
		_, fenced := fixture.o.detachedPortsPending[fixture.sb.VswitchPort]
		fixture.o.networkAllocationMu.Unlock()
		if fenced {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("failed durable network clear did not retain the port fence")
		}
		time.Sleep(time.Millisecond)
	}
	assertDeletingOwnership(t, fixture)
	if _, err := fixture.o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"}); err == nil ||
		!strings.Contains(err.Error(), "network allocation blocked") {
		t.Fatalf("allocation during failed durable clear = %v", err)
	}

	installStoreTrigger(t, fixture.dbPath, `DROP TRIGGER fail_deleting_network_clear`)
	waitForSandboxAbsent(t, fixture.o, context.Background(), fixture.sb.ID, "delete retry after durable network clear")
	if err := fixture.o.DrainSandboxDeletes(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixture.o.networkAllocationMu.Lock()
	_, fenced := fixture.o.detachedPortsPending[fixture.sb.VswitchPort]
	fixture.o.networkAllocationMu.Unlock()
	if fenced {
		t.Fatal("successful live retry retained the detached port fence")
	}
	if _, err := fixture.o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"}); err != nil {
		t.Fatalf("allocation after live retry: %v", err)
	}
}

func TestSandboxDeleteNetworkCASMissKeepsOldFenceAndSuccessorPort(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-network-cas-miss")
	beginDeletingForTest(t, fixture)
	oldPort := fixture.sb.VswitchPort
	fixture.vs.detachHook = func(detached string) {
		if detached != oldPort {
			t.Fatalf("detached port = %q, want %q", detached, oldPort)
		}
		installStoreTrigger(t, fixture.dbPath, `UPDATE sandboxes
			SET created_unix=288,vswitch_port='port-successor',floatingip='192.0.2.88'
			WHERE id='delete-network-cas-miss'`)
	}

	err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID)
	if err == nil || !strings.Contains(err.Error(), "ownership changed before durable clear") {
		t.Fatalf("network CAS miss = %v", err)
	}
	stored, getErr := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if getErr != nil || stored == nil || stored.CreatedUnix != 288 || stored.VswitchPort != "port-successor" {
		t.Fatalf("CAS miss changed successor owner = %+v, %v", stored, getErr)
	}
	fixture.o.networkAllocationMu.Lock()
	_, oldFenced := fixture.o.detachedPortsPending[oldPort]
	_, successorFenced := fixture.o.detachedPortsPending[stored.VswitchPort]
	fixture.o.networkAllocationMu.Unlock()
	if !oldFenced || successorFenced {
		t.Fatalf("CAS miss fences: old=%t successor=%t", oldFenced, successorFenced)
	}
	if fixture.vs.calls != 1 {
		t.Fatalf("CAS miss detached %d ports, want only the old owner", fixture.vs.calls)
	}
	for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("CAS miss removed successor-owned path %s: %v", path, err)
		}
	}
}

func TestSandboxDeleteHardDeleteFailureLeavesNetworkReleased(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-hard-delete-failure")
	beginDeletingForTest(t, fixture)
	installStoreTrigger(t, fixture.dbPath, `CREATE TRIGGER fail_final_sandbox_delete BEFORE DELETE ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced final delete failure'); END`)

	err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID)
	if err == nil || !strings.Contains(err.Error(), "forced final delete failure") {
		t.Fatalf("hard-delete failure = %v", err)
	}
	assertDeletingOwnershipState(t, fixture, false)
	fixture.o.networkAllocationMu.Lock()
	_, fenced := fixture.o.detachedPortsPending[fixture.sb.VswitchPort]
	fixture.o.networkAllocationMu.Unlock()
	if fenced {
		t.Fatal("hard-delete failure reacquired the cleared network owner")
	}
	if _, err := fixture.o.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"}); err != nil {
		t.Fatalf("allocation during hard-delete failure: %v", err)
	}
	for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("hard-delete failure retained physical path %s: %v", path, err)
		}
	}

	installStoreTrigger(t, fixture.dbPath, `DROP TRIGGER fail_final_sandbox_delete`)
	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); err != nil {
		t.Fatalf("hard-delete retry: %v", err)
	}
	if fixture.vs.calls != 1 {
		t.Fatalf("hard-delete retry redetached released port: calls=%d", fixture.vs.calls)
	}
}

func TestKillWithdrawsProxyRouteBeforeFinalizerAndObservesAfterHardDelete(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-live-retry")
	recorder := &objectObserverRecorder{}
	fixture.o.SetExtensionObserver(recorder)
	events, stopEvents := fixture.o.Subscribe()
	defer stopEvents()
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(lifecycleCtx)
	t.Cleanup(func() {
		cancelLifecycle()
		drainCtx, cancelDrain := context.WithTimeout(context.Background(), time.Second)
		defer cancelDrain()
		_ = fixture.o.DrainSandboxDeletes(drainCtx)
	})

	routePath := filepath.Join(t.TempDir(), "routes.shm")
	routeTable, err := proxyshm.Create(routePath, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer routeTable.Close()
	proxyWorkerTable, err := proxyshm.Open(routePath)
	if err != nil {
		t.Fatal(err)
	}
	defer proxyWorkerTable.Close()
	proxyMaster := proxyshm.NewMasterView(routeTable, time.Second, nil)
	proxyWorker := proxyshm.NewWorkerView(proxyWorkerTable, nil, nil, time.Second)
	proxyMaster.BeginSync()
	if err := proxyMaster.ApplyUpsert(fixture.o.routeEntry(fixture.sb)); err != nil {
		t.Fatal(err)
	}
	proxyMaster.Bookmark()
	target := proxy.LegacyTarget(8080)
	binding, found, err := proxyWorker.LookupRoute(context.Background(), fixture.sb.ID, target)
	if err != nil || !found {
		t.Fatalf("initial Proxy LookupRoute = %+v, %t, %v", binding, found, err)
	}
	if route, active, err := proxyWorker.ActivateRoute(context.Background(), binding); err != nil || !active || route.Kind != proxy.KindTCP {
		t.Fatalf("initial Proxy ActivateRoute = %+v, %t, %v", route, active, err)
	}

	stopErr := errors.New("transient Stop failure")
	fixture.lc.mu.Lock()
	fixture.lc.stopErr = stopErr
	fixture.lc.mu.Unlock()
	if killed, err := fixture.o.Kill(context.Background(), fixture.sb.ID, fixture.apiKey); err != nil || !killed {
		t.Fatalf("Kill = %t, %v", killed, err)
	}
	select {
	case event := <-events:
		if event.Kind != routesync.TypeDelete || event.SID != fixture.sb.ID {
			t.Fatalf("delete acceptance route withdrawal = %+v", event)
		}
		proxyMaster.ApplyDelete(event.SID)
	case <-time.After(time.Second):
		t.Fatal("delete acceptance did not withdraw the live route")
	}

	deadline := time.Now().Add(time.Second)
	state, stopCalls := fixture.lc.stopSnapshot()
	for stopCalls == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
		state, stopCalls = fixture.lc.stopSnapshot()
	}
	if stopCalls == 0 {
		t.Fatal("delete finalizer did not reach injected Stop failure")
	}
	if state != "active" {
		t.Fatalf("failed Stop changed runner state to %q", state)
	}
	deleting := assertDeletingOwnership(t, fixture)
	if kinds := recorder.sandboxKinds(); len(kinds) != 0 {
		t.Fatalf("terminal observer published before cleanup: %v", kinds)
	}
	if _, found := proxyWorkerTable.Lookup(deleting.ID); found {
		t.Fatal("Proxy SHM retained deleting sandbox route")
	}
	if got, found, err := proxyWorker.LookupRoute(context.Background(), deleting.ID, target); err != nil || found {
		t.Fatalf("Proxy LookupRoute after withdrawal = %+v, %t, %v", got, found, err)
	}
	if route, active, err := proxyWorker.ActivateRoute(context.Background(), binding); err != nil || active {
		t.Fatalf("Proxy ActivateRoute after withdrawal = %+v, %t, %v", route, active, err)
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
		t.Fatalf("unexpected extra route event before repeated acceptance: %+v", event)
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
	select {
	case event := <-events:
		if event.Kind != routesync.TypeDelete || event.SID != fixture.sb.ID {
			t.Fatalf("repeated delete acceptance route withdrawal = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("repeated delete acceptance did not reassert route withdrawal")
	}

	fixture.lc.clearFaults()
	waitForSandboxAbsent(t, fixture.o, context.Background(), fixture.sb.ID, "live finalizer retry")
	if err := fixture.o.DrainSandboxDeletes(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("delete finalizer published a second route withdrawal: %+v", event)
	default:
	}
	if kinds := recorder.sandboxKinds(); len(kinds) != 1 || kinds[0] != "delete" {
		t.Fatalf("terminal observer events = %v", kinds)
	}
	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); err != nil {
		t.Fatal(err)
	}
	if kinds := recorder.sandboxKinds(); len(kinds) != 1 || kinds[0] != "delete" {
		t.Fatalf("repeated finalizer terminal observer events = %v", kinds)
	}
}

func TestKillDoesNotWithdrawRouteWhenDeleteAcceptanceFails(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-acceptance-failure")
	events, cancel := fixture.o.Subscribe()
	defer cancel()
	installStoreTrigger(t, fixture.dbPath, `
		CREATE TRIGGER fail_begin_sandbox_delete
		BEFORE UPDATE OF state ON sandboxes
		WHEN OLD.id='delete-acceptance-failure'
		BEGIN SELECT RAISE(ABORT, 'forced delete acceptance failure'); END`)

	if killed, err := fixture.o.Kill(context.Background(), fixture.sb.ID, fixture.apiKey); err == nil || killed {
		t.Fatalf("Kill with failed durable acceptance = %t, %v", killed, err)
	}
	select {
	case event := <-events:
		t.Fatalf("failed durable acceptance published route event: %+v", event)
	default:
	}
	stored, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
	if err != nil || stored == nil || stored.State != types.StateRunning {
		t.Fatalf("failed durable acceptance row = %+v, %v", stored, err)
	}
	var routed bool
	if err := fixture.o.Range(context.Background(), func(entry routesync.RouteEntry) error {
		if entry.SandboxID == fixture.sb.ID {
			routed = true
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !routed {
		t.Fatal("failed durable acceptance withdrew full-snapshot route")
	}
	fixture.o.deleteMu.Lock()
	_, finalizing := fixture.o.deleteActive[fixture.sb.ID]
	fixture.o.deleteMu.Unlock()
	if finalizing {
		t.Fatal("failed durable acceptance started a delete finalizer")
	}
}

func TestSandboxDeleteRetryQuiescesOnShutdownAndRestartReconciles(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-shutdown-restart")
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(lifecycleCtx)
	fault := errors.New("injected persistent BaseDir cleanup failure")
	attempted := make(chan struct{}, 1)
	fixture.o.removeSandboxBaseDir = func(string) error {
		select {
		case attempted <- struct{}{}:
		default:
		}
		return fault
	}

	if killed, err := fixture.o.Kill(context.Background(), fixture.sb.ID, fixture.apiKey); err != nil || !killed {
		t.Fatalf("Kill = %t, %v", killed, err)
	}
	select {
	case <-attempted:
	case <-time.After(time.Second):
		t.Fatal("delete worker did not reach the persistent cleanup failure")
	}
	assertDeletingOwnershipState(t, fixture, false)

	// Orderly shutdown must not wait forever for a resource that this process
	// cannot release. The deleting row is the durable handoff to startup.
	cancelLifecycle()
	drainCtx, cancelDrain := context.WithTimeout(context.Background(), time.Second)
	defer cancelDrain()
	if err := fixture.o.DrainSandboxDeletes(drainCtx); err != nil {
		t.Fatalf("DrainSandboxDeletes after lifecycle cancellation: %v", err)
	}
	fixture.o.deleteMu.Lock()
	_, active := fixture.o.deleteActive[fixture.sb.ID]
	fixture.o.deleteMu.Unlock()
	if active {
		t.Fatal("shutdown retained a process-local delete retry worker")
	}
	assertDeletingOwnershipState(t, fixture, false)

	lc := &sandboxFinalizerLauncher{unit: fixture.lc.unit, state: "inactive"}
	vs := &sandboxFinalizerVS{detached: true}
	restarted := New(fixture.o.cfg, fixture.o.st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatalf("restart reconcile: %v", err)
	}
	if got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID); err != nil || got != nil {
		t.Fatalf("restart finalizer row = %+v, %v", got, err)
	}
	if vs.calls != 0 {
		t.Fatalf("restart redetached durably cleared port: calls=%d", vs.calls)
	}
	if _, err := os.Stat(fixture.sb.BaseDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart retained BaseDir: %v", err)
	}
}

func TestRestartKeepsAllocationFencedUntilDeletingNetworkClearSucceeds(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-restart-network-clear")
	beginDeletingForTest(t, fixture)
	installStoreTrigger(t, fixture.dbPath, `CREATE TRIGGER fail_restart_network_clear BEFORE UPDATE OF vswitch_port ON sandboxes BEGIN SELECT RAISE(ABORT, 'forced restart network clear failure'); END`)
	if err := fixture.o.finalizeSandboxDeleteOnce(context.Background(), fixture.sb.ID); err == nil ||
		!strings.Contains(err.Error(), "forced restart network clear failure") {
		t.Fatalf("pre-restart network clear = %v", err)
	}

	lc := &sandboxFinalizerLauncher{unit: fixture.lc.unit, state: "inactive"}
	vs := &sandboxFinalizerVS{detached: true}
	restarted := New(fixture.o.cfg, fixture.o.st, lc, vs, slog.New(slog.NewTextHandler(io.Discard, nil)))
	err := restarted.ReconcileSandboxes(context.Background())
	if err == nil || !strings.Contains(err.Error(), "forced restart network clear failure") {
		t.Fatalf("restart reconcile with failed durable clear = %v", err)
	}
	assertDeletingOwnership(t, fixture)
	restarted.networkAllocationMu.Lock()
	_, fenced := restarted.detachedPortsPending[fixture.sb.VswitchPort]
	restarted.networkAllocationMu.Unlock()
	if !fenced {
		t.Fatal("restart durable-clear failure did not fence the detached port")
	}
	if _, err := restarted.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"}); err == nil ||
		!strings.Contains(err.Error(), "network allocation blocked") {
		t.Fatalf("restart allocation before durable clear = %v", err)
	}

	installStoreTrigger(t, fixture.dbPath, `DROP TRIGGER fail_restart_network_clear`)
	if err := restarted.ReconcileSandboxes(context.Background()); err != nil {
		t.Fatalf("restart reconcile after durable clear recovery: %v", err)
	}
	if got, err := fixture.o.st.Get(context.Background(), fixture.sb.ID); err != nil || got != nil {
		t.Fatalf("restart recovery row = %+v, %v", got, err)
	}
	restarted.networkAllocationMu.Lock()
	_, fenced = restarted.detachedPortsPending[fixture.sb.VswitchPort]
	restarted.networkAllocationMu.Unlock()
	if fenced {
		t.Fatal("restart recovery retained detached port fence")
	}
	if _, err := restarted.attachNetwork(context.Background(), sandboxcfg.NetworkSpec{InnerIP: "169.254.1.1/31"}); err != nil {
		t.Fatalf("restart allocation after durable clear: %v", err)
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
	assertDeletingOwnershipState(t, fixture, false)
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
	if vs.calls != 0 {
		t.Fatalf("restart redetached durably cleared port: calls=%d", vs.calls)
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
	fixture.lc.unit = instanceUnit(fixture.o.cfg.Units.RunnerPoolConfigs()[0].Unit, successor.RunID)
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
	lifecycleCtx, cancelLifecycle := context.WithCancel(context.Background())
	fixture.o.SetLifecycleContext(lifecycleCtx)
	t.Cleanup(func() {
		cancelLifecycle()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := fixture.o.DrainPauses(ctx); err != nil {
			t.Errorf("drain paused cleanup: %v", err)
		}
	})
	fixture.sb.State = types.StatePaused
	fixture.sb.RunID, fixture.sb.VswitchPort, fixture.sb.FloatingIP = "", "", ""
	fixture.sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: "manifest://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}
	if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
		t.Fatal(err)
	}
	runErr := errors.New("injected paused RunDir failure")
	// Install the callback before Connect can start a background retry. Only
	// the atomic fault state changes while that retry is running.
	var allowCleanup atomic.Bool
	fixture.o.removeSandboxRunDir = func(path string) error {
		if !allowCleanup.Load() {
			return runErr
		}
		return os.RemoveAll(path)
	}
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
	allowCleanup.Store(true)
	// The asynchronous retry may already have cleared the saved owner.
	// Use the same lock-and-reload entry point, not that earlier snapshot.
	if err := fixture.o.finalizePausedCleanupOnce(fixture.sb.ID); err != nil {
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
	fixture.sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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

func TestPausedCleanupRetainsOwnershipAtUnitAndNetworkFailures(t *testing.T) {
	fault := errors.New("injected paused ownership cleanup failure")
	for _, test := range []struct {
		name              string
		inject            func(*sandboxFinalizerFixture)
		clear             func(*sandboxFinalizerFixture)
		wantRunnerCleared bool
	}{
		{
			name:   "stop",
			inject: func(f *sandboxFinalizerFixture) { f.lc.stopErr = fault },
			clear:  func(f *sandboxFinalizerFixture) { f.lc.clearFaults() },
		},
		{
			name:   "reset",
			inject: func(f *sandboxFinalizerFixture) { f.lc.resetErr = fault },
			clear:  func(f *sandboxFinalizerFixture) { f.lc.clearFaults() },
		},
		{
			name:              "detach",
			inject:            func(f *sandboxFinalizerFixture) { f.vs.detachErr = fault },
			clear:             func(f *sandboxFinalizerFixture) { f.vs.clearFault() },
			wantRunnerCleared: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSandboxFinalizerFixture(t, "paused-cleanup-"+test.name)
			fixture.sb.State = types.StatePaused
			fixture.sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				Kind: types.ResumeSourceSnapshot,
				Ref:  "manifest://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			}
			if err := fixture.o.st.Put(context.Background(), fixture.sb); err != nil {
				t.Fatal(err)
			}
			originalRunID := fixture.sb.RunID
			originalPort := fixture.sb.VswitchPort
			test.inject(&fixture)

			if err := fixture.o.cleanupPausedOwnership(context.Background(), fixture.sb); !errors.Is(err, fault) {
				t.Fatalf("cleanup error = %v, want %v", err, fault)
			}
			retained, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
			if err != nil || retained == nil || retained.State != types.StatePaused ||
				retained.ResumeSource != fixture.sb.ResumeSource || retained.VswitchPort != originalPort ||
				retained.RunDir != fixture.sb.RunDir || retained.BaseDir != fixture.sb.BaseDir {
				t.Fatalf("retained paused ownership = %+v, %v", retained, err)
			}
			if test.wantRunnerCleared {
				if retained.RunID != "" {
					t.Fatalf("completed runner cleanup retained RunID %q", retained.RunID)
				}
			} else if retained.RunID != originalRunID {
				t.Fatalf("failed runner cleanup changed RunID to %q", retained.RunID)
			}
			for _, path := range []string{fixture.sb.RunDir, fixture.sb.BaseDir} {
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("failed cleanup removed owned path %s: %v", path, err)
				}
			}

			test.clear(&fixture)
			if err := fixture.o.cleanupPausedOwnership(context.Background(), retained); err != nil {
				t.Fatalf("retry cleanup: %v", err)
			}
			cleaned, err := fixture.o.st.Get(context.Background(), fixture.sb.ID)
			if err != nil || cleaned == nil || cleaned.State != types.StatePaused || cleaned.RunID != "" ||
				cleaned.VswitchPort != "" || cleaned.FloatingIP != "" || cleaned.InnerIP != "" ||
				cleaned.PortMAC != "" || cleaned.RunDir != "" || cleaned.EnvdUDS != "" || cleaned.CiUDS != "" {
				t.Fatalf("retried paused cleanup = %+v, %v", cleaned, err)
			}
			if cleaned.BaseDir != fixture.sb.BaseDir || cleaned.ResumeSource != fixture.sb.ResumeSource {
				t.Fatalf("retry changed paused artifact ownership = %+v", cleaned)
			}
			if _, err := os.Stat(fixture.sb.RunDir); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("retry retained RunDir: %v", err)
			}
			if _, err := os.Stat(fixture.sb.BaseDir); err != nil {
				t.Fatalf("retry removed BaseDir: %v", err)
			}
		})
	}
}

func TestKillFinalizesFullyCleanedPausedSandbox(t *testing.T) {
	fixture := newSandboxFinalizerFixture(t, "delete-clean-paused")
	runDir := fixture.sb.RunDir
	fixture.sb.State = types.StatePaused
	fixture.sb.RunID = ""
	fixture.sb.VswitchPort, fixture.sb.FloatingIP, fixture.sb.InnerIP, fixture.sb.PortMAC = "", "", "", ""
	fixture.sb.RunDir, fixture.sb.EnvdUDS, fixture.sb.CiUDS = "", "", ""
	fixture.sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
	fixture.sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
	fixture.sb.ResumeSource = types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
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
