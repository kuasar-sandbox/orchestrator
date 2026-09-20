package orch

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// countingLauncher records how many times a unit was started — the launch count.
type countingLauncher struct {
	starts atomic.Int64
	stops  atomic.Int64
	orch   *Orchestrator

	started             chan<- struct{}
	startGate           <-chan struct{}
	assigned            chan<- string
	connectGate         <-chan struct{}
	readyConnected      chan<- struct{}
	readinessWire       []byte // nil means the exact successful wire; empty means immediate EOF.
	readinessDelay      time.Duration
	readinessNoSend     bool
	readinessNoConnect  bool
	readinessErrors     chan<- error
	artifactSummary     *configsock.ArtifactPrepareSummary
	artifactLaunchModes chan<- string
	snapshotRoots       chan<- string
	artifactPrepareErr  error
	stopEntered         chan<- struct{}
	stopGate            <-chan struct{}
	listedUnits         []launcher.Unit
	resourceMu          sync.Mutex

	lastTaskError error
}

func (l *countingLauncher) Start(ctx context.Context, unit string) error {
	l.starts.Add(1)
	if l.started != nil {
		select {
		case l.started <- struct{}{}:
		default:
		}
	}
	if l.startGate != nil {
		select {
		case <-l.startGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if l.orch != nil {
		prefix := strings.TrimSuffix(l.orch.cfg.Units.Runner, ".service")
		if strings.HasPrefix(unit, prefix) {
			runID := l.orch.unitToRunID(unit)
			// The systemd StartUnit call context only bounds the D-Bus job. The
			// launched process has its own lifetime and keeps waiting afterward.
			go l.runSandbox(runID)
		}
	}
	return nil
}

func (l *countingLauncher) runSandbox(runID string) {
	sid, ok, err := l.orch.WaitAssignment(context.Background(), runKindSandbox, runID)
	if err != nil || !ok {
		l.reportReadinessError(err)
		return
	}
	if l.assigned != nil {
		select {
		case l.assigned <- sid:
		default:
		}
	}
	ctx := l.orch.asyncCtx()
	if l.connectGate != nil {
		select {
		case <-l.connectGate:
		case <-ctx.Done():
			return
		}
	}
	if l.readinessNoConnect {
		return
	}
	conn, err := net.DialUnix("unix", nil, &net.UnixAddr{
		Name: configsock.ReadinessSocketPath(l.orch.cfg.Paths.RunRoot, sid),
		Net:  "unix",
	})
	if err != nil {
		l.reportReadinessError(err)
		return
	}
	defer conn.Close()
	if l.readyConnected != nil {
		select {
		case l.readyConnected <- struct{}{}:
		default:
		}
	}
	taskSpec, found, err := l.orch.SandboxTaskSpecFor(ctx, sid, runID)
	if err != nil || !found {
		l.reportReadinessError(err)
		return
	}
	if taskSpec.Prepare != nil {
		if l.artifactLaunchModes != nil {
			select {
			case l.artifactLaunchModes <- taskSpec.Prepare.LaunchMode:
			default:
			}
		}
		if l.snapshotRoots != nil {
			select {
			case l.snapshotRoots <- taskSpec.Prepare.RootRef:
			default:
			}
		}
		if l.artifactPrepareErr != nil {
			l.reportReadinessError(l.artifactPrepareErr)
			return
		}
		preparedKind := types.ResumeSourceSandbox
		if taskSpec.Prepare.LaunchMode == string(types.LaunchMemory) {
			preparedKind = types.ResumeSourceSnapshot
		}
		summary := configsock.ArtifactPrepareSummary{
			SchemaVersion:      configsock.ArtifactPrepareSchemaVersion,
			PreparedSourceKind: string(preparedKind),
			Capacity:           configsock.ArtifactCapacity{CPU: 2, Memory: "2GiB"},
			DiskTopology:       validArtifactDiskTopology(),
			ResolutionDigest:   strings.Repeat("0", 64),
			RequiredRefCount:   1,
		}
		if l.artifactSummary != nil {
			summary = *l.artifactSummary
		}
		if summary.RootSource.Empty() {
			summary.RootSource = types.ResumeSource{Kind: types.ResumeSourceKind(taskSpec.Prepare.RootSourceKind), Ref: taskSpec.Prepare.RootRef,
				SandboxRef: taskSpec.Prepare.RootSandboxRef}
			if summary.RootSource.Kind == types.ResumeSourceSnapshot && summary.RootSource.SandboxRef == "" {
				summary.RootSource.SandboxRef = "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
			}
		}
		if _, err := l.orch.CompleteSandboxPrepare(ctx, sid, runID, summary); err != nil {
			l.reportReadinessError(err)
			return
		}
	}
	if l.readinessDelay > 0 {
		timer := time.NewTimer(l.readinessDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return
		}
	}
	if l.readinessNoSend {
		// The orchestrator closes the accepted connection on timeout/cancel,
		// which gives this deliberately silent fake a bounded exit path.
		_, _ = io.Copy(io.Discard, conn)
		return
	}
	wire := l.readinessWire
	if wire == nil {
		wire = []byte("control_ready\nready\n")
	}
	if _, err := io.Copy(conn, bytes.NewReader(wire)); err != nil {
		l.reportReadinessError(err)
	}
}

func (l *countingLauncher) reportReadinessError(err error) {
	if err == nil {
		return
	}
	l.resourceMu.Lock()
	l.lastTaskError = err
	l.resourceMu.Unlock()
	if l.readinessErrors == nil {
		return
	}
	select {
	case l.readinessErrors <- err:
	default:
	}
}

func (l *countingLauncher) taskError() error {
	l.resourceMu.Lock()
	defer l.resourceMu.Unlock()
	return l.lastTaskError
}

func (l *countingLauncher) Stop(ctx context.Context, unit string) error {
	l.stops.Add(1)
	if l.stopEntered != nil {
		select {
		case l.stopEntered <- struct{}{}:
		default:
		}
	}
	if l.stopGate != nil {
		select {
		case <-l.stopGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	l.resourceMu.Lock()
	for i := range l.listedUnits {
		if l.listedUnits[i].Name == unit {
			l.listedUnits[i].ActiveState = "inactive"
		}
	}
	l.resourceMu.Unlock()
	return nil
}
func (l *countingLauncher) ResetFailed(context.Context, string) error { return nil }
func (l *countingLauncher) List(context.Context, string) ([]launcher.Unit, error) {
	l.resourceMu.Lock()
	defer l.resourceMu.Unlock()
	return append([]launcher.Unit(nil), l.listedUnits...), nil
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

// TestResumeRace_ConnectAndConcurrentAdmissionSingleLaunch guards the common
// lifecycle admission race: an asynchronous /connect resume and concurrent
// joiners of that attempt must still launch once.
func TestResumeRace_ConnectAndConcurrentAdmissionSingleLaunch(t *testing.T) {
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
	dir := shortOrchestratorTestDir(t)
	cfg.Paths.RunRoot = filepath.Join(dir, "run")
	cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31" // resolveNetwork needs a valid CIDR

	started := make(chan struct{}, 4)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc.orch = o
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	o.SetClusterContext(ctx)
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}

	// bare-img with no snapshot ref → launch reaches lc.Start without the e2b
	// readiness wait or the snapshot-probe exec (RestoreRefFor returns "").
	mk := strings.Repeat("a", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	sid := "sbx-race-1"
	sb := &types.Sandbox{
		ID: sid, Profile: types.ProfileBare, TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(), State: types.StatePaused,
		ResumeSource: types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64)},
		APISecret:    apiSecret,
		ManifestKey:  mk,
		RunDir:       nodepath.SandboxRunDir(cfg.Paths.RunRoot, sid),
		BaseDir:      nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sid),
		CreatedUnix:  1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	// Connect returns while its asynchronous resume is blocked in launcher.Start.
	// That guarantees the admissions below join the same active attempt.
	connected, err := o.Connect(ctx, sid, apiKey, "", api.ConnectOptions{TimeoutSec: 60})
	if err != nil || connected == nil || connected.State != types.StateStarting {
		t.Fatalf("Connect = %+v, %v; want durable starting result", connected, err)
	}
	waitForLauncherStart(t, started)
	if active, found := o.launches.Lookup(sid); !found || active.Kind() != launchResume {
		t.Fatal("Connect resume did not retain an active launch attempt")
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	join := func() error {
		_, attempt, err := o.ensureResumeAccepted(ctx, sid, nil, types.ResumeRequest{
			Trigger: types.ResumeTriggerRoute, Mode: types.ResumeAuto,
		}, nil)
		if err == nil && attempt != nil {
			err = attempt.wait(ctx)
		}
		return err
	}
	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- join()
		}()
	}
	// Execute one join in this goroutine while the Connect attempt is known to be
	// active. The timer releases launcher.Start; until then the join must wait on
	// that same attempt rather than starting a second resume.
	released := make(chan struct{})
	var releaseOnce sync.Once
	releaseStart := func() {
		releaseOnce.Do(func() {
			close(startGate)
			close(released)
		})
	}
	timer := time.AfterFunc(100*time.Millisecond, releaseStart)
	routeErr := join()
	returnedBeforeRelease := timer.Stop()
	if returnedBeforeRelease {
		releaseStart()
	} else {
		<-released
	}
	errs <- routeErr
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("admission during Connect resume: %v", err)
		}
	}
	if returnedBeforeRelease {
		t.Fatal("admission returned while the asynchronous Connect resume was still blocked")
	}
	waitForSandbox(t, o, ctx, sid, func(sb *types.Sandbox) bool {
		return sb.State == types.StateRunning
	}, "running after concurrent resume admission")

	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launch (lc.Start) called %d times; want exactly 1 for concurrent admissions", got)
	}
	got, err := st.Get(ctx, sid)
	if err != nil || got == nil || got.State != types.StateRunning {
		t.Fatalf("sandbox should be running after resume: %+v (err=%v)", got, err)
	}
}
