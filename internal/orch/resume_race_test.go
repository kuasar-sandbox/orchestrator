package orch

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// countingLauncher records how many times a unit was started — the launch count.
type countingLauncher struct {
	starts    atomic.Int64
	orch      *Orchestrator
	started   chan<- struct{}
	startGate <-chan struct{}
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
			go func() { _, _, _ = l.orch.WaitAssignment(context.Background(), runKindSandbox, runID) }()
		}
	}
	return nil
}
func (l *countingLauncher) Stop(context.Context, string) error        { return nil }
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

// TestResumeRace_ConnectAndRouteSingleLaunch is the regression guard for the
// control-plane resume race: an asynchronous /connect resume held inside its
// flight and concurrent data-plane Route calls must still launch exactly once.
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
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Paths.BaseRoot = filepath.Join(t.TempDir(), "lib")
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
		ID: sid, Profile: types.ProfileBare, TemplateID: "bare-img-" + strings.Repeat("b", 64), State: types.StatePaused,
		APISecret:   apiSecret,
		ManifestKey: mk,
		RunDir:      cfg.Paths.RunRoot + "/" + sid,
		BaseDir:     cfg.Paths.BaseRoot + "/" + sid,
		CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	// Connect returns while its asynchronous resume is blocked in launcher.Start.
	// That guarantees the Route calls below enter the same still-active flight.
	connected, err := o.Connect(ctx, sid, apiKey, "", 60)
	if err != nil || connected == nil || connected.State != types.StatePaused {
		t.Fatalf("Connect = %+v, %v; want paused result and async resume", connected, err)
	}
	waitForLauncherStart(t, started)
	o.sf.mu.Lock()
	activeFlight := o.sf.m[sid]
	o.sf.mu.Unlock()
	if activeFlight == nil {
		t.Fatal("asynchronous Connect resume did not register an active flight")
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 7; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := o.Route(ctx, sid, proxy.LegacyTarget(49983))
			errs <- err
		}()
	}
	// Execute one Route in this goroutine while the Connect flight is known to
	// be active. The timer channel releases launcher.Start; until then Route must
	// be waiting on that same flight rather than starting a second resume.
	released := make(chan struct{})
	var releaseOnce sync.Once
	releaseStart := func() {
		releaseOnce.Do(func() {
			close(startGate)
			close(released)
		})
	}
	timer := time.AfterFunc(100*time.Millisecond, releaseStart)
	_, routeErr := o.Route(ctx, sid, proxy.LegacyTarget(49983))
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
			t.Fatalf("Route during Connect resume: %v", err)
		}
	}
	if returnedBeforeRelease {
		t.Fatal("Route returned while the asynchronous Connect resume was still blocked")
	}
	waitForSandbox(t, o, ctx, sid, func(sb *types.Sandbox) bool {
		return sb.State == types.StateRunning
	}, "running after Connect/Route resume")

	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launch (lc.Start) called %d times; want exactly 1 — concurrent connect+route must collapse to one resume", got)
	}
	got, err := st.Get(ctx, sid)
	if err != nil || got == nil || got.State != types.StateRunning {
		t.Fatalf("sandbox should be running after resume: %+v (err=%v)", got, err)
	}
}
