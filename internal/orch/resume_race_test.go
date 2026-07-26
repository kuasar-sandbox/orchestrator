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

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

// countingLauncher records how many times a unit was started — the launch count.
type countingLauncher struct {
	starts atomic.Int64
	orch   *Orchestrator
}

func (l *countingLauncher) Start(ctx context.Context, unit string) error {
	l.starts.Add(1)
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
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	sid := "sbx-race-1"
	sb := &types.Sandbox{
		ID: sid, TemplateID: "bare-img-" + strings.Repeat("b", 64), State: types.StatePaused,
		APISecret:   apiSecret,
		ManifestKey: mk,
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
		launchG(func() { _, _ = o.Route(ctx, sid, 49983) })
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
