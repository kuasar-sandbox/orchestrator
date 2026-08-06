package orch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

type blockedCreateVS struct {
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (v *blockedCreateVS) Attach(ctx context.Context, _ vswitch.AttachReq) (*vswitch.Port, error) {
	v.once.Do(func() { close(v.entered) })
	select {
	case <-v.gate:
		return &vswitch.Port{
			Port: "create-port", FloatingIP: "169.254.1.2",
			MAC: "02:00:00:00:00:31", InnerIP: "169.254.1.1",
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*blockedCreateVS) Detach(context.Context, string) error { return nil }
func (*blockedCreateVS) TapFD(string) vswitch.TapFD {
	return vswitch.TapFD{Exec: []string{"true"}}
}

type failingCreateVS struct{ err error }

func (v failingCreateVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	return nil, v.err
}
func (failingCreateVS) Detach(context.Context, string) error { return nil }
func (failingCreateVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{} }

type blockedKillAttachVS struct {
	entered  chan struct{}
	gate     chan struct{}
	detached chan string
	once     sync.Once
}

func (v *blockedKillAttachVS) Attach(context.Context, vswitch.AttachReq) (*vswitch.Port, error) {
	v.once.Do(func() { close(v.entered) })
	<-v.gate // deliberately model a connector call that is late to observe cancel
	return &vswitch.Port{
		Port: "late-kill-port", FloatingIP: "169.254.1.3",
		MAC: "02:00:00:00:00:32", InnerIP: "169.254.1.1",
	}, nil
}
func (v *blockedKillAttachVS) Detach(_ context.Context, port string) error {
	if port != "" {
		v.detached <- port
	}
	return nil
}
func (*blockedKillAttachVS) TapFD(string) vswitch.TapFD { return vswitch.TapFD{} }

func createRequestFixture(t *testing.T, o *Orchestrator, marker string) api.CreateReq {
	t.Helper()
	manifestKey := strings.Repeat(marker, 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	if _, err := o.st.AddKeyPair(context.Background(), store.KeyPair{
		APISecret: apiSecret, ManifestKey: manifestKey,
	}, "test", 0, ""); err != nil {
		t.Fatal(err)
	}
	return api.CreateReq{
		APIKey:     apiKey,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		TimeoutSec: 60,
		Metadata:   map[string]string{"test-key": "test-value"},
		EnvVars:    map[string]string{"TEST_ENV": "test-value"},
	}
}

func TestCreateReturnsBeforeResourcePreparationAndOutlivesRequestContext(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, lifecycleCtx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	req := createRequestFixture(t, o, "1")
	events, stopEvents := o.Subscribe()
	defer stopEvents()

	requestCtx, cancelRequest := context.WithCancel(lifecycleCtx)
	type createResult struct {
		sb  *types.Sandbox
		err error
	}
	done := make(chan createResult, 1)
	go func() {
		sb, err := o.Create(requestCtx, req)
		done <- createResult{sb: sb, err: err}
	}()
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("background create did not enter resource preparation")
	}
	var result createResult
	select {
	case result = <-done:
		if result.err != nil || result.sb == nil {
			t.Fatalf("Create = %+v, %v", result.sb, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create waited for blocked resource preparation")
	}
	if result.sb.State != types.StateStarting || result.sb.RunID != "" || result.sb.VswitchPort != "" {
		t.Fatalf("accepted Create result = %+v", result.sb)
	}
	stored, err := o.st.Get(lifecycleCtx, result.sb.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" || stored.VswitchPort != "" {
		t.Fatalf("durable acceptance = %+v, %v", stored, err)
	}
	initial := <-events
	if initial.Kind != routesync.TypeUpsert || initial.Route.State != routesync.StateStarting ||
		initial.Route.SandboxID != result.sb.ID || initial.Route.FloatingIP != "" {
		t.Fatalf("initial starting route = %+v", initial)
	}

	// The encoder result, cache snapshot, and launch worker must not share mutable
	// maps or the sandbox pointer. Mutate the caller-owned result while the worker
	// is still blocked; neither durable state nor the eventual launch may change.
	if cached := o.lookup(result.sb.ID); cached == result.sb {
		t.Fatal("Create response aliases the cache entry")
	}
	result.sb.State = types.StateDead
	result.sb.Metadata["test-key"] = "caller-mutated"
	result.sb.Env["TEST_ENV"] = "caller-mutated"
	cancelRequest()
	close(blocked.gate)
	running := waitForSandbox(t, o, lifecycleCtx, stored.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after request cancellation")
	if running.Metadata["test-key"] != "test-value" || running.Env["TEST_ENV"] != "test-value" {
		t.Fatalf("caller mutation reached background launch: %+v", running)
	}
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("runner starts = %d, want 1", got)
	}
}

func TestCreateReturnsBeforeRunnerAssignment(t *testing.T) {
	cfg := &config.Config{}
	started := make(chan struct{}, 1)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	req := createRequestFixture(t, o, "2")

	result, err := o.Create(ctx, req)
	if err != nil || result == nil || result.State != types.StateStarting {
		t.Fatalf("Create = %+v, %v", result, err)
	}
	waitForLauncherStart(t, started)
	stored, err := o.st.Get(ctx, result.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" || stored.VswitchPort == "" {
		t.Fatalf("pre-assignment starting row = %+v, %v", stored, err)
	}
	close(startGate)
	waitForSandbox(t, o, ctx, result.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning && current.RunID != ""
	}, "running after runner assignment")
}

func TestCreateBackgroundFailureReturnsAcceptedThenPublishesDelete(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	wantErr := errors.New("attach failed")
	o.vs = failingCreateVS{err: wantErr}
	req := createRequestFixture(t, o, "3")
	events, stopEvents := o.Subscribe()
	defer stopEvents()

	accepted, err := o.Create(ctx, req)
	if err != nil || accepted == nil || accepted.State != types.StateStarting {
		t.Fatalf("Create = %+v, %v", accepted, err)
	}
	dead := waitForSandbox(t, o, ctx, accepted.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateDead
	}, "dead after network attach failure")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("failed create retained ownership: %+v", dead)
	}
	for _, want := range []string{routesync.StateStarting, routesync.TypeDelete} {
		select {
		case event := <-events:
			if want == routesync.TypeDelete {
				if event.Kind != routesync.TypeDelete || event.SID != accepted.ID {
					t.Fatalf("failure event = %+v, want Delete", event)
				}
			} else if event.Kind != routesync.TypeUpsert || event.Route.State != want {
				t.Fatalf("failure event = %+v, want %s", event, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing %s event", want)
		}
	}
}

func TestCreateRunnerWaitTimeoutRollsBackDead(t *testing.T) {
	cfg := &config.Config{}
	cfg.Units.PoolWaitTimeout = "30ms"
	started := make(chan struct{}, 1)
	lc := &countingLauncher{started: started, startGate: make(chan struct{})}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	req := createRequestFixture(t, o, "4")

	accepted, err := o.Create(ctx, req)
	if err != nil || accepted == nil || accepted.State != types.StateStarting {
		t.Fatalf("Create = %+v, %v", accepted, err)
	}
	waitForLauncherStart(t, started)
	dead := waitForSandbox(t, o, ctx, accepted.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateDead
	}, "dead after runner wait timeout")
	if dead.RunID != "" || dead.VswitchPort != "" {
		t.Fatalf("runner timeout retained ownership: %+v", dead)
	}
}

func TestCreateRejectsClosedLifecycleBeforeInsert(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, _ := newAsyncConnectTestOrchestrator(t, cfg, lc)
	req := createRequestFixture(t, o, "5")
	closed, cancel := context.WithCancel(context.Background())
	cancel()
	o.SetLifecycleContext(closed)

	if sb, err := o.Create(context.Background(), req); !errors.Is(err, context.Canceled) || sb != nil {
		t.Fatalf("Create with closed lifecycle = %+v, %v", sb, err)
	}
	count := 0
	if err := o.st.RangeByState(context.Background(), types.StateStarting, func(*types.Sandbox) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("closed lifecycle inserted %d starting rows", count)
	}
}

func TestKillDuringCreateAttachFencesClaimUntilLateCleanup(t *testing.T) {
	cfg := &config.Config{}
	lc := &countingLauncher{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	vs := &blockedKillAttachVS{
		entered: make(chan struct{}), gate: make(chan struct{}), detached: make(chan string, 1),
	}
	o.vs = vs
	req := createRequestFixture(t, o, "6")
	accepted, err := o.Create(ctx, req)
	if err != nil || accepted == nil {
		t.Fatalf("Create = %+v, %v", accepted, err)
	}
	select {
	case <-vs.entered:
	case <-time.After(time.Second):
		t.Fatal("create did not enter network attach")
	}
	attempt, found := o.launches.Lookup(accepted.ID)
	if !found {
		t.Fatal("accepted create has no launch owner")
	}
	if killed, err := o.Kill(ctx, accepted.ID, req.APIKey); err != nil || !killed {
		t.Fatalf("Kill = %v, %v", killed, err)
	}
	if _, err := o.launches.Claim(ctx, accepted.ID, launchCreate); !errors.Is(err, errLaunchClaimed) {
		t.Fatalf("claim before late attach cleanup = %v, want cleanup fence", err)
	}
	close(vs.gate)
	select {
	case port := <-vs.detached:
		if port != "late-kill-port" {
			t.Fatalf("detached port = %q", port)
		}
	case <-time.After(time.Second):
		t.Fatal("late attached port was not detached")
	}
	if err := attempt.wait(ctx); err == nil {
		t.Fatal("killed launch reported success")
	}
	if stored, err := o.st.Get(ctx, accepted.ID); err != nil || stored != nil {
		t.Fatalf("late attach resurrected deleted sandbox: %+v, %v", stored, err)
	}
	next, err := o.launches.Claim(ctx, accepted.ID, launchCreate)
	if err != nil {
		t.Fatalf("claim after cleanup: %v", err)
	}
	o.launches.Finish(next, nil)
}
