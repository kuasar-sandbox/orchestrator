package orch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestHoldAcrossEntrypointsPauseAndRunChange(t *testing.T) {
	ctx := context.Background()
	o, sb, key, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	sb.Metadata = map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"type":"hold-traffic","path":"/work"}]}`}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := o.GetSandboxHold(ctx, sb.ID, "wrong key"); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := o.SetGuestHold(ctx, sb.ID, sb.RunID, "/missing", 1, nil); !errors.Is(err, api.ErrNotFound) {
		t.Fatal(err)
	}
	zero := int64(0)
	if v, err := o.SetGuestHold(ctx, sb.ID, sb.RunID, "/work", 2, &zero); err != nil || v != 2 {
		t.Fatal(v, err)
	}
	if v, err := o.GetSandboxHold(ctx, sb.ID, key); err != nil || v != 2 {
		t.Fatal(v, err)
	}
	if v, err := o.SetSandboxHold(ctx, sb.ID, key, 2, &zero); !errors.Is(err, api.ErrHoldNotMatched) || v != 2 {
		t.Fatal(v, err)
	}
	// Even a stale immutable snapshot cannot undo a committed hold projection.
	if h := o.routeEntry(sb).Hold; h == nil || h.Value != 2 {
		t.Fatal("stale projection", h)
	}
	b := &fakePauseBarrier{fakeRouteBarrier: newFakeRouteBarrier(nil, nil), activity: routesync.PauseAck{}}
	b.release()
	o.pauseBarriers = b
	if _, err := o.Pause(ctx, sb.ID, key, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, IfIdle: true}); !errors.Is(err, api.ErrSandboxBusy) {
		t.Fatal(err)
	}
	sb.RunID = "next-run"
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := o.SetGuestHold(ctx, sb.ID, "checkpoint-run", "/work", 0, nil); !errors.Is(err, api.ErrHoldStateConflict) {
		t.Fatal("late guest write", err)
	}
	if v, err := o.GetGuestHold(ctx, sb.ID, sb.RunID, "/work"); err != nil || v != 2 {
		t.Fatal(v, err)
	}
}

func TestHoldPauseWinsAndHostReadDoesNotWake(t *testing.T) {
	ctx := context.Background()
	o, sb, key, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	b := &fakePauseBarrier{fakeRouteBarrier: newFakeRouteBarrier(nil, nil)}
	o.pauseBarriers = b
	paused := make(chan error, 1)
	go func() {
		_, err := o.Pause(ctx, sb.ID, key, sandboxcfg.CaptureRequest{Kind: types.CaptureSnapshot, IfIdle: true})
		paused <- err
	}()
	<-b.waiting
	wrote := make(chan error, 1)
	go func() { _, err := o.SetSandboxHold(ctx, sb.ID, key, 3, nil); wrote <- err }()
	b.release()
	if err := <-paused; err != nil {
		t.Fatal(err)
	}
	if err := <-wrote; !errors.Is(err, api.ErrHoldStateConflict) {
		t.Fatal(err)
	}
	if v, err := o.GetSandboxHold(ctx, sb.ID, key); err != nil || v != 0 {
		t.Fatal(v, err)
	}
	current, err := o.st.Get(ctx, sb.ID)
	if err != nil || current.State != types.StatePaused {
		t.Fatal("read woke sandbox", err)
	}
}

func TestHoldZeroHistoryAndRecovery(t *testing.T) {
	ctx := context.Background()
	o, sb, key, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	wall := time.Now().Add(-time.Hour)
	point := func(n int64) idleclock.Point { return idleclock.Point{Wall: wall.Add(time.Duration(n)), BootNS: n} }
	n := int64(20)
	o.idleNow = func() idleclock.Point { return point(n) }
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 0, nil); err != nil {
		t.Fatal(err)
	}
	if h := o.projectHold(sb); h == nil || h.IdleSince != nil {
		t.Fatal("initial no-op created boundary", h)
	}
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 0, nil); err != nil {
		t.Fatal(err)
	}
	n = 30
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 0, nil); err != nil {
		t.Fatal(err)
	}
	h := o.projectHold(sb)
	if h == nil || h.IdleSinceBootNS != 20 {
		t.Fatal("release or duplicate zero mishandled", h)
	}
	proof := idleclock.Proof{}
	proof.Observe(point(1))
	proof.Observe(point(40))
	ack := routesync.PauseAck{AutomaticSafe: true, Evidence: []routesync.IdleEvidence{{SinceBootNS: 2, Points: []idleclock.Point{point(2)}, Clock: proof}}}
	with := withHoldActivity(ack, h)
	old, after := point(10).Wall, point(20).Wall
	for _, before := range []sandboxcfg.CaptureRequest{{IfIdle: true, IfIdleSince: &old}, {IfIdle: true, IdleSinceBootNS: 10}} {
		if err := checkIdleEvidence(with, before); !errors.Is(err, api.ErrSandboxBusy) {
			t.Fatal("old threshold survived pulse", err)
		}
	}
	if err := checkIdleEvidence(with, sandboxcfg.CaptureRequest{IfIdle: true, IfIdleSince: &after}); err != nil {
		t.Fatal(err)
	}
	fresh, _ := o.st.Get(ctx, sb.ID)
	restarted := &Orchestrator{idleNow: o.idleNow}
	if got := restarted.projectHold(fresh); got == nil || got.IdleSinceBootNS != 30 {
		t.Fatal("reused historical pre-restart zero", got)
	}
	if len(ack.Evidence[0].Points) != 1 || ack.Evidence[0].SinceBootNS != 2 {
		t.Fatal("mutated physical evidence")
	}
}

func TestBuildRegisterRejectsHoldRoute(t *testing.T) {
	o := testOrchCfg(t, mmdsFeatureConfig())
	key, _, _ := allowlistedBuildIdentity(t, o)
	header := `{"routes":[{"path":"/work","type":"hold-traffic"}]}`
	if _, err := o.RegisterBuild(context.Background(), key, api.RegisterSpec{Profile: types.ProfileE2B, Resources: testBuildResources(), MMDSHeader: &header}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatal(err)
	}
}

func TestCreateGuestHoldRequiresServingProxySupport(t *testing.T) {
	o, ctx := newAsyncConnectTestOrchestrator(t, mmdsFeatureConfig(), &countingLauncher{})
	req := createRequestFixture(t, o, "7")
	req.Metadata[sandboxcfg.NsMMDS] = `{"routes":[{"path":"/work","type":"hold-traffic"}]}`
	req.Metadata[sandboxcfg.NsIdentity] = `{"id":"guest-hold"}`
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: newFakeRouteBarrier(nil, nil)})
	if created, err := o.Create(ctx, req); created != nil || !errors.Is(err, api.ErrProxyUnavailable) {
		t.Fatalf("accepted unsupported guest declaration: %+v, %v", created, err)
	}
	if stored, err := o.st.Get(ctx, "guest-hold"); err != nil || stored != nil {
		t.Fatalf("failed admission persisted a sandbox: %+v, %v", stored, err)
	}

	o.SetProxyRouteBarrierCoordinator(immediateRouteBarrierCoordinator{})
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	defer close(blocked.gate)
	created, err := o.Create(ctx, req)
	if err != nil || created == nil || created.ID != "guest-hold" {
		t.Fatalf("capable proxy rejected guest declaration: %+v, %v", created, err)
	}
}

func TestHoldDeleteAndRecreateSameSID(t *testing.T) {
	ctx := context.Background()
	o, sb, key, _, _, _ := newCheckpointPauseFixture(t, checkpointOrchestratorConfig(t, config.CheckpointLocal), "")
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 0, nil); err != nil {
		t.Fatal(err)
	}
	old, err := o.st.Get(ctx, sb.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.st.Delete(ctx, sb.ID); err != nil {
		t.Fatal(err)
	}
	o.publishDelete(sb.ID)
	// A full-sync range may still hold the prior SQLite row snapshot.
	_ = o.projectHold(old)
	if len(o.holds) != 0 {
		t.Fatal("late full-sync read resurrected deleted per-parent evidence")
	}
	sb.InstanceID = ""
	if err := o.st.InsertSandbox(ctx, sb); err != nil {
		t.Fatal(err)
	}
	fresh, err := o.st.Get(ctx, sb.ID)
	if err != nil || fresh.InstanceID == old.InstanceID {
		t.Fatal(fresh, err)
	}
	if h := o.projectHold(fresh); h == nil || h.Value != 0 || h.IdleSince != nil {
		t.Fatal("new parent's initial state inherited old hold history", h)
	}
}

func TestHoldAcrossLegacyProxyHandover(t *testing.T) {
	o, sb, _, clock := idleFixture(t)
	key := mintTestAPIKey(t, sb.APISecret)
	ctx := context.Background()
	zero, one := int64(0), int64(1)
	since := o.idleTime().Wall
	// exec became idle after a backwards wall-clock step. The legacy Proxy
	// chose it as the last physical service using CLOCK_BOOTTIME.
	beforeClockStep := since.Add(time.Minute)
	legacyStats := &api.TrafficStats{State: "running", IdleSince: &since, Services: map[string]api.ServiceTrafficStats{
		"forward": {IdleSince: &beforeClockStep}, "exec": {IdleSince: &since},
	}}
	physicalCtx := proxystats.WithPhysicalTelemetry(ctx)
	// Ordinary v11 service remains usable, without fabricating services.hold.
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{})
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 1, &zero); !errors.Is(err, api.ErrProxyUnavailable) {
		t.Fatal(err)
	}
	if n, err := o.GetSandboxHold(ctx, sb.ID, key); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	observeIdle(o, sb, 1)
	if o.idleCandidates[sb.ID] == nil {
		t.Fatal("legacy idle timer lost")
	}
	if s, err := o.checkIngressHold(ctx, sb, legacyStats); err != nil || s != legacyStats {
		t.Fatal(s, err)
	}

	o.SetProxyRouteBarrierCoordinator(immediateRouteBarrierCoordinator{})
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 1, &zero); err != nil {
		t.Fatal(err)
	}
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{})
	// Legacy physical idle reports contain no hint about the retained hold.
	observeIdle(o, sb, 1)
	if o.idleCandidates[sb.ID] != nil {
		t.Fatal("legacy sample armed timer despite retained hold")
	}
	for _, readCtx := range []context.Context{ctx, physicalCtx} {
		if _, err := o.checkIngressHold(readCtx, sb, legacyStats); !errors.Is(err, api.ErrStatsUnavailable) {
			t.Fatal("published false complete idle", err)
		}
	}
	if n, err := o.GetSandboxHold(ctx, sb.ID, key); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	clock.Add(int64(2 * time.Second))
	released := o.idleTime()
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 0, &one); err != nil {
		t.Fatal("cannot release after handover", err)
	}
	observeIdle(o, sb, 1)
	c := o.idleCandidates[sb.ID]
	if c == nil || c.observation.SinceBootNS != released.BootNS || c.t0.BootNS != released.BootNS {
		t.Fatalf("did not start a fresh timeout: %#v", c)
	}
	if s, err := o.checkIngressHold(ctx, sb, legacyStats); err != nil || s.IdleSince != nil || !legacyStats.IdleSince.Equal(since) {
		t.Fatal(s, err)
	}
	if s, err := o.checkIngressHold(physicalCtx, sb, legacyStats); err != nil || s.IdleSince == nil || !s.IdleSince.Equal(since) {
		t.Fatal("physical view lost the Proxy idle timestamp", s, err)
	}
	clock.Add(int64(time.Second))
	if _, err := o.SetSandboxHold(ctx, sb.ID, key, 0, nil); err != nil {
		t.Fatal(err)
	}
	observeIdle(o, sb, 1)
	if o.idleCandidates[sb.ID] != c {
		t.Fatal("duplicate zero moved timer")
	}
}

func TestLegacyProxyNativeIdleAfterHoldRelease(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	id := batchSandboxes(t, o, 1)[0]
	sb, err := o.st.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	sb.RunID = "run"
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	key := mintTestAPIKey(t, sb.APISecret)
	physical := idleclock.Point{BootNS: 10, Wall: time.Unix(200, 0).UTC()}
	services := map[string]api.ServiceTrafficStats{"exec": {IdleSince: &physical.Wall}}
	source := &api.TrafficStats{State: "running", Services: services, IdleSince: &physical.Wall}
	o.SetSandboxTrafficProvider(nativeTrafficFunc(func(context.Context, string) (*api.TrafficStats, error) {
		return source, nil // Only the wall timestamp crosses a legacy stats socket.
	}))
	for _, tc := range []struct {
		name        string
		releaseWall int64
		wantIdle    bool
	}{
		{"wall steps back", 100, false},
		{"wall advances normally", 300, false},
		{"both wall timestamps agree", 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := physical
			o.idleNow = func() idleclock.Point { return now }
			if _, err := o.SetSandboxHold(ctx, id, key, 1, nil); err != nil {
				t.Fatal(err)
			}
			now = idleclock.Point{BootNS: 20, Wall: time.Unix(tc.releaseWall, 0).UTC()}
			if _, err := o.SetSandboxHold(ctx, id, key, 0, nil); err != nil {
				t.Fatal(err)
			}
			got, err := o.TrafficStats(ctx, id, key)
			if err != nil || got == nil || got.Services["exec"] != services["exec"] || got.Inflight != source.Inflight {
				t.Fatalf("traffic response = %+v, %v", got, err)
			}
			if (got.IdleSince != nil) != tc.wantIdle || (got.IdleSince != nil && !got.IdleSince.Equal(physical.Wall)) {
				t.Fatalf("native idle timestamp = %v", got.IdleSince)
			}
		})
	}
}
