package orch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/idleclock"
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
