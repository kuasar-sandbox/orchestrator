package orch

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func TestResourcePoolBuilderPhaseJournalReconcilesCrashBoundaries(t *testing.T) {
	o, s, _ := balanceFixture(t)
	ctx := context.Background()
	run := "br-00000000-0000-7000-8000-000000000516"
	b := buildReconcileRow(t, run)
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	prep, err := decodeBuildRuntimePreparation(b.RuntimePrepareJSON)
	if err != nil {
		t.Fatal(err)
	}
	p := &pendingBuild{build: b, templateID: b.TemplateID, resources: prep.Resources, sandboxResources: prep.SandboxResources, result: make(chan configsock.BuildResult, 1)}
	o.pend[b.BuildID] = p
	o.buildRecoveryReady = nil
	m := store.RunPoolOwnership{RunID: run, Kind: runKindBuild, Pool: "b0", ResourcePool: "numa0", Unit: instanceUnit("b0@.service", run), TaskID: b.BuildID, CapacityMemory: 1 << 30, CapacityCPU: 1000}
	if err := o.st.SaveRunPoolOwnership(ctx, m); err != nil {
		t.Fatal(err)
	}
	o.lc = &reconcileLauncher{units: []launcher.Unit{{Name: m.Unit, ActiveState: "active"}}}
	// Remove the unrelated pressure fixture so phase Create permission is green.
	if _, _, err := s.Release("old-token"); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireExecution("moving", "run-moving"); err != nil {
		t.Fatal(err)
	}
	resolved, err := nodectl.Resolve(o.cfg.ResourceListen)
	if err != nil {
		t.Fatal(err)
	}
	s = nodectl.NewPoolState(resolved)
	o.memoryPressure.Load().state = s
	if err := o.restoreResourcePlacements(ctx, s); err != nil {
		t.Fatal(err)
	}
	sid := "bp-a-pool-test"
	// Simulate a crash after the authoritative phase CAS but before updating
	// the pool journal. Restart must replace the prelaunch charge, not add it.
	if err := o.st.SetBuildPhase(ctx, b.BuildID, "a", sid, "starting"); err != nil {
		t.Fatal(err)
	}
	restarted := nodectl.NewPoolState(resolved)
	if err := o.restoreResourcePlacements(ctx, restarted); err != nil {
		t.Fatal(err)
	}
	if got := restarted.PoolSnapshots()[0].Scheduled; got.MemoryBytes != 1<<30 || got.CPUMilli != 1000 {
		t.Fatalf("phase charge=%+v", got)
	}
	if restarted.SandboxPool(sid) != "numa0" {
		t.Fatal("phase lost builder resource pool")
	}
	o.memoryPressure.Load().state = restarted
	if err := o.PostBuildPhase(ctx, run, b.BuildID, "a", sid, "starting"); err != nil {
		t.Fatal(err)
	}
	// A crash after finished CAS, before retiring the identity charge, must
	// never resurrect that finished phase's scheduling reservation.
	if err := o.st.SetBuildPhase(ctx, b.BuildID, "a", sid, "finished"); err != nil {
		t.Fatal(err)
	}
	afterFinish := nodectl.NewPoolState(resolved)
	if err := o.restoreResourcePlacements(ctx, afterFinish); err != nil {
		t.Fatal(err)
	}
	if got := afterFinish.PoolSnapshots()[0].Scheduled; got != (nodectl.Resources{}) {
		t.Fatalf("finished phase resurrected: %+v", got)
	}
	o.memoryPressure.Load().state = afterFinish
	if err := o.PostBuildPhase(ctx, run, b.BuildID, "b", "bp-b-pool-test", "starting"); err != nil {
		t.Fatal(err)
	}
	if err := o.PostBuildPhase(ctx, run, b.BuildID, "b", "bp-b-pool-test", "finished"); err != nil {
		t.Fatal(err)
	}
	if got := afterFinish.PoolSnapshots()[0].Scheduled; got != (nodectl.Resources{}) {
		t.Fatalf("completed phase charge=%+v", got)
	}
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil || !stored.ExecutionClaimed || stored.Resources != b.Resources {
		t.Fatalf("phase changed business quota: %+v %v", stored, err)
	}
}
