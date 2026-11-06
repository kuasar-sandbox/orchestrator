package orch

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func balanceFixture(t *testing.T) (*Orchestrator, *nodectl.State, *types.Sandbox) {
	t.Helper()
	cfg := buildNetworkTestConfig()
	mem := "1GiB"
	cfg.ResourceListen = &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "numa0", Allocatable: config.ResourceAllocatable{Memory: &mem}}, {Name: "numa1", Allocatable: config.ResourceAllocatable{Memory: &mem}}}}
	cfg.Units.RunnerPools = []config.RunPoolConfig{{Name: "r0", Unit: "r0@.service", ResourcePool: "numa0"}, {Name: "r1", Unit: "r1@.service", ResourcePool: "numa1"}}
	cfg.Units.BuilderPools = []config.RunPoolConfig{{Name: "b0", Unit: "b0@.service", ResourcePool: "numa0"}}
	o := testOrchCfg(t, cfg)
	o.lc = &reconcileLauncher{}
	r, err := nodectl.Resolve(cfg.ResourceListen)
	if err != nil {
		t.Fatal(err)
	}
	s := nodectl.NewPoolState(r)
	a := nodectl.NewAdmissionController(nodectl.AdmissionPolicy{Rate: 100, Burst: 100})
	if err := o.ConfigureMemoryPressure(context.Background(), s, a, r.Pressure); err != nil {
		t.Fatal(err)
	}
	sb := &types.Sandbox{ID: "moving", RunID: "run-moving", State: types.StateRunning, RunningSinceUnixNano: time.Now().Add(-time.Minute).UnixNano()}
	capacity := nodectl.Resources{MemoryBytes: 1 << 30, CPUMilli: 2000}
	if _, err := s.ReservePlacement(sb.ID, sb.RunID, capacity, nodectl.OperationCreate, "numa1", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Admit(nodectl.AdmitSpec{SandboxID: sb.ID, Token: "old-token", Capacity: capacity, InitialBudget: 980 << 20}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.SetSettled("old-token", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	return o, s, sb
}

func TestResourcePoolBalanceRequiresRawCriticalAndFeasibleDestination(t *testing.T) {
	o, s, sb := balanceFixture(t)
	plans := o.pressurePausePlans([]*types.Sandbox{sb}, time.Now(), nil)
	if len(plans) != 1 || plans[0].destination != "numa1" {
		t.Fatalf("balance plans=%+v", plans)
	}
	// A persistent obligation holds effective critical, but after real shrink
	// this source must not continue losing its remaining running sandboxes.
	s.ObserveObligation("already-paused", 1, "paused", false)
	if _, _, err := s.ReconcileAndGrant("old-token", 100<<20, 0, nodectl.UrgencyNormal, nil); err != nil {
		t.Fatal(err)
	}
	if s.PoolStates()["numa0"].PressureSnapshot().Zone != nodectl.ZoneCritical {
		t.Fatal("fixture lost effective critical")
	}
	if plans := o.pressurePausePlans([]*types.Sandbox{sb}, time.Now(), nil); len(plans) != 0 {
		t.Fatal("sticky critical source kept being drained")
	}
}

func TestResourcePoolAllCriticalStopsOnlyNewBalance(t *testing.T) {
	o, s, sb := balanceFixture(t)
	if err := s.PoolStates()["numa1"].ConfigurePressure(nodectl.DefaultPressurePolicy(), nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Version: 1}); err != nil {
		t.Fatal(err)
	}
	if plans := o.pressurePausePlans([]*types.Sandbox{sb}, time.Now(), nil); len(plans) != 0 {
		t.Fatal("balanced into critical pool")
	}
	if !s.CanPlace("numa1", "paused", nodectl.Resources{MemoryBytes: 1 << 30}, 980<<20, nodectl.LaunchAdmission{Operation: nodectl.OperationRecovery, SavedSource: true, Background: true}) {
		t.Fatal("all critical blocked real-budget recovery")
	}
}

func TestResourcePoolBalanceDestinationPersistsAndExcludesSource(t *testing.T) {
	o, s, sb := balanceFixture(t)
	ctx := context.Background()
	// Use the real durable intent transaction. A restarted worker uses the same
	// placement record; lower source usage must not turn it into the destination.
	sb.Profile = types.ProfileBare
	sb.APISecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	sb.ManifestKey = sb.APISecret
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	p := store.RecoveryPlacement{SourcePool: "numa0", DestinationPool: "numa1", CapacityMemory: 1 << 30, CapacityCPU: 2000, Budget: 980 << 20}
	if changed, err := o.st.BeginResourcePause(ctx, sb, p); err != nil || !changed {
		t.Fatalf("intent %t %v", changed, err)
	}
	sb.ResourceObligation = true
	if _, _, err := s.Release("old-token"); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireExecution(sb.ID, sb.RunID); err != nil {
		t.Fatal(err)
	}
	exclude, err := o.recoveryExclusion(ctx, sb)
	if err != nil || exclude != "numa0" {
		t.Fatalf("destination intent lost: %q %v", exclude, err)
	}
	claim, err := s.ReservePlacement(sb.ID, "resumed", nodectl.Resources{MemoryBytes: 1 << 30, CPUMilli: 2000}, nodectl.OperationRecovery, exclude, o.runnerPool.hasSupply)
	if err != nil || claim.Pool != "numa1" {
		t.Fatalf("returned to source: %+v %v", claim, err)
	}
	s.ReleasePlacement(sb.ID, "resumed")
	zero := 0
	o.runnerPool.pools[1].capacity = &zero
	if o.canBackgroundRecover(ctx, sb) {
		t.Fatal("invalid destination fell back into source")
	}
}
