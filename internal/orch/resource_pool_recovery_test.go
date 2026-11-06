package orch

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/launcher"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
)

func TestResourcePoolRestartRestoresIdentityAndBothLedgers(t *testing.T) {
	ctx := context.Background()
	cfg := buildNetworkTestConfig()
	mem := "1GiB"
	capMem := "2GiB"
	limit := 1
	cfg.ResourceListen = &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{{Name: "numa0", Allocatable: config.ResourceAllocatable{Memory: &mem}}, {Name: "numa1", Allocatable: config.ResourceAllocatable{Memory: &mem}, Capacity: config.ResourceAllocatable{Memory: &capMem}}}}
	cfg.Units.RunnerPools = []config.RunPoolConfig{{Name: "r0", Unit: "r0@.service", ResourcePool: "numa0"}, {Name: "r1", Unit: "r1@.service", ResourcePool: "numa1", Capacity: &limit}}
	cfg.Units.BuilderPools = []config.RunPoolConfig{{Name: "b1", Unit: "b1@.service", ResourcePool: "numa1"}}
	o := testOrchCfg(t, cfg)
	run := "sr-00000000-0000-7000-8000-000000000514"
	m := store.RunPoolOwnership{RunID: run, Kind: runKindSandbox, Pool: "r1", ResourcePool: "numa1", Unit: instanceUnit("r1@.service", run), TaskID: "saved", CapacityMemory: 3 << 30, CapacityCPU: 4000}
	if err := o.st.SaveRunPoolOwnership(ctx, m); err != nil {
		t.Fatal(err)
	}
	o.lc = &reconcileLauncher{units: []launcher.Unit{{Name: m.Unit, ActiveState: "active"}}}
	resolved, err := nodectl.Resolve(cfg.ResourceListen)
	if err != nil {
		t.Fatal(err)
	}
	s := nodectl.NewPoolState(resolved)
	if err := o.restoreResourcePlacements(ctx, s); err != nil {
		t.Fatal(err)
	}
	if o.runnerPool.hasSupply("numa1") {
		t.Fatal("restart hid assigned runner slot")
	}
	if p := s.PoolSnapshots()[1]; p.Scheduled.MemoryBytes != 3<<30 || p.Available.MemoryBytes != 0 {
		t.Fatalf("over-limit legacy charge lost: %+v", p)
	}
	if _, err := s.ReservePlacement("new", "new-run", nodectl.Resources{MemoryBytes: 1}, nodectl.OperationRecovery, "numa0", nil); err == nil {
		t.Fatal("lowered capacity admitted additional charge")
	}
	capacity := nodectl.Resources{MemoryBytes: 3 << 30, CPUMilli: 4000}
	if err := s.InstallProvisional(nodectl.ProvisionalSpec{SandboxID: "saved", RecoveryKey: "lease", RecoverySource: nodectl.RecoveryLease, Capacity: capacity, ReservationMemory: 3 << 30, InitialBudget: 3 << 30}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Sync(nodectl.SyncSpec{SandboxID: "saved", Token: "new-token", Capacity: capacity, ReservationMemory: 256 << 20, Settled: true, Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	views := s.PoolSnapshots()
	if views[0].Reserved.MemoryBytes != 0 || views[1].Reserved.MemoryBytes != 256<<20 || views[1].Scheduled.MemoryBytes != 3<<30 {
		t.Fatalf("recovered ledger=%+v", views)
	}
}

func TestResourcePoolLegacySharedTemplateRejectsAmbiguousStrictSlots(t *testing.T) {
	ctx := context.Background()
	cfg := buildNetworkTestConfig()
	limit := 4
	cfg.Units.RunnerPools = []config.RunPoolConfig{{Name: "first", Unit: "shared@.service", Capacity: &limit}, {Name: "second", Unit: "shared@.service"}}
	o := testOrchCfg(t, cfg)
	run := "sr-00000000-0000-7000-8000-000000000515"
	o.lc = &reconcileLauncher{units: []launcher.Unit{{Name: instanceUnit("shared@.service", run), ActiveState: "active"}}}
	if err := o.restoreResourcePlacements(ctx, nil); err == nil {
		t.Fatal("invented logical pool for legacy shared template")
	}
	cfg.Units.RunnerPools[0].Capacity = nil
	o.runnerPool = newRunPools(runKindSandbox, cfg.Units.RunnerPools, time.Second, cfg.Paths.RunRoot, o.lc, &o.runs, o.log)
	if err := o.restoreResourcePlacements(ctx, nil); err != nil {
		t.Fatal(err)
	}
	_, unknown := o.runnerPoolStatus()
	if unknown != 1 {
		t.Fatal("unattributed legacy runner counted as zero")
	}
}

// Production freezes Configure before constructing pools. Legacy test fixtures
// that change cfg.Units after New must also rebuild the runtime pool identity.
func refreshRunPoolsForTest(o *Orchestrator) {
	o.runnerPool = newRunPools(runKindSandbox, o.cfg.Units.RunnerPoolConfigs(), time.Second, o.cfg.Paths.RunRoot, o.lc, &o.runs, o.log)
	o.builderRunPool = newRunPools(runKindBuild, o.cfg.Units.BuilderPoolConfigs(), time.Second, o.cfg.Paths.RunRoot, o.lc, &o.runs, o.log)
}
