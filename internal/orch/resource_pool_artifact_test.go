package orch

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Exercise the real task preparation/final handoff with simulated VM readiness.
// Both templates route to the existing counting launcher's protocol peer.
type placementTestLauncher struct {
	*countingLauncher
	resultFinalized chan error
}

func (l *placementTestLauncher) Start(_ context.Context, unit string) error {
	l.starts.Add(1)
	go l.runSandbox(l.orch.unitToRunID(unit))
	return nil
}

func (l *placementTestLauncher) Stop(ctx context.Context, unit string) error {
	runID := l.orch.unitToRunID(unit)
	sid, found, err := l.orch.st.GetSandboxIDByCurrentRunID(ctx, runID)
	if err != nil {
		return err
	}
	if found && strings.HasPrefix(unit, "r0@") {
		result := types.SandboxExecutionResult{SID: sid, RunID: runID, Stage: types.SandboxResultPrepare, Error: "prepare runner stopped for placement"}
		if err := l.orch.PostSandboxResult(ctx, runID, sid, result); err != nil {
			return err
		}
		go func() { l.resultFinalized <- l.orch.finalizeSandboxResultOnce(ctx, sid, runID) }()
		// A real unit reports before Stop completes. Give the terminal consumer
		// that same window; it must wait for the lifecycle ownership transition.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return l.countingLauncher.Stop(ctx, unit)
}

func TestResourcePoolArtifactPreparationMovesBeforeFinalLaunch(t *testing.T) {
	cfg := buildNetworkTestConfig()
	root := shortOrchestratorTestDir(t)
	cfg.Paths.RunRoot, cfg.Paths.BaseRoot = filepath.Join(root, "run"), filepath.Join(root, "base")
	cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	memory := "4GiB"
	small := "1GiB"
	large := "8GiB"
	cfg.ResourceListen = &config.ResourceListenConfig{Enabled: true, Socket: filepath.Join(root, "resource.sock"), Pools: []config.ResourcePoolConfig{
		{Name: "numa0", Capacity: config.ResourceAllocatable{Memory: &small}, Allocatable: config.ResourceAllocatable{Memory: &memory}},
		{Name: "numa1", Capacity: config.ResourceAllocatable{Memory: &large}, Allocatable: config.ResourceAllocatable{Memory: &memory}},
	}}
	cfg.Units.RunnerPools = []config.RunPoolConfig{{Name: "r0", Unit: "r0@.service", ResourcePool: "numa0", Priority: 100}, {Name: "r1", Unit: "r1@.service", ResourcePool: "numa1"}}
	cfg.Units.BuilderPools = []config.RunPoolConfig{{Name: "b0", Unit: "b0@.service", ResourcePool: "numa0"}}
	o := testOrchCfg(t, cfg)
	lc := &placementTestLauncher{countingLauncher: &countingLauncher{orch: o}, resultFinalized: make(chan error, 1)}
	o.lc, o.vs = lc, stubVS{}
	o.allowLegacyAssignmentWithoutRunSession = true
	o.SetProxyRouteBarrierCoordinator(immediateRouteBarrierCoordinator{})
	refreshRunPoolsForTest(o)
	resolved, err := nodectl.Resolve(cfg.ResourceListen)
	if err != nil {
		t.Fatal(err)
	}
	o.SetResourceControllerSocketIdentity(resolved.SocketIdentity)
	s := nodectl.NewPoolState(resolved)
	a := nodectl.NewAdmissionController(resolved.Admission)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	o.SetClusterContext(ctx)
	if err := o.ConfigureMemoryPressure(ctx, s, a, resolved.Pressure); err != nil {
		t.Fatal(err)
	}
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}
	sid := "artifact-placement"
	mk := strings.Repeat("a", 64)
	secret, _ := defaultTestCredentials(t, mk)
	sb := &types.Sandbox{ID: sid, Profile: types.ProfileBare, TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(), State: types.StatePaused,
		ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64), SandboxRef: "manifest://" + strings.Repeat("e", 64)},
		APISecret:    secret, ManifestKey: mk, RunDir: nodepath.SandboxRunDir(cfg.Paths.RunRoot, sid), BaseDir: nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sid), CreatedUnix: 1}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	_, attempt, err := o.ensureResumeAccepted(ctx, sid, nil, types.ResumeRequest{Trigger: types.ResumeTriggerWake, Mode: types.ResumeMemory}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := attempt.wait(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-lc.resultFinalized:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("retired prepare result was not finalized")
	}
	current, err := o.st.Get(ctx, sid)
	if err != nil || current.State != types.StateRunning {
		t.Fatalf("resume=%+v %v", current, err)
	}
	if current.ExecutionResult != nil {
		t.Fatal("retired prepare result followed the new runner")
	}
	owner, ok := o.runs.ownership(current.RunID)
	if !ok || owner.ResourcePool != "numa1" || owner.CapacityMemory != 2<<30 {
		t.Fatalf("final placement=%+v", owner)
	}
	if lc.starts.Load() != 2 || lc.stops.Load() != 1 {
		t.Fatalf("prepare/final runs=%d/%d", lc.starts.Load(), lc.stops.Load())
	}
	if views := s.PoolSnapshots(); views[0].Scheduled.MemoryBytes != 0 || views[1].Scheduled.MemoryBytes != 2<<30 {
		t.Fatalf("placement leak=%+v", views)
	}
}
