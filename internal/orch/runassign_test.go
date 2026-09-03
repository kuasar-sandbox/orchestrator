package orch

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type phaseResourceProvider struct {
	present atomic.Bool
	calls   atomic.Int64
}

func (p *phaseResourceProvider) SandboxResourceStats(string) (api.ResourceStats, bool) {
	p.calls.Add(1)
	return api.ResourceStats{}, p.present.Load()
}

func prepareReportedBuildPhase(t *testing.T, o *Orchestrator) (*types.Build, string) {
	t.Helper()
	b := &types.Build{
		BuildID: "phase-release-build", TemplateID: "transient-phase-release-build",
		APISecret: strings.Repeat("2", 64), ManifestKey: strings.Repeat("1", 64),
		Profile: types.ProfileE2B,
		Status:  types.BuildBuilding, ExecutionClaimed: true,
		RunID: "br-phase-release", Resources: testBuildResources(), CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	o.pend[b.BuildID] = &pendingBuild{build: b, result: make(chan configsock.BuildResult, 1)}
	sandboxID := "bp-b-phase-release"
	if err := o.st.SetBuildPhase(context.Background(), b.BuildID, "b", sandboxID, "starting"); err != nil {
		t.Fatal(err)
	}
	return b, sandboxID
}

func TestPostBuildPhaseFinishedWaitsForControllerRelease(t *testing.T) {
	o := testOrch(t)
	b, sandboxID := prepareReportedBuildPhase(t, o)
	provider := &phaseResourceProvider{}
	provider.present.Store(true)
	o.SetSandboxResourceProvider(provider)

	done := make(chan error, 1)
	go func() {
		done <- o.PostBuildPhase(context.Background(), b.RunID, b.BuildID, "b", sandboxID, "finished")
	}()

	deadline := time.Now().Add(time.Second)
	for provider.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if provider.calls.Load() == 0 {
		t.Fatal("finished report did not inspect the controller reservation")
	}
	select {
	case err := <-done:
		t.Fatalf("finished report crossed live reservation: %v", err)
	default:
	}
	loaded, err := o.st.GetBuild(context.Background(), b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != "b" || loaded.PhaseSandboxID != sandboxID {
		t.Fatalf("phase cleared before release: phase=%q sid=%q", loaded.Phase, loaded.PhaseSandboxID)
	}

	provider.present.Store(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("finished report after release: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("finished report did not resume after controller release")
	}
	loaded, err = o.st.GetBuild(context.Background(), b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != "" || loaded.PhaseSandboxID != "" {
		t.Fatalf("finished phase remains active: phase=%q sid=%q", loaded.Phase, loaded.PhaseSandboxID)
	}
}

func TestPostBuildPhaseReleaseFenceFailsClosedOnCancellation(t *testing.T) {
	o := testOrch(t)
	b, sandboxID := prepareReportedBuildPhase(t, o)
	provider := &phaseResourceProvider{}
	provider.present.Store(true)
	o.SetSandboxResourceProvider(provider)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- o.PostBuildPhase(ctx, b.RunID, b.BuildID, "b", sandboxID, "finished")
	}()
	deadline := time.Now().Add(time.Second)
	for provider.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if provider.calls.Load() == 0 {
		t.Fatal("finished report did not enter the release fence")
	}
	cancel()
	err := <-done
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("finished report error = %v, want context cancellation", err)
	}
	loaded, getErr := o.st.GetBuild(context.Background(), b.BuildID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if loaded.Phase != "b" || loaded.PhaseSandboxID != sandboxID {
		t.Fatalf("cancelled fence cleared phase: phase=%q sid=%q", loaded.Phase, loaded.PhaseSandboxID)
	}
}

func TestPostBuildPhaseReleaseFenceOnlyAppliesToFinished(t *testing.T) {
	o := testOrch(t)
	b, sandboxID := prepareReportedBuildPhase(t, o)
	provider := &phaseResourceProvider{}
	provider.present.Store(true)
	o.SetSandboxResourceProvider(provider)

	if err := o.PostBuildPhase(context.Background(), b.RunID, b.BuildID, "b", sandboxID, "failed"); err != nil {
		t.Fatalf("failed report: %v", err)
	}
	if calls := provider.calls.Load(); calls != 0 {
		t.Fatalf("failed report inspected release fence %d times", calls)
	}
}

func TestPostBuildPhaseFinishedHasNoControllerFenceInStaticMode(t *testing.T) {
	o := testOrch(t)
	b, sandboxID := prepareReportedBuildPhase(t, o)
	if err := o.PostBuildPhase(context.Background(), b.RunID, b.BuildID, "b", sandboxID, "finished"); err != nil {
		t.Fatalf("static-mode finished report: %v", err)
	}
	loaded, err := o.st.GetBuild(context.Background(), b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Phase != "" || loaded.PhaseSandboxID != "" {
		t.Fatalf("static-mode finished phase remains active: phase=%q sid=%q", loaded.Phase, loaded.PhaseSandboxID)
	}
}
