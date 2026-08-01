package orch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type buildMMDSRouteObservation struct {
	incarnation string
	ok          bool
	err         error
}

type buildMMDSRouteLauncher struct {
	*runPoolTestLauncher
	orch     *Orchestrator
	ctx      context.Context
	observed chan<- buildMMDSRouteObservation
}

func (l *buildMMDSRouteLauncher) Start(_ context.Context, unit string) error {
	prefix := strings.TrimSuffix(l.orch.cfg.Units.Builder, ".service")
	runID := strings.TrimSuffix(strings.TrimPrefix(unit, prefix), ".service")
	go func() {
		buildID, ok, err := l.orch.WaitAssignment(l.ctx, runKindBuild, runID)
		if err != nil || !ok {
			l.observed <- buildMMDSRouteObservation{err: err}
			return
		}
		incarnation, incarnationOK := l.orch.Incarnation("build-" + buildID)
		l.observed <- buildMMDSRouteObservation{incarnation: incarnation, ok: incarnationOK}
		_ = l.orch.PostBuildResult(l.ctx, runID, buildID, configsock.BuildResult{})
	}()
	return nil
}

func TestBuildMMDSRoutePublishedWithAssignedIncarnation(t *testing.T) {
	cfg := buildNetworkTestConfig()
	cfg.Paths.RunRoot = t.TempDir()
	cfg.Units.Builder = "sandbox-builder@.service"
	cfg.MMDS.Enabled = true

	o := testOrchCfg(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	observed := make(chan buildMMDSRouteObservation, 1)
	lc := &buildMMDSRouteLauncher{
		runPoolTestLauncher: newRunPoolTestLauncher(),
		orch:                o,
		ctx:                 ctx,
		observed:            observed,
	}
	o.lc = lc
	o.runnerPool.lc = lc
	o.builderRunPool.lc = lc
	o.vs = &capturingNetworkVS{}
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}

	events, unsubscribe := o.Subscribe()
	t.Cleanup(unsubscribe)
	manifestKey := strings.Repeat("a", 64)
	b := &types.Build{
		BuildID: "mmds-incarnation", TemplateID: "transient-mmds-incarnation",
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: types.BuildBuilding,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := o.runBuildUnit(ctx, b); err != nil {
		t.Fatal(err)
	}

	got := <-observed
	if got.err != nil || !got.ok || got.incarnation == "" || got.incarnation != b.RunID {
		t.Fatalf("build MMDS incarnation = %q ok=%t err=%v, assigned run ID=%q", got.incarnation, got.ok, got.err, b.RunID)
	}
	select {
	case ev := <-events:
		if ev.Kind != routesync.TypeUpsert || ev.Route.SandboxID != "build-"+b.BuildID || ev.Route.RunID != b.RunID {
			t.Fatalf("first build route event = %+v, want upsert with run ID %q", ev, b.RunID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for build MMDS route upsert")
	}
}
