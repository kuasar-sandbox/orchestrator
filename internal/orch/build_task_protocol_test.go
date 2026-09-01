package orch

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestBuildTaskBootstrapUsesExactRunAndSnapshotTwoStage(t *testing.T) {
	cfg := &config.Config{ManifestConfig: filepath.Join(t.TempDir(), "manifest.yaml")}
	cfg.Paths.RunRoot = filepath.Join(t.TempDir(), "run")
	cfg.Builder.TotalTimeoutSec = 90
	o := testOrchCfg(t, cfg)
	manifestKey := strings.Repeat("a", 64)
	build := &types.Build{
		BuildID: "build-snapshot", TemplateID: "transient-build-snapshot",
		FromTemplate: types.TemplateID{
			Profile: types.ProfileE2B, Kind: types.KindSnp,
			Ref: "manifest://" + strings.Repeat("b", 64),
		}.String(),
		Profile: types.ProfileE2B, Status: types.BuildBuilding,
		ExecutionClaimed: true, ExecutionClaimedUnix: time.Now().Unix(), RunID: "br-current",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
	}
	if err := o.st.PutBuild(context.Background(), build); err != nil {
		t.Fatal(err)
	}
	workdir := filepath.Join(cfg.Paths.RunRoot, "build-work")
	o.pend[build.BuildID] = &pendingBuild{
		build: build, workdir: workdir, snapshotTemplate: true,
		handoff: newBuildTaskHandoff(true, ""), result: make(chan configsock.BuildResult, 1),
	}

	if _, found, err := o.BuildTaskAuth(context.Background(), build.BuildID, "br-stale"); err != nil || found {
		t.Fatalf("stale build auth = %t, %v", found, err)
	}
	auth, found, err := o.BuildTaskAuth(context.Background(), build.BuildID, build.RunID)
	if err != nil || !found || auth.PidFile != configsock.BuildPidfile(cfg.Paths.RunRoot, build.BuildID) {
		t.Fatalf("exact build auth = %+v, %t, %v", auth, found, err)
	}
	if _, found, err := o.BuildTaskSpecFor(context.Background(), build.BuildID, "br-stale"); err != nil || found {
		t.Fatalf("stale build bootstrap = %t, %v", found, err)
	}
	task, found, err := o.BuildTaskSpecFor(context.Background(), build.BuildID, build.RunID)
	if err != nil || !found || task.Prepare == nil || task.Final != nil {
		t.Fatalf("snapshot build bootstrap = %+v, %t, %v", task, found, err)
	}
	wantDeadline := time.Unix(build.ExecutionClaimedUnix, 0).Add(90 * time.Second).UnixNano()
	if task.Env["MANIFEST_KEY"] != manifestKey || task.Prepare.RootRef == "" ||
		task.Prepare.AbsoluteDeadlineUnixNano != wantDeadline {
		t.Fatalf("snapshot build bootstrap content = %+v", task)
	}
}

func TestBuildTaskFastPathReturnsFinalInBootstrap(t *testing.T) {
	o := testOrch(t)
	build := &types.Build{BuildID: "build-fast", RunID: "br-fast", ManifestKey: strings.Repeat("c", 64)}
	handoff := newBuildTaskHandoff(false, fastBuildPrepareDigest(build.BuildID))
	want := &configsock.BuildSpec{BuildID: build.BuildID, RunID: build.RunID}
	handoff.PublishFinal(want, nil)
	o.pend[build.BuildID] = &pendingBuild{build: build, handoff: handoff}

	task, found, err := o.BuildTaskSpecFor(context.Background(), build.BuildID, build.RunID)
	if err != nil || !found || task.Final != want || task.Prepare != nil {
		t.Fatalf("fast build bootstrap = %+v, %t, %v", task, found, err)
	}
}

func TestCompleteBuildPrepareExactRunReplayAndConflict(t *testing.T) {
	o := testOrch(t)
	build := &types.Build{BuildID: "build-complete", RunID: "br-complete"}
	handoff := newBuildTaskHandoff(true, "")
	o.pend[build.BuildID] = &pendingBuild{build: build, snapshotTemplate: true, handoff: handoff}
	want := &configsock.BuildSpec{BuildID: build.BuildID, RunID: build.RunID}
	summary := validBuildPrepareSummary()

	if _, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, "br-stale", summary); err == nil || !configsock.IsBuildPrepareRejection(err) {
		t.Fatalf("stale completion = %v", err)
	}
	result := make(chan struct {
		spec *configsock.BuildSpec
		err  error
	}, 1)
	go func() {
		spec, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, build.RunID, summary)
		result <- struct {
			spec *configsock.BuildSpec
			err  error
		}{spec: spec, err: err}
	}()
	if got, err := handoff.WaitPrepare(context.Background()); err != nil || !configsock.EqualArtifactPrepareSummary(got, summary) {
		t.Fatalf("host prepare = %+v, %v", got, err)
	}
	handoff.PublishFinal(want, nil)
	if got := <-result; got.err != nil || got.spec != want {
		t.Fatalf("first completion = %+v, %v", got.spec, got.err)
	}
	if got, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, build.RunID, summary); err != nil || got != want {
		t.Fatalf("completion replay = %+v, %v", got, err)
	}
	conflict := summary
	conflict.ResolutionDigest = strings.Repeat("d", 64)
	if _, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, build.RunID, conflict); err == nil || !configsock.IsBuildPrepareRejection(err) {
		t.Fatalf("conflicting completion = %v", err)
	}
}

func TestBuildTaskEndpointsReturnRetryableErrorDuringRecovery(t *testing.T) {
	build := &types.Build{BuildID: "build-recovering", RunID: "br-recovering"}
	o := &Orchestrator{
		buildRecoveryReady: make(chan struct{}),
		pend: map[string]*pendingBuild{
			build.BuildID: {build: build, handoff: newBuildTaskHandoff(true, "")},
		},
	}
	if _, _, err := o.BuildTaskSpecFor(context.Background(), build.BuildID, build.RunID); !errors.Is(err, errBuildRecoveryInProgress) {
		t.Fatalf("bootstrap during recovery = %v", err)
	}
	if _, err := o.CompleteBuildPrepare(context.Background(), build.BuildID, build.RunID, validBuildPrepareSummary()); !errors.Is(err, errBuildRecoveryInProgress) {
		t.Fatalf("completion during recovery = %v", err)
	}
}
