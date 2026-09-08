package orch

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

func validBuildPrepareSummary() configsock.ArtifactPrepareSummary {
	return configsock.ArtifactPrepareSummary{
		SchemaVersion:      configsock.ArtifactPrepareSchemaVersion,
		PreparedSourceKind: string(types.ResumeSourceSandbox),
		Capacity: configsock.ArtifactCapacity{
			CPU: 2, Memory: "2GiB", AllocatableCPU: 1.5, AllocatableMemory: "1GiB",
		},
		Network: configsock.ArtifactNetwork{
			Hostname: "source", InnerIP: "10.0.0.5/24", Nexthop: "10.0.0.1",
		},
		DiskTopology:     validArtifactDiskTopology(),
		ResolutionDigest: strings.Repeat("a", 64),
		RequiredRefCount: 3,
	}
}

func TestBuildTaskHandoffIdenticalReplayReturnsOneFinalResult(t *testing.T) {
	h := newBuildTaskHandoff(true, "")
	summary := validBuildPrepareSummary()
	if replay, err := h.Submit(summary); err != nil || replay {
		t.Fatalf("first submit = replay %t, err %v", replay, err)
	}
	if got, err := h.WaitPrepare(context.Background()); err != nil || !configsock.EqualArtifactPrepareSummary(got, summary) {
		t.Fatalf("prepare = %+v, %v", got, err)
	}

	// Model a response waiter disappearing after the immutable completion was
	// accepted. Cancellation must not retract the summary.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := h.WaitFinal(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled final wait = %v", err)
	}
	final := &configsock.BuildSpec{BuildID: "build", RunID: "run"}
	h.PublishFinal(final, nil)
	if replay, err := h.Submit(summary); err != nil || !replay {
		t.Fatalf("identical replay = replay %t, err %v", replay, err)
	}
	if got, err := h.WaitFinal(context.Background()); err != nil || got != final {
		t.Fatalf("replayed final = %+v, %v", got, err)
	}
}

func TestBuildTaskHandoffConflictingReplayFailsClosed(t *testing.T) {
	h := newBuildTaskHandoff(true, "")
	first := validBuildPrepareSummary()
	if _, err := h.Submit(first); err != nil {
		t.Fatal(err)
	}
	conflict := first
	conflict.ResolutionDigest = strings.Repeat("b", 64)
	if _, err := h.Submit(conflict); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("conflicting submit = %v", err)
	}
	if _, err := h.WaitFinal(context.Background()); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("final after conflict = %v", err)
	}
	h.PublishFinal(&configsock.BuildSpec{BuildID: "must-not-escape"}, nil)
	if got, err := h.WaitFinal(context.Background()); got != nil || err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("published final after conflict = %+v, %v", got, err)
	}
}

func TestRecoveredBuildTaskHandoffAcceptsOnlyDurableDigest(t *testing.T) {
	want := validBuildPrepareSummary()
	h := newBuildTaskHandoff(true, want.ResolutionDigest)
	if replay, err := h.Submit(want); err != nil || !replay {
		t.Fatalf("durable replay = replay %t, err %v", replay, err)
	}
	changed := want
	changed.ResolutionDigest = strings.Repeat("c", 64)
	if _, err := h.Submit(changed); err == nil || !strings.Contains(err.Error(), "durable") {
		t.Fatalf("durable conflict = %v", err)
	}
}

func TestValidateBuildPrepareSummaryIsStrict(t *testing.T) {
	summary := validBuildPrepareSummary()
	wantNetwork := sandboxcfg.NetworkSpec{Hostname: "source", InnerIP: "10.0.0.5/24", Nexthop: "10.0.0.1"}
	capacity, got, err := validateBuildPrepareSummary(summary)
	if err != nil || !reflect.DeepEqual(got, wantNetwork) || capacity != summary.Capacity {
		t.Fatalf("valid summary = capacity %+v network %+v, %v", capacity, got, err)
	}

	badNetwork := summary
	badNetwork.Network.InnerIP = "not-a-cidr"
	if _, _, err := validateBuildPrepareSummary(badNetwork); err == nil {
		t.Fatal("malformed source-template network was accepted")
	}
	badMemory := summary
	badMemory.Capacity.Memory = "not-a-size"
	if _, _, err := validateBuildPrepareSummary(badMemory); err == nil {
		t.Fatal("invalid snapshot capacity was accepted")
	}
	badAllocatableCPU := summary
	badAllocatableCPU.Capacity.AllocatableCPU = 3
	if _, _, err := validateBuildPrepareSummary(badAllocatableCPU); err == nil {
		t.Fatal("allocatable CPU above capacity was accepted")
	}
	badAllocatableMemory := summary
	badAllocatableMemory.Capacity.AllocatableMemory = "3GiB"
	if _, _, err := validateBuildPrepareSummary(badAllocatableMemory); err == nil {
		t.Fatal("allocatable memory above capacity was accepted")
	}
	uppercaseDigest := summary
	uppercaseDigest.ResolutionDigest = strings.Repeat("A", 64)
	if _, _, err := validateBuildPrepareSummary(uppercaseDigest); err == nil {
		t.Fatal("non-canonical snapshot resolution digest was accepted")
	}
	dataDisk := summary
	dataDisk.DiskTopology.Disks = []types.ArtifactDiskShape{{Name: "data", Mode: types.ArtifactDiskSingle, HasActiveBase: true}}
	if _, _, err := validateBuildPrepareSummary(dataDisk); err == nil || !strings.Contains(err.Error(), "data disks") {
		t.Fatalf("source data disk validation = %v", err)
	}
}

func TestBuildRuntimePreparationRoundTripFreezesResolvedInputs(t *testing.T) {
	want := buildRuntimePreparation{
		SchemaVersion: buildRuntimePrepareSchemaVersion,
		PrepareDigest: strings.Repeat("d", 64),
		Network:       sandboxcfg.NetworkSpec{Hostname: "build", InnerIP: "10.0.0.5/24", Nexthop: "10.0.0.1"},
		TemplateNetwork: sandboxcfg.NetworkSpec{
			Hostname: "sandbox", InnerIP: "10.0.0.5/24", Nexthop: "10.0.0.1",
		},
		Resources: rtconfig.ResourcesConfig{
			Capacity: rtconfig.CapacityConfig{CPU: 2, Memory: "2GiB"},
		},
		SandboxResources: rtconfig.ResourcesConfig{
			Capacity: rtconfig.CapacityConfig{CPU: 1, Memory: "1GiB"},
		},
		CheckpointPolicy: sandboxcfg.SnapshotPolicy{
			MergeRef: orchCheckpointBool(false), DropCaches: orchCheckpointBool(true),
		},
	}
	raw, err := encodeBuildRuntimePreparation(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBuildRuntimePreparation(raw)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, %v; want %+v", got, err, want)
	}
	if _, err := decodeBuildRuntimePreparation(""); err == nil {
		t.Fatal("missing durable preparation was accepted")
	}
	if _, err := decodeBuildRuntimePreparation(`{"schema_version":1}`); err == nil {
		t.Fatal("incomplete durable preparation was accepted")
	}

	// Image outputs have no target Sandbox resources. Their durable record
	// still freezes the independent A/B execution resources.
	image := want
	image.SandboxResources = rtconfig.ResourcesConfig{}
	if _, err := encodeBuildRuntimePreparation(image); err != nil {
		t.Fatalf("image runtime preparation = %v", err)
	}
	partial := image
	partial.SandboxResources.Capacity.Memory = "1GiB"
	if _, err := encodeBuildRuntimePreparation(partial); err == nil {
		t.Fatal("partial sandbox resources were accepted")
	}
}

func TestPublishBuildFinalInstallsMMDSRouteBeforeReleasingTask(t *testing.T) {
	o := testOrch(t)
	o.cfg.MMDS.Enabled = true
	b := &types.Build{
		BuildID: "build-final-route-order", TemplateID: "transient-build-final-route-order",
		Profile: types.ProfileE2B, RunID: "br-final-route-order", CreatedUnix: time.Now().Unix(),
		Builder: types.BuildOptions{Target: &types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}},
	}
	handoff := newBuildTaskHandoff(false, "")
	pend := &pendingBuild{build: b, handoff: handoff}
	final := &configsock.BuildSpec{BuildID: b.BuildID, RunID: b.RunID}

	waiting := make(chan struct{})
	observed := make(chan error, 1)
	go func() {
		close(waiting)
		got, err := handoff.WaitFinal(context.Background())
		if err != nil {
			observed <- err
			return
		}
		if got != final {
			observed <- errors.New("task received an unexpected final spec")
			return
		}
		if o.lookup(buildMMDSID(b.BuildID)) == nil {
			observed <- errors.New("task observed final spec before MMDS route")
			return
		}
		observed <- nil
	}()
	<-waiting
	mmdsRow := o.publishBuildFinal(pend, final)
	if mmdsRow == nil {
		t.Fatal("MMDS route was not installed")
	}
	if err := <-observed; err != nil {
		t.Fatal(err)
	}
	o.uncache(mmdsRow.ID)
	o.publishDelete(mmdsRow.ID)
	o.setMMDSBuildOwner(mmdsRow.ID, "")
}

func TestPublishBuildFinalSkipsPhaseCMMDSRouteForNonMemoryTargets(t *testing.T) {
	for _, target := range []*types.BuildTarget{
		{Kind: types.BuildTargetImage},
		{Kind: types.BuildTargetSandbox},
	} {
		t.Run(string(target.Kind), func(t *testing.T) {
			o := testOrch(t)
			o.cfg.MMDS.Enabled = true
			b := &types.Build{
				BuildID: "build-final-no-route-" + string(target.Kind),
				Profile: types.ProfileE2B,
				Builder: types.BuildOptions{Target: target},
			}
			pend := &pendingBuild{build: b, handoff: newBuildTaskHandoff(false, "")}
			if row := o.publishBuildFinal(pend, &configsock.BuildSpec{BuildID: b.BuildID}); row != nil {
				t.Fatalf("non-memory target published Phase C MMDS row: %+v", row)
			}
			if o.lookup(buildMMDSID(b.BuildID)) != nil {
				t.Fatal("non-memory target retained a synthetic MMDS route")
			}
		})
	}
}

func TestArtifactPrepareFailureResultIsVisibleWithoutBuildJournal(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("f", 64)
	b := &types.Build{
		BuildID: "build-task-prepare-failure", TemplateID: "transient-build-task-prepare-failure",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		Profile: types.ProfileE2B, Status: types.BuildBuilding, ExecutionClaimed: true,
		ExecutionClaimedUnix: time.Now().Unix(), CreatedUnix: time.Now().Unix(),
	}
	if err := o.st.PutBuild(ctx, b); err != nil {
		t.Fatal(err)
	}
	want := "prepare build snapshot: snapshot.cfg is malformed"
	var publishedState, publishedReason string
	o.completeBuildWithPublisher(ctx, b, &buildResult{
		Error: want, FailureStage: "artifact_prepare",
	}, nil, func(_ string, state string, _ string, reason string) {
		publishedState, publishedReason = state, reason
	})
	stored, err := o.st.GetBuild(ctx, b.BuildID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != types.BuildError || stored.Reason != want || stored.ExecutionClaimed {
		t.Fatalf("stored prepare failure = %+v", stored)
	}
	if publishedState != "error" || publishedReason != want {
		t.Fatalf("published prepare failure = state %q reason %q", publishedState, publishedReason)
	}
}
