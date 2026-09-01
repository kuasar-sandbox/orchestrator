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
		PreparedSourceKind: string(types.ResumeSourceSnapshot),
		Capacity:           configsock.ArtifactCapacity{CPU: 2, Memory: "2GiB"},
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
	if got, err := validateBuildPrepareSummary(summary); err != nil || !reflect.DeepEqual(got, wantNetwork) {
		t.Fatalf("valid summary network = %+v, %v", got, err)
	}

	badNetwork := summary
	badNetwork.Network.InnerIP = "not-a-cidr"
	if _, err := validateBuildPrepareSummary(badNetwork); err == nil {
		t.Fatal("malformed source-template network was accepted")
	}
	badMemory := summary
	badMemory.Capacity.Memory = "not-a-size"
	if _, err := validateBuildPrepareSummary(badMemory); err == nil {
		t.Fatal("invalid snapshot capacity was accepted")
	}
	uppercaseDigest := summary
	uppercaseDigest.ResolutionDigest = strings.Repeat("A", 64)
	if _, err := validateBuildPrepareSummary(uppercaseDigest); err == nil {
		t.Fatal("non-canonical snapshot resolution digest was accepted")
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
}

func TestPublishBuildFinalInstallsMMDSRouteBeforeReleasingTask(t *testing.T) {
	o := testOrch(t)
	o.cfg.MMDS.Enabled = true
	b := &types.Build{
		BuildID: "build-final-route-order", TemplateID: "transient-build-final-route-order",
		Profile: types.ProfileE2B, RunID: "br-final-route-order", CreatedUnix: time.Now().Unix(),
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
		if o.lookup("build-"+b.BuildID) == nil {
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
