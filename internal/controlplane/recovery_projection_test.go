package controlplane

import (
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestRecoveredBuildDerivesImmutableTemplateFromDispatchIntent(t *testing.T) {
	_, _, fixture := newEventFixture(t, clusterstate.ExecutionKindBuild, "build-1", "")
	object := recoveryObject(fixture.event(1, string(clusterstate.BuildRegistered)))
	object.EventSeq = 0
	object.TemplateRef = ""
	fact := recoveryBuildFact(fixture, object)
	recovery, page := recoveryFixture(fixture)

	record, err := recoveryRecordFromFact(recovery, page, fact)
	if err != nil {
		t.Fatal(err)
	}
	if record.Build == nil || record.Build.Projection == nil ||
		record.Build.Projection.TemplateRef != fixture.templateRef {
		t.Fatalf("recovered Build = %+v", record.Build)
	}

	fact.Object.TemplateRef = "another-template"
	if _, err := recoveryRecordFromFact(recovery, page, fact); err == nil {
		t.Fatal("recovery accepted an event that changed the immutable template reference")
	}
}

func recoveryBuildFact(fixture eventFixture, object routesync.RecoveryObjectSnapshot) routesync.RecoveryExecutionFact {
	return routesync.RecoveryExecutionFact{
		Object: object, NormalizedDemand: append([]byte(nil), fixture.intent.NormalizedDemand...),
		DemandDigest:          fixture.intent.DemandDigest,
		DispatchSpec:          append([]byte(nil), fixture.intent.DispatchSpec...),
		DispatchSpecDigest:    fixture.intent.DispatchSpecDigest,
		ProviderPolicyVersion: fixture.intent.ProviderPolicyVersion,
		AdmissionState:        string(nodeexec.AdmissionAdmitted), ResourceClaimed: true,
	}
}

func recoveryObject(event routesync.ExecutionEvent) routesync.RecoveryObjectSnapshot {
	return routesync.RecoveryObjectSnapshot{
		ObjectKind: event.ObjectKind, ObjectID: event.ObjectID, NodeID: event.NodeID,
		NodeEpoch: event.NodeEpoch, RegistryGeneration: event.RegistryGeneration,
		Binding: event.Binding, BindingDigest: event.BindingDigest, EventSeq: event.EventSeq,
		State: event.State, DataEndpoint: event.DataEndpoint, TargetPort: event.TargetPort,
		AccessToken: event.AccessToken, TrafficAccessToken: event.TrafficAccessToken,
		TemplateRef: event.TemplateRef, SnapshotRef: event.SnapshotRef,
		SnapshotLocation: event.SnapshotLocation, ArtifactRef: event.ArtifactRef, Reason: event.Reason,
	}
}

func recoveryFixture(fixture eventFixture) (raftstore.RecoveryEpoch, routesync.RecoveryReportPage) {
	recovery := raftstore.RecoveryEpoch{
		Epoch: 2, SourceClusterID: "cluster-1",
		SourceRegistryGeneration:   fixture.binding.RegistryGeneration,
		SourceRegistryLayoutDigest: strings.Repeat("a", 64),
		TargetRegistryGeneration:   "generation-2", TargetRegistryLayoutDigest: strings.Repeat("b", 64),
		Phase: raftstore.RecoveryCollecting,
	}
	page := routesync.RecoveryReportPage{
		RecoveryEpoch: recovery.Epoch, SourceClusterID: recovery.SourceClusterID,
		SourceRegistryGeneration:   recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
		TargetRegistryGeneration:   recovery.TargetRegistryGeneration,
		TargetRegistryLayoutDigest: recovery.TargetRegistryLayoutDigest,
		NodeID:                     fixture.binding.NodeID, NodeEpoch: fixture.binding.NodeEpoch, SessionSeq: 11,
		ReportDigest: strings.Repeat("c", 64), TotalObjects: 1, Complete: true,
	}
	return recovery, page
}
