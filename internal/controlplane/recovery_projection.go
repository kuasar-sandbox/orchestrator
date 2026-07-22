package controlplane

import (
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func recoveryRecordFromFact(
	recovery raftstore.RecoveryEpoch,
	page routesync.RecoveryReportPage,
	fact routesync.RecoveryExecutionFact,
) (raftstore.RecoveryObjectRecord, error) {
	if err := fact.Validate(); err != nil {
		return raftstore.RecoveryObjectRecord{}, err
	}
	event := fact.Object
	source, err := clusterstate.DecodeExecutionBinding(event.Binding)
	if err != nil {
		return raftstore.RecoveryObjectRecord{}, err
	}
	if source.RegistryGeneration != recovery.SourceRegistryGeneration || event.BindingDigest == "" ||
		event.NodeID != page.NodeID || event.NodeEpoch != page.NodeEpoch ||
		source.Kind == clusterstate.ExecutionKindSandbox && event.EventSeq == 0 ||
		source.Kind == clusterstate.ExecutionKindBuild && event.EventSeq != 0 {
		return raftstore.RecoveryObjectRecord{}, errors.New("controlplane: recovery fact is outside the source NodeEpoch")
	}
	intent := clusterstate.DispatchIntent{
		NormalizedDemand: append([]byte(nil), fact.NormalizedDemand...), DemandDigest: fact.DemandDigest,
		DispatchSpec: append([]byte(nil), fact.DispatchSpec...), DispatchSpecDigest: fact.DispatchSpecDigest,
		ProviderPolicyVersion: fact.ProviderPolicyVersion,
	}
	if err := intent.Validate(); err != nil {
		return raftstore.RecoveryObjectRecord{}, err
	}
	target := source
	target.RegistryGeneration = recovery.TargetRegistryGeneration
	targetOpaque, err := clusterstate.EncodeExecutionBinding(target)
	if err != nil {
		return raftstore.RecoveryObjectRecord{}, err
	}
	targetDigest, err := clusterstate.ExecutionBindingDigest(targetOpaque)
	if err != nil {
		return raftstore.RecoveryObjectRecord{}, err
	}
	targetBinding := clusterstate.ExecutionBindingIntent{
		NodeID: source.NodeID, NodeEpoch: source.NodeEpoch, DataEndpoint: event.DataEndpoint,
		RegistryGeneration: recovery.TargetRegistryGeneration,
		OpaqueBinding:      targetOpaque, BindingDigest: targetDigest,
	}
	if err := targetBinding.ValidateWorkflow(source.Kind, source.ObjectID, source.Group, source.RouteKey, intent); err != nil {
		return raftstore.RecoveryObjectRecord{}, err
	}
	record := raftstore.RecoveryObjectRecord{
		RecoveryEpoch: recovery.Epoch, Kind: source.Kind, Group: source.Group,
		RouteKey: source.RouteKey, ObjectID: source.ObjectID,
		NodeID: source.NodeID, NodeEpoch: source.NodeEpoch, SessionSeq: page.SessionSeq,
		EventSeq: event.EventSeq, ReportDigest: page.ReportDigest,
		SourceRegistryGeneration:   recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
		SourceOpaqueBinding:        event.Binding, SourceBindingDigest: event.BindingDigest,
		TargetBinding: targetBinding, State: raftstore.RecoveryObjectStaged,
	}
	switch source.Kind {
	case clusterstate.ExecutionKindSandbox:
		route, err := recoveredRoute(source, targetBinding, intent, fact)
		if err != nil {
			return raftstore.RecoveryObjectRecord{}, err
		}
		record.Route = &route
	case clusterstate.ExecutionKindBuild:
		build, err := recoveredBuild(source, targetBinding, intent, fact)
		if err != nil {
			return raftstore.RecoveryObjectRecord{}, err
		}
		record.Build = &build
	default:
		return raftstore.RecoveryObjectRecord{}, errors.New("controlplane: unsupported recovery execution kind")
	}
	return record, nil
}

func recoveredRoute(
	source clusterstate.ExecutionBinding,
	target clusterstate.ExecutionBindingIntent,
	intent clusterstate.DispatchIntent,
	fact routesync.RecoveryExecutionFact,
) (clusterstate.RouteWorkflowRecord, error) {
	event := fact.Object
	if fact.AdmissionState != string(nodeexec.AdmissionRunning) || !fact.ResourceClaimed {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: recovered Sandbox lacks node-local resource ownership")
	}
	execution := clusterstate.ReadyRoute{
		SandboxID: source.ObjectID, NodeID: source.NodeID, NodeEpoch: source.NodeEpoch,
		DataEndpoint: event.DataEndpoint, TargetPort: event.TargetPort,
		AccessToken: event.AccessToken, TrafficAccessToken: event.TrafficAccessToken,
		TemplateRef: event.TemplateRef, SnapshotRef: event.SnapshotRef,
		RegistryGeneration: target.RegistryGeneration, BindingDigest: target.BindingDigest,
		LastEventSeq: event.EventSeq, Intent: intent, Presentation: event.Presentation.Clone(),
	}
	if err := execution.Validate(); err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	record := clusterstate.RouteWorkflowRecord{Group: source.Group, RouteKey: source.RouteKey}
	switch event.State {
	case string(clusterstate.WorkflowRouteReady):
		record.State, record.Ready = clusterstate.WorkflowRouteReady, &execution
	case string(clusterstate.WorkflowRoutePaused):
		if event.SnapshotRef == "" {
			return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: recovered PAUSED Sandbox has no snapshot reference")
		}
		record.State = clusterstate.WorkflowRoutePaused
		record.Paused = &clusterstate.PausedRouteState{
			Execution: execution, SnapshotRef: event.SnapshotRef, ResumeIntent: intent,
		}
	default:
		return clusterstate.RouteWorkflowRecord{}, fmt.Errorf("controlplane: Sandbox state %q is not recoverable", event.State)
	}
	return record, nil
}

func recoveredBuild(
	source clusterstate.ExecutionBinding,
	target clusterstate.ExecutionBindingIntent,
	intent clusterstate.DispatchIntent,
	fact routesync.RecoveryExecutionFact,
) (clusterstate.BuildRecord, error) {
	event := fact.Object
	spec, err := clusterstate.ParseBuildDispatchSpec(intent.DispatchSpec)
	if err != nil {
		return clusterstate.BuildRecord{}, err
	}
	if event.TemplateRef != "" && event.TemplateRef != spec.TemplateID {
		return clusterstate.BuildRecord{}, errors.New("controlplane: recovered Build changed its immutable template reference")
	}
	if fact.AdmissionState == string(nodeexec.AdmissionRejected) || fact.AdmissionState == "" {
		return clusterstate.BuildRecord{}, errors.New("controlplane: recovered Build has no accepted node-local registration")
	}
	projection := &clusterstate.BuildProjection{
		BuildID: source.ObjectID, NodeID: source.NodeID, NodeEpoch: source.NodeEpoch,
		DataEndpoint: event.DataEndpoint, RegistryGeneration: target.RegistryGeneration,
		BindingDigest: target.BindingDigest, Intent: intent, TemplateRef: spec.TemplateID,
	}
	if err := projection.Validate(); err != nil {
		return clusterstate.BuildRecord{}, err
	}
	return clusterstate.BuildRecord{
		Group: source.Group, BuildID: source.ObjectID, State: clusterstate.BuildRegistered, Projection: projection,
	}, nil
}

func refreshedRecoveryProjection(
	record raftstore.RecoveryObjectRecord,
	ack routesync.CmdAck,
) (*clusterstate.RouteWorkflowRecord, *clusterstate.BuildRecord, error) {
	if ack.RebindObject == nil {
		return nil, nil, errors.New("controlplane: rebind ACK has no durable target object")
	}
	event := *ack.RebindObject
	if err := event.Validate(); err != nil {
		return nil, nil, err
	}
	if event.ObjectID != record.ObjectID || event.NodeID != record.NodeID || event.NodeEpoch != record.NodeEpoch ||
		event.RegistryGeneration != record.TargetBinding.RegistryGeneration ||
		event.Binding != record.TargetBinding.OpaqueBinding ||
		event.BindingDigest != record.TargetBinding.BindingDigest || event.EventSeq < record.EventSeq {
		return nil, nil, errors.New("controlplane: rebind ACK event does not satisfy the staged target Binding")
	}
	var intent clusterstate.DispatchIntent
	if record.Route != nil {
		if record.Route.Ready != nil {
			intent = record.Route.Ready.Intent
		} else {
			intent = record.Route.Paused.Execution.Intent
		}
	} else if record.Build != nil && record.Build.Projection != nil {
		intent = record.Build.Projection.Intent
	} else {
		return nil, nil, errors.New("controlplane: staged recovery record has no immutable intent")
	}
	target, err := clusterstate.DecodeExecutionBinding(record.TargetBinding.OpaqueBinding)
	if err != nil {
		return nil, nil, err
	}
	fact := routesync.RecoveryExecutionFact{
		Object: event, NormalizedDemand: append([]byte(nil), intent.NormalizedDemand...),
		DemandDigest: intent.DemandDigest, DispatchSpec: append([]byte(nil), intent.DispatchSpec...),
		DispatchSpecDigest: intent.DispatchSpecDigest, ProviderPolicyVersion: intent.ProviderPolicyVersion,
		AdmissionState: ack.RebindAdmissionState, ResourceClaimed: ack.RebindResourceClaimed,
	}
	if record.Kind == clusterstate.ExecutionKindSandbox {
		route, err := recoveredRoute(target, record.TargetBinding, intent, fact)
		return &route, nil, err
	}
	build, err := recoveredBuild(target, record.TargetBinding, intent, fact)
	return nil, &build, err
}
