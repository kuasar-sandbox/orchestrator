package controlplane

import (
	"context"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func (s *RegistryService) completeRouteFinalization(
	ctx context.Context,
	record clusterstate.RouteWorkflowRecord,
) (clusterstate.RouteWorkflowRecord, bool, error) {
	if len(record.Finalizations) == 0 {
		return record, false, nil
	}
	intent := record.Finalizations[0]
	if intent.TerminalProof != nil {
		return record, false, errors.New("controlplane: Route rejection finalization carries terminal proof")
	}
	if err := s.sendWorkflowFinalization(ctx, clusterstate.ExecutionKindSandbox, intent); err != nil {
		return record, false, err
	}
	next := record
	next.Finalizations = removeWorkflowFinalization(record.Finalizations, 0)
	committed, err := s.store.CommitRouteWorkflow(ctx, record.Revision, next)
	return committed, err == nil, err
}

func (s *RegistryService) completeBuildFinalization(
	ctx context.Context,
	record clusterstate.BuildRecord,
) (clusterstate.BuildRecord, bool, error) {
	if len(record.Finalizations) == 0 {
		return record, false, nil
	}
	intent := record.Finalizations[0]
	if intent.TerminalProof != nil {
		return record, false, errors.New("controlplane: Build rejection finalization carries terminal proof")
	}
	if err := s.sendWorkflowFinalization(ctx, clusterstate.ExecutionKindBuild, intent); err != nil {
		return record, false, err
	}
	next := record
	next.Finalizations = removeWorkflowFinalization(record.Finalizations, 0)
	committed, err := s.store.CommitBuildWorkflow(ctx, record.Revision, next)
	return committed, err == nil, err
}

func (s *RegistryService) sendWorkflowFinalization(
	ctx context.Context,
	kind clusterstate.ExecutionKind,
	intent clusterstate.WorkflowFinalizationIntent,
) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	identity, err := s.store.ServeIdentity()
	if err != nil {
		return err
	}
	command := &routesync.Command{
		CmdID: newCommandID(), Kind: routesync.CmdFinalizeWorkflow,
		RegistryGeneration: intent.RegistryGeneration, BindingDigest: intent.BindingDigest,
	}
	switch kind {
	case clusterstate.ExecutionKindSandbox:
		command.SID = intent.ObjectID
	case clusterstate.ExecutionKindBuild:
		command.BuildID = intent.ObjectID
	default:
		return errors.New("controlplane: unsupported workflow finalization kind")
	}
	ack, _, err := s.commands.SendNodeCommand(
		ctx, identity, intent.NodeID, intent.NodeEpoch, intent.DataEndpoint, command,
	)
	if err != nil {
		return err
	}
	if ack.Status != routesync.AckAccepted {
		return fmt.Errorf("controlplane: node rejected workflow finalization: %s", ack.Reason)
	}
	return nil
}

func removeWorkflowFinalization(
	intents []clusterstate.WorkflowFinalizationIntent,
	index int,
) []clusterstate.WorkflowFinalizationIntent {
	result := make([]clusterstate.WorkflowFinalizationIntent, 0, len(intents)-1)
	result = append(result, intents[:index]...)
	result = append(result, intents[index+1:]...)
	return result
}
