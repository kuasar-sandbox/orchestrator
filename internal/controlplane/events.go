package controlplane

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type EventConverger struct {
	store *RaftStore
}

func NewEventConverger(store *RaftStore) (*EventConverger, error) {
	if store == nil {
		return nil, errors.New("controlplane: event converger requires the consensus store")
	}
	return &EventConverger{store: store}, nil
}

func (c *EventConverger) ConvergeExecutionEvent(ctx context.Context, event routesync.ExecutionEvent) error {
	if err := event.Validate(); err != nil {
		return err
	}
	binding, err := clusterstate.DecodeExecutionBinding(event.Binding)
	if err != nil {
		return err
	}
	if binding.ObjectID != event.ObjectID || binding.NodeID != event.NodeID ||
		binding.NodeEpoch != event.NodeEpoch || binding.RegistryGeneration != event.RegistryGeneration {
		return errors.New("controlplane: execution event does not match its opaque Binding")
	}
	switch event.ObjectKind {
	case "sandbox":
		if binding.Kind != clusterstate.ExecutionKindSandbox || binding.RouteKey == "" {
			return errors.New("controlplane: Sandbox event carries a non-Sandbox Binding")
		}
		return c.convergeSandbox(ctx, binding, event)
	case "build":
		return errors.New("controlplane: post-registration Build lifecycle is node-local")
	default:
		return errors.New("controlplane: unknown execution event kind")
	}
}

func (c *EventConverger) convergeSandbox(
	ctx context.Context,
	binding clusterstate.ExecutionBinding,
	event routesync.ExecutionEvent,
) error {
	for attempt := 0; attempt < 8; attempt++ {
		record, err := c.store.ReadRouteWorkflow(ctx, binding.Group, binding.RouteKey)
		if err != nil {
			return err
		}
		if record == nil {
			return errors.New("controlplane: execution event names an unknown Route")
		}
		projection, committedSeq, err := sandboxExecution(*record, event)
		if err != nil {
			return err
		}
		if event.EventSeq <= committedSeq {
			if record.State == clusterstate.WorkflowRouteTombstone && record.Tombstone != nil &&
				record.Tombstone.PlacementFailure == nil {
				if err := c.store.EnsureExecutionFence(ctx, fenceFromTombstone(*record)); err != nil {
					return err
				}
			}
			return nil
		}

		switch event.State {
		case string(clusterstate.WorkflowRouteReady):
			if record.State == clusterstate.WorkflowRouteDeleting {
				if err := c.commitDeletingEventProgress(ctx, *record, event); err != nil {
					if errors.Is(err, ErrRevisionConflict) {
						continue
					}
					return err
				}
				return nil
			}
			next, err := readyFromEvent(*record, projection, event)
			if err != nil {
				return err
			}
			if _, err := c.store.CommitRouteWorkflow(ctx, record.Revision, next); err != nil {
				if errors.Is(err, ErrRevisionConflict) {
					continue
				}
				return err
			}
			return nil
		case string(clusterstate.WorkflowRoutePaused):
			if event.SnapshotRef == "" {
				return errors.New("controlplane: PAUSED event requires an authoritative snapshot reference")
			}
			if record.State == clusterstate.WorkflowRouteDeleting {
				if err := c.commitDeletingEventProgress(ctx, *record, event); err != nil {
					if errors.Is(err, ErrRevisionConflict) {
						continue
					}
					return err
				}
				return nil
			}
			if record.State == clusterstate.WorkflowRouteResuming {
				return errors.New("controlplane: PAUSED event cannot regress an active RESUMING workflow")
			}
			if record.State == clusterstate.WorkflowRouteStarting {
				if event.EventSeq < 2 {
					return errors.New("controlplane: PAUSED event lacks a preceding READY sequence")
				}
				readyEvent := event
				readyEvent.State = string(clusterstate.WorkflowRouteReady)
				readyEvent.EventSeq--
				readyEvent.SnapshotRef = ""
				readyEvent.SnapshotLocation = ""
				next, err := readyFromEvent(*record, projection, readyEvent)
				if err != nil {
					return err
				}
				if _, err := c.store.CommitRouteWorkflow(ctx, record.Revision, next); err != nil && !errors.Is(err, ErrRevisionConflict) {
					return err
				}
				continue
			}
			if projection == nil {
				return errors.New("controlplane: PAUSED event has no committed execution")
			}
			execution := *projection
			execution.LastEventSeq = event.EventSeq
			execution.Presentation = event.Presentation.Clone()
			next := clusterstate.RouteWorkflowRecord{
				Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRoutePaused,
				Paused: &clusterstate.PausedRouteState{
					Execution: execution, SnapshotRef: event.SnapshotRef, ResumeIntent: execution.Intent,
				},
			}
			if _, err := c.store.CommitRouteWorkflow(ctx, record.Revision, next); err != nil {
				if errors.Is(err, ErrRevisionConflict) {
					continue
				}
				return err
			}
			return nil
		case "ERROR", "DELETED":
			if record.State != clusterstate.WorkflowRouteStarting && record.State != clusterstate.WorkflowRouteDeleting {
				if projection == nil {
					return errors.New("controlplane: terminal event has no committed execution")
				}
				next := deletingFromTerminal(*record, *projection, event)
				if _, err := c.store.CommitRouteWorkflow(ctx, record.Revision, next); err != nil && !errors.Is(err, ErrRevisionConflict) {
					return err
				}
				continue
			}
			next, fence, err := tombstoneFromEvent(*record, event)
			if err != nil {
				return err
			}
			committed, err := c.store.CommitNodeTerminalRoute(ctx, record.Revision, next, event)
			if err != nil {
				if errors.Is(err, ErrRevisionConflict) {
					continue
				}
				return err
			}
			fence.Revision = clusterstate.Revision{}
			if committed.Tombstone == nil {
				return errors.New("controlplane: terminal Route commit returned no tombstone")
			}
			if err := c.store.EnsureExecutionFence(ctx, fence); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("controlplane: unsupported Sandbox event state %q", event.State)
		}
	}
	return ErrRevisionConflict
}

func (c *EventConverger) commitDeletingEventProgress(
	ctx context.Context,
	record clusterstate.RouteWorkflowRecord,
	event routesync.ExecutionEvent,
) error {
	if record.Deleting == nil {
		return errors.New("controlplane: DELETING Route has no execution state")
	}
	if event.DataEndpoint != "" && event.DataEndpoint != record.Deleting.Execution.DataEndpoint {
		return errors.New("controlplane: delayed event changed data endpoint within NodeEpoch")
	}
	projectionEvent := event
	if event.State == string(clusterstate.WorkflowRoutePaused) {
		projectionEvent.SnapshotRef = ""
		projectionEvent.SnapshotLocation = ""
	}
	if err := validateReadyEventProjection(record.Deleting.Execution, projectionEvent); err != nil {
		return err
	}
	deleting := *record.Deleting
	deleting.LastEventSeq = event.EventSeq
	next := clusterstate.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteDeleting,
		Deleting:      &deleting,
		Finalizations: append([]clusterstate.WorkflowFinalizationIntent(nil), record.Finalizations...),
	}
	_, err := c.store.CommitRouteWorkflow(ctx, record.Revision, next)
	return err
}

func sandboxExecution(
	record clusterstate.RouteWorkflowRecord,
	event routesync.ExecutionEvent,
) (*clusterstate.ReadyRoute, uint64, error) {
	match := func(nodeID string, nodeEpoch uint64, registryGeneration, digest string) error {
		if nodeID != event.NodeID || nodeEpoch != event.NodeEpoch || registryGeneration != event.RegistryGeneration || digest != event.BindingDigest {
			return errors.New("controlplane: execution event is fenced by the committed Route Binding")
		}
		return nil
	}
	switch record.State {
	case clusterstate.WorkflowRouteStarting:
		if record.Starting == nil || record.Starting.Binding == nil || record.Starting.SandboxID != event.ObjectID {
			return nil, 0, errors.New("controlplane: Route has no selected execution Binding")
		}
		binding := record.Starting.Binding
		if err := match(binding.NodeID, binding.NodeEpoch, binding.RegistryGeneration, binding.BindingDigest); err != nil {
			return nil, 0, err
		}
		if event.DataEndpoint != binding.DataEndpoint {
			return nil, 0, errors.New("controlplane: event data endpoint differs from committed Binding intent")
		}
		return nil, record.Starting.LastEventSeq, nil
	case clusterstate.WorkflowRouteReady:
		if err := match(record.Ready.NodeID, record.Ready.NodeEpoch, record.Ready.RegistryGeneration, record.Ready.BindingDigest); err != nil {
			return nil, 0, err
		}
		projection := *record.Ready
		return &projection, projection.LastEventSeq, nil
	case clusterstate.WorkflowRoutePaused:
		projection := record.Paused.Execution
		if err := match(projection.NodeID, projection.NodeEpoch, projection.RegistryGeneration, projection.BindingDigest); err != nil {
			return nil, 0, err
		}
		return &projection, projection.LastEventSeq, nil
	case clusterstate.WorkflowRouteResuming:
		projection := record.Resuming.Execution
		if err := match(projection.NodeID, projection.NodeEpoch, projection.RegistryGeneration, projection.BindingDigest); err != nil {
			return nil, 0, err
		}
		return &projection, projection.LastEventSeq, nil
	case clusterstate.WorkflowRouteDeleting:
		projection := record.Deleting.Execution
		if err := match(projection.NodeID, projection.NodeEpoch, projection.RegistryGeneration, projection.BindingDigest); err != nil {
			return nil, 0, err
		}
		return &projection, max(projection.LastEventSeq, record.Deleting.LastEventSeq), nil
	case clusterstate.WorkflowRouteTombstone:
		tombstone := record.Tombstone
		if tombstone == nil || tombstone.PlacementFailure != nil ||
			match(tombstone.NodeID, tombstone.NodeEpoch, record.Revision.RegistryGeneration, tombstone.BindingDigest) != nil ||
			tombstone.SandboxID != event.ObjectID {
			return nil, 0, errors.New("controlplane: event is fenced by a Route tombstone")
		}
		return nil, tombstone.LastEventSeq, nil
	default:
		return nil, 0, errors.New("controlplane: invalid Route workflow state")
	}
}

func readyFromEvent(
	record clusterstate.RouteWorkflowRecord,
	current *clusterstate.ReadyRoute,
	event routesync.ExecutionEvent,
) (clusterstate.RouteWorkflowRecord, error) {
	var ready clusterstate.ReadyRoute
	if current != nil {
		ready = *current
		if err := validateReadyEventProjection(ready, event); err != nil {
			return clusterstate.RouteWorkflowRecord{}, err
		}
	} else {
		starting := record.Starting
		if starting == nil || starting.Binding == nil {
			return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: READY event has no STARTING Binding")
		}
		ready = clusterstate.ReadyRoute{
			SandboxID: starting.SandboxID, NodeID: starting.Binding.NodeID,
			NodeEpoch: starting.Binding.NodeEpoch, DataEndpoint: starting.Binding.DataEndpoint,
			RegistryGeneration: starting.Binding.RegistryGeneration,
			BindingDigest:      starting.Binding.BindingDigest, Intent: starting.Intent,
			Presentation: event.Presentation.Clone(),
		}
	}
	if event.DataEndpoint != "" && event.DataEndpoint != ready.DataEndpoint {
		return clusterstate.RouteWorkflowRecord{}, errors.New("controlplane: READY event changed data endpoint within NodeEpoch")
	}
	ready.LastEventSeq = event.EventSeq
	ready.Presentation = event.Presentation.Clone()
	if current == nil {
		ready.TargetPort = event.TargetPort
		ready.AccessToken = event.AccessToken
		ready.TrafficAccessToken = event.TrafficAccessToken
		ready.TemplateRef = event.TemplateRef
		ready.SnapshotRef = event.SnapshotRef
	}
	if err := ready.Validate(); err != nil {
		return clusterstate.RouteWorkflowRecord{}, err
	}
	return clusterstate.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteReady, Ready: &ready,
	}, nil
}

func validateReadyEventProjection(ready clusterstate.ReadyRoute, event routesync.ExecutionEvent) error {
	if event.TargetPort != 0 && event.TargetPort != ready.TargetPort {
		return errors.New("controlplane: READY event changed target port within one execution")
	}
	for _, field := range []struct {
		name    string
		current string
		event   string
	}{
		{name: "access token", current: ready.AccessToken, event: event.AccessToken},
		{name: "traffic access token", current: ready.TrafficAccessToken, event: event.TrafficAccessToken},
		{name: "template reference", current: ready.TemplateRef, event: event.TemplateRef},
		{name: "snapshot reference", current: ready.SnapshotRef, event: event.SnapshotRef},
	} {
		if field.event != "" && field.event != field.current {
			return fmt.Errorf("controlplane: READY event changed %s within one execution", field.name)
		}
	}
	return nil
}

func deletingFromTerminal(
	record clusterstate.RouteWorkflowRecord,
	execution clusterstate.ReadyRoute,
	event routesync.ExecutionEvent,
) clusterstate.RouteWorkflowRecord {
	spec := []byte("node-terminal-event-v1\x00" + event.State + "\x00" + event.BindingDigest)
	digest := sha256.Sum256(spec)
	return clusterstate.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey, State: clusterstate.WorkflowRouteDeleting,
		Deleting: &clusterstate.DeletingRouteState{
			Execution: execution, DeleteSpec: spec, DeleteSpecDigest: hex.EncodeToString(digest[:]),
			LastEventSeq: execution.LastEventSeq,
		},
	}
}

func tombstoneFromEvent(
	record clusterstate.RouteWorkflowRecord,
	event routesync.ExecutionEvent,
) (clusterstate.RouteWorkflowRecord, clusterstate.ExecutionFence, error) {
	proof := clusterstate.TerminalProof{
		Kind: clusterstate.ProofNodeTerminal, ProofDigest: terminalEventDigest(event),
		FencedNodeID: event.NodeID, FencedNodeEpoch: event.NodeEpoch,
	}
	reason := event.Reason
	if reason == "" {
		reason = event.State
	}
	tombstone := &clusterstate.RouteTombstoneState{
		SandboxID: event.ObjectID, NodeID: event.NodeID, NodeEpoch: event.NodeEpoch,
		RegistryGeneration: event.RegistryGeneration,
		BindingDigest:      event.BindingDigest, LastEventSeq: event.EventSeq,
		Proof: proof, TerminalReason: reason,
	}
	next := clusterstate.RouteWorkflowRecord{
		Group: record.Group, RouteKey: record.RouteKey,
		State: clusterstate.WorkflowRouteTombstone, Tombstone: tombstone,
	}
	fence := clusterstate.ExecutionFence{
		Group: record.Group, RouteKey: record.RouteKey, SandboxID: event.ObjectID,
		NodeID: event.NodeID, NodeEpoch: event.NodeEpoch,
		RegistryGeneration: event.RegistryGeneration, BindingDigest: event.BindingDigest,
		LastEventSeq: event.EventSeq, FinalOutboxWatermark: event.EventSeq, Proof: proof,
	}
	return next, fence, nil
}

func fenceFromTombstone(record clusterstate.RouteWorkflowRecord) clusterstate.ExecutionFence {
	tombstone := record.Tombstone
	return clusterstate.ExecutionFence{
		Group: record.Group, RouteKey: record.RouteKey, SandboxID: tombstone.SandboxID,
		NodeID: tombstone.NodeID, NodeEpoch: tombstone.NodeEpoch,
		RegistryGeneration: tombstone.RegistryGeneration, BindingDigest: tombstone.BindingDigest,
		LastEventSeq: tombstone.LastEventSeq, FinalOutboxWatermark: tombstone.LastEventSeq,
		Proof: tombstone.Proof,
	}
}

func terminalEventDigest(event routesync.ExecutionEvent) string {
	hash := sha256.New()
	_, _ = hash.Write([]byte("kuasar-node-terminal-proof-v1"))
	for _, field := range []string{
		event.ObjectKind, event.ObjectID, event.NodeID, event.RegistryGeneration,
		event.BindingDigest, event.State, event.Reason,
	} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(field)))
		_, _ = hash.Write(length[:])
		_, _ = hash.Write([]byte(field))
	}
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], event.NodeEpoch)
	_, _ = hash.Write(number[:])
	binary.BigEndian.PutUint64(number[:], event.EventSeq)
	_, _ = hash.Write(number[:])
	return hex.EncodeToString(hash.Sum(nil))
}
