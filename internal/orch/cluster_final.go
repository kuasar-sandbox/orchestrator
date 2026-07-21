package orch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/buildcfg"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
	"github.com/kuasar-sandbox/orchestrator/internal/nodeexec"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type ClusterSession struct {
	store      *store.Store
	nodeID     string
	nodeEpoch  uint64
	endpoint   string
	mu         sync.RWMutex
	current    nodeexec.LocalSessionIdentity
	firstReady chan struct{}
	readyOnce  sync.Once
}

func NewClusterSession(st *store.Store, identity store.ClusterStartIdentity) (*ClusterSession, error) {
	if st == nil || identity.NodeID == "" || identity.NodeEpoch == 0 || identity.DataEndpoint == "" {
		return nil, errors.New("orch: final cluster session requires a prepared durable identity")
	}
	return &ClusterSession{
		store: st, nodeID: identity.NodeID, nodeEpoch: identity.NodeEpoch,
		endpoint: identity.DataEndpoint, firstReady: make(chan struct{}),
	}, nil
}

func (o *Orchestrator) FencePriorClusterEpoch(ctx context.Context, identity store.ClusterStartIdentity) error {
	if !identity.ResetRequired {
		return nil
	}
	// Pools have not started yet, so every matching unit belongs to the prior
	// epoch. Stop both execution classes synchronously before deleting their
	// durable records or allowing the new epoch to establish a node-link.
	for _, pattern := range []string{o.builderPattern(), o.runnerPattern()} {
		units, err := o.lc.List(ctx, pattern)
		if err != nil {
			return fmt.Errorf("list prior NodeEpoch units %q: %w", pattern, err)
		}
		for _, unit := range units {
			switch unit.ActiveState {
			case "active", "activating", "reloading", "deactivating", "failed":
				if err := o.lc.Stop(ctx, unit.Name); err != nil {
					return fmt.Errorf("stop prior NodeEpoch unit %q: %w", unit.Name, err)
				}
				if err := o.lc.ResetFailed(ctx, unit.Name); err != nil {
					return fmt.Errorf("reset prior NodeEpoch unit %q: %w", unit.Name, err)
				}
			}
		}
	}
	records, err := o.st.PriorNodeEpochWorkflows(ctx, identity.NodeID, identity.NodeEpoch)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Kind != clusterstate.ExecutionKindSandbox {
			continue
		}
		sandbox, err := o.st.Get(ctx, record.ObjectID)
		if err != nil {
			return err
		}
		if sandbox != nil {
			o.teardown(ctx, sandbox)
			o.uncache(sandbox.ID)
		}
	}
	if err := o.st.ResetPriorNodeEpoch(ctx, identity.NodeID, identity.NodeEpoch); err != nil {
		return err
	}
	return nil
}

func (s *ClusterSession) NextSession(ctx context.Context) (routesync.SessionTuple, error) {
	identity, err := s.store.NextClusterSession(ctx, s.nodeEpoch)
	if err != nil {
		return routesync.SessionTuple{}, err
	}
	if identity.NodeID != s.nodeID || identity.DataEndpoint != s.endpoint {
		return routesync.SessionTuple{}, errors.New("orch: durable cluster identity changed during process lifetime")
	}
	if err := s.store.CompactFinalizedNodeWorkflows(ctx, identity.NodeID, identity.NodeEpoch, identity.SessionSeq); err != nil {
		return routesync.SessionTuple{}, err
	}
	current := nodeexec.LocalSessionIdentity{
		NodeID: identity.NodeID, NodeEpoch: identity.NodeEpoch,
		SessionSeq: identity.SessionSeq, DataEndpoint: identity.DataEndpoint,
	}
	s.mu.Lock()
	s.current = current
	s.mu.Unlock()
	s.readyOnce.Do(func() { close(s.firstReady) })
	return routesync.SessionTuple{NodeEpoch: identity.NodeEpoch, SessionSeq: identity.SessionSeq}, nil
}

func (s *ClusterSession) Current(context.Context) (nodeexec.LocalSessionIdentity, error) {
	s.mu.RLock()
	current := s.current
	s.mu.RUnlock()
	if err := current.Validate(); err != nil {
		return nodeexec.LocalSessionIdentity{}, err
	}
	return current, nil
}

func (s *ClusterSession) WaitReady(ctx context.Context) error {
	select {
	case <-s.firstReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type ClusterResourceLoad struct {
	Controller              bool
	WaterZone               string
	Draining                bool
	NodeAllocatedMemory     uint64
	AllocatablePoolMemory   uint64
	BuildReservedMemory     uint64
	StartupAllocatedMemory  uint64
	StartupPoolMemory       uint64
	AdmissionTokenAvailable bool
	SafetyRejectReason      string
}

type ClusterNodeOptions struct {
	Session             *ClusterSession
	SandboxAdmission    nodeexec.SandboxAdmissionController
	SandboxUsage        func(context.Context) (nodectl.PreparedAdmissionUsage, error)
	ResourceLoad        func() ClusterResourceLoad
	SandboxSlotCapacity uint64
	SandboxQueueLimit   uint64
	BuildCapacity       nodeexec.BuildCapacity
	SandboxWorkers      int
	BuildWorkers        int
}

type FinalClusterNode struct {
	core      *Orchestrator
	store     *store.Store
	session   *ClusterSession
	authority *nodeexec.Authority
	options   ClusterNodeOptions
	log       *slog.Logger

	activeMu       sync.Mutex
	active         map[string]struct{}
	sandboxSem     chan struct{}
	buildSem       chan struct{}
	executionReady chan struct{}
	placementWake  chan struct{}
	readyOnce      sync.Once
}

func NewFinalClusterNode(core *Orchestrator, st *store.Store, options ClusterNodeOptions, log *slog.Logger) (*FinalClusterNode, error) {
	if core == nil || st == nil || options.Session == nil || options.SandboxAdmission == nil || options.SandboxUsage == nil ||
		options.SandboxSlotCapacity == 0 || options.SandboxQueueLimit == 0 {
		return nil, errors.New("orch: final cluster node requires core, store, session, Admission, usage, and Sandbox limits")
	}
	if err := options.BuildCapacity.Validate(); err != nil {
		return nil, err
	}
	if options.SandboxWorkers <= 0 {
		options.SandboxWorkers = 8
	}
	if options.BuildWorkers <= 0 {
		options.BuildWorkers = int(options.BuildCapacity.Slots)
	}
	if log == nil {
		log = slog.Default()
	}
	node := &FinalClusterNode{
		core: core, store: st, session: options.Session, options: options, log: log,
		active: make(map[string]struct{}), sandboxSem: make(chan struct{}, options.SandboxWorkers),
		buildSem: make(chan struct{}, options.BuildWorkers), executionReady: make(chan struct{}),
		placementWake: make(chan struct{}, 1),
	}
	authority, err := nodeexec.NewAuthority(
		st, options.SandboxAdmission, options.Session.Current,
		node.buildCapacity, node.buildObject, node.sandboxObject, node.sandboxDemand,
	)
	if err != nil {
		return nil, err
	}
	node.authority = authority
	return node, nil
}

func (n *FinalClusterNode) Run(ctx context.Context) {
	if err := n.session.WaitReady(ctx); err != nil {
		return
	}
	if err := n.authority.ReconcileSandboxAdmissions(ctx, 256); err != nil {
		n.log.Error("cluster Sandbox Admission reconciliation failed", "err", err)
		return
	}
	n.readyOnce.Do(func() { close(n.executionReady) })
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := n.reconcileWork(ctx); err != nil && ctx.Err() == nil {
			n.log.Warn("cluster execution reconciliation", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-n.authority.WorkWake():
		case <-n.store.WorkflowWake():
		case <-n.authority.SandboxAdmissionWake():
		case <-ticker.C:
		}
	}
}

func (n *FinalClusterNode) reconcileWork(ctx context.Context) error {
	var joined error
	if err := n.authority.PromoteSandboxQueue(ctx); err != nil {
		joined = errors.Join(joined, err)
	}
	if _, err := n.authority.PromoteBuildQueue(ctx); err != nil {
		joined = errors.Join(joined, err)
	}
	for _, kind := range []clusterstate.ExecutionKind{clusterstate.ExecutionKindSandbox, clusterstate.ExecutionKindBuild} {
		after := ""
		for {
			records, err := n.authority.Launchable(ctx, kind, after, 64)
			if err != nil {
				joined = errors.Join(joined, err)
				break
			}
			for _, record := range records {
				n.startWorkflow(ctx, record)
			}
			if len(records) < 64 {
				break
			}
			after = records[len(records)-1].ObjectID
		}
	}
	return joined
}

func (n *FinalClusterNode) startWorkflow(ctx context.Context, record *nodeexec.WorkflowRecord) {
	if record == nil {
		return
	}
	key := fmt.Sprintf("%d/%s", record.Kind, record.ObjectID)
	n.activeMu.Lock()
	if _, exists := n.active[key]; exists {
		n.activeMu.Unlock()
		return
	}
	n.active[key] = struct{}{}
	n.activeMu.Unlock()
	n.signalPlacement()
	sem := n.sandboxSem
	if record.Kind == clusterstate.ExecutionKindBuild {
		sem = n.buildSem
	}
	go func() {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			n.finishWorkflow(key)
			return
		}
		defer func() {
			<-sem
			n.finishWorkflow(key)
		}()
		var err error
		if record.Kind == clusterstate.ExecutionKindSandbox {
			err = n.executeSandbox(ctx, record)
		} else {
			err = n.executeBuild(ctx, record)
		}
		if err != nil && ctx.Err() == nil {
			n.log.Error("cluster workflow execution", "kind", record.Kind, "object", record.ObjectID, "err", err)
		}
	}()
}

func (n *FinalClusterNode) finishWorkflow(key string) {
	n.activeMu.Lock()
	delete(n.active, key)
	n.activeMu.Unlock()
	n.signalPlacement()
}

func (n *FinalClusterNode) PlacementWake() <-chan struct{} { return n.placementWake }

func (n *FinalClusterNode) signalPlacement() {
	select {
	case n.placementWake <- struct{}{}:
	default:
	}
}

func (n *FinalClusterNode) HandleCommand(ctx context.Context, wire *routesync.Command) *routesync.CmdAck {
	if wire == nil {
		return &routesync.CmdAck{Status: routesync.AckRejected, Reason: "empty command"}
	}
	select {
	case <-n.executionReady:
	default:
		return finalReject(wire, routesync.DispatchSessionMoved, errors.New("node execution reconciliation is not complete"))
	}
	if err := n.verifyCurrentSessionTuple(ctx, wire); err != nil {
		return finalReject(wire, routesync.DispatchSessionMoved, err)
	}
	defer n.signalPlacement()
	switch wire.Kind {
	case routesync.CmdKeyPut:
		if wire.KeyLease == nil || wire.KeyLeaseRef != nil {
			return finalReject(wire, routesync.DispatchConflict, errors.New("key_put requires exactly one complete key lease"))
		}
		if wire.AuthKeyFingerprint != wire.KeyLease.AuthKey.Fingerprint ||
			wire.ManifestKeyFingerprint != wire.KeyLease.ManifestKey.Fingerprint {
			return finalReject(wire, routesync.DispatchConflict, errors.New("key_put command fingerprints do not match its lease"))
		}
		ref, err := n.core.putClusterKeyLease(ctx, *wire.KeyLease)
		if err != nil {
			return finalReject(wire, routesync.DispatchConflict, err)
		}
		return &routesync.CmdAck{CmdID: wire.CmdID, Status: routesync.AckAccepted, KeyLeaseRef: &ref}
	case routesync.CmdKeyDrop:
		if wire.KeyLeaseRef == nil || wire.KeyLease != nil {
			return finalReject(wire, routesync.DispatchConflict, errors.New("key_drop requires exactly one key lease reference"))
		}
		ref := *wire.KeyLeaseRef
		if err := ref.Validate(); err != nil || wire.AuthKeyFingerprint != ref.AuthKeyFingerprint ||
			wire.ManifestKeyFingerprint != ref.ManifestKeyFingerprint {
			return finalReject(wire, routesync.DispatchConflict, errors.Join(err, errors.New("invalid key_drop reference")))
		}
		if _, err := n.store.DropKeyLeaseRef(
			ctx, ref.Group, ref.AuthKeyFingerprint, ref.ManifestKeyFingerprint,
		); err != nil {
			return finalReject(wire, routesync.DispatchConflict, err)
		}
		return &routesync.CmdAck{CmdID: wire.CmdID, Status: routesync.AckAccepted, KeyLeaseRef: &ref}
	case routesync.CmdSandboxAdmitDispatch, routesync.CmdBuildAdmitDispatch:
		local, err := n.session.Current(ctx)
		if err != nil {
			return finalReject(wire, routesync.DispatchSessionMoved, err)
		}
		command, err := nodeexec.DispatchCommandFromWire(wire, local)
		if err != nil {
			return finalReject(wire, routesync.DispatchWrongBinding, err)
		}
		reply, err := n.authority.AdmitAndDispatch(ctx, command)
		if err != nil {
			return finalReject(wire, routesync.DispatchUnknown, err)
		}
		return &routesync.CmdAck{CmdID: wire.CmdID, Status: routesync.AckAccepted, Outcome: string(reply.Outcome), Reason: reply.Reason}
	case routesync.CmdSandboxResume:
		if err := n.verifyCurrentCommand(ctx, wire, clusterstate.ExecutionKindSandbox, wire.SID); err != nil {
			return finalReject(wire, routesync.DispatchWrongBinding, err)
		}
		go n.resumeSandbox(n.core.asyncCtx(), wire)
		return finalAccept(wire)
	case routesync.CmdSandboxDelete:
		if err := n.verifyCurrentCommand(ctx, wire, clusterstate.ExecutionKindSandbox, wire.SID); err != nil {
			return finalReject(wire, routesync.DispatchWrongBinding, err)
		}
		go n.deleteSandbox(n.core.asyncCtx(), wire)
		return finalAccept(wire)
	case routesync.CmdRebindExecution:
		if err := n.core.rebindClusterExecution(ctx, wire); err != nil {
			outcome := routesync.DispatchUnknown
			if errors.Is(err, errWrongExecutionBinding) {
				outcome = routesync.DispatchWrongBinding
			}
			return finalReject(wire, outcome, err)
		}
		kind, objectID := clusterstate.ExecutionKindBuild, wire.BuildID
		if wire.SID != "" {
			kind, objectID = clusterstate.ExecutionKindSandbox, wire.SID
		}
		fact, err := n.store.RecoveryExecutionFact(ctx, kind, objectID, wire.RegistryGeneration)
		if err != nil {
			return finalReject(wire, routesync.DispatchConflict, errors.Join(err, errors.New("rebound workflow has no durable object")))
		}
		object := fact.Object
		if object.Binding != wire.Binding || object.BindingDigest != wire.BindingDigest {
			return finalReject(wire, routesync.DispatchWrongBinding, errors.New("rebound object does not match target Binding"))
		}
		return &routesync.CmdAck{
			CmdID: wire.CmdID, Status: routesync.AckAccepted, RebindObject: &object,
			RebindAdmissionState: fact.AdmissionState, RebindResourceClaimed: fact.ResourceClaimed,
		}
	case routesync.CmdAckRecoveryEvent:
		record, err := n.ackRecoveryEvent(ctx, wire)
		if err != nil {
			return finalReject(wire, routesync.DispatchConflict, err)
		}
		object := recoveryObjectFromEvent(*record.LatestEvent)
		return &routesync.CmdAck{
			CmdID: wire.CmdID, Status: routesync.AckAccepted, RebindObject: &object,
			RebindAdmissionState: string(record.AdmissionState), RebindResourceClaimed: record.ResourceClaimed,
		}
	case routesync.CmdFinalizeWorkflow:
		if err := n.finalizeWorkflow(ctx, wire); err != nil {
			return finalReject(wire, routesync.DispatchConflict, err)
		}
		return finalAccept(wire)
	case routesync.CmdCollectRecovery:
		page, err := n.collectRecoveryReport(ctx, wire)
		if err != nil {
			return finalReject(wire, routesync.DispatchConflict, err)
		}
		return &routesync.CmdAck{CmdID: wire.CmdID, Status: routesync.AckAccepted, Recovery: page}
	default:
		return finalReject(wire, "", fmt.Errorf("final node-link forbids command kind %q", wire.Kind))
	}
}

func recoveryObjectFromEvent(event routesync.ExecutionEvent) routesync.RecoveryObjectSnapshot {
	return routesync.RecoveryObjectSnapshot{
		ObjectKind: event.ObjectKind, ObjectID: event.ObjectID, NodeID: event.NodeID, NodeEpoch: event.NodeEpoch,
		RegistryGeneration: event.RegistryGeneration, Binding: event.Binding, BindingDigest: event.BindingDigest,
		EventSeq: event.EventSeq, State: event.State, DataEndpoint: event.DataEndpoint,
		TargetPort: event.TargetPort, AccessToken: event.AccessToken,
		TrafficAccessToken: event.TrafficAccessToken, TemplateRef: event.TemplateRef,
		SnapshotRef: event.SnapshotRef, SnapshotLocation: event.SnapshotLocation,
		ArtifactRef: event.ArtifactRef, Reason: event.Reason,
	}
}

func (n *FinalClusterNode) ackRecoveryEvent(ctx context.Context, command *routesync.Command) (*nodeexec.WorkflowRecord, error) {
	if command == nil || command.EventAck == nil {
		return nil, errors.New("recovery event ACK command is incomplete")
	}
	ack := *command.EventAck
	if command.SID == "" || command.BuildID != "" {
		return nil, errors.New("recovery event ACK is valid only for one Sandbox")
	}
	const objectKind = "sandbox"
	objectID := command.SID
	if ack.ObjectKind != objectKind || ack.ObjectID != objectID || ack.EventSeq == 0 {
		return nil, errors.New("recovery event ACK identifies another execution")
	}
	if err := n.verifyCurrentCommand(ctx, command, clusterstate.ExecutionKindSandbox, objectID); err != nil {
		return nil, err
	}
	record, err := n.store.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, objectID)
	if err != nil {
		return nil, err
	}
	if record == nil || record.LatestEvent == nil || record.EventSeq < ack.EventSeq ||
		record.BindingDigest != command.BindingDigest ||
		record.LatestEvent.RegistryGeneration != command.RegistryGeneration ||
		record.LatestEvent.BindingDigest != command.BindingDigest {
		return nil, errors.New("recovery event ACK does not match the durable target event")
	}
	local, err := n.session.Current(ctx)
	if err != nil {
		return nil, err
	}
	if err := n.store.AckExecutionEvent(ctx, local.NodeID, local.NodeEpoch, ack); err != nil {
		return nil, err
	}
	return record, nil
}

func (n *FinalClusterNode) collectRecoveryReport(ctx context.Context, command *routesync.Command) (*routesync.RecoveryReportPage, error) {
	if command == nil || command.Recovery == nil {
		return nil, errors.New("recovery report command is incomplete")
	}
	request := *command.Recovery
	if err := request.Validate(); err != nil {
		return nil, err
	}
	local, err := n.session.Current(ctx)
	if err != nil {
		return nil, err
	}
	if command.NodeEpoch != local.NodeEpoch || command.SessionSeq != local.SessionSeq {
		return nil, errors.New("recovery report command carries a stale node-link tuple")
	}
	objects, err := n.store.RecoveryExecutionReport(ctx, local.NodeID, local.NodeEpoch, request.SourceRegistryGeneration)
	if err != nil {
		return nil, err
	}
	digest, err := routesync.CanonicalRecoveryReportDigest(objects)
	if err != nil {
		return nil, err
	}
	if request.ExpectedReportDigest != "" && request.ExpectedReportDigest != digest {
		return nil, errors.New("durable recovery report changed between pages")
	}
	if request.Offset > uint64(len(objects)) {
		return nil, errors.New("recovery report offset is beyond the snapshot")
	}
	end := min(request.Offset+uint64(request.Limit), uint64(len(objects)))
	pageObjects := slices.Clone(objects[request.Offset:end])
	for len(pageObjects) > 0 {
		encoded, encodeErr := json.Marshal(pageObjects)
		if encodeErr != nil {
			return nil, encodeErr
		}
		if uint32(len(encoded)) <= request.MaxBytes {
			break
		}
		pageObjects = pageObjects[:len(pageObjects)-1]
		end--
	}
	if request.Offset < uint64(len(objects)) && len(pageObjects) == 0 {
		return nil, errors.New("one recovery report object exceeds the requested page byte limit")
	}
	page := &routesync.RecoveryReportPage{
		RecoveryEpoch: request.RecoveryEpoch, SourceClusterID: request.SourceClusterID,
		SourceRegistryGeneration: request.SourceRegistryGeneration, SourceRegistryLayoutDigest: request.SourceRegistryLayoutDigest,
		TargetRegistryGeneration: request.TargetRegistryGeneration, TargetRegistryLayoutDigest: request.TargetRegistryLayoutDigest,
		NodeID: local.NodeID, NodeEpoch: local.NodeEpoch, SessionSeq: local.SessionSeq,
		ReportDigest: digest, TotalObjects: uint64(len(objects)), Offset: request.Offset,
		NextOffset: end, Complete: end == uint64(len(objects)), Objects: pageObjects,
	}
	if err := page.ValidateFor(request, local.NodeID, local.NodeEpoch, local.SessionSeq); err != nil {
		return nil, err
	}
	return page, nil
}

func finalAccept(command *routesync.Command) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: command.CmdID, Status: routesync.AckAccepted}
}

func finalReject(command *routesync.Command, outcome string, err error) *routesync.CmdAck {
	return &routesync.CmdAck{CmdID: command.CmdID, Status: routesync.AckRejected, Outcome: outcome, Reason: err.Error()}
}

func (n *FinalClusterNode) verifyCurrentSessionTuple(ctx context.Context, command *routesync.Command) error {
	local, err := n.session.Current(ctx)
	if err != nil {
		return err
	}
	if command.NodeEpoch != local.NodeEpoch || command.SessionSeq != local.SessionSeq {
		return errors.New("cluster command carries a stale node-link tuple")
	}
	return nil
}

func (n *FinalClusterNode) verifyCurrentCommand(ctx context.Context, command *routesync.Command, kind clusterstate.ExecutionKind, objectID string) error {
	if objectID == "" {
		return errors.New("cluster command object ID is required")
	}
	if kind == clusterstate.ExecutionKindSandbox {
		return n.core.verifySandboxCommandBinding(ctx, command)
	}
	build, err := n.store.GetBuild(ctx, objectID)
	if err != nil {
		return err
	}
	if build == nil {
		return errWrongExecutionBinding
	}
	binding, opaque, err := clusterstate.ExecutionBindingFromMetadata(build.Metadata)
	if err != nil {
		return err
	}
	digest, err := clusterstate.ExecutionBindingDigest(opaque)
	if err != nil {
		return err
	}
	if binding.Kind != kind || binding.ObjectID != objectID || binding.NodeEpoch != command.NodeEpoch ||
		binding.RegistryGeneration != command.RegistryGeneration || digest != command.BindingDigest {
		return errWrongExecutionBinding
	}
	return nil
}

func (n *FinalClusterNode) finalizeWorkflow(ctx context.Context, command *routesync.Command) error {
	var kind clusterstate.ExecutionKind
	var objectID string
	switch {
	case command.SID != "" && command.BuildID == "":
		kind, objectID = clusterstate.ExecutionKindSandbox, command.SID
	case command.BuildID != "" && command.SID == "":
		kind, objectID = clusterstate.ExecutionKindBuild, command.BuildID
	default:
		return errors.New("workflow finalization requires exactly one Sandbox or Build ID")
	}
	return n.authority.FinalizeWorkflow(ctx, kind, objectID, command.BindingDigest)
}

func (n *FinalClusterNode) executeSandbox(ctx context.Context, record *nodeexec.WorkflowRecord) error {
	claimed, err := n.authority.ClaimSandbox(ctx, record)
	if err != nil {
		return err
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(claimed.DispatchSpec)
	if err != nil {
		return n.failSandbox(ctx, claimed, nil, err)
	}
	existing, err := n.store.Get(ctx, claimed.ObjectID)
	if err != nil {
		return err
	}
	if existing == nil {
		return n.failSandbox(ctx, claimed, nil, errors.New("accepted Sandbox object is missing"))
	}
	if existing != nil && existing.State == types.StateRunning && existing.RunID != "" && n.core.unitActive(ctx, n.core.runnerUnit(existing.RunID)) {
		_, err = n.store.CommitSandboxEvent(ctx, existing, nodeexec.EventUpdate{
			State: string(clusterstate.WorkflowRouteReady), TargetPort: spec.TargetPort,
		})
		return err
	}
	template, err := types.ParseTemplateID(existing.TemplateID)
	if err != nil {
		return n.failSandbox(ctx, claimed, existing, err)
	}
	sandbox := existing
	sandbox.State = types.StateRunning
	if err := n.core.launch(ctx, sandbox, template); err != nil {
		n.core.teardown(context.Background(), sandbox)
		return n.failSandbox(ctx, claimed, sandbox, err)
	}
	if _, err := n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: spec.TargetPort,
	}); err != nil {
		return err
	}
	n.core.publishUpsert(sandbox)
	return nil
}

func (n *FinalClusterNode) sandboxObject(
	ctx context.Context,
	record nodeexec.DispatchRecord,
) (*types.Sandbox, error) {
	spec, err := clusterstate.ParseSandboxDispatchSpec(record.DispatchSpec)
	if err != nil {
		return nil, err
	}
	lease, err := n.core.resolveByFingerprints(
		ctx, record.Group, spec.AuthKeyFingerprint, spec.ManifestKeyFingerprint,
	)
	if err != nil {
		return nil, err
	}
	create, err := api.ParseSandboxCreateEnvelope(spec.Request)
	if err != nil || create.TemplateID != spec.TemplateRef || create.TimeoutSec != spec.TimeoutSeconds ||
		!reflect.DeepEqual(create.Metadata, spec.Config) {
		return nil, errors.Join(err, errors.New("Sandbox request envelope does not match dispatch spec"))
	}
	template, err := types.ParseTemplateID(spec.TemplateRef)
	if err != nil {
		return nil, err
	}
	trafficToken, err := keysMintToken()
	if err != nil {
		return nil, err
	}
	timeout := spec.TimeoutSeconds
	if timeout <= 0 {
		timeout = n.core.cfg.Sandbox.TimeoutSec
	}
	sandbox := &types.Sandbox{
		ID: record.ObjectID, TemplateID: spec.TemplateRef, State: types.StateStarting,
		RunDir:  n.core.cfg.Paths.RunRoot + "/" + record.ObjectID,
		BaseDir: n.core.cfg.Paths.BaseRoot + "/" + record.ObjectID,
		AuthKey: lease.AuthKey, ManifestKey: lease.ManifestKey,
		EnvdAccessToken: spec.AccessToken, TrafficAccessToken: trafficToken,
		Metadata: clusterstate.WithoutSystemMetadata(create.Metadata), Env: create.EnvVars,
		CreatedUnix:  time.Now().Unix(),
		DeadlineUnix: time.Now().Add(time.Duration(timeout) * time.Second).Unix(),
	}
	if template.Profile == types.ProfileE2B {
		sandbox.EnvdUDS = sandbox.RunDir + "/envd.sock"
		sandbox.CiUDS = sandbox.RunDir + "/ci.sock"
	}
	return sandbox, nil
}

func (n *FinalClusterNode) failSandbox(ctx context.Context, record *nodeexec.WorkflowRecord, sandbox *types.Sandbox, cause error) error {
	reason := cause.Error()
	if sandbox == nil || sandbox.ID == "" {
		if err := n.authority.FailSandbox(ctx, record, reason); err != nil {
			return errors.Join(cause, err)
		}
		return cause
	}
	terminal, err := n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{State: "ERROR", Reason: reason})
	if err != nil {
		return errors.Join(cause, err)
	}
	if err := n.authority.ReleaseSandboxResources(ctx, terminal, reason); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (n *FinalClusterNode) executeBuild(ctx context.Context, record *nodeexec.WorkflowRecord) error {
	claimed, err := n.authority.ClaimBuild(ctx, record)
	if err != nil {
		return err
	}
	build, err := n.store.GetBuild(ctx, claimed.ObjectID)
	if err != nil {
		return err
	}
	if build == nil {
		return errors.New("cluster Build object disappeared after durable Admission")
	}
	switch build.Status {
	case types.BuildWaiting:
		if _, err := n.store.CommitClusterBuildState(ctx, build, nodeexec.EventUpdate{State: string(types.BuildBuilding)}); err != nil {
			return err
		}
		build.Status = types.BuildBuilding
		n.core.executeBuild(ctx, build)
		return nil
	case types.BuildBuilding:
		n.core.executeBuild(ctx, build)
		return nil
	default:
		return fmt.Errorf("cluster Build worker received non-launchable status %q", build.Status)
	}
}

func (n *FinalClusterNode) resumeSandbox(ctx context.Context, command *routesync.Command) {
	if err := n.resumeSandboxSync(ctx, command); err != nil && ctx.Err() == nil {
		n.log.Error("cluster Sandbox resume", "sandbox", command.SID, "err", err)
	}
}

func (n *FinalClusterNode) resumeSandboxSync(ctx context.Context, command *routesync.Command) error {
	record, err := n.store.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, command.SID)
	if err != nil || record == nil {
		return errors.Join(err, nodeexec.ErrWorkflowMissing)
	}
	sandbox, err := n.store.Get(ctx, command.SID)
	if err != nil || sandbox == nil {
		return errors.Join(err, errWrongExecutionBinding)
	}
	if sandbox.State == types.StatePaused {
		if err := n.core.resume(ctx, sandbox); err != nil {
			return n.failSandbox(ctx, record, sandbox, err)
		}
		sandbox, err = n.store.Get(ctx, command.SID)
		if err != nil {
			return err
		}
	}
	spec, err := clusterstate.ParseSandboxDispatchSpec(record.DispatchSpec)
	if err != nil {
		return err
	}
	_, err = n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{
		State: string(clusterstate.WorkflowRouteReady), TargetPort: spec.TargetPort,
	})
	return err
}

func (n *FinalClusterNode) deleteSandbox(ctx context.Context, command *routesync.Command) {
	if err := n.deleteSandboxSync(ctx, command); err != nil && ctx.Err() == nil {
		n.log.Error("cluster Sandbox delete", "sandbox", command.SID, "err", err)
	}
}

func (n *FinalClusterNode) deleteSandboxSync(ctx context.Context, command *routesync.Command) error {
	record, err := n.store.GetNodeWorkflow(ctx, clusterstate.ExecutionKindSandbox, command.SID)
	if err != nil || record == nil {
		return errors.Join(err, nodeexec.ErrWorkflowMissing)
	}
	if record.ObjectState == "DELETED" {
		return nil
	}
	sandbox, err := n.store.Get(ctx, command.SID)
	if err != nil || sandbox == nil {
		return errors.Join(err, errWrongExecutionBinding)
	}
	n.core.teardown(ctx, sandbox)
	terminal, err := n.store.CommitSandboxEvent(ctx, sandbox, nodeexec.EventUpdate{State: "DELETED"})
	if err != nil {
		return err
	}
	n.core.uncache(command.SID)
	n.core.publishDelete(command.SID)
	return n.authority.ReleaseSandboxResources(ctx, terminal, "deleted")
}

func (n *FinalClusterNode) sandboxDemand(ctx context.Context, record nodeexec.DispatchRecord) (nodectl.SandboxAdmissionDemand, error) {
	spec, err := clusterstate.ParseSandboxDispatchSpec(record.DispatchSpec)
	if err != nil {
		return nodectl.SandboxAdmissionDemand{}, err
	}
	if _, err := n.core.resolveByFingerprints(
		ctx, record.Group, spec.AuthKeyFingerprint, spec.ManifestKeyFingerprint,
	); err != nil {
		return nodectl.SandboxAdmissionDemand{}, err
	}
	normalized, err := placement.ParseNormalizedDemand(record.NormalizedDemand)
	if err != nil || normalized.Sandbox == nil {
		return nodectl.SandboxAdmissionDemand{}, errors.Join(err, errors.New("Sandbox normalized demand is missing"))
	}
	demand := normalized.Sandbox
	resources, err := sandboxcfg.ResolveResources(spec.Config)
	if err != nil {
		return nodectl.SandboxAdmissionDemand{}, err
	}
	if resources.FloorMemoryBytes > 0 && resources.FloorMemoryBytes != demand.FloorMemory ||
		resources.StartupMemoryBytes > 0 && resources.StartupMemoryBytes != demand.StartupBudgetMemory {
		return nodectl.SandboxAdmissionDemand{}, errors.New("Sandbox normalized demand does not match effective resource config")
	}
	capacityMemory := uint64(max(n.core.cfg.Sandbox.Resources.MemoryMiB(), 0)) << 20
	capacityCPU := n.core.cfg.Sandbox.Resources.VCPU
	if resources.CapacityMemoryBytes > 0 {
		capacityMemory = resources.CapacityMemoryBytes
	}
	if resources.CapacityCPU > 0 {
		capacityCPU = resources.CapacityCPU
	}
	return nodectl.SandboxAdmissionDemand{
		SlotUnits:             demand.SlotUnits,
		CapacityMemoryBytes:   capacityMemory,
		CapacityCPU:           capacityCPU,
		FloorMemoryBytes:      demand.FloorMemory,
		FloorCPU:              resources.FloorCPU,
		StartupBudgetMemory:   demand.StartupBudgetMemory,
		AllocatableAtSnapshot: demand.AllocatableAtSnapshot,
	}, nil
}

func (n *FinalClusterNode) buildObject(ctx context.Context, record nodeexec.DispatchRecord) (*types.Build, error) {
	spec, err := clusterstate.ParseBuildDispatchSpec(record.DispatchSpec)
	if err != nil {
		return nil, err
	}
	lease, err := n.core.resolveByFingerprints(
		ctx, record.Group, spec.AuthKeyFingerprint, spec.ManifestKeyFingerprint,
	)
	if err != nil {
		return nil, err
	}
	register, err := api.ParseBuildRegisterEnvelope(spec.Request)
	if err != nil || register.Profile != spec.Profile || register.CPUCount != spec.CPUCount ||
		register.MemoryMB != spec.MemoryMB || !slices.Equal(nonEmpty(register.Name), spec.Names) ||
		!slices.Equal(register.Tags, spec.Aliases) || !reflect.DeepEqual(register.Metadata, spec.Metadata) {
		return nil, errors.Join(err, errors.New("Build request envelope does not match dispatch spec"))
	}
	metadata := sandboxcfg.SetCapacity(register.Metadata, register.CPUCount, register.MemoryMB)
	metadata, builder, err := buildcfg.Extract(metadata)
	if err != nil {
		return nil, err
	}
	if err := n.core.validateBuildOptions(builder, false); err != nil {
		return nil, err
	}
	build := &types.Build{
		BuildID: record.ObjectID, TemplateID: spec.TemplateID,
		AuthKey: lease.AuthKey, ManifestKey: lease.ManifestKey,
		Profile: spec.Profile, CPUCount: spec.CPUCount, MemoryMB: spec.MemoryMB,
		Kind: types.KindImg, Names: append([]string(nil), spec.Names...),
		Aliases: append([]string(nil), spec.Aliases...), Metadata: clusterstate.WithoutSystemMetadata(metadata),
		Builder: builder, RegistryAuth: lease.RegistryAuth,
		Status: types.BuildRegistered, CreatedUnix: time.Now().Unix(),
	}
	return build, nil
}

func (n *FinalClusterNode) buildCapacity(context.Context) (nodeexec.BuildCapacity, string, error) {
	if n.options.ResourceLoad != nil {
		load := n.options.ResourceLoad()
		if load.SafetyRejectReason != "" {
			return n.options.BuildCapacity, load.SafetyRejectReason, nil
		}
	}
	return n.options.BuildCapacity, "", nil
}

func (n *FinalClusterNode) PlacementLoad(ctx context.Context) (*routesync.PlacementLoadSnapshot, error) {
	select {
	case <-n.executionReady:
	default:
		return nil, errors.New("node execution reconciliation is not complete")
	}
	sandboxUsage, err := n.options.SandboxUsage(ctx)
	if err != nil {
		return nil, err
	}
	identity, err := n.session.Current(ctx)
	if err != nil {
		return nil, err
	}
	buildTotal, _, err := n.store.BuildAdmissionUsage(ctx, identity.NodeID, identity.NodeEpoch)
	if err != nil {
		return nil, err
	}
	buildQueueDepth, err := n.store.BuildQueueDepth(ctx, identity.NodeID, identity.NodeEpoch)
	if err != nil {
		return nil, err
	}
	snapshot := &routesync.PlacementLoadSnapshot{
		WaterZone: "green", SandboxSlotUsed: sandboxUsage.SlotUsed,
		SandboxSlotHardLimit: saturatingUint64(n.options.SandboxSlotCapacity, n.options.SandboxQueueLimit),
		SandboxQueueDepth:    sandboxUsage.QueueDepth, SandboxQueueLimit: n.options.SandboxQueueLimit,
		SandboxRateTokenAvailable: true,
		BuildSlotHardLimit:        saturatingUint64(n.options.BuildCapacity.Slots, uint64(n.options.BuildCapacity.QueueLimit)),
		BuildSlotsUsed:            buildTotal.Slots, BuildCPUUsed: buildTotal.CPU,
		BuildMemoryUsed: buildTotal.Memory, BuildStorageUsed: buildTotal.Storage,
		BuildQueueDepth: buildQueueDepth, BuildQueueLimit: uint64(n.options.BuildCapacity.QueueLimit),
		BuildRateTokenAvailable: true,
	}
	if n.options.ResourceLoad != nil {
		load := n.options.ResourceLoad()
		snapshot.SandboxResourceController = load.Controller
		snapshot.WaterZone = load.WaterZone
		snapshot.Draining = load.Draining
		snapshot.NodeAllocatedMemory = load.NodeAllocatedMemory
		snapshot.AllocatablePoolMemory = load.AllocatablePoolMemory
		snapshot.BuildReservedMemory = load.BuildReservedMemory
		snapshot.StartupAllocatedMemory = load.StartupAllocatedMemory
		snapshot.StartupPoolMemory = load.StartupPoolMemory
		snapshot.SandboxRateTokenAvailable = load.AdmissionTokenAvailable
	}
	return snapshot, nil
}

func saturatingUint64(left, right uint64) uint64 {
	if right > math.MaxUint64-left {
		return math.MaxUint64
	}
	return left + right
}

func keysMintToken() (string, error) { return keys.MintToken() }
