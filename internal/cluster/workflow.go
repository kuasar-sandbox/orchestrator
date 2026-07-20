package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

const MaxDispatchSpecBytes = 64 << 10

type Revision struct {
	RegistryGeneration string `json:"registry_generation"`
	ShardID            uint32 `json:"shard_id"`
	LogIndex           uint64 `json:"log_index"`
}

func (r Revision) Validate() error {
	if r.RegistryGeneration == "" || r.LogIndex == 0 {
		return errors.New("cluster: revision requires Registry History Generation and committed log index")
	}
	return nil
}

func (r Revision) AtLeast(min Revision) bool {
	return r.RegistryGeneration == min.RegistryGeneration && r.ShardID == min.ShardID && r.LogIndex >= min.LogIndex
}

type RouteWorkflowState string

const (
	WorkflowRouteStarting  RouteWorkflowState = "STARTING"
	WorkflowRouteReady     RouteWorkflowState = "READY"
	WorkflowRoutePaused    RouteWorkflowState = "PAUSED"
	WorkflowRouteResuming  RouteWorkflowState = "RESUMING"
	WorkflowRouteDeleting  RouteWorkflowState = "DELETING"
	WorkflowRouteTombstone RouteWorkflowState = "TOMBSTONE"
)

type BuildWorkflowState string

const (
	BuildStarting   BuildWorkflowState = "BUILD_STARTING"
	BuildQueued     BuildWorkflowState = "BUILD_QUEUED"
	BuildRegistered BuildWorkflowState = "BUILD_REGISTERED"
	BuildBuilding   BuildWorkflowState = "BUILDING"
	BuildReady      BuildWorkflowState = "BUILD_READY"
	BuildError      BuildWorkflowState = "BUILD_ERROR"
	BuildTombstone  BuildWorkflowState = "BUILD_TOMBSTONE"
)

type DispatchOutcome string

const (
	DispatchAcceptedAdmitted DispatchOutcome = "ACCEPTED_ADMITTED"
	DispatchAcceptedQueued   DispatchOutcome = "ACCEPTED_QUEUED"
	DispatchDefinitiveReject DispatchOutcome = "DEFINITIVE_REJECT"
	DispatchSessionMoved     DispatchOutcome = "SESSION_MOVED"
	DispatchConflict         DispatchOutcome = "CONFLICT"
	DispatchWrongBinding     DispatchOutcome = "WRONG_BINDING"
	DispatchUnknown          DispatchOutcome = "UNKNOWN"
)

func (o DispatchOutcome) Validate() error {
	switch o {
	case DispatchAcceptedAdmitted, DispatchAcceptedQueued, DispatchDefinitiveReject,
		DispatchSessionMoved, DispatchConflict, DispatchWrongBinding, DispatchUnknown:
		return nil
	default:
		return errors.New("cluster: invalid dispatch outcome")
	}
}

type PlacementCandidate struct {
	NodeID        string `json:"node_id"`
	FailureDomain string `json:"failure_domain,omitempty"`
	RuntimeDigest string `json:"runtime_digest,omitempty"`
}

type DispatchIntent struct {
	NormalizedDemand      []byte `json:"normalized_demand"`
	DemandDigest          string `json:"demand_digest"`
	DispatchSpec          []byte `json:"dispatch_spec"`
	DispatchSpecDigest    string `json:"dispatch_spec_digest"`
	ProviderPolicyVersion string `json:"provider_policy_version"`
}

func NewDispatchIntent(demand, spec []byte, providerPolicyVersion string) (DispatchIntent, error) {
	intent := DispatchIntent{
		NormalizedDemand:      append([]byte(nil), demand...),
		DispatchSpec:          append([]byte(nil), spec...),
		ProviderPolicyVersion: providerPolicyVersion,
	}
	demandDigest := sha256.Sum256(demand)
	specDigest := sha256.Sum256(spec)
	intent.DemandDigest = hex.EncodeToString(demandDigest[:])
	intent.DispatchSpecDigest = hex.EncodeToString(specDigest[:])
	return intent, intent.Validate()
}

func (i DispatchIntent) Validate() error {
	if len(i.NormalizedDemand) == 0 || len(i.DispatchSpec) == 0 || i.ProviderPolicyVersion == "" {
		return errors.New("cluster: normalized demand, dispatch spec, and provider/policy version are required")
	}
	if len(i.DispatchSpec) > MaxDispatchSpecBytes {
		return fmt.Errorf("cluster: dispatch spec exceeds %d bytes", MaxDispatchSpecBytes)
	}
	if !digestMatches(i.DemandDigest, i.NormalizedDemand) || !digestMatches(i.DispatchSpecDigest, i.DispatchSpec) {
		return errors.New("cluster: demand or dispatch digest mismatch")
	}
	return nil
}

type ExecutionBindingIntent struct {
	NodeID             string `json:"node_id"`
	NodeEpoch          uint64 `json:"node_epoch"`
	DataEndpoint       string `json:"data_endpoint"`
	RegistryGeneration string `json:"registry_generation"`
	OpaqueBinding      string `json:"opaque_binding"`
	BindingDigest      string `json:"binding_digest"`
}

func (i ExecutionBindingIntent) Validate(kind ExecutionKind, objectID string) error {
	if i.NodeID == "" || i.NodeEpoch == 0 || i.DataEndpoint == "" || i.RegistryGeneration == "" || i.OpaqueBinding == "" || i.BindingDigest == "" {
		return errors.New("cluster: incomplete execution Binding intent")
	}
	binding, err := DecodeExecutionBinding(i.OpaqueBinding)
	if err != nil {
		return err
	}
	if binding.Kind != kind || binding.ObjectID != objectID || binding.NodeID != i.NodeID ||
		binding.NodeEpoch != i.NodeEpoch || binding.RegistryGeneration != i.RegistryGeneration {
		return errors.New("cluster: execution Binding intent identity mismatch")
	}
	digest, err := ExecutionBindingDigest(i.OpaqueBinding)
	if err != nil {
		return err
	}
	if digest != i.BindingDigest {
		return errors.New("cluster: execution Binding intent digest mismatch")
	}
	return nil
}

func (i ExecutionBindingIntent) ValidateWorkflow(kind ExecutionKind, objectID, group, routeKey string, intent DispatchIntent) error {
	if err := i.Validate(kind, objectID); err != nil {
		return err
	}
	if err := intent.Validate(); err != nil {
		return err
	}
	binding, err := DecodeExecutionBinding(i.OpaqueBinding)
	if err != nil {
		return err
	}
	if binding.Group != group || binding.RouteKey != routeKey ||
		hex.EncodeToString(binding.DemandDigest[:]) != intent.DemandDigest ||
		hex.EncodeToString(binding.DispatchSpecDigest[:]) != intent.DispatchSpecDigest {
		return errors.New("cluster: execution Binding does not cover the committed workflow intent")
	}
	return nil
}

type RouteStartingState struct {
	SandboxID            string                  `json:"sandbox_id"`
	PlacementRound       uint64                  `json:"placement_round"`
	CandidatePool        []PlacementCandidate    `json:"candidate_pool"`
	SelectedCandidate    *uint32                 `json:"selected_candidate,omitempty"`
	DefinitivelyRejected []uint32                `json:"definitively_rejected,omitempty"`
	Intent               DispatchIntent          `json:"intent"`
	Binding              *ExecutionBindingIntent `json:"binding,omitempty"`
	LastEventSeq         uint64                  `json:"last_event_seq,omitempty"`
}

func (s RouteStartingState) Validate() error {
	if s.SandboxID == "" || s.PlacementRound == 0 || len(s.CandidatePool) == 0 {
		return errors.New("cluster: STARTING requires sandbox ID, placement round, and candidates")
	}
	if err := validateCandidates(s.CandidatePool, s.SelectedCandidate, s.DefinitivelyRejected); err != nil {
		return err
	}
	if err := s.Intent.Validate(); err != nil {
		return err
	}
	if s.SelectedCandidate != nil {
		if s.Binding == nil {
			return errors.New("cluster: selected STARTING candidate requires a Binding")
		}
		if err := s.Binding.Validate(ExecutionKindSandbox, s.SandboxID); err != nil {
			return err
		}
		if s.CandidatePool[*s.SelectedCandidate].NodeID != s.Binding.NodeID {
			return errors.New("cluster: STARTING Binding does not match selected candidate")
		}
	} else if s.Binding != nil {
		return errors.New("cluster: unselected STARTING workflow cannot have a Binding")
	}
	return nil
}

type ReadyRoute struct {
	SandboxID string `json:"sandbox_id"`

	NodeID       string `json:"node_id"`
	NodeEpoch    uint64 `json:"node_epoch"`
	DataEndpoint string `json:"data_endpoint"`

	TargetPort         int    `json:"target_port,omitempty"`
	AccessToken        string `json:"access_token"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`

	TemplateRef        string `json:"template_ref"`
	SnapshotRef        string `json:"snapshot_ref,omitempty"`
	RegistryGeneration string `json:"registry_generation"`
	BindingDigest      string `json:"binding_digest"`
	LastEventSeq       uint64 `json:"last_event_seq"`
}

func (r ReadyRoute) Validate() error {
	if r.SandboxID == "" || r.NodeID == "" || r.NodeEpoch == 0 || r.DataEndpoint == "" ||
		r.AccessToken == "" || r.TemplateRef == "" || r.RegistryGeneration == "" || r.LastEventSeq == 0 {
		return errors.New("cluster: incomplete READY forwarding projection")
	}
	if !validDigest(r.BindingDigest) {
		return errors.New("cluster: invalid READY Binding digest")
	}
	if r.TargetPort < 0 || r.TargetPort > 65535 {
		return errors.New("cluster: invalid READY target port")
	}
	return nil
}

type PausedRouteState struct {
	Execution    ReadyRoute     `json:"execution"`
	SnapshotRef  string         `json:"snapshot_ref"`
	ResumeIntent DispatchIntent `json:"resume_intent"`
}

func (s PausedRouteState) Validate() error {
	if err := s.Execution.Validate(); err != nil {
		return err
	}
	if s.SnapshotRef == "" {
		return errors.New("cluster: PAUSED requires an authoritative snapshot reference")
	}
	return s.ResumeIntent.Validate()
}

type ResumingRouteState struct {
	Execution ReadyRoute     `json:"execution"`
	Intent    DispatchIntent `json:"intent"`
}

func (s ResumingRouteState) Validate() error {
	if err := s.Execution.Validate(); err != nil {
		return err
	}
	return s.Intent.Validate()
}

type DeletingRouteState struct {
	Execution        ReadyRoute `json:"execution"`
	DeleteSpec       []byte     `json:"delete_spec"`
	DeleteSpecDigest string     `json:"delete_spec_digest"`
	LastEventSeq     uint64     `json:"last_event_seq"`
}

func (s DeletingRouteState) Validate() error {
	if err := s.Execution.Validate(); err != nil {
		return err
	}
	if len(s.DeleteSpec) == 0 || len(s.DeleteSpec) > MaxDispatchSpecBytes || !digestMatches(s.DeleteSpecDigest, s.DeleteSpec) {
		return errors.New("cluster: invalid immutable delete spec")
	}
	if s.LastEventSeq < s.Execution.LastEventSeq {
		return errors.New("cluster: delete event watermark regressed")
	}
	return nil
}

type TerminalProofKind string

const (
	ProofNodeTerminal   TerminalProofKind = "NODE_TERMINAL"
	ProofNewerNodeEpoch TerminalProofKind = "NEWER_NODE_EPOCH"
	ProofExternalFence  TerminalProofKind = "EXTERNAL_FENCE"
)

type TerminalProof struct {
	Kind              TerminalProofKind `json:"kind"`
	ProofDigest       string            `json:"proof_digest"`
	FencedNodeID      string            `json:"fenced_node_id"`
	FencedNodeEpoch   uint64            `json:"fenced_node_epoch"`
	ObservedNodeEpoch uint64            `json:"observed_node_epoch,omitempty"`
}

func (p TerminalProof) Validate() error {
	if p.FencedNodeID == "" || p.FencedNodeEpoch == 0 || !validDigest(p.ProofDigest) {
		return errors.New("cluster: incomplete terminal/fencing proof")
	}
	switch p.Kind {
	case ProofNodeTerminal, ProofExternalFence:
		return nil
	case ProofNewerNodeEpoch:
		if p.ObservedNodeEpoch <= p.FencedNodeEpoch {
			return errors.New("cluster: newer-epoch proof does not advance NodeEpoch")
		}
		return nil
	default:
		return errors.New("cluster: invalid terminal/fencing proof kind")
	}
}

type RouteTombstoneState struct {
	SandboxID          string                      `json:"sandbox_id,omitempty"`
	NodeID             string                      `json:"node_id,omitempty"`
	NodeEpoch          uint64                      `json:"node_epoch,omitempty"`
	RegistryGeneration string                      `json:"registry_generation,omitempty"`
	BindingDigest      string                      `json:"binding_digest,omitempty"`
	FenceCompacted     bool                        `json:"fence_compacted,omitempty"`
	LastEventSeq       uint64                      `json:"last_event_seq,omitempty"`
	Proof              TerminalProof               `json:"proof,omitempty"`
	TerminalReason     string                      `json:"terminal_reason,omitempty"`
	FailureRevision    Revision                    `json:"failure_revision,omitempty"`
	PlacementFailure   *RoutePlacementFailureState `json:"placement_failure,omitempty"`
}

func (s RouteTombstoneState) Validate() error {
	if s.PlacementFailure != nil {
		if s.SandboxID != "" || s.NodeID != "" || s.NodeEpoch != 0 || s.LastEventSeq != 0 ||
			s.RegistryGeneration != "" || s.BindingDigest != "" || s.FenceCompacted || s.TerminalReason != "" ||
			s.Proof != (TerminalProof{}) || s.FailureRevision != (Revision{}) {
			return errors.New("cluster: placement-failure TOMBSTONE cannot contain an execution proof")
		}
		return s.PlacementFailure.Validate()
	}
	if s.SandboxID == "" || s.NodeID == "" || s.NodeEpoch == 0 || s.RegistryGeneration == "" ||
		!validDigest(s.BindingDigest) || s.LastEventSeq == 0 || s.TerminalReason == "" {
		return errors.New("cluster: incomplete TOMBSTONE")
	}
	if s.Proof.FencedNodeID != s.NodeID || s.Proof.FencedNodeEpoch != s.NodeEpoch {
		return errors.New("cluster: TOMBSTONE proof identifies another execution")
	}
	if err := s.Proof.Validate(); err != nil {
		return err
	}
	return s.FailureRevision.Validate()
}

type RoutePlacementFailureState struct {
	SandboxID            string               `json:"sandbox_id"`
	PlacementRound       uint64               `json:"placement_round"`
	CandidatePool        []PlacementCandidate `json:"candidate_pool"`
	DefinitivelyRejected []uint32             `json:"definitively_rejected"`
	Intent               DispatchIntent       `json:"intent"`
	Reason               string               `json:"reason"`
}

func (s RoutePlacementFailureState) Validate() error {
	if s.SandboxID == "" || s.PlacementRound == 0 || len(s.CandidatePool) == 0 || s.Reason == "" {
		return errors.New("cluster: incomplete placement-failure TOMBSTONE")
	}
	if err := validateCandidates(s.CandidatePool, nil, s.DefinitivelyRejected); err != nil {
		return err
	}
	if len(s.DefinitivelyRejected) != len(s.CandidatePool) {
		return errors.New("cluster: placement-failure TOMBSTONE requires an exhausted candidate pool")
	}
	return s.Intent.Validate()
}

type RouteWorkflowRecord struct {
	Group     string               `json:"group"`
	RouteKey  string               `json:"route_key"`
	State     RouteWorkflowState   `json:"state"`
	Revision  Revision             `json:"revision"`
	Starting  *RouteStartingState  `json:"starting,omitempty"`
	Ready     *ReadyRoute          `json:"ready,omitempty"`
	Paused    *PausedRouteState    `json:"paused,omitempty"`
	Resuming  *ResumingRouteState  `json:"resuming,omitempty"`
	Deleting  *DeletingRouteState  `json:"deleting,omitempty"`
	Tombstone *RouteTombstoneState `json:"tombstone,omitempty"`
}

func (r RouteWorkflowRecord) Validate() error {
	if r.Group == "" || r.RouteKey == "" {
		return errors.New("cluster: Route requires group and route key")
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if countPresent(r.Starting != nil, r.Ready != nil, r.Paused != nil, r.Resuming != nil, r.Deleting != nil, r.Tombstone != nil) != 1 {
		return errors.New("cluster: Route state union must contain exactly one value")
	}
	switch r.State {
	case WorkflowRouteStarting:
		if r.Starting == nil {
			return errors.New("cluster: missing STARTING state")
		}
		if err := r.Starting.Validate(); err != nil {
			return err
		}
		if r.Starting.Binding != nil {
			if err := r.Starting.Binding.ValidateWorkflow(ExecutionKindSandbox, r.Starting.SandboxID, r.Group, r.RouteKey, r.Starting.Intent); err != nil {
				return err
			}
			return validateProjectionRegistryGeneration(r.Revision, r.Starting.Binding.RegistryGeneration)
		}
		return nil
	case WorkflowRouteReady:
		if r.Ready == nil {
			return errors.New("cluster: missing READY state")
		}
		if err := r.Ready.Validate(); err != nil {
			return err
		}
		return validateProjectionRegistryGeneration(r.Revision, r.Ready.RegistryGeneration)
	case WorkflowRoutePaused:
		if r.Paused == nil {
			return errors.New("cluster: missing PAUSED state")
		}
		if err := r.Paused.Validate(); err != nil {
			return err
		}
		return validateProjectionRegistryGeneration(r.Revision, r.Paused.Execution.RegistryGeneration)
	case WorkflowRouteResuming:
		if r.Resuming == nil {
			return errors.New("cluster: missing RESUMING state")
		}
		if err := r.Resuming.Validate(); err != nil {
			return err
		}
		return validateProjectionRegistryGeneration(r.Revision, r.Resuming.Execution.RegistryGeneration)
	case WorkflowRouteDeleting:
		if r.Deleting == nil {
			return errors.New("cluster: missing DELETING state")
		}
		if err := r.Deleting.Validate(); err != nil {
			return err
		}
		return validateProjectionRegistryGeneration(r.Revision, r.Deleting.Execution.RegistryGeneration)
	case WorkflowRouteTombstone:
		if r.Tombstone == nil {
			return errors.New("cluster: missing TOMBSTONE state")
		}
		if err := r.Tombstone.Validate(); err != nil {
			return err
		}
		if r.Tombstone.PlacementFailure == nil {
			if err := validateProjectionRegistryGeneration(r.Revision, r.Tombstone.RegistryGeneration); err != nil {
				return err
			}
			return validateFailureRevision(r.Revision, r.Tombstone.FailureRevision)
		}
		return nil
	default:
		return errors.New("cluster: invalid Route state")
	}
}

type BuildStartingState struct {
	BuildID              string                  `json:"build_id"`
	CandidatePool        []PlacementCandidate    `json:"candidate_pool"`
	SelectedCandidate    *uint32                 `json:"selected_candidate,omitempty"`
	DefinitivelyRejected []uint32                `json:"definitively_rejected,omitempty"`
	Intent               DispatchIntent          `json:"intent"`
	Binding              *ExecutionBindingIntent `json:"binding,omitempty"`
	LastEventSeq         uint64                  `json:"last_event_seq,omitempty"`
}

func (s BuildStartingState) Validate() error {
	if s.BuildID == "" || len(s.CandidatePool) == 0 {
		return errors.New("cluster: BUILD_STARTING requires build ID and candidates")
	}
	if err := validateCandidates(s.CandidatePool, s.SelectedCandidate, s.DefinitivelyRejected); err != nil {
		return err
	}
	if err := s.Intent.Validate(); err != nil {
		return err
	}
	if s.SelectedCandidate != nil {
		if s.Binding == nil {
			return errors.New("cluster: selected BUILD_STARTING candidate requires a Binding")
		}
		if err := s.Binding.Validate(ExecutionKindBuild, s.BuildID); err != nil {
			return err
		}
		if s.CandidatePool[*s.SelectedCandidate].NodeID != s.Binding.NodeID {
			return errors.New("cluster: Build Binding does not match selected candidate")
		}
	} else if s.Binding != nil {
		return errors.New("cluster: unselected BUILD_STARTING workflow cannot have a Binding")
	}
	return nil
}

type BuildProjection struct {
	BuildID            string `json:"build_id"`
	NodeID             string `json:"node_id"`
	NodeEpoch          uint64 `json:"node_epoch"`
	RegistryGeneration string `json:"registry_generation"`
	BindingDigest      string `json:"binding_digest"`
	TemplateRef        string `json:"template_ref,omitempty"`
	ArtifactRef        string `json:"artifact_ref,omitempty"`
	Reason             string `json:"reason,omitempty"`
	LastEventSeq       uint64 `json:"last_event_seq"`
}

func (p BuildProjection) Validate() error {
	if p.BuildID == "" || p.NodeID == "" || p.NodeEpoch == 0 || p.RegistryGeneration == "" ||
		!validDigest(p.BindingDigest) || p.LastEventSeq == 0 {
		return errors.New("cluster: incomplete Build projection")
	}
	return nil
}

type BuildTombstoneState struct {
	Projection      BuildProjection `json:"projection"`
	Proof           TerminalProof   `json:"proof"`
	FailureRevision Revision        `json:"failure_revision"`
}

type BuildPlacementFailureState struct {
	BuildID              string               `json:"build_id"`
	CandidatePool        []PlacementCandidate `json:"candidate_pool"`
	DefinitivelyRejected []uint32             `json:"definitively_rejected"`
	Intent               DispatchIntent       `json:"intent"`
	Reason               string               `json:"reason"`
}

func (s BuildPlacementFailureState) Validate() error {
	if s.BuildID == "" || len(s.CandidatePool) == 0 || s.Reason == "" {
		return errors.New("cluster: incomplete Build placement failure")
	}
	if err := validateCandidates(s.CandidatePool, nil, s.DefinitivelyRejected); err != nil {
		return err
	}
	if len(s.DefinitivelyRejected) != len(s.CandidatePool) {
		return errors.New("cluster: Build placement failure requires an exhausted candidate pool")
	}
	return s.Intent.Validate()
}

type BuildRecord struct {
	Group      string                      `json:"group"`
	BuildID    string                      `json:"build_id"`
	State      BuildWorkflowState          `json:"state"`
	Revision   Revision                    `json:"revision"`
	Starting   *BuildStartingState         `json:"starting,omitempty"`
	Projection *BuildProjection            `json:"projection,omitempty"`
	Tombstone  *BuildTombstoneState        `json:"tombstone,omitempty"`
	Failure    *BuildPlacementFailureState `json:"failure,omitempty"`
}

func (r BuildRecord) Validate() error {
	if r.Group == "" || r.BuildID == "" {
		return errors.New("cluster: Build requires group and build ID")
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if countPresent(r.Starting != nil, r.Projection != nil, r.Tombstone != nil, r.Failure != nil) != 1 {
		return errors.New("cluster: Build state union must contain exactly one value")
	}
	switch r.State {
	case BuildStarting:
		if r.Starting == nil || r.Starting.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched BUILD_STARTING state")
		}
		if err := r.Starting.Validate(); err != nil {
			return err
		}
		if r.Starting.Binding != nil {
			if err := r.Starting.Binding.ValidateWorkflow(ExecutionKindBuild, r.BuildID, r.Group, "", r.Starting.Intent); err != nil {
				return err
			}
			return validateProjectionRegistryGeneration(r.Revision, r.Starting.Binding.RegistryGeneration)
		}
		return nil
	case BuildQueued, BuildRegistered, BuildBuilding, BuildReady:
		if r.Projection == nil || r.Projection.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched Build projection")
		}
		if err := r.Projection.Validate(); err != nil {
			return err
		}
		if err := validateProjectionRegistryGeneration(r.Revision, r.Projection.RegistryGeneration); err != nil {
			return err
		}
		if r.State == BuildReady && r.Projection.ArtifactRef == "" {
			return errors.New("cluster: BUILD_READY requires artifact reference")
		}
		return nil
	case BuildError:
		if r.Failure != nil {
			if r.Failure.BuildID != r.BuildID {
				return errors.New("cluster: mismatched Build placement failure")
			}
			return r.Failure.Validate()
		}
		if r.Projection == nil || r.Projection.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched BUILD_ERROR projection")
		}
		if err := r.Projection.Validate(); err != nil {
			return err
		}
		if err := validateProjectionRegistryGeneration(r.Revision, r.Projection.RegistryGeneration); err != nil {
			return err
		}
		if r.Projection.Reason == "" {
			return errors.New("cluster: BUILD_ERROR requires reason")
		}
		return nil
	case BuildTombstone:
		if r.Tombstone == nil || r.Tombstone.Projection.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched BUILD_TOMBSTONE state")
		}
		if err := r.Tombstone.Projection.Validate(); err != nil {
			return err
		}
		if err := validateProjectionRegistryGeneration(r.Revision, r.Tombstone.Projection.RegistryGeneration); err != nil {
			return err
		}
		if err := r.Tombstone.Proof.Validate(); err != nil {
			return err
		}
		if r.Tombstone.Proof.FencedNodeID != r.Tombstone.Projection.NodeID ||
			r.Tombstone.Proof.FencedNodeEpoch != r.Tombstone.Projection.NodeEpoch {
			return errors.New("cluster: BUILD_TOMBSTONE proof identifies another execution")
		}
		if err := r.Tombstone.FailureRevision.Validate(); err != nil {
			return err
		}
		return validateFailureRevision(r.Revision, r.Tombstone.FailureRevision)
	default:
		return errors.New("cluster: invalid Build state")
	}
}

type ExecutionFence struct {
	Group                string        `json:"group"`
	RouteKey             string        `json:"route_key"`
	SandboxID            string        `json:"sandbox_id"`
	NodeID               string        `json:"node_id"`
	NodeEpoch            uint64        `json:"node_epoch"`
	RegistryGeneration   string        `json:"registry_generation"`
	BindingDigest        string        `json:"binding_digest"`
	LastEventSeq         uint64        `json:"last_event_seq"`
	FinalOutboxWatermark uint64        `json:"final_outbox_watermark"`
	Proof                TerminalProof `json:"proof"`
	Revision             Revision      `json:"revision"`
}

func (f ExecutionFence) Validate() error {
	if f.Group == "" || f.RouteKey == "" || f.SandboxID == "" || f.NodeID == "" || f.NodeEpoch == 0 ||
		f.RegistryGeneration == "" || !validDigest(f.BindingDigest) || f.LastEventSeq == 0 {
		return errors.New("cluster: incomplete execution fence")
	}
	if f.Proof.FencedNodeID != f.NodeID || f.Proof.FencedNodeEpoch != f.NodeEpoch {
		return errors.New("cluster: execution fence proof identifies another execution")
	}
	if err := f.Proof.Validate(); err != nil {
		return err
	}
	if err := f.Revision.Validate(); err != nil {
		return err
	}
	if f.Revision.RegistryGeneration != f.RegistryGeneration {
		return errors.New("cluster: execution fence revision belongs to another Registry History Generation")
	}
	return nil
}

type FenceCompactionProof struct {
	TerminalProofCommitted     bool
	FinalOutboxWatermarkAcked  bool
	NodeEpochPermanentlyFenced bool
	AllReplicasApplied         bool
	MinimumRetentionElapsed    bool
}

func CanCompactExecutionFence(fence ExecutionFence, proof FenceCompactionProof) bool {
	outboxCovered := proof.FinalOutboxWatermarkAcked && fence.FinalOutboxWatermark >= fence.LastEventSeq
	return fence.Validate() == nil && proof.TerminalProofCommitted &&
		(outboxCovered || proof.NodeEpochPermanentlyFenced) &&
		proof.AllReplicasApplied && proof.MinimumRetentionElapsed
}

func validateCandidates(candidates []PlacementCandidate, selected *uint32, rejected []uint32) error {
	seenNodes := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if candidate.NodeID == "" {
			return errors.New("cluster: candidate node ID is required")
		}
		if _, exists := seenNodes[candidate.NodeID]; exists {
			return errors.New("cluster: duplicate placement candidate")
		}
		seenNodes[candidate.NodeID] = struct{}{}
	}
	rejectedSet := make(map[uint32]struct{}, len(rejected))
	for _, index := range rejected {
		if int(index) >= len(candidates) {
			return errors.New("cluster: rejected candidate index is out of range")
		}
		if _, exists := rejectedSet[index]; exists {
			return errors.New("cluster: duplicate rejected candidate index")
		}
		rejectedSet[index] = struct{}{}
	}
	if selected != nil {
		if int(*selected) >= len(candidates) {
			return errors.New("cluster: selected candidate index is out of range")
		}
		if _, rejected := rejectedSet[*selected]; rejected {
			return errors.New("cluster: selected candidate was definitively rejected")
		}
	}
	return nil
}

func validDigest(digest string) bool {
	if len(digest) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(digest)
	return err == nil && hex.EncodeToString(decoded) == digest
}

func digestMatches(digest string, value []byte) bool {
	if !validDigest(digest) {
		return false
	}
	want := sha256.Sum256(value)
	return digest == hex.EncodeToString(want[:])
}

func countPresent(values ...bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func validateProjectionRegistryGeneration(revision Revision, registryGeneration string) error {
	if registryGeneration != revision.RegistryGeneration {
		return errors.New("cluster: workflow projection belongs to another Registry History Generation")
	}
	return nil
}

func validateFailureRevision(current, failure Revision) error {
	if current.RegistryGeneration != failure.RegistryGeneration || current.ShardID != failure.ShardID || current.LogIndex < failure.LogIndex {
		return errors.New("cluster: workflow failure revision is outside the record history")
	}
	return nil
}
