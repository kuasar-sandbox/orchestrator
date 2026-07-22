package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/http/httpguts"
)

const (
	MaxDispatchSpecBytes               = 64 << 10
	MaxNormalizedDemandBytes           = 64 << 10
	MaxProviderPolicyVersionBytes      = 256
	MaxPlacementCandidates             = 4
	MaxPlacementCandidateMetadataBytes = 256
	MaxPlacementFailureReasonBytes     = 1024
	MaxTerminalReasonBytes             = 1024
)

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
	BuildRegistered BuildWorkflowState = "BUILD_REGISTERED"
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
	if len(demand) > MaxNormalizedDemandBytes {
		return DispatchIntent{}, fmt.Errorf("cluster: normalized demand exceeds %d bytes", MaxNormalizedDemandBytes)
	}
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
	if len(i.ProviderPolicyVersion) > MaxProviderPolicyVersionBytes || !utf8.ValidString(i.ProviderPolicyVersion) {
		return fmt.Errorf("cluster: provider/policy version exceeds %d bytes or is not valid UTF-8", MaxProviderPolicyVersionBytes)
	}
	if len(i.NormalizedDemand) > MaxNormalizedDemandBytes {
		return fmt.Errorf("cluster: normalized demand exceeds %d bytes", MaxNormalizedDemandBytes)
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
	if err := ValidateTCPDataEndpoint(i.DataEndpoint); err != nil {
		return err
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
	if err := validateSandboxDispatchIntent(s.Intent); err != nil {
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

	TemplateRef        string         `json:"template_ref"`
	SnapshotRef        string         `json:"snapshot_ref,omitempty"`
	RegistryGeneration string         `json:"registry_generation"`
	OpaqueBinding      string         `json:"opaque_binding"`
	BindingDigest      string         `json:"binding_digest"`
	LastEventSeq       uint64         `json:"last_event_seq"`
	Intent             DispatchIntent `json:"intent"`
}

func (r ReadyRoute) Validate() error {
	if r.SandboxID == "" || r.NodeID == "" || r.NodeEpoch == 0 || r.DataEndpoint == "" ||
		r.AccessToken == "" || r.TrafficAccessToken == "" || r.TemplateRef == "" ||
		r.RegistryGeneration == "" || r.LastEventSeq == 0 {
		return errors.New("cluster: incomplete READY forwarding projection")
	}
	if err := ValidateTCPDataEndpoint(r.DataEndpoint); err != nil {
		return err
	}
	for name, value := range map[string]string{
		"sandbox ID":                  r.SandboxID,
		"node ID":                     r.NodeID,
		"Registry History Generation": r.RegistryGeneration,
		"access token":                r.AccessToken,
		"Binding digest":              r.BindingDigest,
	} {
		if !utf8.ValidString(value) || !httpguts.ValidHeaderFieldValue(value) || strings.TrimSpace(value) != value {
			return fmt.Errorf("cluster: READY %s is not a canonical CONNECT header value", name)
		}
	}
	if err := validateSandboxDispatchIntent(r.Intent); err != nil {
		return fmt.Errorf("cluster: READY dispatch intent: %w", err)
	}
	if err := r.bindingIntent().Validate(ExecutionKindSandbox, r.SandboxID); err != nil {
		return fmt.Errorf("cluster: READY execution Binding: %w", err)
	}
	spec, err := ParseSandboxDispatchSpec(r.Intent.DispatchSpec)
	if err != nil {
		return fmt.Errorf("cluster: READY Sandbox dispatch spec: %w", err)
	}
	if r.TemplateRef != spec.TemplateRef || r.AccessToken != spec.AccessToken || r.TargetPort != spec.TargetPort {
		return errors.New("cluster: READY projection does not match immutable Sandbox dispatch spec")
	}
	if r.TargetPort < 0 || r.TargetPort > 65535 {
		return errors.New("cluster: invalid READY target port")
	}
	return nil
}

func (r ReadyRoute) ValidateWorkflow(group, routeKey string) error {
	if err := r.Validate(); err != nil {
		return err
	}
	return r.bindingIntent().ValidateWorkflow(ExecutionKindSandbox, r.SandboxID, group, routeKey, r.Intent)
}

func (r ReadyRoute) bindingIntent() ExecutionBindingIntent {
	return ExecutionBindingIntent{
		NodeID: r.NodeID, NodeEpoch: r.NodeEpoch, DataEndpoint: r.DataEndpoint,
		RegistryGeneration: r.RegistryGeneration, OpaqueBinding: r.OpaqueBinding, BindingDigest: r.BindingDigest,
	}
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
	return validateSandboxDispatchIntent(s.ResumeIntent)
}

type ResumingRouteState struct {
	Execution ReadyRoute     `json:"execution"`
	Intent    DispatchIntent `json:"intent"`
}

func (s ResumingRouteState) Validate() error {
	if err := s.Execution.Validate(); err != nil {
		return err
	}
	return validateSandboxDispatchIntent(s.Intent)
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
	ProofNodeTerminal        TerminalProofKind = "NODE_TERMINAL"
	ProofNewerNodeEpoch      TerminalProofKind = "NEWER_NODE_EPOCH"
	ProofExternalFence       TerminalProofKind = "EXTERNAL_FENCE"
	MaxWorkflowFinalizations                   = 256
)

type TerminalProof struct {
	Kind                  TerminalProofKind `json:"kind"`
	ProofDigest           string            `json:"proof_digest"`
	FencedNodeID          string            `json:"fenced_node_id"`
	FencedNodeEpoch       uint64            `json:"fenced_node_epoch"`
	ObservedNodeEpoch     uint64            `json:"observed_node_epoch,omitempty"`
	SystemEpoch           uint64            `json:"system_epoch,omitempty"`
	SystemCommitIndex     uint64            `json:"system_commit_index,omitempty"`
	EnrollmentID          string            `json:"enrollment_id,omitempty"`
	EnrollmentCommitIndex uint64            `json:"enrollment_commit_index,omitempty"`
}

func (p TerminalProof) Validate() error {
	if p.FencedNodeID == "" || p.FencedNodeEpoch == 0 || !validDigest(p.ProofDigest) {
		return errors.New("cluster: incomplete terminal/fencing proof")
	}
	switch p.Kind {
	case ProofNodeTerminal, ProofExternalFence:
		if p.ObservedNodeEpoch != 0 || p.SystemEpoch != 0 || p.SystemCommitIndex != 0 ||
			p.EnrollmentID != "" || p.EnrollmentCommitIndex != 0 {
			return errors.New("cluster: terminal/external proof cannot carry an observed NodeEpoch")
		}
		return nil
	case ProofNewerNodeEpoch:
		if p.ObservedNodeEpoch <= p.FencedNodeEpoch || p.SystemEpoch == 0 || p.SystemCommitIndex == 0 ||
			p.EnrollmentID == "" || p.EnrollmentCommitIndex == 0 || p.EnrollmentCommitIndex > p.SystemCommitIndex {
			return errors.New("cluster: newer-epoch proof does not advance NodeEpoch")
		}
		return nil
	default:
		return errors.New("cluster: invalid terminal/fencing proof kind")
	}
}

// WorkflowFinalizationIntent is the durable Registry retry intent for one
// exact node-local Admission/dedupe record. ObjectID remains the business ID.
type WorkflowFinalizationIntent struct {
	ObjectID           string         `json:"object_id"`
	NodeID             string         `json:"node_id"`
	NodeEpoch          uint64         `json:"node_epoch"`
	DataEndpoint       string         `json:"data_endpoint"`
	RegistryGeneration string         `json:"registry_generation"`
	OpaqueBinding      string         `json:"opaque_binding"`
	BindingDigest      string         `json:"binding_digest"`
	TerminalProof      *TerminalProof `json:"terminal_proof,omitempty"`
}

func (i WorkflowFinalizationIntent) Validate() error {
	if i.ObjectID == "" || i.NodeID == "" || i.NodeEpoch == 0 || i.DataEndpoint == "" ||
		i.RegistryGeneration == "" || i.OpaqueBinding == "" || !validDigest(i.BindingDigest) {
		return errors.New("cluster: incomplete workflow finalization intent")
	}
	if err := ValidateTCPDataEndpoint(i.DataEndpoint); err != nil {
		return err
	}
	binding, err := DecodeExecutionBinding(i.OpaqueBinding)
	if err != nil {
		return err
	}
	digest, err := ExecutionBindingDigest(i.OpaqueBinding)
	if err != nil {
		return err
	}
	if binding.ObjectID != i.ObjectID || binding.NodeID != i.NodeID || binding.NodeEpoch != i.NodeEpoch ||
		binding.RegistryGeneration != i.RegistryGeneration || digest != i.BindingDigest {
		return errors.New("cluster: workflow finalization does not match its execution Binding")
	}
	if i.TerminalProof != nil {
		if i.TerminalProof.Kind != ProofNodeTerminal || i.TerminalProof.FencedNodeID != i.NodeID ||
			i.TerminalProof.FencedNodeEpoch != i.NodeEpoch {
			return errors.New("cluster: workflow finalization proof identifies another execution")
		}
		return i.TerminalProof.Validate()
	}
	return nil
}

func (i WorkflowFinalizationIntent) ValidateFor(kind ExecutionKind, group, routeKey string) error {
	if err := i.Validate(); err != nil {
		return err
	}
	binding, err := DecodeExecutionBinding(i.OpaqueBinding)
	if err != nil {
		return err
	}
	if binding.Kind != kind || binding.Group != group || binding.RouteKey != routeKey {
		return errors.New("cluster: workflow finalization belongs to another workflow")
	}
	return nil
}

func NewWorkflowFinalizationIntent(
	objectID string,
	binding ExecutionBindingIntent,
	proof *TerminalProof,
) (WorkflowFinalizationIntent, error) {
	intent := WorkflowFinalizationIntent{
		ObjectID: objectID, NodeID: binding.NodeID, NodeEpoch: binding.NodeEpoch,
		DataEndpoint: binding.DataEndpoint, RegistryGeneration: binding.RegistryGeneration,
		OpaqueBinding: binding.OpaqueBinding, BindingDigest: binding.BindingDigest,
	}
	if proof != nil {
		copy := *proof
		intent.TerminalProof = &copy
	}
	return intent, intent.Validate()
}

func (i WorkflowFinalizationIntent) MatchesBuild(projection BuildProjection) bool {
	return i.ObjectID == projection.BuildID && i.NodeID == projection.NodeID &&
		i.NodeEpoch == projection.NodeEpoch && i.DataEndpoint == projection.DataEndpoint &&
		i.RegistryGeneration == projection.RegistryGeneration && i.BindingDigest == projection.BindingDigest
}

func validateWorkflowFinalizations(intents []WorkflowFinalizationIntent) error {
	if len(intents) > MaxWorkflowFinalizations {
		return errors.New("cluster: too many pending workflow finalizations")
	}
	seen := make(map[string]struct{}, len(intents))
	for _, intent := range intents {
		if err := intent.Validate(); err != nil {
			return err
		}
		if _, duplicate := seen[intent.BindingDigest]; duplicate {
			return errors.New("cluster: duplicate workflow finalization Binding")
		}
		seen[intent.BindingDigest] = struct{}{}
	}
	return nil
}

type RouteTombstoneState struct {
	SandboxID          string                      `json:"sandbox_id,omitempty"`
	NodeID             string                      `json:"node_id,omitempty"`
	NodeEpoch          uint64                      `json:"node_epoch,omitempty"`
	RegistryGeneration string                      `json:"registry_generation,omitempty"`
	BindingDigest      string                      `json:"binding_digest,omitempty"`
	LastEventSeq       uint64                      `json:"last_event_seq,omitempty"`
	Proof              TerminalProof               `json:"proof,omitempty"`
	TerminalReason     string                      `json:"terminal_reason,omitempty"`
	FailureRevision    Revision                    `json:"failure_revision,omitempty"`
	PlacementFailure   *RoutePlacementFailureState `json:"placement_failure,omitempty"`
}

func (s RouteTombstoneState) Validate() error {
	if s.PlacementFailure != nil {
		if s.SandboxID != "" || s.NodeID != "" || s.NodeEpoch != 0 || s.LastEventSeq != 0 ||
			s.RegistryGeneration != "" || s.BindingDigest != "" || s.TerminalReason != "" ||
			s.Proof != (TerminalProof{}) || s.FailureRevision != (Revision{}) {
			return errors.New("cluster: placement-failure TOMBSTONE cannot contain an execution proof")
		}
		return s.PlacementFailure.Validate()
	}
	if s.SandboxID == "" || s.NodeID == "" || s.NodeEpoch == 0 || s.RegistryGeneration == "" ||
		!validDigest(s.BindingDigest) || s.TerminalReason == "" ||
		s.LastEventSeq == 0 && s.Proof.Kind == ProofNodeTerminal {
		return errors.New("cluster: incomplete TOMBSTONE")
	}
	if len(s.TerminalReason) > MaxTerminalReasonBytes || !utf8.ValidString(s.TerminalReason) {
		return fmt.Errorf("cluster: terminal reason exceeds %d bytes or is not valid UTF-8", MaxTerminalReasonBytes)
	}
	if s.Proof.FencedNodeID != s.NodeID || s.Proof.FencedNodeEpoch != s.NodeEpoch {
		return errors.New("cluster: TOMBSTONE proof identifies another execution")
	}
	if err := s.Proof.Validate(); err != nil {
		return err
	}
	switch s.Proof.Kind {
	case ProofNodeTerminal:
		digest, err := NodeTerminalProofDigest(
			s.Proof, s.RegistryGeneration, s.SandboxID, s.BindingDigest, s.LastEventSeq,
		)
		if err != nil || digest != s.Proof.ProofDigest {
			return errors.New("cluster: TOMBSTONE node-terminal proof digest mismatch")
		}
	case ProofNewerNodeEpoch:
		digest, err := NewerNodeEpochProofDigest(
			s.Proof, s.FailureRevision.RegistryGeneration, s.SandboxID, s.BindingDigest, s.LastEventSeq,
		)
		if err != nil || digest != s.Proof.ProofDigest {
			return errors.New("cluster: TOMBSTONE newer-NodeEpoch proof digest mismatch")
		}
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
	if len(s.Reason) > MaxPlacementFailureReasonBytes || !utf8.ValidString(s.Reason) {
		return fmt.Errorf("cluster: placement failure reason exceeds %d bytes or is not valid UTF-8", MaxPlacementFailureReasonBytes)
	}
	if err := validateCandidates(s.CandidatePool, nil, s.DefinitivelyRejected); err != nil {
		return err
	}
	if len(s.DefinitivelyRejected) != len(s.CandidatePool) {
		return errors.New("cluster: placement-failure TOMBSTONE requires an exhausted candidate pool")
	}
	return validateSandboxDispatchIntent(s.Intent)
}

type RouteWorkflowRecord struct {
	Group         string                       `json:"group"`
	RouteKey      string                       `json:"route_key"`
	State         RouteWorkflowState           `json:"state"`
	Revision      Revision                     `json:"revision"`
	Starting      *RouteStartingState          `json:"starting,omitempty"`
	Ready         *ReadyRoute                  `json:"ready,omitempty"`
	Paused        *PausedRouteState            `json:"paused,omitempty"`
	Resuming      *ResumingRouteState          `json:"resuming,omitempty"`
	Deleting      *DeletingRouteState          `json:"deleting,omitempty"`
	Tombstone     *RouteTombstoneState         `json:"tombstone,omitempty"`
	Finalizations []WorkflowFinalizationIntent `json:"pending_finalizations"`
}

func (r RouteWorkflowRecord) Validate() error {
	if r.Group == "" || r.RouteKey == "" {
		return errors.New("cluster: Route requires group and route key")
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if err := validateWorkflowFinalizations(r.Finalizations); err != nil {
		return err
	}
	for _, intent := range r.Finalizations {
		if err := intent.ValidateFor(ExecutionKindSandbox, r.Group, r.RouteKey); err != nil ||
			intent.RegistryGeneration != r.Revision.RegistryGeneration || intent.TerminalProof != nil {
			return errors.New("cluster: invalid Route workflow finalization intent")
		}
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
		if err := r.Ready.ValidateWorkflow(r.Group, r.RouteKey); err != nil {
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
		if err := r.Paused.Execution.ValidateWorkflow(r.Group, r.RouteKey); err != nil {
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
		if err := r.Resuming.Execution.ValidateWorkflow(r.Group, r.RouteKey); err != nil {
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
		if err := r.Deleting.Execution.ValidateWorkflow(r.Group, r.RouteKey); err != nil {
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
}

func (s BuildStartingState) Validate() error {
	if s.BuildID == "" || len(s.CandidatePool) == 0 {
		return errors.New("cluster: BUILD_STARTING requires build ID and candidates")
	}
	if err := validateCandidates(s.CandidatePool, s.SelectedCandidate, s.DefinitivelyRejected); err != nil {
		return err
	}
	if err := validateBuildDispatchIntent(s.Intent); err != nil {
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
	BuildID            string         `json:"build_id"`
	NodeID             string         `json:"node_id"`
	NodeEpoch          uint64         `json:"node_epoch"`
	DataEndpoint       string         `json:"data_endpoint"`
	RegistryGeneration string         `json:"registry_generation"`
	OpaqueBinding      string         `json:"opaque_binding"`
	BindingDigest      string         `json:"binding_digest"`
	Intent             DispatchIntent `json:"intent"`
	TemplateRef        string         `json:"template_ref,omitempty"`
}

func (p BuildProjection) Validate() error {
	if p.BuildID == "" || p.NodeID == "" || p.NodeEpoch == 0 || p.DataEndpoint == "" || p.RegistryGeneration == "" ||
		p.TemplateRef == "" || !validDigest(p.BindingDigest) {
		return errors.New("cluster: incomplete Build registration projection")
	}
	if err := ValidateTCPDataEndpoint(p.DataEndpoint); err != nil {
		return err
	}
	if err := validateBuildDispatchIntent(p.Intent); err != nil {
		return err
	}
	if err := p.bindingIntent().Validate(ExecutionKindBuild, p.BuildID); err != nil {
		return fmt.Errorf("cluster: Build projection execution Binding: %w", err)
	}
	spec, err := ParseBuildDispatchSpec(p.Intent.DispatchSpec)
	if err != nil {
		return fmt.Errorf("cluster: Build dispatch spec: %w", err)
	}
	if p.TemplateRef != spec.TemplateID {
		return errors.New("cluster: Build registration does not match immutable dispatch spec")
	}
	return nil
}

func (p BuildProjection) ValidateWorkflow(group string) error {
	if err := p.Validate(); err != nil {
		return err
	}
	return p.bindingIntent().ValidateWorkflow(ExecutionKindBuild, p.BuildID, group, "", p.Intent)
}

func (p BuildProjection) bindingIntent() ExecutionBindingIntent {
	return ExecutionBindingIntent{
		NodeID: p.NodeID, NodeEpoch: p.NodeEpoch, DataEndpoint: p.DataEndpoint,
		RegistryGeneration: p.RegistryGeneration, OpaqueBinding: p.OpaqueBinding, BindingDigest: p.BindingDigest,
	}
}

type BuildTombstoneState struct {
	PlacementFailure BuildPlacementFailureState `json:"placement_failure"`
}

func (s BuildTombstoneState) Validate() error {
	return s.PlacementFailure.Validate()
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
	if len(s.Reason) > MaxPlacementFailureReasonBytes || !utf8.ValidString(s.Reason) {
		return fmt.Errorf("cluster: Build placement failure reason exceeds %d bytes or is not valid UTF-8", MaxPlacementFailureReasonBytes)
	}
	if err := validateCandidates(s.CandidatePool, nil, s.DefinitivelyRejected); err != nil {
		return err
	}
	if len(s.DefinitivelyRejected) != len(s.CandidatePool) {
		return errors.New("cluster: Build placement failure requires an exhausted candidate pool")
	}
	return validateBuildDispatchIntent(s.Intent)
}

func validateSandboxDispatchIntent(intent DispatchIntent) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	_, err := ParseSandboxDispatchSpec(intent.DispatchSpec)
	return err
}

func validateBuildDispatchIntent(intent DispatchIntent) error {
	if err := intent.Validate(); err != nil {
		return err
	}
	_, err := ParseBuildDispatchSpec(intent.DispatchSpec)
	return err
}

type BuildRecord struct {
	Group         string                       `json:"group"`
	BuildID       string                       `json:"build_id"`
	State         BuildWorkflowState           `json:"state"`
	Revision      Revision                     `json:"revision"`
	Starting      *BuildStartingState          `json:"starting,omitempty"`
	Projection    *BuildProjection             `json:"projection,omitempty"`
	Tombstone     *BuildTombstoneState         `json:"tombstone,omitempty"`
	Finalizations []WorkflowFinalizationIntent `json:"pending_finalizations"`
}

func (r BuildRecord) Validate() error {
	if r.Group == "" || r.BuildID == "" {
		return errors.New("cluster: Build requires group and build ID")
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if err := validateWorkflowFinalizations(r.Finalizations); err != nil {
		return err
	}
	for _, intent := range r.Finalizations {
		if err := intent.ValidateFor(ExecutionKindBuild, r.Group, ""); err != nil || intent.ObjectID != r.BuildID ||
			intent.RegistryGeneration != r.Revision.RegistryGeneration || intent.TerminalProof != nil {
			return errors.New("cluster: invalid Build registration finalization")
		}
	}
	if countPresent(r.Starting != nil, r.Projection != nil, r.Tombstone != nil) != 1 {
		return errors.New("cluster: Build registration state union must contain exactly one value")
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
	case BuildRegistered:
		if r.Projection == nil || r.Projection.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched Build registration projection")
		}
		if err := r.Projection.ValidateWorkflow(r.Group); err != nil {
			return err
		}
		if err := validateProjectionRegistryGeneration(r.Revision, r.Projection.RegistryGeneration); err != nil {
			return err
		}
		return nil
	case BuildTombstone:
		if r.Tombstone == nil {
			return errors.New("cluster: missing or mismatched BUILD_TOMBSTONE state")
		}
		if err := r.Tombstone.Validate(); err != nil {
			return err
		}
		if r.Tombstone.PlacementFailure.BuildID != r.BuildID {
			return errors.New("cluster: mismatched BUILD_TOMBSTONE placement failure")
		}
		return nil
	default:
		return errors.New("cluster: invalid Build state")
	}
}

type ExecutionFence struct {
	Group                  string                      `json:"group"`
	RouteKey               string                      `json:"route_key"`
	SandboxID              string                      `json:"sandbox_id"`
	NodeID                 string                      `json:"node_id,omitempty"`
	NodeEpoch              uint64                      `json:"node_epoch,omitempty"`
	RegistryGeneration     string                      `json:"registry_generation"`
	BindingDigest          string                      `json:"binding_digest,omitempty"`
	LastEventSeq           uint64                      `json:"last_event_seq,omitempty"`
	FinalOutboxWatermark   uint64                      `json:"final_outbox_watermark,omitempty"`
	Proof                  TerminalProof               `json:"proof,omitempty"`
	PlacementFailure       *RoutePlacementFailureState `json:"placement_failure,omitempty"`
	PlacementFailureDigest string                      `json:"placement_failure_digest,omitempty"`
	Revision               Revision                    `json:"revision"`
}

func (f ExecutionFence) Validate() error {
	if f.Group == "" || f.RouteKey == "" || f.SandboxID == "" || f.RegistryGeneration == "" {
		return errors.New("cluster: incomplete execution fence")
	}
	if err := f.Revision.Validate(); err != nil {
		return err
	}
	if f.Revision.RegistryGeneration != f.RegistryGeneration {
		return errors.New("cluster: execution fence revision belongs to another Registry History Generation")
	}
	if f.PlacementFailure != nil {
		if f.NodeID != "" || f.NodeEpoch != 0 || f.BindingDigest != "" || f.LastEventSeq != 0 ||
			f.FinalOutboxWatermark != 0 || f.Proof != (TerminalProof{}) ||
			f.PlacementFailure.SandboxID != f.SandboxID {
			return errors.New("cluster: placement fence contains execution identity")
		}
		if err := f.PlacementFailure.Validate(); err != nil {
			return err
		}
		digest, err := PlacementFailureProofDigest(
			f.Group, f.RouteKey, f.RegistryGeneration, *f.PlacementFailure,
		)
		if err != nil || digest != f.PlacementFailureDigest {
			return errors.New("cluster: placement-failure fence proof digest mismatch")
		}
		return nil
	}
	if f.PlacementFailureDigest != "" || f.NodeID == "" || f.NodeEpoch == 0 ||
		!validDigest(f.BindingDigest) || f.LastEventSeq == 0 && f.Proof.Kind == ProofNodeTerminal {
		return errors.New("cluster: incomplete execution fence")
	}
	if f.Proof.FencedNodeID != f.NodeID || f.Proof.FencedNodeEpoch != f.NodeEpoch {
		return errors.New("cluster: execution fence proof identifies another execution")
	}
	if f.FinalOutboxWatermark < f.LastEventSeq {
		return errors.New("cluster: execution fence final outbox watermark does not cover the last event")
	}
	if err := f.Proof.Validate(); err != nil {
		return err
	}
	switch f.Proof.Kind {
	case ProofNodeTerminal:
		digest, err := NodeTerminalProofDigest(
			f.Proof, f.RegistryGeneration, f.SandboxID, f.BindingDigest, f.LastEventSeq,
		)
		if err != nil || digest != f.Proof.ProofDigest {
			return errors.New("cluster: execution-fence node-terminal proof digest mismatch")
		}
	case ProofNewerNodeEpoch:
		digest, err := NewerNodeEpochProofDigest(
			f.Proof, f.RegistryGeneration, f.SandboxID, f.BindingDigest, f.LastEventSeq,
		)
		if err != nil || digest != f.Proof.ProofDigest {
			return errors.New("cluster: execution-fence newer-NodeEpoch proof digest mismatch")
		}
	}
	return nil
}

func NodeTerminalProofDigest(
	proof TerminalProof,
	registryGeneration string,
	sandboxID string,
	bindingDigest string,
	lastEventSeq uint64,
) (string, error) {
	if proof.Kind != ProofNodeTerminal || proof.FencedNodeID == "" || proof.FencedNodeEpoch == 0 ||
		proof.ObservedNodeEpoch != 0 || proof.SystemEpoch != 0 || proof.SystemCommitIndex != 0 ||
		proof.EnrollmentID != "" || proof.EnrollmentCommitIndex != 0 || registryGeneration == "" ||
		sandboxID == "" || !validDigest(bindingDigest) || lastEventSeq == 0 {
		return "", errors.New("cluster: incomplete node-terminal proof evidence")
	}
	value := struct {
		RegistryGeneration string `json:"registry_generation"`
		SandboxID          string `json:"sandbox_id"`
		BindingDigest      string `json:"binding_digest"`
		LastEventSeq       uint64 `json:"last_event_seq"`
		FencedNodeID       string `json:"fenced_node_id"`
		FencedNodeEpoch    uint64 `json:"fenced_node_epoch"`
	}{
		RegistryGeneration: registryGeneration, SandboxID: sandboxID, BindingDigest: bindingDigest,
		LastEventSeq: lastEventSeq, FencedNodeID: proof.FencedNodeID, FencedNodeEpoch: proof.FencedNodeEpoch,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("kuasar-node-terminal-proof-v1\x00"), raw...))
	return hex.EncodeToString(digest[:]), nil
}

func NewerNodeEpochProofDigest(
	proof TerminalProof,
	registryGeneration string,
	sandboxID string,
	bindingDigest string,
	lastEventSeq uint64,
) (string, error) {
	if proof.Kind != ProofNewerNodeEpoch || proof.FencedNodeID == "" || proof.FencedNodeEpoch == 0 ||
		proof.ObservedNodeEpoch <= proof.FencedNodeEpoch || proof.SystemEpoch == 0 || proof.SystemCommitIndex == 0 ||
		proof.EnrollmentID == "" || proof.EnrollmentCommitIndex == 0 ||
		proof.EnrollmentCommitIndex > proof.SystemCommitIndex || registryGeneration == "" || sandboxID == "" ||
		!validDigest(bindingDigest) {
		return "", errors.New("cluster: incomplete newer-NodeEpoch proof evidence")
	}
	value := struct {
		RegistryGeneration    string `json:"registry_generation"`
		SandboxID             string `json:"sandbox_id"`
		BindingDigest         string `json:"binding_digest"`
		LastEventSeq          uint64 `json:"last_event_seq"`
		FencedNodeID          string `json:"fenced_node_id"`
		FencedNodeEpoch       uint64 `json:"fenced_node_epoch"`
		ObservedNodeEpoch     uint64 `json:"observed_node_epoch"`
		SystemEpoch           uint64 `json:"system_epoch"`
		SystemCommitIndex     uint64 `json:"system_commit_index"`
		EnrollmentID          string `json:"enrollment_id"`
		EnrollmentCommitIndex uint64 `json:"enrollment_commit_index"`
	}{
		RegistryGeneration: registryGeneration, SandboxID: sandboxID, BindingDigest: bindingDigest,
		LastEventSeq: lastEventSeq, FencedNodeID: proof.FencedNodeID,
		FencedNodeEpoch: proof.FencedNodeEpoch, ObservedNodeEpoch: proof.ObservedNodeEpoch,
		SystemEpoch: proof.SystemEpoch, SystemCommitIndex: proof.SystemCommitIndex,
		EnrollmentID: proof.EnrollmentID, EnrollmentCommitIndex: proof.EnrollmentCommitIndex,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("kuasar-newer-node-epoch-proof-v1\x00"), raw...))
	return hex.EncodeToString(digest[:]), nil
}

func NewPlacementFailureFence(group, routeKey, registryGeneration string, failure RoutePlacementFailureState) (ExecutionFence, error) {
	if group == "" || routeKey == "" || registryGeneration == "" {
		return ExecutionFence{}, errors.New("cluster: incomplete placement-failure fence identity")
	}
	if err := failure.Validate(); err != nil {
		return ExecutionFence{}, err
	}
	digest, err := PlacementFailureProofDigest(group, routeKey, registryGeneration, failure)
	if err != nil {
		return ExecutionFence{}, err
	}
	copyFailure := failure
	copyFailure.CandidatePool = append([]PlacementCandidate(nil), failure.CandidatePool...)
	copyFailure.DefinitivelyRejected = append([]uint32(nil), failure.DefinitivelyRejected...)
	copyFailure.Intent.NormalizedDemand = append([]byte(nil), failure.Intent.NormalizedDemand...)
	copyFailure.Intent.DispatchSpec = append([]byte(nil), failure.Intent.DispatchSpec...)
	return ExecutionFence{
		Group: group, RouteKey: routeKey, SandboxID: failure.SandboxID,
		RegistryGeneration: registryGeneration, PlacementFailure: &copyFailure,
		PlacementFailureDigest: digest,
	}, nil
}

func PlacementFailureProofDigest(
	group, routeKey, registryGeneration string,
	failure RoutePlacementFailureState,
) (string, error) {
	if group == "" || routeKey == "" || registryGeneration == "" {
		return "", errors.New("cluster: incomplete placement-failure proof identity")
	}
	if err := failure.Validate(); err != nil {
		return "", err
	}
	value := struct {
		Group              string                     `json:"group"`
		RouteKey           string                     `json:"route_key"`
		RegistryGeneration string                     `json:"registry_generation"`
		Failure            RoutePlacementFailureState `json:"failure"`
	}{
		Group: group, RouteKey: routeKey, RegistryGeneration: registryGeneration, Failure: failure,
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("kuasar-placement-failure-proof-v1\x00"), raw...))
	return hex.EncodeToString(digest[:]), nil
}

func (f ExecutionFence) ProofDigest() string {
	if f.PlacementFailure != nil {
		return f.PlacementFailureDigest
	}
	return f.Proof.ProofDigest
}

func validateCandidates(candidates []PlacementCandidate, selected *uint32, rejected []uint32) error {
	if len(candidates) == 0 || len(candidates) > MaxPlacementCandidates {
		return fmt.Errorf("cluster: placement candidate count must be between 1 and %d", MaxPlacementCandidates)
	}
	if len(rejected) > len(candidates) {
		return errors.New("cluster: rejected candidate count exceeds the candidate pool")
	}
	seenNodes := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if err := ValidateExecutionBindingNodeID(candidate.NodeID); err != nil {
			return fmt.Errorf("cluster: invalid placement candidate node ID: %w", err)
		}
		for name, value := range map[string]string{
			"failure domain": candidate.FailureDomain,
			"runtime digest": candidate.RuntimeDigest,
		} {
			if !utf8.ValidString(value) || len(value) > MaxPlacementCandidateMetadataBytes {
				return fmt.Errorf("cluster: placement candidate %s is invalid or exceeds %d bytes", name, MaxPlacementCandidateMetadataBytes)
			}
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
