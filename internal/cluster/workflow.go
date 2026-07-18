package cluster

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

const MaxDispatchSpecBytes = 64 << 10

type Revision struct {
	StorageGeneration string `json:"storage_generation"`
	ShardID           uint32 `json:"shard_id"`
	LogIndex          uint64 `json:"log_index"`
}

func (r Revision) Validate() error {
	if r.StorageGeneration == "" || r.LogIndex == 0 {
		return errors.New("cluster: revision requires storage generation and committed log index")
	}
	return nil
}

func (r Revision) AtLeast(min Revision) bool {
	return r.StorageGeneration == min.StorageGeneration && r.ShardID == min.ShardID && r.LogIndex >= min.LogIndex
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
	NodeID            string `json:"node_id"`
	NodeEpoch         uint64 `json:"node_epoch"`
	StorageGeneration string `json:"storage_generation"`
	OpaqueBinding     string `json:"opaque_binding"`
	BindingDigest     string `json:"binding_digest"`
}

func (i ExecutionBindingIntent) Validate(kind ExecutionKind, objectID string) error {
	if i.NodeID == "" || i.NodeEpoch == 0 || i.StorageGeneration == "" || i.OpaqueBinding == "" || i.BindingDigest == "" {
		return errors.New("cluster: incomplete execution Binding intent")
	}
	binding, err := DecodeExecutionBinding(i.OpaqueBinding)
	if err != nil {
		return err
	}
	if binding.Kind != kind || binding.ObjectID != objectID || binding.NodeID != i.NodeID ||
		binding.NodeEpoch != i.NodeEpoch || binding.StorageGeneration != i.StorageGeneration {
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

	TemplateRef       string `json:"template_ref"`
	SnapshotRef       string `json:"snapshot_ref,omitempty"`
	StorageGeneration string `json:"storage_generation"`
	BindingDigest     string `json:"binding_digest"`
	LastEventSeq      uint64 `json:"last_event_seq"`
}

func (r ReadyRoute) Validate() error {
	if r.SandboxID == "" || r.NodeID == "" || r.NodeEpoch == 0 || r.DataEndpoint == "" ||
		r.TemplateRef == "" || r.StorageGeneration == "" || r.LastEventSeq == 0 {
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
	SandboxID       string        `json:"sandbox_id"`
	NodeID          string        `json:"node_id"`
	NodeEpoch       uint64        `json:"node_epoch"`
	LastEventSeq    uint64        `json:"last_event_seq"`
	Proof           TerminalProof `json:"proof"`
	TerminalReason  string        `json:"terminal_reason"`
	FailureRevision Revision      `json:"failure_revision"`
}

func (s RouteTombstoneState) Validate() error {
	if s.SandboxID == "" || s.NodeID == "" || s.NodeEpoch == 0 || s.LastEventSeq == 0 || s.TerminalReason == "" {
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
		return r.Starting.Validate()
	case WorkflowRouteReady:
		if r.Ready == nil {
			return errors.New("cluster: missing READY state")
		}
		return r.Ready.Validate()
	case WorkflowRoutePaused:
		if r.Paused == nil {
			return errors.New("cluster: missing PAUSED state")
		}
		return r.Paused.Validate()
	case WorkflowRouteResuming:
		if r.Resuming == nil {
			return errors.New("cluster: missing RESUMING state")
		}
		return r.Resuming.Validate()
	case WorkflowRouteDeleting:
		if r.Deleting == nil {
			return errors.New("cluster: missing DELETING state")
		}
		return r.Deleting.Validate()
	case WorkflowRouteTombstone:
		if r.Tombstone == nil {
			return errors.New("cluster: missing TOMBSTONE state")
		}
		return r.Tombstone.Validate()
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
	BuildID           string `json:"build_id"`
	NodeID            string `json:"node_id"`
	NodeEpoch         uint64 `json:"node_epoch"`
	StorageGeneration string `json:"storage_generation"`
	BindingDigest     string `json:"binding_digest"`
	TemplateRef       string `json:"template_ref,omitempty"`
	ArtifactRef       string `json:"artifact_ref,omitempty"`
	Reason            string `json:"reason,omitempty"`
	LastEventSeq      uint64 `json:"last_event_seq"`
}

func (p BuildProjection) Validate() error {
	if p.BuildID == "" || p.NodeID == "" || p.NodeEpoch == 0 || p.StorageGeneration == "" ||
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

type BuildRecord struct {
	Group      string               `json:"group"`
	BuildID    string               `json:"build_id"`
	State      BuildWorkflowState   `json:"state"`
	Revision   Revision             `json:"revision"`
	Starting   *BuildStartingState  `json:"starting,omitempty"`
	Projection *BuildProjection     `json:"projection,omitempty"`
	Tombstone  *BuildTombstoneState `json:"tombstone,omitempty"`
}

func (r BuildRecord) Validate() error {
	if r.Group == "" || r.BuildID == "" {
		return errors.New("cluster: Build requires group and build ID")
	}
	if err := r.Revision.Validate(); err != nil {
		return err
	}
	if countPresent(r.Starting != nil, r.Projection != nil, r.Tombstone != nil) != 1 {
		return errors.New("cluster: Build state union must contain exactly one value")
	}
	switch r.State {
	case BuildStarting:
		if r.Starting == nil || r.Starting.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched BUILD_STARTING state")
		}
		return r.Starting.Validate()
	case BuildQueued, BuildRegistered, BuildBuilding, BuildReady, BuildError:
		if r.Projection == nil || r.Projection.BuildID != r.BuildID {
			return errors.New("cluster: missing or mismatched Build projection")
		}
		if err := r.Projection.Validate(); err != nil {
			return err
		}
		if r.State == BuildReady && r.Projection.ArtifactRef == "" {
			return errors.New("cluster: BUILD_READY requires artifact reference")
		}
		if r.State == BuildError && r.Projection.Reason == "" {
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
		if err := r.Tombstone.Proof.Validate(); err != nil {
			return err
		}
		if r.Tombstone.Proof.FencedNodeID != r.Tombstone.Projection.NodeID ||
			r.Tombstone.Proof.FencedNodeEpoch != r.Tombstone.Projection.NodeEpoch {
			return errors.New("cluster: BUILD_TOMBSTONE proof identifies another execution")
		}
		return r.Tombstone.FailureRevision.Validate()
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
	StorageGeneration    string        `json:"storage_generation"`
	BindingDigest        string        `json:"binding_digest"`
	LastEventSeq         uint64        `json:"last_event_seq"`
	FinalOutboxWatermark uint64        `json:"final_outbox_watermark"`
	Proof                TerminalProof `json:"proof"`
	Revision             Revision      `json:"revision"`
}

func (f ExecutionFence) Validate() error {
	if f.Group == "" || f.RouteKey == "" || f.SandboxID == "" || f.NodeID == "" || f.NodeEpoch == 0 ||
		f.StorageGeneration == "" || !validDigest(f.BindingDigest) || f.LastEventSeq == 0 {
		return errors.New("cluster: incomplete execution fence")
	}
	if f.Proof.FencedNodeID != f.NodeID || f.Proof.FencedNodeEpoch != f.NodeEpoch {
		return errors.New("cluster: execution fence proof identifies another execution")
	}
	if err := f.Proof.Validate(); err != nil {
		return err
	}
	return f.Revision.Validate()
}

type FenceCompactionProof struct {
	TerminalProofCommitted     bool
	FinalOutboxWatermarkAcked  bool
	NodeEpochPermanentlyFenced bool
	AllReplicasApplied         bool
	MinimumRetentionElapsed    bool
}

func CanCompactExecutionFence(fence ExecutionFence, proof FenceCompactionProof) bool {
	return fence.Validate() == nil && proof.TerminalProofCommitted &&
		(proof.FinalOutboxWatermarkAcked || proof.NodeEpochPermanentlyFenced) &&
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
