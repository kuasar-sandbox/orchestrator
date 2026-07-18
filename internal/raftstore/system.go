package raftstore

import (
	"errors"
	"fmt"
)

type RecoveryPhase string

const (
	RecoveryClosed      RecoveryPhase = "CLOSED"
	RecoveryPreparing   RecoveryPhase = "PREPARING"
	RecoveryCollecting  RecoveryPhase = "COLLECTING"
	RecoveryReconciling RecoveryPhase = "RECONCILING"
	RecoveryFinalizing  RecoveryPhase = "FINALIZING"
)

type RecoveryEpoch struct {
	Epoch                   uint64        `json:"epoch"`
	SourceClusterID         string        `json:"source_cluster_id"`
	SourceStorageGeneration string        `json:"source_storage_generation"`
	SourceManifestDigest    string        `json:"source_manifest_digest"`
	TargetStorageGeneration string        `json:"target_storage_generation"`
	TargetManifestDigest    string        `json:"target_manifest_digest"`
	Phase                   RecoveryPhase `json:"phase"`
}

func (r RecoveryEpoch) Validate() error {
	if r.Epoch == 0 || r.SourceClusterID == "" || r.SourceStorageGeneration == "" ||
		r.TargetStorageGeneration == "" || r.SourceStorageGeneration == r.TargetStorageGeneration ||
		!isSHA256(r.SourceManifestDigest) || !isSHA256(r.TargetManifestDigest) {
		return errors.New("raftstore: incomplete recovery epoch")
	}
	switch r.Phase {
	case RecoveryPreparing, RecoveryCollecting, RecoveryReconciling, RecoveryFinalizing:
		return nil
	default:
		return errors.New("raftstore: invalid open recovery phase")
	}
}

type TransitionStage string

const (
	TransitionPending    TransitionStage = "PENDING"
	TransitionCatchingUp TransitionStage = "CATCHING_UP"
	TransitionPromoted   TransitionStage = "PROMOTED"
	TransitionOldRemoved TransitionStage = "OLD_REMOVED"
	TransitionComplete   TransitionStage = "COMPLETE"
)

type ShardTransition struct {
	ShardID uint32          `json:"shard_id"`
	Stage   TransitionStage `json:"stage"`
}

type ManifestTransition struct {
	Version         uint64            `json:"version"`
	Digest          string            `json:"digest"`
	PreviousDigest  string            `json:"previous_digest"`
	NextSystemEpoch uint64            `json:"next_system_epoch"`
	Shards          []ShardTransition `json:"shards"`
}

func (t ManifestTransition) Validate(virtualShards uint32) error {
	if t.Version == 0 || !isSHA256(t.Digest) || !isSHA256(t.PreviousDigest) || t.Digest == t.PreviousDigest ||
		t.NextSystemEpoch == 0 || len(t.Shards) != int(virtualShards)+1 {
		return errors.New("raftstore: incomplete manifest transition")
	}
	for i, shard := range t.Shards {
		want := uint32(i)
		if i == 0 {
			want = ^uint32(0)
		} else {
			want = uint32(i - 1)
		}
		if shard.ShardID != want || shard.Stage != TransitionPending {
			return errors.New("raftstore: transition progress must begin complete, ordered, and pending")
		}
	}
	return nil
}

type GenerationClosure struct {
	TargetStorageGeneration string            `json:"target_storage_generation"`
	TargetManifestDigest    string            `json:"target_manifest_digest"`
	Kind                    RolloverProofKind `json:"kind"`
	ProofDigest             string            `json:"proof_digest"`
	CommitIndex             uint64            `json:"commit_index"`
}

type SystemState struct {
	Initialized                bool                `json:"initialized"`
	ClusterID                  string              `json:"cluster_id"`
	StorageGeneration          string              `json:"storage_generation"`
	SystemEpoch                uint64              `json:"system_epoch"`
	SchemaVersion              uint32              `json:"schema_version"`
	ProtocolVersion            uint32              `json:"protocol_version"`
	VirtualShardCount          uint32              `json:"virtual_shard_count"`
	ActiveManifestVersion      uint64              `json:"active_manifest_version"`
	ActiveManifestDigest       string              `json:"active_manifest_digest"`
	ServePermitMaxMillis       uint64              `json:"serve_permit_max_millis"`
	HasPredecessor             bool                `json:"has_predecessor"`
	PredecessorProofKind       RolloverProofKind   `json:"predecessor_proof_kind,omitempty"`
	PredecessorProofDigest     string              `json:"predecessor_proof_digest,omitempty"`
	PredecessorPermitMaxMillis uint64              `json:"predecessor_permit_max_millis,omitempty"`
	ServeGate                  bool                `json:"serve_gate"`
	WriteGate                  bool                `json:"write_gate"`
	CutoverGate                bool                `json:"cutover_gate"`
	PredecessorDrainComplete   bool                `json:"predecessor_drain_complete"`
	Retired                    bool                `json:"retired"`
	Transition                 *ManifestTransition `json:"transition,omitempty"`
	Recovery                   *RecoveryEpoch      `json:"recovery,omitempty"`
	Closure                    *GenerationClosure  `json:"closure,omitempty"`
	LastApplied                uint64              `json:"last_applied"`
}

func (s SystemState) Identity() PermitIdentity {
	return PermitIdentity{
		ClusterID: s.ClusterID, StorageGeneration: s.StorageGeneration,
		SystemEpoch: s.SystemEpoch, ManifestDigest: s.ActiveManifestDigest,
	}
}

func (s SystemState) Validate() error {
	if !s.Initialized {
		if s != (SystemState{}) {
			return errors.New("raftstore: uninitialized System Group contains state")
		}
		return nil
	}
	if err := s.Identity().Validate(); err != nil {
		return err
	}
	if s.SchemaVersion == 0 || s.ProtocolVersion == 0 || s.VirtualShardCount == 0 ||
		s.ActiveManifestVersion == 0 || s.ServePermitMaxMillis == 0 || s.LastApplied == 0 {
		return errors.New("raftstore: incomplete System Group state")
	}
	if s.HasPredecessor {
		if !isSHA256(s.PredecessorProofDigest) || s.PredecessorPermitMaxMillis == 0 ||
			(s.PredecessorProofKind != RolloverConsensusClosure && s.PredecessorProofKind != RolloverExternalFence) {
			return errors.New("raftstore: incomplete predecessor fencing state")
		}
	} else if s.PredecessorProofKind != "" || s.PredecessorProofDigest != "" || s.PredecessorPermitMaxMillis != 0 ||
		!s.PredecessorDrainComplete {
		return errors.New("raftstore: invalid first-generation predecessor state")
	}
	if s.Retired && (s.ServeGate || s.WriteGate || s.CutoverGate) {
		return errors.New("raftstore: retired generation has an open gate")
	}
	if s.Recovery != nil {
		if err := s.Recovery.Validate(); err != nil {
			return err
		}
		if s.ServeGate || s.WriteGate || s.CutoverGate {
			return errors.New("raftstore: recovery epoch requires closed normal-operation gates")
		}
	}
	return nil
}

type SystemCommandType string

const (
	SystemBootstrap          SystemCommandType = "BOOTSTRAP"
	SystemRefreshPermit      SystemCommandType = "REFRESH_PERMIT"
	SystemSetGates           SystemCommandType = "SET_GATES"
	SystemBeginTransition    SystemCommandType = "BEGIN_MANIFEST_TRANSITION"
	SystemAdvanceTransition  SystemCommandType = "ADVANCE_MANIFEST_TRANSITION"
	SystemActivateTransition SystemCommandType = "ACTIVATE_MANIFEST_TRANSITION"
	SystemCloseGeneration    SystemCommandType = "CLOSE_GENERATION"
	SystemConfirmDrain       SystemCommandType = "CONFIRM_PREDECESSOR_PERMIT_DRAIN"
	SystemBeginRecovery      SystemCommandType = "BEGIN_RECOVERY"
	SystemAdvanceRecovery    SystemCommandType = "ADVANCE_RECOVERY"
)

type GateUpdate struct {
	Serve   bool `json:"serve"`
	Write   bool `json:"write"`
	Cutover bool `json:"cutover"`
}

type TransitionAdvance struct {
	ShardID uint32          `json:"shard_id"`
	From    TransitionStage `json:"from"`
	To      TransitionStage `json:"to"`
}

type RecoveryAdvance struct {
	From RecoveryPhase `json:"from"`
	To   RecoveryPhase `json:"to"`
}

type DrainConfirmation struct {
	PredecessorProofDigest string `json:"predecessor_proof_digest"`
	WaitedMillis           uint64 `json:"waited_millis"`
	EvidenceDigest         string `json:"evidence_digest"`
}

type SystemCommand struct {
	Type            SystemCommandType   `json:"type"`
	Manifest        *Manifest           `json:"manifest,omitempty"`
	Digest          string              `json:"digest,omitempty"`
	Gates           *GateUpdate         `json:"gates,omitempty"`
	Transition      *ManifestTransition `json:"transition,omitempty"`
	Advance         *TransitionAdvance  `json:"advance,omitempty"`
	Closure         *GenerationClosure  `json:"closure,omitempty"`
	Drain           *DrainConfirmation  `json:"drain,omitempty"`
	Recovery        *RecoveryEpoch      `json:"recovery,omitempty"`
	RecoveryAdvance *RecoveryAdvance    `json:"recovery_advance,omitempty"`
}

type SystemApplyResult struct {
	Applied     bool         `json:"applied"`
	Conflict    bool         `json:"conflict,omitempty"`
	Reason      string       `json:"reason,omitempty"`
	PermitGrant *PermitGrant `json:"permit_grant,omitempty"`
}

func ApplySystemCommand(state SystemState, index uint64, command SystemCommand) (SystemState, SystemApplyResult) {
	if index == 0 || index <= state.LastApplied {
		return state, systemConflict("non-monotonic committed log index")
	}
	next := state
	switch command.Type {
	case SystemBootstrap:
		if state.Initialized || command.Manifest == nil || !isSHA256(command.Digest) {
			return state, systemConflict("System Group bootstrap is not authorized for non-empty state")
		}
		manifest := *command.Manifest
		digest, err := manifest.Digest()
		if err != nil || digest != command.Digest {
			return state, systemConflict("invalid generation bootstrap manifest")
		}
		next = SystemState{
			Initialized: true, ClusterID: manifest.ClusterID, StorageGeneration: manifest.StorageGeneration,
			SystemEpoch: 1, SchemaVersion: manifest.SchemaVersion, ProtocolVersion: manifest.ProtocolVersion,
			VirtualShardCount: manifest.VirtualShardCount, ActiveManifestVersion: manifest.ManifestVersion,
			ActiveManifestDigest: digest, ServePermitMaxMillis: manifest.ServePermitMaxMillis,
			HasPredecessor: manifest.Predecessor != nil,
		}
		if manifest.Predecessor == nil {
			next.PredecessorDrainComplete = true
		} else {
			next.PredecessorProofKind = manifest.Predecessor.Kind
			next.PredecessorProofDigest = manifest.Predecessor.ProofDigest
			next.PredecessorPermitMaxMillis = manifest.Predecessor.ServePermitMaxMillis
			next.PredecessorDrainComplete = manifest.Predecessor.Kind == RolloverExternalFence
		}
	case SystemRefreshPermit:
		if !state.Initialized || state.Retired {
			return state, systemConflict("generation cannot grant a Serve Permit")
		}
	case SystemSetGates:
		if !state.Initialized || state.Retired || state.Recovery != nil || command.Gates == nil ||
			!state.PredecessorDrainComplete {
			return state, systemConflict("normal-operation gates are fenced")
		}
		if command.Gates.Write && !command.Gates.Serve || command.Gates.Cutover && !command.Gates.Serve {
			return state, systemConflict("write/cutover gate requires serve gate")
		}
		next.ServeGate, next.WriteGate, next.CutoverGate = command.Gates.Serve, command.Gates.Write, command.Gates.Cutover
	case SystemBeginTransition:
		if !state.Initialized || state.Retired || state.Transition != nil || state.Recovery != nil || command.Transition == nil {
			return state, systemConflict("manifest transition cannot begin")
		}
		if command.Transition.Version != state.ActiveManifestVersion+1 ||
			command.Transition.PreviousDigest != state.ActiveManifestDigest ||
			command.Transition.NextSystemEpoch != state.SystemEpoch+1 ||
			command.Transition.Validate(state.VirtualShardCount) != nil {
			return state, systemConflict("invalid manifest transition")
		}
		transition := *command.Transition
		transition.Shards = append([]ShardTransition(nil), command.Transition.Shards...)
		next.Transition = &transition
	case SystemAdvanceTransition:
		if state.Transition == nil || command.Advance == nil {
			return state, systemConflict("no matching manifest transition")
		}
		position := transitionPosition(state.Transition.Shards, command.Advance.ShardID)
		if position < 0 || state.Transition.Shards[position].Stage != command.Advance.From ||
			!validTransitionAdvance(command.Advance.From, command.Advance.To) {
			return state, systemConflict("manifest transition stage conflict")
		}
		transition := *state.Transition
		transition.Shards = append([]ShardTransition(nil), state.Transition.Shards...)
		transition.Shards[position].Stage = command.Advance.To
		next.Transition = &transition
	case SystemActivateTransition:
		if state.Transition == nil || !allTransitionsComplete(state.Transition.Shards) {
			return state, systemConflict("manifest transition is incomplete")
		}
		next.ActiveManifestVersion = state.Transition.Version
		next.ActiveManifestDigest = state.Transition.Digest
		next.SystemEpoch = state.Transition.NextSystemEpoch
		next.Transition = nil
	case SystemCloseGeneration:
		if !state.Initialized || state.Retired || command.Closure == nil || command.Closure.CommitIndex != 0 ||
			command.Closure.TargetStorageGeneration == "" || command.Closure.TargetStorageGeneration == state.StorageGeneration ||
			!isSHA256(command.Closure.TargetManifestDigest) || !isSHA256(command.Closure.ProofDigest) ||
			command.Closure.Kind != RolloverConsensusClosure {
			return state, systemConflict("invalid consensus generation closure")
		}
		closure := *command.Closure
		closure.CommitIndex = index
		next.Closure = &closure
		next.Retired = true
		next.ServeGate, next.WriteGate, next.CutoverGate = false, false, false
	case SystemConfirmDrain:
		if !state.Initialized || !state.HasPredecessor || state.PredecessorDrainComplete || command.Drain == nil ||
			command.Drain.PredecessorProofDigest != state.PredecessorProofDigest ||
			command.Drain.WaitedMillis < state.PredecessorPermitMaxMillis || !isSHA256(command.Drain.EvidenceDigest) {
			return state, systemConflict("predecessor permit drain confirmation is invalid")
		}
		next.PredecessorDrainComplete = true
	case SystemBeginRecovery:
		if !state.Initialized || state.Retired || state.Recovery != nil || command.Recovery == nil || command.Recovery.Validate() != nil ||
			command.Recovery.TargetStorageGeneration != state.StorageGeneration ||
			command.Recovery.TargetManifestDigest != state.ActiveManifestDigest || command.Recovery.Phase != RecoveryPreparing {
			return state, systemConflict("invalid recovery epoch")
		}
		recovery := *command.Recovery
		next.Recovery = &recovery
		next.SystemEpoch++
		next.ServeGate, next.WriteGate, next.CutoverGate = false, false, false
	case SystemAdvanceRecovery:
		if state.Recovery == nil || command.RecoveryAdvance == nil ||
			state.Recovery.Phase != command.RecoveryAdvance.From ||
			!validRecoveryAdvance(command.RecoveryAdvance.From, command.RecoveryAdvance.To) {
			return state, systemConflict("recovery phase conflict")
		}
		if command.RecoveryAdvance.To == RecoveryClosed {
			next.Recovery = nil
			next.SystemEpoch++
		} else {
			recovery := *state.Recovery
			recovery.Phase = command.RecoveryAdvance.To
			next.Recovery = &recovery
		}
	default:
		return state, systemConflict(fmt.Sprintf("unknown System Group command %q", command.Type))
	}
	next.LastApplied = index
	if err := next.Validate(); err != nil {
		return state, systemConflict(err.Error())
	}
	result := SystemApplyResult{Applied: true}
	if command.Type == SystemRefreshPermit {
		grant := PermitGrant{
			PermitIdentity: next.Identity(), CommitIndex: index, MaxLifetimeMillis: next.ServePermitMaxMillis,
			ServeGate: next.ServeGate, WriteGate: next.WriteGate, CutoverGate: next.CutoverGate,
			RecoveryClosed: next.Recovery == nil,
		}
		result.PermitGrant = &grant
	}
	return next, result
}

func systemConflict(reason string) SystemApplyResult {
	return SystemApplyResult{Conflict: true, Reason: reason}
}

func transitionPosition(shards []ShardTransition, shardID uint32) int {
	if shardID == ^uint32(0) {
		if len(shards) > 0 && shards[0].ShardID == shardID {
			return 0
		}
		return -1
	}
	position := int(shardID) + 1
	if position >= len(shards) || shards[position].ShardID != shardID {
		return -1
	}
	return position
}

func validTransitionAdvance(from, to TransitionStage) bool {
	return from == TransitionPending && to == TransitionCatchingUp ||
		from == TransitionCatchingUp && to == TransitionPromoted ||
		from == TransitionPromoted && to == TransitionOldRemoved ||
		from == TransitionOldRemoved && to == TransitionComplete
}

func allTransitionsComplete(shards []ShardTransition) bool {
	for _, shard := range shards {
		if shard.Stage != TransitionComplete {
			return false
		}
	}
	return true
}

func validRecoveryAdvance(from, to RecoveryPhase) bool {
	return from == RecoveryPreparing && to == RecoveryCollecting ||
		from == RecoveryCollecting && to == RecoveryReconciling ||
		from == RecoveryReconciling && to == RecoveryFinalizing ||
		from == RecoveryFinalizing && to == RecoveryClosed
}
