package raftstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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
	Epoch                      uint64        `json:"epoch"`
	SourceClusterID            string        `json:"source_cluster_id"`
	SourceRegistryGeneration   string        `json:"source_registry_generation"`
	SourceRegistryLayoutDigest string        `json:"source_registry_layout_digest"`
	TargetRegistryGeneration   string        `json:"target_registry_generation"`
	TargetRegistryLayoutDigest string        `json:"target_registry_layout_digest"`
	Phase                      RecoveryPhase `json:"phase"`
	PermitDrainComplete        bool          `json:"permit_drain_complete"`
	PermitDrainIndex           uint64        `json:"permit_drain_index,omitempty"`
}

func (r RecoveryEpoch) Validate() error {
	if r.Epoch == 0 || r.SourceClusterID == "" || r.SourceRegistryGeneration == "" ||
		r.TargetRegistryGeneration == "" || r.SourceRegistryGeneration == r.TargetRegistryGeneration ||
		!isSHA256(r.SourceRegistryLayoutDigest) || !isSHA256(r.TargetRegistryLayoutDigest) {
		return errors.New("raftstore: incomplete recovery epoch")
	}
	switch r.Phase {
	case RecoveryPreparing, RecoveryCollecting, RecoveryReconciling, RecoveryFinalizing:
		if r.PermitDrainComplete != (r.PermitDrainIndex != 0) {
			return errors.New("raftstore: recovery permit drain state is inconsistent")
		}
		if r.Phase != RecoveryPreparing && !r.PermitDrainComplete {
			return errors.New("raftstore: recovery advanced before old permits drained")
		}
		return nil
	default:
		return errors.New("raftstore: invalid open recovery phase")
	}
}

type TransitionStage string

const (
	TransitionPending      TransitionStage = "PENDING"
	TransitionCatchingUp   TransitionStage = "CATCHING_UP"
	TransitionPromoted     TransitionStage = "PROMOTED"
	TransitionComplete     TransitionStage = "COMPLETE"
	TransitionEpochRetired TransitionStage = "EPOCH_RETIRED"
)

type ShardTransition struct {
	ShardID uint32          `json:"shard_id"`
	Stage   TransitionStage `json:"stage"`
}

type RegistryLayoutTransition struct {
	Version                     uint64            `json:"version"`
	Digest                      string            `json:"digest"`
	PreviousDigest              string            `json:"previous_digest"`
	NextSystemEpoch             uint64            `json:"next_system_epoch"`
	Activated                   bool              `json:"activated"`
	ActivationIndex             uint64            `json:"activation_index,omitempty"`
	PreviousPermitDrainComplete bool              `json:"previous_permit_drain_complete"`
	PreviousPermitDrainIndex    uint64            `json:"previous_permit_drain_index,omitempty"`
	Shards                      []ShardTransition `json:"shards"`
}

func (t RegistryLayoutTransition) Validate(virtualShards uint32) error {
	if t.Version == 0 || !isSHA256(t.Digest) || !isSHA256(t.PreviousDigest) || t.Digest == t.PreviousDigest ||
		t.NextSystemEpoch == 0 || t.Activated || t.ActivationIndex != 0 || t.PreviousPermitDrainComplete ||
		t.PreviousPermitDrainIndex != 0 || len(t.Shards) != int(virtualShards)+1 {
		return errors.New("raftstore: incomplete registryLayout transition")
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

type RegistryGenerationClosure struct {
	TargetRegistryGeneration         string            `json:"target_registry_generation"`
	TargetRegistryLayoutIntentDigest string            `json:"target_registry_layout_intent_digest"`
	Kind                             RolloverProofKind `json:"kind"`
	ProofDigest                      string            `json:"proof_digest"`
	CommitIndex                      uint64            `json:"commit_index"`
}

type SystemState struct {
	Initialized                                 bool                       `json:"initialized"`
	ClusterID                                   string                     `json:"cluster_id"`
	RegistryGeneration                          string                     `json:"registry_generation"`
	SystemEpoch                                 uint64                     `json:"system_epoch"`
	SchemaVersion                               uint32                     `json:"schema_version"`
	ProtocolVersion                             uint32                     `json:"protocol_version"`
	VirtualShardCount                           uint32                     `json:"virtual_shard_count"`
	ActiveRegistryLayoutVersion                 uint64                     `json:"active_registry_layout_version"`
	ActiveRegistryLayoutDigest                  string                     `json:"active_registry_layout_digest"`
	ServePermitMaxMillis                        uint64                     `json:"serve_permit_max_millis"`
	HasPredecessor                              bool                       `json:"has_predecessor"`
	PredecessorRegistryGeneration               string                     `json:"predecessor_registry_generation,omitempty"`
	PredecessorRegistryLayoutDigest             string                     `json:"predecessor_registry_layout_digest,omitempty"`
	PredecessorProofKind                        RolloverProofKind          `json:"predecessor_proof_kind,omitempty"`
	PredecessorProofDigest                      string                     `json:"predecessor_proof_digest,omitempty"`
	PredecessorProofCommitIndex                 uint64                     `json:"predecessor_proof_commit_index,omitempty"`
	PredecessorTargetRegistryLayoutIntentDigest string                     `json:"predecessor_target_registry_layout_intent_digest,omitempty"`
	PredecessorPermitMaxMillis                  uint64                     `json:"predecessor_permit_max_millis,omitempty"`
	ServeGate                                   bool                       `json:"serve_gate"`
	WriteGate                                   bool                       `json:"write_gate"`
	CutoverGate                                 bool                       `json:"cutover_gate"`
	PredecessorDrainComplete                    bool                       `json:"predecessor_drain_complete"`
	Retired                                     bool                       `json:"retired"`
	Transition                                  *RegistryLayoutTransition  `json:"transition,omitempty"`
	Recovery                                    *RecoveryEpoch             `json:"recovery,omitempty"`
	Closure                                     *RegistryGenerationClosure `json:"closure,omitempty"`
	LastApplied                                 uint64                     `json:"last_applied"`
}

// ConsensusPredecessorProof exports the committed closure in the form a
// successor registryLayout must carry. The closure is useful only after the source
// Registry History Generation has permanently retired.
func (s SystemState) ConsensusPredecessorProof() (PredecessorProof, error) {
	if err := s.Validate(); err != nil {
		return PredecessorProof{}, err
	}
	if !s.Retired || s.Closure == nil || s.Closure.Kind != RolloverConsensusClosure {
		return PredecessorProof{}, errors.New("raftstore: Registry History Generation has no committed consensus closure")
	}
	proof := PredecessorProof{
		RegistryGeneration:               s.RegistryGeneration,
		RegistryLayoutDigest:             s.ActiveRegistryLayoutDigest,
		ServePermitMaxMillis:             s.ServePermitMaxMillis,
		Kind:                             s.Closure.Kind,
		TargetRegistryLayoutIntentDigest: s.Closure.TargetRegistryLayoutIntentDigest,
		CommitIndex:                      s.Closure.CommitIndex,
		ProofDigest:                      s.Closure.ProofDigest,
	}
	if err := proof.Validate(); err != nil {
		return PredecessorProof{}, err
	}
	return proof, nil
}

func (s SystemState) Identity() PermitIdentity {
	return PermitIdentity{
		ClusterID: s.ClusterID, RegistryGeneration: s.RegistryGeneration,
		SystemEpoch: s.SystemEpoch, RegistryLayoutDigest: s.ActiveRegistryLayoutDigest,
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
		s.ActiveRegistryLayoutVersion == 0 || s.ServePermitMaxMillis == 0 ||
		s.ServePermitMaxMillis > MaximumServePermitMillis || s.LastApplied == 0 {
		return errors.New("raftstore: incomplete System Group state")
	}
	if s.HasPredecessor {
		if s.PredecessorRegistryGeneration == "" || s.PredecessorRegistryGeneration == s.RegistryGeneration ||
			!isSHA256(s.PredecessorRegistryLayoutDigest) || !isSHA256(s.PredecessorProofDigest) ||
			s.PredecessorPermitMaxMillis == 0 || s.PredecessorPermitMaxMillis > MaximumServePermitMillis ||
			(s.PredecessorProofKind != RolloverConsensusClosure && s.PredecessorProofKind != RolloverExternalFence) {
			return errors.New("raftstore: incomplete predecessor fencing state")
		}
		if s.PredecessorProofKind == RolloverConsensusClosure {
			if s.PredecessorProofCommitIndex == 0 || !isSHA256(s.PredecessorTargetRegistryLayoutIntentDigest) ||
				s.PredecessorProofDigest != consensusClosureProofDigest(
					s.ClusterID, s.PredecessorRegistryGeneration, s.PredecessorRegistryLayoutDigest,
					s.RegistryGeneration, s.PredecessorTargetRegistryLayoutIntentDigest, s.PredecessorPermitMaxMillis,
					s.PredecessorProofCommitIndex,
				) {
				return errors.New("raftstore: invalid consensus predecessor proof")
			}
		} else if s.PredecessorProofCommitIndex != 0 || s.PredecessorTargetRegistryLayoutIntentDigest != "" {
			return errors.New("raftstore: external predecessor fence contains consensus fields")
		}
	} else if s.PredecessorRegistryGeneration != "" || s.PredecessorRegistryLayoutDigest != "" ||
		s.PredecessorProofKind != "" || s.PredecessorProofDigest != "" || s.PredecessorProofCommitIndex != 0 ||
		s.PredecessorTargetRegistryLayoutIntentDigest != "" || s.PredecessorPermitMaxMillis != 0 ||
		!s.PredecessorDrainComplete {
		return errors.New("raftstore: invalid first Registry History Generation predecessor state")
	}
	if (s.WriteGate || s.CutoverGate) && !s.ServeGate {
		return errors.New("raftstore: write/cutover gate requires serve gate")
	}
	if (s.ServeGate || s.WriteGate || s.CutoverGate) && !s.PredecessorDrainComplete {
		return errors.New("raftstore: predecessor permits have not drained")
	}
	if s.Retired && (s.ServeGate || s.WriteGate || s.CutoverGate) {
		return errors.New("raftstore: retired Registry History Generation has an open gate")
	}
	if s.Retired != (s.Closure != nil) {
		return errors.New("raftstore: Registry History Generation closure and retired state disagree")
	}
	if s.Closure != nil {
		if s.Closure.TargetRegistryGeneration == "" || s.Closure.TargetRegistryGeneration == s.RegistryGeneration ||
			!isSHA256(s.Closure.TargetRegistryLayoutIntentDigest) || !isSHA256(s.Closure.ProofDigest) ||
			s.Closure.Kind != RolloverConsensusClosure || s.Closure.CommitIndex == 0 ||
			s.Closure.CommitIndex > s.LastApplied ||
			s.Closure.ProofDigest != registryGenerationClosureProofDigest(s, *s.Closure) {
			return errors.New("raftstore: invalid committed Registry History Generation closure")
		}
	}
	if s.Transition != nil {
		transition := s.Transition
		if s.Recovery != nil || validateRegistryLayoutTransitionState(*transition, s.VirtualShardCount) != nil {
			return errors.New("raftstore: invalid active registryLayout transition")
		}
		if !transition.Activated {
			if transition.Version != s.ActiveRegistryLayoutVersion+1 ||
				transition.PreviousDigest != s.ActiveRegistryLayoutDigest ||
				transition.NextSystemEpoch != s.SystemEpoch+1 || transition.ActivationIndex != 0 ||
				transition.PreviousPermitDrainComplete || transition.PreviousPermitDrainIndex != 0 ||
				anyTransitionAtStage(transition.Shards, TransitionEpochRetired) {
				return errors.New("raftstore: invalid pending registryLayout transition")
			}
		} else {
			if transition.Version != s.ActiveRegistryLayoutVersion || transition.Digest != s.ActiveRegistryLayoutDigest ||
				transition.NextSystemEpoch != s.SystemEpoch || transition.ActivationIndex == 0 ||
				transition.ActivationIndex > s.LastApplied || !allTransitionsAtLeastComplete(transition.Shards) {
				return errors.New("raftstore: invalid activated registryLayout transition")
			}
			if transition.PreviousPermitDrainComplete {
				if transition.PreviousPermitDrainIndex < transition.ActivationIndex ||
					transition.PreviousPermitDrainIndex > s.LastApplied {
					return errors.New("raftstore: invalid previous registryLayout permit drain proof")
				}
			} else if transition.PreviousPermitDrainIndex != 0 ||
				anyTransitionAtStage(transition.Shards, TransitionEpochRetired) {
				return errors.New("raftstore: previous registryLayout epoch retired before permit drain")
			}
		}
	}
	if s.Recovery != nil {
		if err := s.Recovery.Validate(); err != nil {
			return err
		}
		if !s.HasPredecessor || !s.PredecessorDrainComplete ||
			s.Recovery.Epoch != s.SystemEpoch ||
			s.Recovery.SourceClusterID != s.ClusterID ||
			s.Recovery.SourceRegistryGeneration != s.PredecessorRegistryGeneration ||
			s.Recovery.SourceRegistryLayoutDigest != s.PredecessorRegistryLayoutDigest ||
			s.Recovery.TargetRegistryGeneration != s.RegistryGeneration ||
			s.Recovery.TargetRegistryLayoutDigest != s.ActiveRegistryLayoutDigest {
			return errors.New("raftstore: recovery epoch does not match its fenced source and target")
		}
		if s.Recovery.PermitDrainIndex > s.LastApplied {
			return errors.New("raftstore: recovery permit drain proof is ahead of System consensus")
		}
		if s.ServeGate || s.WriteGate || s.CutoverGate {
			return errors.New("raftstore: recovery epoch requires closed normal-operation gates")
		}
	}
	return nil
}

type SystemCommandType string

const (
	SystemBootstrap               SystemCommandType = "BOOTSTRAP"
	SystemRefreshPermit           SystemCommandType = "REFRESH_PERMIT"
	SystemSetGates                SystemCommandType = "SET_GATES"
	SystemBeginTransition         SystemCommandType = "BEGIN_REGISTRY_LAYOUT_TRANSITION"
	SystemAdvanceTransition       SystemCommandType = "ADVANCE_REGISTRY_LAYOUT_TRANSITION"
	SystemActivateTransition      SystemCommandType = "ACTIVATE_REGISTRY_LAYOUT_TRANSITION"
	SystemConfirmTransitionDrain  SystemCommandType = "CONFIRM_REGISTRY_LAYOUT_TRANSITION_PERMIT_DRAIN"
	SystemFinalizeTransition      SystemCommandType = "FINALIZE_REGISTRY_LAYOUT_TRANSITION"
	SystemCloseRegistryGeneration SystemCommandType = "CLOSE_REGISTRY_GENERATION"
	SystemConfirmDrain            SystemCommandType = "CONFIRM_PREDECESSOR_PERMIT_DRAIN"
	SystemBeginRecovery           SystemCommandType = "BEGIN_RECOVERY"
	SystemConfirmRecoveryDrain    SystemCommandType = "CONFIRM_RECOVERY_PERMIT_DRAIN"
	SystemAdvanceRecovery         SystemCommandType = "ADVANCE_RECOVERY"
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

type RecoveryDrainConfirmation struct {
	RecoveryEpoch              uint64 `json:"recovery_epoch"`
	TargetRegistryGeneration   string `json:"target_registry_generation"`
	TargetRegistryLayoutDigest string `json:"target_registry_layout_digest"`
	WaitedMillis               uint64 `json:"waited_millis"`
}

type DrainConfirmation struct {
	PredecessorProofDigest string `json:"predecessor_proof_digest"`
	WaitedMillis           uint64 `json:"waited_millis"`
	EvidenceDigest         string `json:"evidence_digest"`
}

type TransitionDrainConfirmation struct {
	PreviousRegistryLayoutDigest string `json:"previous_registry_layout_digest"`
	PreviousSystemEpoch          uint64 `json:"previous_system_epoch"`
	ActivationIndex              uint64 `json:"activation_index"`
	WaitedMillis                 uint64 `json:"waited_millis"`
}

type SystemCommand struct {
	Type            SystemCommandType            `json:"type"`
	RegistryLayout  *RegistryLayout              `json:"registry_layout,omitempty"`
	Digest          string                       `json:"digest,omitempty"`
	Gates           *GateUpdate                  `json:"gates,omitempty"`
	Transition      *RegistryLayoutTransition    `json:"transition,omitempty"`
	Advance         *TransitionAdvance           `json:"advance,omitempty"`
	Closure         *RegistryGenerationClosure   `json:"closure,omitempty"`
	Drain           *DrainConfirmation           `json:"drain,omitempty"`
	TransitionDrain *TransitionDrainConfirmation `json:"transition_drain,omitempty"`
	Recovery        *RecoveryEpoch               `json:"recovery,omitempty"`
	RecoveryDrain   *RecoveryDrainConfirmation   `json:"recovery_drain,omitempty"`
	RecoveryAdvance *RecoveryAdvance             `json:"recovery_advance,omitempty"`
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
		if state.Initialized || command.RegistryLayout == nil || command.RegistryLayout.RegistryLayoutVersion != 1 ||
			!isSHA256(command.Digest) {
			return state, systemConflict("System Group bootstrap is not authorized for non-empty state")
		}
		registryLayout := *command.RegistryLayout
		digest, err := registryLayout.Digest()
		if err != nil || digest != command.Digest {
			return state, systemConflict("invalid Registry History Generation bootstrap Registry Layout")
		}
		next = SystemState{
			Initialized: true, ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
			SystemEpoch: 1, SchemaVersion: registryLayout.SchemaVersion, ProtocolVersion: registryLayout.ProtocolVersion,
			VirtualShardCount: registryLayout.VirtualShardCount, ActiveRegistryLayoutVersion: registryLayout.RegistryLayoutVersion,
			ActiveRegistryLayoutDigest: digest, ServePermitMaxMillis: registryLayout.ServePermitMaxMillis,
			HasPredecessor: registryLayout.Predecessor != nil,
		}
		if registryLayout.Predecessor == nil {
			next.PredecessorDrainComplete = true
		} else {
			next.PredecessorRegistryGeneration = registryLayout.Predecessor.RegistryGeneration
			next.PredecessorRegistryLayoutDigest = registryLayout.Predecessor.RegistryLayoutDigest
			next.PredecessorProofKind = registryLayout.Predecessor.Kind
			next.PredecessorProofDigest = registryLayout.Predecessor.ProofDigest
			next.PredecessorProofCommitIndex = registryLayout.Predecessor.CommitIndex
			next.PredecessorTargetRegistryLayoutIntentDigest = registryLayout.Predecessor.TargetRegistryLayoutIntentDigest
			next.PredecessorPermitMaxMillis = registryLayout.Predecessor.ServePermitMaxMillis
			next.PredecessorDrainComplete = registryLayout.Predecessor.Kind == RolloverExternalFence
		}
	case SystemRefreshPermit:
		if !state.Initialized || state.Retired {
			return state, systemConflict("Registry History Generation cannot grant a Serve Permit")
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
			return state, systemConflict("registryLayout transition cannot begin")
		}
		if command.Transition.Version != state.ActiveRegistryLayoutVersion+1 ||
			command.Transition.PreviousDigest != state.ActiveRegistryLayoutDigest ||
			command.Transition.NextSystemEpoch != state.SystemEpoch+1 ||
			command.Transition.Validate(state.VirtualShardCount) != nil {
			return state, systemConflict("invalid registryLayout transition")
		}
		transition := *command.Transition
		transition.Shards = append([]ShardTransition(nil), command.Transition.Shards...)
		next.Transition = &transition
	case SystemAdvanceTransition:
		if state.Transition == nil || command.Advance == nil {
			return state, systemConflict("no matching registryLayout transition")
		}
		position := transitionPosition(state.Transition.Shards, command.Advance.ShardID)
		if position < 0 || state.Transition.Shards[position].Stage != command.Advance.From ||
			!validTransitionAdvance(command.Advance.From, command.Advance.To) ||
			(command.Advance.To == TransitionEpochRetired &&
				(!state.Transition.Activated || !state.Transition.PreviousPermitDrainComplete)) ||
			(command.Advance.To != TransitionEpochRetired && state.Transition.Activated) {
			return state, systemConflict("registryLayout transition stage conflict")
		}
		transition := *state.Transition
		transition.Shards = append([]ShardTransition(nil), state.Transition.Shards...)
		transition.Shards[position].Stage = command.Advance.To
		next.Transition = &transition
	case SystemActivateTransition:
		if state.Transition == nil || state.Transition.Activated || !allTransitionsComplete(state.Transition.Shards) {
			return state, systemConflict("registryLayout transition is incomplete")
		}
		transition := *state.Transition
		transition.Shards = append([]ShardTransition(nil), state.Transition.Shards...)
		transition.Activated = true
		transition.ActivationIndex = index
		next.ActiveRegistryLayoutVersion = transition.Version
		next.ActiveRegistryLayoutDigest = transition.Digest
		next.SystemEpoch = transition.NextSystemEpoch
		next.Transition = &transition
	case SystemConfirmTransitionDrain:
		if state.Transition == nil || !state.Transition.Activated ||
			state.Transition.PreviousPermitDrainComplete || command.TransitionDrain == nil ||
			command.TransitionDrain.PreviousRegistryLayoutDigest != state.Transition.PreviousDigest ||
			command.TransitionDrain.PreviousSystemEpoch+1 != state.Transition.NextSystemEpoch ||
			command.TransitionDrain.ActivationIndex != state.Transition.ActivationIndex ||
			command.TransitionDrain.WaitedMillis < state.ServePermitMaxMillis {
			return state, systemConflict("previous registryLayout permit drain confirmation is invalid")
		}
		transition := *state.Transition
		transition.Shards = append([]ShardTransition(nil), state.Transition.Shards...)
		transition.PreviousPermitDrainComplete = true
		transition.PreviousPermitDrainIndex = index
		next.Transition = &transition
	case SystemFinalizeTransition:
		if state.Transition == nil || !state.Transition.Activated ||
			!state.Transition.PreviousPermitDrainComplete ||
			!allTransitionsAtStage(state.Transition.Shards, TransitionEpochRetired) {
			return state, systemConflict("registryLayout transition is not ready to finalize")
		}
		next.Transition = nil
	case SystemCloseRegistryGeneration:
		if !state.Initialized || state.Retired || state.Transition != nil || state.Recovery != nil ||
			command.Closure == nil || command.Closure.CommitIndex != 0 ||
			command.Closure.TargetRegistryGeneration == "" || command.Closure.TargetRegistryGeneration == state.RegistryGeneration ||
			!isSHA256(command.Closure.TargetRegistryLayoutIntentDigest) || command.Closure.ProofDigest != "" ||
			command.Closure.Kind != RolloverConsensusClosure {
			return state, systemConflict("invalid consensus Registry History Generation closure")
		}
		closure := *command.Closure
		closure.CommitIndex = index
		closure.ProofDigest = registryGenerationClosureProofDigest(state, closure)
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
		if !state.Initialized || state.Retired || !state.HasPredecessor || !state.PredecessorDrainComplete ||
			state.Transition != nil || state.Recovery != nil || command.Recovery == nil || command.Recovery.Validate() != nil ||
			command.Recovery.Epoch != state.SystemEpoch+1 || command.Recovery.SourceClusterID != state.ClusterID ||
			command.Recovery.SourceRegistryGeneration != state.PredecessorRegistryGeneration ||
			command.Recovery.SourceRegistryLayoutDigest != state.PredecessorRegistryLayoutDigest ||
			command.Recovery.TargetRegistryGeneration != state.RegistryGeneration ||
			command.Recovery.TargetRegistryLayoutDigest != state.ActiveRegistryLayoutDigest ||
			command.Recovery.Phase != RecoveryPreparing || command.Recovery.PermitDrainComplete ||
			command.Recovery.PermitDrainIndex != 0 {
			return state, systemConflict("invalid recovery epoch")
		}
		recovery := *command.Recovery
		next.Recovery = &recovery
		next.SystemEpoch = recovery.Epoch
		next.ServeGate, next.WriteGate, next.CutoverGate = false, false, false
	case SystemConfirmRecoveryDrain:
		if state.Recovery == nil || state.Recovery.Phase != RecoveryPreparing ||
			state.Recovery.PermitDrainComplete || command.RecoveryDrain == nil ||
			command.RecoveryDrain.RecoveryEpoch != state.Recovery.Epoch ||
			command.RecoveryDrain.TargetRegistryGeneration != state.Recovery.TargetRegistryGeneration ||
			command.RecoveryDrain.TargetRegistryLayoutDigest != state.Recovery.TargetRegistryLayoutDigest ||
			command.RecoveryDrain.WaitedMillis < state.ServePermitMaxMillis {
			return state, systemConflict("recovery permit drain confirmation is invalid")
		}
		recovery := *state.Recovery
		recovery.PermitDrainComplete = true
		recovery.PermitDrainIndex = index
		next.Recovery = &recovery
	case SystemAdvanceRecovery:
		if state.Recovery == nil || command.RecoveryAdvance == nil ||
			state.Recovery.Phase != command.RecoveryAdvance.From ||
			!state.Recovery.PermitDrainComplete ||
			!validRecoveryAdvance(command.RecoveryAdvance.From, command.RecoveryAdvance.To) {
			return state, systemConflict("recovery phase conflict")
		}
		if command.RecoveryAdvance.To == RecoveryClosed {
			next.Recovery = nil
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

func registryGenerationClosureProofDigest(state SystemState, closure RegistryGenerationClosure) string {
	return consensusClosureProofDigest(
		state.ClusterID, state.RegistryGeneration, state.ActiveRegistryLayoutDigest,
		closure.TargetRegistryGeneration, closure.TargetRegistryLayoutIntentDigest, state.ServePermitMaxMillis,
		closure.CommitIndex,
	)
}

func consensusClosureProofDigest(
	clusterID string,
	sourceRegistryGeneration string,
	sourceRegistryLayoutDigest string,
	targetRegistryGeneration string,
	targetRegistryLayoutIntentDigest string,
	servePermitMaxMillis uint64,
	commitIndex uint64,
) string {
	var payload bytes.Buffer
	payload.WriteString("kuasar-registry-generation-closure-v1")
	for _, field := range []string{
		clusterID, sourceRegistryGeneration, sourceRegistryLayoutDigest,
		targetRegistryGeneration, targetRegistryLayoutIntentDigest,
	} {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(field)))
		payload.Write(length[:])
		payload.WriteString(field)
	}
	var permitLifetime [8]byte
	binary.BigEndian.PutUint64(permitLifetime[:], servePermitMaxMillis)
	payload.Write(permitLifetime[:])
	var index [8]byte
	binary.BigEndian.PutUint64(index[:], commitIndex)
	payload.Write(index[:])
	digest := sha256.Sum256(payload.Bytes())
	return hex.EncodeToString(digest[:])
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
		from == TransitionPromoted && to == TransitionComplete ||
		from == TransitionComplete && to == TransitionEpochRetired
}

func allTransitionsComplete(shards []ShardTransition) bool {
	return allTransitionsAtStage(shards, TransitionComplete)
}

func allTransitionsAtStage(shards []ShardTransition, stage TransitionStage) bool {
	for _, shard := range shards {
		if shard.Stage != stage {
			return false
		}
	}
	return true
}

func allTransitionsAtLeastComplete(shards []ShardTransition) bool {
	for _, shard := range shards {
		if shard.Stage != TransitionComplete && shard.Stage != TransitionEpochRetired {
			return false
		}
	}
	return true
}

func anyTransitionAtStage(shards []ShardTransition, stage TransitionStage) bool {
	for _, shard := range shards {
		if shard.Stage == stage {
			return true
		}
	}
	return false
}

func validateRegistryLayoutTransitionState(transition RegistryLayoutTransition, virtualShards uint32) error {
	if transition.Version == 0 || !isSHA256(transition.Digest) || !isSHA256(transition.PreviousDigest) ||
		transition.Digest == transition.PreviousDigest || transition.NextSystemEpoch == 0 ||
		len(transition.Shards) != int(virtualShards)+1 {
		return errors.New("raftstore: incomplete registryLayout transition")
	}
	for index, shard := range transition.Shards {
		want := uint32(index - 1)
		if index == 0 {
			want = ^uint32(0)
		}
		if shard.ShardID != want {
			return errors.New("raftstore: transition shards are not complete and ordered")
		}
		switch shard.Stage {
		case TransitionPending, TransitionCatchingUp, TransitionPromoted, TransitionComplete,
			TransitionEpochRetired:
		default:
			return errors.New("raftstore: invalid transition stage")
		}
	}
	return nil
}

func validRecoveryAdvance(from, to RecoveryPhase) bool {
	return from == RecoveryPreparing && to == RecoveryCollecting ||
		from == RecoveryCollecting && to == RecoveryReconciling ||
		from == RecoveryReconciling && to == RecoveryFinalizing ||
		from == RecoveryFinalizing && to == RecoveryClosed
}
