package raftstore

import (
	"context"
	"errors"
	"fmt"

	dragonboat "github.com/lni/dragonboat/v4"
)

type ReplicaCatchUpProof struct {
	ShardID              uint64 `json:"shard_id"`
	ReplicaID            uint64 `json:"replica_id"`
	MemberID             string `json:"member_id"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	AppliedIndex         uint64 `json:"applied_index"`
}

type ReplicaCatchUpRequest struct {
	ShardID              uint64 `json:"shard_id"`
	ReplicaID            uint64 `json:"replica_id"`
	MemberID             string `json:"member_id"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	MinimumAppliedIndex  uint64 `json:"minimum_applied_index"`
}

func (r ReplicaCatchUpRequest) Validate() error {
	if r.ShardID == 0 || r.ReplicaID == 0 || r.MemberID == "" || !isSHA256(r.RegistryLayoutDigest) ||
		r.MinimumAppliedIndex == 0 {
		return errors.New("raftstore: replica catch-up request is incomplete")
	}
	return nil
}

func (p ReplicaCatchUpProof) Validate(request ReplicaCatchUpRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if p.ShardID != request.ShardID || p.ReplicaID != request.ReplicaID || p.MemberID != request.MemberID ||
		p.RegistryLayoutDigest != request.RegistryLayoutDigest || p.AppliedIndex < request.MinimumAppliedIndex {
		return errors.New("raftstore: replica catch-up proof is incomplete")
	}
	return nil
}

type ReplicaPromotionRequest struct {
	ShardID              uint64 `json:"shard_id"`
	ReplicaID            uint64 `json:"replica_id"`
	MemberID             string `json:"member_id"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
}

type ReplicaAppliedRequest struct {
	ShardID              uint64 `json:"shard_id"`
	ReplicaID            uint64 `json:"replica_id"`
	MemberID             string `json:"member_id"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	MinimumAppliedIndex  uint64 `json:"minimum_applied_index"`
}

func (r ReplicaAppliedRequest) Validate() error {
	if r.ShardID < firstDataRaftShardID || r.ReplicaID == 0 || r.MemberID == "" ||
		!isSHA256(r.RegistryLayoutDigest) || r.MinimumAppliedIndex == 0 {
		return errors.New("raftstore: replica applied-index request is incomplete")
	}
	return nil
}

type ReplicaAppliedProof struct {
	ShardID              uint64 `json:"shard_id"`
	ReplicaID            uint64 `json:"replica_id"`
	MemberID             string `json:"member_id"`
	RegistryLayoutDigest string `json:"registry_layout_digest"`
	AppliedIndex         uint64 `json:"applied_index"`
}

func (p ReplicaAppliedProof) Validate(request ReplicaAppliedRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if p.ShardID != request.ShardID || p.ReplicaID != request.ReplicaID || p.MemberID != request.MemberID ||
		p.RegistryLayoutDigest != request.RegistryLayoutDigest || p.AppliedIndex < request.MinimumAppliedIndex {
		return errors.New("raftstore: replica applied-index proof is incomplete")
	}
	return nil
}

func (r ReplicaPromotionRequest) Validate() error {
	if r.ShardID == 0 || r.ReplicaID == 0 || r.MemberID == "" || !isSHA256(r.RegistryLayoutDigest) {
		return errors.New("raftstore: replica promotion request is incomplete")
	}
	return nil
}

// ReplicaTransitionClient calls the target Registry member over the
// authenticated internal control plane. Implementations must route both calls
// to the exact MemberID.
type ReplicaTransitionClient interface {
	ProbeReplicaCatchUp(context.Context, ReplicaCatchUpRequest) (ReplicaCatchUpProof, error)
	ProbeReplicaApplied(context.Context, ReplicaAppliedRequest) (ReplicaAppliedProof, error)
	ConfirmReplicaPromoted(context.Context, ReplicaPromotionRequest) error
}

type ReplicaTransitionClientFuncs struct {
	Probe   func(context.Context, ReplicaCatchUpRequest) (ReplicaCatchUpProof, error)
	Applied func(context.Context, ReplicaAppliedRequest) (ReplicaAppliedProof, error)
	Confirm func(context.Context, ReplicaPromotionRequest) error
}

func (f ReplicaTransitionClientFuncs) ProbeReplicaApplied(
	ctx context.Context,
	request ReplicaAppliedRequest,
) (ReplicaAppliedProof, error) {
	if f.Applied == nil {
		return ReplicaAppliedProof{}, errors.New("raftstore: remote replica applied-index probe is unavailable")
	}
	return f.Applied(ctx, request)
}

func (f ReplicaTransitionClientFuncs) ProbeReplicaCatchUp(
	ctx context.Context,
	request ReplicaCatchUpRequest,
) (ReplicaCatchUpProof, error) {
	if f.Probe == nil {
		return ReplicaCatchUpProof{}, errors.New("raftstore: remote replica catch-up probe is unavailable")
	}
	return f.Probe(ctx, request)
}

func (f ReplicaTransitionClientFuncs) ConfirmReplicaPromoted(
	ctx context.Context,
	request ReplicaPromotionRequest,
) error {
	if f.Confirm == nil {
		return errors.New("raftstore: remote replica promotion confirmation is unavailable")
	}
	return f.Confirm(ctx, request)
}

// ProveLocalReplicaCaughtUp executes a linearizable read on the exact local
// learner. It is the target-side handler for the authenticated catch-up probe;
// callers cannot substitute process-local counters or heartbeat observations.
func (r *Runtime) ProveLocalReplicaCaughtUp(
	ctx context.Context,
	request ReplicaCatchUpRequest,
) (ReplicaCatchUpProof, error) {
	if err := request.Validate(); err != nil {
		return ReplicaCatchUpProof{}, err
	}
	if request.RegistryLayoutDigest != r.registryLayoutDigest || request.MemberID != r.member.MemberID {
		return ReplicaCatchUpProof{}, errors.New("raftstore: catch-up request targets another registryLayout member")
	}
	placement, err := r.placementForRaftShard(request.ShardID)
	if err != nil {
		return ReplicaCatchUpProof{}, err
	}
	placed := false
	for _, replica := range placement {
		if replica.ReplicaID == request.ReplicaID && replica.MemberID == request.MemberID {
			placed = true
			break
		}
	}
	if !placed {
		return ReplicaCatchUpProof{}, errors.New("raftstore: local learner is absent from the desired registryLayout")
	}

	r.mu.Lock()
	position := r.replicaPosition(request.ShardID)
	if position < 0 {
		r.mu.Unlock()
		return ReplicaCatchUpProof{}, ErrNoLocalReplica
	}
	local := r.enrollment.Replicas[position]
	r.mu.Unlock()
	if local.ReplicaID != request.ReplicaID || local.LocalState != ReplicaActive || !local.NonVoting {
		return ReplicaCatchUpProof{}, errors.New("raftstore: requested local learner is not active and non-voting")
	}
	membership, err := r.syncGetShardMembership(ctx, request.ShardID)
	if err != nil {
		return ReplicaCatchUpProof{}, err
	}
	if membership.NonVotings[request.ReplicaID] != r.member.RaftEndpoint {
		return ReplicaCatchUpProof{}, errors.New("raftstore: consensus membership does not bind the local learner")
	}

	var applied uint64
	if request.ShardID == SystemRaftShardID {
		state, err := r.ReadSystemStrong(ctx)
		if err != nil {
			return ReplicaCatchUpProof{}, err
		}
		if _, err := r.requireRegistryLayoutTransition(state); err != nil {
			return ReplicaCatchUpProof{}, err
		}
		applied = state.LastApplied
	} else {
		logicalID, ok := LogicalShardID(request.ShardID)
		if !ok {
			return ReplicaCatchUpProof{}, errors.New("raftstore: unknown Raft shard")
		}
		state, err := r.readDataStateStrong(ctx, request.ShardID)
		if err != nil {
			return ReplicaCatchUpProof{}, err
		}
		if err := r.validateDataState(state, logicalID); err != nil {
			return ReplicaCatchUpProof{}, err
		}
		prepared := false
		for _, epoch := range state.ServingEpochs {
			if epoch.RegistryLayoutDigest == request.RegistryLayoutDigest {
				prepared = true
				break
			}
		}
		if !prepared {
			return ReplicaCatchUpProof{}, errors.New("raftstore: local learner has not applied the target registryLayout epoch")
		}
		applied = state.LastApplied
	}
	proof := ReplicaCatchUpProof{
		ShardID: request.ShardID, ReplicaID: request.ReplicaID, MemberID: request.MemberID,
		RegistryLayoutDigest: request.RegistryLayoutDigest, AppliedIndex: applied,
	}
	if err := proof.Validate(request); err != nil {
		return ReplicaCatchUpProof{}, err
	}
	return proof, nil
}

// ProveLocalReplicaApplied performs a linearizable read on one exact active
// voter. It is used only by the fence-compaction coordinator.
func (r *Runtime) ProveLocalReplicaApplied(
	ctx context.Context,
	request ReplicaAppliedRequest,
) (ReplicaAppliedProof, error) {
	if err := request.Validate(); err != nil {
		return ReplicaAppliedProof{}, err
	}
	if request.RegistryLayoutDigest != r.registryLayoutDigest || request.MemberID != r.member.MemberID {
		return ReplicaAppliedProof{}, errors.New("raftstore: applied-index request targets another registryLayout member")
	}
	logicalID, ok := LogicalShardID(request.ShardID)
	if !ok || int(logicalID) >= len(r.registryLayout.DataShards) {
		return ReplicaAppliedProof{}, errors.New("raftstore: applied-index request targets an unknown data shard")
	}
	placement := r.registryLayout.DataShards[logicalID].Replicas
	placed := false
	for _, replica := range placement {
		if replica.ReplicaID == request.ReplicaID && replica.MemberID == request.MemberID {
			placed = true
			break
		}
	}
	if !placed {
		return ReplicaAppliedProof{}, errors.New("raftstore: local voter is absent from the active registryLayout")
	}
	r.mu.Lock()
	position := r.replicaPosition(request.ShardID)
	if position < 0 {
		r.mu.Unlock()
		return ReplicaAppliedProof{}, ErrNoLocalReplica
	}
	local := r.enrollment.Replicas[position]
	r.mu.Unlock()
	if local.ReplicaID != request.ReplicaID || local.LocalState != ReplicaActive || local.NonVoting {
		return ReplicaAppliedProof{}, errors.New("raftstore: requested local voter is not active")
	}
	membership, err := r.syncGetShardMembership(ctx, request.ShardID)
	if err != nil {
		return ReplicaAppliedProof{}, err
	}
	if membership.Nodes[request.ReplicaID] != r.member.RaftEndpoint {
		return ReplicaAppliedProof{}, errors.New("raftstore: consensus membership does not bind the local voter")
	}
	state, err := r.readDataStateStrong(ctx, request.ShardID)
	if err != nil {
		return ReplicaAppliedProof{}, err
	}
	if err := r.validateDataState(state, logicalID); err != nil {
		return ReplicaAppliedProof{}, err
	}
	if len(state.ServingEpochs) != 1 || state.ServingEpochs[0].RegistryLayoutDigest != request.RegistryLayoutDigest {
		return ReplicaAppliedProof{}, errors.New("raftstore: local voter does not serve the stable active registryLayout")
	}
	proof := ReplicaAppliedProof{
		ShardID: request.ShardID, ReplicaID: request.ReplicaID, MemberID: request.MemberID,
		RegistryLayoutDigest: request.RegistryLayoutDigest, AppliedIndex: state.LastApplied,
	}
	if err := proof.Validate(request); err != nil {
		return ReplicaAppliedProof{}, err
	}
	return proof, nil
}

func (r *Runtime) probeReplicaCatchUp(
	ctx context.Context,
	request ReplicaCatchUpRequest,
) (ReplicaCatchUpProof, error) {
	if request.MemberID == r.member.MemberID {
		return r.ProveLocalReplicaCaughtUp(ctx, request)
	}
	if r.transitionClient == nil {
		return ReplicaCatchUpProof{}, errors.New("raftstore: authenticated remote replica catch-up probe is unavailable")
	}
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	proof, err := r.transitionClient.ProbeReplicaCatchUp(operation, request)
	if err != nil {
		return ReplicaCatchUpProof{}, err
	}
	if err := proof.Validate(request); err != nil {
		return ReplicaCatchUpProof{}, err
	}
	return proof, nil
}

func (r *Runtime) probeReplicaApplied(
	ctx context.Context,
	request ReplicaAppliedRequest,
) (ReplicaAppliedProof, error) {
	if request.MemberID == r.member.MemberID {
		return r.ProveLocalReplicaApplied(ctx, request)
	}
	if r.transitionClient == nil {
		return ReplicaAppliedProof{}, errors.New("raftstore: authenticated remote applied-index probe is unavailable")
	}
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	proof, err := r.transitionClient.ProbeReplicaApplied(operation, request)
	if err != nil {
		return ReplicaAppliedProof{}, err
	}
	if err := proof.Validate(request); err != nil {
		return ReplicaAppliedProof{}, err
	}
	return proof, nil
}

func (r *Runtime) confirmReplicaPromoted(ctx context.Context, request ReplicaPromotionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.MemberID == r.member.MemberID {
		return r.ConfirmLocalReplicaPromoted(ctx, request)
	}
	if r.transitionClient == nil {
		return errors.New("raftstore: authenticated remote replica promotion confirmation is unavailable")
	}
	operation, cancel := r.operationContext(ctx)
	defer cancel()
	return r.transitionClient.ConfirmReplicaPromoted(operation, request)
}

func (r *Runtime) AddNonVoting(ctx context.Context, shardID, replicaID uint64, memberID string) error {
	target, err := r.desiredReplicaTarget(shardID, replicaID, memberID)
	if err != nil {
		return err
	}
	membership, err := r.syncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	if owner, found := membershipReplicaAtTarget(membership, target); found && owner != replicaID {
		return fmt.Errorf("raftstore: Raft target is already bound to replica %d in shard %d", owner, shardID)
	}
	if _, removed := membership.Removed[replicaID]; removed {
		return errors.New("raftstore: removed replica ID cannot be reused in a shard")
	}
	if current, found := membership.Nodes[replicaID]; found {
		if current != target {
			return errors.New("raftstore: replica ID is bound to another target")
		}
		return nil
	}
	if current, found := membership.NonVotings[replicaID]; found {
		if current != target {
			return errors.New("raftstore: non-voting replica ID is bound to another target")
		}
		return nil
	}
	requestErr := r.syncRequestAddNonVoting(ctx, shardID, replicaID, target, membership.ConfigChangeID)
	verifyErr := r.verifyMembership(ctx, shardID, func(current *membershipView) bool {
		return current.nonVotings[replicaID] == target
	})
	if verifyErr == nil {
		return nil
	}
	if requestErr != nil {
		return requestErr
	}
	return verifyErr
}

func membershipReplicaAtTarget(membership *dragonboat.Membership, target string) (uint64, bool) {
	for replicaID, current := range membership.Nodes {
		if current == target {
			return replicaID, true
		}
	}
	for replicaID, current := range membership.NonVotings {
		if current == target {
			return replicaID, true
		}
	}
	for replicaID, current := range membership.Witnesses {
		if current == target {
			return replicaID, true
		}
	}
	return 0, false
}

func (r *Runtime) promoteNonVoting(
	ctx context.Context,
	request ReplicaCatchUpRequest,
	proof ReplicaCatchUpProof,
) error {
	if err := proof.Validate(request); err != nil {
		return err
	}
	if proof.RegistryLayoutDigest != r.registryLayoutDigest {
		return errors.New("raftstore: catch-up proof belongs to another registryLayout")
	}
	placement, err := r.placementForRaftShard(proof.ShardID)
	if err != nil {
		return err
	}
	if !placementContainsReplica(placement, proof.ReplicaID) {
		return errors.New("raftstore: promoted replica is absent from the desired registryLayout")
	}
	var expectedTarget string
	for _, replica := range placement {
		if replica.ReplicaID != proof.ReplicaID {
			continue
		}
		if replica.MemberID != proof.MemberID {
			return errors.New("raftstore: catch-up proof names the wrong registryLayout member")
		}
		expectedTarget, err = r.desiredReplicaTarget(proof.ShardID, replica.ReplicaID, replica.MemberID)
		if err != nil {
			return err
		}
		break
	}
	membership, err := r.syncGetShardMembership(ctx, proof.ShardID)
	if err != nil {
		return err
	}
	if current, found := membership.Nodes[proof.ReplicaID]; found {
		if current != expectedTarget {
			return errors.New("raftstore: voting replica ID is bound to another target")
		}
		return nil
	}
	target, found := membership.NonVotings[proof.ReplicaID]
	if !found {
		return errors.New("raftstore: target replica is not a non-voting member")
	}
	if target != expectedTarget {
		return errors.New("raftstore: non-voting replica ID is bound to another target")
	}
	requestErr := r.syncRequestAddReplica(
		ctx, proof.ShardID, proof.ReplicaID, target, membership.ConfigChangeID,
	)
	verifyErr := r.verifyMembership(ctx, proof.ShardID, func(current *membershipView) bool {
		votingTarget, voting := current.nodes[proof.ReplicaID]
		_, nonVoting := current.nonVotings[proof.ReplicaID]
		return voting && votingTarget == expectedTarget && !nonVoting
	})
	if verifyErr == nil {
		return nil
	}
	if requestErr != nil {
		return requestErr
	}
	return verifyErr
}

func (r *Runtime) RemoveOldReplica(ctx context.Context, shardID, replicaID uint64) error {
	placement, err := r.placementForRaftShard(shardID)
	if err != nil {
		return err
	}
	if placementContainsReplica(placement, replicaID) {
		return errors.New("raftstore: desired registryLayout replica cannot be removed")
	}
	membership, err := r.syncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	if _, removed := membership.Removed[replicaID]; removed {
		return nil
	}
	if _, found := membership.Nodes[replicaID]; !found {
		return errors.New("raftstore: old voting replica is absent")
	}
	for _, desired := range placement {
		if _, ready := membership.Nodes[desired.ReplicaID]; !ready {
			return errors.New("raftstore: cannot remove old replica before every desired voter is promoted")
		}
	}
	if len(membership.Nodes) < int(DefaultReplication)+1 {
		return errors.New("raftstore: removal would reduce the voting set before replacement")
	}
	requestErr := r.syncRequestDeleteReplica(ctx, shardID, replicaID, membership.ConfigChangeID)
	verifyErr := r.verifyMembership(ctx, shardID, func(current *membershipView) bool {
		_, removed := current.removed[replicaID]
		return removed
	})
	if verifyErr == nil {
		return nil
	}
	if requestErr != nil {
		return requestErr
	}
	return verifyErr
}

func (r *Runtime) RemoveJoiningReplica(ctx context.Context, shardID, replicaID uint64) error {
	membership, err := r.syncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	if _, removed := membership.Removed[replicaID]; removed {
		return nil
	}
	if _, found := membership.NonVotings[replicaID]; !found {
		return errors.New("raftstore: abort target is not a non-voting replica")
	}
	requestErr := r.syncRequestDeleteReplica(ctx, shardID, replicaID, membership.ConfigChangeID)
	verifyErr := r.verifyMembership(ctx, shardID, func(current *membershipView) bool {
		_, removed := current.removed[replicaID]
		return removed
	})
	if verifyErr == nil {
		return nil
	}
	if requestErr != nil {
		return requestErr
	}
	return verifyErr
}

func (r *Runtime) ConfirmLocalReplicaPromoted(ctx context.Context, request ReplicaPromotionRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if request.RegistryLayoutDigest != r.registryLayoutDigest || request.MemberID != r.member.MemberID {
		return errors.New("raftstore: promotion confirmation targets another registryLayout member")
	}
	placement, err := r.placementForRaftShard(request.ShardID)
	if err != nil {
		return err
	}
	placed := false
	for _, replica := range placement {
		if replica.ReplicaID == request.ReplicaID && replica.MemberID == request.MemberID {
			placed = true
			break
		}
	}
	if !placed {
		return errors.New("raftstore: promoted local replica is absent from the desired registryLayout")
	}
	r.mu.Lock()
	position := r.replicaPosition(request.ShardID)
	if position < 0 {
		r.mu.Unlock()
		return ErrNoLocalReplica
	}
	replica := r.enrollment.Replicas[position]
	r.mu.Unlock()
	if replica.ReplicaID != request.ReplicaID || replica.LocalState != ReplicaActive {
		return errors.New("raftstore: local replica is not active")
	}
	membership, err := r.syncGetShardMembership(ctx, request.ShardID)
	if err != nil {
		return err
	}
	if membership.Nodes[replica.ReplicaID] != r.member.RaftEndpoint {
		return errors.New("raftstore: consensus membership has not promoted the local replica")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	position = r.replicaPosition(request.ShardID)
	if position < 0 || r.enrollment.Replicas[position].ReplicaID != request.ReplicaID ||
		r.enrollment.Replicas[position].LocalState != ReplicaActive {
		return errors.New("raftstore: local replica enrollment changed during promotion confirmation")
	}
	if !r.enrollment.Replicas[position].NonVoting {
		return nil
	}
	next := r.enrollment
	next.Replicas = append([]LocalReplicaEnrollment(nil), r.enrollment.Replicas...)
	next.Replicas[position].NonVoting = false
	if err := r.enrollmentStore.Store(next); err != nil {
		return err
	}
	r.enrollment = next
	return nil
}

func (r *Runtime) RemoveLocalReplicaData(ctx context.Context, shardID uint64) error {
	r.mu.Lock()
	position := r.replicaPosition(shardID)
	if position < 0 {
		r.mu.Unlock()
		return ErrNoLocalReplica
	}
	replica := r.enrollment.Replicas[position]
	switch replica.LocalState {
	case ReplicaRemoved:
		r.mu.Unlock()
		return nil
	case ReplicaRemoving:
		r.mu.Unlock()
	case ReplicaActive:
		r.mu.Unlock()
		membership, err := r.syncGetShardMembership(ctx, shardID)
		if err != nil {
			return err
		}
		if _, removed := membership.Removed[replica.ReplicaID]; !removed {
			return errors.New("raftstore: local replica data cannot be removed before committed membership removal")
		}
		r.mu.Lock()
		position = r.replicaPosition(shardID)
		if position < 0 || r.enrollment.Replicas[position] != replica {
			r.mu.Unlock()
			return errors.New("raftstore: local replica enrollment changed during removal")
		}
		if err := r.setReplicaState(position, ReplicaRemoving); err != nil {
			r.mu.Unlock()
			return err
		}
		r.mu.Unlock()
	default:
		r.mu.Unlock()
		return errors.New("raftstore: only an active local replica can be removed")
	}
	if r.nodeHost.HasNodeInfo(shardID, replica.ReplicaID) {
		if err := r.nodeHost.StopReplica(shardID, replica.ReplicaID); err != nil &&
			!errors.Is(err, dragonboat.ErrShardNotFound) {
			return err
		}
	}
	if err := r.syncRemoveData(ctx, shardID, replica.ReplicaID); err != nil {
		return err
	}
	if err := r.stateEngine.RemoveReplica(shardID, replica.ReplicaID); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	position = r.replicaPosition(shardID)
	if position < 0 || r.enrollment.Replicas[position].ReplicaID != replica.ReplicaID ||
		r.enrollment.Replicas[position].LocalState != ReplicaRemoving {
		return errors.New("raftstore: local replica enrollment changed during removal")
	}
	return r.setReplicaState(position, ReplicaRemoved)
}

type membershipView struct {
	nodes      map[uint64]string
	nonVotings map[uint64]string
	removed    map[uint64]struct{}
}

func (r *Runtime) verifyMembership(
	ctx context.Context,
	shardID uint64,
	verify func(*membershipView) bool,
) error {
	membership, err := r.syncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	view := &membershipView{nodes: membership.Nodes, nonVotings: membership.NonVotings, removed: membership.Removed}
	if !verify(view) {
		return errors.New("raftstore: committed membership does not match the requested transition")
	}
	return nil
}

func (r *Runtime) desiredReplicaTarget(shardID, replicaID uint64, memberID string) (string, error) {
	placement, err := r.placementForRaftShard(shardID)
	if err != nil {
		return "", err
	}
	for _, replica := range placement {
		if replica.ReplicaID != replicaID || replica.MemberID != memberID {
			continue
		}
		member, found := registryLayoutMember(r.registryLayout, memberID)
		if !found {
			return "", errors.New("raftstore: desired replica member is unknown")
		}
		return member.RaftEndpoint, nil
	}
	return "", fmt.Errorf("raftstore: shard %d replica %d is absent from the desired registryLayout", shardID, replicaID)
}

func placementContainsReplica(placement []ReplicaPlacement, replicaID uint64) bool {
	for _, replica := range placement {
		if replica.ReplicaID == replicaID {
			return true
		}
	}
	return false
}
