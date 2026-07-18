package raftstore

import (
	"context"
	"errors"
	"fmt"
)

type ReplicaCatchUpProof struct {
	ShardID        uint64 `json:"shard_id"`
	ReplicaID      uint64 `json:"replica_id"`
	ManifestDigest string `json:"manifest_digest"`
	LeaderApplied  uint64 `json:"leader_applied"`
	TargetApplied  uint64 `json:"target_applied"`
}

func (p ReplicaCatchUpProof) Validate() error {
	if p.ShardID == 0 || p.ReplicaID == 0 || !isSHA256(p.ManifestDigest) ||
		p.LeaderApplied == 0 || p.TargetApplied < p.LeaderApplied {
		return errors.New("raftstore: replica catch-up proof is incomplete")
	}
	return nil
}

func (r *Runtime) AddNonVoting(ctx context.Context, shardID, replicaID uint64, memberID string) error {
	target, err := r.desiredReplicaTarget(shardID, replicaID, memberID)
	if err != nil {
		return err
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
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
	if err := r.nodeHost.SyncRequestAddNonVoting(ctx, shardID, replicaID, target, membership.ConfigChangeID); err != nil {
		return err
	}
	return r.verifyMembership(ctx, shardID, func(current *membershipView) bool {
		return current.nonVotings[replicaID] == target
	})
}

func (r *Runtime) PromoteNonVoting(ctx context.Context, proof ReplicaCatchUpProof) error {
	if err := proof.Validate(); err != nil {
		return err
	}
	if proof.ManifestDigest != r.manifestDigest {
		return errors.New("raftstore: catch-up proof belongs to another manifest")
	}
	placement, err := r.placementForRaftShard(proof.ShardID)
	if err != nil {
		return err
	}
	if !placementContainsReplica(placement, proof.ReplicaID) {
		return errors.New("raftstore: promoted replica is absent from the desired manifest")
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, proof.ShardID)
	if err != nil {
		return err
	}
	if _, found := membership.Nodes[proof.ReplicaID]; found {
		return nil
	}
	if _, found := membership.NonVotings[proof.ReplicaID]; !found {
		return errors.New("raftstore: target replica is not a non-voting member")
	}
	if err := r.nodeHost.SyncRequestAddReplica(
		ctx, proof.ShardID, proof.ReplicaID, "", membership.ConfigChangeID,
	); err != nil {
		return err
	}
	return r.verifyMembership(ctx, proof.ShardID, func(current *membershipView) bool {
		_, voting := current.nodes[proof.ReplicaID]
		_, nonVoting := current.nonVotings[proof.ReplicaID]
		return voting && !nonVoting
	})
}

func (r *Runtime) RemoveOldReplica(ctx context.Context, shardID, replicaID uint64) error {
	placement, err := r.placementForRaftShard(shardID)
	if err != nil {
		return err
	}
	if placementContainsReplica(placement, replicaID) {
		return errors.New("raftstore: desired manifest replica cannot be removed")
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, shardID)
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
	if err := r.nodeHost.SyncRequestDeleteReplica(ctx, shardID, replicaID, membership.ConfigChangeID); err != nil {
		return err
	}
	return r.verifyMembership(ctx, shardID, func(current *membershipView) bool {
		_, removed := current.removed[replicaID]
		return removed
	})
}

func (r *Runtime) RemoveJoiningReplica(ctx context.Context, shardID, replicaID uint64) error {
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	if _, removed := membership.Removed[replicaID]; removed {
		return nil
	}
	if _, found := membership.NonVotings[replicaID]; !found {
		return errors.New("raftstore: abort target is not a non-voting replica")
	}
	if err := r.nodeHost.SyncRequestDeleteReplica(ctx, shardID, replicaID, membership.ConfigChangeID); err != nil {
		return err
	}
	return r.verifyMembership(ctx, shardID, func(current *membershipView) bool {
		_, removed := current.removed[replicaID]
		return removed
	})
}

func (r *Runtime) MarkLocalReplicaPromoted(ctx context.Context, shardID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	position := r.replicaPosition(shardID)
	if position < 0 {
		return ErrNoLocalReplica
	}
	replica := r.enrollment.Replicas[position]
	if replica.LocalState != ReplicaActive {
		return errors.New("raftstore: local replica is not active")
	}
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	if _, voting := membership.Nodes[replica.ReplicaID]; !voting {
		return errors.New("raftstore: consensus membership has not promoted the local replica")
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
	defer r.mu.Unlock()
	position := r.replicaPosition(shardID)
	if position < 0 {
		return ErrNoLocalReplica
	}
	replica := r.enrollment.Replicas[position]
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, shardID)
	if err != nil {
		return err
	}
	if _, removed := membership.Removed[replica.ReplicaID]; !removed {
		return errors.New("raftstore: local replica data cannot be removed before committed membership removal")
	}
	if err := r.nodeHost.StopReplica(shardID, replica.ReplicaID); err != nil {
		return err
	}
	if err := r.nodeHost.SyncRemoveData(ctx, shardID, replica.ReplicaID); err != nil {
		return err
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
	membership, err := r.nodeHost.SyncGetShardMembership(ctx, shardID)
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
		member, found := manifestMember(r.manifest, memberID)
		if !found {
			return "", errors.New("raftstore: desired replica member is unknown")
		}
		return member.RaftEndpoint, nil
	}
	return "", fmt.Errorf("raftstore: shard %d replica %d is absent from the desired manifest", shardID, replicaID)
}

func placementContainsReplica(placement []ReplicaPlacement, replicaID uint64) bool {
	for _, replica := range placement {
		if replica.ReplicaID == replicaID {
			return true
		}
	}
	return false
}
