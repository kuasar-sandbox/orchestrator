package raftstore

import "errors"

// AuthorizeServe applies the locally cached, monotonic Serve Permit to a
// control-plane operation. It performs no System Group read and therefore does
// not turn the permit into a per-request consensus dependency.
func (r *Runtime) AuthorizeServe(identity PermitIdentity, operation PermitOperation) error {
	if r == nil || r.permitCache == nil {
		return ErrPermitMissing
	}
	return r.permitCache.Authorize(identity, operation)
}

// RegistryLayoutSnapshot returns the verified immutable Registry History Generation's Registry Layout and the
// local Registry member identity. Callers receive copies of every slice/map
// that they could otherwise mutate.
func (r *Runtime) RegistryLayoutSnapshot() (RegistryLayout, string, RegistryMember) {
	if r == nil {
		return RegistryLayout{}, "", RegistryMember{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	registryLayout := CloneRegistryLayout(r.registryLayout)
	return registryLayout, r.registryLayoutDigest, r.member
}

// LocalDataShardLeader is a process-local scheduling hint. Consensus CAS still
// protects correctness; callers use this only to avoid duplicate coordinator
// work on every replica.
func (r *Runtime) LocalDataShardLeader(shardID uint32) (bool, error) {
	if r == nil || r.nodeHost == nil || int(shardID) >= len(r.registryLayout.DataShards) {
		return false, ErrNoLocalReplica
	}
	r.mu.Lock()
	position := r.replicaPosition(DataRaftShardID(shardID))
	if position < 0 {
		r.mu.Unlock()
		return false, ErrNoLocalReplica
	}
	local := r.enrollment.Replicas[position]
	r.mu.Unlock()
	if local.LocalState != ReplicaActive || local.NonVoting {
		return false, ErrNoLocalReplica
	}
	leaderID, _, valid, err := r.nodeHost.GetLeaderID(DataRaftShardID(shardID))
	if err != nil {
		return false, err
	}
	if !valid || leaderID == 0 {
		return false, errors.New("raftstore: data shard has no known leader")
	}
	return leaderID == local.ReplicaID, nil
}

func (r *Runtime) LocalSystemLeader() (bool, error) {
	if r == nil || r.nodeHost == nil {
		return false, ErrNoLocalReplica
	}
	if !r.HasLocalSystemReplica() {
		return false, nil
	}
	r.mu.Lock()
	position := r.replicaPosition(SystemRaftShardID)
	if position < 0 {
		r.mu.Unlock()
		return false, ErrNoLocalReplica
	}
	local := r.enrollment.Replicas[position]
	r.mu.Unlock()
	if local.LocalState != ReplicaActive || local.NonVoting {
		return false, ErrNoLocalReplica
	}
	leaderID, _, valid, err := r.nodeHost.GetLeaderID(SystemRaftShardID)
	if err != nil {
		return false, err
	}
	if !valid || leaderID == 0 {
		return false, errors.New("raftstore: System Group has no known leader")
	}
	return leaderID == local.ReplicaID, nil
}

func CloneRegistryLayout(source RegistryLayout) RegistryLayout {
	clone := source
	if source.Predecessor != nil {
		predecessor := *source.Predecessor
		clone.Predecessor = &predecessor
	}
	clone.Members = append([]RegistryMember(nil), source.Members...)
	clone.SystemReplicas = append([]ReplicaPlacement(nil), source.SystemReplicas...)
	clone.DataShards = make([]ShardPlacement, len(source.DataShards))
	for index, shard := range source.DataShards {
		clone.DataShards[index] = ShardPlacement{
			ShardID:  shard.ShardID,
			Replicas: append([]ReplicaPlacement(nil), shard.Replicas...),
		}
	}
	return clone
}
