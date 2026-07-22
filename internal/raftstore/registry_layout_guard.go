package raftstore

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type ShardReplicaIDHistory struct {
	ShardID    uint64   `json:"shard_id"`
	ReplicaIDs []uint64 `json:"replica_ids"`
}

type AcceptedRegistryLayout struct {
	ClusterID             string                  `json:"cluster_id"`
	RegistryGeneration    string                  `json:"registry_generation"`
	RegistryLayoutVersion uint64                  `json:"registry_layout_version"`
	RegistryLayoutDigest  string                  `json:"registry_layout_digest"`
	FormatVersion         uint32                  `json:"format_version"`
	SchemaVersion         uint32                  `json:"schema_version"`
	ProtocolVersion       uint32                  `json:"protocol_version"`
	HashVersion           string                  `json:"hash_version"`
	VirtualShardCount     uint32                  `json:"virtual_shard_count"`
	RouteBucketCount      uint32                  `json:"route_bucket_count"`
	BuildBucketCount      uint32                  `json:"build_bucket_count"`
	ReplicationFactor     uint32                  `json:"replication_factor"`
	ServePermitMaxMillis  uint64                  `json:"serve_permit_max_millis"`
	BootstrapTokenDigest  string                  `json:"bootstrap_token_digest"`
	ReplicaIDHistory      []ShardReplicaIDHistory `json:"replica_id_history"`
	RegistryLayout        RegistryLayout          `json:"registry_layout"`
}

func (a AcceptedRegistryLayout) Validate() error {
	if a.ClusterID == "" || a.RegistryGeneration == "" || a.RegistryLayoutVersion == 0 || !isSHA256(a.RegistryLayoutDigest) ||
		a.FormatVersion != RegistryLayoutFormatV1 || a.SchemaVersion == 0 || a.ProtocolVersion == 0 ||
		a.HashVersion != "ShardHashV1" || !isPowerOfTwo(a.VirtualShardCount) ||
		!isPowerOfTwo(a.RouteBucketCount) || !isPowerOfTwo(a.BuildBucketCount) ||
		a.ReplicationFactor != DefaultReplication || a.ServePermitMaxMillis == 0 ||
		!isSHA256(a.BootstrapTokenDigest) {
		return errors.New("raftstore: invalid accepted registryLayout state")
	}
	if err := a.RegistryLayout.Validate(); err != nil {
		return errors.New("raftstore: accepted state lacks its exact registryLayout artifact")
	}
	if err := validateReplicaIDHistory(a.RegistryLayout, a.ReplicaIDHistory); err != nil {
		return err
	}
	digest, err := a.RegistryLayout.Digest()
	if err != nil || digest != a.RegistryLayoutDigest || a.RegistryLayout.ClusterID != a.ClusterID ||
		a.RegistryLayout.RegistryGeneration != a.RegistryGeneration ||
		a.RegistryLayout.RegistryLayoutVersion != a.RegistryLayoutVersion ||
		a.RegistryLayout.FormatVersion != a.FormatVersion ||
		a.RegistryLayout.SchemaVersion != a.SchemaVersion ||
		a.RegistryLayout.ProtocolVersion != a.ProtocolVersion ||
		a.RegistryLayout.HashVersion != a.HashVersion ||
		a.RegistryLayout.VirtualShardCount != a.VirtualShardCount ||
		a.RegistryLayout.RouteBucketCount != a.RouteBucketCount ||
		a.RegistryLayout.BuildBucketCount != a.BuildBucketCount ||
		a.RegistryLayout.ReplicationFactor != a.ReplicationFactor ||
		a.RegistryLayout.ServePermitMaxMillis != a.ServePermitMaxMillis ||
		a.RegistryLayout.BootstrapTokenDigest != a.BootstrapTokenDigest {
		return errors.New("raftstore: accepted registryLayout artifact differs from its guard identity")
	}
	return nil
}

func (a AcceptedRegistryLayout) matchesFrozenParameters(next RegistryLayout) bool {
	return a.FormatVersion == next.FormatVersion &&
		a.SchemaVersion == next.SchemaVersion &&
		a.ProtocolVersion == next.ProtocolVersion &&
		a.HashVersion == next.HashVersion &&
		a.VirtualShardCount == next.VirtualShardCount &&
		a.RouteBucketCount == next.RouteBucketCount &&
		a.BuildBucketCount == next.BuildBucketCount &&
		a.ReplicationFactor == next.ReplicationFactor &&
		a.ServePermitMaxMillis == next.ServePermitMaxMillis &&
		a.BootstrapTokenDigest == next.BootstrapTokenDigest
}

func acceptedRegistryLayout(registryLayout RegistryLayout, digest string) AcceptedRegistryLayout {
	return AcceptedRegistryLayout{
		ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
		RegistryLayoutVersion: registryLayout.RegistryLayoutVersion, RegistryLayoutDigest: digest,
		FormatVersion: registryLayout.FormatVersion, SchemaVersion: registryLayout.SchemaVersion,
		ProtocolVersion: registryLayout.ProtocolVersion, HashVersion: registryLayout.HashVersion,
		VirtualShardCount: registryLayout.VirtualShardCount, RouteBucketCount: registryLayout.RouteBucketCount,
		BuildBucketCount: registryLayout.BuildBucketCount, ReplicationFactor: registryLayout.ReplicationFactor,
		ServePermitMaxMillis: registryLayout.ServePermitMaxMillis,
		BootstrapTokenDigest: registryLayout.BootstrapTokenDigest,
		ReplicaIDHistory:     replicaIDHistoryForLayout(registryLayout),
		RegistryLayout:       cloneRegistryLayout(registryLayout),
	}
}

func (a AcceptedRegistryLayout) Accept(next RegistryLayout, digest string) (AcceptedRegistryLayout, error) {
	if err := a.Validate(); err != nil {
		return AcceptedRegistryLayout{}, err
	}
	if err := next.Validate(); err != nil {
		return AcceptedRegistryLayout{}, err
	}
	if !isSHA256(digest) || next.ClusterID != a.ClusterID {
		return AcceptedRegistryLayout{}, errors.New("raftstore: unrelated registryLayout lineage")
	}
	if next.RegistryGeneration == a.RegistryGeneration {
		if !a.matchesFrozenParameters(next) {
			return AcceptedRegistryLayout{}, errors.New("raftstore: same Registry History Generation Registry Layout changed frozen parameters")
		}
		switch {
		case next.RegistryLayoutVersion < a.RegistryLayoutVersion:
			return AcceptedRegistryLayout{}, errors.New("raftstore: registryLayout rollback rejected")
		case next.RegistryLayoutVersion == a.RegistryLayoutVersion:
			if digest != a.RegistryLayoutDigest {
				return AcceptedRegistryLayout{}, errors.New("raftstore: registryLayout version equivocation")
			}
			return a, nil
		case next.RegistryLayoutVersion != a.RegistryLayoutVersion+1 ||
			next.PreviousRegistryLayoutVersion != a.RegistryLayoutVersion ||
			next.PreviousRegistryLayoutDigest != a.RegistryLayoutDigest:
			return AcceptedRegistryLayout{}, errors.New("raftstore: registryLayout lineage gap")
		}
		if err := validateRetainedReplicaTargets(a.RegistryLayout, next); err != nil {
			return AcceptedRegistryLayout{}, err
		}
		if err := validateReplicaIDHistoryAdvance(a.RegistryLayout, next, a.ReplicaIDHistory); err != nil {
			return AcceptedRegistryLayout{}, err
		}
	} else {
		if next.Predecessor == nil || next.Predecessor.RegistryGeneration != a.RegistryGeneration ||
			next.Predecessor.RegistryLayoutDigest != a.RegistryLayoutDigest ||
			next.Predecessor.ServePermitMaxMillis != a.ServePermitMaxMillis || next.RegistryLayoutVersion != 1 {
			return AcceptedRegistryLayout{}, errors.New("raftstore: Registry History Generation rollover is not linked to the accepted predecessor")
		}
	}
	accepted := acceptedRegistryLayout(next, digest)
	if next.RegistryGeneration == a.RegistryGeneration {
		accepted.ReplicaIDHistory = extendReplicaIDHistory(a.ReplicaIDHistory, next)
	}
	return accepted, nil
}

func replicaIDHistoryForLayout(registryLayout RegistryLayout) []ShardReplicaIDHistory {
	history := make([]ShardReplicaIDHistory, 0, len(registryLayout.DataShards)+1)
	appendShard := func(shardID uint64, replicas []ReplicaPlacement) {
		ids := make([]uint64, len(replicas))
		for index, replica := range replicas {
			ids[index] = replica.ReplicaID
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		history = append(history, ShardReplicaIDHistory{ShardID: shardID, ReplicaIDs: ids})
	}
	appendShard(SystemRaftShardID, registryLayout.SystemReplicas)
	for _, shard := range registryLayout.DataShards {
		appendShard(DataRaftShardID(shard.ShardID), shard.Replicas)
	}
	return history
}

func validateReplicaIDHistory(registryLayout RegistryLayout, history []ShardReplicaIDHistory) error {
	current := replicaIDHistoryForLayout(registryLayout)
	if len(history) != len(current) {
		return errors.New("raftstore: accepted registryLayout has incomplete replica ID history")
	}
	for index, shard := range history {
		if shard.ShardID != current[index].ShardID || len(shard.ReplicaIDs) < len(current[index].ReplicaIDs) {
			return errors.New("raftstore: accepted registryLayout has invalid replica ID history")
		}
		for position, replicaID := range shard.ReplicaIDs {
			if replicaID == 0 || position > 0 && replicaID <= shard.ReplicaIDs[position-1] {
				return errors.New("raftstore: accepted replica ID history is not unique and ordered")
			}
		}
		for _, replicaID := range current[index].ReplicaIDs {
			position := sort.Search(len(shard.ReplicaIDs), func(i int) bool { return shard.ReplicaIDs[i] >= replicaID })
			if position == len(shard.ReplicaIDs) || shard.ReplicaIDs[position] != replicaID {
				return errors.New("raftstore: accepted replica ID history omits an active replica")
			}
		}
	}
	return nil
}

func validateReplicaIDHistoryAdvance(previous, next RegistryLayout, history []ShardReplicaIDHistory) error {
	previousActive := replicaIDHistoryForLayout(previous)
	nextActive := replicaIDHistoryForLayout(next)
	for index, shard := range nextActive {
		for _, replicaID := range shard.ReplicaIDs {
			if containsReplicaID(previousActive[index].ReplicaIDs, replicaID) {
				continue
			}
			if containsReplicaID(history[index].ReplicaIDs, replicaID) {
				return fmt.Errorf("raftstore: shard %d reuses retired replica ID %d", shard.ShardID, replicaID)
			}
		}
	}
	return nil
}

func extendReplicaIDHistory(history []ShardReplicaIDHistory, registryLayout RegistryLayout) []ShardReplicaIDHistory {
	active := replicaIDHistoryForLayout(registryLayout)
	extended := make([]ShardReplicaIDHistory, len(history))
	for index, shard := range history {
		ids := append([]uint64(nil), shard.ReplicaIDs...)
		for _, replicaID := range active[index].ReplicaIDs {
			if !containsReplicaID(ids, replicaID) {
				ids = append(ids, replicaID)
			}
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		extended[index] = ShardReplicaIDHistory{ShardID: shard.ShardID, ReplicaIDs: ids}
	}
	return extended
}

func containsReplicaID(replicaIDs []uint64, target uint64) bool {
	position := sort.Search(len(replicaIDs), func(index int) bool { return replicaIDs[index] >= target })
	return position < len(replicaIDs) && replicaIDs[position] == target
}

func validateRetainedReplicaTargets(previous, next RegistryLayout) error {
	type replicaKey struct {
		ShardID   uint64
		ReplicaID uint64
	}
	type target struct {
		MemberID string
		Endpoint string
	}
	type occupiedTarget struct {
		ShardID uint64
		Target  string
	}
	targets := make(map[replicaKey]target)
	occupiedMembers := make(map[occupiedTarget]uint64)
	occupiedEndpoints := make(map[occupiedTarget]uint64)
	add := func(layout RegistryLayout, shardID uint64, replicas []ReplicaPlacement, output map[replicaKey]target) {
		for _, replica := range replicas {
			member, _ := registryLayoutMember(layout, replica.MemberID)
			output[replicaKey{ShardID: shardID, ReplicaID: replica.ReplicaID}] = target{
				MemberID: replica.MemberID, Endpoint: member.RaftEndpoint,
			}
			occupiedMembers[occupiedTarget{ShardID: shardID, Target: replica.MemberID}] = replica.ReplicaID
			occupiedEndpoints[occupiedTarget{ShardID: shardID, Target: member.RaftEndpoint}] = replica.ReplicaID
		}
	}
	add(previous, SystemRaftShardID, previous.SystemReplicas, targets)
	for _, shard := range previous.DataShards {
		add(previous, DataRaftShardID(shard.ShardID), shard.Replicas, targets)
	}
	check := func(shardID uint64, replicas []ReplicaPlacement) error {
		for _, replica := range replicas {
			member, _ := registryLayoutMember(next, replica.MemberID)
			if priorID, occupied := occupiedMembers[occupiedTarget{ShardID: shardID, Target: replica.MemberID}]; occupied &&
				priorID != replica.ReplicaID {
				return errors.New("raftstore: replacement replica reuses an occupied member target")
			}
			if priorID, occupied := occupiedEndpoints[occupiedTarget{ShardID: shardID, Target: member.RaftEndpoint}]; occupied &&
				priorID != replica.ReplicaID {
				return errors.New("raftstore: replacement replica reuses an occupied Raft endpoint")
			}
			prior, retained := targets[replicaKey{ShardID: shardID, ReplicaID: replica.ReplicaID}]
			if !retained {
				continue
			}
			if prior.MemberID != replica.MemberID || prior.Endpoint != member.RaftEndpoint {
				return errors.New("raftstore: retained Raft replica changed member or endpoint")
			}
		}
		return nil
	}
	if err := check(SystemRaftShardID, next.SystemReplicas); err != nil {
		return err
	}
	for _, shard := range next.DataShards {
		if err := check(DataRaftShardID(shard.ShardID), shard.Replicas); err != nil {
			return err
		}
	}
	return nil
}

func cloneRegistryLayout(source RegistryLayout) RegistryLayout {
	clone := source
	clone.Members = append([]RegistryMember(nil), source.Members...)
	clone.SystemReplicas = append([]ReplicaPlacement(nil), source.SystemReplicas...)
	clone.DataShards = make([]ShardPlacement, len(source.DataShards))
	for index, shard := range source.DataShards {
		clone.DataShards[index] = shard
		clone.DataShards[index].Replicas = append([]ReplicaPlacement(nil), shard.Replicas...)
	}
	if source.Predecessor != nil {
		predecessor := *source.Predecessor
		clone.Predecessor = &predecessor
	}
	return clone
}

func FirstAcceptedRegistryLayout(registryLayout RegistryLayout, digest string) (AcceptedRegistryLayout, error) {
	if err := registryLayout.Validate(); err != nil {
		return AcceptedRegistryLayout{}, err
	}
	if registryLayout.RegistryLayoutVersion != 1 || !isSHA256(digest) {
		return AcceptedRegistryLayout{}, errors.New("raftstore: first accepted registryLayout must start at version one")
	}
	return acceptedRegistryLayout(registryLayout, digest), nil
}

type RegistryLayoutGuard struct{ Path string }

func (g RegistryLayoutGuard) AcceptSignedChain(
	chain []SignedRegistryLayout,
	keyring map[string]ed25519.PublicKey,
) (AcceptedRegistryLayout, error) {
	accepted, err := g.EvaluateSignedChain(chain, keyring)
	if err != nil {
		return AcceptedRegistryLayout{}, err
	}
	if err := g.store(accepted); err != nil {
		return AcceptedRegistryLayout{}, err
	}
	return accepted, nil
}

// EvaluateSignedChain verifies and advances a registryLayout lineage in memory. It
// lets runtime bootstrap validate every local prerequisite before making the
// anti-rollback decision durable.
func (g RegistryLayoutGuard) EvaluateSignedChain(
	chain []SignedRegistryLayout,
	keyring map[string]ed25519.PublicKey,
) (AcceptedRegistryLayout, error) {
	if len(chain) == 0 || len(chain) > 1024 {
		return AcceptedRegistryLayout{}, errors.New("raftstore: registryLayout chain must be non-empty and bounded")
	}
	current, err := g.Load()
	if err != nil {
		return AcceptedRegistryLayout{}, err
	}
	digests := make([]string, len(chain))
	for index, signed := range chain {
		digest, verifyErr := signed.Verify(keyring)
		if verifyErr != nil {
			return AcceptedRegistryLayout{}, verifyErr
		}
		digests[index] = digest
	}
	start := 0
	if current != nil {
		anchor := -1
		for index, signed := range chain {
			if signed.RegistryLayout.ClusterID == current.ClusterID &&
				signed.RegistryLayout.RegistryGeneration == current.RegistryGeneration &&
				signed.RegistryLayout.RegistryLayoutVersion == current.RegistryLayoutVersion && digests[index] == current.RegistryLayoutDigest {
				anchor = index
				break
			}
		}
		if anchor >= 0 {
			for index := 1; index <= anchor; index++ {
				previous := acceptedRegistryLayout(chain[index-1].RegistryLayout, digests[index-1])
				if chain[index].RegistryLayout.RegistryGeneration == chain[index-1].RegistryLayout.RegistryGeneration {
					if transitionErr := ValidateRegistryLayoutTransition(chain[index-1].RegistryLayout, chain[index].RegistryLayout); transitionErr != nil {
						return AcceptedRegistryLayout{}, fmt.Errorf("raftstore: invalid signed registryLayout transition: %w", transitionErr)
					}
				}
				if _, linkErr := previous.Accept(chain[index].RegistryLayout, digests[index]); linkErr != nil {
					return AcceptedRegistryLayout{}, fmt.Errorf("raftstore: invalid signed registryLayout chain link: %w", linkErr)
				}
			}
			start = anchor + 1
		}
	}
	for index := start; index < len(chain); index++ {
		signed := chain[index]
		digest := digests[index]
		if current == nil {
			accepted, acceptErr := FirstAcceptedRegistryLayout(signed.RegistryLayout, digest)
			if acceptErr != nil {
				return AcceptedRegistryLayout{}, acceptErr
			}
			current = &accepted
			continue
		}
		if signed.RegistryLayout.RegistryGeneration == current.RegistryGeneration {
			if transitionErr := ValidateRegistryLayoutTransition(current.RegistryLayout, signed.RegistryLayout); transitionErr != nil {
				return AcceptedRegistryLayout{}, fmt.Errorf("raftstore: invalid signed registryLayout transition: %w", transitionErr)
			}
		}
		accepted, acceptErr := current.Accept(signed.RegistryLayout, digest)
		if acceptErr != nil {
			return AcceptedRegistryLayout{}, acceptErr
		}
		current = &accepted
	}
	if current == nil {
		return AcceptedRegistryLayout{}, errors.New("raftstore: registryLayout chain did not produce an accepted state")
	}
	return *current, nil
}

func (g RegistryLayoutGuard) Load() (*AcceptedRegistryLayout, error) {
	raw, err := os.ReadFile(g.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var accepted AcceptedRegistryLayout
	if err := json.Unmarshal(raw, &accepted); err != nil {
		return nil, fmt.Errorf("raftstore: decode registryLayout guard: %w", err)
	}
	if err := accepted.Validate(); err != nil {
		return nil, err
	}
	return &accepted, nil
}

func (g RegistryLayoutGuard) store(accepted AcceptedRegistryLayout) error {
	if err := accepted.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(accepted)
	if err != nil {
		return err
	}
	dir := filepath.Dir(g.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".registryLayout-guard-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, g.Path); err != nil {
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
