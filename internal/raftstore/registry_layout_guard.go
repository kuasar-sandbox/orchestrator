package raftstore

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

type AcceptedRegistryLayout struct {
	ClusterID             string         `json:"cluster_id"`
	RegistryGeneration    string         `json:"registry_generation"`
	RegistryLayoutVersion uint64         `json:"registry_layout_version"`
	RegistryLayoutDigest  string         `json:"registry_layout_digest"`
	FormatVersion         uint32         `json:"format_version"`
	SchemaVersion         uint32         `json:"schema_version"`
	ProtocolVersion       uint32         `json:"protocol_version"`
	HashVersion           string         `json:"hash_version"`
	VirtualShardCount     uint32         `json:"virtual_shard_count"`
	RouteBucketCount      uint32         `json:"route_bucket_count"`
	BuildBucketCount      uint32         `json:"build_bucket_count"`
	ReplicationFactor     uint32         `json:"replication_factor"`
	ServePermitMaxMillis  uint64         `json:"serve_permit_max_millis"`
	BootstrapTokenDigest  string         `json:"bootstrap_token_digest"`
	RegistryLayout        RegistryLayout `json:"registry_layout"`
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
	digest, err := a.RegistryLayout.Digest()
	if err != nil || digest != a.RegistryLayoutDigest || a.RegistryLayout.ClusterID != a.ClusterID ||
		a.RegistryLayout.RegistryGeneration != a.RegistryGeneration ||
		a.RegistryLayout.RegistryLayoutVersion != a.RegistryLayoutVersion {
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
	} else {
		if next.Predecessor == nil || next.Predecessor.RegistryGeneration != a.RegistryGeneration ||
			next.Predecessor.RegistryLayoutDigest != a.RegistryLayoutDigest ||
			next.Predecessor.ServePermitMaxMillis != a.ServePermitMaxMillis || next.RegistryLayoutVersion != 1 {
			return AcceptedRegistryLayout{}, errors.New("raftstore: Registry History Generation rollover is not linked to the accepted predecessor")
		}
	}
	return acceptedRegistryLayout(next, digest), nil
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
	targets := make(map[replicaKey]target)
	add := func(layout RegistryLayout, shardID uint64, replicas []ReplicaPlacement, output map[replicaKey]target) {
		for _, replica := range replicas {
			member, _ := registryLayoutMember(layout, replica.MemberID)
			output[replicaKey{ShardID: shardID, ReplicaID: replica.ReplicaID}] = target{
				MemberID: replica.MemberID, Endpoint: member.RaftEndpoint,
			}
		}
	}
	add(previous, SystemRaftShardID, previous.SystemReplicas, targets)
	for _, shard := range previous.DataShards {
		add(previous, DataRaftShardID(shard.ShardID), shard.Replicas, targets)
	}
	check := func(shardID uint64, replicas []ReplicaPlacement) error {
		for _, replica := range replicas {
			prior, retained := targets[replicaKey{ShardID: shardID, ReplicaID: replica.ReplicaID}]
			if !retained {
				continue
			}
			member, _ := registryLayoutMember(next, replica.MemberID)
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

func (g RegistryLayoutGuard) AcceptSigned(
	signed SignedRegistryLayout,
	keyring map[string]ed25519.PublicKey,
) (AcceptedRegistryLayout, error) {
	digest, err := signed.Verify(keyring)
	if err != nil {
		return AcceptedRegistryLayout{}, err
	}
	current, err := g.Load()
	if err != nil {
		return AcceptedRegistryLayout{}, err
	}
	var next AcceptedRegistryLayout
	if current == nil {
		next, err = FirstAcceptedRegistryLayout(signed.RegistryLayout, digest)
	} else {
		next, err = current.Accept(signed.RegistryLayout, digest)
	}
	if err != nil {
		return AcceptedRegistryLayout{}, err
	}
	if err := g.store(next); err != nil {
		return AcceptedRegistryLayout{}, err
	}
	return next, nil
}

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
