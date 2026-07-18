package raftstore

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
)

const (
	ManifestFormatV1        = uint32(1)
	DefaultVirtualShards    = uint32(4096)
	DefaultRouteBuckets     = uint32(16)
	DefaultBuildBuckets     = uint32(16)
	DefaultReplication      = uint32(3)
	SystemRaftShardID       = uint64(1)
	firstDataRaftShardID    = uint64(2)
	manifestSignatureDomain = "kuasar-registry-manifest-v1\x00"
	rolloverIntentDomain    = "kuasar-storage-rollover-intent-v1\x00"
	zeroSHA256              = "0000000000000000000000000000000000000000000000000000000000000000"
)

type RolloverProofKind string

const (
	RolloverConsensusClosure RolloverProofKind = "CONSENSUS_CLOSURE"
	RolloverExternalFence    RolloverProofKind = "EXTERNAL_HARD_FENCE"
)

type PredecessorProof struct {
	StorageGeneration          string            `json:"storage_generation"`
	ManifestDigest             string            `json:"manifest_digest"`
	ServePermitMaxMillis       uint64            `json:"serve_permit_max_millis"`
	Kind                       RolloverProofKind `json:"kind"`
	TargetManifestIntentDigest string            `json:"target_manifest_intent_digest"`
	CommitIndex                uint64            `json:"commit_index"`
	ProofDigest                string            `json:"proof_digest"`
}

func (p PredecessorProof) Validate() error {
	return p.validate(true)
}

func (p PredecessorProof) validate(requireProof bool) error {
	if p.StorageGeneration == "" || p.ServePermitMaxMillis == 0 ||
		p.ServePermitMaxMillis > MaximumServePermitMillis ||
		!isSHA256(p.ManifestDigest) {
		return errors.New("raftstore: incomplete predecessor proof")
	}
	switch p.Kind {
	case RolloverConsensusClosure:
		if !requireProof {
			return nil
		}
		if p.CommitIndex == 0 || !isSHA256(p.TargetManifestIntentDigest) || !isSHA256(p.ProofDigest) {
			return errors.New("raftstore: incomplete consensus predecessor proof")
		}
		return nil
	case RolloverExternalFence:
		if !requireProof {
			return nil
		}
		if p.CommitIndex != 0 || p.TargetManifestIntentDigest != "" || !isSHA256(p.ProofDigest) {
			return errors.New("raftstore: invalid external predecessor fence")
		}
		return nil
	default:
		return errors.New("raftstore: unknown predecessor proof kind")
	}
}

type RegistryMember struct {
	MemberID         string `json:"member_id"`
	InternalEndpoint string `json:"internal_endpoint"`
	RaftEndpoint     string `json:"raft_endpoint"`
}

func (m RegistryMember) Validate() error {
	if m.MemberID == "" {
		return errors.New("raftstore: member identity is required")
	}
	u, err := url.Parse(m.InternalEndpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return errors.New("raftstore: member internal endpoint must be an HTTPS URL")
	}
	if _, _, err := net.SplitHostPort(m.RaftEndpoint); err != nil {
		return fmt.Errorf("raftstore: invalid member Raft endpoint: %w", err)
	}
	return nil
}

type ReplicaPlacement struct {
	MemberID  string `json:"member_id"`
	ReplicaID uint64 `json:"replica_id"`
}

type ShardPlacement struct {
	ShardID  uint32             `json:"shard_id"`
	Replicas []ReplicaPlacement `json:"replicas"`
}

type Manifest struct {
	FormatVersion          uint32             `json:"format_version"`
	ClusterID              string             `json:"cluster_id"`
	StorageGeneration      string             `json:"storage_generation"`
	ManifestVersion        uint64             `json:"manifest_version"`
	PreviousManifestDigest string             `json:"previous_manifest_digest,omitempty"`
	Predecessor            *PredecessorProof  `json:"predecessor,omitempty"`
	SchemaVersion          uint32             `json:"schema_version"`
	ProtocolVersion        uint32             `json:"protocol_version"`
	HashVersion            string             `json:"hash_version"`
	VirtualShardCount      uint32             `json:"virtual_shard_count"`
	RouteBucketCount       uint32             `json:"route_bucket_count"`
	BuildBucketCount       uint32             `json:"build_bucket_count"`
	ReplicationFactor      uint32             `json:"replication_factor"`
	ServePermitMaxMillis   uint64             `json:"serve_permit_max_millis"`
	BootstrapTokenDigest   string             `json:"bootstrap_token_digest"`
	Members                []RegistryMember   `json:"members"`
	SystemReplicas         []ReplicaPlacement `json:"system_replicas"`
	DataShards             []ShardPlacement   `json:"data_shards"`
}

func (m Manifest) Validate() error {
	return m.validate(true)
}

func (m Manifest) validate(requireRolloverProof bool) error {
	if m.FormatVersion != ManifestFormatV1 || m.ClusterID == "" || m.StorageGeneration == "" ||
		m.ManifestVersion == 0 || m.SchemaVersion == 0 || m.ProtocolVersion == 0 {
		return errors.New("raftstore: incomplete manifest identity")
	}
	if m.HashVersion != "ShardHashV1" || m.VirtualShardCount == 0 ||
		!isPowerOfTwo(m.VirtualShardCount) || !isPowerOfTwo(m.RouteBucketCount) ||
		!isPowerOfTwo(m.BuildBucketCount) || m.ReplicationFactor != DefaultReplication {
		return errors.New("raftstore: invalid fixed keyspace parameters")
	}
	if m.ServePermitMaxMillis == 0 || m.ServePermitMaxMillis > MaximumServePermitMillis ||
		!isSHA256(m.BootstrapTokenDigest) {
		return errors.New("raftstore: permit lifetime and bootstrap token digest are required")
	}
	if m.ManifestVersion == 1 && m.PreviousManifestDigest != "" {
		return errors.New("raftstore: first manifest cannot name a previous manifest")
	}
	if m.ManifestVersion > 1 && !isSHA256(m.PreviousManifestDigest) {
		return errors.New("raftstore: manifest lineage digest is required")
	}
	if m.Predecessor != nil {
		if err := m.Predecessor.validate(requireRolloverProof); err != nil {
			return err
		}
		if m.Predecessor.StorageGeneration == m.StorageGeneration {
			return errors.New("raftstore: predecessor generation did not change")
		}
		if requireRolloverProof && m.Predecessor.Kind == RolloverConsensusClosure {
			intentDigest, err := m.RolloverIntentDigest()
			if err != nil || intentDigest != m.Predecessor.TargetManifestIntentDigest ||
				m.Predecessor.ProofDigest != consensusClosureProofDigest(
					m.ClusterID, m.Predecessor.StorageGeneration, m.Predecessor.ManifestDigest,
					m.StorageGeneration, intentDigest, m.Predecessor.ServePermitMaxMillis,
					m.Predecessor.CommitIndex,
				) {
				return errors.New("raftstore: consensus predecessor proof does not commit this successor manifest")
			}
		}
	}
	if len(m.Members) < int(m.ReplicationFactor) || !sort.SliceIsSorted(m.Members, func(i, j int) bool {
		return m.Members[i].MemberID < m.Members[j].MemberID
	}) {
		return errors.New("raftstore: members must be unique and sorted")
	}
	members := make(map[string]RegistryMember, len(m.Members))
	internalEndpoints := make(map[string]struct{}, len(m.Members))
	raftEndpoints := make(map[string]struct{}, len(m.Members))
	for _, member := range m.Members {
		if err := member.Validate(); err != nil {
			return err
		}
		if _, found := members[member.MemberID]; found {
			return errors.New("raftstore: duplicate member ID")
		}
		if _, found := internalEndpoints[member.InternalEndpoint]; found {
			return errors.New("raftstore: duplicate member internal endpoint")
		}
		if _, found := raftEndpoints[member.RaftEndpoint]; found {
			return errors.New("raftstore: duplicate member Raft endpoint")
		}
		members[member.MemberID] = member
		internalEndpoints[member.InternalEndpoint] = struct{}{}
		raftEndpoints[member.RaftEndpoint] = struct{}{}
	}
	if err := validateReplicaSet(m.SystemReplicas, members, m.ReplicationFactor); err != nil {
		return fmt.Errorf("raftstore: System Group: %w", err)
	}
	if len(m.DataShards) != int(m.VirtualShardCount) {
		return errors.New("raftstore: manifest does not place every virtual shard")
	}
	for i, shard := range m.DataShards {
		if shard.ShardID != uint32(i) {
			return errors.New("raftstore: data shard placements must be complete and ordered")
		}
		if err := validateReplicaSet(shard.Replicas, members, m.ReplicationFactor); err != nil {
			return fmt.Errorf("raftstore: shard %d: %w", shard.ShardID, err)
		}
	}
	return nil
}

// RolloverIntentDigest commits every successor manifest field except the
// consensus proof outputs that are only known after the predecessor closes.
func (m Manifest) RolloverIntentDigest() (string, error) {
	if m.Predecessor == nil || m.Predecessor.Kind != RolloverConsensusClosure {
		return "", errors.New("raftstore: rollover intent requires a consensus predecessor")
	}
	if err := m.validate(false); err != nil {
		return "", err
	}
	normalized := m
	predecessor := *m.Predecessor
	predecessor.TargetManifestIntentDigest = zeroSHA256
	predecessor.CommitIndex = 0
	predecessor.ProofDigest = zeroSHA256
	normalized.Predecessor = &predecessor
	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte(rolloverIntentDomain), raw...))
	return hex.EncodeToString(digest[:]), nil
}

func (m Manifest) CanonicalBytes() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func (m Manifest) Digest() (string, error) {
	raw, err := m.CanonicalBytes()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

type SignedManifest struct {
	Manifest  Manifest `json:"manifest"`
	KeyID     string   `json:"key_id"`
	Signature string   `json:"signature"`
}

func SignManifest(manifest Manifest, keyID string, key ed25519.PrivateKey) (SignedManifest, error) {
	if keyID == "" || len(key) != ed25519.PrivateKeySize {
		return SignedManifest{}, errors.New("raftstore: valid manifest signing key is required")
	}
	raw, err := manifest.CanonicalBytes()
	if err != nil {
		return SignedManifest{}, err
	}
	signature := ed25519.Sign(key, append([]byte(manifestSignatureDomain), raw...))
	return SignedManifest{Manifest: manifest, KeyID: keyID, Signature: base64.RawStdEncoding.EncodeToString(signature)}, nil
}

func (s SignedManifest) Verify(keyring map[string]ed25519.PublicKey) (string, error) {
	key := keyring[s.KeyID]
	if len(key) != ed25519.PublicKeySize {
		return "", errors.New("raftstore: manifest signing key is not trusted")
	}
	raw, err := s.Manifest.CanonicalBytes()
	if err != nil {
		return "", err
	}
	signature, err := base64.RawStdEncoding.DecodeString(s.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize ||
		!ed25519.Verify(key, append([]byte(manifestSignatureDomain), raw...), signature) {
		return "", errors.New("raftstore: invalid manifest signature")
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func DataRaftShardID(shardID uint32) uint64 { return firstDataRaftShardID + uint64(shardID) }

func LogicalShardID(raftShardID uint64) (uint32, bool) {
	if raftShardID < firstDataRaftShardID || raftShardID-firstDataRaftShardID > uint64(^uint32(0)) {
		return 0, false
	}
	return uint32(raftShardID - firstDataRaftShardID), true
}

func validateReplicaSet(set []ReplicaPlacement, members map[string]RegistryMember, replication uint32) error {
	if len(set) != int(replication) || !sort.SliceIsSorted(set, func(i, j int) bool {
		return set[i].MemberID < set[j].MemberID
	}) {
		return errors.New("replica set must have exactly three sorted members")
	}
	seen := make(map[string]struct{}, len(set))
	seenReplicaIDs := make(map[uint64]struct{}, len(set))
	for _, replica := range set {
		if _, found := members[replica.MemberID]; !found || replica.ReplicaID == 0 {
			return errors.New("replica does not match a manifest member")
		}
		if _, duplicate := seen[replica.MemberID]; duplicate {
			return errors.New("duplicate replica member")
		}
		seen[replica.MemberID] = struct{}{}
		if _, duplicate := seenReplicaIDs[replica.ReplicaID]; duplicate {
			return errors.New("duplicate replica ID within a shard")
		}
		seenReplicaIDs[replica.ReplicaID] = struct{}{}
	}
	return nil
}

func isPowerOfTwo(value uint32) bool { return value != 0 && value&(value-1) == 0 }

func isSHA256(value string) bool {
	raw, err := hex.DecodeString(value)
	return err == nil && len(raw) == sha256.Size
}
