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
	"reflect"
	"sort"
	"strconv"
	"strings"
)

const (
	RegistryLayoutFormatV1        = uint32(1)
	DefaultVirtualShards          = uint32(4096)
	DefaultRouteBuckets           = uint32(16)
	DefaultBuildBuckets           = uint32(16)
	DefaultReplication            = uint32(3)
	SystemRaftShardID             = uint64(1)
	firstDataRaftShardID          = uint64(2)
	registryLayoutSignatureDomain = "kuasar-registry-layout-v1\x00"
	rolloverIntentDomain          = "kuasar-registry-history-rollover-intent-v1\x00"
	zeroSHA256                    = "0000000000000000000000000000000000000000000000000000000000000000"
	MaxRegistryLayoutBytes        = MaxRaftCommandBytes - (64 << 10)
)

type RolloverProofKind string

const (
	RolloverConsensusClosure RolloverProofKind = "CONSENSUS_CLOSURE"
	RolloverExternalFence    RolloverProofKind = "EXTERNAL_HARD_FENCE"
)

type PredecessorProof struct {
	RegistryGeneration               string            `json:"registry_generation"`
	RegistryLayoutDigest             string            `json:"registry_layout_digest"`
	ServePermitMaxMillis             uint64            `json:"serve_permit_max_millis"`
	Kind                             RolloverProofKind `json:"kind"`
	TargetRegistryLayoutIntentDigest string            `json:"target_registry_layout_intent_digest"`
	CommitIndex                      uint64            `json:"commit_index"`
	ProofDigest                      string            `json:"proof_digest"`
}

func (p PredecessorProof) Validate() error {
	return p.validate(true)
}

func (p PredecessorProof) validate(requireProof bool) error {
	if p.RegistryGeneration == "" || p.ServePermitMaxMillis == 0 ||
		p.ServePermitMaxMillis > MaximumServePermitMillis ||
		!isSHA256(p.RegistryLayoutDigest) {
		return errors.New("raftstore: incomplete predecessor proof")
	}
	switch p.Kind {
	case RolloverConsensusClosure:
		if !requireProof {
			return nil
		}
		if p.CommitIndex == 0 || !isSHA256(p.TargetRegistryLayoutIntentDigest) || !isSHA256(p.ProofDigest) {
			return errors.New("raftstore: incomplete consensus predecessor proof")
		}
		return nil
	case RolloverExternalFence:
		if !requireProof {
			return nil
		}
		if p.CommitIndex != 0 || p.TargetRegistryLayoutIntentDigest != "" || !isSHA256(p.ProofDigest) {
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
	if _, err := canonicalInternalEndpoint(m.InternalEndpoint); err != nil {
		return err
	}
	if _, err := canonicalRaftEndpoint(m.RaftEndpoint); err != nil {
		return err
	}
	return nil
}

func canonicalInternalEndpoint(endpoint string) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.Path != "" ||
		u.RawPath != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || u.Opaque != "" {
		return "", errors.New("raftstore: member internal endpoint must be an HTTPS base URL")
	}
	host, err := canonicalEndpointHost(u.Hostname())
	if err != nil {
		return "", errors.New("raftstore: member internal endpoint has an invalid host")
	}
	port := uint64(443)
	if text := u.Port(); text != "" {
		port, err = strconv.ParseUint(text, 10, 16)
		if err != nil || port == 0 {
			return "", errors.New("raftstore: member internal endpoint requires a numeric nonzero port")
		}
	}
	return "https://" + net.JoinHostPort(host, strconv.FormatUint(port, 10)), nil
}

func canonicalRaftEndpoint(endpoint string) (string, error) {
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil {
		return "", fmt.Errorf("raftstore: invalid member Raft endpoint: %w", err)
	}
	host, err = canonicalEndpointHost(host)
	if err != nil {
		return "", errors.New("raftstore: member Raft endpoint has an invalid host")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return "", errors.New("raftstore: member Raft endpoint requires a host and numeric nonzero port")
	}
	return net.JoinHostPort(host, strconv.FormatUint(port, 10)), nil
}

func canonicalEndpointHost(host string) (string, error) {
	if host == "" || strings.Contains(host, "%") {
		return "", errors.New("invalid endpoint host")
	}
	if address := net.ParseIP(host); address != nil {
		return address.String(), nil
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "" || strings.ContainsAny(host, " /?#[]:@") {
		return "", errors.New("invalid endpoint host")
	}
	return host, nil
}

type ReplicaPlacement struct {
	MemberID  string `json:"member_id"`
	ReplicaID uint64 `json:"replica_id"`
}

type ShardPlacement struct {
	ShardID  uint32             `json:"shard_id"`
	Replicas []ReplicaPlacement `json:"replicas"`
}

type RegistryLayout struct {
	FormatVersion                 uint32             `json:"format_version"`
	ClusterID                     string             `json:"cluster_id"`
	RegistryGeneration            string             `json:"registry_generation"`
	RegistryLayoutVersion         uint64             `json:"registry_layout_version"`
	PreviousRegistryLayoutVersion uint64             `json:"previous_registry_layout_version,omitempty"`
	PreviousRegistryLayoutDigest  string             `json:"previous_registry_layout_digest,omitempty"`
	Predecessor                   *PredecessorProof  `json:"predecessor,omitempty"`
	SchemaVersion                 uint32             `json:"schema_version"`
	ProtocolVersion               uint32             `json:"protocol_version"`
	HashVersion                   string             `json:"hash_version"`
	VirtualShardCount             uint32             `json:"virtual_shard_count"`
	RouteBucketCount              uint32             `json:"route_bucket_count"`
	BuildBucketCount              uint32             `json:"build_bucket_count"`
	ReplicationFactor             uint32             `json:"replication_factor"`
	ServePermitMaxMillis          uint64             `json:"serve_permit_max_millis"`
	BootstrapTokenDigest          string             `json:"bootstrap_token_digest"`
	Members                       []RegistryMember   `json:"members"`
	SystemReplicas                []ReplicaPlacement `json:"system_replicas"`
	DataShards                    []ShardPlacement   `json:"data_shards"`
}

func (m RegistryLayout) Validate() error {
	return m.validate(true)
}

func (m RegistryLayout) validate(requireRolloverProof bool) error {
	raw, err := json.Marshal(m)
	if err != nil || len(raw) > MaxRegistryLayoutBytes {
		return fmt.Errorf("raftstore: Registry Layout exceeds %d bytes", MaxRegistryLayoutBytes)
	}
	if m.FormatVersion != RegistryLayoutFormatV1 || m.ClusterID == "" || m.RegistryGeneration == "" ||
		m.RegistryLayoutVersion == 0 || m.SchemaVersion == 0 || m.ProtocolVersion == 0 {
		return errors.New("raftstore: incomplete registryLayout identity")
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
	if m.RegistryLayoutVersion == 1 &&
		(m.PreviousRegistryLayoutVersion != 0 || m.PreviousRegistryLayoutDigest != "") {
		return errors.New("raftstore: first Registry Layout cannot name a previous Registry Layout")
	}
	if m.RegistryLayoutVersion > 1 &&
		(m.PreviousRegistryLayoutVersion != m.RegistryLayoutVersion-1 || !isSHA256(m.PreviousRegistryLayoutDigest)) {
		return errors.New("raftstore: exact previous Registry Layout version and digest are required")
	}
	if m.Predecessor != nil {
		if err := m.Predecessor.validate(requireRolloverProof); err != nil {
			return err
		}
		if m.Predecessor.RegistryGeneration == m.RegistryGeneration {
			return errors.New("raftstore: predecessor Registry History Generation did not change")
		}
		if requireRolloverProof && m.Predecessor.Kind == RolloverConsensusClosure {
			intentDigest, err := m.RolloverIntentDigest()
			if err != nil || intentDigest != m.Predecessor.TargetRegistryLayoutIntentDigest ||
				m.Predecessor.ProofDigest != consensusClosureProofDigest(
					m.ClusterID, m.Predecessor.RegistryGeneration, m.Predecessor.RegistryLayoutDigest,
					m.RegistryGeneration, intentDigest, m.Predecessor.ServePermitMaxMillis,
					m.Predecessor.CommitIndex,
				) {
				return errors.New("raftstore: consensus predecessor proof does not commit this successor registryLayout")
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
		internalEndpoint, _ := canonicalInternalEndpoint(member.InternalEndpoint)
		raftEndpoint, _ := canonicalRaftEndpoint(member.RaftEndpoint)
		if _, found := internalEndpoints[internalEndpoint]; found {
			return errors.New("raftstore: duplicate member internal endpoint")
		}
		if _, found := raftEndpoints[raftEndpoint]; found {
			return errors.New("raftstore: duplicate member Raft endpoint")
		}
		members[member.MemberID] = member
		internalEndpoints[internalEndpoint] = struct{}{}
		raftEndpoints[raftEndpoint] = struct{}{}
	}
	if err := validateReplicaSet(m.SystemReplicas, members, m.ReplicationFactor); err != nil {
		return fmt.Errorf("raftstore: System Group: %w", err)
	}
	if len(m.DataShards) != int(m.VirtualShardCount) {
		return errors.New("raftstore: registryLayout does not place every virtual shard")
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

// RolloverIntentDigest commits every successor registryLayout field except the
// consensus proof outputs that are only known after the predecessor closes.
func (m RegistryLayout) RolloverIntentDigest() (string, error) {
	if m.Predecessor == nil || m.Predecessor.Kind != RolloverConsensusClosure {
		return "", errors.New("raftstore: rollover intent requires a consensus predecessor")
	}
	if err := m.validate(false); err != nil {
		return "", err
	}
	normalized := m
	predecessor := *m.Predecessor
	predecessor.TargetRegistryLayoutIntentDigest = zeroSHA256
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

func (m RegistryLayout) CanonicalBytes() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(m)
}

func (m RegistryLayout) Digest() (string, error) {
	raw, err := m.CanonicalBytes()
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// ValidateRegistryLayoutTransition checks identities that are meaningful only
// across two signed Registry Layout artifacts in one History Generation.
func ValidateRegistryLayoutTransition(previous, next RegistryLayout) error {
	if err := previous.Validate(); err != nil {
		return err
	}
	if err := next.Validate(); err != nil {
		return err
	}
	previousDigest, err := previous.Digest()
	if err != nil {
		return err
	}
	if next.ClusterID != previous.ClusterID || next.RegistryGeneration != previous.RegistryGeneration ||
		next.RegistryLayoutVersion != previous.RegistryLayoutVersion+1 ||
		next.PreviousRegistryLayoutVersion != previous.RegistryLayoutVersion ||
		next.PreviousRegistryLayoutDigest != previousDigest {
		return errors.New("raftstore: registryLayout transition is not the exact next artifact")
	}
	if !acceptedRegistryLayout(previous, previousDigest).matchesFrozenParameters(next) ||
		!reflect.DeepEqual(previous.Predecessor, next.Predecessor) {
		return errors.New("raftstore: Registry Layout transition changed Registry-History-Generation-frozen identity")
	}

	previousMembers := make(map[string]RegistryMember, len(previous.Members))
	previousInternalEndpoints := make(map[string]string, len(previous.Members))
	previousRaftEndpoints := make(map[string]string, len(previous.Members))
	for _, member := range previous.Members {
		internalEndpoint, _ := canonicalInternalEndpoint(member.InternalEndpoint)
		raftEndpoint, _ := canonicalRaftEndpoint(member.RaftEndpoint)
		previousMembers[member.MemberID] = member
		previousInternalEndpoints[internalEndpoint] = member.MemberID
		previousRaftEndpoints[raftEndpoint] = member.MemberID
	}
	for _, member := range next.Members {
		if retained, found := previousMembers[member.MemberID]; found {
			if retained != member {
				return errors.New("raftstore: retained Registry member changed an endpoint")
			}
			continue
		}
		internalEndpoint, _ := canonicalInternalEndpoint(member.InternalEndpoint)
		raftEndpoint, _ := canonicalRaftEndpoint(member.RaftEndpoint)
		if previousInternalEndpoints[internalEndpoint] != "" || previousRaftEndpoints[raftEndpoint] != "" {
			return errors.New("raftstore: replacement Registry member reused a predecessor endpoint")
		}
	}
	if err := validateReplicaTransition(previous.SystemReplicas, next.SystemReplicas); err != nil {
		return fmt.Errorf("raftstore: System Group transition: %w", err)
	}
	if len(previous.DataShards) != len(next.DataShards) {
		return errors.New("raftstore: registryLayout transition changed the fixed data-shard count")
	}
	for index := range previous.DataShards {
		if err := validateReplicaTransition(previous.DataShards[index].Replicas, next.DataShards[index].Replicas); err != nil {
			return fmt.Errorf("raftstore: shard %d transition: %w", index, err)
		}
	}
	return nil
}

func validateReplicaTransition(previous, next []ReplicaPlacement) error {
	previousByMember := make(map[string]uint64, len(previous))
	previousByReplica := make(map[uint64]string, len(previous))
	for _, replica := range previous {
		previousByMember[replica.MemberID] = replica.ReplicaID
		previousByReplica[replica.ReplicaID] = replica.MemberID
	}
	for _, replica := range next {
		if previousID, retained := previousByMember[replica.MemberID]; retained && previousID != replica.ReplicaID {
			return errors.New("retained member changed its replica ID")
		}
		if previousMember, reused := previousByReplica[replica.ReplicaID]; reused && previousMember != replica.MemberID {
			return errors.New("replacement member reused a predecessor replica ID")
		}
	}
	return nil
}

type SignedRegistryLayout struct {
	RegistryLayout RegistryLayout `json:"registry_layout"`
	KeyID          string         `json:"key_id"`
	Signature      string         `json:"signature"`
}

func SignRegistryLayout(registryLayout RegistryLayout, keyID string, key ed25519.PrivateKey) (SignedRegistryLayout, error) {
	if keyID == "" || len(key) != ed25519.PrivateKeySize {
		return SignedRegistryLayout{}, errors.New("raftstore: valid registryLayout signing key is required")
	}
	raw, err := registryLayout.CanonicalBytes()
	if err != nil {
		return SignedRegistryLayout{}, err
	}
	signature := ed25519.Sign(key, append([]byte(registryLayoutSignatureDomain), raw...))
	return SignedRegistryLayout{RegistryLayout: registryLayout, KeyID: keyID, Signature: base64.RawStdEncoding.EncodeToString(signature)}, nil
}

func (s SignedRegistryLayout) Verify(keyring map[string]ed25519.PublicKey) (string, error) {
	key := keyring[s.KeyID]
	if len(key) != ed25519.PublicKeySize {
		return "", errors.New("raftstore: registryLayout signing key is not trusted")
	}
	raw, err := s.RegistryLayout.CanonicalBytes()
	if err != nil {
		return "", err
	}
	signature, err := base64.RawStdEncoding.DecodeString(s.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize ||
		!ed25519.Verify(key, append([]byte(registryLayoutSignatureDomain), raw...), signature) {
		return "", errors.New("raftstore: invalid registryLayout signature")
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
			return errors.New("replica does not match a registryLayout member")
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
