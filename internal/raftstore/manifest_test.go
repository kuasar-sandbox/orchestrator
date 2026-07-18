package raftstore

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func digestFor(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func testManifest(shards uint32, generation string) Manifest {
	members := []RegistryMember{
		{MemberID: "registry-a", ReplicaID: 1, InternalEndpoint: "https://registry-a:9443", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", ReplicaID: 2, InternalEndpoint: "https://registry-b:9443", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", ReplicaID: 3, InternalEndpoint: "https://registry-c:9443", RaftEndpoint: "registry-c:63001"},
	}
	replicas := []ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-c", ReplicaID: 3},
	}
	placements := make([]ShardPlacement, shards)
	for shardID := uint32(0); shardID < shards; shardID++ {
		placements[shardID] = ShardPlacement{ShardID: shardID, Replicas: append([]ReplicaPlacement(nil), replicas...)}
	}
	return Manifest{
		FormatVersion: ManifestFormatV1, ClusterID: "cluster-1", StorageGeneration: generation,
		ManifestVersion: 1, SchemaVersion: 1, ProtocolVersion: 1, HashVersion: "ShardHashV1",
		VirtualShardCount: shards, RouteBucketCount: 16, BuildBucketCount: 16,
		ReplicationFactor: 3, ServePermitMaxMillis: 5000, BootstrapTokenDigest: digestFor("bootstrap-" + generation),
		Members: members, SystemReplicas: replicas, DataShards: placements,
	}
}

func TestManifestSignatureAndFixedPlacement(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest(4, "generation-1")
	signed, err := SignManifest(manifest, "root-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := signed.Verify(map[string]ed25519.PublicKey{"root-1": publicKey})
	if err != nil {
		t.Fatal(err)
	}
	want, err := manifest.Digest()
	if err != nil || digest != want {
		t.Fatalf("manifest digest = %q, %v; want %q", digest, err, want)
	}

	signed.Manifest.DataShards[0].Replicas[0].MemberID = "registry-z"
	if _, err := signed.Verify(map[string]ed25519.PublicKey{"root-1": publicKey}); err == nil {
		t.Fatal("tampered manifest signature verified")
	}
	if got, ok := LogicalShardID(DataRaftShardID(3)); !ok || got != 3 {
		t.Fatalf("logical shard mapping = %d, %v", got, ok)
	}
}

func TestManifestRejectsIncompleteOrUnstablePlacement(t *testing.T) {
	manifest := testManifest(4, "generation-1")
	manifest.Members[0], manifest.Members[1] = manifest.Members[1], manifest.Members[0]
	if err := manifest.Validate(); err == nil {
		t.Fatal("unordered member set accepted")
	}
	manifest = testManifest(4, "generation-1")
	manifest.DataShards = manifest.DataShards[:3]
	if err := manifest.Validate(); err == nil {
		t.Fatal("incomplete shard placement accepted")
	}
}

func TestManifestGuardRejectsRollbackEquivocationAndUnrelatedGeneration(t *testing.T) {
	first := testManifest(4, "generation-1")
	firstDigest, _ := first.Digest()
	accepted, err := FirstAcceptedManifest(first, firstDigest)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.ManifestVersion = 2
	second.PreviousManifestDigest = firstDigest
	second.BootstrapTokenDigest = digestFor("manifest-2")
	secondDigest, _ := second.Digest()
	accepted, err = accepted.Accept(second, secondDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Accept(first, firstDigest); err == nil {
		t.Fatal("manifest rollback accepted")
	}
	equivocation := second
	equivocation.BootstrapTokenDigest = digestFor("equivocation")
	equivocationDigest, _ := equivocation.Digest()
	if _, err := accepted.Accept(equivocation, equivocationDigest); err == nil {
		t.Fatal("same-version manifest equivocation accepted")
	}

	successor := testManifest(4, "generation-2")
	successor.Predecessor = &PredecessorProof{
		StorageGeneration: accepted.StorageGeneration, ManifestDigest: accepted.ManifestDigest,
		ServePermitMaxMillis: 5000, Kind: RolloverConsensusClosure, ProofDigest: digestFor("closure"),
	}
	successorDigest, _ := successor.Digest()
	next, err := accepted.Accept(successor, successorDigest)
	if err != nil || next.StorageGeneration != "generation-2" {
		t.Fatalf("linked successor = %+v, %v", next, err)
	}
	successor.Predecessor.StorageGeneration = "unrelated"
	unrelatedDigest, _ := successor.Digest()
	if _, err := accepted.Accept(successor, unrelatedDigest); err == nil {
		t.Fatal("unrelated generation accepted")
	}
}

func TestManifestGuardPersistsWithRestrictedMode(t *testing.T) {
	manifest := testManifest(4, "generation-1")
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignManifest(manifest, "root-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "guard", "manifest.json")
	guard := ManifestGuard{Path: path}
	accepted, err := guard.AcceptSigned(signed, map[string]ed25519.PublicKey{"root-1": publicKey})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := guard.Load()
	if err != nil || loaded == nil || *loaded != accepted {
		t.Fatalf("loaded guard = %+v, %v", loaded, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("guard mode = %v, %v", info.Mode().Perm(), err)
	}
}
