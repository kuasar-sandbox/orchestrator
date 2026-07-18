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
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a:9443", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b:9443", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c:9443", RaftEndpoint: "registry-c:63001"},
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

func finalizeConsensusSuccessor(
	t *testing.T,
	predecessor Manifest,
	predecessorDigest string,
	successor Manifest,
) (Manifest, SystemState) {
	t.Helper()
	successor.Predecessor = &PredecessorProof{
		StorageGeneration:    predecessor.StorageGeneration,
		ManifestDigest:       predecessorDigest,
		ServePermitMaxMillis: predecessor.ServePermitMaxMillis,
		Kind:                 RolloverConsensusClosure,
	}
	intentDigest, err := successor.RolloverIntentDigest()
	if err != nil {
		t.Fatal(err)
	}
	source := SystemState{
		Initialized: true, ClusterID: predecessor.ClusterID,
		StorageGeneration: predecessor.StorageGeneration, SystemEpoch: 1,
		SchemaVersion: predecessor.SchemaVersion, ProtocolVersion: predecessor.ProtocolVersion,
		VirtualShardCount:     predecessor.VirtualShardCount,
		ActiveManifestVersion: predecessor.ManifestVersion, ActiveManifestDigest: predecessorDigest,
		ServePermitMaxMillis:     predecessor.ServePermitMaxMillis,
		PredecessorDrainComplete: true, LastApplied: 1,
	}
	if predecessor.Predecessor != nil {
		source.HasPredecessor = true
		source.PredecessorGeneration = predecessor.Predecessor.StorageGeneration
		source.PredecessorManifestDigest = predecessor.Predecessor.ManifestDigest
		source.PredecessorProofKind = predecessor.Predecessor.Kind
		source.PredecessorProofDigest = predecessor.Predecessor.ProofDigest
		source.PredecessorProofCommitIndex = predecessor.Predecessor.CommitIndex
		source.PredecessorTargetManifestIntentDigest = predecessor.Predecessor.TargetManifestIntentDigest
		source.PredecessorPermitMaxMillis = predecessor.Predecessor.ServePermitMaxMillis
	}
	if err := source.Validate(); err != nil {
		t.Fatal(err)
	}
	closed, result := ApplySystemCommand(source, 2, SystemCommand{
		Type: SystemCloseGeneration,
		Closure: &GenerationClosure{
			TargetStorageGeneration:    successor.StorageGeneration,
			TargetManifestIntentDigest: intentDigest,
			Kind:                       RolloverConsensusClosure,
		},
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("close predecessor = %+v", result)
	}
	proof, err := closed.ConsensusPredecessorProof()
	if err != nil {
		t.Fatal(err)
	}
	successor.Predecessor = &proof
	if err := successor.Validate(); err != nil {
		t.Fatal(err)
	}
	return successor, closed
}

func TestManifestReplicaIdentityIsScopedToEachShard(t *testing.T) {
	manifest := testManifest(2, "generation-1")
	manifest.DataShards[0].Replicas[0].ReplicaID = 101
	manifest.DataShards[1].Replicas[0].ReplicaID = 201
	if err := manifest.Validate(); err != nil {
		t.Fatalf("per-shard replica IDs rejected: %v", err)
	}
	manifest.DataShards[0].Replicas[1].ReplicaID = 101
	if err := manifest.Validate(); err == nil {
		t.Fatal("duplicate replica ID within one shard accepted")
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
	second.Members = append([]RegistryMember(nil), first.Members...)
	second.ManifestVersion = 2
	second.PreviousManifestDigest = firstDigest
	second.Members[0].InternalEndpoint = "https://registry-a-next:9443"
	secondDigest, _ := second.Digest()
	accepted, err = accepted.Accept(second, secondDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Accept(first, firstDigest); err == nil {
		t.Fatal("manifest rollback accepted")
	}
	equivocation := second
	equivocation.Members = append([]RegistryMember(nil), second.Members...)
	equivocation.Members[0].InternalEndpoint = "https://registry-a-equivocation:9443"
	equivocationDigest, _ := equivocation.Digest()
	if _, err := accepted.Accept(equivocation, equivocationDigest); err == nil {
		t.Fatal("same-version manifest equivocation accepted")
	}

	successor, _ := finalizeConsensusSuccessor(t, second, secondDigest, testManifest(4, "generation-2"))
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

func TestManifestGuardFreezesGenerationParameters(t *testing.T) {
	first := testManifest(4, "generation-1")
	firstDigest, _ := first.Digest()
	accepted, err := FirstAcceptedManifest(first, firstDigest)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(*Manifest){
		"schema":          func(manifest *Manifest) { manifest.SchemaVersion++ },
		"protocol":        func(manifest *Manifest) { manifest.ProtocolVersion++ },
		"route buckets":   func(manifest *Manifest) { manifest.RouteBucketCount *= 2 },
		"build buckets":   func(manifest *Manifest) { manifest.BuildBucketCount *= 2 },
		"permit lifetime": func(manifest *Manifest) { manifest.ServePermitMaxMillis++ },
		"bootstrap token": func(manifest *Manifest) { manifest.BootstrapTokenDigest = digestFor("replacement-token") },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			next := first
			next.ManifestVersion = 2
			next.PreviousManifestDigest = firstDigest
			mutate(&next)
			digest, digestErr := next.Digest()
			if digestErr != nil {
				t.Fatal(digestErr)
			}
			if _, acceptErr := accepted.Accept(next, digest); acceptErr == nil {
				t.Fatal("same-generation frozen parameter change accepted")
			}
		})
	}
}

func TestManifestGuardBindsSuccessorToPredecessorPermitLifetime(t *testing.T) {
	first := testManifest(4, "generation-1")
	firstDigest, _ := first.Digest()
	accepted, err := FirstAcceptedManifest(first, firstDigest)
	if err != nil {
		t.Fatal(err)
	}
	successor := testManifest(4, "generation-2")
	successor.Predecessor = &PredecessorProof{
		StorageGeneration: first.StorageGeneration, ManifestDigest: firstDigest,
		ServePermitMaxMillis: first.ServePermitMaxMillis - 1,
		Kind:                 RolloverExternalFence, ProofDigest: digestFor("external-hard-fence"),
	}
	digest, err := successor.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Accept(successor, digest); err == nil {
		t.Fatal("successor with an understated predecessor permit lifetime was accepted")
	}
}

func TestConsensusRolloverProofCommitsExactSuccessorManifest(t *testing.T) {
	predecessor := testManifest(2, "generation-1")
	predecessorDigest, err := predecessor.Digest()
	if err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*Manifest){
		"bootstrap token": func(manifest *Manifest) {
			manifest.BootstrapTokenDigest = digestFor("different-bootstrap-token")
		},
		"member endpoint": func(manifest *Manifest) {
			manifest.Members = append([]RegistryMember(nil), manifest.Members...)
			manifest.Members[0].InternalEndpoint = "https://registry-a-new:9443"
		},
		"replica placement": func(manifest *Manifest) {
			manifest.DataShards = append([]ShardPlacement(nil), manifest.DataShards...)
			manifest.DataShards[0].Replicas = append([]ReplicaPlacement(nil), manifest.DataShards[0].Replicas...)
			manifest.DataShards[0].Replicas[0].ReplicaID = 101
		},
	} {
		t.Run(name, func(t *testing.T) {
			finalized, _ := finalizeConsensusSuccessor(
				t, predecessor, predecessorDigest, testManifest(2, "generation-2"),
			)
			mutate(&finalized)
			if err := finalized.Validate(); err == nil {
				t.Fatal("manifest mutation retained a valid predecessor proof")
			}
		})
	}

	finalized, _ := finalizeConsensusSuccessor(
		t, predecessor, predecessorDigest, testManifest(2, "generation-2"),
	)
	finalized.Predecessor.ProofDigest = digestFor("forged-closure")
	if err := finalized.Validate(); err == nil {
		t.Fatal("forged consensus closure proof was accepted")
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

func TestManifestGuardReplaysFullChainFromDurableAnchor(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyring := map[string]ed25519.PublicKey{"root-1": publicKey}
	first := testManifest(4, "generation-1")
	firstDigest, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.ManifestVersion = 2
	second.PreviousManifestDigest = firstDigest
	second.Members = append([]RegistryMember(nil), first.Members...)
	second.Members[0].InternalEndpoint = "https://registry-a-v2:9443"
	secondDigest, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	third := second
	third.ManifestVersion = 3
	third.PreviousManifestDigest = secondDigest
	third.Members = append([]RegistryMember(nil), second.Members...)
	third.Members[0].InternalEndpoint = "https://registry-a-v3:9443"
	signed := make([]SignedManifest, 0, 3)
	for _, manifest := range []Manifest{first, second, third} {
		value, signErr := SignManifest(manifest, "root-1", privateKey)
		if signErr != nil {
			t.Fatal(signErr)
		}
		signed = append(signed, value)
	}
	guard := ManifestGuard{Path: filepath.Join(t.TempDir(), "manifest.json")}
	if _, err := guard.AcceptSignedChain(signed[:2], keyring); err != nil {
		t.Fatal(err)
	}
	accepted, err := guard.EvaluateSignedChain(signed, keyring)
	if err != nil {
		t.Fatal(err)
	}
	thirdDigest, err := third.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if accepted.ManifestVersion != third.ManifestVersion || accepted.ManifestDigest != thirdDigest {
		t.Fatalf("replayed manifest = %+v", accepted)
	}
	if _, err := guard.EvaluateSignedChain(signed[:2], keyring); err != nil {
		t.Fatalf("idempotent full-chain replay failed: %v", err)
	}
}
