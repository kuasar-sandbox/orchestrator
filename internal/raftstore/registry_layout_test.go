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

func testRegistryLayout(shards uint32, generation string) RegistryLayout {
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
	return RegistryLayout{
		FormatVersion: RegistryLayoutFormatV1, ClusterID: "cluster-1", RegistryGeneration: generation,
		RegistryLayoutVersion: 1, SchemaVersion: 1, ProtocolVersion: 1, HashVersion: "ShardHashV1",
		VirtualShardCount: shards, RouteBucketCount: 16, BuildBucketCount: 16,
		ReplicationFactor: 3, ServePermitMaxMillis: 5000, BootstrapTokenDigest: digestFor("bootstrap-" + generation),
		Members: members, SystemReplicas: replicas, DataShards: placements,
	}
}

func finalizeConsensusSuccessor(
	t *testing.T,
	predecessor RegistryLayout,
	predecessorDigest string,
	successor RegistryLayout,
) (RegistryLayout, SystemState) {
	t.Helper()
	successor.Predecessor = &PredecessorProof{
		RegistryGeneration:   predecessor.RegistryGeneration,
		RegistryLayoutDigest: predecessorDigest,
		ServePermitMaxMillis: predecessor.ServePermitMaxMillis,
		Kind:                 RolloverConsensusClosure,
	}
	intentDigest, err := successor.RolloverIntentDigest()
	if err != nil {
		t.Fatal(err)
	}
	source := SystemState{
		Initialized: true, ClusterID: predecessor.ClusterID,
		RegistryGeneration: predecessor.RegistryGeneration, SystemEpoch: 1,
		SchemaVersion: predecessor.SchemaVersion, ProtocolVersion: predecessor.ProtocolVersion,
		VirtualShardCount:           predecessor.VirtualShardCount,
		ActiveRegistryLayoutVersion: predecessor.RegistryLayoutVersion, ActiveRegistryLayoutDigest: predecessorDigest,
		ServePermitMaxMillis:     predecessor.ServePermitMaxMillis,
		PredecessorDrainComplete: true, LastApplied: 1,
	}
	if predecessor.Predecessor != nil {
		source.HasPredecessor = true
		source.PredecessorRegistryGeneration = predecessor.Predecessor.RegistryGeneration
		source.PredecessorRegistryLayoutDigest = predecessor.Predecessor.RegistryLayoutDigest
		source.PredecessorProofKind = predecessor.Predecessor.Kind
		source.PredecessorProofDigest = predecessor.Predecessor.ProofDigest
		source.PredecessorProofCommitIndex = predecessor.Predecessor.CommitIndex
		source.PredecessorTargetRegistryLayoutIntentDigest = predecessor.Predecessor.TargetRegistryLayoutIntentDigest
		source.PredecessorPermitMaxMillis = predecessor.Predecessor.ServePermitMaxMillis
	}
	if err := source.Validate(); err != nil {
		t.Fatal(err)
	}
	closed, result := ApplySystemCommand(source, 2, SystemCommand{
		Type: SystemCloseRegistryGeneration,
		Closure: &RegistryGenerationClosure{
			TargetRegistryGeneration:         successor.RegistryGeneration,
			TargetRegistryLayoutIntentDigest: intentDigest,
			Kind:                             RolloverConsensusClosure,
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

func TestRegistryLayoutReplicaIdentityIsScopedToEachShard(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-1")
	registryLayout.DataShards[0].Replicas[0].ReplicaID = 101
	registryLayout.DataShards[1].Replicas[0].ReplicaID = 201
	if err := registryLayout.Validate(); err != nil {
		t.Fatalf("per-shard replica IDs rejected: %v", err)
	}
	registryLayout.DataShards[0].Replicas[1].ReplicaID = 101
	if err := registryLayout.Validate(); err == nil {
		t.Fatal("duplicate replica ID within one shard accepted")
	}
}

func TestRegistryLayoutSignatureAndFixedPlacement(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	registryLayout := testRegistryLayout(4, "generation-1")
	signed, err := SignRegistryLayout(registryLayout, "root-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := signed.Verify(map[string]ed25519.PublicKey{"root-1": publicKey})
	if err != nil {
		t.Fatal(err)
	}
	want, err := registryLayout.Digest()
	if err != nil || digest != want {
		t.Fatalf("registryLayout digest = %q, %v; want %q", digest, err, want)
	}

	signed.RegistryLayout.DataShards[0].Replicas[0].MemberID = "registry-z"
	if _, err := signed.Verify(map[string]ed25519.PublicKey{"root-1": publicKey}); err == nil {
		t.Fatal("tampered registryLayout signature verified")
	}
	if got, ok := LogicalShardID(DataRaftShardID(3)); !ok || got != 3 {
		t.Fatalf("logical shard mapping = %d, %v", got, ok)
	}
}

func TestRegistryLayoutRejectsIncompleteOrUnstablePlacement(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	registryLayout.Members[0], registryLayout.Members[1] = registryLayout.Members[1], registryLayout.Members[0]
	if err := registryLayout.Validate(); err == nil {
		t.Fatal("unordered member set accepted")
	}
	registryLayout = testRegistryLayout(4, "generation-1")
	registryLayout.DataShards = registryLayout.DataShards[:3]
	if err := registryLayout.Validate(); err == nil {
		t.Fatal("incomplete shard placement accepted")
	}
}

func TestRegistryLayoutGuardRejectsRollbackEquivocationAndUnrelatedGeneration(t *testing.T) {
	first := testRegistryLayout(4, "generation-1")
	firstDigest, _ := first.Digest()
	accepted, err := FirstAcceptedRegistryLayout(first, firstDigest)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.Members = append([]RegistryMember(nil), first.Members...)
	second.RegistryLayoutVersion = 2
	second.PreviousRegistryLayoutVersion = 1
	second.PreviousRegistryLayoutDigest = firstDigest
	second.Members[0].InternalEndpoint = "https://registry-a-next:9443"
	secondDigest, _ := second.Digest()
	accepted, err = accepted.Accept(second, secondDigest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := accepted.Accept(first, firstDigest); err == nil {
		t.Fatal("registryLayout rollback accepted")
	}
	equivocation := second
	equivocation.Members = append([]RegistryMember(nil), second.Members...)
	equivocation.Members[0].InternalEndpoint = "https://registry-a-equivocation:9443"
	equivocationDigest, _ := equivocation.Digest()
	if _, err := accepted.Accept(equivocation, equivocationDigest); err == nil {
		t.Fatal("same-version registryLayout equivocation accepted")
	}

	successor, _ := finalizeConsensusSuccessor(t, second, secondDigest, testRegistryLayout(4, "generation-2"))
	successorDigest, _ := successor.Digest()
	next, err := accepted.Accept(successor, successorDigest)
	if err != nil || next.RegistryGeneration != "generation-2" {
		t.Fatalf("linked successor = %+v, %v", next, err)
	}
	successor.Predecessor.RegistryGeneration = "unrelated"
	unrelatedDigest, _ := successor.Digest()
	if _, err := accepted.Accept(successor, unrelatedDigest); err == nil {
		t.Fatal("unrelated generation accepted")
	}
}

func TestRegistryLayoutGuardFreezesGenerationParameters(t *testing.T) {
	first := testRegistryLayout(4, "generation-1")
	firstDigest, _ := first.Digest()
	accepted, err := FirstAcceptedRegistryLayout(first, firstDigest)
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(*RegistryLayout){
		"schema":          func(registryLayout *RegistryLayout) { registryLayout.SchemaVersion++ },
		"protocol":        func(registryLayout *RegistryLayout) { registryLayout.ProtocolVersion++ },
		"route buckets":   func(registryLayout *RegistryLayout) { registryLayout.RouteBucketCount *= 2 },
		"build buckets":   func(registryLayout *RegistryLayout) { registryLayout.BuildBucketCount *= 2 },
		"permit lifetime": func(registryLayout *RegistryLayout) { registryLayout.ServePermitMaxMillis++ },
		"bootstrap token": func(registryLayout *RegistryLayout) {
			registryLayout.BootstrapTokenDigest = digestFor("replacement-token")
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			next := first
			next.RegistryLayoutVersion = 2
			next.PreviousRegistryLayoutVersion = 1
			next.PreviousRegistryLayoutDigest = firstDigest
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

func TestRegistryLayoutGuardBindsSuccessorToPredecessorPermitLifetime(t *testing.T) {
	first := testRegistryLayout(4, "generation-1")
	firstDigest, _ := first.Digest()
	accepted, err := FirstAcceptedRegistryLayout(first, firstDigest)
	if err != nil {
		t.Fatal(err)
	}
	successor := testRegistryLayout(4, "generation-2")
	successor.Predecessor = &PredecessorProof{
		RegistryGeneration: first.RegistryGeneration, RegistryLayoutDigest: firstDigest,
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

func TestConsensusRolloverProofCommitsExactSuccessorRegistryLayout(t *testing.T) {
	predecessor := testRegistryLayout(2, "generation-1")
	predecessorDigest, err := predecessor.Digest()
	if err != nil {
		t.Fatal(err)
	}

	for name, mutate := range map[string]func(*RegistryLayout){
		"bootstrap token": func(registryLayout *RegistryLayout) {
			registryLayout.BootstrapTokenDigest = digestFor("different-bootstrap-token")
		},
		"member endpoint": func(registryLayout *RegistryLayout) {
			registryLayout.Members = append([]RegistryMember(nil), registryLayout.Members...)
			registryLayout.Members[0].InternalEndpoint = "https://registry-a-new:9443"
		},
		"replica placement": func(registryLayout *RegistryLayout) {
			registryLayout.DataShards = append([]ShardPlacement(nil), registryLayout.DataShards...)
			registryLayout.DataShards[0].Replicas = append([]ReplicaPlacement(nil), registryLayout.DataShards[0].Replicas...)
			registryLayout.DataShards[0].Replicas[0].ReplicaID = 101
		},
	} {
		t.Run(name, func(t *testing.T) {
			finalized, _ := finalizeConsensusSuccessor(
				t, predecessor, predecessorDigest, testRegistryLayout(2, "generation-2"),
			)
			mutate(&finalized)
			if err := finalized.Validate(); err == nil {
				t.Fatal("registryLayout mutation retained a valid predecessor proof")
			}
		})
	}

	finalized, _ := finalizeConsensusSuccessor(
		t, predecessor, predecessorDigest, testRegistryLayout(2, "generation-2"),
	)
	finalized.Predecessor.ProofDigest = digestFor("forged-closure")
	if err := finalized.Validate(); err == nil {
		t.Fatal("forged consensus closure proof was accepted")
	}
}

func TestRegistryLayoutGuardPersistsWithRestrictedMode(t *testing.T) {
	registryLayout := testRegistryLayout(4, "generation-1")
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignRegistryLayout(registryLayout, "root-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "guard", "registryLayout.json")
	guard := RegistryLayoutGuard{Path: path}
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

func TestRegistryLayoutGuardReplaysFullChainFromDurableAnchor(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyring := map[string]ed25519.PublicKey{"root-1": publicKey}
	first := testRegistryLayout(4, "generation-1")
	firstDigest, err := first.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second := first
	second.RegistryLayoutVersion = 2
	second.PreviousRegistryLayoutVersion = 1
	second.PreviousRegistryLayoutDigest = firstDigest
	second.Members = append([]RegistryMember(nil), first.Members...)
	second.Members[0].InternalEndpoint = "https://registry-a-v2:9443"
	secondDigest, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	third := second
	third.RegistryLayoutVersion = 3
	third.PreviousRegistryLayoutVersion = 2
	third.PreviousRegistryLayoutDigest = secondDigest
	third.Members = append([]RegistryMember(nil), second.Members...)
	third.Members[0].InternalEndpoint = "https://registry-a-v3:9443"
	signed := make([]SignedRegistryLayout, 0, 3)
	for _, registryLayout := range []RegistryLayout{first, second, third} {
		value, signErr := SignRegistryLayout(registryLayout, "root-1", privateKey)
		if signErr != nil {
			t.Fatal(signErr)
		}
		signed = append(signed, value)
	}
	guard := RegistryLayoutGuard{Path: filepath.Join(t.TempDir(), "registryLayout.json")}
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
	if accepted.RegistryLayoutVersion != third.RegistryLayoutVersion || accepted.RegistryLayoutDigest != thirdDigest {
		t.Fatalf("replayed registryLayout = %+v", accepted)
	}
	if _, err := guard.EvaluateSignedChain(signed[:2], keyring); err != nil {
		t.Fatalf("idempotent full-chain replay failed: %v", err)
	}
}
