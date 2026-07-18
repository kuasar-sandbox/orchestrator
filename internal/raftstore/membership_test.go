package raftstore

import (
	"context"
	"testing"

	dragonboat "github.com/lni/dragonboat/v4"
)

func transitionManifest(t *testing.T) Manifest {
	t.Helper()
	manifest := testManifest(2, "generation-1")
	manifest.Members = append(manifest.Members, RegistryMember{
		MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001",
	})
	desired := []ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-d", ReplicaID: 4},
	}
	manifest.SystemReplicas = append([]ReplicaPlacement(nil), desired...)
	for index := range manifest.DataShards {
		manifest.DataShards[index].Replicas = append([]ReplicaPlacement(nil), desired...)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestOrderedMembershipChangeRequiresCatchUpBeforeOldReplicaRemoval(t *testing.T) {
	manifest := transitionManifest(t)
	digest, _ := manifest.Digest()
	host := newFakeNodeHost()
	shardID := DataRaftShardID(0)
	host.memberships[shardID] = &dragonboat.Membership{
		ConfigChangeID: 10,
		Nodes: map[uint64]string{
			1: "registry-a:63001", 2: "registry-b:63001", 3: "registry-c:63001",
		},
		NonVotings: map[uint64]string{}, Witnesses: map[uint64]string{}, Removed: map[uint64]struct{}{},
	}
	runtime := &Runtime{manifest: manifest, manifestDigest: digest, nodeHost: host}
	ctx := context.Background()

	if err := runtime.AddNonVoting(ctx, shardID, 4, "registry-d"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RemoveOldReplica(ctx, shardID, 3); err == nil {
		t.Fatal("old replica removed before the desired voter was promoted")
	}
	if err := runtime.PromoteNonVoting(ctx, ReplicaCatchUpProof{
		ShardID: shardID, ReplicaID: 4, ManifestDigest: digest, LeaderApplied: 100, TargetApplied: 99,
	}); err == nil {
		t.Fatal("lagging non-voting replica was promoted")
	}
	if err := runtime.PromoteNonVoting(ctx, ReplicaCatchUpProof{
		ShardID: shardID, ReplicaID: 4, ManifestDigest: digest, LeaderApplied: 100, TargetApplied: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RemoveOldReplica(ctx, shardID, 3); err != nil {
		t.Fatal(err)
	}
	membership := host.memberships[shardID]
	if _, found := membership.Nodes[4]; !found {
		t.Fatal("desired replica was not promoted")
	}
	if _, removed := membership.Removed[3]; !removed {
		t.Fatal("old replica removal was not committed")
	}
}
