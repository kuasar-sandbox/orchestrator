package raftstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	dragonboat "github.com/lni/dragonboat/v4"
)

func TestLocalReplicaRemovalResumesAfterCleanupFailure(t *testing.T) {
	registryLayout := transitionRegistryLayout(t)
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	member, found := registryLayoutMember(registryLayout, "registry-c")
	if !found {
		t.Fatal("removed member absent from registryLayout member catalog")
	}
	shardID := DataRaftShardID(0)
	replica := LocalReplicaEnrollment{
		ShardID: shardID, ReplicaID: 3, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
	}
	enrollment := LocalEnrollment{
		Version: localEnrollmentVersion, ClusterID: registryLayout.ClusterID,
		RegistryGeneration: registryLayout.RegistryGeneration, MemberID: member.MemberID,
		DeploymentID: 1, RaftAddress: member.RaftEndpoint,
		NodeHostDir: "/nodehost", StateEngineDir: "/state",
		RuntimeConfigDigest: digestFor("runtime-config"), RegistryLayoutVersion: registryLayout.RegistryLayoutVersion,
		RegistryLayoutDigest: digest, Mode: EnrollmentBootstrap, Replicas: []LocalReplicaEnrollment{replica},
	}
	store := EnrollmentStore{Path: filepath.Join(t.TempDir(), "enrollment.json")}
	if err := store.Store(enrollment); err != nil {
		t.Fatal(err)
	}
	engine, err := OpenPebbleStateEngine(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	host := newFakeNodeHost()
	host.history[[2]uint64{shardID, replica.ReplicaID}] = true
	host.memberships[shardID] = &dragonboat.Membership{
		Nodes: map[uint64]string{}, NonVotings: map[uint64]string{}, Witnesses: map[uint64]string{},
		Removed: map[uint64]struct{}{replica.ReplicaID: {}},
	}
	host.removeDataErr = errors.New("injected cleanup failure")
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, member: member, nodeHost: host,
		enrollment: enrollment, enrollmentStore: store, stateEngine: engine,
	}
	if err := runtime.RemoveLocalReplicaData(context.Background(), shardID); err == nil {
		t.Fatal("injected cleanup failure was hidden")
	}
	if runtime.enrollment.Replicas[0].LocalState != ReplicaRemoving {
		t.Fatalf("failed cleanup state = %s", runtime.enrollment.Replicas[0].LocalState)
	}
	delete(host.memberships, shardID)
	delete(host.history, [2]uint64{shardID, replica.ReplicaID})
	host.stopErr = dragonboat.ErrShardNotFound
	if err := runtime.RemoveLocalReplicaData(context.Background(), shardID); err != nil {
		t.Fatal(err)
	}
	if runtime.enrollment.Replicas[0].LocalState != ReplicaRemoved {
		t.Fatalf("completed cleanup state = %s", runtime.enrollment.Replicas[0].LocalState)
	}
	if host.stopCalls != 1 || host.removeCalls != 2 {
		t.Fatalf("cleanup calls: stop=%d remove=%d", host.stopCalls, host.removeCalls)
	}
	if err := runtime.startReplica(0); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("removed replica start error = %v", err)
	}
}

func transitionRegistryLayout(t *testing.T) RegistryLayout {
	t.Helper()
	registryLayout := testRegistryLayout(2, "generation-1")
	registryLayout.Members = append(registryLayout.Members, RegistryMember{
		MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001",
	})
	desired := []ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-d", ReplicaID: 4},
	}
	registryLayout.SystemReplicas = append([]ReplicaPlacement(nil), desired...)
	for index := range registryLayout.DataShards {
		registryLayout.DataShards[index].Replicas = append([]ReplicaPlacement(nil), desired...)
	}
	if err := registryLayout.Validate(); err != nil {
		t.Fatal(err)
	}
	return registryLayout
}

func TestOrderedMembershipChangeRequiresCatchUpBeforeOldReplicaRemoval(t *testing.T) {
	registryLayout := transitionRegistryLayout(t)
	digest, _ := registryLayout.Digest()
	host := newFakeNodeHost()
	shardID := DataRaftShardID(0)
	host.memberships[shardID] = &dragonboat.Membership{
		ConfigChangeID: 10,
		Nodes: map[uint64]string{
			1: "registry-a:63001", 2: "registry-b:63001", 3: "registry-c:63001",
		},
		NonVotings: map[uint64]string{}, Witnesses: map[uint64]string{}, Removed: map[uint64]struct{}{},
	}
	runtime := &Runtime{registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host}
	ctx := context.Background()

	if err := runtime.AddNonVoting(ctx, shardID, 4, "registry-d"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RemoveJoiningReplica(ctx, shardID, 4); err == nil {
		t.Fatal("desired learner was removed from consensus membership")
	}
	if _, present := host.memberships[shardID].NonVotings[4]; !present {
		t.Fatal("desired learner disappeared after rejected removal")
	}
	if err := runtime.RemoveOldReplica(ctx, shardID, 3); err == nil {
		t.Fatal("old replica removed before the desired voter was promoted")
	}
	request := ReplicaCatchUpRequest{
		ShardID: shardID, ReplicaID: 4, MemberID: "registry-d",
		RegistryLayoutDigest: digest, MinimumAppliedIndex: 100,
	}
	if err := runtime.promoteNonVoting(ctx, request, ReplicaCatchUpProof{
		ShardID: shardID, ReplicaID: 4, MemberID: "registry-d",
		RegistryLayoutDigest: digest, AppliedIndex: 99,
	}); err == nil {
		t.Fatal("lagging non-voting replica was promoted")
	}
	if err := runtime.promoteNonVoting(ctx, request, ReplicaCatchUpProof{
		ShardID: shardID, ReplicaID: 4, MemberID: "registry-d",
		RegistryLayoutDigest: digest, AppliedIndex: 100,
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

func TestLocalLearnerCatchUpProofIsBoundToEnrollmentAndLinearizedState(t *testing.T) {
	registryLayout := transitionRegistryLayout(t)
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	identity := registryLayoutShardIdentity(t, registryLayout, 0)
	data := initializeDataShard(t, registryLayout, identity)
	shardID := DataRaftShardID(0)
	member, found := registryLayoutMember(registryLayout, "registry-d")
	if !found {
		t.Fatal("target member absent")
	}
	host := newFakeNodeHost()
	host.memberships[shardID] = &dragonboat.Membership{
		Nodes: map[uint64]string{}, NonVotings: map[uint64]string{4: member.RaftEndpoint},
		Witnesses: map[uint64]string{}, Removed: map[uint64]struct{}{},
	}
	host.read = func(gotShardID uint64, _ any) (any, error) {
		if gotShardID != shardID {
			return nil, ErrNoLocalReplica
		}
		return data, nil
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, member: member, nodeHost: host,
		enrollmentStore: EnrollmentStore{Path: t.TempDir() + "/enrollment.json"},
		enrollment: LocalEnrollment{
			Version: localEnrollmentVersion, ClusterID: registryLayout.ClusterID,
			RegistryGeneration: registryLayout.RegistryGeneration, MemberID: member.MemberID,
			DeploymentID: 1, RaftAddress: member.RaftEndpoint,
			NodeHostDir: "/nodehost", StateEngineDir: "/state",
			RuntimeConfigDigest: digestFor("runtime-config"), RegistryLayoutVersion: registryLayout.RegistryLayoutVersion,
			RegistryLayoutDigest: digest, Mode: EnrollmentJoin,
			Replicas: []LocalReplicaEnrollment{{
				ShardID: shardID, ReplicaID: 4, StartPlan: ReplicaJoin,
				NonVoting: true, LocalState: ReplicaActive,
			}},
		},
	}
	request := ReplicaCatchUpRequest{
		ShardID: shardID, ReplicaID: 4, MemberID: member.MemberID,
		RegistryLayoutDigest: digest, MinimumAppliedIndex: data.LastApplied,
	}
	proof, err := runtime.ProveLocalReplicaCaughtUp(context.Background(), request)
	if err != nil || proof.AppliedIndex != data.LastApplied {
		t.Fatalf("local catch-up proof = %+v, %v", proof, err)
	}
	request.MinimumAppliedIndex++
	if _, err := runtime.ProveLocalReplicaCaughtUp(context.Background(), request); err == nil {
		t.Fatal("learner below the linearized barrier produced a proof")
	}
	host.memberships[shardID].Nodes[4] = member.RaftEndpoint
	delete(host.memberships[shardID].NonVotings, 4)
	if err := runtime.ConfirmLocalReplicaPromoted(context.Background(), ReplicaPromotionRequest{
		ShardID: shardID, ReplicaID: 4, MemberID: member.MemberID, RegistryLayoutDigest: digest,
	}); err != nil {
		t.Fatal(err)
	}
	if runtime.enrollment.Replicas[0].NonVoting {
		t.Fatal("committed voter role was not persisted in local enrollment")
	}
}

func TestRemoteCatchUpProbeCannotReturnProofForAnotherTarget(t *testing.T) {
	request := ReplicaCatchUpRequest{
		ShardID: DataRaftShardID(0), ReplicaID: 4, MemberID: "registry-d",
		RegistryLayoutDigest: digestFor("registryLayout"), MinimumAppliedIndex: 100,
	}
	runtime := &Runtime{
		member: RegistryMember{MemberID: "registry-a"},
		transitionClient: ReplicaTransitionClientFuncs{
			Probe: func(context.Context, ReplicaCatchUpRequest) (ReplicaCatchUpProof, error) {
				return ReplicaCatchUpProof{
					ShardID: request.ShardID, ReplicaID: request.ReplicaID, MemberID: "registry-e",
					RegistryLayoutDigest: request.RegistryLayoutDigest, AppliedIndex: request.MinimumAppliedIndex,
				}, nil
			},
			Confirm: func(context.Context, ReplicaPromotionRequest) error { return nil },
		},
	}
	if _, err := runtime.probeReplicaCatchUp(context.Background(), request); err == nil {
		t.Fatal("remote probe proof for another member was accepted")
	}
}

func TestMembershipChangeRejectsSecondReplicaOnOneNodeHost(t *testing.T) {
	registryLayout := transitionRegistryLayout(t)
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	shardID := DataRaftShardID(0)
	host := newFakeNodeHost()
	host.memberships[shardID] = &dragonboat.Membership{
		ConfigChangeID: 10,
		Nodes: map[uint64]string{
			1: "registry-a:63001", 2: "registry-b:63001", 3: "registry-c:63001",
			5: "registry-d:63001",
		},
		NonVotings: map[uint64]string{}, Witnesses: map[uint64]string{}, Removed: map[uint64]struct{}{},
	}
	runtime := &Runtime{registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host}
	if err := runtime.AddNonVoting(context.Background(), shardID, 4, "registry-d"); err == nil {
		t.Fatal("second replica ID was added to an already occupied NodeHost target")
	}
	if _, added := host.memberships[shardID].NonVotings[4]; added {
		t.Fatal("invalid target collision mutated consensus membership")
	}
}
