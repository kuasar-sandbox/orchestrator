package raftstore

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	dragonboat "github.com/lni/dragonboat/v4"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestFenceOutboxAckMustCoverExactFinalWatermark(t *testing.T) {
	fence := clusterstate.ExecutionFence{LastEventSeq: 7, FinalOutboxWatermark: 9}
	proof := digestFor("outbox-ack")
	for _, watermark := range []uint64{7, 8, 10} {
		if (FenceOutboxAckEvidence{AckedWatermark: watermark, ProofDigest: proof}).validates(fence) {
			t.Fatalf("non-final outbox watermark %d was accepted", watermark)
		}
	}
	if !(FenceOutboxAckEvidence{AckedWatermark: 9, ProofDigest: proof}).validates(fence) {
		t.Fatal("exact final outbox watermark was rejected")
	}
	fence.FinalOutboxWatermark = fence.LastEventSeq - 1
	if (FenceOutboxAckEvidence{AckedWatermark: fence.FinalOutboxWatermark, ProofDigest: proof}).validates(fence) {
		t.Fatal("regressed final outbox watermark was accepted")
	}
}

func TestRuntimeCompactsFenceOnlyAfterRetentionAndEveryReplicaProof(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-1")
	registryLayout.ServePermitMaxMillis = 1_000
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	identity := routeShardIdentity(t, registryLayout, "/g", "rk")
	data := initializeDataShard(t, registryLayout, identity)
	starting := routeStarting(t, registryLayout, "/g", "rk", "sandbox-1", 1, true)
	applyDataOK(t, &data, 2, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	ready := readyRecord(starting, 1)
	applyDataOK(t, &data, 3, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 2}, Route: &ready,
	})
	deleting := deletingRecord(ready)
	applyDataOK(t, &data, 4, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 3}, Route: &deleting,
	})
	tombstone, fence := terminalRouteAndFence(deleting, 2)
	applyDataOK(t, &data, 5, DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{LogIndex: 4}, Route: &tombstone,
	})
	applyDataOK(t, &data, 6, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})

	system, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	system, _ = applySystem(t, system, 2, SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	host := newFakeNodeHost()
	raftShardID := DataRaftShardID(identity.ShardID)
	host.history[[2]uint64{raftShardID, 1}] = true
	host.memberships[raftShardID] = &dragonboat.Membership{
		Nodes: map[uint64]string{
			1: "registry-a:63001", 2: "registry-b:63001", 3: "registry-c:63001",
		},
		NonVotings: map[uint64]string{}, Witnesses: map[uint64]string{}, Removed: map[uint64]struct{}{},
	}
	host.read = func(shardID uint64, query any) (any, error) {
		if shardID == SystemRaftShardID {
			return system, nil
		}
		switch value := query.(type) {
		case DataLookup:
			return LookupData(data, value)
		case DataStateLookup:
			return cloneDataStateForLookup(data), nil
		default:
			return nil, ErrNoLocalReplica
		}
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		if command, decodeErr := DecodeSystemCommand(raw); decodeErr == nil {
			var result SystemApplyResult
			system, result = ApplySystemCommand(system, system.LastApplied+1, command)
			encoded, marshalErr := json.Marshal(result)
			return sm.Result{Data: encoded}, marshalErr
		}
		command, decodeErr := DecodeDataCommand(raw)
		if decodeErr != nil {
			return sm.Result{}, decodeErr
		}
		result := ApplyDataCommand(&data, data.LastApplied+1, command)
		encoded, marshalErr := json.Marshal(result)
		return sm.Result{Data: encoded}, marshalErr
	}
	permitCache := NewPermitCache(time.Now)
	if err := permitCache.Install(PermitGrant{
		PermitIdentity: system.Identity(), CommitIndex: system.LastApplied,
		MaxLifetimeMillis: registryLayout.ServePermitMaxMillis,
		ServeGate:         true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	var remoteProofs atomic.Int32
	runtime := &Runtime{
		config: RuntimeConfig{Tuning: func() RuntimeTuning {
			tuning := DefaultRuntimeTuning()
			tuning.FenceRetentionMillis = 1
			return tuning
		}()},
		registryLayout: registryLayout, registryLayoutDigest: digest, member: registryLayout.Members[0],
		nodeHost: host, permitCache: permitCache,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: raftShardID, ReplicaID: 1, StartPlan: ReplicaInitial,
			LocalState: ReplicaActive,
		}}},
		outboxAckVerifier: FenceOutboxAckVerifierFunc(func(_ context.Context, request FenceOutboxAckRequest) (FenceOutboxAckEvidence, error) {
			if request.SandboxID != "sandbox-1" || request.FinalOutboxWatermark != 2 {
				t.Fatalf("outbox ACK request = %+v", request)
			}
			return FenceOutboxAckEvidence{AckedWatermark: 2, ProofDigest: digestFor("outbox-ack")}, nil
		}),
		transitionClient: ReplicaTransitionClientFuncs{
			Applied: func(_ context.Context, request ReplicaAppliedRequest) (ReplicaAppliedProof, error) {
				remoteProofs.Add(1)
				return ReplicaAppliedProof{
					ShardID: request.ShardID, ReplicaID: request.ReplicaID, MemberID: request.MemberID,
					RegistryLayoutDigest: request.RegistryLayoutDigest, AppliedIndex: data.LastApplied,
				}, nil
			},
		},
	}
	if err := runtime.CompactExecutionFence(
		context.Background(), identity, "/g", "rk", "sandbox-1",
	); err != nil {
		t.Fatal(err)
	}
	if remoteProofs.Load() != 2 {
		t.Fatalf("remote applied-index proofs = %d, want 2", remoteProofs.Load())
	}
	if _, found := data.Fences[fenceMapKey("/g", "rk", "sandbox-1")]; found {
		t.Fatal("execution fence remained after complete compaction proof")
	}
	compactedRoute := data.Routes[routeMapKey("/g", "rk")]
	if compactedRoute.Tombstone == nil || !compactedRoute.Tombstone.FenceCompacted {
		t.Fatal("Route tombstone did not retain the fence-compaction marker")
	}
	replayed := ApplyDataCommand(&data, data.LastApplied+1, DataCommand{
		Type: DataPutFence, Identity: identity, Expect: RevisionExpectation{Absent: true}, Fence: &fence,
	})
	if !replayed.Conflict {
		t.Fatal("compacted execution fence was replayed")
	}
	replacement := routeStarting(t, registryLayout, "/g", "rk", "sandbox-2", 2, false)
	applyDataOK(t, &data, data.LastApplied+1, DataCommand{
		Type: DataPutRoute, Identity: identity,
		Expect: RevisionExpectation{LogIndex: compactedRoute.Revision.LogIndex}, Route: &replacement,
	})
}
