package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	dragonboat "github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

func TestDragonboatThreeReplicaRecoveryAndMembershipChange(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Dragonboat integration test in short mode")
	}
	for _, name := range []string{"dragonboat", "config", "logdb", "raft", "rsm", "transport"} {
		log := logger.GetLogger(name)
		log.SetLevel(logger.CRITICAL)
		t.Cleanup(func() { log.SetLevel(logger.INFO) })
	}
	addresses := make([]string, 4)
	for index := range addresses {
		addresses[index] = freeTCPAddress(t)
	}
	nodeHosts := make([]*dragonboat.NodeHost, 4)
	for index, address := range addresses {
		expert := dbconfig.GetDefaultExpertConfig()
		expert.LogDB = dbconfig.GetTinyMemLogDBConfig()
		expert.Engine.SnapshotShards = 2
		config := dbconfig.NodeHostConfig{
			DeploymentID: 0x46, NodeHostDir: t.TempDir(), RTTMillisecond: 2,
			RaftAddress: address, Expert: expert,
		}
		nodeHost, err := dragonboat.NewNodeHost(config)
		if err != nil {
			t.Fatal(err)
		}
		nodeHosts[index] = nodeHost
		t.Cleanup(nodeHost.Close)
	}

	manifest := testManifest(2, "generation-integration")
	for index := 0; index < 3; index++ {
		manifest.Members[index].RaftEndpoint = addresses[index]
	}
	if err := manifest.Validate(); err != nil {
		t.Fatal(err)
	}
	initial := map[uint64]dragonboat.Target{1: addresses[0], 2: addresses[1], 3: addresses[2]}
	for index := 0; index < 3; index++ {
		if err := nodeHosts[index].StartReplica(initial, false, NewSystemStateMachine,
			integrationRaftConfig(SystemRaftShardID, uint64(index+1), false)); err != nil {
			t.Fatal(err)
		}
	}
	digest, _ := manifest.Digest()
	bootstrapRaw, err := EncodeSystemCommand(SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	systemResult := proposeEventually(t, nodeHosts[:3], SystemRaftShardID, bootstrapRaw)
	var bootstrapResult SystemApplyResult
	if err := json.Unmarshal(systemResult.Data, &bootstrapResult); err != nil || !bootstrapResult.Applied {
		t.Fatalf("System bootstrap result = %+v, %v", bootstrapResult, err)
	}
	for _, nodeHost := range nodeHosts[:3] {
		waitForSystemState(t, nodeHost, func(state SystemState) bool {
			return state.Initialized && state.ActiveManifestDigest == digest
		})
	}

	identity := routeShardIdentity(t, manifest, "/integration", "ready")
	dataRaftShardID := DataRaftShardID(identity.ShardID)
	for index := 0; index < 3; index++ {
		if err := nodeHosts[index].StartReplica(initial, false, NewDataStateMachine,
			integrationRaftConfig(dataRaftShardID, uint64(index+1), false)); err != nil {
			t.Fatal(err)
		}
	}
	bootstrap, err := NewDataShardBootstrap(manifest, identity.ShardID)
	if err != nil {
		t.Fatal(err)
	}
	initializeRaw, err := EncodeDataCommand(DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	dataResult := decodeIntegrationDataResult(t, proposeEventually(t, nodeHosts[:3], dataRaftShardID, initializeRaw))
	if !dataResult.Applied {
		t.Fatalf("data bootstrap result = %+v", dataResult)
	}

	starting := routeStarting(t, manifest, "/integration", "ready", "sandbox-integration", 1, true)
	putStarting, err := EncodeDataCommand(DataCommand{
		Type: DataPutRoute, Identity: identity, Expect: RevisionExpectation{Absent: true}, Route: &starting,
	})
	if err != nil {
		t.Fatal(err)
	}
	startingResult := decodeIntegrationDataResult(t, proposeEventually(t, nodeHosts[:3], dataRaftShardID, putStarting))
	ready := readyRecord(starting, 1)
	putReady, err := EncodeDataCommand(DataCommand{
		Type: DataPutRoute, Identity: identity,
		Expect: RevisionExpectation{LogIndex: startingResult.Revision}, Route: &ready,
	})
	if err != nil {
		t.Fatal(err)
	}
	readyResult := decodeIntegrationDataResult(t, proposeEventually(t, nodeHosts[:3], dataRaftShardID, putReady))
	if !readyResult.Applied {
		t.Fatalf("READY result = %+v", readyResult)
	}
	query := DataLookup{Route: &routeapi.ReadRouteRequest{
		RequestIdentity: routeIdentity(identity), Group: "/integration", RouteKey: "ready",
	}}
	for _, nodeHost := range nodeHosts[:3] {
		waitForDataLookup(t, nodeHost, dataRaftShardID, query, routeapi.ReadReady)
	}

	snapshotContext, cancelSnapshot := context.WithTimeout(context.Background(), 10*time.Second)
	if _, err := nodeHosts[0].SyncRequestSnapshot(snapshotContext, dataRaftShardID, dragonboat.SnapshotOption{}); err != nil {
		cancelSnapshot()
		t.Fatal(err)
	}
	cancelSnapshot()
	if err := nodeHosts[0].StopReplica(dataRaftShardID, 1); err != nil {
		t.Fatal(err)
	}
	integrationEventually(t, func() error {
		return nodeHosts[0].StartReplica(nil, false, NewDataStateMachine,
			integrationRaftConfig(dataRaftShardID, 1, false))
	})
	waitForDataLookup(t, nodeHosts[0], dataRaftShardID, query, routeapi.ReadReady)

	membership := getMembershipEventually(t, nodeHosts[:3], SystemRaftShardID)
	requester := nodeHosts[0]
	changeContext, cancelChange := context.WithTimeout(context.Background(), 10*time.Second)
	if err := requester.SyncRequestAddNonVoting(
		changeContext, SystemRaftShardID, 4, addresses[3], membership.ConfigChangeID,
	); err != nil {
		cancelChange()
		t.Fatal(err)
	}
	cancelChange()
	if err := nodeHosts[3].StartReplica(nil, true, NewSystemStateMachine,
		integrationRaftConfig(SystemRaftShardID, 4, true)); err != nil {
		t.Fatal(err)
	}
	waitForSystemState(t, nodeHosts[3], func(state SystemState) bool {
		return state.Initialized && state.ActiveManifestDigest == digest
	})
	membership = getMembershipEventually(t, nodeHosts[:3], SystemRaftShardID)
	changeContext, cancelChange = context.WithTimeout(context.Background(), 10*time.Second)
	if err := requester.SyncRequestAddReplica(
		changeContext, SystemRaftShardID, 4, addresses[3], membership.ConfigChangeID,
	); err != nil {
		cancelChange()
		t.Fatal(err)
	}
	cancelChange()
	integrationEventually(t, func() error {
		membership = getMembership(nodeHosts[:3], SystemRaftShardID)
		if membership == nil {
			return errors.New("membership unavailable")
		}
		if _, voting := membership.Nodes[4]; !voting {
			return errors.New("replica 4 is not promoted")
		}
		return nil
	})
	changeContext, cancelChange = context.WithTimeout(context.Background(), 10*time.Second)
	if err := requester.SyncRequestDeleteReplica(
		changeContext, SystemRaftShardID, 3, membership.ConfigChangeID,
	); err != nil {
		cancelChange()
		t.Fatal(err)
	}
	cancelChange()
	integrationEventually(t, func() error {
		membership = getMembership(nodeHosts[:3], SystemRaftShardID)
		if membership == nil {
			return errors.New("membership unavailable")
		}
		if _, removed := membership.Removed[3]; !removed {
			return errors.New("replica 3 is not removed")
		}
		return nil
	})
}

func integrationRaftConfig(shardID, replicaID uint64, nonVoting bool) dbconfig.Config {
	return dbconfig.Config{
		ShardID: shardID, ReplicaID: replicaID, CheckQuorum: true, PreVote: true,
		ElectionRTT: 20, HeartbeatRTT: 2, OrderedConfigChange: true,
		SnapshotEntries: 0, CompactionOverhead: 0, MaxInMemLogSize: 16 << 20,
		IsNonVoting: nonVoting,
	}
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func proposeEventually(t *testing.T, nodeHosts []*dragonboat.NodeHost, shardID uint64, command []byte) sm.Result {
	t.Helper()
	var result sm.Result
	integrationEventually(t, func() error {
		var last error
		for _, nodeHost := range nodeHosts {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			candidate, err := nodeHost.SyncPropose(ctx, nodeHost.GetNoOPSession(shardID), command)
			cancel()
			if err == nil {
				result = candidate
				return nil
			}
			last = err
		}
		return last
	})
	return result
}

func waitForSystemState(t *testing.T, nodeHost *dragonboat.NodeHost, accept func(SystemState) bool) {
	t.Helper()
	integrationEventually(t, func() error {
		value, err := nodeHost.StaleRead(SystemRaftShardID, SystemStateLookup{})
		if err != nil {
			return err
		}
		state, ok := value.(SystemState)
		if !ok || !accept(state) {
			return fmt.Errorf("System state not ready: %+v", value)
		}
		return nil
	})
}

func waitForDataLookup(
	t *testing.T,
	nodeHost *dragonboat.NodeHost,
	shardID uint64,
	query DataLookup,
	want string,
) {
	t.Helper()
	integrationEventually(t, func() error {
		value, err := nodeHost.StaleRead(shardID, query)
		if err != nil {
			return err
		}
		result, ok := value.(DataLookupResult)
		if !ok || result.Route == nil || result.Route.Outcome != want {
			return fmt.Errorf("data lookup not ready: %+v", value)
		}
		return nil
	})
}

func getMembershipEventually(t *testing.T, nodeHosts []*dragonboat.NodeHost, shardID uint64) *dragonboat.Membership {
	t.Helper()
	var membership *dragonboat.Membership
	integrationEventually(t, func() error {
		membership = getMembership(nodeHosts, shardID)
		if membership == nil {
			return errors.New("membership unavailable")
		}
		return nil
	})
	return membership
}

func getMembership(nodeHosts []*dragonboat.NodeHost, shardID uint64) *dragonboat.Membership {
	for _, nodeHost := range nodeHosts {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		membership, err := nodeHost.SyncGetShardMembership(ctx, shardID)
		cancel()
		if err == nil {
			return membership
		}
	}
	return nil
}

func decodeIntegrationDataResult(t *testing.T, result sm.Result) DataApplyResult {
	t.Helper()
	var applied DataApplyResult
	if err := json.Unmarshal(result.Data, &applied); err != nil {
		t.Fatal(err)
	}
	return applied
}

func integrationEventually(t *testing.T, check func() error) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		if err := check(); err == nil {
			return
		} else {
			last = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met: %v", last)
}
