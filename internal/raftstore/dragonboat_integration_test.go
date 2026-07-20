package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	dragonboat "github.com/lni/dragonboat/v4"
	dbconfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"github.com/lni/dragonboat/v4/raftio"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type integrationSystemEvents struct {
	mu                sync.Mutex
	snapshotReceived  []raftio.SnapshotInfo
	snapshotRecovered []raftio.SnapshotInfo
}

func (*integrationSystemEvents) NodeHostShuttingDown()                       {}
func (*integrationSystemEvents) NodeUnloaded(raftio.NodeInfo)                {}
func (*integrationSystemEvents) NodeDeleted(raftio.NodeInfo)                 {}
func (*integrationSystemEvents) NodeReady(raftio.NodeInfo)                   {}
func (*integrationSystemEvents) MembershipChanged(raftio.NodeInfo)           {}
func (*integrationSystemEvents) ConnectionEstablished(raftio.ConnectionInfo) {}
func (*integrationSystemEvents) ConnectionFailed(raftio.ConnectionInfo)      {}
func (*integrationSystemEvents) SendSnapshotStarted(raftio.SnapshotInfo)     {}
func (*integrationSystemEvents) SendSnapshotCompleted(raftio.SnapshotInfo)   {}
func (*integrationSystemEvents) SendSnapshotAborted(raftio.SnapshotInfo)     {}
func (*integrationSystemEvents) SnapshotCreated(raftio.SnapshotInfo)         {}
func (*integrationSystemEvents) SnapshotCompacted(raftio.SnapshotInfo)       {}
func (*integrationSystemEvents) LogCompacted(raftio.EntryInfo)               {}
func (*integrationSystemEvents) LogDBCompacted(raftio.EntryInfo)             {}

func (e *integrationSystemEvents) SnapshotReceived(info raftio.SnapshotInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshotReceived = append(e.snapshotReceived, info)
}

func (e *integrationSystemEvents) SnapshotRecovered(info raftio.SnapshotInfo) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.snapshotRecovered = append(e.snapshotRecovered, info)
}

func (e *integrationSystemEvents) installedSnapshot(shardID, replicaID, minimumIndex uint64) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	received := false
	for _, info := range e.snapshotReceived {
		if info.ShardID == shardID && info.ReplicaID == replicaID {
			received = true
			break
		}
	}
	if !received {
		return false
	}
	for _, info := range e.snapshotRecovered {
		if info.ShardID == shardID && info.ReplicaID == replicaID && info.Index >= minimumIndex {
			return true
		}
	}
	return false
}

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
	stateEngines := make([]*PebbleStateEngine, 4)
	events := make([]*integrationSystemEvents, 4)
	for index, address := range addresses {
		stateEngine, err := OpenPebbleStateEngine(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		stateEngines[index] = stateEngine
		t.Cleanup(func() { _ = stateEngine.Close() })
		expert := dbconfig.GetDefaultExpertConfig()
		expert.LogDB = dbconfig.GetTinyMemLogDBConfig()
		expert.Engine.SnapshotShards = 2
		events[index] = &integrationSystemEvents{}
		config := dbconfig.NodeHostConfig{
			DeploymentID: 0x46, NodeHostDir: t.TempDir(), RTTMillisecond: 2,
			RaftAddress: address, Expert: expert, SystemEventListener: events[index],
		}
		nodeHost, err := dragonboat.NewNodeHost(config)
		if err != nil {
			t.Fatal(err)
		}
		nodeHosts[index] = nodeHost
		t.Cleanup(nodeHost.Close)
	}

	registryLayout := testRegistryLayout(2, "generation-integration")
	for index := 0; index < 3; index++ {
		registryLayout.Members[index].RaftEndpoint = addresses[index]
	}
	if err := registryLayout.Validate(); err != nil {
		t.Fatal(err)
	}
	initial := map[uint64]dragonboat.Target{1: addresses[0], 2: addresses[1], 3: addresses[2]}
	for index := 0; index < 3; index++ {
		if err := nodeHosts[index].StartOnDiskReplica(initial, false, stateEngines[index].NewStateMachine,
			integrationRaftConfig(SystemRaftShardID, uint64(index+1), false)); err != nil {
			t.Fatal(err)
		}
	}
	digest, _ := registryLayout.Digest()
	bootstrapRaw, err := EncodeSystemCommand(SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
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
			return state.Initialized && state.ActiveRegistryLayoutDigest == digest
		})
	}
	setGatesRaw, err := EncodeSystemCommand(SystemCommand{
		Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	setGatesResult := proposeEventually(t, nodeHosts[:3], SystemRaftShardID, setGatesRaw)
	var gatesResult SystemApplyResult
	if err := json.Unmarshal(setGatesResult.Data, &gatesResult); err != nil || !gatesResult.Applied {
		t.Fatalf("System gate result = %+v, %v", gatesResult, err)
	}

	identity := routeShardIdentity(t, registryLayout, "/integration", "ready")
	dataRaftShardID := DataRaftShardID(identity.ShardID)
	for index := 0; index < 3; index++ {
		if err := nodeHosts[index].StartOnDiskReplica(initial, false, stateEngines[index].NewStateMachine,
			integrationRaftConfig(dataRaftShardID, uint64(index+1), false)); err != nil {
			t.Fatal(err)
		}
	}
	bootstrap, err := NewDataShardBootstrap(registryLayout, identity.ShardID)
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

	starting := routeStarting(t, registryLayout, "/integration", "ready", "sandbox-integration", 1, true)
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
		return nodeHosts[0].StartOnDiskReplica(nil, false, stateEngines[0].NewStateMachine,
			integrationRaftConfig(dataRaftShardID, 1, false))
	})
	waitForDataLookup(t, nodeHosts[0], dataRaftShardID, query, routeapi.ReadReady)

	membership := getMembershipEventually(t, nodeHosts[:3], SystemRaftShardID)
	requester := leaderNodeHostEventually(t, nodeHosts[:3], SystemRaftShardID)
	changeContext, cancelChange := context.WithTimeout(context.Background(), 10*time.Second)
	if err := requester.SyncRequestAddNonVoting(
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
		if target, found := membership.NonVotings[4]; !found || target != addresses[3] {
			return errors.New("replica 4 is not a committed non-voting member")
		}
		return nil
	})

	requester = leaderNodeHostEventually(t, nodeHosts[:3], SystemRaftShardID)
	snapshotContext, cancelSystemSnapshot := context.WithTimeout(context.Background(), 10*time.Second)
	snapshotIndex, err := requester.SyncRequestSnapshot(snapshotContext, SystemRaftShardID, dragonboat.SnapshotOption{
		OverrideCompactionOverhead: true,
	})
	cancelSystemSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	integrationEventually(t, func() error {
		reader, err := requester.GetLogReader(SystemRaftShardID)
		if err != nil {
			return err
		}
		first, _ := reader.GetRange()
		if reader.Snapshot().Index < snapshotIndex || first <= 1 {
			return fmt.Errorf("leader log has not compacted through snapshot %d: first=%d snapshot=%d",
				snapshotIndex, first, reader.Snapshot().Index)
		}
		return nil
	})
	if err := nodeHosts[3].StartOnDiskReplica(nil, true, stateEngines[3].NewStateMachine,
		integrationRaftConfig(SystemRaftShardID, 4, true)); err != nil {
		t.Fatal(err)
	}
	integrationEventually(t, func() error {
		if !events[3].installedSnapshot(SystemRaftShardID, 4, snapshotIndex) {
			return errors.New("replica 4 has not received the compacted System snapshot")
		}
		return nil
	})
	waitForSystemState(t, nodeHosts[3], func(state SystemState) bool {
		return state.Initialized && state.ActiveRegistryLayoutDigest == digest && state.ServeGate && state.WriteGate
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
	if err := nodeHosts[3].StopReplica(SystemRaftShardID, 4); err != nil {
		t.Fatal(err)
	}
	integrationEventually(t, func() error {
		return nodeHosts[3].StartOnDiskReplica(nil, false, stateEngines[3].NewStateMachine,
			integrationRaftConfig(SystemRaftShardID, 4, true))
	})
	integrationEventually(t, func() error {
		info := nodeHosts[3].GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
		for _, shard := range info.ShardInfoList {
			if shard.ShardID == SystemRaftShardID && shard.ReplicaID == 4 {
				if shard.IsNonVoting {
					return errors.New("promoted replica restarted with stale non-voting role")
				}
				return nil
			}
		}
		return errors.New("restarted promoted replica is unavailable")
	})
	changeContext, cancelChange = context.WithTimeout(context.Background(), 10*time.Second)
	// A removed leader may close before returning the committed result, so the
	// membership read below resolves the ambiguous request outcome.
	_ = requester.SyncRequestDeleteReplica(
		changeContext, SystemRaftShardID, 3, membership.ConfigChangeID,
	)
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
	if err := nodeHosts[2].StopReplica(SystemRaftShardID, 3); err != nil &&
		!errors.Is(err, dragonboat.ErrShardNotFound) {
		t.Fatal(err)
	}
	integrationEventually(t, func() error {
		info := nodeHosts[2].GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
		for _, shard := range info.ShardInfoList {
			if shard.ShardID == SystemRaftShardID && shard.ReplicaID == 3 {
				return errors.New("removed replica has not stopped")
			}
		}
		return nil
	})
}

func leaderNodeHostEventually(
	t *testing.T,
	nodeHosts []*dragonboat.NodeHost,
	shardID uint64,
) *dragonboat.NodeHost {
	t.Helper()
	var leader *dragonboat.NodeHost
	integrationEventually(t, func() error {
		for _, nodeHost := range nodeHosts {
			leaderID, term, valid, err := nodeHost.GetLeaderID(shardID)
			if err != nil || !valid || leaderID == 0 || term == 0 {
				continue
			}
			for _, candidate := range nodeHosts {
				info := candidate.GetNodeHostInfo(dragonboat.DefaultNodeHostInfoOption)
				for _, shard := range info.ShardInfoList {
					if shard.ShardID == shardID && shard.ReplicaID == leaderID {
						leader = candidate
						return nil
					}
				}
			}
		}
		return errors.New("leader unavailable")
	})
	return leader
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
