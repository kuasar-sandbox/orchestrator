package controlplane

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

type recoveryResolutionConsensus struct {
	*memoryConsensus
	systemMu    sync.Mutex
	system      raftstore.SystemState
	systemIndex uint64
	operations  []string
}

func (r *recoveryResolutionConsensus) LocalSystemLeader() (bool, error) { return true, nil }

func (r *recoveryResolutionConsensus) ReadSystemStrong(context.Context) (raftstore.SystemState, error) {
	r.systemMu.Lock()
	defer r.systemMu.Unlock()
	return r.system, nil
}

func (r *recoveryResolutionConsensus) ApplySystem(
	_ context.Context,
	command raftstore.SystemCommand,
) (raftstore.SystemApplyResult, error) {
	r.systemMu.Lock()
	defer r.systemMu.Unlock()
	if command.Type != raftstore.SystemUpdateRecoveryNode || command.RecoveryNode == nil || r.system.Recovery == nil {
		return raftstore.SystemApplyResult{}, errors.New("unexpected System command")
	}
	update := *command.RecoveryNode
	current, found := r.system.Recovery.Nodes[update.NodeID]
	if !found || current.State != update.From || current.EnrollmentID != update.EnrollmentID ||
		current.NodeEpoch != update.NodeEpoch {
		return raftstore.SystemApplyResult{Conflict: true, Reason: "recovery progress changed"}, nil
	}
	r.systemIndex++
	recovery := *r.system.Recovery
	recovery.Nodes = make(map[string]raftstore.RecoveryNodeProgress, len(r.system.Recovery.Nodes))
	for nodeID, progress := range r.system.Recovery.Nodes {
		recovery.Nodes[nodeID] = progress
	}
	current.State = update.To
	current.SessionSeq = update.SessionSeq
	current.ReportDigest = update.ReportDigest
	current.ReportedObjects = update.ReportedObjects
	current.ResolvedObjects = update.ResolvedObjects
	current.ConflictObjects = update.ConflictObjects
	current.ResolutionProofDigest = update.ResolutionProofDigest
	current.ResolutionReason = update.ResolutionReason
	current.LastAppliedIndex = r.systemIndex
	recovery.Nodes[update.NodeID] = current
	r.system.Recovery = &recovery
	r.operations = append(r.operations, "system:"+string(update.To))
	return raftstore.SystemApplyResult{Applied: true}, nil
}

func (r *recoveryResolutionConsensus) ApplyRecoveryData(
	_ context.Context,
	command raftstore.DataCommand,
) (raftstore.DataApplyResult, error) {
	r.memoryConsensus.mu.Lock()
	defer r.memoryConsensus.mu.Unlock()
	state, err := r.memoryConsensus.stateLocked(raftstore.ShardRequestIdentity{
		PermitIdentity: r.memoryConsensus.identity,
		ShardID:        command.Identity.ShardID,
	})
	if err != nil {
		return raftstore.DataApplyResult{}, err
	}
	index := r.memoryConsensus.nextIndex[command.Identity.ShardID]
	r.memoryConsensus.nextIndex[command.Identity.ShardID]++
	result := raftstore.ApplyDataCommand(state, index, command)
	if result.Applied && (command.Type == raftstore.DataResetRecoveryNode || command.Type == raftstore.DataQuarantineRecovery) {
		r.systemMu.Lock()
		r.operations = append(r.operations, fmt.Sprintf("data:%s:%d", command.Type, command.Identity.ShardID))
		r.systemMu.Unlock()
	}
	return result, nil
}

func (r *recoveryResolutionConsensus) ReadRecoveryData(
	_ context.Context,
	query raftstore.RecoveryLookup,
) (raftstore.RecoveryLookupResult, error) {
	r.memoryConsensus.mu.Lock()
	defer r.memoryConsensus.mu.Unlock()
	state := r.memoryConsensus.states[query.Identity.ShardID]
	if state == nil {
		return raftstore.RecoveryLookupResult{}, errors.New("missing recovery shard")
	}
	result, err := raftstore.LookupData(*state, raftstore.DataLookup{Recovery: &query})
	if err != nil || result.Recovery == nil {
		return raftstore.RecoveryLookupResult{}, errors.Join(err, errors.New("missing recovery lookup result"))
	}
	return *result.Recovery, nil
}

func TestOperatorMissingResetsDataBeforeSystemTerminalState(t *testing.T) {
	progress := raftstore.RecoveryNodeProgress{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7,
		SessionSeq: 11, State: raftstore.RecoveryNodeCollecting, LastAppliedIndex: 1,
	}
	coordinator, runtime, _, identity := newRecoveryResolutionFixture(t, progress)
	runtime.insertRecoveryRecord(0, testRecoveryResolutionRecord(progress, "sandbox-partial", "report-partial"))
	request := ResolveRecoveryNodeRequest{
		RegistryServeIdentity: identity, NodeID: progress.NodeID,
		Resolution:  string(raftstore.RecoveryNodeMissing),
		ProofDigest: strings.Repeat("d", 64), Reason: "node is externally fenced and unavailable",
	}
	if err := coordinator.ResolveRecoveryNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	for shardID := uint32(0); shardID < runtime.registryLayout.VirtualShardCount; shardID++ {
		state := runtime.dataState(shardID)
		if len(state.RecoveryRecords) != 0 ||
			state.Recovery.TerminalResetSessions[testRecoveryNodeIdentityKey(progress.NodeID, progress.NodeEpoch)] != progress.SessionSeq {
			t.Fatalf("shard %d was not terminally reset: %+v", shardID, state.Recovery)
		}
	}
	state, _ := runtime.ReadSystemStrong(context.Background())
	resolved := state.Recovery.Nodes[progress.NodeID]
	if resolved.State != raftstore.RecoveryNodeMissing || resolved.ResolutionProofDigest != request.ProofDigest {
		t.Fatalf("System recovery progress = %+v", resolved)
	}
	assertRecoverySystemOperationLast(t, runtime.operations, "system:MISSING", int(runtime.registryLayout.VirtualShardCount))
	if err := coordinator.ResolveRecoveryNode(context.Background(), request); err != nil {
		t.Fatalf("exact MISSING retry: %v", err)
	}
}

func TestOperatorQuarantineCommitsEveryRecordBeforeSystemState(t *testing.T) {
	progress := raftstore.RecoveryNodeProgress{
		NodeID: "node-1", EnrollmentID: "enrollment-1", NodeEpoch: 7,
		SessionSeq: 11, State: raftstore.RecoveryNodeReported,
		ReportDigest: strings.Repeat("a", 64), ReportedObjects: 1, LastAppliedIndex: 1,
	}
	coordinator, runtime, _, identity := newRecoveryResolutionFixture(t, progress)
	record := testRecoveryResolutionRecord(progress, "sandbox-reported", "")
	record.ReportDigest = progress.ReportDigest
	runtime.insertRecoveryRecord(0, record)
	request := ResolveRecoveryNodeRequest{
		RegistryServeIdentity: identity, NodeID: progress.NodeID,
		Resolution:  string(raftstore.RecoveryNodeQuarantined),
		ProofDigest: strings.Repeat("d", 64), Reason: "operator rejected this recovery report",
	}
	if err := coordinator.ResolveRecoveryNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stored := runtime.dataState(0).RecoveryRecords[testRecoveryRecordKey(record)]
	if stored.State != raftstore.RecoveryObjectQuarantined || stored.QuarantineReason != request.Reason {
		t.Fatalf("quarantined recovery object = %+v", stored)
	}
	state, _ := runtime.ReadSystemStrong(context.Background())
	resolved := state.Recovery.Nodes[progress.NodeID]
	if resolved.State != raftstore.RecoveryNodeQuarantined || resolved.ConflictObjects != 1 ||
		resolved.ResolutionProofDigest != request.ProofDigest {
		t.Fatalf("System recovery progress = %+v", resolved)
	}
	assertRecoverySystemOperationLast(t, runtime.operations, "system:QUARANTINED", 1)
	if err := coordinator.ResolveRecoveryNode(context.Background(), request); err != nil {
		t.Fatalf("exact QUARANTINED retry: %v", err)
	}
}

func newRecoveryResolutionFixture(
	t *testing.T,
	progress raftstore.RecoveryNodeProgress,
) (*RecoveryCoordinator, *recoveryResolutionConsensus, raftstore.RecoveryEpoch, routeapi.RegistryServeIdentity) {
	t.Helper()
	target := testControlRegistryLayout()
	source := raftstore.CloneRegistryLayout(target)
	source.RegistryGeneration = "generation-source"
	sourceDigest, err := source.Digest()
	if err != nil {
		t.Fatal(err)
	}
	targetDigest, err := target.Digest()
	if err != nil {
		t.Fatal(err)
	}
	memory, err := newMemoryConsensus(target)
	if err != nil {
		t.Fatal(err)
	}
	recovery := raftstore.RecoveryEpoch{
		Epoch: 2, SourceClusterID: target.ClusterID,
		SourceRegistryGeneration: source.RegistryGeneration, SourceRegistryLayoutDigest: sourceDigest,
		TargetRegistryGeneration: target.RegistryGeneration, TargetRegistryLayoutDigest: targetDigest,
		Phase: raftstore.RecoveryCollecting, Nodes: map[string]raftstore.RecoveryNodeProgress{progress.NodeID: progress},
		PermitDrainComplete: true, PermitDrainIndex: 1,
	}
	runtime := &recoveryResolutionConsensus{
		memoryConsensus: memory, systemIndex: 1,
		system: raftstore.SystemState{
			Initialized: true, ClusterID: target.ClusterID, RegistryGeneration: target.RegistryGeneration,
			SystemEpoch: 2, ActiveRegistryLayoutDigest: targetDigest, Recovery: &recovery,
		},
	}
	store, err := NewRaftStore(runtime, target, targetDigest)
	if err != nil {
		t.Fatal(err)
	}
	dataRecovery := raftstore.DataRecoveryState{
		RecoveryEpoch: recovery.Epoch, SourceClusterID: recovery.SourceClusterID,
		SourceRegistryGeneration:   recovery.SourceRegistryGeneration,
		SourceRegistryLayoutDigest: recovery.SourceRegistryLayoutDigest,
		Target: raftstore.PermitIdentity{
			ClusterID: target.ClusterID, RegistryGeneration: target.RegistryGeneration,
			SystemEpoch: recovery.Epoch, RegistryLayoutDigest: targetDigest,
		},
	}
	for shardID := uint32(0); shardID < target.VirtualShardCount; shardID++ {
		result, err := store.ApplyRecoveryData(context.Background(), raftstore.DataCommand{
			Type: raftstore.DataBeginRecovery,
			Identity: raftstore.ShardRequestIdentity{
				PermitIdentity: dataRecovery.Target, ShardID: shardID,
			},
			RecoveryStart: &dataRecovery,
		})
		if err != nil || !result.Applied || result.Conflict {
			t.Fatalf("begin recovery shard %d: %+v, %v", shardID, result, err)
		}
	}
	peers := make([]SessionPeer, 0, len(target.Members)-1)
	for _, member := range target.Members {
		if member.MemberID != "registry-a" {
			peers = append(peers, SessionPeer{
				MemberID: member.MemberID, Endpoint: member.InternalEndpoint, Client: &http.Client{},
			})
		}
	}
	mesh, err := NewRecoveryMesh("registry-a", store, peers)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewRecoveryCoordinator(
		store, mesh, session.NewDirectory(allowTestDirectoryEntries{}), recoveryConfigSender{},
		RecoveryCoordinatorConfig{Workers: 4, PerNodeWorkers: 1, LookupPage: 32}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := routeapi.RegistryServeIdentity{
		ClusterID: target.ClusterID, RegistryGeneration: target.RegistryGeneration,
		SystemEpoch: recovery.Epoch, RegistryLayoutDigest: targetDigest,
	}
	return coordinator, runtime, recovery, identity
}

func testRecoveryResolutionRecord(
	progress raftstore.RecoveryNodeProgress,
	objectID string,
	report string,
) raftstore.RecoveryObjectRecord {
	reportDigest := strings.Repeat("a", 64)
	if report != "" {
		reportDigest = fmt.Sprintf("%064x", len(report))
	}
	return raftstore.RecoveryObjectRecord{
		RecoveryEpoch: 2, Kind: clusterstate.ExecutionKindSandbox,
		Group: "/group", RouteKey: "route-1", ObjectID: objectID,
		NodeID: progress.NodeID, NodeEpoch: progress.NodeEpoch, SessionSeq: progress.SessionSeq,
		ReportDigest: reportDigest, SourceBindingDigest: strings.Repeat("b", 64),
		TargetBinding: clusterstate.ExecutionBindingIntent{BindingDigest: strings.Repeat("c", 64)},
		State:         raftstore.RecoveryObjectStaged,
	}
}

func (r *recoveryResolutionConsensus) insertRecoveryRecord(shardID uint32, record raftstore.RecoveryObjectRecord) {
	r.memoryConsensus.mu.Lock()
	defer r.memoryConsensus.mu.Unlock()
	r.memoryConsensus.states[shardID].RecoveryRecords[testRecoveryRecordKey(record)] = record
}

func (r *recoveryResolutionConsensus) dataState(shardID uint32) raftstore.DataState {
	r.memoryConsensus.mu.Lock()
	defer r.memoryConsensus.mu.Unlock()
	return *r.memoryConsensus.states[shardID]
}

func testRecoveryRecordKey(record raftstore.RecoveryObjectRecord) string {
	return testLengthKey(fmt.Sprint(uint8(record.Kind)), record.Group, record.RouteKey, record.ObjectID)
}

func testRecoveryNodeIdentityKey(nodeID string, nodeEpoch uint64) string {
	return nodeID + "\x00" + fmt.Sprint(nodeEpoch)
}

func testLengthKey(fields ...string) string {
	var key []byte
	for _, field := range fields {
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(field)))
		key = append(key, length[:]...)
		key = append(key, field...)
	}
	return string(key)
}

func assertRecoverySystemOperationLast(t *testing.T, operations []string, want string, minimumData int) {
	t.Helper()
	if len(operations) < minimumData+1 || operations[len(operations)-1] != want {
		t.Fatalf("recovery operation order = %v", operations)
	}
	for _, operation := range operations[:len(operations)-1] {
		if !strings.HasPrefix(operation, "data:") {
			t.Fatalf("System terminal state preceded data resolution: %v", operations)
		}
	}
}
