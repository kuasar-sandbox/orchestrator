package controlplane

import (
	"context"
	"errors"
	"strings"
	"sync"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

type memoryConsensus struct {
	mu             sync.Mutex
	registryLayout raftstore.RegistryLayout
	digest         string
	identity       raftstore.PermitIdentity
	states         map[uint32]*raftstore.DataState
	nextIndex      map[uint32]uint64
	failFenceOnce  bool
}

func newMemoryConsensus(registryLayout raftstore.RegistryLayout) (*memoryConsensus, error) {
	digest, err := registryLayout.Digest()
	if err != nil {
		return nil, err
	}
	return &memoryConsensus{
		registryLayout: registryLayout, digest: digest,
		identity: raftstore.PermitIdentity{
			ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
			SystemEpoch: 1, RegistryLayoutDigest: digest,
		},
		states: make(map[uint32]*raftstore.DataState), nextIndex: make(map[uint32]uint64),
	}, nil
}

func (m *memoryConsensus) RefreshPermit(context.Context) (raftstore.PermitGrant, error) {
	return raftstore.PermitGrant{
		PermitIdentity: m.identity, CommitIndex: 1,
		MaxLifetimeMillis: m.registryLayout.ServePermitMaxMillis,
		ServeGate:         true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, nil
}

func (m *memoryConsensus) AuthorizeServe(identity raftstore.PermitIdentity, _ raftstore.PermitOperation) error {
	if identity != m.identity {
		return raftstore.ErrPermitMismatch
	}
	return nil
}

func (m *memoryConsensus) ReadSystemStrong(context.Context) (raftstore.SystemState, error) {
	return raftstore.SystemState{}, errors.New("memory consensus: system reads are not configured")
}

func (m *memoryConsensus) ApplySystem(context.Context, raftstore.SystemCommand) (raftstore.SystemApplyResult, error) {
	return raftstore.SystemApplyResult{}, errors.New("memory consensus: system mutations are not configured")
}

func (m *memoryConsensus) ApplyData(_ context.Context, command raftstore.DataCommand) (raftstore.DataApplyResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if command.Type == raftstore.DataPutFence && m.failFenceOnce {
		m.failFenceOnce = false
		return raftstore.DataApplyResult{}, errors.New("injected fence commit failure")
	}
	state, err := m.stateLocked(command.Identity)
	if err != nil {
		return raftstore.DataApplyResult{}, err
	}
	index := m.nextIndex[command.Identity.ShardID]
	m.nextIndex[command.Identity.ShardID]++
	return raftstore.ApplyDataCommand(state, index, command), nil
}

func (m *memoryConsensus) ReadData(_ context.Context, query raftstore.DataLookup) (raftstore.DataLookupResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	identity, err := lookupIdentity(query)
	if err != nil {
		return raftstore.DataLookupResult{}, err
	}
	state, err := m.stateLocked(identity)
	if err != nil {
		return raftstore.DataLookupResult{}, err
	}
	return raftstore.LookupData(*state, query)
}

func (m *memoryConsensus) CompactExecutionFence(
	context.Context,
	raftstore.ShardRequestIdentity,
	string,
	string,
	string,
) error {
	return errors.New("memory consensus: fence compaction is not configured")
}

func (m *memoryConsensus) stateLocked(identity raftstore.ShardRequestIdentity) (*raftstore.DataState, error) {
	if identity.PermitIdentity != m.identity {
		return nil, raftstore.ErrPermitMismatch
	}
	if state := m.states[identity.ShardID]; state != nil {
		return state, nil
	}
	bootstrap, err := raftstore.NewDataShardBootstrap(m.registryLayout, identity.ShardID)
	if err != nil {
		return nil, err
	}
	state := &raftstore.DataState{}
	result := raftstore.ApplyDataCommand(state, 1, raftstore.DataCommand{
		Type: raftstore.DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: []uint64{1, 2, 3},
	})
	if !result.Applied || result.Conflict {
		return nil, errors.New("memory consensus: initialize data shard")
	}
	m.states[identity.ShardID] = state
	m.nextIndex[identity.ShardID] = 2
	return state, nil
}

func (m *memoryConsensus) fenceCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := 0
	for _, state := range m.states {
		total += len(state.Fences)
	}
	return total
}

func lookupIdentity(query raftstore.DataLookup) (raftstore.ShardRequestIdentity, error) {
	switch {
	case query.Workflow != nil:
		return query.Workflow.Identity, nil
	case query.Fence != nil:
		return query.Fence.Identity, nil
	case query.Pending != nil:
		return query.Pending.Identity, nil
	case query.RouteBucket != nil:
		return query.RouteBucket.Identity, nil
	case query.Changefeed != nil:
		return query.Changefeed.Identity, nil
	case query.Route != nil:
		identity := query.Route.RequestIdentity
		return raftstore.ShardRequestIdentity{PermitIdentity: raftstore.PermitIdentity{
			ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
			SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
		}, ShardID: identity.ShardID}, nil
	case query.Build != nil:
		identity := query.Build.RequestIdentity
		return raftstore.ShardRequestIdentity{PermitIdentity: raftstore.PermitIdentity{
			ClusterID: identity.ClusterID, RegistryGeneration: identity.RegistryGeneration,
			SystemEpoch: identity.SystemEpoch, RegistryLayoutDigest: identity.RegistryLayoutDigest,
		}, ShardID: identity.ShardID}, nil
	default:
		return raftstore.ShardRequestIdentity{}, errors.New("memory consensus: unsupported lookup")
	}
}

func testControlRegistryLayout() raftstore.RegistryLayout {
	members := []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a:9443", RaftEndpoint: "registry-a:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b:9443", RaftEndpoint: "registry-b:63001"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c:9443", RaftEndpoint: "registry-c:63001"},
	}
	replicas := []raftstore.ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-c", ReplicaID: 3},
	}
	const shards = uint32(16)
	placements := make([]raftstore.ShardPlacement, shards)
	for shardID := uint32(0); shardID < shards; shardID++ {
		placements[shardID] = raftstore.ShardPlacement{
			ShardID: shardID, Replicas: append([]raftstore.ReplicaPlacement(nil), replicas...),
		}
	}
	return raftstore.RegistryLayout{
		FormatVersion: raftstore.RegistryLayoutFormatV1, ClusterID: "cluster-1", RegistryGeneration: "generation-1",
		RegistryLayoutVersion: 1, SchemaVersion: 1, ProtocolVersion: 1, HashVersion: "ShardHashV1",
		VirtualShardCount: shards, RouteBucketCount: 16, BuildBucketCount: 16,
		ReplicationFactor: 3, ServePermitMaxMillis: 5000,
		BootstrapTokenDigest: strings.Repeat("a", 64), Members: members,
		SystemReplicas: replicas, DataShards: placements,
	}
}
