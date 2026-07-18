package raftstore

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dbconfig "github.com/lni/dragonboat/v4/config"
	sm "github.com/lni/dragonboat/v4/statemachine"
	"golang.org/x/sync/errgroup"
)

var (
	ErrBootstrapUnauthorized = errors.New("raftstore: explicit bootstrap authorization is required")
	ErrReplicaHistoryLost    = errors.New("raftstore: enrolled Raft replica history is missing")
	ErrReplicaStartAmbiguous = errors.New("raftstore: Raft replica start outcome is ambiguous")
	ErrNoLocalReplica        = errors.New("raftstore: no local replica is enrolled for the shard")
)

type RuntimeOpenMode string

const (
	RuntimeBootstrap RuntimeOpenMode = "BOOTSTRAP"
	RuntimeJoin      RuntimeOpenMode = "JOIN"
	RuntimeRestart   RuntimeOpenMode = "RESTART"
)

type RuntimeOpenOptions struct {
	Mode            RuntimeOpenMode
	BootstrapSecret []byte
}

type raftNodeHost interface {
	StartReplica(map[uint64]dragonboat.Target, bool, sm.CreateStateMachineFunc, dbconfig.Config) error
	HasNodeInfo(uint64, uint64) bool
	SyncPropose(context.Context, *client.Session, []byte) (sm.Result, error)
	SyncRead(context.Context, uint64, any) (any, error)
	StaleRead(uint64, any) (any, error)
	GetNoOPSession(uint64) *client.Session
	SyncGetShardMembership(context.Context, uint64) (*dragonboat.Membership, error)
	SyncRequestAddNonVoting(context.Context, uint64, uint64, string, uint64) error
	SyncRequestAddReplica(context.Context, uint64, uint64, string, uint64) error
	SyncRequestDeleteReplica(context.Context, uint64, uint64, uint64) error
	StopReplica(uint64, uint64) error
	SyncRemoveData(context.Context, uint64, uint64) error
	GetLeaderID(uint64) (uint64, uint64, bool, error)
	Close()
}

type nodeHostFactory func(dbconfig.NodeHostConfig) (raftNodeHost, error)

type Runtime struct {
	mu              sync.Mutex
	config          RuntimeConfig
	manifest        Manifest
	manifestDigest  string
	member          RegistryMember
	enrollment      LocalEnrollment
	enrollmentStore EnrollmentStore
	nodeHost        raftNodeHost
	permitCache     *PermitCache
}

func OpenRuntime(
	config RuntimeConfig,
	manifestChain []SignedManifest,
	keyring map[string]ed25519.PublicKey,
	options RuntimeOpenOptions,
) (*Runtime, error) {
	return openRuntime(config, manifestChain, keyring, options, func(config dbconfig.NodeHostConfig) (raftNodeHost, error) {
		return dragonboat.NewNodeHost(config)
	})
}

func openRuntime(
	config RuntimeConfig,
	manifestChain []SignedManifest,
	keyring map[string]ed25519.PublicKey,
	options RuntimeOpenOptions,
	factory nodeHostFactory,
) (*Runtime, error) {
	if len(manifestChain) == 0 || factory == nil {
		return nil, errors.New("raftstore: a signed manifest chain and NodeHost factory are required")
	}
	currentSigned := manifestChain[len(manifestChain)-1]
	currentDigest, err := currentSigned.Verify(keyring)
	if err != nil {
		return nil, err
	}
	manifest := currentSigned.Manifest
	member, found := manifestMember(manifest, config.MemberID)
	if !found {
		return nil, errors.New("raftstore: local member is absent from the signed manifest")
	}
	nodeHostConfig, err := config.dragonboatConfig(manifest, member)
	if err != nil {
		return nil, err
	}
	guard := ManifestGuard{Path: config.ManifestGuardPath}
	accepted, err := guard.AcceptSignedChain(manifestChain, keyring)
	if err != nil {
		return nil, err
	}
	if accepted.ManifestVersion != manifest.ManifestVersion || accepted.ManifestDigest != currentDigest ||
		accepted.StorageGeneration != manifest.StorageGeneration {
		return nil, errors.New("raftstore: supplied chain does not end at the accepted manifest")
	}

	store := EnrollmentStore{Path: config.EnrollmentPath}
	enrollment, err := store.Load()
	if err != nil {
		return nil, err
	}
	if enrollment == nil {
		if options.Mode != RuntimeBootstrap && options.Mode != RuntimeJoin {
			return nil, ErrBootstrapUnauthorized
		}
		if err := requireEmptyRuntimeStorage(config); err != nil {
			return nil, err
		}
		mode := EnrollmentBootstrap
		if options.Mode == RuntimeBootstrap {
			if manifest.ManifestVersion != 1 || !bootstrapSecretMatches(options.BootstrapSecret, manifest.BootstrapTokenDigest) {
				return nil, ErrBootstrapUnauthorized
			}
		} else {
			if manifest.ManifestVersion == 1 {
				return nil, errors.New("raftstore: a first manifest member must use explicit generation bootstrap")
			}
			mode = EnrollmentJoin
		}
		created, createErr := newLocalEnrollment(mode, manifest, currentDigest, member, config)
		if createErr != nil {
			return nil, createErr
		}
		if err := store.Store(created); err != nil {
			return nil, err
		}
		enrollment = &created
	} else {
		if options.Mode != RuntimeRestart {
			return nil, errors.New("raftstore: an enrolled member must restart; bootstrap/join cannot be replayed")
		}
		if err := enrollment.Matches(manifest, currentDigest, member, config); err != nil {
			return nil, err
		}
		enrollment.ManifestVersion = manifest.ManifestVersion
		enrollment.ManifestDigest = currentDigest
		if err := store.Store(*enrollment); err != nil {
			return nil, err
		}
	}
	if err := config.attestStorage(); err != nil {
		return nil, err
	}
	nodeHost, err := factory(nodeHostConfig)
	if err != nil {
		return nil, fmt.Errorf("raftstore: create Dragonboat NodeHost: %w", err)
	}
	return &Runtime{
		config: config, manifest: manifest, manifestDigest: currentDigest, member: member,
		enrollment: *enrollment, enrollmentStore: store, nodeHost: nodeHost,
		permitCache: NewPermitCache(time.Now),
	}, nil
}

func (r *Runtime) Close() {
	if r == nil || r.nodeHost == nil {
		return
	}
	r.permitCache.Clear()
	r.nodeHost.Close()
}

func (r *Runtime) StartSystemReplica() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	position := r.replicaPosition(SystemRaftShardID)
	if position < 0 {
		return ErrNoLocalReplica
	}
	return r.startReplica(position)
}

func (r *Runtime) StartDataReplicas(system SystemState) error {
	if err := r.authorizeManifestState(system); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for position := range r.enrollment.Replicas {
		if r.enrollment.Replicas[position].ShardID == SystemRaftShardID ||
			r.enrollment.Replicas[position].LocalState == ReplicaRemoved {
			continue
		}
		if err := r.startReplica(position); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) PlanManifestJoins(system SystemState) error {
	if err := r.authorizeManifestState(system); err != nil {
		return err
	}
	if system.Transition == nil || system.Transition.Digest != r.manifestDigest {
		return errors.New("raftstore: local joins require a committed manifest transition")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	desired := localReplicas(r.manifest, r.member, ReplicaJoin, true)
	next := r.enrollment
	next.Replicas = append([]LocalReplicaEnrollment(nil), r.enrollment.Replicas...)
	for _, planned := range desired {
		position := -1
		for index, current := range next.Replicas {
			if current.ShardID == planned.ShardID {
				position = index
				break
			}
		}
		if position < 0 {
			next.Replicas = append(next.Replicas, planned)
			continue
		}
		current := next.Replicas[position]
		if current.ReplicaID == planned.ReplicaID {
			continue
		}
		if current.LocalState != ReplicaRemoved {
			return fmt.Errorf("raftstore: shard %d already has live local replica %d", current.ShardID, current.ReplicaID)
		}
		next.Replicas[position] = planned
	}
	sort.Slice(next.Replicas, func(i, j int) bool { return next.Replicas[i].ShardID < next.Replicas[j].ShardID })
	if err := r.enrollmentStore.Store(next); err != nil {
		return err
	}
	r.enrollment = next
	return nil
}

func (r *Runtime) startReplica(position int) error {
	replica := r.enrollment.Replicas[position]
	if replica.LocalState == ReplicaRemoved {
		return ErrNoLocalReplica
	}
	hasHistory := r.nodeHost.HasNodeInfo(replica.ShardID, replica.ReplicaID)
	switch replica.LocalState {
	case ReplicaActive:
		if !hasHistory {
			return fmt.Errorf("%w: shard=%d replica=%d", ErrReplicaHistoryLost, replica.ShardID, replica.ReplicaID)
		}
		return r.launchReplica(replica, nil, false)
	case ReplicaStarting:
		if !hasHistory {
			return fmt.Errorf("%w: shard=%d replica=%d", ErrReplicaStartAmbiguous, replica.ShardID, replica.ReplicaID)
		}
		if err := r.setReplicaState(position, ReplicaActive); err != nil {
			return err
		}
		return r.launchReplica(r.enrollment.Replicas[position], nil, false)
	case ReplicaPlanned:
		if hasHistory {
			return fmt.Errorf("%w: unstarted shard=%d replica=%d already has history", ErrReplicaStartAmbiguous, replica.ShardID, replica.ReplicaID)
		}
		if err := r.setReplicaState(position, ReplicaStarting); err != nil {
			return err
		}
		replica = r.enrollment.Replicas[position]
		var initial map[uint64]dragonboat.Target
		join := replica.StartPlan == ReplicaJoin
		if !join {
			var err error
			initial, err = r.initialMembers(replica.ShardID)
			if err != nil {
				return err
			}
		}
		if err := r.launchReplica(replica, initial, join); err != nil {
			return err
		}
		if !r.nodeHost.HasNodeInfo(replica.ShardID, replica.ReplicaID) {
			return fmt.Errorf("%w: shard=%d replica=%d", ErrReplicaStartAmbiguous, replica.ShardID, replica.ReplicaID)
		}
		return r.setReplicaState(position, ReplicaActive)
	default:
		return errors.New("raftstore: invalid enrolled replica state")
	}
}

func (r *Runtime) launchReplica(replica LocalReplicaEnrollment, initial map[uint64]dragonboat.Target, join bool) error {
	factory := NewDataStateMachine
	if replica.ShardID == SystemRaftShardID {
		factory = NewSystemStateMachine
	}
	err := r.nodeHost.StartReplica(initial, join, factory,
		r.config.raftConfig(replica.ShardID, replica.ReplicaID, replica.NonVoting))
	if errors.Is(err, dragonboat.ErrShardAlreadyExist) {
		return nil
	}
	return err
}

func (r *Runtime) setReplicaState(position int, state ReplicaLocalState) error {
	next := r.enrollment
	next.Replicas = append([]LocalReplicaEnrollment(nil), r.enrollment.Replicas...)
	next.Replicas[position].LocalState = state
	if err := r.enrollmentStore.Store(next); err != nil {
		return err
	}
	r.enrollment = next
	return nil
}

func (r *Runtime) ReadSystemLocal() (SystemState, error) {
	value, err := r.nodeHost.StaleRead(SystemRaftShardID, SystemStateLookup{})
	if err != nil {
		return SystemState{}, err
	}
	state, ok := value.(SystemState)
	if !ok {
		return SystemState{}, errors.New("raftstore: Dragonboat returned an invalid System lookup type")
	}
	return state, nil
}

func (r *Runtime) ReadSystemStrong(ctx context.Context) (SystemState, error) {
	value, err := r.nodeHost.SyncRead(ctx, SystemRaftShardID, SystemStateLookup{})
	if err != nil {
		return SystemState{}, err
	}
	state, ok := value.(SystemState)
	if !ok {
		return SystemState{}, errors.New("raftstore: Dragonboat returned an invalid System lookup type")
	}
	return state, nil
}

func (r *Runtime) AwaitSystemManifest(ctx context.Context) (SystemState, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := r.ReadSystemLocal()
		if err == nil && state.Initialized {
			if err := r.authorizeManifestState(state); err != nil {
				return SystemState{}, err
			}
			return state, nil
		}
		select {
		case <-ctx.Done():
			return SystemState{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runtime) BootstrapSystem(ctx context.Context) (SystemState, error) {
	if r.enrollment.Mode != EnrollmentBootstrap || r.manifest.ManifestVersion != 1 {
		return SystemState{}, ErrBootstrapUnauthorized
	}
	if state, err := r.ReadSystemStrong(ctx); err == nil && state.Initialized {
		if err := r.authorizeManifestState(state); err != nil {
			return SystemState{}, err
		}
		return state, nil
	}
	_, err := r.proposeSystem(ctx, SystemCommand{
		Type: SystemBootstrap, Manifest: &r.manifest, Digest: r.manifestDigest,
	})
	if err != nil {
		state, readErr := r.ReadSystemStrong(ctx)
		if readErr != nil || r.authorizeManifestState(state) != nil {
			return SystemState{}, err
		}
		return state, nil
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeManifestState(state); err != nil {
		return SystemState{}, err
	}
	return state, nil
}

func (r *Runtime) InitializeDataShards(ctx context.Context, system SystemState, workers int) error {
	if r.enrollment.Mode != EnrollmentBootstrap || r.manifest.ManifestVersion != 1 {
		return ErrBootstrapUnauthorized
	}
	if err := r.authorizeManifestState(system); err != nil {
		return err
	}
	if system.ActiveManifestDigest != r.manifestDigest || system.SystemEpoch != 1 {
		return errors.New("raftstore: data shards require the initial committed System manifest")
	}
	if workers <= 0 || workers > 256 {
		return errors.New("raftstore: data-shard initialization concurrency must be between 1 and 256")
	}
	r.mu.Lock()
	replicas := append([]LocalReplicaEnrollment(nil), r.enrollment.Replicas...)
	r.mu.Unlock()
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(workers)
	for _, replica := range replicas {
		replica := replica
		if replica.ShardID == SystemRaftShardID || replica.LocalState == ReplicaRemoved {
			continue
		}
		if replica.LocalState != ReplicaActive {
			return fmt.Errorf("raftstore: data shard %d is not locally active", replica.ShardID)
		}
		group.Go(func() error { return r.initializeDataShard(groupCtx, replica) })
	}
	return group.Wait()
}

func (r *Runtime) initializeDataShard(ctx context.Context, replica LocalReplicaEnrollment) error {
	shardID, ok := LogicalShardID(replica.ShardID)
	if !ok {
		return errors.New("raftstore: invalid enrolled data shard")
	}
	if value, err := r.nodeHost.SyncRead(ctx, replica.ShardID, DataStateLookup{}); err == nil {
		state, valid := value.(DataState)
		if valid && state.Initialized {
			return r.validateDataState(state, shardID)
		}
	}
	bootstrap, err := dataShardBootstrap(r.manifest, r.manifestDigest, shardID)
	if err != nil {
		return err
	}
	identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: r.manifest.ClusterID, StorageGeneration: r.manifest.StorageGeneration,
		SystemEpoch: 1, ManifestDigest: r.manifestDigest,
	}, ShardID: shardID}
	result, err := r.proposeDataRaw(ctx, DataCommand{
		Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
		ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	if err != nil || result.Conflict {
		value, readErr := r.nodeHost.SyncRead(ctx, replica.ShardID, DataStateLookup{})
		if readErr != nil {
			if err != nil {
				return err
			}
			return errors.New(result.Reason)
		}
		state, valid := value.(DataState)
		if !valid {
			return errors.New("raftstore: Dragonboat returned an invalid data lookup type")
		}
		return r.validateDataState(state, shardID)
	}
	return nil
}

func (r *Runtime) RefreshPermit(ctx context.Context) (PermitGrant, error) {
	started := time.Now()
	result, err := r.proposeSystem(ctx, SystemCommand{Type: SystemRefreshPermit})
	if err != nil {
		return PermitGrant{}, err
	}
	if result.Conflict || result.PermitGrant == nil {
		return PermitGrant{}, errors.New(result.Reason)
	}
	if err := r.permitCache.Install(*result.PermitGrant, started); err != nil {
		return PermitGrant{}, err
	}
	return *result.PermitGrant, nil
}

func (r *Runtime) ApplyData(ctx context.Context, command DataCommand) (DataApplyResult, error) {
	if err := r.permitCache.Authorize(command.Identity.PermitIdentity, PermitRegistryWrite); err != nil {
		return DataApplyResult{}, err
	}
	return r.proposeDataRaw(ctx, command)
}

func (r *Runtime) proposeSystem(ctx context.Context, command SystemCommand) (SystemApplyResult, error) {
	raw, err := EncodeSystemCommand(command)
	if err != nil {
		return SystemApplyResult{}, err
	}
	result, err := r.nodeHost.SyncPropose(ctx, r.nodeHost.GetNoOPSession(SystemRaftShardID), raw)
	if err != nil {
		return SystemApplyResult{}, err
	}
	var applied SystemApplyResult
	if err := json.Unmarshal(result.Data, &applied); err != nil {
		return SystemApplyResult{}, fmt.Errorf("raftstore: decode System apply result: %w", err)
	}
	return applied, nil
}

func (r *Runtime) proposeDataRaw(ctx context.Context, command DataCommand) (DataApplyResult, error) {
	raw, err := EncodeDataCommand(command)
	if err != nil {
		return DataApplyResult{}, err
	}
	shardID := DataRaftShardID(command.Identity.ShardID)
	result, err := r.nodeHost.SyncPropose(ctx, r.nodeHost.GetNoOPSession(shardID), raw)
	if err != nil {
		return DataApplyResult{}, err
	}
	var applied DataApplyResult
	if err := json.Unmarshal(result.Data, &applied); err != nil {
		return DataApplyResult{}, fmt.Errorf("raftstore: decode data apply result: %w", err)
	}
	return applied, nil
}

func (r *Runtime) authorizeManifestState(system SystemState) error {
	if err := system.Validate(); err != nil {
		return err
	}
	if system.ClusterID != r.manifest.ClusterID || system.StorageGeneration != r.manifest.StorageGeneration ||
		system.SchemaVersion != r.manifest.SchemaVersion || system.ProtocolVersion != r.manifest.ProtocolVersion ||
		system.VirtualShardCount != r.manifest.VirtualShardCount {
		return errors.New("raftstore: System state differs from the signed generation")
	}
	if system.ActiveManifestDigest == r.manifestDigest && system.ActiveManifestVersion == r.manifest.ManifestVersion {
		return nil
	}
	if system.Transition != nil && system.Transition.Digest == r.manifestDigest &&
		system.Transition.Version == r.manifest.ManifestVersion {
		return nil
	}
	return errors.New("raftstore: signed manifest is not committed active or next System state")
}

func (r *Runtime) validateDataState(state DataState, shardID uint32) error {
	if err := state.Validate(); err != nil {
		return err
	}
	if state.ClusterID != r.manifest.ClusterID || state.StorageGeneration != r.manifest.StorageGeneration ||
		state.ShardID != shardID || state.SchemaVersion != r.manifest.SchemaVersion ||
		state.ProtocolVersion != r.manifest.ProtocolVersion || state.HashVersion != r.manifest.HashVersion ||
		state.VirtualShardCount != r.manifest.VirtualShardCount ||
		state.RouteBucketCount != r.manifest.RouteBucketCount || state.BuildBucketCount != r.manifest.BuildBucketCount {
		return errors.New("raftstore: data state differs from its signed manifest")
	}
	return nil
}

func (r *Runtime) initialMembers(shardID uint64) (map[uint64]dragonboat.Target, error) {
	placements, err := r.placementForRaftShard(shardID)
	if err != nil {
		return nil, err
	}
	members := make(map[uint64]dragonboat.Target, len(placements))
	for _, placement := range placements {
		member, found := manifestMember(r.manifest, placement.MemberID)
		if !found {
			return nil, errors.New("raftstore: replica placement names an unknown member")
		}
		members[placement.ReplicaID] = member.RaftEndpoint
	}
	return members, nil
}

func (r *Runtime) placementForRaftShard(shardID uint64) ([]ReplicaPlacement, error) {
	if shardID == SystemRaftShardID {
		return r.manifest.SystemReplicas, nil
	}
	logical, ok := LogicalShardID(shardID)
	if !ok || logical >= uint32(len(r.manifest.DataShards)) {
		return nil, errors.New("raftstore: unknown Raft shard")
	}
	return r.manifest.DataShards[logical].Replicas, nil
}

func (r *Runtime) replicaPosition(shardID uint64) int {
	for position, replica := range r.enrollment.Replicas {
		if replica.ShardID == shardID {
			return position
		}
	}
	return -1
}

func manifestMember(manifest Manifest, memberID string) (RegistryMember, bool) {
	for _, member := range manifest.Members {
		if member.MemberID == memberID {
			return member, true
		}
	}
	return RegistryMember{}, false
}

func bootstrapSecretMatches(secret []byte, digest string) bool {
	if len(secret) == 0 {
		return false
	}
	want, err := hex.DecodeString(digest)
	if err != nil || len(want) != sha256.Size {
		return false
	}
	got := sha256.Sum256(secret)
	return subtle.ConstantTimeCompare(got[:], want) == 1
}

func requireEmptyRuntimeStorage(config RuntimeConfig) error {
	paths := []string{config.NodeHostDir}
	if config.WALDir != "" && config.WALDir != config.NodeHostDir {
		paths = append(paths, config.WALDir)
	}
	for _, path := range paths {
		empty, err := directoryEmpty(path)
		if err != nil {
			return err
		}
		if !empty {
			return errors.New("raftstore: bootstrap/join requires empty generation-specific Dragonboat storage")
		}
	}
	return nil
}
