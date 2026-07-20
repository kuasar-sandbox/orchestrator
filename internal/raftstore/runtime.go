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
	Mode             RuntimeOpenMode
	BootstrapSecret  []byte
	TransitionClient ReplicaTransitionClient
}

type raftNodeHost interface {
	StartOnDiskReplica(map[uint64]dragonboat.Target, bool, sm.CreateOnDiskStateMachineFunc, dbconfig.Config) error
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
	mu                   sync.Mutex
	transitionMu         sync.Mutex
	config               RuntimeConfig
	registryLayout       RegistryLayout
	registryLayoutDigest string
	member               RegistryMember
	enrollment           LocalEnrollment
	enrollmentStore      EnrollmentStore
	nodeHost             raftNodeHost
	stateEngine          *PebbleStateEngine
	permitCache          *PermitCache
	transitionClient     ReplicaTransitionClient
	systemEvents         *runtimeSystemEvents
}

func OpenRuntime(
	config RuntimeConfig,
	registryLayoutChain []SignedRegistryLayout,
	keyring map[string]ed25519.PublicKey,
	options RuntimeOpenOptions,
) (*Runtime, error) {
	return openRuntime(config, registryLayoutChain, keyring, options, func(config dbconfig.NodeHostConfig) (raftNodeHost, error) {
		return dragonboat.NewNodeHost(config)
	})
}

func openRuntime(
	config RuntimeConfig,
	registryLayoutChain []SignedRegistryLayout,
	keyring map[string]ed25519.PublicKey,
	options RuntimeOpenOptions,
	factory nodeHostFactory,
) (*Runtime, error) {
	if len(registryLayoutChain) == 0 || factory == nil {
		return nil, errors.New("raftstore: a signed registryLayout chain and NodeHost factory are required")
	}
	latestSigned := registryLayoutChain[len(registryLayoutChain)-1]
	latestDigest, err := latestSigned.Verify(keyring)
	if err != nil {
		return nil, err
	}
	guard := RegistryLayoutGuard{Path: config.RegistryLayoutGuardPath}
	accepted, err := guard.EvaluateSignedChain(registryLayoutChain, keyring)
	if err != nil {
		return nil, err
	}
	if accepted.RegistryLayoutVersion != latestSigned.RegistryLayout.RegistryLayoutVersion || accepted.RegistryLayoutDigest != latestDigest ||
		accepted.RegistryGeneration != latestSigned.RegistryLayout.RegistryGeneration {
		return nil, errors.New("raftstore: supplied chain does not end at the accepted registryLayout")
	}

	store := EnrollmentStore{Path: config.EnrollmentPath}
	loadedEnrollment, err := store.Load()
	if err != nil {
		return nil, err
	}
	currentSigned := latestSigned
	currentDigest := latestDigest
	if loadedEnrollment != nil && loadedEnrollment.RegistryGeneration == latestSigned.RegistryLayout.RegistryGeneration {
		if _, found := registryLayoutMember(latestSigned.RegistryLayout, config.MemberID); !found {
			selected := false
			for _, candidate := range registryLayoutChain {
				digest, verifyErr := candidate.Verify(keyring)
				if verifyErr != nil {
					return nil, verifyErr
				}
				if candidate.RegistryLayout.RegistryGeneration == loadedEnrollment.RegistryGeneration &&
					candidate.RegistryLayout.RegistryLayoutVersion == loadedEnrollment.RegistryLayoutVersion &&
					digest == loadedEnrollment.RegistryLayoutDigest {
					if _, memberFound := registryLayoutMember(candidate.RegistryLayout, config.MemberID); !memberFound {
						return nil, errors.New("raftstore: enrolled member is absent from its active signed registryLayout")
					}
					currentSigned, currentDigest, selected = candidate, digest, true
					break
				}
			}
			if !selected {
				return nil, errors.New("raftstore: complete signed registryLayout chain is required to restart a removed member")
			}
		}
	}
	registryLayout := currentSigned.RegistryLayout
	member, found := registryLayoutMember(registryLayout, config.MemberID)
	if !found {
		return nil, errors.New("raftstore: local member is absent from the signed registryLayout")
	}
	nodeHostConfig, err := config.dragonboatConfig(registryLayout, member)
	if err != nil {
		return nil, err
	}
	systemEvents := &runtimeSystemEvents{}
	nodeHostConfig.SystemEventListener = systemEvents
	var enrollment LocalEnrollment
	if loadedEnrollment == nil {
		if options.Mode != RuntimeBootstrap && options.Mode != RuntimeJoin {
			return nil, ErrBootstrapUnauthorized
		}
		if err := requireEmptyRuntimeStorage(config); err != nil {
			return nil, err
		}
		mode := EnrollmentBootstrap
		if options.Mode == RuntimeBootstrap {
			if registryLayout.RegistryLayoutVersion != 1 || !bootstrapSecretMatches(options.BootstrapSecret, registryLayout.BootstrapTokenDigest) {
				return nil, ErrBootstrapUnauthorized
			}
		} else {
			if registryLayout.RegistryLayoutVersion == 1 {
				return nil, errors.New("raftstore: a first Registry Layout member must use explicit Registry History Generation bootstrap")
			}
			mode = EnrollmentJoin
		}
		created, createErr := newLocalEnrollment(mode, registryLayout, currentDigest, member, config)
		if createErr != nil {
			return nil, createErr
		}
		enrollment = created
	} else if loadedEnrollment.RegistryGeneration != registryLayout.RegistryGeneration {
		if options.Mode != RuntimeBootstrap || registryLayout.RegistryLayoutVersion != 1 || registryLayout.Predecessor == nil ||
			loadedEnrollment.ClusterID != registryLayout.ClusterID ||
			registryLayout.Predecessor.RegistryGeneration != loadedEnrollment.RegistryGeneration ||
			registryLayout.Predecessor.RegistryLayoutDigest != loadedEnrollment.RegistryLayoutDigest ||
			!bootstrapSecretMatches(options.BootstrapSecret, registryLayout.BootstrapTokenDigest) {
			return nil, ErrBootstrapUnauthorized
		}
		if err := requireEmptyRuntimeStorage(config); err != nil {
			return nil, err
		}
		created, createErr := newLocalEnrollment(EnrollmentBootstrap, registryLayout, currentDigest, member, config)
		if createErr != nil {
			return nil, createErr
		}
		enrollment = created
	} else {
		if options.Mode != RuntimeRestart {
			return nil, errors.New("raftstore: an enrolled member must restart; bootstrap/join cannot be replayed")
		}
		if err := loadedEnrollment.Matches(registryLayout, currentDigest, member, config); err != nil {
			return nil, err
		}
		enrollment = *loadedEnrollment
	}
	if err := config.attestStorage(); err != nil {
		return nil, err
	}
	if err := guard.store(accepted); err != nil {
		return nil, err
	}
	if err := store.Store(enrollment); err != nil {
		return nil, err
	}
	stateEngine, err := OpenPebbleStateEngine(config.StateEngineDir, config.Tuning.StateEngine)
	if err != nil {
		return nil, err
	}
	nodeHost, err := factory(nodeHostConfig)
	if err != nil {
		stateEngine.Close()
		return nil, fmt.Errorf("raftstore: create Dragonboat NodeHost: %w", err)
	}
	runtime := &Runtime{
		config: config, registryLayout: registryLayout, registryLayoutDigest: currentDigest, member: member,
		enrollment: enrollment, enrollmentStore: store, nodeHost: nodeHost, stateEngine: stateEngine,
		permitCache: NewPermitCache(time.Now), transitionClient: options.TransitionClient,
		systemEvents: systemEvents,
	}
	systemEvents.bind(runtime)
	return runtime, nil
}

func (r *Runtime) Close() {
	if r == nil || r.nodeHost == nil {
		return
	}
	if r.systemEvents != nil {
		r.systemEvents.unbind()
	}
	r.permitCache.Clear()
	r.nodeHost.Close()
	_ = r.stateEngine.Close()
}

func (r *Runtime) StartSystemReplica() error {
	if r.systemEvents != nil {
		if err := r.systemEvents.Err(); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	position := r.replicaPosition(SystemRaftShardID)
	if position < 0 {
		return ErrNoLocalReplica
	}
	return r.startReplica(position)
}

func (r *Runtime) StartDataReplicas(system SystemState) error {
	if r.systemEvents != nil {
		if err := r.systemEvents.Err(); err != nil {
			return err
		}
	}
	if err := r.authorizeRegistryLayoutState(system); err != nil {
		return err
	}
	r.mu.Lock()
	for position := range r.enrollment.Replicas {
		if r.enrollment.Replicas[position].ShardID == SystemRaftShardID ||
			r.enrollment.Replicas[position].LocalState == ReplicaRemoving ||
			r.enrollment.Replicas[position].LocalState == ReplicaRemoved {
			continue
		}
		if err := r.startReplica(position); err != nil {
			r.mu.Unlock()
			return err
		}
	}
	r.mu.Unlock()
	return r.SyncLocalRegistryLayout(system)
}

func (r *Runtime) PlanRegistryLayoutJoins(system SystemState) error {
	if err := r.authorizeRegistryLayoutState(system); err != nil {
		return err
	}
	if system.Transition == nil || system.Transition.Digest != r.registryLayoutDigest {
		return errors.New("raftstore: local joins require a committed registryLayout transition")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	desired := localReplicas(r.registryLayout, r.member, ReplicaJoin, true)
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
	if replica.LocalState == ReplicaRemoving || replica.LocalState == ReplicaRemoved {
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
	err := r.nodeHost.StartOnDiskReplica(initial, join, r.stateEngine.NewStateMachine,
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

func (r *Runtime) markReplicaRemoving(shardID, replicaID uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	position := r.replicaPosition(shardID)
	if position < 0 || r.enrollment.Replicas[position].ReplicaID != replicaID {
		return nil
	}
	switch r.enrollment.Replicas[position].LocalState {
	case ReplicaRemoving, ReplicaRemoved:
		return nil
	default:
		return r.setReplicaState(position, ReplicaRemoving)
	}
}

// SyncLocalRegistryLayout advances the enrollment's active-registryLayout fence only
// after the System Group has committed that exact signed registryLayout as active.
func (r *Runtime) SyncLocalRegistryLayout(system SystemState) error {
	if err := r.authorizeRegistryLayoutState(system); err != nil {
		return err
	}
	if system.ActiveRegistryLayoutVersion != r.registryLayout.RegistryLayoutVersion ||
		system.ActiveRegistryLayoutDigest != r.registryLayoutDigest {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.enrollment.RegistryLayoutVersion > system.ActiveRegistryLayoutVersion ||
		r.enrollment.RegistryLayoutVersion == system.ActiveRegistryLayoutVersion &&
			r.enrollment.RegistryLayoutDigest != system.ActiveRegistryLayoutDigest {
		return errors.New("raftstore: local enrollment active-registryLayout fence conflicts with System consensus")
	}
	if r.enrollment.RegistryLayoutVersion == system.ActiveRegistryLayoutVersion {
		return nil
	}
	next := r.enrollment
	next.Replicas = append([]LocalReplicaEnrollment(nil), r.enrollment.Replicas...)
	next.RegistryLayoutVersion = system.ActiveRegistryLayoutVersion
	next.RegistryLayoutDigest = system.ActiveRegistryLayoutDigest
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

func (r *Runtime) AwaitSystemRegistryLayout(ctx context.Context) (SystemState, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := r.ReadSystemLocal()
		if err == nil && state.Initialized {
			if err := r.authorizeRegistryLayoutState(state); err != nil {
				return SystemState{}, err
			}
			if err := r.SyncLocalRegistryLayout(state); err != nil {
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
	if r.enrollment.Mode != EnrollmentBootstrap || r.registryLayout.RegistryLayoutVersion != 1 {
		return SystemState{}, ErrBootstrapUnauthorized
	}
	if state, err := r.ReadSystemStrong(ctx); err == nil && state.Initialized {
		if err := r.authorizeRegistryLayoutState(state); err != nil {
			return SystemState{}, err
		}
		if err := r.SyncLocalRegistryLayout(state); err != nil {
			return SystemState{}, err
		}
		return state, nil
	}
	_, err := r.proposeSystem(ctx, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &r.registryLayout, Digest: r.registryLayoutDigest,
	})
	if err != nil {
		state, readErr := r.ReadSystemStrong(ctx)
		if readErr != nil || r.authorizeRegistryLayoutState(state) != nil {
			return SystemState{}, err
		}
		if syncErr := r.SyncLocalRegistryLayout(state); syncErr != nil {
			return SystemState{}, syncErr
		}
		return state, nil
	}
	state, err := r.ReadSystemStrong(ctx)
	if err != nil {
		return SystemState{}, err
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return SystemState{}, err
	}
	if err := r.SyncLocalRegistryLayout(state); err != nil {
		return SystemState{}, err
	}
	return state, nil
}

func (r *Runtime) InitializeDataShards(ctx context.Context, system SystemState, workers int) error {
	if r.enrollment.Mode != EnrollmentBootstrap || r.registryLayout.RegistryLayoutVersion != 1 {
		return ErrBootstrapUnauthorized
	}
	if err := r.authorizeRegistryLayoutState(system); err != nil {
		return err
	}
	if system.ActiveRegistryLayoutDigest != r.registryLayoutDigest || system.SystemEpoch != 1 {
		return errors.New("raftstore: data shards require the initial committed System registryLayout")
	}
	if err := r.SyncLocalRegistryLayout(system); err != nil {
		return err
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
		if replica.ShardID == SystemRaftShardID || replica.LocalState == ReplicaRemoving ||
			replica.LocalState == ReplicaRemoved {
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
	bootstrap, err := dataShardBootstrap(r.registryLayout, r.registryLayoutDigest, shardID)
	if err != nil {
		return err
	}
	identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: r.registryLayout.ClusterID, RegistryGeneration: r.registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: r.registryLayoutDigest,
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
	switch command.Type {
	case DataInitializeShard, DataPrepareEpoch, DataRetireEpoch:
		return DataApplyResult{}, errors.New("raftstore: data-shard lifecycle commands require the dedicated runtime workflow")
	case DataCompactFence:
		return DataApplyResult{}, errors.New("raftstore: execution-fence compaction requires the dedicated proof workflow")
	case DataPutRoute, DataPutBuild, DataPutFence:
	default:
		return DataApplyResult{}, errors.New("raftstore: unsupported data mutation command")
	}
	if err := r.authorizeLocalDataReplica(command.Identity); err != nil {
		return DataApplyResult{}, err
	}
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

func (r *Runtime) authorizeRegistryLayoutState(system SystemState) error {
	if err := system.Validate(); err != nil {
		return err
	}
	if system.ClusterID != r.registryLayout.ClusterID || system.RegistryGeneration != r.registryLayout.RegistryGeneration ||
		system.SchemaVersion != r.registryLayout.SchemaVersion || system.ProtocolVersion != r.registryLayout.ProtocolVersion ||
		system.VirtualShardCount != r.registryLayout.VirtualShardCount {
		return errors.New("raftstore: System state differs from the signed Registry History Generation")
	}
	if system.ActiveRegistryLayoutDigest == r.registryLayoutDigest && system.ActiveRegistryLayoutVersion == r.registryLayout.RegistryLayoutVersion {
		return nil
	}
	if system.Transition != nil && system.Transition.Digest == r.registryLayoutDigest &&
		system.Transition.Version == r.registryLayout.RegistryLayoutVersion {
		return nil
	}
	return errors.New("raftstore: signed registryLayout is not committed active or next System state")
}

func (r *Runtime) validateDataState(state DataState, shardID uint32) error {
	if err := state.Validate(); err != nil {
		return err
	}
	if state.ClusterID != r.registryLayout.ClusterID || state.RegistryGeneration != r.registryLayout.RegistryGeneration ||
		state.ShardID != shardID || state.SchemaVersion != r.registryLayout.SchemaVersion ||
		state.ProtocolVersion != r.registryLayout.ProtocolVersion || state.HashVersion != r.registryLayout.HashVersion ||
		state.VirtualShardCount != r.registryLayout.VirtualShardCount ||
		state.RouteBucketCount != r.registryLayout.RouteBucketCount || state.BuildBucketCount != r.registryLayout.BuildBucketCount {
		return errors.New("raftstore: data state differs from its signed registryLayout")
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
		member, found := registryLayoutMember(r.registryLayout, placement.MemberID)
		if !found {
			return nil, errors.New("raftstore: replica placement names an unknown member")
		}
		members[placement.ReplicaID] = member.RaftEndpoint
	}
	return members, nil
}

func (r *Runtime) placementForRaftShard(shardID uint64) ([]ReplicaPlacement, error) {
	if shardID == SystemRaftShardID {
		return r.registryLayout.SystemReplicas, nil
	}
	logical, ok := LogicalShardID(shardID)
	if !ok || logical >= uint32(len(r.registryLayout.DataShards)) {
		return nil, errors.New("raftstore: unknown Raft shard")
	}
	return r.registryLayout.DataShards[logical].Replicas, nil
}

func (r *Runtime) replicaPosition(shardID uint64) int {
	for position, replica := range r.enrollment.Replicas {
		if replica.ShardID == shardID {
			return position
		}
	}
	return -1
}

func registryLayoutMember(registryLayout RegistryLayout, memberID string) (RegistryMember, bool) {
	for _, member := range registryLayout.Members {
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
	paths := []string{config.NodeHostDir, config.StateEngineDir}
	if config.WALDir != "" && config.WALDir != config.NodeHostDir {
		paths = append(paths, config.WALDir)
	}
	for _, path := range paths {
		empty, err := directoryEmpty(path)
		if err != nil {
			return err
		}
		if !empty {
			return errors.New("raftstore: bootstrap/join requires empty Registry-History-Generation-specific Dragonboat storage")
		}
	}
	return nil
}
