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
	SystemClient     RemoteSystemClient
}

// RemoteSystemClient is the trusted internal path used by Registry members
// that do not host one of the three System Group replicas. Every method still
// executes against the sole System Group; this interface creates no secondary
// authority or local lease source.
type RemoteSystemClient interface {
	ReadSystemStrong(context.Context) (SystemState, error)
	ApplySystem(context.Context, SystemCommand) (SystemApplyResult, error)
	RefreshPermit(context.Context) (PermitGrant, error)
	CloseRegistryGeneration(context.Context, RegistryLayout) (SystemState, error)
	ConfirmPredecessorPermitDrain(context.Context, string) (SystemState, error)
	BeginRecovery(context.Context) (SystemState, error)
	AdvanceRecovery(context.Context, RecoveryPhase, RecoveryPhase) (SystemState, error)
	SetServingGates(context.Context, GateUpdate) (SystemState, error)
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
	mu                    sync.Mutex
	transitionMu          sync.Mutex
	systemCacheMu         sync.RWMutex
	config                RuntimeConfig
	registryLayout        RegistryLayout
	registryLayoutDigest  string
	startupRegistryLayout RegistryLayout
	member                RegistryMember
	enrollment            LocalEnrollment
	enrollmentStore       EnrollmentStore
	nodeHost              raftNodeHost
	stateEngine           *PebbleStateEngine
	permitCache           *PermitCache
	transitionClient      ReplicaTransitionClient
	systemClient          RemoteSystemClient
	systemCache           *SystemState
	outboxAckVerifier     FenceOutboxAckVerifier
	systemEvents          *runtimeSystemEvents
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
	startupSigned := latestSigned
	if loadedEnrollment != nil && loadedEnrollment.RegistryGeneration == latestSigned.RegistryLayout.RegistryGeneration {
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
				startupSigned, selected = candidate, true
				break
			}
		}
		if !selected {
			return nil, errors.New("raftstore: complete signed registryLayout chain is required to restart an enrolled member")
		}
	}
	registryLayout := latestSigned.RegistryLayout
	member, found := registryLayoutMember(startupSigned.RegistryLayout, config.MemberID)
	if !found {
		return nil, errors.New("raftstore: local member is absent from its startup registryLayout")
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
		created, createErr := newLocalEnrollment(mode, registryLayout, latestDigest, member, config)
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
		created, createErr := newLocalEnrollment(EnrollmentBootstrap, registryLayout, latestDigest, member, config)
		if createErr != nil {
			return nil, createErr
		}
		enrollment = created
	} else {
		if options.Mode != RuntimeRestart {
			return nil, errors.New("raftstore: an enrolled member must restart; bootstrap/join cannot be replayed")
		}
		if err := loadedEnrollment.Matches(registryLayout, latestDigest, member, config); err != nil {
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
		config: config, registryLayout: registryLayout, registryLayoutDigest: latestDigest,
		startupRegistryLayout: startupSigned.RegistryLayout, member: member,
		enrollment: enrollment, enrollmentStore: store, nodeHost: nodeHost, stateEngine: stateEngine,
		permitCache: NewPermitCache(time.Now), transitionClient: options.TransitionClient,
		systemClient: options.SystemClient,
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

func (r *Runtime) HasLocalSystemReplica() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	position := r.replicaPosition(SystemRaftShardID)
	if position < 0 {
		return false
	}
	switch r.enrollment.Replicas[position].LocalState {
	case ReplicaPlanned, ReplicaStarting, ReplicaActive:
		return true
	default:
		return false
	}
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
	if err := r.removalFenceError(); err != nil {
		return SystemState{}, err
	}
	if !r.HasLocalSystemReplica() {
		r.systemCacheMu.RLock()
		state := r.systemCache
		if state != nil {
			copy := cloneSystemState(*state)
			state = &copy
		}
		r.systemCacheMu.RUnlock()
		if state == nil {
			return SystemState{}, ErrNoLocalReplica
		}
		return *state, nil
	}
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
	if err := r.removalFenceError(); err != nil {
		return SystemState{}, err
	}
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemState{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		state, err := r.systemClient.ReadSystemStrong(operation)
		if err != nil {
			return SystemState{}, err
		}
		if err := state.Validate(); err != nil {
			return SystemState{}, err
		}
		if err := r.authorizeRemoteSystemState(state); err != nil {
			return SystemState{}, err
		}
		r.cacheRemoteSystem(state)
		return cloneSystemState(state), nil
	}
	value, err := r.syncRead(ctx, SystemRaftShardID, SystemStateLookup{})
	if err != nil {
		return SystemState{}, err
	}
	state, ok := value.(SystemState)
	if !ok {
		return SystemState{}, errors.New("raftstore: Dragonboat returned an invalid System lookup type")
	}
	return state, nil
}

func (r *Runtime) authorizeRemoteSystemState(state SystemState) error {
	if err := r.authorizeRegistryLayoutState(state); err == nil {
		return nil
	}
	if r.registryLayout.RegistryLayoutVersion > 1 && state.Transition == nil && !state.Retired && state.Recovery == nil &&
		state.ClusterID == r.registryLayout.ClusterID && state.RegistryGeneration == r.registryLayout.RegistryGeneration &&
		state.ActiveRegistryLayoutVersion+1 == r.registryLayout.RegistryLayoutVersion &&
		state.ActiveRegistryLayoutDigest == r.registryLayout.PreviousRegistryLayoutDigest {
		return nil
	}
	return errors.New("raftstore: remote System state does not authorize the verified registryLayout")
}

func (r *Runtime) cacheRemoteSystem(state SystemState) {
	copy := cloneSystemState(state)
	r.systemCacheMu.Lock()
	if r.systemCache == nil || copy.LastApplied >= r.systemCache.LastApplied {
		r.systemCache = &copy
	}
	r.systemCacheMu.Unlock()
}

func (r *Runtime) acceptRemoteSystemState(state SystemState) error {
	if err := state.Validate(); err != nil {
		return err
	}
	if err := r.authorizeRegistryLayoutState(state); err != nil {
		return err
	}
	r.cacheRemoteSystem(state)
	return nil
}

func (r *Runtime) AwaitSystemRegistryLayout(ctx context.Context) (SystemState, error) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := r.ReadSystemLocal()
		if !r.HasLocalSystemReplica() {
			state, err = r.ReadSystemStrong(ctx)
		}
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
	var committed SystemState
	accept := func(state SystemState) (bool, error) {
		if !state.Initialized {
			return false, nil
		}
		if err := r.authorizeRegistryLayoutState(state); err != nil {
			return false, err
		}
		if err := r.SyncLocalRegistryLayout(state); err != nil {
			return false, err
		}
		committed = state
		return true, nil
	}
	err := r.retryStartupOperation(ctx, func(operation context.Context) (bool, error) {
		state, readErr := r.ReadSystemStrong(operation)
		if readErr == nil {
			if done, err := accept(state); done || err != nil {
				return done, err
			}
		} else if !dragonboat.IsTempError(readErr) {
			return false, readErr
		}
		result, proposeErr := r.proposeSystem(operation, SystemCommand{
			Type: SystemBootstrap, RegistryLayout: &r.registryLayout, Digest: r.registryLayoutDigest,
		})
		if proposeErr != nil {
			return false, proposeErr
		}
		state, readErr = r.ReadSystemStrong(operation)
		if readErr != nil {
			return false, readErr
		}
		if done, err := accept(state); done || err != nil {
			return done, err
		}
		if result.Conflict {
			return false, errors.New(result.Reason)
		}
		return false, errors.New("raftstore: committed System bootstrap did not initialize state")
	})
	return committed, err
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
	bootstrap, err := dataShardBootstrap(r.registryLayout, r.registryLayoutDigest, shardID)
	if err != nil {
		return err
	}
	identity := ShardRequestIdentity{PermitIdentity: PermitIdentity{
		ClusterID: r.registryLayout.ClusterID, RegistryGeneration: r.registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: r.registryLayoutDigest,
	}, ShardID: shardID}
	accept := func(value any) (bool, error) {
		state, valid := value.(DataState)
		if !valid {
			return false, errors.New("raftstore: Dragonboat returned an invalid data lookup type")
		}
		if !state.Initialized {
			return false, nil
		}
		return true, r.validateDataState(state, shardID)
	}
	return r.retryStartupOperation(ctx, func(operation context.Context) (bool, error) {
		value, readErr := r.syncRead(operation, replica.ShardID, DataStateLookup{})
		if readErr == nil {
			if done, err := accept(value); done || err != nil {
				return done, err
			}
		} else if !dragonboat.IsTempError(readErr) {
			return false, readErr
		}
		result, proposeErr := r.proposeDataRaw(operation, DataCommand{
			Type: DataInitializeShard, Identity: identity, Bootstrap: &bootstrap,
			ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
		})
		if proposeErr != nil {
			return false, proposeErr
		}
		value, readErr = r.syncRead(operation, replica.ShardID, DataStateLookup{})
		if readErr != nil {
			return false, readErr
		}
		if done, err := accept(value); done || err != nil {
			return done, err
		}
		if result.Conflict {
			return false, errors.New(result.Reason)
		}
		return false, errors.New("raftstore: committed data-shard bootstrap did not initialize state")
	})
}

func (r *Runtime) RefreshPermit(ctx context.Context) (PermitGrant, error) {
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return PermitGrant{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		started := time.Now()
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		grant, err := r.systemClient.RefreshPermit(operation)
		if err != nil {
			return PermitGrant{}, err
		}
		state, err := r.systemClient.ReadSystemStrong(operation)
		if err != nil {
			return PermitGrant{}, err
		}
		if err := state.Validate(); err != nil || state.LastApplied < grant.CommitIndex || state.Identity() != grant.PermitIdentity {
			return PermitGrant{}, errors.Join(err, errors.New("raftstore: remote Permit is not covered by the fetched System state"))
		}
		if err := r.permitCache.Install(grant, started); err != nil {
			return PermitGrant{}, err
		}
		r.cacheRemoteSystem(state)
		return grant, nil
	}
	var result SystemApplyResult
	var started time.Time
	err := r.retryStartupOperation(ctx, func(operation context.Context) (bool, error) {
		started = time.Now()
		candidate, proposeErr := r.proposeSystem(operation, SystemCommand{Type: SystemRefreshPermit})
		if proposeErr != nil {
			return false, proposeErr
		}
		result = candidate
		return true, nil
	})
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
	case DataInitializeShard, DataPrepareEpoch, DataRetireEpoch,
		DataBeginRecovery, DataStageRecovery, DataAckRecovery, DataActivateRecovery,
		DataQuarantineRecovery, DataFinalizeRecovery:
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
	result, err := r.proposeDataRaw(ctx, command)
	if err == nil {
		return result, nil
	}
	resolved, readErr := r.resolveDataMutation(ctx, command)
	if readErr == nil && resolved.Committed {
		return DataApplyResult{Applied: true, Revision: resolved.Revision}, nil
	}
	if readErr != nil {
		return DataApplyResult{}, errors.Join(err, fmt.Errorf("raftstore: resolve ambiguous data mutation: %w", readErr))
	}
	return DataApplyResult{}, err
}

func (r *Runtime) resolveDataMutation(ctx context.Context, command DataCommand) (DataMutationStatus, error) {
	value, err := r.syncRead(
		ctx, DataRaftShardID(command.Identity.ShardID), DataMutationLookup{Command: command},
	)
	if err != nil {
		return DataMutationStatus{}, err
	}
	status, ok := value.(DataMutationStatus)
	if !ok {
		return DataMutationStatus{}, errors.New("raftstore: Dragonboat returned an invalid mutation status")
	}
	return status, nil
}

func (r *Runtime) proposeSystem(ctx context.Context, command SystemCommand) (SystemApplyResult, error) {
	if err := r.removalFenceError(); err != nil {
		return SystemApplyResult{}, err
	}
	if !r.HasLocalSystemReplica() {
		if r.systemClient == nil {
			return SystemApplyResult{}, errors.New("raftstore: remote System Group client is unavailable")
		}
		operation, cancel := r.operationContext(ctx)
		defer cancel()
		state, err := r.systemClient.ReadSystemStrong(operation)
		if err != nil {
			return SystemApplyResult{}, err
		}
		if command.Type == SystemBeginTransition {
			err = r.authorizeRemoteSystemState(state)
		} else {
			err = r.authorizeRegistryLayoutState(state)
		}
		if err != nil {
			return SystemApplyResult{}, err
		}
		r.cacheRemoteSystem(state)
		return r.systemClient.ApplySystem(operation, command)
	}
	raw, err := EncodeSystemCommand(command)
	if err != nil {
		return SystemApplyResult{}, err
	}
	result, err := r.syncPropose(ctx, r.nodeHost.GetNoOPSession(SystemRaftShardID), raw)
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
	if err := r.removalFenceError(); err != nil {
		return DataApplyResult{}, err
	}
	raw, err := EncodeDataCommand(command)
	if err != nil {
		return DataApplyResult{}, err
	}
	shardID := DataRaftShardID(command.Identity.ShardID)
	result, err := r.syncPropose(ctx, r.nodeHost.GetNoOPSession(shardID), raw)
	if err != nil {
		return DataApplyResult{}, err
	}
	var applied DataApplyResult
	if err := json.Unmarshal(result.Data, &applied); err != nil {
		return DataApplyResult{}, fmt.Errorf("raftstore: decode data apply result: %w", err)
	}
	return applied, nil
}

func (r *Runtime) removalFenceError() error {
	if r != nil && r.systemEvents != nil {
		if err := r.systemEvents.Err(); err != nil {
			return fmt.Errorf("raftstore: local replica removal fence is not durable: %w", err)
		}
	}
	return nil
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
	if system.Transition == nil && r.registryLayout.RegistryLayoutVersion > 1 &&
		system.ActiveRegistryLayoutVersion == r.registryLayout.PreviousRegistryLayoutVersion &&
		system.ActiveRegistryLayoutDigest == r.registryLayout.PreviousRegistryLayoutDigest {
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
	layout := r.startupRegistryLayout
	if layout.ClusterID == "" {
		layout = r.registryLayout
	}
	placements, err := placementForRaftShard(layout, shardID)
	if err != nil {
		return nil, err
	}
	members := make(map[uint64]dragonboat.Target, len(placements))
	for _, placement := range placements {
		member, found := registryLayoutMember(layout, placement.MemberID)
		if !found {
			return nil, errors.New("raftstore: replica placement names an unknown member")
		}
		members[placement.ReplicaID] = member.RaftEndpoint
	}
	return members, nil
}

func placementForRaftShard(layout RegistryLayout, shardID uint64) ([]ReplicaPlacement, error) {
	if shardID == SystemRaftShardID {
		return layout.SystemReplicas, nil
	}
	logical, ok := LogicalShardID(shardID)
	if !ok || logical >= uint32(len(layout.DataShards)) {
		return nil, errors.New("raftstore: unknown Raft shard")
	}
	return layout.DataShards[logical].Replicas, nil
}

func (r *Runtime) placementForRaftShard(shardID uint64) ([]ReplicaPlacement, error) {
	return placementForRaftShard(r.registryLayout, shardID)
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
