package raftstore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dbconfig "github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/raftio"
	sm "github.com/lni/dragonboat/v4/statemachine"
)

type fakeReplicaStart struct {
	shardID       uint64
	replicaID     uint64
	join          bool
	initialMember int
}

type fakeNodeHost struct {
	history       map[[2]uint64]bool
	starts        []fakeReplicaStart
	createOnStart bool
	startErr      error
	read          func(uint64, any) (any, error)
	memberships   map[uint64]*dragonboat.Membership
	leaderID      uint64
	leaderTerm    uint64
	propose       func([]byte) (sm.Result, error)
	stopErr       error
	removeDataErr error
	stopCalls     int
	removeCalls   int
}

func newFakeNodeHost() *fakeNodeHost {
	return &fakeNodeHost{
		history: make(map[[2]uint64]bool), createOnStart: true,
		memberships: make(map[uint64]*dragonboat.Membership),
	}
}

func (f *fakeNodeHost) StartOnDiskReplica(
	initial map[uint64]dragonboat.Target,
	join bool,
	_ sm.CreateOnDiskStateMachineFunc,
	config dbconfig.Config,
) error {
	f.starts = append(f.starts, fakeReplicaStart{
		shardID: config.ShardID, replicaID: config.ReplicaID, join: join, initialMember: len(initial),
	})
	if f.createOnStart {
		f.history[[2]uint64{config.ShardID, config.ReplicaID}] = true
	}
	return f.startErr
}

func (f *fakeNodeHost) HasNodeInfo(shardID, replicaID uint64) bool {
	return f.history[[2]uint64{shardID, replicaID}]
}

func (f *fakeNodeHost) SyncPropose(_ context.Context, _ *client.Session, command []byte) (sm.Result, error) {
	if f.propose != nil {
		return f.propose(command)
	}
	return sm.Result{}, errors.New("not implemented")
}

func (f *fakeNodeHost) SyncRead(_ context.Context, shardID uint64, query any) (any, error) {
	if f.read == nil {
		return nil, errors.New("not implemented")
	}
	return f.read(shardID, query)
}

func (f *fakeNodeHost) StaleRead(shardID uint64, query any) (any, error) {
	if f.read == nil {
		return nil, errors.New("not implemented")
	}
	return f.read(shardID, query)
}

func (f *fakeNodeHost) GetNoOPSession(uint64) *client.Session { return nil }

func (f *fakeNodeHost) SyncGetShardMembership(_ context.Context, shardID uint64) (*dragonboat.Membership, error) {
	membership := f.memberships[shardID]
	if membership == nil {
		return nil, errors.New("membership unavailable")
	}
	return cloneMembership(membership), nil
}

func (f *fakeNodeHost) SyncRequestAddNonVoting(
	_ context.Context, shardID, replicaID uint64, target string, configChangeID uint64,
) error {
	membership := f.memberships[shardID]
	if membership == nil || membership.ConfigChangeID != configChangeID {
		return errors.New("stale config change")
	}
	membership.ConfigChangeID++
	membership.NonVotings[replicaID] = target
	return nil
}

func (f *fakeNodeHost) SyncRequestAddReplica(
	_ context.Context, shardID, replicaID uint64, _ string, configChangeID uint64,
) error {
	membership := f.memberships[shardID]
	if membership == nil || membership.ConfigChangeID != configChangeID {
		return errors.New("stale config change")
	}
	target, found := membership.NonVotings[replicaID]
	if !found {
		return errors.New("non-voting replica absent")
	}
	delete(membership.NonVotings, replicaID)
	membership.Nodes[replicaID] = target
	membership.ConfigChangeID++
	return nil
}

func (f *fakeNodeHost) SyncRequestDeleteReplica(
	_ context.Context, shardID, replicaID, configChangeID uint64,
) error {
	membership := f.memberships[shardID]
	if membership == nil || membership.ConfigChangeID != configChangeID {
		return errors.New("stale config change")
	}
	delete(membership.Nodes, replicaID)
	delete(membership.NonVotings, replicaID)
	membership.Removed[replicaID] = struct{}{}
	membership.ConfigChangeID++
	return nil
}

func (f *fakeNodeHost) StopReplica(uint64, uint64) error {
	f.stopCalls++
	return f.stopErr
}

func (f *fakeNodeHost) SyncRemoveData(_ context.Context, shardID, replicaID uint64) error {
	f.removeCalls++
	if f.removeDataErr != nil {
		err := f.removeDataErr
		f.removeDataErr = nil
		return err
	}
	delete(f.history, [2]uint64{shardID, replicaID})
	return nil
}

func (f *fakeNodeHost) GetLeaderID(uint64) (uint64, uint64, bool, error) {
	return f.leaderID, f.leaderTerm, f.leaderID != 0 && f.leaderTerm != 0, nil
}

func (f *fakeNodeHost) Close() {}

func cloneMembership(source *dragonboat.Membership) *dragonboat.Membership {
	clone := &dragonboat.Membership{
		ConfigChangeID: source.ConfigChangeID, Nodes: make(map[uint64]string, len(source.Nodes)),
		NonVotings: make(map[uint64]string, len(source.NonVotings)),
		Witnesses:  make(map[uint64]string, len(source.Witnesses)), Removed: make(map[uint64]struct{}, len(source.Removed)),
	}
	for id, target := range source.Nodes {
		clone.Nodes[id] = target
	}
	for id, target := range source.NonVotings {
		clone.NonVotings[id] = target
	}
	for id, target := range source.Witnesses {
		clone.Witnesses[id] = target
	}
	for id := range source.Removed {
		clone.Removed[id] = struct{}{}
	}
	return clone
}

type runtimeFixture struct {
	config  RuntimeConfig
	signed  SignedManifest
	keyring map[string]ed25519.PublicKey
	key     ed25519.PrivateKey
	secret  []byte
}

func newRuntimeFixture(t *testing.T) runtimeFixture {
	t.Helper()
	root := t.TempDir()
	secret := []byte("bootstrap-generation-1")
	manifest := testManifest(2, "generation-1")
	manifest.BootstrapTokenDigest = digestFor(string(secret))
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignManifest(manifest, "root-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	config := RuntimeConfig{
		MemberID: "registry-a", NodeHostDir: filepath.Join(root, "nodehost"),
		WALDir: filepath.Join(root, "wal"), StateEngineDir: filepath.Join(root, "state"),
		ManifestGuardPath: filepath.Join(root, "identity", "manifest.json"),
		EnrollmentPath:    filepath.Join(root, "identity", "enrollment.json"),
		TLS:               RaftTLS{CAFile: "ca.pem", CertFile: "cert.pem", KeyFile: "key.pem"},
		Tuning:            DefaultRuntimeTuning(),
		StorageAttestor:   StorageAttestorFunc(func(...string) error { return nil }),
	}
	return runtimeFixture{
		config: config, signed: signed, keyring: map[string]ed25519.PublicKey{"root-1": publicKey},
		key: privateKey, secret: secret,
	}
}

func (f runtimeFixture) open(t *testing.T, host *fakeNodeHost, options RuntimeOpenOptions) (*Runtime, error) {
	t.Helper()
	return openRuntime(f.config, []SignedManifest{f.signed}, f.keyring, options,
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return host, nil })
}

func TestRuntimeBootstrapIsExplicitAndRestartUsesDurableEnrollment(t *testing.T) {
	fixture := newRuntimeFixture(t)
	if _, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{Mode: RuntimeRestart}); !errors.Is(err, ErrBootstrapUnauthorized) {
		t.Fatalf("unenrolled restart error = %v", err)
	}
	if _, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: []byte("wrong"),
	}); !errors.Is(err, ErrBootstrapUnauthorized) {
		t.Fatalf("wrong bootstrap secret error = %v", err)
	}

	host := newFakeNodeHost()
	runtime, err := fixture.open(t, host, RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.StartSystemReplica(); err != nil {
		t.Fatal(err)
	}
	if len(host.starts) != 1 || host.starts[0].join || host.starts[0].initialMember != 3 {
		t.Fatalf("initial System start = %+v", host.starts)
	}
	runtime.Close()

	restarted, err := fixture.open(t, host, RuntimeOpenOptions{Mode: RuntimeRestart})
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.StartSystemReplica(); err != nil {
		t.Fatal(err)
	}
	last := host.starts[len(host.starts)-1]
	if last.join || last.initialMember != 0 {
		t.Fatalf("durable restart launched as bootstrap/join: %+v", last)
	}
}

func TestRuntimeFailsClosedOnAmbiguousStartAndLostHistory(t *testing.T) {
	t.Run("ambiguous start", func(t *testing.T) {
		fixture := newRuntimeFixture(t)
		failed := newFakeNodeHost()
		failed.createOnStart = false
		failed.startErr = errors.New("injected start failure")
		runtime, err := fixture.open(t, failed, RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.StartSystemReplica(); err == nil {
			t.Fatal("injected start failure was hidden")
		}
		runtime.Close()

		restarted, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{Mode: RuntimeRestart})
		if err != nil {
			t.Fatal(err)
		}
		if err := restarted.StartSystemReplica(); !errors.Is(err, ErrReplicaStartAmbiguous) {
			t.Fatalf("ambiguous restart error = %v", err)
		}
	})

	t.Run("lost active history", func(t *testing.T) {
		fixture := newRuntimeFixture(t)
		host := newFakeNodeHost()
		runtime, err := fixture.open(t, host, RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.StartSystemReplica(); err != nil {
			t.Fatal(err)
		}
		runtime.Close()

		restarted, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{Mode: RuntimeRestart})
		if err != nil {
			t.Fatal(err)
		}
		if err := restarted.StartSystemReplica(); !errors.Is(err, ErrReplicaHistoryLost) {
			t.Fatalf("lost-history restart error = %v", err)
		}
	})
}

func TestCommittedNodeDeletedEventDurablyFencesLocalReplica(t *testing.T) {
	fixture := newRuntimeFixture(t)
	runtime, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	runtime.systemEvents.NodeDeleted(raftio.NodeInfo{ShardID: SystemRaftShardID, ReplicaID: 1})
	if runtime.enrollment.Replicas[0].LocalState != ReplicaRemoving {
		t.Fatalf("deleted replica state = %s", runtime.enrollment.Replicas[0].LocalState)
	}
	stored, err := runtime.enrollmentStore.Load()
	if err != nil || stored == nil || stored.Replicas[0].LocalState != ReplicaRemoving {
		t.Fatalf("durable deleted replica state = %+v, %v", stored, err)
	}
	if err := runtime.StartSystemReplica(); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("deleted replica restart error = %v", err)
	}
	if err := runtime.RemoveLocalReplicaData(context.Background(), SystemRaftShardID); err != nil {
		t.Fatal(err)
	}
	if runtime.enrollment.Replicas[0].LocalState != ReplicaRemoved {
		t.Fatalf("cleaned replica state = %s", runtime.enrollment.Replicas[0].LocalState)
	}
}

func TestRuntimeKeepsEnrolledReplicaUntilLocalRemovalIsDurable(t *testing.T) {
	manifest := transitionManifest(t)
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	member, found := manifestMember(manifest, "registry-c")
	if !found {
		t.Fatal("removed replica member absent from manifest member catalog")
	}
	host := newFakeNodeHost()
	host.history[[2]uint64{SystemRaftShardID, 3}] = true
	host.history[[2]uint64{DataRaftShardID(0), 3}] = true
	runtime := &Runtime{
		manifest: manifest, manifestDigest: digest, member: member, nodeHost: host,
		enrollment: LocalEnrollment{ManifestVersion: manifest.ManifestVersion, ManifestDigest: digest, Replicas: []LocalReplicaEnrollment{
			{ShardID: SystemRaftShardID, ReplicaID: 3, LocalState: ReplicaActive},
			{ShardID: DataRaftShardID(0), ReplicaID: 3, LocalState: ReplicaActive},
		}},
	}
	if err := runtime.StartSystemReplica(); err != nil {
		t.Fatal(err)
	}
	system, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("System bootstrap = %+v", result)
	}
	if err := runtime.StartDataReplicas(system); err != nil {
		t.Fatal(err)
	}
	if len(host.starts) != 2 {
		t.Fatalf("enrolled old replicas were not kept alive: %+v", host.starts)
	}
	runtime.enrollment.Replicas[0].LocalState = ReplicaRemoving
	runtime.enrollment.Replicas[1].LocalState = ReplicaRemoved
	if err := runtime.StartSystemReplica(); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("removing System replica start error = %v", err)
	}
	if err := runtime.StartDataReplicas(system); err != nil {
		t.Fatal(err)
	}
	if len(host.starts) != 2 {
		t.Fatalf("locally fenced replicas restarted: %+v", host.starts)
	}
}

func TestRuntimeExplicitlyRollsEnrollmentToLinkedSuccessorGeneration(t *testing.T) {
	fixture := newRuntimeFixture(t)
	firstHost := newFakeNodeHost()
	first, err := fixture.open(t, firstHost, RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	first.Close()

	firstDigest, err := fixture.signed.Manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	successorSecret := []byte("bootstrap-generation-2")
	successor := testManifest(2, "generation-2")
	successor.BootstrapTokenDigest = digestFor(string(successorSecret))
	successor, _ = finalizeConsensusSuccessor(t, fixture.signed.Manifest, firstDigest, successor)
	signedSuccessor, err := SignManifest(successor, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	successorConfig := fixture.config
	root := filepath.Dir(fixture.config.NodeHostDir)
	successorConfig.NodeHostDir = filepath.Join(root, "nodehost-generation-2")
	successorConfig.WALDir = filepath.Join(root, "wal-generation-2")
	successorConfig.StateEngineDir = filepath.Join(root, "state-generation-2")
	if _, err := openRuntime(
		successorConfig, []SignedManifest{signedSuccessor}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: []byte("wrong-successor-secret")},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) {
			t.Fatal("unauthorized rollover created a NodeHost")
			return nil, nil
		},
	); !errors.Is(err, ErrBootstrapUnauthorized) {
		t.Fatalf("unauthorized successor error = %v", err)
	}
	guarded, err := (ManifestGuard{Path: fixture.config.ManifestGuardPath}).Load()
	if err != nil || guarded == nil || guarded.StorageGeneration != fixture.signed.Manifest.StorageGeneration ||
		guarded.ManifestDigest != firstDigest {
		t.Fatalf("failed successor advanced manifest guard = %+v, %v", guarded, err)
	}
	persistedEnrollment, err := (EnrollmentStore{Path: fixture.config.EnrollmentPath}).Load()
	if err != nil || persistedEnrollment == nil ||
		persistedEnrollment.StorageGeneration != fixture.signed.Manifest.StorageGeneration {
		t.Fatalf("failed successor replaced source enrollment = %+v, %v", persistedEnrollment, err)
	}

	successorRuntime, err := openRuntime(
		successorConfig, []SignedManifest{signedSuccessor}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: successorSecret},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer successorRuntime.Close()
	if successorRuntime.enrollment.StorageGeneration != "generation-2" ||
		successorRuntime.enrollment.Mode != EnrollmentBootstrap {
		t.Fatalf("successor enrollment = %+v", successorRuntime.enrollment)
	}
	if _, err := openRuntime(
		fixture.config, []SignedManifest{fixture.signed}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	); err == nil {
		t.Fatal("accepted manifest guard rolled back to the predecessor generation")
	}
}

func TestRuntimeAdvancesEnrollmentManifestOnlyAfterConsensusActivation(t *testing.T) {
	fixture := newRuntimeFixture(t)
	first, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	firstDigest, err := fixture.signed.Manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next := fixture.signed.Manifest
	next.ManifestVersion = 2
	next.PreviousManifestDigest = firstDigest
	next.Members = append([]RegistryMember(nil), next.Members...)
	next.Members[1].InternalEndpoint = "https://registry-b-v2:9443"
	signedNext, err := SignManifest(next, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := openRuntime(
		fixture.config, []SignedManifest{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.enrollment.ManifestVersion != fixture.signed.Manifest.ManifestVersion ||
		restarted.enrollment.ManifestDigest != firstDigest {
		t.Fatalf("published next manifest advanced enrollment before consensus: %+v", restarted.enrollment)
	}
	nextDigest, err := next.Digest()
	if err != nil {
		t.Fatal(err)
	}
	system, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &fixture.signed.Manifest, Digest: firstDigest,
	})
	shards := []ShardTransition{{ShardID: ^uint32(0), Stage: TransitionPending}}
	for shardID := uint32(0); shardID < next.VirtualShardCount; shardID++ {
		shards = append(shards, ShardTransition{ShardID: shardID, Stage: TransitionPending})
	}
	system, _ = applySystem(t, system, 2, SystemCommand{
		Type: SystemBeginTransition,
		Transition: &ManifestTransition{
			Version: next.ManifestVersion, Digest: nextDigest, PreviousDigest: firstDigest,
			NextSystemEpoch: 2, Shards: shards,
		},
	})
	index := uint64(3)
	for _, shard := range shards {
		for _, edge := range [][2]TransitionStage{
			{TransitionPending, TransitionCatchingUp},
			{TransitionCatchingUp, TransitionPromoted},
			{TransitionPromoted, TransitionOldRemoved},
			{TransitionOldRemoved, TransitionComplete},
		} {
			system, _ = applySystem(t, system, index, SystemCommand{
				Type:    SystemAdvanceTransition,
				Advance: &TransitionAdvance{ShardID: shard.ShardID, From: edge[0], To: edge[1]},
			})
			index++
		}
	}
	system, _ = applySystem(t, system, index, SystemCommand{Type: SystemActivateTransition})
	if err := restarted.SyncLocalManifest(system); err != nil {
		t.Fatal(err)
	}
	if restarted.enrollment.ManifestVersion != next.ManifestVersion ||
		restarted.enrollment.ManifestDigest != nextDigest {
		t.Fatalf("activated manifest was not persisted: %+v", restarted.enrollment)
	}
	restarted.Close()
	if _, err := openRuntime(
		fixture.config, []SignedManifest{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	); err != nil {
		t.Fatalf("full manifest chain was not restart-idempotent: %v", err)
	}
}

func TestRemovedMemberRestartsFromActiveArtifactWithoutGuardRollback(t *testing.T) {
	fixture := newRuntimeFixture(t)
	fixture.config.MemberID = "registry-c"
	first, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	firstDigest, err := fixture.signed.Manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next := fixture.signed.Manifest
	next.ManifestVersion = 2
	next.PreviousManifestDigest = firstDigest
	next.Members = []RegistryMember{
		fixture.signed.Manifest.Members[0], fixture.signed.Manifest.Members[1],
		{MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001"},
	}
	desired := []ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-d", ReplicaID: 4},
	}
	next.SystemReplicas = append([]ReplicaPlacement(nil), desired...)
	next.DataShards = append([]ShardPlacement(nil), fixture.signed.Manifest.DataShards...)
	for index := range next.DataShards {
		next.DataShards[index].Replicas = append([]ReplicaPlacement(nil), desired...)
	}
	signedNext, err := SignManifest(next, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := openRuntime(
		fixture.config, []SignedManifest{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if restarted.manifestDigest != firstDigest || restarted.member.MemberID != "registry-c" {
		t.Fatalf("removed member selected wrong runtime manifest: digest=%s member=%s",
			restarted.manifestDigest, restarted.member.MemberID)
	}
	nextDigest, err := next.Digest()
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := (ManifestGuard{Path: fixture.config.ManifestGuardPath}).Load()
	if err != nil || accepted == nil || accepted.ManifestDigest != nextDigest {
		t.Fatalf("anti-rollback guard did not retain next artifact: %+v, %v", accepted, err)
	}
}

func TestDataShardBootstrapCommandDoesNotEmbedWholeManifest(t *testing.T) {
	manifest := testManifest(DefaultVirtualShards, "generation-1")
	bootstrap, err := NewDataShardBootstrap(manifest, DefaultVirtualShards-1)
	if err != nil {
		t.Fatal(err)
	}
	command, err := EncodeDataCommand(DataCommand{
		Type: DataInitializeShard,
		Identity: ShardRequestIdentity{PermitIdentity: PermitIdentity{
			ClusterID: manifest.ClusterID, StorageGeneration: manifest.StorageGeneration,
			SystemEpoch: 1, ManifestDigest: bootstrap.ManifestDigest,
		}, ShardID: bootstrap.ShardID},
		Bootstrap: &bootstrap, ReplicaIDs: append([]uint64(nil), bootstrap.ReplicaIDs...),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(command) > 2048 {
		t.Fatalf("compact data bootstrap command is %d bytes", len(command))
	}
}

func TestRuntimePredecessorDrainUsesFullMonotonicWait(t *testing.T) {
	predecessor := testManifest(2, "generation-1")
	predecessor.ServePermitMaxMillis = 25
	predecessorDigest, err := predecessor.Digest()
	if err != nil {
		t.Fatal(err)
	}
	manifest := testManifest(2, "generation-2")
	manifest.ServePermitMaxMillis = 50
	manifest, _ = finalizeConsensusSuccessor(t, predecessor, predecessorDigest, manifest)
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	if !result.Applied {
		t.Fatalf("successor bootstrap = %+v", result)
	}
	host := newFakeNodeHost()
	host.read = func(shardID uint64, query any) (any, error) {
		if shardID != SystemRaftShardID {
			return nil, errors.New("unexpected shard")
		}
		return cloneSystemState(state), nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		command, err := DecodeSystemCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		next, applied := ApplySystemCommand(state, state.LastApplied+1, command)
		state = next
		encoded, err := json.Marshal(applied)
		return sm.Result{Value: 1, Data: encoded}, err
	}
	runtime := &Runtime{manifest: manifest, manifestDigest: digest, nodeHost: host}
	if _, err := runtime.ApplySystem(context.Background(), SystemCommand{
		Type: SystemConfirmDrain, Drain: &DrainConfirmation{
			PredecessorProofDigest: manifest.Predecessor.ProofDigest,
			WaitedMillis:           manifest.Predecessor.ServePermitMaxMillis, EvidenceDigest: digestFor("evidence"),
		},
	}); err == nil {
		t.Fatal("generic System operation bypassed the monotonic drain wait")
	}
	started := time.Now()
	confirmed, err := runtime.ConfirmPredecessorPermitDrain(context.Background(), digestFor("evidence"))
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond {
		t.Fatalf("predecessor drain completed after %s", elapsed)
	}
	if !confirmed.PredecessorDrainComplete || state.LastApplied != 2 {
		t.Fatalf("confirmed successor state = %+v", confirmed)
	}
}

func TestRuntimeRejectsUnprovenGenericSystemLifecycleCommands(t *testing.T) {
	runtime := &Runtime{}
	commands := []SystemCommand{
		{Type: SystemSetGates, Gates: &GateUpdate{Serve: true, Write: true, Cutover: true}},
		{Type: SystemBeginRecovery, Recovery: &RecoveryEpoch{}},
		{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{}},
	}
	for _, command := range commands {
		if _, err := runtime.ApplySystem(context.Background(), command); err == nil {
			t.Fatalf("generic System operation accepted %s", command.Type)
		}
	}
}

func TestRuntimeClosesGenerationOnlyForCompleteSuccessorIntent(t *testing.T) {
	manifest := testManifest(2, "generation-1")
	digest, err := manifest.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, Manifest: &manifest, Digest: digest,
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("bootstrap = %+v", result)
	}
	host := newFakeNodeHost()
	host.read = func(shardID uint64, query any) (any, error) {
		if shardID != SystemRaftShardID {
			return nil, errors.New("unexpected shard")
		}
		return cloneSystemState(state), nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		command, decodeErr := DecodeSystemCommand(raw)
		if decodeErr != nil {
			return sm.Result{}, decodeErr
		}
		next, applied := ApplySystemCommand(state, state.LastApplied+1, command)
		state = next
		encoded, encodeErr := json.Marshal(applied)
		return sm.Result{Value: 1, Data: encoded}, encodeErr
	}
	runtime := &Runtime{manifest: manifest, manifestDigest: digest, nodeHost: host}

	successor := testManifest(2, "generation-2")
	successor.Predecessor = &PredecessorProof{
		StorageGeneration: manifest.StorageGeneration, ManifestDigest: digest,
		ServePermitMaxMillis: manifest.ServePermitMaxMillis, Kind: RolloverConsensusClosure,
	}
	if _, err := runtime.ApplySystem(context.Background(), SystemCommand{
		Type: SystemCloseGeneration,
		Closure: &GenerationClosure{
			TargetStorageGeneration:    successor.StorageGeneration,
			TargetManifestIntentDigest: digestFor("unverified-intent"), Kind: RolloverConsensusClosure,
		},
	}); err == nil {
		t.Fatal("generic System operation bypassed successor-manifest validation")
	}
	closed, err := runtime.CloseStorageGeneration(context.Background(), successor)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := closed.ConsensusPredecessorProof()
	if err != nil {
		t.Fatal(err)
	}
	successor.Predecessor = &proof
	if err := successor.Validate(); err != nil {
		t.Fatalf("final successor manifest = %v", err)
	}
	if replayed, err := runtime.CloseStorageGeneration(context.Background(), successor); err != nil ||
		replayed.Closure == nil || replayed.Closure.ProofDigest != proof.ProofDigest {
		t.Fatalf("idempotent close = %+v, %v", replayed, err)
	}

	different := successor
	different.Members = append([]RegistryMember(nil), successor.Members...)
	different.Members[0].InternalEndpoint = "https://registry-a-other:9443"
	if _, err := runtime.CloseStorageGeneration(context.Background(), different); err == nil {
		t.Fatal("retired generation authorized a different successor intent")
	}
}
