package raftstore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
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
	readCtx       func(context.Context, uint64, any) (any, error)
	memberships   map[uint64]*dragonboat.Membership
	leaderID      uint64
	leaderTerm    uint64
	propose       func([]byte) (sm.Result, error)
	proposeCtx    func(context.Context, []byte) (sm.Result, error)
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

func (f *fakeNodeHost) SyncPropose(ctx context.Context, _ *client.Session, command []byte) (sm.Result, error) {
	if f.proposeCtx != nil {
		return f.proposeCtx(ctx, command)
	}
	if f.propose != nil {
		return f.propose(command)
	}
	return sm.Result{}, errors.New("not implemented")
}

func (f *fakeNodeHost) SyncRead(ctx context.Context, shardID uint64, query any) (any, error) {
	if f.readCtx != nil {
		return f.readCtx(ctx, shardID, query)
	}
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
	signed  SignedRegistryLayout
	keyring map[string]ed25519.PublicKey
	key     ed25519.PrivateKey
	secret  []byte
}

func newRuntimeFixture(t *testing.T) runtimeFixture {
	t.Helper()
	root := t.TempDir()
	secret := []byte("bootstrap-generation-1")
	registryLayout := testRegistryLayout(2, "generation-1")
	registryLayout.BootstrapTokenDigest = digestFor(string(secret))
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := SignRegistryLayout(registryLayout, "root-1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	config := RuntimeConfig{
		MemberID: "registry-a", NodeHostDir: filepath.Join(root, "nodehost"),
		WALDir: filepath.Join(root, "wal"), StateEngineDir: filepath.Join(root, "state"),
		RegistryLayoutGuardPath: filepath.Join(root, "identity", "registryLayout.json"),
		EnrollmentPath:          filepath.Join(root, "identity", "enrollment.json"),
		TLS:                     RaftTLS{CAFile: "ca.pem", CertFile: "cert.pem", KeyFile: "key.pem"},
		Tuning:                  DefaultRuntimeTuning(),
		StorageAttestor:         StorageAttestorFunc(func(...string) error { return nil }),
	}
	return runtimeFixture{
		config: config, signed: signed, keyring: map[string]ed25519.PublicKey{"root-1": publicKey},
		key: privateKey, secret: secret,
	}
}

func (f runtimeFixture) open(t *testing.T, host *fakeNodeHost, options RuntimeOpenOptions) (*Runtime, error) {
	t.Helper()
	return openRuntime(f.config, []SignedRegistryLayout{f.signed}, f.keyring, options,
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

func TestRuntimeBootstrapRetriesUntilSystemLeaderIsReady(t *testing.T) {
	fixture := newRuntimeFixture(t)
	host := newFakeNodeHost()
	var state SystemState
	readCalls := 0
	proposeCalls := 0
	host.read = func(shardID uint64, _ any) (any, error) {
		if shardID != SystemRaftShardID {
			return nil, errors.New("unexpected shard")
		}
		readCalls++
		if readCalls == 1 {
			return nil, dragonboat.ErrShardNotReady
		}
		return state, nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		proposeCalls++
		if proposeCalls == 1 {
			return sm.Result{}, dragonboat.ErrShardNotReady
		}
		command, err := DecodeSystemCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		var result SystemApplyResult
		state, result = ApplySystemCommand(state, 1, command)
		encoded, err := json.Marshal(result)
		return sm.Result{Data: encoded}, err
	}
	runtime, err := fixture.open(t, host, RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	committed, err := runtime.BootstrapSystem(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !committed.Initialized || proposeCalls != 2 || readCalls < 3 {
		t.Fatalf("bootstrap result = %+v, reads = %d, proposals = %d", committed, readCalls, proposeCalls)
	}
}

func TestRuntimeDataShardInitializationRetriesUntilLeaderIsReady(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-1")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	host := newFakeNodeHost()
	state := DataState{}
	readCalls := 0
	proposeCalls := 0
	host.read = func(shardID uint64, _ any) (any, error) {
		if shardID != DataRaftShardID(0) {
			return nil, errors.New("unexpected shard")
		}
		readCalls++
		if readCalls == 1 {
			return nil, dragonboat.ErrShardNotReady
		}
		return state, nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		proposeCalls++
		if proposeCalls == 1 {
			return sm.Result{}, dragonboat.ErrShardNotReady
		}
		command, err := DecodeDataCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		result := ApplyDataCommand(&state, 1, command)
		encoded, err := json.Marshal(result)
		return sm.Result{Data: encoded}, err
	}
	runtime := &Runtime{
		config: RuntimeConfig{Tuning: DefaultRuntimeTuning()}, registryLayout: registryLayout,
		registryLayoutDigest: digest, nodeHost: host,
	}
	if err := runtime.initializeDataShard(context.Background(), LocalReplicaEnrollment{
		ShardID: DataRaftShardID(0), ReplicaID: 1, LocalState: ReplicaActive,
	}); err != nil {
		t.Fatal(err)
	}
	if !state.Initialized || proposeCalls != 2 || readCalls < 3 {
		t.Fatalf("data state = %+v, reads = %d, proposals = %d", state, readCalls, proposeCalls)
	}
}

func TestRuntimeRetainsDeepCloneOfVerifiedRegistryLayout(t *testing.T) {
	fixture := newRuntimeFixture(t)
	runtime, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	originalMember := runtime.registryLayout.Members[0]
	originalReplica := runtime.registryLayout.DataShards[0].Replicas[0]

	fixture.signed.RegistryLayout.Members[0].InternalEndpoint = "https://mutated:9443"
	fixture.signed.RegistryLayout.DataShards[0].Replicas[0] = ReplicaPlacement{
		MemberID: "registry-c", ReplicaID: 999,
	}
	if runtime.registryLayout.Members[0] != originalMember ||
		runtime.registryLayout.DataShards[0].Replicas[0] != originalReplica ||
		runtime.startupRegistryLayout.Members[0] != originalMember {
		t.Fatal("runtime retained caller-owned Registry Layout slices")
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

func TestRuntimeJoinRequiresNewPredecessorIdentity(t *testing.T) {
	makeTransition := func(t *testing.T, fixture runtimeFixture) SignedRegistryLayout {
		t.Helper()
		previous := fixture.signed.RegistryLayout
		previousDigest, err := previous.Digest()
		if err != nil {
			t.Fatal(err)
		}
		next := cloneRegistryLayout(previous)
		next.RegistryLayoutVersion = 2
		next.PreviousRegistryLayoutVersion = previous.RegistryLayoutVersion
		next.PreviousRegistryLayoutDigest = previousDigest
		next.Members = append(next.Members, RegistryMember{
			MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001",
		})
		replacement := []ReplicaPlacement{
			{MemberID: "registry-a", ReplicaID: 1},
			{MemberID: "registry-b", ReplicaID: 2},
			{MemberID: "registry-d", ReplicaID: 4},
		}
		next.SystemReplicas = append([]ReplicaPlacement(nil), replacement...)
		for index := range next.DataShards {
			next.DataShards[index].Replicas = append([]ReplicaPlacement(nil), replacement...)
		}
		signed, err := SignRegistryLayout(next, "root-1", fixture.key)
		if err != nil {
			t.Fatal(err)
		}
		return signed
	}

	t.Run("new member", func(t *testing.T) {
		fixture := newRuntimeFixture(t)
		fixture.config.MemberID = "registry-d"
		next := makeTransition(t, fixture)
		runtime, err := openRuntime(
			fixture.config, []SignedRegistryLayout{fixture.signed, next}, fixture.keyring,
			RuntimeOpenOptions{Mode: RuntimeJoin},
			func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
		)
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close()
		if runtime.enrollment.Mode != EnrollmentJoin {
			t.Fatalf("new member enrollment mode = %s", runtime.enrollment.Mode)
		}
	})

	t.Run("new member with durable predecessor guard", func(t *testing.T) {
		fixture := newRuntimeFixture(t)
		fixture.config.MemberID = "registry-d"
		next := makeTransition(t, fixture)
		previousDigest, err := fixture.signed.RegistryLayout.Digest()
		if err != nil {
			t.Fatal(err)
		}
		accepted, err := FirstAcceptedRegistryLayout(fixture.signed.RegistryLayout, previousDigest)
		if err != nil {
			t.Fatal(err)
		}
		if err := (RegistryLayoutGuard{Path: fixture.config.RegistryLayoutGuardPath}).store(accepted); err != nil {
			t.Fatal(err)
		}
		runtime, err := openRuntime(
			fixture.config, []SignedRegistryLayout{next}, fixture.keyring,
			RuntimeOpenOptions{Mode: RuntimeJoin},
			func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
		)
		if err != nil {
			t.Fatal(err)
		}
		defer runtime.Close()
		if runtime.enrollment.Mode != EnrollmentJoin {
			t.Fatalf("new member enrollment mode = %s", runtime.enrollment.Mode)
		}
	})

	t.Run("lost retained member", func(t *testing.T) {
		fixture := newRuntimeFixture(t)
		next := makeTransition(t, fixture)
		if _, err := openRuntime(
			fixture.config, []SignedRegistryLayout{fixture.signed, next}, fixture.keyring,
			RuntimeOpenOptions{Mode: RuntimeJoin},
			func(dbconfig.NodeHostConfig) (raftNodeHost, error) {
				t.Fatal("lost predecessor identity created a NodeHost")
				return nil, nil
			},
		); !errors.Is(err, ErrReplicaHistoryLost) {
			t.Fatalf("lost predecessor identity error = %v", err)
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

func TestRemovalPersistenceFailureImmediatelyFailsClosed(t *testing.T) {
	fixture := newRuntimeFixture(t)
	runtime, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	blockedParent := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockedParent, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	runtime.enrollmentStore.Path = filepath.Join(blockedParent, "enrollment.json")
	runtime.systemEvents.NodeDeleted(raftio.NodeInfo{ShardID: SystemRaftShardID, ReplicaID: 1})
	if _, err := runtime.ReadSystemLocal(); err == nil {
		t.Fatal("runtime served after failing to persist its removal fence")
	}
	if _, err := runtime.proposeSystem(context.Background(), SystemCommand{Type: SystemRefreshPermit}); err == nil {
		t.Fatal("runtime mutated after failing to persist its removal fence")
	}
}

func TestRuntimeRejectsSharedGuardAndEnrollmentFile(t *testing.T) {
	fixture := newRuntimeFixture(t)
	fixture.config.EnrollmentPath = fixture.config.RegistryLayoutGuardPath
	if _, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	}); err == nil {
		t.Fatal("shared registryLayout guard and enrollment path was accepted")
	}
}

func TestRemovedMemberRetainsTargetLayoutToCoordinateFullReplacement(t *testing.T) {
	fixture := newRuntimeFixture(t)
	host := newFakeNodeHost()
	runtime, err := fixture.open(t, host, RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.StartSystemReplica(); err != nil {
		t.Fatal(err)
	}
	runtime.Close()

	previous := fixture.signed.RegistryLayout
	previousDigest, _ := previous.Digest()
	next := cloneRegistryLayout(previous)
	next.RegistryLayoutVersion = 2
	next.PreviousRegistryLayoutVersion = 1
	next.PreviousRegistryLayoutDigest = previousDigest
	next.Members = []RegistryMember{
		{MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001"},
		{MemberID: "registry-e", InternalEndpoint: "https://registry-e:9443", RaftEndpoint: "registry-e:63001"},
		{MemberID: "registry-f", InternalEndpoint: "https://registry-f:9443", RaftEndpoint: "registry-f:63001"},
	}
	next.SystemReplicas = []ReplicaPlacement{{MemberID: "registry-d", ReplicaID: 11}, {MemberID: "registry-e", ReplicaID: 12}, {MemberID: "registry-f", ReplicaID: 13}}
	for index := range next.DataShards {
		next.DataShards[index].Replicas = append([]ReplicaPlacement(nil), next.SystemReplicas...)
	}
	signedNext, err := SignRegistryLayout(next, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := openRuntime(
		fixture.config, []SignedRegistryLayout{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return host, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	nextDigest, _ := next.Digest()
	if restarted.registryLayoutDigest != nextDigest || restarted.member.MemberID != "registry-a" ||
		restarted.startupRegistryLayout.RegistryLayoutVersion != 1 {
		t.Fatalf("target-aware removed-member runtime = target %s member %s startup %d",
			restarted.registryLayoutDigest, restarted.member.MemberID, restarted.startupRegistryLayout.RegistryLayoutVersion)
	}
	active, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &previous, Digest: previousDigest,
	})
	if err := restarted.authorizeRegistryLayoutState(active); err != nil {
		t.Fatalf("removed member cannot coordinate the verified target transition: %v", err)
	}
}

func TestRuntimeKeepsEnrolledReplicaUntilLocalRemovalIsDurable(t *testing.T) {
	registryLayout := transitionRegistryLayout(t)
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	member, found := registryLayoutMember(registryLayout, "registry-c")
	if !found {
		t.Fatal("removed replica member absent from registryLayout member catalog")
	}
	host := newFakeNodeHost()
	host.history[[2]uint64{SystemRaftShardID, 3}] = true
	host.history[[2]uint64{DataRaftShardID(0), 3}] = true
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, member: member, nodeHost: host,
		enrollment: LocalEnrollment{RegistryLayoutVersion: registryLayout.RegistryLayoutVersion, RegistryLayoutDigest: digest, Replicas: []LocalReplicaEnrollment{
			{ShardID: SystemRaftShardID, ReplicaID: 3, LocalState: ReplicaActive},
			{ShardID: DataRaftShardID(0), ReplicaID: 3, LocalState: ReplicaActive},
		}},
	}
	if err := runtime.StartSystemReplica(); err != nil {
		t.Fatal(err)
	}
	system, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("System bootstrap = %+v", result)
	}
	host.read = func(uint64, any) (any, error) { return system, nil }
	if err := runtime.StartDataReplicas(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(host.starts) != 2 {
		t.Fatalf("enrolled old replicas were not kept alive: %+v", host.starts)
	}
	runtime.enrollment.Replicas[0].LocalState = ReplicaRemoving
	runtime.enrollment.Replicas[1].LocalState = ReplicaRemoved
	runtime.systemClient = &testRemoteSystemClient{state: system}
	if err := runtime.StartSystemReplica(); !errors.Is(err, ErrNoLocalReplica) {
		t.Fatalf("removing System replica start error = %v", err)
	}
	if err := runtime.StartDataReplicas(context.Background()); err != nil {
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

	firstDigest, err := fixture.signed.RegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	successorSecret := []byte("bootstrap-generation-2")
	successor := testRegistryLayout(2, "generation-2")
	successor.BootstrapTokenDigest = digestFor(string(successorSecret))
	successor, _ = finalizeConsensusSuccessor(t, fixture.signed.RegistryLayout, firstDigest, successor)
	signedSuccessor, err := SignRegistryLayout(successor, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	successorConfig := fixture.config
	root := filepath.Dir(fixture.config.NodeHostDir)
	successorConfig.NodeHostDir = filepath.Join(root, "nodehost-generation-2")
	successorConfig.WALDir = filepath.Join(root, "wal-generation-2")
	successorConfig.StateEngineDir = filepath.Join(root, "state-generation-2")
	if _, err := openRuntime(
		successorConfig, []SignedRegistryLayout{signedSuccessor}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: []byte("wrong-successor-secret")},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) {
			t.Fatal("unauthorized rollover created a NodeHost")
			return nil, nil
		},
	); !errors.Is(err, ErrBootstrapUnauthorized) {
		t.Fatalf("unauthorized successor error = %v", err)
	}
	guarded, err := (RegistryLayoutGuard{Path: fixture.config.RegistryLayoutGuardPath}).Load()
	if err != nil || guarded == nil || guarded.RegistryGeneration != fixture.signed.RegistryLayout.RegistryGeneration ||
		guarded.RegistryLayoutDigest != firstDigest {
		t.Fatalf("failed successor advanced registryLayout guard = %+v, %v", guarded, err)
	}
	persistedEnrollment, err := (EnrollmentStore{Path: fixture.config.EnrollmentPath}).Load()
	if err != nil || persistedEnrollment == nil ||
		persistedEnrollment.RegistryGeneration != fixture.signed.RegistryLayout.RegistryGeneration {
		t.Fatalf("failed successor replaced source enrollment = %+v, %v", persistedEnrollment, err)
	}

	successorRuntime, err := openRuntime(
		successorConfig, []SignedRegistryLayout{signedSuccessor}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeBootstrap, BootstrapSecret: successorSecret},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer successorRuntime.Close()
	if successorRuntime.enrollment.RegistryGeneration != "generation-2" ||
		successorRuntime.enrollment.Mode != EnrollmentBootstrap {
		t.Fatalf("successor enrollment = %+v", successorRuntime.enrollment)
	}
	if _, err := openRuntime(
		fixture.config, []SignedRegistryLayout{fixture.signed}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	); err == nil {
		t.Fatal("accepted registryLayout guard rolled back to the predecessor generation")
	}
}

func TestRuntimeAdvancesEnrollmentRegistryLayoutOnlyAfterConsensusActivation(t *testing.T) {
	fixture := newRuntimeFixture(t)
	first, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	firstDigest, err := fixture.signed.RegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next := CloneRegistryLayout(fixture.signed.RegistryLayout)
	next.RegistryLayoutVersion = 2
	next.PreviousRegistryLayoutVersion = 1
	next.PreviousRegistryLayoutDigest = firstDigest
	signedNext, err := SignRegistryLayout(next, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	restartHost := newFakeNodeHost()
	restarted, err := openRuntime(
		fixture.config, []SignedRegistryLayout{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return restartHost, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.enrollment.RegistryLayoutVersion != fixture.signed.RegistryLayout.RegistryLayoutVersion ||
		restarted.enrollment.RegistryLayoutDigest != firstDigest {
		t.Fatalf("published next registryLayout advanced enrollment before consensus: %+v", restarted.enrollment)
	}
	nextDigest, err := next.Digest()
	if err != nil {
		t.Fatal(err)
	}
	system, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &fixture.signed.RegistryLayout, Digest: firstDigest,
	})
	shards := []ShardTransition{{ShardID: ^uint32(0), Stage: TransitionPending}}
	for shardID := uint32(0); shardID < next.VirtualShardCount; shardID++ {
		shards = append(shards, ShardTransition{ShardID: shardID, Stage: TransitionPending})
	}
	system, _ = applySystem(t, system, 2, SystemCommand{
		Type: SystemBeginTransition,
		Transition: &RegistryLayoutTransition{
			Version: next.RegistryLayoutVersion, Digest: nextDigest, PreviousDigest: firstDigest,
			NextSystemEpoch: 2, Shards: shards,
		},
	})
	index := uint64(3)
	for _, shard := range shards {
		for _, edge := range [][2]TransitionStage{
			{TransitionPending, TransitionCatchingUp},
			{TransitionCatchingUp, TransitionPromoted},
			{TransitionPromoted, TransitionComplete},
		} {
			system, _ = applySystem(t, system, index, SystemCommand{
				Type:    SystemAdvanceTransition,
				Advance: &TransitionAdvance{ShardID: shard.ShardID, From: edge[0], To: edge[1]},
			})
			index++
		}
	}
	system, _ = applySystem(t, system, index, SystemCommand{Type: SystemActivateTransition})
	restartHost.read = func(uint64, any) (any, error) { return system, nil }
	if err := restarted.SyncLocalRegistryLayout(context.Background()); err != nil {
		t.Fatal(err)
	}
	if restarted.enrollment.RegistryLayoutVersion != next.RegistryLayoutVersion ||
		restarted.enrollment.RegistryLayoutDigest != nextDigest {
		t.Fatalf("activated registryLayout was not persisted: %+v", restarted.enrollment)
	}
	restarted.Close()
	if _, err := openRuntime(
		fixture.config, []SignedRegistryLayout{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	); err != nil {
		t.Fatalf("full registryLayout chain was not restart-idempotent: %v", err)
	}
}

func TestRuntimeCommitsPublishedRegistryLayoutBeforeStartingTargetReplicas(t *testing.T) {
	previous := testRegistryLayout(2, "generation-1")
	previousDigest, err := previous.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next := CloneRegistryLayout(previous)
	next.RegistryLayoutVersion = 2
	next.PreviousRegistryLayoutVersion = 1
	next.PreviousRegistryLayoutDigest = previousDigest
	nextDigest, err := next.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &previous, Digest: previousDigest,
	})
	if result.Conflict || !result.Applied {
		t.Fatalf("bootstrap = %+v", result)
	}
	host := newFakeNodeHost()
	host.read = func(shardID uint64, _ any) (any, error) {
		if shardID != SystemRaftShardID {
			return nil, ErrNoLocalReplica
		}
		return cloneSystemState(state), nil
	}
	host.propose = func(raw []byte) (sm.Result, error) {
		command, decodeErr := DecodeSystemCommand(raw)
		if decodeErr != nil {
			return sm.Result{}, decodeErr
		}
		var applied SystemApplyResult
		state, applied = ApplySystemCommand(state, state.LastApplied+1, command)
		encoded, encodeErr := json.Marshal(applied)
		return sm.Result{Data: encoded}, encodeErr
	}
	runtime := &Runtime{
		config:         RuntimeConfig{Tuning: DefaultRuntimeTuning()},
		registryLayout: next, registryLayoutDigest: nextDigest, nodeHost: host,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	committed, err := runtime.AwaitOrBeginRegistryLayoutTransition(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if committed.Transition == nil || committed.Transition.Digest != nextDigest ||
		committed.Transition.PreviousDigest != previousDigest ||
		len(committed.Transition.Shards) != int(next.VirtualShardCount)+1 {
		t.Fatalf("committed transition = %+v", committed.Transition)
	}
}

func TestRemovedMemberRetainsStartupLayoutUntilLocalReplicasAreRemoved(t *testing.T) {
	fixture := newRuntimeFixture(t)
	runtime, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	previousVersion := runtime.enrollment.RegistryLayoutVersion
	next := cloneRegistryLayout(runtime.registryLayout)
	next.RegistryLayoutVersion++
	next.Members = slices.DeleteFunc(next.Members, func(member RegistryMember) bool {
		return member.MemberID == runtime.member.MemberID
	})
	nextDigest := digestFor("removed-member-layout")
	runtime.registryLayout = next
	runtime.registryLayoutDigest = nextDigest

	system, _ := applySystem(t, SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &fixture.signed.RegistryLayout,
		Digest: runtime.enrollment.RegistryLayoutDigest,
	})
	system.ActiveRegistryLayoutVersion = next.RegistryLayoutVersion
	system.ActiveRegistryLayoutDigest = nextDigest
	if err := runtime.syncLocalRegistryLayout(system); err != nil {
		t.Fatal(err)
	}
	if runtime.enrollment.RegistryLayoutVersion != previousVersion {
		t.Fatalf("removed member advanced before cleanup: %+v", runtime.enrollment)
	}

	for index := range runtime.enrollment.Replicas {
		runtime.enrollment.Replicas[index].LocalState = ReplicaRemoved
	}
	if err := runtime.syncLocalRegistryLayout(system); err != nil {
		t.Fatal(err)
	}
	if runtime.enrollment.RegistryLayoutVersion != next.RegistryLayoutVersion ||
		runtime.enrollment.RegistryLayoutDigest != nextDigest {
		t.Fatalf("removed member did not finalize its Layout fence after cleanup: %+v", runtime.enrollment)
	}
}

func TestRuntimeRejectsRegistryLayoutJumpPastEnrollment(t *testing.T) {
	fixture := newRuntimeFixture(t)
	first, err := fixture.open(t, newFakeNodeHost(), RuntimeOpenOptions{
		Mode: RuntimeBootstrap, BootstrapSecret: fixture.secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	firstDigest, err := fixture.signed.RegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	second := fixture.signed.RegistryLayout
	second.RegistryLayoutVersion = 2
	second.PreviousRegistryLayoutVersion = 1
	second.PreviousRegistryLayoutDigest = firstDigest
	second.Members = append([]RegistryMember(nil), second.Members...)
	second.Members[0].InternalEndpoint = "https://registry-a-v2:9443"
	secondDigest, err := second.Digest()
	if err != nil {
		t.Fatal(err)
	}
	third := second
	third.RegistryLayoutVersion = 3
	third.PreviousRegistryLayoutVersion = 2
	third.PreviousRegistryLayoutDigest = secondDigest
	third.Members = append([]RegistryMember(nil), third.Members...)
	third.Members[0].InternalEndpoint = "https://registry-a-v3:9443"
	signedSecond, err := SignRegistryLayout(second, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	signedThird, err := SignRegistryLayout(third, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openRuntime(
		fixture.config, []SignedRegistryLayout{fixture.signed, signedSecond, signedThird}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) {
			t.Fatal("registryLayout jump created a NodeHost")
			return nil, nil
		},
	); err == nil {
		t.Fatal("enrolled v1 member accepted a v3 runtime target")
	}
	guarded, err := (RegistryLayoutGuard{Path: fixture.config.RegistryLayoutGuardPath}).Load()
	if err != nil || guarded == nil || guarded.RegistryLayoutVersion != 1 || guarded.RegistryLayoutDigest != firstDigest {
		t.Fatalf("rejected jump advanced registryLayout guard = %+v, %v", guarded, err)
	}
}

func TestDataShardBootstrapResolvesCommittedProposalAfterCallerCancellation(t *testing.T) {
	registryLayout := testRegistryLayout(1, "generation-1")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	data := DataState{}
	host := newFakeNodeHost()
	ctx, cancel := context.WithCancel(context.Background())
	host.readCtx = func(ctx context.Context, shardID uint64, query any) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if shardID != DataRaftShardID(0) {
			return nil, ErrNoLocalReplica
		}
		if _, ok := query.(DataStateLookup); !ok {
			return nil, errors.New("unexpected data lookup")
		}
		return cloneDataStateForLookup(data), nil
	}
	host.proposeCtx = func(ctx context.Context, raw []byte) (sm.Result, error) {
		command, err := DecodeDataCommand(raw)
		if err != nil {
			return sm.Result{}, err
		}
		result := ApplyDataCommand(&data, 1, command)
		if !result.Applied || result.Conflict {
			t.Fatalf("bootstrap apply = %+v", result)
		}
		cancel()
		return sm.Result{}, ctx.Err()
	}
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	replica := LocalReplicaEnrollment{ShardID: DataRaftShardID(0), ReplicaID: 1, LocalState: ReplicaActive}
	if err := runtime.initializeDataShard(ctx, replica); err != nil {
		t.Fatalf("resolve committed bootstrap: %v", err)
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
	firstDigest, err := fixture.signed.RegistryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	next := fixture.signed.RegistryLayout
	next.RegistryLayoutVersion = 2
	next.PreviousRegistryLayoutVersion = 1
	next.PreviousRegistryLayoutDigest = firstDigest
	next.Members = []RegistryMember{
		fixture.signed.RegistryLayout.Members[0], fixture.signed.RegistryLayout.Members[1],
		{MemberID: "registry-d", InternalEndpoint: "https://registry-d:9443", RaftEndpoint: "registry-d:63001"},
	}
	desired := []ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-d", ReplicaID: 4},
	}
	next.SystemReplicas = append([]ReplicaPlacement(nil), desired...)
	next.DataShards = append([]ShardPlacement(nil), fixture.signed.RegistryLayout.DataShards...)
	for index := range next.DataShards {
		next.DataShards[index].Replicas = append([]ReplicaPlacement(nil), desired...)
	}
	signedNext, err := SignRegistryLayout(next, "root-1", fixture.key)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := openRuntime(
		fixture.config, []SignedRegistryLayout{fixture.signed, signedNext}, fixture.keyring,
		RuntimeOpenOptions{Mode: RuntimeRestart},
		func(dbconfig.NodeHostConfig) (raftNodeHost, error) { return newFakeNodeHost(), nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	nextDigest, err := next.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if restarted.registryLayoutDigest != nextDigest || restarted.member.MemberID != "registry-c" ||
		restarted.startupRegistryLayout.RegistryLayoutVersion != 1 {
		t.Fatalf("removed member selected wrong runtime registryLayout: digest=%s member=%s",
			restarted.registryLayoutDigest, restarted.member.MemberID)
	}
	accepted, err := (RegistryLayoutGuard{Path: fixture.config.RegistryLayoutGuardPath}).Load()
	if err != nil || accepted == nil || accepted.RegistryLayoutDigest != nextDigest {
		t.Fatalf("anti-rollback guard did not retain next artifact: %+v, %v", accepted, err)
	}
}

func TestDataShardBootstrapCommandDoesNotEmbedWholeRegistryLayout(t *testing.T) {
	registryLayout := testRegistryLayout(DefaultVirtualShards, "generation-1")
	bootstrap, err := NewDataShardBootstrap(registryLayout, DefaultVirtualShards-1)
	if err != nil {
		t.Fatal(err)
	}
	command, err := EncodeDataCommand(DataCommand{
		Type: DataInitializeShard,
		Identity: ShardRequestIdentity{PermitIdentity: PermitIdentity{
			ClusterID: registryLayout.ClusterID, RegistryGeneration: registryLayout.RegistryGeneration,
			SystemEpoch: 1, RegistryLayoutDigest: bootstrap.RegistryLayoutDigest,
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
	predecessor := testRegistryLayout(2, "generation-1")
	predecessor.ServePermitMaxMillis = 25
	predecessorDigest, err := predecessor.Digest()
	if err != nil {
		t.Fatal(err)
	}
	registryLayout := testRegistryLayout(2, "generation-2")
	registryLayout.ServePermitMaxMillis = 50
	registryLayout, _ = finalizeConsensusSuccessor(t, predecessor, predecessorDigest, registryLayout)
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
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
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}
	if _, err := runtime.ApplySystem(context.Background(), SystemCommand{
		Type: SystemConfirmDrain, Drain: &DrainConfirmation{
			PredecessorProofDigest: registryLayout.Predecessor.ProofDigest,
			WaitedMillis:           registryLayout.Predecessor.ServePermitMaxMillis, EvidenceDigest: digestFor("evidence"),
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
		{Type: SystemConfirmRecoveryDrain, RecoveryDrain: &RecoveryDrainConfirmation{}},
		{Type: SystemAdvanceRecovery, RecoveryAdvance: &RecoveryAdvance{}},
	}
	for _, command := range commands {
		if _, err := runtime.ApplySystem(context.Background(), command); err == nil {
			t.Fatalf("generic System operation accepted %s", command.Type)
		}
	}
}

func TestRuntimeClosesGenerationOnlyForCompleteSuccessorIntent(t *testing.T) {
	registryLayout := testRegistryLayout(2, "generation-1")
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	state, result := ApplySystemCommand(SystemState{}, 1, SystemCommand{
		Type: SystemBootstrap, RegistryLayout: &registryLayout, Digest: digest,
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
	runtime := &Runtime{
		registryLayout: registryLayout, registryLayoutDigest: digest, nodeHost: host,
		enrollment: LocalEnrollment{Replicas: []LocalReplicaEnrollment{{
			ShardID: SystemRaftShardID, ReplicaID: 1, StartPlan: ReplicaInitial, LocalState: ReplicaActive,
		}}},
	}

	successor := testRegistryLayout(2, "generation-2")
	successor.Predecessor = &PredecessorProof{
		RegistryGeneration: registryLayout.RegistryGeneration, RegistryLayoutDigest: digest,
		ServePermitMaxMillis: registryLayout.ServePermitMaxMillis, Kind: RolloverConsensusClosure,
	}
	if _, err := runtime.ApplySystem(context.Background(), SystemCommand{
		Type: SystemCloseRegistryGeneration,
		Closure: &RegistryGenerationClosure{
			TargetRegistryGeneration:         successor.RegistryGeneration,
			TargetRegistryLayoutIntentDigest: digestFor("unverified-intent"), Kind: RolloverConsensusClosure,
		},
	}); err == nil {
		t.Fatal("generic System operation bypassed successor-registryLayout validation")
	}
	closed, err := runtime.CloseRegistryGeneration(context.Background(), successor)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := closed.ConsensusPredecessorProof()
	if err != nil {
		t.Fatal(err)
	}
	successor.Predecessor = &proof
	if err := successor.Validate(); err != nil {
		t.Fatalf("final successor registryLayout = %v", err)
	}
	if replayed, err := runtime.CloseRegistryGeneration(context.Background(), successor); err != nil ||
		replayed.Closure == nil || replayed.Closure.ProofDigest != proof.ProofDigest {
		t.Fatalf("idempotent close = %+v, %v", replayed, err)
	}

	different := successor
	different.Members = append([]RegistryMember(nil), successor.Members...)
	different.Members[0].InternalEndpoint = "https://registry-a-other:9443"
	if _, err := runtime.CloseRegistryGeneration(context.Background(), different); err == nil {
		t.Fatal("retired generation authorized a different successor intent")
	}
}
