package raftstore

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"path/filepath"
	"testing"

	dragonboat "github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/client"
	dbconfig "github.com/lni/dragonboat/v4/config"
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
}

func newFakeNodeHost() *fakeNodeHost {
	return &fakeNodeHost{
		history: make(map[[2]uint64]bool), createOnStart: true,
		memberships: make(map[uint64]*dragonboat.Membership),
	}
}

func (f *fakeNodeHost) StartReplica(
	initial map[uint64]dragonboat.Target,
	join bool,
	_ sm.CreateStateMachineFunc,
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

func (f *fakeNodeHost) SyncPropose(context.Context, *client.Session, []byte) (sm.Result, error) {
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

func (f *fakeNodeHost) StopReplica(uint64, uint64) error { return nil }

func (f *fakeNodeHost) SyncRemoveData(context.Context, uint64, uint64) error { return nil }

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
		WALDir: filepath.Join(root, "wal"), ManifestGuardPath: filepath.Join(root, "identity", "manifest.json"),
		EnrollmentPath:  filepath.Join(root, "identity", "enrollment.json"),
		TLS:             RaftTLS{CAFile: "ca.pem", CertFile: "cert.pem", KeyFile: "key.pem"},
		Tuning:          DefaultRuntimeTuning(),
		StorageAttestor: StorageAttestorFunc(func(...string) error { return nil }),
	}
	return runtimeFixture{config: config, signed: signed, keyring: map[string]ed25519.PublicKey{"root-1": publicKey}, secret: secret}
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
