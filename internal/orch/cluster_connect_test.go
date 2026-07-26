package orch

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	"github.com/kuasar-sandbox/orchestrator/internal/vswitch"
)

func TestHandleClusterConnectExistingTargetIgnoresWithinLimitMigrationToken(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("6", 64)
	apiSecret := deriveTestAPISecret(t, manifestKey)
	fingerprint, err := store.APISecretHash(apiSecret)
	if err != nil {
		t.Fatal(err)
	}
	sb := &types.Sandbox{
		ID:                 "stable-g1",
		Profile:            types.ProfileBare,
		Cluster:            &types.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a"},
		AuthSandboxIDValue: "stable",
		TemplateID:         "bare-img-" + strings.Repeat("a", 64),
		State:              types.StateRunning,
		APISecret:          apiSecret,
		ManifestKey:        manifestKey,
		CreatedUnix:        1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	// Keep the asynchronous resume check on the in-memory running row so it is a
	// no-op and cannot race test cleanup through a launcher or the store.
	o.cache(sb)

	for name, token := range map[string]string{
		"malformed":             "not-a-kmt1-token",
		"exact limit malformed": strings.Repeat("!", migrationtoken.MaxWireSize),
	} {
		t.Run(name, func(t *testing.T) {
			ack := o.HandleCommand(ctx, &routesync.Command{
				CmdID: "connect-" + name, Kind: routesync.CmdConnect, SID: sb.ID,
				Profile: string(sb.Profile), APISecretFingerprint: fingerprint,
				Cluster: &routesync.ClusterSandboxContext{
					Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable",
				},
				MigrationToken: token,
			})
			if ack.Status != routesync.AckAccepted {
				t.Fatalf("existing-target connect ack = %+v", ack)
			}
		})
	}

	ack := o.HandleCommand(ctx, &routesync.Command{
		CmdID: "connect-oversized", Kind: routesync.CmdConnect, SID: sb.ID,
		Profile: string(sb.Profile), APISecretFingerprint: fingerprint,
		Cluster: &routesync.ClusterSandboxContext{
			Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable",
		},
		MigrationToken: strings.Repeat("!", migrationtoken.MaxWireSize+1),
	})
	if ack.Status != routesync.AckRejected || ack.Reason != migrationtoken.ErrTokenTooLarge.Error() {
		t.Fatalf("oversized existing-target connect ack = %+v", ack)
	}
}

func TestHandleClusterConnectMissingTargetRequiresMigrationToken(t *testing.T) {
	o := testOrch(t)
	ack := o.HandleCommand(context.Background(), &routesync.Command{
		CmdID: "connect-missing", Kind: routesync.CmdConnect, SID: "stable-g1",
		Profile: string(types.ProfileBare), APISecretFingerprint: strings.Repeat("a", 64),
		Cluster: &routesync.ClusterSandboxContext{
			Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable",
		},
	})
	if ack.Status != routesync.AckRejected || !strings.Contains(ack.Reason, "sandbox not found") {
		t.Fatalf("missing-target connect ack = %+v", ack)
	}
	if sb, err := o.st.Get(context.Background(), "stable-g1"); err != nil || sb != nil {
		t.Fatalf("missing-target connect inserted a row: sandbox=%+v err=%v", sb, err)
	}
}

func TestHandleClusterConnectImportsBeforeAckAndResumesAsynchronously(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	blocker := &blockingClusterConnectVS{
		started:  make(chan struct{}),
		returned: make(chan struct{}),
	}
	fixture.o.vs = blocker
	asyncCtx, cancel := context.WithCancel(context.Background())
	fixture.o.SetClusterContext(asyncCtx)
	t.Cleanup(func() {
		cancel()
		select {
		case <-blocker.returned:
		case <-time.After(2 * time.Second):
			t.Error("asynchronous cluster resume did not stop")
		}
	})

	cmd := fixture.command("stable-g1", fixture.token)
	ack := fixture.o.HandleCommand(context.Background(), cmd)
	if ack.Status != routesync.AckAccepted {
		t.Fatalf("cluster migration connect ack = %+v", ack)
	}

	// HandleCommand may return Accepted only after KMT authentication and the
	// insert-only paused-row write have completed. The blocked Attach prevents the
	// asynchronous resume from changing the persisted state during this check.
	got, err := fixture.o.st.Get(context.Background(), cmd.SID)
	if err != nil || got == nil {
		t.Fatalf("imported row is not visible after Ack: sandbox=%+v err=%v", got, err)
	}
	if got.ID != "stable-g1" || got.State != types.StatePaused || got.Profile != fixture.source.Profile ||
		got.AuthSandboxID() != fixture.source.AuthSandboxID() || got.CreatedUnix != fixture.source.CreatedUnix {
		t.Fatalf("imported identity/state = %+v", got)
	}
	if got.Cluster == nil || got.Cluster.Group != cmd.Cluster.Group || got.Cluster.RouteKey != cmd.Cluster.RouteKey {
		t.Fatalf("imported cluster context = %+v, command = %+v", got.Cluster, cmd.Cluster)
	}
	if got.APISecret != fixture.pair.APISecret || got.ManifestKey != fixture.pair.ManifestKey {
		t.Fatal("import did not use the target node key row for raw credential roots")
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(got), sandboxCredentials(fixture.source))

	select {
	case <-blocker.started:
		// Reaching the blocked attach proves resume was scheduled after the
		// synchronous import instead of being included in the Ack path.
	case <-time.After(2 * time.Second):
		t.Fatal("accepted cluster connect did not schedule asynchronous resume")
	}
	cancel()
	select {
	case <-blocker.returned:
	case <-time.After(2 * time.Second):
		t.Fatal("asynchronous cluster resume did not observe cancellation")
	}
}

func TestPrepareClusterConnectRejectsTokenMismatchBeforeInsert(t *testing.T) {
	for name, mutate := range map[string]func(*testing.T, *clusterConnectFixture, *routesync.Command) error{
		"profile": func(_ *testing.T, _ *clusterConnectFixture, cmd *routesync.Command) error {
			cmd.Profile = string(types.ProfileBare)
			return migrationtoken.ErrIncompatible
		},
		"authentication subject": func(_ *testing.T, _ *clusterConnectFixture, cmd *routesync.Command) error {
			cmd.Cluster.AuthSandboxID = "other-stable"
			return migrationtoken.ErrIncompatible
		},
		"API secret fingerprint": func(t *testing.T, fixture *clusterConnectFixture, cmd *routesync.Command) error {
			alternate := store.KeyPair{APISecret: strings.Repeat("1", 64), ManifestKey: fixture.pair.ManifestKey}
			if _, err := fixture.o.st.AddKeyPair(context.Background(), alternate, "", 0, ""); err != nil {
				t.Fatal(err)
			}
			fingerprint, err := store.APISecretHash(alternate.APISecret)
			if err != nil {
				t.Fatal(err)
			}
			cmd.APISecretFingerprint = fingerprint
			return migrationtoken.ErrCredentialMismatch
		},
		"runtime": func(t *testing.T, fixture *clusterConnectFixture, _ *routesync.Command) error {
			if err := os.WriteFile(fixture.o.runtimeFileFor(types.ProfileE2B), []byte("different-runtime"), 0o644); err != nil {
				t.Fatal(err)
			}
			return migrationtoken.ErrIncompatible
		},
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newClusterConnectFixture(t)
			cmd := fixture.command("stable-g1", fixture.token)
			wantErr := mutate(t, fixture, cmd)
			if _, err := fixture.o.prepareClusterConnect(context.Background(), cmd); !errors.Is(err, wantErr) {
				t.Fatalf("prepareClusterConnect error = %v, want %v", err, wantErr)
			}
			if sb, err := fixture.o.st.Get(context.Background(), cmd.SID); err != nil || sb != nil {
				t.Fatalf("mismatched token inserted target: sandbox=%+v err=%v", sb, err)
			}
		})
	}
}

func TestPrepareClusterConnectConcurrentTargetIsInsertOnly(t *testing.T) {
	fixture := newClusterConnectFixture(t)
	sources := []*types.Sandbox{fixture.source, cloneClusterMigrationSource(fixture.source)}
	sources[0].CreatedUnix = 101
	sources[0].Metadata = map[string]string{"candidate": "first"}
	if err := materializeSandboxCredentials(sources[0], sandboxcfg.Credentials{
		ServiceSecret: strings.Repeat("1", 64), EnvdAccessToken: "envd-first", TrafficAccessToken: "traffic-first",
	}); err != nil {
		t.Fatal(err)
	}
	sources[1].CreatedUnix = 202
	sources[1].Metadata = map[string]string{"candidate": "second"}
	if err := materializeSandboxCredentials(sources[1], sandboxcfg.Credentials{
		ServiceSecret: strings.Repeat("2", 64), EnvdAccessToken: "envd-second", TrafficAccessToken: "traffic-second",
	}); err != nil {
		t.Fatal(err)
	}
	tokens := make([]string, len(sources))
	for i, source := range sources {
		var err error
		tokens[i], err = fixture.o.mintSandboxToken(source, source.SnapshotRef)
		if err != nil {
			t.Fatal(err)
		}
	}

	type result struct {
		sandbox *types.Sandbox
		err     error
	}
	results := make(chan result, len(tokens))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, token := range tokens {
		wg.Add(1)
		go func(i int, token string) {
			defer wg.Done()
			<-start
			sb, err := fixture.o.prepareClusterConnect(context.Background(), fixture.command("stable-g1", token))
			results <- result{sandbox: sb, err: err}
		}(i, token)
	}
	close(start)
	wg.Wait()
	close(results)

	stored, err := fixture.o.st.Get(context.Background(), "stable-g1")
	if err != nil || stored == nil {
		t.Fatalf("concurrent import target = %+v, %v", stored, err)
	}
	storedSignature := clusterMigrationSignatureOf(stored)
	wantFirst := clusterMigrationSignatureOf(sources[0])
	wantSecond := clusterMigrationSignatureOf(sources[1])
	if !reflect.DeepEqual(storedSignature, wantFirst) && !reflect.DeepEqual(storedSignature, wantSecond) {
		t.Fatalf("concurrent imports produced a mixed/unknown row: %+v", storedSignature)
	}
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent prepareClusterConnect: %v", got.err)
		}
		if !reflect.DeepEqual(clusterMigrationSignatureOf(got.sandbox), storedSignature) {
			t.Fatal("a concurrent same-target connect returned a row that was later overwritten")
		}
	}
}

func TestHandleClusterConnectRejectsInvalidTargetID(t *testing.T) {
	for _, sid := range []string{"", "UPPER-g1", "stable/g1", "-stable-g1", strings.Repeat("a", 58)} {
		t.Run(sid, func(t *testing.T) {
			o := testOrch(t)
			ack := o.HandleCommand(context.Background(), &routesync.Command{
				CmdID: "connect-invalid", Kind: routesync.CmdConnect, SID: sid,
				Profile: string(types.ProfileE2B), APISecretFingerprint: strings.Repeat("a", 64),
				Cluster:        &routesync.ClusterSandboxContext{Group: "group-a", RouteKey: "route-a", AuthSandboxID: "stable"},
				MigrationToken: "kmt1.invalid",
			})
			if ack.Status != routesync.AckRejected || !strings.Contains(ack.Reason, "valid sandbox id") {
				t.Fatalf("invalid SID %q ack = %+v", sid, ack)
			}
		})
	}
}

type clusterConnectFixture struct {
	o           *Orchestrator
	pair        store.KeyPair
	fingerprint string
	source      *types.Sandbox
	token       string
}

func newClusterConnectFixture(t *testing.T) *clusterConnectFixture {
	t.Helper()
	dir := t.TempDir()
	o := migrationOrchestrator(t, dir, []byte("cluster-runtime"))
	// migrationOrchestrator intentionally builds a minimal Config without loading
	// defaults; provide the one network value needed for the async-resume probe to
	// reach the blocking vswitch instead of failing CIDR validation first.
	o.cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	pair := store.KeyPair{
		ManifestKey: strings.Repeat("6", 64),
	}
	pair.APISecret = deriveTestAPISecret(t, pair.ManifestKey)
	if _, err := o.st.AddKeyPair(context.Background(), pair, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	fingerprint, err := store.APISecretHash(pair.APISecret)
	if err != nil {
		t.Fatal(err)
	}
	source := migrationSandbox(t, dir, "stable-g0", pair.ManifestKey, "manifest://"+strings.Repeat("b", 64))
	source.AuthSandboxIDValue = "stable"
	source.CreatedUnix = 1_700_000_000
	source.DeadlineUnix = 1_900_000_000
	source.Env = map[string]string{"SOURCE": "stable-g0"}
	source.Metadata = map[string]string{"portable": "yes"}
	if err := materializeSandboxCredentials(source, sandboxcfg.Credentials{
		ServiceSecret:   strings.Repeat("3", 64),
		EnvdAccessToken: "source-envd", TrafficAccessToken: "source-traffic",
	}); err != nil {
		t.Fatal(err)
	}
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}
	return &clusterConnectFixture{o: o, pair: pair, fingerprint: fingerprint, source: source, token: token}
}

func (f *clusterConnectFixture) command(targetID, token string) *routesync.Command {
	return &routesync.Command{
		CmdID: "connect-" + targetID, Kind: routesync.CmdConnect, SID: targetID,
		Profile: string(f.source.Profile), APISecretFingerprint: f.fingerprint,
		Cluster: &routesync.ClusterSandboxContext{
			Group: "/tenant/workloads", RouteKey: "route-stable", AuthSandboxID: f.source.AuthSandboxID(),
		},
		MigrationToken: token,
	}
}

func cloneClusterMigrationSource(source *types.Sandbox) *types.Sandbox {
	clone := *source
	clone.Env = copyClusterConnectStringMap(source.Env)
	clone.Metadata = copyClusterConnectStringMap(source.Metadata)
	return &clone
}

func copyClusterConnectStringMap(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

type clusterMigrationSignature struct {
	Created     int64
	Metadata    map[string]string
	Credentials migrationCredentials
}

func clusterMigrationSignatureOf(sb *types.Sandbox) clusterMigrationSignature {
	if sb == nil {
		return clusterMigrationSignature{}
	}
	return clusterMigrationSignature{
		Created: sb.CreatedUnix, Metadata: sb.Metadata, Credentials: sandboxCredentials(sb),
	}
}

type blockingClusterConnectVS struct {
	started  chan struct{}
	returned chan struct{}
	once     sync.Once
}

func (v *blockingClusterConnectVS) Attach(ctx context.Context, _ vswitch.AttachReq) (*vswitch.Port, error) {
	v.once.Do(func() { close(v.started) })
	<-ctx.Done()
	close(v.returned)
	return nil, ctx.Err()
}

func (*blockingClusterConnectVS) Detach(context.Context, string) error { return nil }
func (*blockingClusterConnectVS) TapFD(string) vswitch.TapFD           { return vswitch.TapFD{} }
