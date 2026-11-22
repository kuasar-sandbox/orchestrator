package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
)

func staleNodeProfile(t *testing.T, reg *Registry, nodeID string) metadata.NodeSnapshot {
	t.Helper()
	ctx := context.Background()
	profile, err := reg.stores.nodes.ReadNodeProfile(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	value := profile.Value
	value.NodeID, value.LinkOwner = nodeID, reg.stores.WriterID()
	value.LastHeartbeatUnix = time.Now().Add(-time.Hour).Unix()
	profile, err = reg.stores.nodes.Register(ctx, profile.Version, value)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestReaperRecoversDisconnectBeforeClaimAfterRestart(t *testing.T) {
	reg, db := registryWithTypedFixture(t)
	ctx := context.Background()
	staleNodeProfile(t, reg, "n1")
	route := testSandboxIdentityRecord("/g", "unclaimed-restart", "sb-restart", "n1", StateReserved)
	if _, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{}); err != nil || !ok {
		t.Fatalf("seed route: %v %v", ok, err)
	}
	// A disconnected stream's process exits before its delayed BeginReap. The
	// restarted process has no connection and no in-memory disconnect timer.
	claims, err := db.ListReaping(ctx, "", 128)
	if err != nil || len(claims.Items) != 0 {
		t.Fatalf("unexpected durable claim: %+v %v", claims, err)
	}
	stores, err := NewStores(db.Repositories(), metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	restarted := New(stores, nil, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
	reapCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { restarted.RunReaper(reapCtx, 30*time.Second); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.After(time.Second)
	for {
		profile, err := stores.nodes.ReadNodeProfile(ctx, "n1")
		if err != nil {
			t.Fatal(err)
		}
		if !profile.Exists {
			break
		}
		select {
		case <-deadline:
			t.Fatal("restart left an expired unclaimed profile permanently live")
		case <-time.After(time.Millisecond):
		}
	}
	if _, _, found, err := stores.GetSandbox(ctx, route.Group, route.RouteKey); err != nil || found {
		t.Fatalf("route survived recovery: found=%v err=%v", found, err)
	}
	owners, err := stores.ownership.ListSandboxOwners(ctx, "n1", "", 128)
	if err != nil || len(owners.Items) != 0 {
		t.Fatalf("ownership survived recovery: %+v %v", owners, err)
	}
	view, err := stores.directory.Load(ctx)
	if err != nil || len(view.Nodes) != 0 {
		t.Fatalf("catalog survived recovery: %+v %v", view, err)
	}
}

type unknownRecoveryClaim struct {
	metadata.NodeRepository
	committed bool
	calls     int
}

func (s *unknownRecoveryClaim) BeginReap(ctx context.Context, nodeID string, version metadata.Version) (metadata.ReapClaim, error) {
	s.calls++
	if s.calls == 1 {
		if s.committed {
			if _, err := s.NodeRepository.BeginReap(ctx, nodeID, version); err != nil {
				return metadata.ReapClaim{}, err
			}
		}
		return metadata.ReapClaim{}, &metadata.Unknown{Cause: errors.New("reap claim response lost")}
	}
	return s.NodeRepository.BeginReap(ctx, nodeID, version)
}

func TestExpiredNodeRecoveryResumesUnknownClaim(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			ctx := context.Background()
			staleNodeProfile(t, reg, "n1")
			route := testSandboxIdentityRecord("/g", "unknown-reap", "sb-unknown-reap", "n1", StateReserved)
			if _, ok, err := reg.stores.commitSandboxIntent(ctx, route, metadata.Version{}); err != nil || !ok {
				t.Fatalf("seed route: %v %v", ok, err)
			}
			unknown := &unknownRecoveryClaim{NodeRepository: reg.stores.nodes, committed: committed}
			reg.stores.nodes = unknown
			reg.recoverExpiredNodes(ctx, 30*time.Second)
			profile, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
			if err != nil || !profile.Exists || profile.Reaping != committed || unknown.calls != 1 {
				t.Fatalf("ambiguous claim was mishandled: %+v calls=%d err=%v", profile, unknown.calls, err)
			}
			if _, _, found, err := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey); err != nil || !found {
				t.Fatalf("unknown claim triggered speculative cleanup: found=%v err=%v", found, err)
			}
			reapCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() { reg.RunReaper(reapCtx, 30*time.Second); close(done) }()
			t.Cleanup(func() { cancel(); <-done })
			deadline := time.After(time.Second)
			for {
				profile, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
				if err != nil {
					t.Fatal(err)
				}
				if !profile.Exists {
					break
				}
				select {
				case <-deadline:
					t.Fatal("unknown claim did not recover from its persisted state")
				case <-time.After(time.Millisecond):
				}
			}
			if _, _, found, err := reg.stores.GetSandbox(ctx, route.Group, route.RouteKey); err != nil || found {
				t.Fatalf("recovered claim left the route: found=%v err=%v", found, err)
			}
		})
	}
}

type recoveryProbeOwner struct {
	NodeOwner
	probe func(context.Context, string) error
}

func (o recoveryProbeOwner) Connected(ctx context.Context, nodeID string) error {
	return o.probe(ctx, nodeID)
}

func TestExpiredNodeRecoveryChecksLocalAndRemoteConnections(t *testing.T) {
	for _, mode := range []string{"local_connected", "remote_connected", "remote_uncertain", "remote_gone"} {
		t.Run(mode, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			profile := staleNodeProfile(t, reg, "n1")
			ctx := context.Background()
			probes := 0
			if mode == "local_connected" {
				reg.addNode(&fakeConn{nodeID: "n1"})
			} else {
				next := profile.Value
				next.LinkOwner = "remote"
				if _, err := reg.stores.nodes.Register(ctx, profile.Version, next); err != nil {
					t.Fatal(err)
				}
			}
			reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": recoveryProbeOwner{probe: func(context.Context, string) error {
				probes++
				switch mode {
				case "remote_connected":
					return nil
				case "remote_uncertain":
					return errors.New("owner connection interrupted")
				default:
					return ErrNodeGone
				}
			}}})
			reg.recoverExpiredNodes(ctx, 30*time.Second)
			got, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
			if err != nil || got.Exists != (mode != "remote_gone") || got.Reaping {
				t.Fatalf("connection protection failed: %+v %v", got, err)
			}
			if mode == "local_connected" && probes != 0 {
				t.Fatalf("local connected node caused %d remote probes", probes)
			}
			if mode != "local_connected" && probes != 1 {
				t.Fatalf("remote node probes=%d", probes)
			}
		})
	}
}

func TestDelayedNodeReapChecksReconnectedRemoteOwner(t *testing.T) {
	for _, mode := range []string{"connected", "uncertain", "gone"} {
		t.Run(mode, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			ctx := context.Background()
			old := staleNodeProfile(t, reg, "n1")
			next := old.Value
			next.LinkOwner = "remote"
			registered, err := reg.stores.nodes.Register(ctx, old.Version, next)
			if err != nil {
				t.Fatal(err)
			}
			reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": recoveryProbeOwner{probe: func(context.Context, string) error {
				switch mode {
				case "connected":
					return nil
				case "uncertain":
					return errors.New("owner connection interrupted")
				default:
					return ErrNodeGone
				}
			}}})
			// The original Registry's delayed timer sees the replacement profile,
			// whose heartbeats may be temporarily delayed despite an active stream.
			reg.sweepNode(ctx, "n1", 30*time.Second)
			got, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
			if err != nil || got.Exists != (mode != "gone") || got.Reaping {
				t.Fatalf("old timer mishandled remote connection: %+v %v", got, err)
			}
			if mode != "gone" && got.Registration != registered.Registration {
				t.Fatal("old timer replaced the remote registration")
			}
		})
	}
}

func TestExpiredNodeRecoveryBoundsUnavailableOwnerProbe(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	staleNodeProfile(t, reg, "n1")
	for _, id := range []string{"a-remote", "b-remote"} {
		if _, err := reg.stores.nodes.Register(ctx, metadata.Version{}, metadata.NodeProfileRecord{
			NodeID: id, LinkOwner: "remote", LastHeartbeatUnix: time.Now().Add(-time.Hour).Unix(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	probes := 0
	reg.SetRemoteNodeOwners(map[string]NodeOwner{"remote": recoveryProbeOwner{probe: func(ctx context.Context, _ string) error {
		probes++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("connection recovery has no bounded probe deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	}}})
	reg.recoverExpiredNodes(ctx, 20*time.Millisecond)
	if probes != 1 {
		t.Fatalf("unavailable owner was probed once per node: probes=%d", probes)
	}
	if profile, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1"); err != nil || profile.Exists {
		t.Fatalf("unavailable owner blocked local recovery: %+v %v", profile, err)
	}
	for _, id := range []string{"a-remote", "b-remote"} {
		profile, err := reg.stores.nodes.ReadNodeProfile(ctx, id)
		if err != nil || !profile.Exists || profile.Reaping {
			t.Fatalf("uncertain remote %s was removed: %+v %v", id, profile, err)
		}
	}
}

type observedExpiredProfiles struct {
	metadata.NodeRepository
	emptyPageWithNext bool
	calls             int
}

func (s *observedExpiredProfiles) ListExpiredNodeProfiles(ctx context.Context, cutoff int64, cursor metadata.Cursor, limit int) (metadata.Page[metadata.Versioned[metadata.NodeProfileRecord]], error) {
	s.calls++
	page, err := s.NodeRepository.ListExpiredNodeProfiles(ctx, cutoff, cursor, limit)
	if len(page.Items) == 0 && page.Next != "" {
		s.emptyPageWithNext = true
	}
	return page, err
}

func TestExpiredNodeRecoveryContinuesAcrossEmptyFilteredPage(t *testing.T) {
	reg, _ := registryWithTypedFixture(t)
	ctx := context.Background()
	staleNodeProfile(t, reg, "n1")
	staleNodeProfile(t, reg, "zz")
	for i := 0; i < 128; i++ {
		if _, err := reg.stores.nodes.Register(ctx, metadata.Version{}, metadata.NodeProfileRecord{
			NodeID: fmt.Sprintf("a%03d", i), LinkOwner: "registry", LastHeartbeatUnix: time.Now().Unix(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	observed := &observedExpiredProfiles{NodeRepository: reg.stores.nodes}
	reg.stores.nodes = observed
	reg.recoverExpiredNodes(ctx, 30*time.Second)
	if !observed.emptyPageWithNext || observed.calls != 2 {
		t.Fatalf("did not traverse bounded empty page: empty=%v calls=%d", observed.emptyPageWithNext, observed.calls)
	}
	for _, id := range []string{"n1", "zz"} {
		if profile, err := reg.stores.nodes.ReadNodeProfile(ctx, id); err != nil || profile.Exists {
			t.Fatalf("expired tail profile %s survived: %+v %v", id, profile, err)
		}
	}
	view, err := reg.stores.directory.Load(ctx)
	if err != nil || len(view.Nodes) != 128 {
		t.Fatalf("fresh profiles were altered: count=%d err=%v", len(view.Nodes), err)
	}
}

func TestExpiredNodeRecoveryRejectsCandidateChangedDuringConnectionProbe(t *testing.T) {
	for _, mode := range []string{"heartbeat", "reconnect"} {
		t.Run(mode, func(t *testing.T) {
			reg, _ := registryWithTypedFixture(t)
			ctx := context.Background()
			old := staleNodeProfile(t, reg, "n1")
			reg.SetNodeOwner(recoveryProbeOwner{probe: func(context.Context, string) error {
				if mode == "heartbeat" {
					reg.updateHeartbeat(withNodeRegistration(ctx, old.Registration), "n1", &routesync.Heartbeat{Counts: 7})
				} else {
					if _, err := reg.updateNodeRegister(ctx, &routesync.NodeRegister{NodeID: "n1", APIEndpoint: "new-session"}); err != nil {
						t.Fatal(err)
					}
				}
				// The probe raced the new frame/session. Its disconnect result must
				// never authorize cleanup against a freshly reread version.
				return ErrNodeGone
			}})
			reg.recoverExpiredNodes(ctx, 30*time.Second)
			got, err := reg.stores.nodes.ReadNodeProfile(ctx, "n1")
			if err != nil || !got.Exists || got.Reaping || got.Version == old.Version {
				t.Fatalf("changed candidate was reaped: %+v %v", got, err)
			}
			if mode == "heartbeat" && (got.Registration != old.Registration || got.Value.Counts != 7) {
				t.Fatalf("heartbeat changed registration: %+v", got)
			}
			if mode == "reconnect" && (got.Registration.Fence == old.Registration.Fence || got.Value.APIEndpoint != "new-session") {
				t.Fatalf("reconnect did not replace fence: %+v", got)
			}
		})
	}
}
