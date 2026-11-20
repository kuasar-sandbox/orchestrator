//go:build fdb && cgo

package fdbdriver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func nativeStore(t *testing.T) *Store {
	t.Helper()
	file := os.Getenv("FDB_CLUSTER_FILE")
	if file == "" {
		t.Fatal("native conformance requires explicit FDB_CLUSTER_FILE pointing to an isolated test cluster")
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	options := Options{ClusterFile: file, Prefix: "/orchestrator-native-test/" + hex.EncodeToString(id[:]) + "/"}
	store, err := Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.beforeCommit = nil
		store.afterNativeCommit = nil
		store.afterReadBeforeWatch = nil
		store.mu.Lock()
		closed := store.closed
		store.mu.Unlock()
		cleanupStore := store
		if closed {
			var err error
			cleanupStore, err = Open(options)
			if err != nil {
				t.Error(err)
				return
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := cleanupStore.mutate(ctx, func(m *mutation) (bool, error) {
			m.tr.ClearRange(rangeFor(fdb.Key(cleanupStore.prefix)))
			return true, nil
		})
		if err != nil {
			t.Errorf("cleanup isolated namespace: %v", err)
		}
		_ = cleanupStore.Close()
	})
	return store
}
func companion(t *testing.T, s *Store) *Store {
	t.Helper()
	out, err := Open(Options{ClusterFile: s.clusterFile, Prefix: string(s.prefix[:len(s.prefix)-1])})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = out.Close() })
	return out
}
func check(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func testFingerprint(secret string) string {
	raw, _ := hex.DecodeString(secret)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func routeFixture(id string) metadata.SandboxRecord {
	return metadata.SandboxRecord{Group: "/g", RouteKey: id, SandboxID: id, NodeSandboxID: id + "-g0", NextSandboxGeneration: 1, State: metadata.StateReserved, NodeID: "node", Profile: "bare", APISecretFingerprint: testFingerprint(strings.Repeat("11", 32))}
}
func buildFixture(id string) metadata.BuildRecord {
	return metadata.BuildRecord{Group: "/g", BuildID: id, NodeID: "node", Profile: types.ProfileBare, APISecretFingerprint: testFingerprint(strings.Repeat("11", 32)), TemplateID: "transient-" + id, State: metadata.BuildStarting, RegistrationImageRepo: "private-image", RegistrationRegistryAuth: "private-auth"}
}
func registerNode(t *testing.T, s *Store, nodeID string) metadata.NodeSnapshot {
	t.Helper()
	out, err := s.Register(context.Background(), metadata.Version{}, metadata.NodeProfileRecord{NodeID: nodeID, LinkOwner: "registry", Capacity: 100, APIEndpoint: "api:7443", DataEndpoint: "data:8080", Labels: map[string]string{"zone": "z"}})
	check(t, err)
	return out
}
func awaitInvalidation(t *testing.T, o metadata.Invalidation) {
	t.Helper()
	select {
	case <-o.Done():
	case <-time.After(5 * time.Second):
		o.Cancel()
		t.Fatal("observation did not finish")
	}
}

func TestNativeConformance(t *testing.T) {
	if os.Getenv("FDB_CLUSTER_FILE") == "" {
		t.Fatal("set FDB_CLUSTER_FILE to the explicitly provisioned native test database; this gate never silently skips")
	}
	t.Run("full_stamp_cas_delete_recreate", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		value := routeFixture("cas")
		key := metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}
		first, err := s.CompareAndPutRoute(ctx, key, metadata.Version{}, value)
		check(t, err)
		if !first.IsCommitted() {
			t.Fatal("no full committed stamp")
		}
		read, err := s.ReadRoute(ctx, key)
		check(t, err)
		if read.Version != first || read.Value.NodeSandboxID != value.NodeSandboxID {
			t.Fatal("read stamp/value")
		}
		forged := first
		forged[9] ^= 1
		_, err = s.CompareAndPutRoute(ctx, key, forged, value)
		if !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("batch-order byte was ignored: %v", err)
		}
		value.State = metadata.StateReady
		second, err := s.CompareAndPutRoute(ctx, key, first, value)
		check(t, err)
		if second.Compare(first) <= 0 {
			t.Fatal("committed order")
		}
		if err = s.DeleteRoute(ctx, key, first); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("stale delete: %v", err)
		}
		check(t, s.DeleteRoute(ctx, key, second))
		third, err := s.CompareAndPutRoute(ctx, key, metadata.Version{}, value)
		check(t, err)
		if third == first || third.Compare(second) <= 0 {
			t.Fatal("recreated version ABA")
		}
	})
	t.Run("known_noncommit_pins_business_expected", func(t *testing.T) {
		s := nativeStore(t)
		other := companion(t, s)
		ctx := context.Background()
		value := routeFixture("pin")
		key := metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}
		version, err := s.CompareAndPutRoute(ctx, key, metadata.Version{}, value)
		check(t, err)
		calls := 0
		s.beforeCommit = func(attempt int, tr fdb.Transaction) error {
			calls++
			if attempt == 1 {
				replacement := value
				replacement.State = metadata.StatePaused
				_, err := other.CompareAndPutRoute(ctx, key, version, replacement)
				return err
			}
			return nil
		}
		value.State = metadata.StateReady
		_, err = s.CompareAndPutRoute(ctx, key, version, value)
		s.beforeCommit = nil
		if !errors.Is(err, metadata.ErrConflict) || calls != 1 {
			t.Fatalf("retry adopted newer expected: calls=%d err=%v", calls, err)
		}
		current, err := s.ReadRoute(ctx, key)
		check(t, err)
		if current.Value.State != metadata.StatePaused {
			t.Fatal("intervening writer overwritten")
		}
	})
	t.Run("unknown_is_not_retried_and_chunk_cleanup", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		value := buildFixture("chunks")
		value.RegistrationConfig = map[string]string{"large": strings.Repeat("x", 900_000)}
		key := metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}
		var calls atomic.Int32
		s.afterNativeCommit = func(int) error { calls.Add(1); return fdb.Error{Code: 1021} }
		_, err := s.CompareAndPutBuild(ctx, key, metadata.Version{}, value)
		s.afterNativeCommit = nil
		if !errors.Is(err, metadata.ErrUnknown) || calls.Load() != 1 {
			t.Fatalf("unknown replayed: %d %v", calls.Load(), err)
		}
		current, err := s.ReadBuild(ctx, key)
		check(t, err)
		if !current.Exists || current.Value.RegistrationConfig["large"] != value.RegistrationConfig["large"] {
			t.Fatal("unknown committed chunk record missing")
		}
		value.RegistrationConfig = map[string]string{"small": "v"}
		version, err := s.CompareAndPutBuild(ctx, key, current.Version, value)
		check(t, err)
		check(t, s.read(ctx, func(tr fdb.Transaction) error {
			rows, err := tr.GetRange(chunkRange(s.buildKey(key)), fdb.RangeOptions{Limit: 1}).GetSliceWithError()
			if err == nil && len(rows) != 0 {
				t.Fatal("shrink leaked chunks")
			}
			return err
		}))
		check(t, s.DeleteBuild(ctx, key, version))
		page, err := s.ListBuildsByTemplate(ctx, "/g", value.TemplateID, "", 4)
		check(t, err)
		if len(page.Items) != 0 {
			t.Fatal("delete leaked template index")
		}
		value.RegistrationConfig = map[string]string{"oversized": strings.Repeat("z", transactionBytes)}
		_, err = s.CompareAndPutBuild(ctx, key, metadata.Version{}, value)
		if !errors.Is(err, metadata.ErrCapacity) {
			t.Fatalf("oversized mutation not rejected: %v", err)
		}
		current, err = s.ReadBuild(ctx, key)
		check(t, err)
		if current.Exists {
			t.Fatal("oversized transaction became visible")
		}
	})
	t.Run("pagination_scope_and_nonunique_build_index", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		for i := 0; i < 4; i++ {
			value := buildFixture(fmt.Sprintf("page-%d", i))
			value.TemplateID = "transient-shared"
			value.RegistrationConfig = map[string]string{"large": strings.Repeat("v", 800_000)}
			_, err := s.CompareAndPutBuild(ctx, metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}, metadata.Version{}, value)
			check(t, err)
		}
		first, err := s.ListBuildsByTemplate(ctx, "/g", "transient-shared", "", 512)
		check(t, err)
		if len(first.Items) != 2 || first.Next == "" {
			t.Fatalf("byte bound ignored: rows=%d next=%q", len(first.Items), first.Next)
		}
		second, err := s.ListBuildsByTemplate(ctx, "/g", "transient-shared", first.Next, 512)
		check(t, err)
		if len(second.Items) != 2 || second.Next != "" {
			t.Fatal("nonunique index pagination lost/duplicated rows")
		}
		if _, err = s.ListBuilds(ctx, "/other", first.Next, 10); !errors.Is(err, metadata.ErrInvalidRecord) {
			t.Fatalf("cross-scope cursor: %v", err)
		}
	})
	t.Run("atomic_intent_owner_and_rollback", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		registerNode(t, s, "node")
		value := routeFixture("intent")
		ref := metadata.SandboxRef(value)
		key := metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}
		version, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, value, ref)
		check(t, err)
		owner, err := s.ReadSandboxOwner(ctx, "node", ref.NodeSandboxID)
		check(t, err)
		if !owner.Exists || owner.Version != version {
			t.Fatal("intent and ownership did not share commit")
		}
		conflict := value
		conflict.Group = "/other"
		_, err = s.CommitIntentAndOwnership(ctx, metadata.Version{}, conflict, metadata.SandboxRef(conflict))
		if !errors.Is(err, metadata.ErrOwnershipConflict) {
			t.Fatalf("identity collision was not distinct: %v", err)
		}
		absent, err := s.ReadRoute(ctx, metadata.RouteKey{Group: conflict.Group, RouteKey: conflict.RouteKey})
		check(t, err)
		if absent.Exists {
			t.Fatal("collision partially wrote route")
		}
		replacement := value
		replacement.SandboxGeneration = 1
		replacement.NodeSandboxID = value.SandboxID + "-g1"
		replacement.NextSandboxGeneration = 2
		next, err := s.CommitIntentAndOwnership(ctx, version, replacement, metadata.SandboxRef(replacement))
		check(t, err)
		restored, err := s.RollbackIntent(ctx, key, next, &value, true)
		check(t, err)
		current, err := s.ReadRoute(ctx, key)
		check(t, err)
		if current.Version != restored || current.Value.NextSandboxGeneration != 2 || current.Value.NodeSandboxID != value.NodeSandboxID {
			t.Fatal("rollback lost identity/generation fence")
		}
		owner, err = s.ReadSandboxOwner(ctx, "node", replacement.NodeSandboxID)
		check(t, err)
		if owner.Exists {
			t.Fatal("rollback leaked replacement owner")
		}
		check(t, s.DeleteRouteAndOwnership(ctx, key, restored, "node", ref))
		current, err = s.ReadRoute(ctx, key)
		check(t, err)
		owner, err = s.ReadSandboxOwner(ctx, "node", ref.NodeSandboxID)
		check(t, err)
		if current.Exists || owner.Exists {
			t.Fatal("delete was not route/ref atomic")
		}
		check(t, s.DeleteRouteAndOwnership(ctx, key, metadata.Version{}, "node", ref))
	})
	t.Run("connect_preserves_ready_and_pausing", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		registerNode(t, s, "node")
		for _, state := range []metadata.SandboxState{metadata.StateReady, metadata.StatePausing} {
			value := routeFixture("connect-" + string(state))
			key := metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}
			version, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, value, metadata.SandboxRef(value))
			check(t, err)
			value.State = state
			version, err = s.CompareAndPutRoute(ctx, key, version, value)
			check(t, err)
			value.LastActive = time.Now().Unix()
			connected, err := s.CommitIntentAndOwnership(ctx, version, value, metadata.SandboxRef(value))
			check(t, err)
			current, err := s.ReadRoute(ctx, key)
			check(t, err)
			owner, err := s.ReadSandboxOwner(ctx, "node", value.NodeSandboxID)
			check(t, err)
			if current.Value.State != state || current.Version != connected || owner.Version != connected {
				t.Fatal("Connect regressed state or split ownership commit")
			}
		}
	})
	t.Run("migration_rollback_restores_route_when_old_node_is_not_live", func(t *testing.T) {
		for _, reaping := range []bool{false, true} {
			t.Run(fmt.Sprintf("reaping=%v", reaping), func(t *testing.T) {
				s := nativeStore(t)
				ctx := context.Background()
				current := registerNode(t, s, "node")
				if reaping {
					old := registerNode(t, s, "old-node")
					_, err := s.BeginReap(ctx, "old-node", old.Version)
					check(t, err)
				}
				for _, eventRollback := range []bool{false, true} {
					id := fmt.Sprintf("migration-%v", eventRollback)
					candidate := routeFixture(id)
					candidate.SandboxGeneration = 1
					candidate.NodeSandboxID = id + "-g1"
					candidate.NextSandboxGeneration = 2
					version, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, candidate, metadata.SandboxRef(candidate))
					check(t, err)
					restore := routeFixture(id)
					restore.NodeID = "old-node"
					restore.State = metadata.StatePaused
					restore.ArtifactLocation = "remote"
					timeout := int64(3600)
					restore.IdleTimeout = &timeout
					restore.APISecret = "retained-credential"
					key := metadata.RouteKey{Group: candidate.Group, RouteKey: candidate.RouteKey}
					if eventRollback {
						check(t, s.ApplyRouteDelete(ctx, current.Registration, version, metadata.SandboxRef(candidate), &restore))
					} else {
						_, err = s.RollbackIntent(ctx, key, version, &restore, true)
						check(t, err)
					}
					got, err := s.ReadRoute(ctx, key)
					check(t, err)
					if !got.Exists || got.Value.State != metadata.StatePaused || got.Value.NodeID != "old-node" || got.Value.NextSandboxGeneration != 2 || got.Value.ArtifactLocation != "remote" || got.Value.IdleTimeout == nil || *got.Value.IdleTimeout != 3600 || got.Value.APISecret != "retained-credential" {
						t.Fatal("old-node outage lost rollback route data")
					}
					oldOwner, err := s.ReadSandboxOwner(ctx, "old-node", restore.NodeSandboxID)
					check(t, err)
					candidateOwner, err := s.ReadSandboxOwner(ctx, "node", candidate.NodeSandboxID)
					check(t, err)
					if oldOwner.Exists || candidateOwner.Exists {
						t.Fatal("rollback inserted a reference on absent/reaping node or retained candidate")
					}
				}
			})
		}
	})
	t.Run("build_report_ack_delete_and_unknown", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		node := registerNode(t, s, "node")
		value := buildFixture("report")
		ref := metadata.NodeBuildRef{Group: value.Group, BuildID: value.BuildID, TemplateID: value.TemplateID}
		key := metadata.BuildKey{Group: value.Group, BuildID: value.BuildID}
		_, err := s.CommitRegistrationIntent(ctx, value, ref)
		check(t, err)
		check(t, s.DeleteBuildProjection(ctx, node.Registration, ref))
		current, err := s.ReadBuild(ctx, key)
		check(t, err)
		if !current.Exists || current.Value.State != metadata.BuildStarting {
			t.Fatal("snapshot removed ambiguous registration intent")
		}
		check(t, s.ApplyBuildReport(ctx, node.Registration, ref, routesync.BuildEvent{BuildID: value.BuildID, TemplateID: value.TemplateID, State: string(metadata.BuildReady), PersistID: "canonical-artifact"}))
		target := &types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true}
		accepted, err := s.AcceptBuildRegistration(ctx, key, "node", value.TemplateID, target)
		check(t, err)
		if accepted.Value.State != metadata.BuildReady || accepted.Value.PersistID != "canonical-artifact" || !accepted.Value.RegistrationTargetSet || accepted.Value.RegistrationRegistryAuth != "" {
			t.Fatal("late ACK regressed report or retained replay credentials")
		}
		if err = s.RejectRegistrationIntent(ctx, key, "node", value.TemplateID); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("accepted registration rejected: %v", err)
		}
		wrong := ref
		wrong.TemplateID = "transient-other"
		check(t, s.DeleteBuildProjection(ctx, node.Registration, wrong))
		current, err = s.ReadBuild(ctx, key)
		check(t, err)
		if !current.Exists {
			t.Fatal("wrong template deleted build")
		}
		var commits atomic.Int32
		s.afterNativeCommit = func(int) error { commits.Add(1); return fdb.Error{Code: 1021} }
		err = s.DeleteBuildProjection(ctx, node.Registration, ref)
		s.afterNativeCommit = nil
		if !errors.Is(err, metadata.ErrUnknown) || commits.Load() != 1 {
			t.Fatal("projection delete unknown retried")
		}
		current, err = s.ReadBuild(ctx, key)
		check(t, err)
		owner, err := s.ReadBuildOwner(ctx, "node", value.BuildID)
		check(t, err)
		page, err := s.ListBuildsByTemplate(ctx, "/g", value.TemplateID, "", 10)
		check(t, err)
		if current.Exists || owner.Exists || len(page.Items) != 0 {
			t.Fatal("unknown delete did not atomically remove build/ref/index")
		}
	})
	t.Run("owner_refresh_and_concurrent_key_pair_binding", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		registerNode(t, s, "node")
		build := buildFixture("refresh")
		ref := metadata.NodeBuildRef{Group: build.Group, BuildID: build.BuildID, TemplateID: build.TemplateID}
		_, err := s.CommitRegistrationIntent(ctx, build, ref)
		check(t, err)
		old, err := s.ReadBuildOwner(ctx, "node", build.BuildID)
		check(t, err)
		check(t, s.EnsureBuildOwner(ctx, "node", ref))
		fresh, err := s.ReadBuildOwner(ctx, "node", build.BuildID)
		check(t, err)
		if fresh.Version == old.Version {
			t.Fatal("same ownership did not refresh stamp")
		}
		if err = s.RemoveBuildOwner(ctx, "node", build.BuildID, old.Version); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old captured ref removed refreshed ownership: %v", err)
		}
		api := strings.Repeat("88", 32)
		first := metadata.NodeKeyPair{APISecret: api, APISecretFingerprint: testFingerprint(api), ManifestKey: strings.Repeat("55", 32), ExpiresUnix: time.Now().Unix() + 100}
		first.ManifestKeyFingerprint = testFingerprint(first.ManifestKey)
		second := first
		second.ManifestKey = strings.Repeat("66", 32)
		second.ManifestKeyFingerprint = testFingerprint(second.ManifestKey)
		start := make(chan struct{})
		results := make(chan error, 2)
		for _, pair := range []metadata.NodeKeyPair{first, second} {
			go func(pair metadata.NodeKeyPair) { <-start; _, err := s.PutKeyPair(ctx, "node", pair); results <- err }(pair)
		}
		close(start)
		wins := 0
		for i := 0; i < 2; i++ {
			if err := <-results; err == nil {
				wins++
			} else if !errors.Is(err, metadata.ErrConflict) && !errors.Is(err, metadata.ErrOwnershipConflict) {
				t.Fatal(err)
			}
		}
		if wins != 1 {
			t.Fatalf("credential binding winners=%d", wins)
		}
		page, err := s.ListKeyPairs(ctx, "node", "", 10)
		check(t, err)
		if len(page.Items) != 1 {
			t.Fatal("credential collision produced multiple records")
		}
		stored := page.Items[0].Value
		if stored.ManifestKeyFingerprint != first.ManifestKeyFingerprint && stored.ManifestKeyFingerprint != second.ManifestKeyFingerprint {
			t.Fatal("credential collision produced mixed material")
		}
	})
	t.Run("registration_fence_and_reaper_recovery", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		old := registerNode(t, s, "node")
		value := routeFixture("reap")
		version, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, value, metadata.SandboxRef(value))
		check(t, err)
		owner, err := s.ReadSandboxOwner(ctx, "node", value.NodeSandboxID)
		check(t, err)
		claim, err := s.BeginReap(ctx, "node", old.Version)
		check(t, err)
		markers, err := s.ListReaping(ctx, "", 10)
		check(t, err)
		if len(markers.Items) != 1 || markers.Items[0] != claim {
			t.Fatal("reaping marker not restart recoverable")
		}
		next := old.Value
		next.LinkOwner = "registry"
		registered, err := s.Register(ctx, claim.ProfileVersion, next)
		check(t, err)
		if registered.Registration.Fence == old.Registration.Fence {
			t.Fatal("same-owner registration reused fence")
		}
		_, err = s.UpdateProfile(ctx, old.Registration, registered.Version, registered.Value)
		if !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old stream adopted new profile: %v", err)
		}
		err = s.CleanupSandbox(ctx, claim, owner, version, true)
		if !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old reap claim survived register: %v", err)
		}
		route, err := s.ReadRoute(ctx, metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey})
		check(t, err)
		if !route.Exists {
			t.Fatal("stale reaper removed new registration's route")
		}
		claim, err = s.BeginReap(ctx, "node", registered.Version)
		check(t, err)
		if _, err = s.CommitIntentAndOwnership(ctx, metadata.Version{}, routeFixture("late"), metadata.SandboxRef(routeFixture("late"))); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("ref inserted after claim: %v", err)
		}
		check(t, s.CleanupSandbox(ctx, claim, owner, version, false))
		route, err = s.ReadRoute(ctx, metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey})
		check(t, err)
		if !route.Exists {
			t.Fatal("inflight reserve reaped")
		}
		check(t, s.CleanupSandbox(ctx, claim, owner, version, true))
		removed, err := s.FinalizeReap(ctx, claim)
		check(t, err)
		if !removed {
			t.Fatal("empty reaper did not finalize")
		}
		markers, err = s.ListReaping(ctx, "", 10)
		check(t, err)
		if len(markers.Items) != 0 {
			t.Fatal("finalized reap marker remained")
		}
	})
	t.Run("old_stream_cannot_report_delete_or_rollback_after_register", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		old := registerNode(t, s, "node")
		route := routeFixture("stream-ready")
		routeKey := metadata.RouteKey{Group: route.Group, RouteKey: route.RouteKey}
		routeVersion, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, route, metadata.SandboxRef(route))
		check(t, err)
		route.State = metadata.StateReady
		routeVersion, err = s.CompareAndPutRoute(ctx, routeKey, routeVersion, route)
		check(t, err)
		reserved := routeFixture("stream-reserve")
		reservedVersion, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, reserved, metadata.SandboxRef(reserved))
		check(t, err)
		build := buildFixture("stream-build")
		buildRef := metadata.NodeBuildRef{Group: build.Group, BuildID: build.BuildID, TemplateID: build.TemplateID}
		buildVersion, err := s.CommitRegistrationIntent(ctx, build, buildRef)
		check(t, err)
		build.State = metadata.BuildReady
		buildVersion, err = s.CompareAndPutBuild(ctx, metadata.BuildKey{Group: build.Group, BuildID: build.BuildID}, buildVersion, build)
		check(t, err)
		fresh, err := s.Register(ctx, old.Version, old.Value)
		check(t, err)
		if fresh.Registration.Fence == old.Registration.Fence {
			t.Fatal("new registration reused old fence")
		}
		check(t, s.EnsureBuildOwner(ctx, "node", buildRef))
		buildOwner, err := s.ReadBuildOwner(ctx, "node", build.BuildID)
		check(t, err)
		_, err = s.ApplyRouteReport(ctx, old.Registration, metadata.SandboxRef(route), routesync.RouteEntry{SandboxID: route.NodeSandboxID, Profile: route.Profile, State: routesync.StateDead})
		if !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old route report accepted: %v", err)
		}
		err = s.ApplyBuildReport(ctx, old.Registration, buildRef, routesync.BuildEvent{BuildID: build.BuildID, TemplateID: build.TemplateID, State: string(metadata.BuildError)})
		if !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old build report accepted: %v", err)
		}
		if err = s.DeleteBuildProjection(ctx, old.Registration, buildRef); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old snapshot deleted build: %v", err)
		}
		if err = s.ApplyRouteDelete(ctx, old.Registration, routeVersion, metadata.SandboxRef(route), nil); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old stream deleted route: %v", err)
		}
		restore := reserved
		restore.State = metadata.StatePaused
		if err = s.ApplyRouteDelete(ctx, old.Registration, reservedVersion, metadata.SandboxRef(reserved), &restore); !errors.Is(err, metadata.ErrConflict) {
			t.Fatalf("old stream rolled back reserve: %v", err)
		}
		got, err := s.ReadRoute(ctx, routeKey)
		check(t, err)
		if got.Version != routeVersion || got.Value.State != metadata.StateReady {
			t.Fatal("old stream changed route")
		}
		gotBuild, err := s.ReadBuild(ctx, metadata.BuildKey{Group: build.Group, BuildID: build.BuildID})
		check(t, err)
		gotOwner, err := s.ReadBuildOwner(ctx, "node", build.BuildID)
		check(t, err)
		if gotBuild.Version != buildVersion || gotOwner.Version != buildOwner.Version {
			t.Fatal("old stream changed build or refreshed reference")
		}
		check(t, s.ApplyRouteDelete(ctx, fresh.Registration, reservedVersion, metadata.SandboxRef(reserved), &restore))
		got, err = s.ReadRoute(ctx, metadata.RouteKey{Group: reserved.Group, RouteKey: reserved.RouteKey})
		check(t, err)
		owner, err := s.ReadSandboxOwner(ctx, "node", reserved.NodeSandboxID)
		check(t, err)
		if got.Value.State != metadata.StatePaused || !owner.Exists {
			t.Fatal("current stream rollback lost retained same-owner reference")
		}
	})
	t.Run("terminal_reap_retention", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		node := registerNode(t, s, "node")
		value := routeFixture("terminal")
		version, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, value, metadata.SandboxRef(value))
		check(t, err)
		value.State = metadata.StateDead
		version, err = s.CompareAndPutRoute(ctx, metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}, version, value)
		check(t, err)
		owner, err := s.ReadSandboxOwner(ctx, "node", value.NodeSandboxID)
		check(t, err)
		build := buildFixture("terminal")
		buildVersion, err := s.CommitRegistrationIntent(ctx, build, metadata.NodeBuildRef{Group: build.Group, BuildID: build.BuildID, TemplateID: build.TemplateID})
		check(t, err)
		buildOwner, err := s.ReadBuildOwner(ctx, "node", build.BuildID)
		check(t, err)
		claim, err := s.BeginReap(ctx, "node", node.Version)
		check(t, err)
		check(t, s.CleanupSandbox(ctx, claim, owner, version, true))
		check(t, s.CleanupBuild(ctx, claim, buildOwner, buildVersion))
		current, err := s.ReadBuild(ctx, metadata.BuildKey{Group: build.Group, BuildID: build.BuildID})
		check(t, err)
		if current.Value.State != metadata.BuildError || current.Value.RegistrationRegistryAuth != "" {
			t.Fatal("reaper did not terminalize build and clear dispatch secret")
		}
		check(t, s.CleanupBuild(ctx, claim, buildOwner, current.Version))
		done, err := s.FinalizeReap(ctx, claim)
		check(t, err)
		if done {
			t.Fatal("retained terminal bindings prematurely finalized")
		}
		route, err := s.ReadRoute(ctx, metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey})
		check(t, err)
		if !route.Exists || route.Value.State != metadata.StateDead {
			t.Fatal("terminal route was lost")
		}
	})
	t.Run("route_report_credentials_generation_and_terminal_order", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		node := registerNode(t, s, "node")
		value := routeFixture("report-route")
		ref := metadata.SandboxRef(value)
		_, err := s.CommitIntentAndOwnership(ctx, metadata.Version{}, value, ref)
		check(t, err)
		secret := strings.Repeat("22", 32)
		token, err := keys.MintForwardAccessToken(secret, value.SandboxID)
		check(t, err)
		event := routesync.RouteEntry{SandboxID: value.NodeSandboxID, Profile: "bare", State: routesync.StateRunning, InstanceID: "instance", PressureVersion: 8, StableID: value.SandboxID, APISecret: strings.Repeat("11", 32), APISecretFingerprint: value.APISecretFingerprint, ManifestKeyFingerprint: strings.Repeat("33", 32), ServiceSecret: secret, ForwardAccessToken: token}
		result, err := s.ApplyRouteReport(ctx, node.Registration, ref, event)
		check(t, err)
		if result.Disposition != metadata.ReportWritten || !result.Record.Version.IsCommitted() {
			t.Fatal("valid route not written")
		}
		event.State = routesync.StateDead
		result, err = s.ApplyRouteReport(ctx, node.Registration, ref, event)
		check(t, err)
		terminal := result.Record.Version
		event.State = routesync.StateRunning
		result, err = s.ApplyRouteReport(ctx, node.Registration, ref, event)
		check(t, err)
		if result.Disposition != metadata.ReportIgnored || result.Record.Version != terminal {
			t.Fatal("same-version terminal resurrection")
		}
		event.PressureVersion++
		event.ServiceSecret = strings.Repeat("44", 32)
		event.ForwardAccessToken, err = keys.MintForwardAccessToken(event.ServiceSecret, event.StableID)
		check(t, err)
		result, err = s.ApplyRouteReport(ctx, node.Registration, ref, event)
		check(t, err)
		if result.Disposition != metadata.ReportIgnored {
			t.Fatal("credential rebinding accepted")
		}
		ref.SandboxGeneration++
		ref.NodeSandboxID = ref.SandboxID + "-g1"
		event.SandboxID = ref.NodeSandboxID
		result, err = s.ApplyRouteReport(ctx, node.Registration, ref, event)
		check(t, err)
		if result.Disposition != metadata.ReportOrphan {
			t.Fatal("replaced generation not orphaned")
		}
	})
	t.Run("key_pair_ack_identity_and_import_lease", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		registerNode(t, s, "node")
		api := strings.Repeat("11", 32)
		manifest := strings.Repeat("22", 32)
		pair := metadata.NodeKeyPair{APISecretFingerprint: testFingerprint(api), APISecret: api, ManifestKeyFingerprint: testFingerprint(manifest), ManifestKey: manifest, ExpiresUnix: time.Now().Unix() + 300}
		first, err := s.PutKeyPair(ctx, "node", pair)
		check(t, err)
		ack, err := s.AckKeyPair(ctx, "node", pair)
		check(t, err)
		if !ack {
			t.Fatal("desired key pair not acknowledged")
		}
		pair.ExpiresUnix++
		second, err := s.PutKeyPair(ctx, "node", pair)
		check(t, err)
		if second.Value.AckedExpiresUnix != pair.ExpiresUnix-1 {
			t.Fatal("desired renewal lost previous ACK")
		}
		stale := pair
		stale.ExpiresUnix--
		ack, err = s.AckKeyPair(ctx, "node", stale)
		check(t, err)
		if ack {
			t.Fatal("stale desired material ACKed renewal")
		}
		if err = s.DropKeyPair(ctx, "node", pair.APISecretFingerprint, first.Version); !errors.Is(err, metadata.ErrConflict) {
			t.Fatal("stale prune removed renewed pair")
		}
		state, acquired, err := s.Acquire(ctx, "source", "owner", "run", time.Minute)
		check(t, err)
		if !acquired || state.Term == 0 {
			t.Fatal("lease not acquired")
		}
		_, acquired, err = s.Acquire(ctx, "source", "other", "other", time.Minute)
		check(t, err)
		if acquired {
			t.Fatal("active lease stolen")
		}
		state, err = s.Checkpoint(ctx, "source", "owner", "run", state.Term, "cursor", false, "")
		check(t, err)
		if state.Cursor != "cursor" {
			t.Fatal("checkpoint not persisted")
		}
		if _, err = s.Checkpoint(ctx, "source", "other", "other", state.Term, "bad", false, ""); !errors.Is(err, metadata.ErrStaleLease) {
			t.Fatal("stale checkpoint accepted")
		}
		state, err = s.Checkpoint(ctx, "source", "owner", "run", state.Term, "", true, "")
		check(t, err)
		if state.Cursor != "" || state.Round != 1 {
			t.Fatal("round checkpoint")
		}
	})
	t.Run("directory_pages_projection_and_invalidation", func(t *testing.T) {
		s := nativeStore(t)
		ctx := context.Background()
		var first metadata.NodeSnapshot
		count := 0
		for candidate := 0; count < 130; candidate++ {
			id := fmt.Sprintf("catalog-%d", candidate)
			if bucket(id) != 0 {
				continue
			}
			node := registerNode(t, s, id)
			if count == 0 {
				first = node
			}
			count++
		}
		view, err := s.Load(ctx)
		check(t, err)
		if len(view.Nodes) != 130 {
			t.Fatalf("directory published partial page: %d", len(view.Nodes))
		}
		for _, node := range view.Nodes {
			if node.APIEndpoint != "api:7443" || node.DataEndpoint != "data:8080" || node.Labels["zone"] != "z" || node.Capacity != 100 || !node.Version.IsCommitted() {
				t.Fatal("directory projection fields/version")
			}
		}
		heartbeat := first.Value
		heartbeat.LastHeartbeatUnix = time.Now().Unix()
		heartbeat.Counts = 42
		version, err := s.UpdateProfile(ctx, first.Registration, first.Version, heartbeat)
		check(t, err)
		quiet, err := s.Load(ctx)
		check(t, err)
		if quiet.Token != view.Token {
			t.Fatal("dynamic heartbeat invalidated low-frequency directory")
		}
		observation, err := s.Observe(ctx, quiet.Token)
		check(t, err)
		heartbeat.Capacity++
		_, err = s.UpdateProfile(ctx, first.Registration, version, heartbeat)
		check(t, err)
		awaitInvalidation(t, observation)
		check(t, observation.Err())
		observation.Cancel()
		if s.ObservationSlots() != 0 {
			t.Fatal("directory future cancellation leaked budget")
		}
		changed, err := s.Load(ctx)
		check(t, err)
		if changed.Token == quiet.Token {
			t.Fatal("projection change missing token")
		}
	})
	t.Run("lost_wakeup_shared_budget_native_capacity_and_close", func(t *testing.T) {
		s := nativeStore(t)
		other := companion(t, s)
		ctx := context.Background()
		value := routeFixture("watch")
		key := metadata.RouteKey{Group: value.Group, RouteKey: value.RouteKey}
		version, err := s.CompareAndPutRoute(ctx, key, metadata.Version{}, value)
		check(t, err)
		var raced atomic.Bool
		s.afterReadBeforeWatch = func() {
			if raced.CompareAndSwap(false, true) {
				next := value
				next.State = metadata.StateReady
				_, err := other.CompareAndPutRoute(ctx, key, version, next)
				check(t, err)
			}
		}
		current, watch, err := s.ReadAndObserveRoute(ctx, key)
		s.afterReadBeforeWatch = nil
		check(t, err)
		if current.Value.State != metadata.StateReady {
			awaitInvalidation(t, watch)
			check(t, watch.Err())
		}
		watch.Cancel()
		if s.ObservationSlots() != 0 {
			t.Fatal("watch leaked slot")
		}
		check(t, s.owner.budget.Configure(300))
		var watchers []metadata.Invalidation
		for i := 0; i < 44; i++ {
			_, watch, err := s.ReadAndObserveRoute(ctx, metadata.RouteKey{Group: "/watch", RouteKey: fmt.Sprint(i)})
			check(t, err)
			watchers = append(watchers, watch)
		}
		directory, err := other.Observe(ctx, metadata.DirectoryToken{})
		check(t, err)
		if s.ObservationSlots() != 300 {
			t.Fatal("wrappers did not share route/catalog budget")
		}
		_, _, err = other.ReadAndObserveRoute(ctx, key)
		if !errors.Is(err, metadata.ErrCapacity) {
			t.Fatalf("capacity did not fail before setup: %v", err)
		}
		directory.Cancel()
		for _, watch := range watchers {
			watch.Cancel()
		}
		if s.ObservationSlots() != 0 {
			t.Fatal("shared cancellation leaked slots")
		}
		check(t, s.owner.budget.Configure(1))
		reserved, release, err := s.ReserveObservation(ctx)
		check(t, err)
		_, _, err = other.ReserveObservation(ctx)
		if !errors.Is(err, metadata.ErrCapacity) {
			t.Fatal("second wrapper bypassed held reservation")
		}
		_, watch, err = s.ReadAndObserveRoute(reserved, key)
		check(t, err)
		release()
		awaitInvalidation(t, watch)
		if s.ObservationSlots() != 0 {
			t.Fatal("reservation released before native reclamation")
		}
		check(t, s.owner.budget.Configure(8192))
		check(t, s.owner.db.Options().SetMaxWatches(1))
		_, first, err := s.ReadAndObserveRoute(ctx, key)
		check(t, err)
		_, second, err := other.ReadAndObserveRoute(ctx, metadata.RouteKey{Group: "/g", RouteKey: "second-watch"})
		if err == nil {
			awaitInvalidation(t, second)
			err = second.Err()
			second.Cancel()
		}
		if !errors.Is(err, metadata.ErrCapacity) {
			t.Fatalf("native 1032 not typed Capacity: %v", err)
		}
		first.Cancel()
		check(t, s.owner.db.Options().SetMaxWatches(10000))
		_, watch, err = s.ReadAndObserveRoute(ctx, key)
		check(t, err)
		check(t, s.Close())
		awaitInvalidation(t, watch)
		if s.ActiveObservations() != 0 || s.ObservationSlots() != 0 {
			t.Fatal("Close returned before future cleanup")
		}
		_, err = other.ReadRoute(ctx, key)
		check(t, err)
	})
}

func TestChunkEnvelopeCorruption(t *testing.T) {
	payload := []byte("durable")
	raw := envelope(payload, 0)
	raw = raw[:len(raw)-4]
	copy(raw[:10], []byte{0, 0, 0, 0, 0, 0, 0, 1, 0, 2})
	decoded, err := decodeRecord(fdb.Transaction{}, fdb.Key("inline"), raw)
	check(t, err)
	if !bytes.Equal(decoded.payload, payload) {
		t.Fatal("inline")
	}
	raw[10] = 2
	if _, err = decodeRecord(fdb.Transaction{}, fdb.Key("inline"), raw); !errors.Is(err, metadata.ErrInvalidRecord) {
		t.Fatal("unknown schema accepted")
	}
}
