package conductorext

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestSourcesGetReturnIndependentRedactedViews(t *testing.T) {
	storage := testStore(t)
	ctx := context.Background()
	sandbox := testSandbox("sandbox-get", 10)
	if err := storage.InsertSandbox(ctx, sandbox); err != nil {
		t.Fatal(err)
	}
	build := testBuild("build-get", types.BuildBuilding, 20)
	if err := storage.PutBuild(ctx, build); err != nil {
		t.Fatal(err)
	}
	host, _ := New(storage, nil)

	firstSandbox, found, err := host.Sandboxes().Get(ctx, sandbox.ID)
	if err != nil || !found {
		t.Fatalf("sandbox Get found=%t err=%v", found, err)
	}
	if firstSandbox.APISecretFingerprint == "" || firstSandbox.ManifestKeyFingerprint == "" ||
		firstSandbox.APISecretFingerprint == sandbox.APISecret || firstSandbox.ManifestKeyFingerprint == sandbox.ManifestKey ||
		firstSandbox.ID != sandbox.ID || firstSandbox.StableID != sandbox.StableID() {
		t.Fatalf("sandbox fingerprints were not projected: %+v", firstSandbox)
	}
	firstSandbox.Metadata["key"] = "mutated"
	firstSandbox.Cluster.Group = "mutated"
	secondSandbox, found, err := host.Sandboxes().Get(ctx, sandbox.ID)
	if err != nil || !found || secondSandbox.Metadata["key"] != "sandbox" || secondSandbox.Cluster.Group != "group" {
		t.Fatalf("sandbox view was not independent: found=%t err=%v view=%+v", found, err, secondSandbox)
	}

	firstBuild, found, err := host.Builds().Get(ctx, build.BuildID)
	if err != nil || !found {
		t.Fatalf("build Get found=%t err=%v", found, err)
	}
	firstBuild.Metadata["key"] = "mutated"
	firstBuild.Names[0] = "mutated"
	firstBuild.Steps[0].Args[0] = "mutated"
	*firstBuild.Builder.Referer.Enabled = false
	firstBuild.Builder.Registry.TLS.CABundlePEM = "mutated"
	secondBuild, found, err := host.Builds().Get(ctx, build.BuildID)
	if err != nil || !found || secondBuild.Metadata["key"] != "build" || secondBuild.Names[0] != "name" ||
		secondBuild.Steps[0].Args[0] != "echo" || !*secondBuild.Builder.Referer.Enabled ||
		secondBuild.Builder.Registry.TLS.CABundlePEM != "certificate" {
		t.Fatalf("build view was not independent: found=%t err=%v view=%+v", found, err, secondBuild)
	}
	if _, found, err := host.Sandboxes().Get(ctx, "missing"); err != nil || found {
		t.Fatalf("missing sandbox found=%t err=%v", found, err)
	}
	if _, found, err := host.Builds().Get(ctx, "missing"); err != nil || found {
		t.Fatalf("missing build found=%t err=%v", found, err)
	}
}

func TestSandboxWatchSubscribesBeforeSnapshotAndAbandonsOverflowedGeneration(t *testing.T) {
	storage := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seed := testSandbox("sandbox-seed", 1)
	if err := storage.InsertSandbox(ctx, seed); err != nil {
		t.Fatal(err)
	}
	host, observer := newWithCapacity(storage, 1)

	firstSnapshot := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	var mu sync.Mutex
	var events []conductorextension.SandboxEvent
	go func() {
		done <- host.Sandboxes().Watch(ctx, func(event conductorextension.SandboxEvent) error {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
			if event.Generation == 1 && event.Kind == conductorextension.SandboxUpsert {
				once.Do(func() { close(firstSnapshot) })
				<-releaseSnapshot
			}
			if event.Generation >= 2 && event.Kind == conductorextension.SandboxSyncEnd {
				cancel()
			}
			return nil
		})
	}()
	waitSignal(t, firstSnapshot)
	for index, id := range []string{"sandbox-live-a", "sandbox-live-b"} {
		sandbox := testSandbox(id, int64(index+2))
		if err := storage.InsertSandbox(context.Background(), sandbox); err != nil {
			t.Fatal(err)
		}
		observer.SandboxUpsert(sandbox)
	}
	close(releaseSnapshot)
	if err := waitResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error=%v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	complete := make(map[uint64]bool)
	seen := make(map[uint64]map[string]bool)
	for _, event := range events {
		if event.Kind == conductorextension.SandboxSyncEnd {
			complete[event.Generation] = true
		}
		if event.Kind == conductorextension.SandboxUpsert {
			if seen[event.Generation] == nil {
				seen[event.Generation] = make(map[string]bool)
			}
			seen[event.Generation][event.SandboxID] = true
		}
	}
	if complete[1] {
		t.Fatal("overflowed first generation emitted SyncEnd")
	}
	if !complete[2] || !seen[2][seed.ID] || !seen[2]["sandbox-live-a"] || !seen[2]["sandbox-live-b"] {
		t.Fatalf("resync did not converge: complete=%v seen=%v events=%v", complete, seen, events)
	}
}

func TestBuildWatchSubscribesBeforeSnapshotAndAbandonsOverflowedGeneration(t *testing.T) {
	storage := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seed := testBuild("build-seed", types.BuildRegistered, 1)
	if err := storage.PutBuild(ctx, seed); err != nil {
		t.Fatal(err)
	}
	host, observer := newWithCapacity(storage, 1)

	firstSnapshot := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	var mu sync.Mutex
	var events []conductorextension.BuildEvent
	go func() {
		done <- host.Builds().Watch(ctx, func(event conductorextension.BuildEvent) error {
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
			if event.Generation == 1 && event.Kind == conductorextension.BuildUpsert {
				once.Do(func() { close(firstSnapshot) })
				<-releaseSnapshot
			}
			if event.Generation >= 2 && event.Kind == conductorextension.BuildSyncEnd {
				cancel()
			}
			return nil
		})
	}()
	waitSignal(t, firstSnapshot)
	for index, id := range []string{"build-live-a", "build-live-b"} {
		build := testBuild(id, types.BuildWaiting, int64(index+2))
		if err := storage.PutBuild(context.Background(), build); err != nil {
			t.Fatal(err)
		}
		observer.BuildUpsert(build)
	}
	close(releaseSnapshot)
	if err := waitResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error=%v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	complete := make(map[uint64]bool)
	seen := make(map[uint64]map[string]bool)
	for _, event := range events {
		if event.Kind == conductorextension.BuildSyncEnd {
			complete[event.Generation] = true
		}
		if event.Kind == conductorextension.BuildUpsert {
			if seen[event.Generation] == nil {
				seen[event.Generation] = make(map[string]bool)
			}
			seen[event.Generation][event.BuildID] = true
		}
	}
	if complete[1] {
		t.Fatal("overflowed first generation emitted SyncEnd")
	}
	if !complete[2] || !seen[2][seed.BuildID] || !seen[2]["build-live-a"] || !seen[2]["build-live-b"] {
		t.Fatalf("resync did not converge: complete=%v seen=%v events=%v", complete, seen, events)
	}
}

func TestSandboxWatchAllowsSnapshotLiveDuplicateAndConverges(t *testing.T) {
	storage := testStore(t)
	host, observer := newWithCapacity(storage, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	begin := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var once sync.Once
	seen := make(map[string]conductorextension.SandboxView)
	upserts := 0
	go func() {
		done <- host.Sandboxes().Watch(ctx, func(event conductorextension.SandboxEvent) error {
			if event.Kind == conductorextension.SandboxSyncBegin {
				once.Do(func() { close(begin) })
				<-release
			}
			if event.Kind == conductorextension.SandboxUpsert && event.View != nil {
				seen[event.SandboxID] = *event.View
				upserts++
				if upserts == 2 {
					cancel()
				}
			}
			return nil
		})
	}()
	waitSignal(t, begin)
	sandbox := testSandbox("snapshot-live-duplicate", 1)
	if err := storage.InsertSandbox(context.Background(), sandbox); err != nil {
		t.Fatal(err)
	}
	observer.SandboxUpsert(sandbox)
	close(release)
	if err := waitResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error=%v", err)
	}
	if upserts != 2 || seen[sandbox.ID].RunID != sandbox.RunID || len(seen) != 1 {
		t.Fatalf("duplicate convergence upserts=%d state=%+v", upserts, seen)
	}
}

func TestSlowSandboxWatcherOnlyResyncsItself(t *testing.T) {
	storage := testStore(t)
	host, observer := newWithCapacity(storage, 1)
	slowCtx, cancelSlow := context.WithCancel(context.Background())
	fastCtx, cancelFast := context.WithCancel(context.Background())
	defer cancelSlow()
	defer cancelFast()

	slowReady := make(chan struct{})
	fastReady := make(chan struct{})
	slowBlocked := make(chan struct{})
	releaseSlow := make(chan struct{})
	slowResynced := make(chan struct{})
	fastLive := make(chan string, 4)
	var slowReadyOnce, fastReadyOnce, blockedOnce, resyncOnce sync.Once
	var slowGeneration, fastGeneration uint64
	var fastMu sync.Mutex
	fastBegins := 0
	slowDone := make(chan error, 1)
	fastDone := make(chan error, 1)
	go func() {
		slowDone <- host.Sandboxes().Watch(slowCtx, func(event conductorextension.SandboxEvent) error {
			if event.Kind == conductorextension.SandboxSyncBegin && slowGeneration == 0 {
				slowGeneration = event.Generation
			}
			if event.Kind == conductorextension.SandboxSyncEnd {
				if event.Generation == slowGeneration {
					slowReadyOnce.Do(func() { close(slowReady) })
				} else {
					resyncOnce.Do(func() { close(slowResynced) })
				}
			}
			if event.Generation == slowGeneration && event.Kind == conductorextension.SandboxUpsert && event.SandboxID == "slow-live-1" {
				blockedOnce.Do(func() { close(slowBlocked) })
				<-releaseSlow
			}
			return nil
		})
	}()
	go func() {
		fastDone <- host.Sandboxes().Watch(fastCtx, func(event conductorextension.SandboxEvent) error {
			if event.Kind == conductorextension.SandboxSyncBegin {
				if fastGeneration == 0 {
					fastGeneration = event.Generation
				}
				fastMu.Lock()
				fastBegins++
				fastMu.Unlock()
			}
			if event.Kind == conductorextension.SandboxSyncEnd && event.Generation == fastGeneration {
				fastReadyOnce.Do(func() { close(fastReady) })
			}
			if event.Kind == conductorextension.SandboxUpsert && strings.HasPrefix(event.SandboxID, "slow-live-") {
				fastLive <- event.SandboxID
			}
			return nil
		})
	}()
	waitSignal(t, slowReady)
	waitSignal(t, fastReady)

	for index := 1; index <= 3; index++ {
		id := fmt.Sprintf("slow-live-%d", index)
		sandbox := testSandbox(id, int64(index))
		if err := storage.InsertSandbox(context.Background(), sandbox); err != nil {
			t.Fatal(err)
		}
		observer.SandboxUpsert(sandbox)
		if got := waitResult(t, fastLive); got != id {
			t.Fatalf("fast watcher got %q, want %q", got, id)
		}
		if index == 1 {
			waitSignal(t, slowBlocked)
		}
	}
	close(releaseSlow)
	waitSignal(t, slowResynced)
	fastMu.Lock()
	gotFastBegins := fastBegins
	fastMu.Unlock()
	if gotFastBegins != 1 {
		t.Fatalf("fast watcher generations=%d, want 1", gotFastBegins)
	}
	cancelSlow()
	cancelFast()
	if err := waitResult(t, slowDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("slow Watch error=%v", err)
	}
	if err := waitResult(t, fastDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("fast Watch error=%v", err)
	}
}

func TestWatchCallbackErrorAndContextCancellation(t *testing.T) {
	storage := testStore(t)
	host, _ := New(storage, nil)
	want := errors.New("callback stopped")
	err := host.Sandboxes().Watch(context.Background(), func(event conductorextension.SandboxEvent) error {
		if event.Kind == conductorextension.SandboxSyncBegin {
			return want
		}
		return nil
	})
	if !errors.Is(err, want) {
		t.Fatalf("callback error=%v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := host.Builds().Watch(canceled, func(conductorextension.BuildEvent) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Watch error=%v", err)
	}
	if err := host.Sandboxes().Watch(context.Background(), nil); err == nil {
		t.Fatal("nil Sandbox Watch callback succeeded")
	}
	if err := host.Builds().Watch(context.Background(), nil); err == nil {
		t.Fatal("nil Build Watch callback succeeded")
	}
}

func TestBuildWatchErrorRemovalIsLiveOnly(t *testing.T) {
	storage := testStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	current := testBuild("build-current", types.BuildBuilding, 1)
	historical := testBuild("build-historical", types.BuildError, 2)
	historical.Reason = "already failed"
	if err := storage.PutBuild(ctx, current); err != nil {
		t.Fatal(err)
	}
	if err := storage.PutBuild(ctx, historical); err != nil {
		t.Fatal(err)
	}
	host, observer := New(storage, nil)
	ready := make(chan struct{})
	removed := make(chan conductorextension.BuildEvent, 1)
	done := make(chan error, 1)
	var readyOnce sync.Once
	var snapshotIDsMu sync.Mutex
	var snapshotIDs []string
	go func() {
		done <- host.Builds().Watch(ctx, func(event conductorextension.BuildEvent) error {
			if event.Kind == conductorextension.BuildUpsert {
				snapshotIDsMu.Lock()
				snapshotIDs = append(snapshotIDs, event.BuildID)
				snapshotIDsMu.Unlock()
			}
			if event.Kind == conductorextension.BuildSyncEnd {
				readyOnce.Do(func() { close(ready) })
			}
			if event.Kind == conductorextension.BuildRemove {
				removed <- event
			}
			return nil
		})
	}()
	waitSignal(t, ready)
	failed := cloneBuildForTest(current)
	failed.Status, failed.Reason = types.BuildError, "runtime failed"
	if err := storage.PutBuild(context.Background(), failed); err != nil {
		t.Fatal(err)
	}
	observer.BuildRemove(failed)
	event := waitResult(t, removed)
	if event.BuildID != failed.BuildID || event.View == nil || event.View.State != conductorextension.BuildStateError || event.Reason != failed.Reason {
		t.Fatalf("remove event=%+v", event)
	}
	cancel()
	if err := waitResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Watch error=%v", err)
	}
	snapshotIDsMu.Lock()
	gotIDs := append([]string(nil), snapshotIDs...)
	snapshotIDsMu.Unlock()
	if !reflect.DeepEqual(gotIDs, []string{current.BuildID}) {
		t.Fatalf("initial current set=%v", gotIDs)
	}
	view, found, err := host.Builds().Get(context.Background(), failed.BuildID)
	if err != nil || !found || view.State != conductorextension.BuildStateError {
		t.Fatalf("durable error Get found=%t err=%v view=%+v", found, err, view)
	}

	resyncCtx, cancelResync := context.WithCancel(context.Background())
	var resyncIDs []string
	err = host.Builds().Watch(resyncCtx, func(event conductorextension.BuildEvent) error {
		if event.Kind == conductorextension.BuildUpsert {
			resyncIDs = append(resyncIDs, event.BuildID)
		}
		if event.Kind == conductorextension.BuildSyncEnd {
			cancelResync()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || len(resyncIDs) != 0 {
		t.Fatalf("resync error=%v ids=%v", err, resyncIDs)
	}
}

func TestWatchersReceiveIndependentLiveViewsAndPublishDoesNotRunCallbacks(t *testing.T) {
	storage := testStore(t)
	host, observer := newWithCapacity(storage, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readyA, readyB := make(chan struct{}), make(chan struct{})
	mutated := make(chan struct{})
	seen := make(chan string, 1)
	block := make(chan struct{})
	var readyAOnce, readyBOnce, mutateOnce sync.Once
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() {
		doneA <- host.Sandboxes().Watch(ctx, func(event conductorextension.SandboxEvent) error {
			if event.Kind == conductorextension.SandboxSyncEnd {
				readyAOnce.Do(func() { close(readyA) })
			}
			if event.SandboxID == "sandbox-independent" && event.View != nil {
				event.View.Metadata["key"] = "watcher-a"
				mutateOnce.Do(func() { close(mutated) })
				<-block
			}
			return nil
		})
	}()
	go func() {
		doneB <- host.Sandboxes().Watch(ctx, func(event conductorextension.SandboxEvent) error {
			if event.Kind == conductorextension.SandboxSyncEnd {
				readyBOnce.Do(func() { close(readyB) })
			}
			if event.SandboxID == "sandbox-independent" && event.View != nil {
				seen <- event.View.Metadata["key"]
			}
			return nil
		})
	}()
	waitSignal(t, readyA)
	waitSignal(t, readyB)
	sandbox := testSandbox("sandbox-independent", 1)
	returned := make(chan struct{})
	go func() {
		observer.SandboxUpsert(sandbox)
		close(returned)
	}()
	waitSignal(t, returned)
	waitSignal(t, mutated)
	if got := waitResult(t, seen); got != "sandbox" {
		t.Fatalf("watcher B observed watcher A mutation %q", got)
	}
	close(block)
	cancel()
	if err := waitResult(t, doneA); !errors.Is(err, context.Canceled) {
		t.Fatalf("watcher A error=%v", err)
	}
	if err := waitResult(t, doneB); !errors.Is(err, context.Canceled) {
		t.Fatalf("watcher B error=%v", err)
	}
}

func testStore(t *testing.T) *store.Store {
	t.Helper()
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	storage, err := store.Open(filepath.Join(t.TempDir(), "objects.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = storage.Close() })
	return storage
}

func testSandbox(id string, created int64) *types.Sandbox {
	stableID := "stable-" + id
	serviceSecret := strings.Repeat("3", 64)
	forward, err := keys.MintForwardAccessToken(serviceSecret, stableID)
	if err != nil {
		panic(err)
	}
	return &types.Sandbox{
		ID: id, Profile: types.ProfileBare, StableIDValue: stableID,
		Cluster:    &types.ClusterSandboxContext{Group: "group", RouteKey: "route"},
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
		State:      types.StateRunning, DeadlineUnix: created + 100, RunDir: "/run/" + id, BaseDir: "/base/" + id,
		RunID: "run-" + id, FloatingIP: "192.0.2.1", VswitchPort: "port-1", InnerIP: "10.0.0.1/24",
		PortMAC: "02:00:00:00:00:01", APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("2", 64),
		ResumeSource:    types.ResumeSource{SandboxRef: "manifest://eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", Kind: types.ResumeSourceSnapshot, Ref: "/snapshots/" + id},
		AutoPauseMemory: true, ServiceSecret: serviceSecret, ForwardAccessToken: forward,
		Metadata: map[string]string{"key": "sandbox"}, Env: map[string]string{"SECRET_ENV": "not projected"}, CreatedUnix: created,
	}
}

func testBuild(id string, state types.BuildState, created int64) *types.Build {
	enabled, writeback := true, false
	return &types.Build{
		BuildID: id, TemplateID: "transient-" + id, APISecret: strings.Repeat("4", 64), ManifestKey: strings.Repeat("5", 64),
		Profile: types.ProfileE2B, Kind: types.KindImg, Status: state, Reason: "reason", CreatedUnix: created,
		WaitingUnix: created + 1, ExecutionClaimed: state == types.BuildBuilding, ExecutionClaimedUnix: created + 2,
		Names: []string{"name"}, Aliases: []string{"alias"}, FromImage: "registry.example/image:tag",
		Resources: types.BuildResources{CPU: 1000, Memory: 1024, Storage: 2048},
		Steps:     []types.TemplateStep{{Type: "RUN", Args: []string{"echo", "hello"}}}, StartCmd: "start", ReadyCmd: "ready",
		Metadata: map[string]string{"key": "build"}, RunID: "run-" + id,
		Phase: "b", PhaseSandboxID: "phase-sandbox", RuntimeVswitchPort: "port-2",
		RuntimeFloatingIP: "192.0.2.2", RuntimePortMAC: "02:00:00:00:00:02", RuntimeEnvdAccessToken: "not projected",
		RuntimePrepareJSON: `{"not":"projected"}`, RegistryAuth: "not projected", ClusterGroup: "cluster-group",
		Builder: types.BuildOptions{
			Target:    &types.BuildTarget{Kind: types.BuildTargetSandbox, Memory: true},
			Resources: &types.BuildResources{CPU: 500, Memory: 512},
			Referer:   &types.BuildRefererOptions{Enabled: &enabled, Writeback: &writeback},
			Registry:  &types.BuildRegistryOptions{TLS: &types.BuildRegistryTLSOptions{CABundlePEM: "certificate"}},
		},
	}
}

func cloneBuildForTest(build *types.Build) *types.Build {
	clone := *build
	clone.Names = append([]string(nil), build.Names...)
	clone.Aliases = append([]string(nil), build.Aliases...)
	clone.Metadata = make(map[string]string, len(build.Metadata))
	for key, value := range build.Metadata {
		clone.Metadata[key] = value
	}
	clone.Steps = append([]types.TemplateStep(nil), build.Steps...)
	for index := range clone.Steps {
		clone.Steps[index].Args = append([]string(nil), build.Steps[index].Args...)
	}
	return &clone
}

func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for signal")
	}
}

func waitResult[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result")
		var zero T
		return zero
	}
}
