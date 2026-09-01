package orch

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type fakeRouteBarrierCoordinator struct {
	barrier *fakeRouteBarrier
	err     error
}

func (c *fakeRouteBarrierCoordinator) BeginProxyRouteBarrier() (routesync.RouteBarrier, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.barrier, nil
}

type fakeRouteBarrier struct {
	id        string
	releaseCh chan struct{}
	waiting   chan struct{}
	canceled  chan struct{}
	waitErr   error
	commitErr error

	releaseOnce sync.Once
	waitOnce    sync.Once
	cancelOnce  sync.Once
	mu          sync.Mutex
	committed   bool
}

func newFakeRouteBarrier(waitErr, commitErr error) *fakeRouteBarrier {
	return &fakeRouteBarrier{
		id: "test-route-barrier", releaseCh: make(chan struct{}),
		waiting: make(chan struct{}), canceled: make(chan struct{}),
		waitErr: waitErr, commitErr: commitErr,
	}
}

func (b *fakeRouteBarrier) ID() string { return b.id }

func (b *fakeRouteBarrier) Wait(ctx context.Context) error {
	b.waitOnce.Do(func() { close(b.waiting) })
	select {
	case <-b.releaseCh:
		return b.waitErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *fakeRouteBarrier) Commit() error {
	if b.commitErr != nil {
		return b.commitErr
	}
	b.mu.Lock()
	b.committed = true
	b.mu.Unlock()
	return nil
}

func (b *fakeRouteBarrier) Cancel() {
	b.mu.Lock()
	committed := b.committed
	b.mu.Unlock()
	if !committed {
		b.cancelOnce.Do(func() { close(b.canceled) })
	}
}

func (b *fakeRouteBarrier) release() { b.releaseOnce.Do(func() { close(b.releaseCh) }) }

func receiveCreateRouteEvent(t *testing.T, events <-chan routesync.Event, want string) routesync.Event {
	t.Helper()
	select {
	case event := <-events:
		if event.Kind != want {
			t.Fatalf("route event = %+v, want kind %s", event, want)
		}
		return event
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for route event %s", want)
		return routesync.Event{}
	}
}

func TestExternalCreateWaitsForRouteBarrierBeforeLaunch(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Mode = config.ProxyExternal
	cfg.Proxy.ParkTimeout = "1s"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
	blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
	o.vs = blocked
	barrier := newFakeRouteBarrier(nil, nil)
	o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
	events, stopEvents := o.Subscribe()
	defer stopEvents()

	type result struct {
		sb  *types.Sandbox
		err error
	}
	req := createRequestFixture(t, o, "a")
	done := make(chan result, 1)
	go func() {
		sb, err := o.Create(ctx, req)
		done <- result{sb: sb, err: err}
	}()
	upsert := receiveCreateRouteEvent(t, events, routesync.TypeUpsert)
	barrierEvent := receiveCreateRouteEvent(t, events, routesync.TypeRouteBarrier)
	if barrierEvent.BarrierID != barrier.ID() || upsert.Route.SandboxID == "" {
		t.Fatalf("ordered admission events = %+v, %+v", upsert, barrierEvent)
	}
	select {
	case <-barrier.waiting:
	case <-time.After(time.Second):
		t.Fatal("Create did not wait for route barrier")
	}
	select {
	case <-blocked.entered:
		t.Fatal("resource preparation started before route ACK")
	case <-time.After(20 * time.Millisecond):
	}

	barrier.release()
	var created result
	select {
	case created = <-done:
		if created.err != nil || created.sb == nil || created.sb.ID != upsert.Route.SandboxID {
			t.Fatalf("Create = %+v, %v", created.sb, created.err)
		}
	case <-time.After(time.Second):
		t.Fatal("Create did not return after route ACK")
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("launch did not start after route ACK")
	}
	close(blocked.gate)
	waitForSandbox(t, o, ctx, created.sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning
	}, "running after route ACK")
}

func TestExternalCreateBarrierFailureDeletesPreLaunchAdmission(t *testing.T) {
	for _, test := range []struct {
		name      string
		waitErr   error
		commitErr error
	}{
		{name: "disconnect", waitErr: configsock.ErrProxyRouteDisconnected},
		{name: "commit lease change", commitErr: configsock.ErrProxyRouteLeaseChanged},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Proxy.Mode = config.ProxyExternal
			cfg.Proxy.ParkTimeout = "1s"
			o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
			blocked := &blockedCreateVS{entered: make(chan struct{}), gate: make(chan struct{})}
			o.vs = blocked
			barrier := newFakeRouteBarrier(test.waitErr, test.commitErr)
			barrier.release()
			o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
			events, stopEvents := o.Subscribe()
			defer stopEvents()

			created, err := o.Create(ctx, createRequestFixture(t, o, "b"))
			if created != nil || !errors.Is(err, api.ErrProxyUnavailable) {
				t.Fatalf("Create = %+v, %v", created, err)
			}
			upsert := receiveCreateRouteEvent(t, events, routesync.TypeUpsert)
			receiveCreateRouteEvent(t, events, routesync.TypeRouteBarrier)
			deleted := receiveCreateRouteEvent(t, events, routesync.TypeDelete)
			if deleted.SID != upsert.Route.SandboxID {
				t.Fatalf("Delete = %+v, initial = %+v", deleted, upsert)
			}
			stored, getErr := o.st.Get(ctx, deleted.SID)
			if getErr != nil || stored != nil || o.lookup(deleted.SID) != nil {
				t.Fatalf("failed admission retained state: store=%+v cache=%+v err=%v", stored, o.lookup(deleted.SID), getErr)
			}
			if _, found := o.launches.Lookup(deleted.SID); found {
				t.Fatal("failed admission retained launch claim")
			}
			select {
			case <-blocked.entered:
				t.Fatal("failed route barrier started resource preparation")
			default:
			}
		})
	}
}

func TestPreLaunchRollbackRefreshesCleanupContextUntilExactDelete(t *testing.T) {
	o := testOrch(t)
	manifestKey := strings.Repeat("a", 64)
	sb := &types.Sandbox{
		ID: "retry-pre-launch-delete", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:      types.StateStarting,
		LaunchMode: types.LaunchImage,
		APISecret:  deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "base"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.InsertSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	attempts := 0
	err := o.rollbackPreLaunchAdmissionWith(sb.ID, func() (context.Context, context.CancelFunc) {
		attempts++
		ctx, cancel := context.WithCancel(context.Background())
		if attempts == 1 {
			cancel()
		}
		return ctx, cancel
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("rollback error = %v, want retained first transient error", err)
	}
	if attempts != 2 {
		t.Fatalf("cleanup context attempts = %d, want 2", attempts)
	}
	stored, getErr := o.st.Get(context.Background(), sb.ID)
	if getErr != nil || stored != nil || o.lookup(sb.ID) != nil {
		t.Fatalf("pre-launch rollback retained state: store=%+v cache=%+v err=%v", stored, o.lookup(sb.ID), getErr)
	}
}

func TestExternalCreateBarrierTimeoutAndCancellationRollback(t *testing.T) {
	for _, test := range []struct {
		name            string
		park            string
		cancelRequest   bool
		cancelLifecycle bool
	}{
		{name: "timeout", park: "20ms"},
		{name: "request cancel", park: "1s", cancelRequest: true},
		{name: "lifecycle cancel", park: "1s", cancelLifecycle: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Proxy.Mode = config.ProxyExternal
			cfg.Proxy.ParkTimeout = test.park
			o, lifecycleCtx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
			barrier := newFakeRouteBarrier(nil, nil)
			o.SetProxyRouteBarrierCoordinator(&fakeRouteBarrierCoordinator{barrier: barrier})
			var cancelLifecycle context.CancelFunc
			if test.cancelLifecycle {
				var lifecycle context.Context
				lifecycle, cancelLifecycle = context.WithCancel(context.Background())
				o.SetLifecycleContext(lifecycle)
				defer cancelLifecycle()
			}
			events, stopEvents := o.Subscribe()
			defer stopEvents()
			requestCtx, cancelRequest := context.WithCancel(lifecycleCtx)
			defer cancelRequest()
			req := createRequestFixture(t, o, "c")

			done := make(chan error, 1)
			go func() {
				_, err := o.Create(requestCtx, req)
				done <- err
			}()
			upsert := receiveCreateRouteEvent(t, events, routesync.TypeUpsert)
			receiveCreateRouteEvent(t, events, routesync.TypeRouteBarrier)
			if test.cancelRequest {
				cancelRequest()
			}
			if test.cancelLifecycle {
				cancelLifecycle()
			}
			select {
			case err := <-done:
				if !errors.Is(err, api.ErrProxyUnavailable) {
					t.Fatalf("Create error = %v", err)
				}
				if (test.cancelRequest || test.cancelLifecycle) && !errors.Is(err, context.Canceled) {
					t.Fatalf("Create cancel error = %v", err)
				}
				if !test.cancelRequest && !test.cancelLifecycle && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("Create timeout error = %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Create did not finish after barrier failure")
			}
			deleted := receiveCreateRouteEvent(t, events, routesync.TypeDelete)
			if deleted.SID != upsert.Route.SandboxID {
				t.Fatalf("Delete = %+v, initial = %+v", deleted, upsert)
			}
			stored, err := o.st.Get(lifecycleCtx, deleted.SID)
			if err != nil || stored != nil {
				t.Fatalf("barrier failure retained row = %+v, %v", stored, err)
			}
		})
	}
}

func TestExternalCreateWithoutProxyRejectsBeforeInsert(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Mode = config.ProxyExternal
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, &countingLauncher{})
	created, err := o.Create(ctx, createRequestFixture(t, o, "d"))
	if created != nil || !errors.Is(err, api.ErrProxyUnavailable) {
		t.Fatalf("Create = %+v, %v", created, err)
	}
	count := 0
	if err := o.st.RangeByState(ctx, types.StateStarting, func(*types.Sandbox) error {
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("Create without proxy inserted %d rows", count)
	}
}
