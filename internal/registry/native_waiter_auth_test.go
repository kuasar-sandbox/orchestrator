package registry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type nativeWaiterKey struct{}
type nativeWaiterContext struct {
	context.Context
	reads           atomic.Int32
	armed, observed atomic.Bool
	waiting         chan struct{}
}

func (c *nativeWaiterContext) Done() <-chan struct{} {
	if c.armed.Load() && c.observed.CompareAndSwap(false, true) {
		close(c.waiting)
	}
	return c.Context.Done()
}
func (c *nativeWaiterContext) Value(key any) any {
	if _, ok := key.(nativeWaiterKey); ok {
		return c
	}
	return c.Context.Value(key)
}

type nativeWaiterRoutes struct{ metadata.RouteRepository }

func (r nativeWaiterRoutes) ReadRoute(ctx context.Context, key metadata.RouteKey) (metadata.Versioned[metadata.SandboxRecord], error) {
	row, err := r.RouteRepository.ReadRoute(ctx, key)
	if waiter, ok := ctx.Value(nativeWaiterKey{}).(*nativeWaiterContext); ok && err == nil {
		// The first read precedes public authentication. The second is the
		// absent-route snapshot immediately before entering the shared wait.
		// Arm only after the native read has returned; waitCall's Done select
		// then provides a deterministic admission barrier without a sleep.
		if waiter.reads.Add(1) == 2 {
			waiter.armed.Store(true)
		}
	}
	return row, err
}

func TestRegistryNativeSharedWaitersValidateFinalPermission(t *testing.T) {
	for backend, factory := range registryNativeBackends {
		t.Run(backend, func(t *testing.T) {
			for _, condition := range []string{"different_valid_Group_credential", "incompatible_caller_Placement"} {
				t.Run(condition, func(t *testing.T) {
					r, _ := newNativeFaultRegistry(t, factory(t))
					r.parkTimeout = 3 * time.Second
					secondKey, secondEntry := registryGroupEntry(t)
					if secondKey == testAPIKeyValue() {
						t.Fatal("fixture credentials are not distinct")
					}
					reportRegistryGroup(t, r, "n1", nil, nodegroup.Support{Group: "/g", Version: 1, Entries: []nodegroup.Entry{
						{AuthRef: testAPIFingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}, secondEntry,
					}, DefaultContext: &nodegroup.DefaultContext{TemplateID: testTemplateRef}})
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					entered, release := make(chan struct{}), make(chan struct{})
					var once sync.Once
					unblock := func() { once.Do(func() { close(release) }) }
					defer unblock()
					r.stores.routes = nativeWaiterRoutes{RouteRepository: &testGroupCommit{RouteRepository: r.stores.routes, before: func(metadata.SandboxRecord) {
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
						}
					}}}
					commands := make(chan *routesync.Command, 1)
					owner := &groupCommandOwner{}
					owner.call = func(_ context.Context, _ string, cmd *routesync.Command) (*routesync.CmdAck, error) {
						commands <- cmd
						return &routesync.CmdAck{CmdID: cmd.CmdID, Status: routesync.AckAccepted}, nil
					}
					r.SetNodeOwner(owner)
					req := testCreateReserve("/g", "shared", nil)
					first := make(chan error, 1)
					go func() { _, err := r.ReserveSandbox(ctx, req); first <- err }()
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("leader did not reach the native intent boundary")
					}
					waiter := &nativeWaiterContext{Context: ctx, waiting: make(chan struct{})}
					secondReq := req
					want := ErrReserveForbidden
					if condition == "different_valid_Group_credential" {
						secondReq.APIKey = secondKey
					} else {
						secondReq.Constraints = placement.Request{NodeSelectors: []map[string]string{{"missing-pool": "selected"}}}
						want = ErrReserveConflict
					}
					second := make(chan error, 1)
					go func() { _, err := r.ReserveSandbox(waiter, secondReq); second <- err }()
					select {
					case <-waiter.waiting:
					case err := <-second:
						t.Fatalf("caller did not join shared work: %v", err)
					case <-ctx.Done():
						t.Fatal("second caller did not reach shared wait")
					}
					unblock()
					var command *routesync.Command
					select {
					case command = <-commands:
					case <-ctx.Done():
						t.Fatal("committed leader did not dispatch")
					}
					event := testE2BRoute(command.Cluster.StableID, routesync.StateRunning)
					event.SandboxID, event.TemplateID = command.SID, testTemplateRef
					if err := r.applyRoute(testNodeSession(ctx, r.stores, "n1"), "n1", &event); err != nil {
						t.Fatal(err)
					}
					if err := <-first; err != nil {
						t.Fatalf("authorized leader failed: %v", err)
					}
					if err := <-second; !errors.Is(err, want) {
						t.Fatalf("waiter inherited another caller's permission: %v", err)
					}
					if len(owner.commands) != 1 {
						t.Fatal("waiter dispatched a second execution")
					}
					t.Log("F12: both callers passed initial Group auth and joined one flight; final exact-object permission rejected the incompatible waiter")
				})
			}
		})
	}
}
