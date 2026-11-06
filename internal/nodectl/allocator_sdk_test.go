package nodectl

import (
	"context"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandboxer/pkg/resctl"
)

// Keep the production wire server and sandboxer's actual reservation adapter in
// this regression. Only allocator time advances virtually; no long sleeps or
// alternate resource parser/allocator are involved.
func TestSDKGrowAccumulatesPoolReservationsAtSmallPositiveRates(t *testing.T) {
	for _, factor := range []float64{0.05, 1.0 / 1024, 1.0 / (1 << 30)} {
		for _, urgency := range []string{UrgencyNormal, UrgencyLow} {
			t.Run(fmt.Sprintf("%g/%s", factor, urgency), func(t *testing.T) {
				small, large := "1GiB", "2GiB"
				cfg := &config.ResourceListenConfig{Pools: []config.ResourcePoolConfig{
					{Name: "small", Allocatable: config.ResourceAllocatable{Memory: &small}},
					{Name: "large", Allocatable: config.ResourceAllocatable{Memory: &large}},
				}}
				cfg.RateLimits.MemoryGrantPerSecFactor = factor
				r, err := Resolve(cfg)
				if err != nil {
					t.Fatal(err)
				}
				state := NewPoolState(r)
				var elapsed atomic.Int64
				now := func() time.Time { return time.Unix(0, elapsed.Load()) }
				for _, p := range state.PoolStates() {
					p.poolAllocator.clock, p.poolAllocator.lastRefill = now, now()
					p.pressure.clock = now
				}
				dir := shortTestDir(t)
				socket, cgroup := filepath.Join(dir, "ctl.sock"), filepath.Join(dir, "cgroup")
				srv := &Server{Path: socket, State: state, Admission: NewAdmissionController(r.Admission), Allocator: NewAllocator(r.Allocator),
					Inventory: &Inventory{ControllerSocket: socket, CgroupScanPaths: []string{cgroup}}, Logf: t.Logf}
				srv.Admission.SetWiring(state, nil, srv.BuildAdmitOKFromQueue)
				if err := srv.Listen(); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				done := make(chan struct{})
				go func() { defer close(done); _ = srv.Serve(ctx) }()
				defer func() { cancel(); <-done }()
				for _, name := range []string{"small", "large"} {
					placePoolTest(t, state, name, name, Resources{MemoryBytes: 512 << 20, CPUMilli: 1000})
					sandbox := recoverySandboxConfig(socket, cgroup)
					hooks, err := resctl.NewControllerHooks(resctl.ControllerHookOptions{SocketPath: socket, CgroupPath: cgroup, SandboxID: name, Context: ctx}, sandbox)
					if err != nil {
						t.Fatal(err)
					}
					defer hooks.Release("test complete")
					if budget, err := hooks.Admit(name, 0); err != nil || budget != 256<<20 {
						t.Fatalf("initial budget=%d err=%v", budget, err)
					}
					if err := hooks.Settled(); err != nil {
						t.Fatal(err)
					}
					const goal = uint64(320 << 20)
					for attempts := 0; attempts < 100 && hooks.ReservationMemory() < goal; attempts++ {
						baseline := hooks.ReservationMemory()
						granted, reservation, cooldown, err := hooks.RequestBudget(baseline, goal-baseline, urgency, "small-pool-regression")
						if err != nil || reservation != baseline+granted || hooks.ReservationMemory() != reservation {
							t.Fatalf("SDK grant=%d reservation=%d err=%v", granted, reservation, err)
						}
						if p := state.PoolStates()[name].PressureSnapshot(); p.PauseEligible || len(p.Demands) != 0 {
							t.Fatalf("SDK token wait became pressure: %+v", p)
						}
						elapsed.Add(int64(cooldown))
					}
					if hooks.ReservationMemory() != goal {
						t.Fatal("SDK grow stalled")
					}
					hooks.Release("completed pool")
					if state.PoolStates()[name].ResourceSnapshot().Reserved.MemoryBytes != 0 {
						t.Fatal("SDK release leaked reservation")
					}
				}
			})
		}
	}
}
