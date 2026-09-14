package telemetry

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func testRoute(id string) routesync.RouteEntry {
	return routesync.RouteEntry{SandboxID: id, StableID: "stable-" + id, Profile: "e2b", State: routesync.StateRunning, EnvdUDS: "/envd/" + id + ".sock", EnvdAccessToken: "token-" + id, FloatingIP: "127.0.0.1"}
}
func upsert(t testing.TB, view *View, route routesync.RouteEntry) *target {
	t.Helper()
	if err := view.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
	view.mu.RLock()
	defer view.mu.RUnlock()
	return view.byID[route.SandboxID]
}

func TestViewFullSyncLifecycleAndCancellation(t *testing.T) {
	v := NewView(10)
	schedule := newScrapeSchedule(v, 5*time.Second)
	route := testRoute("sandbox")
	v.BeginSync()
	first := upsert(t, v, route)
	if _, ok := v.ByFloatingIP(route.FloatingIP); ok {
		t.Fatal("identity before bookmark")
	}
	if jobs := schedule.takeDue(time.Now().Add(time.Hour), 10); len(jobs) != 0 {
		t.Fatal("scrape before bookmark")
	}
	v.Bookmark()
	if id, ok := v.ByFloatingIP("::ffff:127.0.0.1"); !ok || id != route.SandboxID {
		t.Fatalf("identity = %q %v", id, ok)
	}
	jobs := schedule.takeDue(time.Now().Add(time.Hour), 10)
	if len(jobs) != 1 || jobs[0] != first {
		t.Fatalf("initial jobs = %v", jobs)
	}
	if jobs := schedule.takeDue(time.Now().Add(2*time.Hour), 10); len(jobs) != 0 {
		t.Fatal("overlapping scrape")
	}
	schedule.finished(first)
	route.State = routesync.StatePaused
	upsert(t, v, route)
	if first.ctx.Err() == nil {
		t.Fatal("paused target not cancelled")
	}
	if err := v.withCurrent(first, func() error { t.Fatal("stale delivery"); return nil }); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
	if _, ok := v.ByFloatingIP(route.FloatingIP); ok {
		t.Fatal("paused identity")
	}
	if jobs := schedule.takeDue(time.Now().Add(time.Hour), 10); len(jobs) != 0 {
		t.Fatal("paused scrape")
	}
	route.State = routesync.StateRunning
	resumed := upsert(t, v, route)
	if resumed.generation == first.generation {
		t.Fatal("local generation reused")
	}
	if len(schedule.takeDue(time.Now().Add(time.Hour), 10)) != 1 {
		t.Fatal("resume did not restart")
	}
	v.ApplyDelete(route.SandboxID)
	if resumed.ctx.Err() == nil {
		t.Fatal("delete did not cancel")
	}
	if _, ok := v.ByFloatingIP(route.FloatingIP); ok {
		t.Fatal("deleted identity")
	}
	upsert(t, v, route)
	v.InvalidateSync()
	if _, ok := v.ByFloatingIP(route.FloatingIP); ok {
		t.Fatal("disconnected identity")
	}
	v.BeginSync()
	v.Bookmark()
	if len(v.byID) != 0 {
		t.Fatal("old full-sync target survived")
	}
}

func TestViewTargetChangeRemapAndRunIdentityExcluded(t *testing.T) {
	v := NewView(10)
	route := testRoute("old")
	first := upsert(t, v, route)
	v.Bookmark()
	route.RunID = "runner-restarted"
	if got := upsert(t, v, route); got != first {
		t.Fatal("runner identity changed telemetry target")
	}
	route.EnvdUDS = "/new.sock"
	second := upsert(t, v, route)
	if first.ctx.Err() == nil || first == second {
		t.Fatal("target replacement not cancelled")
	}
	successor := testRoute("new")
	upsert(t, v, successor)
	if second.ctx.Err() == nil {
		t.Fatal("FloatingIP remap did not revoke persistent peer lease")
	}
	if id, ok := v.ByFloatingIP(successor.FloatingIP); !ok || id != "new" {
		t.Fatalf("remap = %s %v", id, ok)
	}
	v.ApplyDelete("old")
	if id, ok := v.ByFloatingIP(successor.FloatingIP); !ok || id != "new" {
		t.Fatal("late delete removed successor")
	}
	successor.FloatingIP = "127.0.0.2"
	upsert(t, v, successor)
	if _, ok := v.ByFloatingIP("127.0.0.1"); ok {
		t.Fatal("stale reverse index")
	}
	for _, ip := range []string{"not-ip", "::1", "127.0.0.3"} {
		if _, ok := v.ByFloatingIP(ip); ok {
			t.Fatal(ip)
		}
	}
}

func TestScrapeScheduleReplacementDoesNotCompleteSuccessor(t *testing.T) {
	v := NewView(1)
	defer v.InvalidateSync()
	fast := newScrapeSchedule(v, time.Second)
	slow := newScrapeSchedule(v, 10*time.Second)
	defer fast.detach()
	defer slow.detach()
	route := testRoute("one")
	first := upsert(t, v, route)
	v.Bookmark()
	now := time.Now().Add(time.Minute)
	for _, schedule := range []*scrapeSchedule{fast, slow} {
		if jobs := schedule.takeDue(now, 1); len(jobs) != 1 || jobs[0] != first {
			t.Fatal("receiver instance missed initial target", jobs)
		}
	}
	// A duplicate bookmark must not turn a still-running job into an idle one.
	v.Bookmark()
	if jobs := fast.takeDue(now.Add(time.Minute), 1); len(jobs) != 0 {
		t.Fatal("bookmark allowed overlapping scrape", jobs)
	}
	route.EnvdUDS = "/replacement.sock"
	second := upsert(t, v, route)
	for _, schedule := range []*scrapeSchedule{fast, slow} {
		if jobs := schedule.takeDue(now.Add(2*time.Minute), 1); len(jobs) != 1 || jobs[0] != second {
			t.Fatal("receiver instance missed replacement", jobs)
		}
		schedule.finished(first)
		if jobs := schedule.takeDue(now.Add(3*time.Minute), 1); len(jobs) != 0 {
			t.Fatal("late completion released successor", jobs)
		}
	}
	fast.finished(second)
	if len(fast.takeDue(now.Add(4*time.Minute), 1)) != 1 || len(slow.takeDue(now.Add(4*time.Minute), 1)) != 0 {
		t.Fatal("receiver instances shared in-flight state")
	}
	v.BeginSync()
	if len(fast.takeDue(now.Add(5*time.Minute), 1)) != 0 || len(slow.takeDue(now.Add(5*time.Minute), 1)) != 0 {
		t.Fatal("disconnected schedules retained targets")
	}
	fast.detach()
	v.mu.RLock()
	_, retained := v.schedules[fast]
	v.mu.RUnlock()
	if retained {
		t.Fatal("stopped receiver retained schedule registration")
	}
}

func TestViewScrapeEligibilityAndCapacity(t *testing.T) {
	for _, change := range []func(*routesync.RouteEntry){func(r *routesync.RouteEntry) { r.Profile = "custom" }, func(r *routesync.RouteEntry) { r.State = routesync.StateStarting }, func(r *routesync.RouteEntry) { r.EnvdUDS = "" }, func(r *routesync.RouteEntry) { r.StableID = "" }} {
		v := NewView(1)
		schedule := newScrapeSchedule(v, 5*time.Second)
		route := testRoute("one")
		change(&route)
		upsert(t, v, route)
		v.Bookmark()
		if len(schedule.takeDue(time.Now().Add(time.Hour), 1)) != 0 {
			t.Fatal("ineligible scrape")
		}
		if err := v.ApplyUpsert(testRoute("two")); err == nil {
			t.Fatal("capacity not enforced")
		}
	}
}

func TestViewLinearizesAcceptanceWithInvalidation(t *testing.T) {
	v := NewView(1)
	entry := upsert(t, v, testRoute("one"))
	v.Bookmark()
	started, release, delivered := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(delivered)
		_ = v.withCurrent(entry, func() error { close(started); <-release; return nil })
	}()
	<-started
	invalidated := make(chan struct{})
	go func() { v.InvalidateSync(); close(invalidated) }()
	select {
	case <-invalidated:
		t.Fatal("invalidation passed active acceptance validation")
	default:
	}
	close(release)
	<-delivered
	<-invalidated
	if err := v.withCurrent(entry, func() error { return nil }); !errors.Is(err, ErrIdentity) {
		t.Fatal(err)
	}
}

func BenchmarkScrapeScheduler(b *testing.B) {
	for _, count := range []int{100, 1000, 10000, 50000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			before := runtime.NumGoroutine()
			v := NewView(count)
			schedule := newScrapeSchedule(v, 5*time.Second)
			for n := 0; n < count; n++ {
				r := testRoute(fmt.Sprint(n))
				r.FloatingIP = ""
				upsert(b, v, r)
			}
			v.Bookmark()
			now := time.Now().Add(5 * time.Second)
			targetGoroutines := runtime.NumGoroutine() - before
			b.ReportAllocs()
			b.ResetTimer()
			for n := 0; n < b.N; n++ {
				for processed := 0; processed < count; {
					jobs := schedule.takeDue(now, 64)
					if len(jobs) == 0 {
						b.Fatal("missing due target")
					}
					for _, job := range jobs {
						schedule.finished(job)
					}
					processed += len(jobs)
				}
				now = now.Add(5 * time.Second)
			}
			b.StopTimer()
			b.ReportMetric(float64(targetGoroutines), "target-goroutines")
			v.InvalidateSync()
		})
	}
}

func BenchmarkScrapeSchedulerRouteChange(b *testing.B) {
	for _, count := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			view := NewView(count)
			defer view.InvalidateSync()
			schedule := newScrapeSchedule(view, 5*time.Second)
			for i := 0; i < count; i++ {
				route := testRoute(fmt.Sprint(i))
				route.FloatingIP = ""
				upsert(b, view, route)
			}
			view.Bookmark()
			schedule.takeDue(time.Now(), 0)
			route := testRoute("0")
			route.FloatingIP = ""
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				route.EnvdAccessToken = fmt.Sprint(i)
				upsert(b, view, route)
				schedule.takeDue(time.Now(), 0)
			}
		})
	}
}
