package proxyshm

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestTableSharedLookupAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	master, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	worker, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	entry := routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning,
		TemplateID: "tmpl", EnvdUDS: "/run/s1/envd.sock", CiUDS: "/run/s1/ci.sock",
		FloatingIP: "100.100.0.2", AuthSandboxID: "stable-s1",
		APISecret: strings.Repeat("1", 64), APISecretFingerprint: strings.Repeat("2", 64),
		ManifestKeyFingerprint: strings.Repeat("3", 64), ServiceSecret: strings.Repeat("4", 64),
		EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		SnapshotLocation: "remote", MmdsSecret: hex.EncodeToString([]byte("mmds")),
	}
	master.BeginSync()
	if err := master.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	master.Bookmark()

	got, ok := worker.Lookup("s1")
	if !ok || got != entry {
		t.Fatalf("lookup = %+v ok=%v", got, ok)
	}
	_, _, liveRev := worker.LookupRevision("s1")
	idx, ok := master.findSlot("s1", false)
	if !ok {
		t.Fatal("route slot not found before delete")
	}
	if !master.Delete("s1") {
		t.Fatal("delete returned false")
	}
	if _, ok := worker.Lookup("s1"); ok {
		t.Fatal("worker still sees deleted route")
	}
	if deleted, found, rev := worker.LookupRevision("s1"); found || deleted != (routesync.RouteEntry{}) || rev <= liveRev {
		t.Fatalf("deleted lookup = %+v found=%v rev=%d, live rev=%d", deleted, found, rev, liveRev)
	}
	deleted, status, ok := readRecord(&master.records[idx])
	if !ok || status != statusEmpty {
		t.Fatalf("deleted record status=%d ok=%v", status, ok)
	}
	if deleted.SandboxID != "" || deleted.AuthSandboxID != "" || deleted.APISecret != "" || deleted.APISecretFingerprint != "" ||
		deleted.ManifestKeyFingerprint != "" || deleted.ServiceSecret != "" ||
		deleted.EnvdAccessToken != "" || deleted.TrafficAccessToken != "" ||
		deleted.ForwardAccessToken != "" || deleted.MmdsSecret != "" {
		t.Fatalf("deleted record retained credential material: %+v", deleted)
	}
}

func TestTableLookupRevisionTracksLiveAndDeletedSnapshots(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	route := routesync.RouteEntry{SandboxID: "revision-snapshot", State: routesync.StatePaused}
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	paused, found, pausedRev := tbl.LookupRevision(route.SandboxID)
	if !found || paused.State != routesync.StatePaused || pausedRev == 0 {
		t.Fatalf("paused snapshot = %+v, found=%v rev=%d", paused, found, pausedRev)
	}

	route.State = routesync.StateStarting
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	starting, found, startingRev := tbl.LookupRevision(route.SandboxID)
	if !found || starting.State != routesync.StateStarting || startingRev <= pausedRev {
		t.Fatalf("starting snapshot = %+v, found=%v rev=%d; paused rev=%d", starting, found, startingRev, pausedRev)
	}
	if got := tbl.RouteRev(route.SandboxID); got != startingRev {
		t.Fatalf("RouteRev = %d, want atomic lookup rev %d", got, startingRev)
	}

	if !tbl.Delete(route.SandboxID) {
		t.Fatal("delete returned false")
	}
	deleted, found, deletedRev := tbl.LookupRevision(route.SandboxID)
	if found || deleted != (routesync.RouteEntry{}) || deletedRev <= startingRev {
		t.Fatalf("deleted snapshot = %+v, found=%v rev=%d; starting rev=%d", deleted, found, deletedRev, startingRev)
	}

	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	running, found, runningRev := tbl.LookupRevision(route.SandboxID)
	if !found || running.State != routesync.StateRunning || runningRev <= deletedRev {
		t.Fatalf("running snapshot = %+v, found=%v rev=%d; deleted rev=%d", running, found, runningRev, deletedRev)
	}
}

func TestTableIdenticalUpsertKeepsLifecycleRevision(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	route := routesync.RouteEntry{SandboxID: "duplicate-paused", State: routesync.StatePaused}
	tbl.BeginSync()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	_, found, routeRev := tbl.LookupRevision(route.SandboxID)
	if !found || routeRev == 0 {
		t.Fatalf("initial route found=%v rev=%d", found, routeRev)
	}
	globalRev := tbl.Rev()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if _, found, got := tbl.LookupRevision(route.SandboxID); !found || got != routeRev || tbl.Rev() != globalRev {
		t.Fatalf("same-generation replay found=%v route_rev=%d global_rev=%d; want %d/%d",
			found, got, tbl.Rev(), routeRev, globalRev)
	}

	// An identical route in a new Range generation must refresh SyncGen so the
	// Bookmark retains it, but that replay still is not a lifecycle transition.
	tbl.BeginSync()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if _, found, got := tbl.LookupRevision(route.SandboxID); !found || got != routeRev || tbl.Rev() != globalRev {
		t.Fatalf("new-generation replay found=%v route_rev=%d global_rev=%d; want %d/%d",
			found, got, tbl.Rev(), routeRev, globalRev)
	}
	tbl.Bookmark()
	if got, found := tbl.Lookup(route.SandboxID); !found || got != route {
		t.Fatalf("identical replay was swept at Bookmark: %+v found=%v", got, found)
	}
}

func TestTableDeleteBackshiftPreservesCollidingRoutes(t *testing.T) {
	const capacity = 16
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	byHome := make(map[uint64][]string)
	var colliding []string
	for i := 0; len(colliding) < 6; i++ {
		sid := fmt.Sprintf("collision-%d", i)
		home := hashSID(sid) % capacity
		byHome[home] = append(byHome[home], sid)
		if len(byHome[home]) == 6 {
			colliding = byHome[home]
		}
	}
	for _, sid := range colliding {
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateRunning}); err != nil {
			t.Fatal(err)
		}
	}
	deletedSID := colliding[2]
	if !tbl.Delete(deletedSID) {
		t.Fatal("delete returned false")
	}
	if _, found := tbl.Lookup(deletedSID); found {
		t.Fatalf("deleted colliding route %q remains live", deletedSID)
	}
	for _, sid := range append(colliding[:2], colliding[3:]...) {
		if got, found := tbl.Lookup(sid); !found || got.SandboxID != sid {
			t.Fatalf("colliding route %q lost after backshift: %+v found=%v", sid, got, found)
		}
	}
}

func TestTableDeleteChurnLeavesLiveProbeTableReclaimable(t *testing.T) {
	const capacity = 16
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	for i := 0; i < 1000; i++ {
		sid := fmt.Sprintf("churn-%d", i)
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateStarting}); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
		if !tbl.Delete(sid) {
			t.Fatalf("delete %d returned false", i)
		}
		// Unknown-Wake terminal correlation also stays entirely outside the live
		// probe table, even under valid-length random-SID churn.
		tbl.Delete(fmt.Sprintf("unknown-%d", i))
	}
	for i := range tbl.records {
		if status := atomic.LoadUint32(&tbl.records[i].Status); status != statusEmpty {
			t.Fatalf("live record %d retained status %d after churn", i, status)
		}
	}
	if len(tbl.terminals) != capacity {
		t.Fatalf("terminal cache capacity = %d, want bounded %d", len(tbl.terminals), capacity)
	}
	if got := terminalCapacity(defaultCapacity); got != maxTerminalRevisions {
		t.Fatalf("default terminal cache capacity = %d, want bounded %d", got, maxTerminalRevisions)
	}
}

func TestTableBackshiftMatchesMapUnderRandomChurn(t *testing.T) {
	const capacity = 17
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	rng := rand.New(rand.NewSource(135))
	want := make(map[string]routesync.RouteEntry)
	allSIDs := make([]string, capacity*4)
	for i := range allSIDs {
		allSIDs[i] = fmt.Sprintf("model-%d", i)
	}
	for step := 0; step < 10000; step++ {
		sid := allSIDs[rng.Intn(len(allSIDs))]
		if _, found := want[sid]; found || len(want) == capacity || rng.Intn(3) == 0 {
			tbl.Delete(sid)
			delete(want, sid)
		} else {
			route := routesync.RouteEntry{
				SandboxID: sid,
				State:     []string{routesync.StateStarting, routesync.StateRunning, routesync.StatePaused}[step%3],
				Profile:   fmt.Sprintf("p-%d", step),
			}
			if err := tbl.Upsert(route); err != nil {
				t.Fatalf("step %d upsert %q with %d live routes: %v", step, sid, len(want), err)
			}
			want[sid] = route
		}
		for _, probe := range allSIDs {
			got, found := tbl.Lookup(probe)
			expected, expectedFound := want[probe]
			if found != expectedFound || found && got != expected {
				t.Fatalf("step %d lookup %q = %+v found=%v, want %+v found=%v",
					step, probe, got, found, expected, expectedFound)
			}
		}
	}
}

func TestTableLookupRevisionNeverMixesConcurrentRouteAndRevision(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	const sid = "revision-race"
	firstWritten := make(chan struct{})
	continueWriter := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		route := routesync.RouteEntry{SandboxID: sid, State: routesync.StateStarting}
		for i := 0; i < 5000; i++ {
			nextRev := tbl.Rev() + 1
			route.TemplateID = strconv.FormatUint(nextRev, 10)
			if i%2 == 0 {
				route.State = routesync.StateStarting
			} else {
				route.State = routesync.StateRunning
			}
			if err := tbl.Upsert(route); err != nil {
				done <- err
				return
			}
			if i == 0 {
				close(firstWritten)
				<-continueWriter
			}
			runtime.Gosched()
		}
		done <- nil
	}()
	select {
	case <-firstWritten:
	case err := <-done:
		t.Fatalf("first route write: %v", err)
	}
	firstRoute, found, firstRev := tbl.LookupRevision(sid)
	close(continueWriter)
	if !found || firstRoute.TemplateID != strconv.FormatUint(firstRev, 10) {
		writerErr := <-done
		if writerErr != nil {
			t.Fatal(writerErr)
		}
		t.Fatalf("invalid first route/revision snapshot: route=%+v found=%v rev=%d", firstRoute, found, firstRev)
	}

	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
		route, found, rev := tbl.LookupRevision(sid)
		if found && route.TemplateID != strconv.FormatUint(rev, 10) {
			writerErr := <-done
			if writerErr != nil {
				t.Fatal(writerErr)
			}
			t.Fatalf("mixed route/revision snapshot: route=%+v rev=%d", route, rev)
		}
		runtime.Gosched()
	}
}

func TestTableSupportsPortableTemplateID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	templateID := types.TemplateID{
		Profile: types.ProfileE2B,
		Kind:    types.KindSnp,
		Ref: "file://" + strings.Repeat("a", 64) + ".snapshot@location:" +
			"019fb713-6fd5-71f0-b9bc-4a2a20147e53",
	}.String()
	if len(templateID) <= 128 {
		t.Fatalf("portable template ID length = %d, want >128", len(templateID))
	}
	entry := routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateRunning, TemplateID: templateID,
	}
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	got, ok := tbl.Lookup("s1")
	if !ok || got.TemplateID != templateID {
		t.Fatalf("template ID round-trip = %q ok=%v", got.TemplateID, ok)
	}
}

func TestTableCredentialFieldBoundariesAndOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	tests := []struct {
		name string
		max  int
		set  func(*routesync.RouteEntry, string)
		get  func(routesync.RouteEntry) string
	}{
		{"auth_sandbox_id", maxSandboxID, func(r *routesync.RouteEntry, v string) { r.AuthSandboxID = v }, func(r routesync.RouteEntry) string { return r.AuthSandboxID }},
		{"api_secret", maxSecret, func(r *routesync.RouteEntry, v string) { r.APISecret = v }, func(r routesync.RouteEntry) string { return r.APISecret }},
		{"api_secret_fingerprint", maxFingerprint, func(r *routesync.RouteEntry, v string) { r.APISecretFingerprint = v }, func(r routesync.RouteEntry) string { return r.APISecretFingerprint }},
		{"manifest_key_fingerprint", maxFingerprint, func(r *routesync.RouteEntry, v string) { r.ManifestKeyFingerprint = v }, func(r routesync.RouteEntry) string { return r.ManifestKeyFingerprint }},
		{"service_secret", maxSecret, func(r *routesync.RouteEntry, v string) { r.ServiceSecret = v }, func(r routesync.RouteEntry) string { return r.ServiceSecret }},
		{"envd_access_token", maxAccessToken, func(r *routesync.RouteEntry, v string) { r.EnvdAccessToken = v }, func(r routesync.RouteEntry) string { return r.EnvdAccessToken }},
		{"traffic_access_token", maxAccessToken, func(r *routesync.RouteEntry, v string) { r.TrafficAccessToken = v }, func(r routesync.RouteEntry) string { return r.TrafficAccessToken }},
		{"forward_access_token", maxAccessToken, func(r *routesync.RouteEntry, v string) { r.ForwardAccessToken = v }, func(r routesync.RouteEntry) string { return r.ForwardAccessToken }},
		{"mmds_secret", maxMmdsSecret, func(r *routesync.RouteEntry, v string) { r.MmdsSecret = v }, func(r routesync.RouteEntry) string { return r.MmdsSecret }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			route := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning}
			exact := strings.Repeat("x", tc.max)
			tc.set(&route, exact)
			if err := tbl.Upsert(route); err != nil {
				t.Fatalf("exact boundary rejected: %v", err)
			}
			got, ok := tbl.Lookup("s1")
			if !ok || tc.get(got) != exact {
				t.Fatalf("exact boundary round-trip length=%d ok=%v", len(tc.get(got)), ok)
			}

			short := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning}
			tc.set(&short, "y")
			if err := tbl.Upsert(short); err != nil {
				t.Fatal(err)
			}
			got, ok = tbl.Lookup("s1")
			if !ok || tc.get(got) != "y" {
				t.Fatalf("short overwrite = %q ok=%v", tc.get(got), ok)
			}

			overlong := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning}
			tc.set(&overlong, strings.Repeat("z", tc.max+1))
			if err := tbl.Upsert(overlong); err == nil {
				t.Fatal("overlong value accepted")
			}
		})
	}
}

func TestBookmarkSweepsMissingRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	for _, sid := range []string{"s1", "s2"} {
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateRunning}); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark()
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{SandboxID: "s2", State: routesync.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Lookup("s1"); !ok {
		t.Fatal("route swept before bookmark")
	}
	tbl.Bookmark()
	if _, ok := tbl.Lookup("s1"); ok {
		t.Fatal("route not swept at bookmark")
	}
}

func TestWorkerResolveWakesAndWaitsForSharedUpdate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, time.Second, nil)
	updates := &Updates{ch: make(chan struct{})}
	worker := NewWorkerView(tbl, updates, master.Wake, 500*time.Millisecond)
	master.BeginSync()
	master.Bookmark()

	done := make(chan proxy.Route, 1)
	go func() {
		route, _ := worker.Route(context.Background(), "s1", proxy.LegacyTarget(49983))
		done <- route
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sid, ok := master.NextWake(ctx)
	if !ok || sid != "s1" {
		t.Fatalf("wake = %q ok=%v", sid, ok)
	}
	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning,
		EnvdUDS: "/run/s1/envd.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
	})
	updates.bump()
	select {
	case route := <-done:
		if route.Kind != proxy.KindUDS || route.UDS == "" || route.AccessToken != "envd" {
			t.Fatalf("route = %+v", route)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not unpark")
	}
}

func TestWorkerMissingAndPausedTransitionsThroughStarting(t *testing.T) {
	for _, tc := range []struct {
		name           string
		initialPaused  bool
		directTerminal bool
		terminal       string
		wantRunning    bool
	}{
		{name: "missing to running", terminal: routesync.StateRunning, wantRunning: true},
		{name: "missing to paused rollback", terminal: routesync.StatePaused},
		{name: "missing to delete", terminal: routesync.TypeDelete},
		{name: "missing direct delete", directTerminal: true, terminal: routesync.TypeDelete},
		{name: "paused to running", initialPaused: true, terminal: routesync.StateRunning, wantRunning: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.shm")
			tbl, err := Create(path, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			updates := &Updates{ch: make(chan struct{})}
			var wakes atomic.Int32
			wakeSeen := make(chan struct{}, 1)
			worker := NewWorkerView(tbl, updates, func(string) {
				wakes.Add(1)
				select {
				case wakeSeen <- struct{}{}:
				default:
				}
			}, 5*time.Second)
			route := routesync.RouteEntry{
				SandboxID: "transition", Profile: "e2b", State: routesync.StatePaused,
				EnvdUDS: "/run/transition/envd.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
			}
			tbl.BeginSync()
			if tc.initialPaused {
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
			}
			tbl.Bookmark()

			done := make(chan proxy.Route, 1)
			go func() {
				got, _ := worker.Route(context.Background(), route.SandboxID, proxy.LegacyTarget(49983))
				done <- got
			}()
			select {
			case <-wakeSeen:
			case <-time.After(time.Second):
				t.Fatal("initial missing/paused route did not emit Wake")
			}
			if tc.initialPaused {
				_, _, beforeReplay := tbl.LookupRevision(route.SandboxID)
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
				updates.bump()
				if _, _, afterReplay := tbl.LookupRevision(route.SandboxID); afterReplay != beforeReplay {
					t.Fatalf("identical paused replay advanced route revision: %d -> %d", beforeReplay, afterReplay)
				}
				select {
				case got := <-done:
					t.Fatalf("duplicate paused replay completed Wake: %+v", got)
				default:
				}
			}
			if !tc.directTerminal {
				route.State = routesync.StateStarting
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
				updates.bump()
				select {
				case got := <-done:
					t.Fatalf("route returned at starting: %+v", got)
				default:
				}
			}

			if tc.terminal == routesync.TypeDelete {
				tbl.Delete(route.SandboxID)
			} else {
				route.State = tc.terminal
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
			}
			updates.bump()
			select {
			case got := <-done:
				if tc.wantRunning {
					if got.Kind != proxy.KindUDS || got.UDS != route.EnvdUDS {
						t.Fatalf("running route = %+v", got)
					}
				} else if got.Kind != proxy.KindNotFound {
					t.Fatalf("rollback route = %+v, want not found", got)
				}
			case <-time.After(time.Second):
				t.Fatal("terminal update consumed the full park timeout")
			}
			if got := wakes.Load(); got != 1 {
				t.Fatalf("transition emitted %d Wake calls, want exactly 1", got)
			}
		})
	}
}

func TestWorkerStartingRouteWaitsWithoutWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	worker := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, 500*time.Millisecond)
	route := routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StateStarting,
		EnvdUDS: "/run/s1/envd.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
	}
	tbl.BeginSync()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok := worker.Resolve(canceled, route.SandboxID); ok {
		t.Fatal("canceled starting route resolved before running")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("starting route emitted %d wakes", got)
	}

	done := make(chan proxy.Route, 1)
	go func() {
		got, _ := worker.Route(context.Background(), route.SandboxID, proxy.LegacyTarget(49983))
		done <- got
	}()
	select {
	case got := <-done:
		t.Fatalf("starting route returned before running update: %+v", got)
	case <-time.After(20 * time.Millisecond):
	}
	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	select {
	case got := <-done:
		if got.Kind != proxy.KindUDS || got.UDS != route.EnvdUDS || got.AccessToken != route.EnvdAccessToken {
			t.Fatalf("route after running update = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("starting route did not observe running update")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("starting route emitted %d wakes while waiting", got)
	}
}

func TestWorkerStartingRouteStopsWaitingOnRollback(t *testing.T) {
	for _, rollback := range []string{routesync.StatePaused, routesync.StateDead} {
		t.Run(rollback, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.shm")
			tbl, err := Create(path, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			updates := &Updates{ch: make(chan struct{})}
			var wakes atomic.Int32
			worker := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)
			route := routesync.RouteEntry{
				SandboxID: "s1", Profile: "e2b", State: routesync.StateStarting,
				EnvdUDS: "/run/s1/envd.sock", EnvdAccessToken: "envd",
			}
			tbl.BeginSync()
			if err := tbl.Upsert(route); err != nil {
				t.Fatal(err)
			}
			tbl.Bookmark()
			done := make(chan proxy.Route, 1)
			go func() {
				got, _ := worker.Route(context.Background(), route.SandboxID, proxy.LegacyTarget(49983))
				done <- got
			}()
			select {
			case got := <-done:
				t.Fatalf("starting route returned before rollback: %+v", got)
			case <-time.After(20 * time.Millisecond):
			}
			if rollback == routesync.StateDead {
				tbl.Delete(route.SandboxID)
			} else {
				route.State = rollback
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
			}
			updates.bump()
			select {
			case got := <-done:
				if got.Kind != proxy.KindNotFound {
					t.Fatalf("route after %s rollback = %+v", rollback, got)
				}
			case <-time.After(time.Second):
				t.Fatalf("starting route did not stop waiting after %s rollback", rollback)
			}
			if got := wakes.Load(); got != 0 {
				t.Fatalf("starting rollback emitted %d wakes", got)
			}
		})
	}
}

func TestWorkerDeadRouteReturnsNotFoundWithoutWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	var wakes atomic.Int32
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "dead", Profile: "e2b", State: routesync.StateDead,
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	worker := NewWorkerView(tbl, nil, func(string) { wakes.Add(1) }, time.Second)
	route, err := worker.Route(context.Background(), "dead", proxy.LegacyTarget(49983))
	if err != nil || route.Kind != proxy.KindNotFound {
		t.Fatalf("dead route = %+v err=%v, want not found", route, err)
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("dead route emitted %d wakes", got)
	}
}

func TestWorkerRouteSelectsPurposeSpecificAccessToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	for _, route := range []routesync.RouteEntry{
		{
			SandboxID: "e2b", Profile: "e2b", State: routesync.StateRunning,
			EnvdUDS: "/run/e2b/envd.sock", CiUDS: "/run/e2b/ci.sock", FloatingIP: "100.100.0.2",
			EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		},
		{
			SandboxID: "bare", Profile: "bare", State: routesync.StateRunning,
			FloatingIP: "100.100.0.3", EnvdAccessToken: "unused-envd",
			TrafficAccessToken: "unused-traffic", ForwardAccessToken: "bare-forward",
		},
	} {
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark()
	view := NewWorkerView(tbl, nil, nil, time.Second)

	tests := []struct {
		sid       string
		target    proxy.ConnectTarget
		wantKind  proxy.Kind
		wantToken string
	}{
		{"e2b", proxy.LegacyTarget(49983), proxy.KindUDS, "envd"},
		{"e2b", proxy.LegacyTarget(49999), proxy.KindUDS, "envd"},
		{"e2b", proxy.LegacyTarget(8080), proxy.KindTCP, "forward"},
		{"bare", proxy.LegacyTarget(49983), proxy.KindTCP, "bare-forward"},
		{"bare", proxy.LegacyTarget(49999), proxy.KindTCP, "bare-forward"},
		{"bare", proxy.LegacyTarget(8080), proxy.KindTCP, "bare-forward"},
		{"e2b", proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 49983}, proxy.KindTCP, "forward"},
		{"e2b", proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd, Port: 8080}, proxy.KindUDS, "envd"},
		{"bare", proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd}, proxy.KindDeny, ""},
		{"e2b", proxy.ConnectTarget{Service: proxy.ConnectServiceExec}, proxy.KindDeny, ""},
	}
	for _, tc := range tests {
		route, err := view.Route(context.Background(), tc.sid, tc.target)
		if err != nil {
			t.Fatalf("Route(%s, %+v): %v", tc.sid, tc.target, err)
		}
		if route.Kind != tc.wantKind || route.AccessToken != tc.wantToken {
			t.Fatalf("Route(%s, %+v) = %+v, want kind=%v token=%q", tc.sid, tc.target, route, tc.wantKind, tc.wantToken)
		}
	}
}

func TestWorkerKnownUnsupportedServiceDoesNotWakePausedRoute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 50*time.Millisecond, nil)
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StatePaused,
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	worker := NewWorkerView(tbl, nil, master.Wake, 50*time.Millisecond)

	route, err := worker.Route(context.Background(), "s1", proxy.ConnectTarget{Service: proxy.ConnectServiceExec})
	if err != nil || route.Kind != proxy.KindDeny {
		t.Fatalf("exec route = %+v err=%v, want deny", route, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if sid, ok := master.NextWake(ctx); ok {
		t.Fatalf("unsupported service woke paused sandbox %q", sid)
	}
}

func TestMMDSSourceFromSharedTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	secret := []byte("secret")
	tbl.BeginSync()
	route := routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateStarting, TemplateID: "tmpl",
		FloatingIP: "100.100.0.3", EnvdAccessToken: "envd", MmdsSecret: hex.EncodeToString(secret),
	}
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	tbl.SetMMDSSynced(true)
	view := NewWorkerView(tbl, nil, nil, time.Second)
	assertSource := func(state string) {
		t.Helper()
		if sid, ok := view.ByFloatingIP("100.100.0.3"); !ok || sid != "s1" {
			t.Fatalf("ByFloatingIP(%s) = %q ok=%v", state, sid, ok)
		}
		if tid, tok, ok := view.SandboxInfo("s1"); !ok || tid != "tmpl" || tok != "envd" {
			t.Fatalf("SandboxInfo(%s) = %q %q ok=%v", state, tid, tok, ok)
		}
	}
	assertSource(routesync.StateStarting)
	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	assertSource(routesync.StateRunning)
	if got, ok := view.MmdsSecret("s1"); !ok || string(got) != string(secret) {
		t.Fatalf("MmdsSecret = %x ok=%v", got, ok)
	}
	route.State = routesync.StatePaused
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if sid, ok := view.ByFloatingIP(route.FloatingIP); ok || sid != "" {
		t.Fatalf("paused ByFloatingIP = %q ok=%v", sid, ok)
	}
	if tid, tok, ok := view.SandboxInfo(route.SandboxID); ok || tid != "" || tok != "" {
		t.Fatalf("paused SandboxInfo = %q %q ok=%v", tid, tok, ok)
	}
}
