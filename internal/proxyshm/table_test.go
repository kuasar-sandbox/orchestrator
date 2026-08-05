package proxyshm

import (
	"context"
	"encoding/hex"
	"path/filepath"
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
	deleted, status, ok := readRecord(&master.records[idx])
	if !ok || status != statusDeleted {
		t.Fatalf("deleted record status=%d ok=%v", status, ok)
	}
	if deleted.AuthSandboxID != "" || deleted.APISecret != "" || deleted.APISecretFingerprint != "" ||
		deleted.ManifestKeyFingerprint != "" || deleted.ServiceSecret != "" ||
		deleted.EnvdAccessToken != "" || deleted.TrafficAccessToken != "" ||
		deleted.ForwardAccessToken != "" || deleted.MmdsSecret != "" {
		t.Fatalf("deleted record retained credential material: %+v", deleted)
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
