package orch

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRouteEntryProjectsExplicitCredentials(t *testing.T) {
	apiSecret := strings.Repeat("1", 64)
	manifestKey := strings.Repeat("2", 64)
	sb := &types.Sandbox{
		ID: "node-s1", AuthSandboxIDValue: "stable-s1", Profile: types.ProfileE2B,
		TemplateID: "template", State: types.StateRunning,
		EnvdUDS: "/run/s1/envd.sock", CiUDS: "/run/s1/ci.sock", FloatingIP: "100.100.0.2",
		APISecret: apiSecret, ManifestKey: manifestKey, ServiceSecret: strings.Repeat("3", 64),
		EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		SnapshotRef: "manifest://" + strings.Repeat("4", 64),
	}
	apiFingerprint, err := store.APISecretHash(apiSecret)
	if err != nil {
		t.Fatal(err)
	}
	manifestFingerprint, err := store.ManifestKeyHash(manifestKey)
	if err != nil {
		t.Fatal(err)
	}

	got := (&Orchestrator{}).routeEntry(sb)
	if got.SandboxID != sb.ID || got.AuthSandboxID != "stable-s1" ||
		got.APISecret != apiSecret || got.APISecretFingerprint != apiFingerprint ||
		got.ManifestKeyFingerprint != manifestFingerprint || got.ServiceSecret != sb.ServiceSecret ||
		got.EnvdAccessToken != "envd" || got.TrafficAccessToken != "traffic" ||
		got.ForwardAccessToken != "forward" || got.SnapshotLocation != "remote" ||
		got.MmdsSecret != hex.EncodeToString(keys.MmdsSecret(manifestKey, sb.ID)) {
		t.Fatalf("route entry = %+v", got)
	}
	wire, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), manifestKey) {
		t.Fatal("route entry exposed manifest encryption key")
	}
}

func TestSnapshotLocationTreatsLocatedRefAsRemote(t *testing.T) {
	ref := "file://" + strings.Repeat("a", 64) + ".snapshot@location:source"
	if got := snapshotLocation(ref); got != "remote" {
		t.Fatalf("snapshotLocation() = %q, want remote", got)
	}
}

func TestInternalRouteSelectsPurposeSpecificAccessToken(t *testing.T) {
	o := &Orchestrator{reg: map[string]*types.Sandbox{
		"e2b": {
			ID: "e2b", Profile: types.ProfileE2B, State: types.StateRunning,
			EnvdUDS: "/run/e2b/envd.sock", CiUDS: "/run/e2b/ci.sock", FloatingIP: "100.100.0.2",
			EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		},
		"bare": {
			ID: "bare", Profile: types.ProfileBare, State: types.StateRunning, FloatingIP: "100.100.0.3",
			EnvdAccessToken: "unused-envd", TrafficAccessToken: "unused-traffic", ForwardAccessToken: "bare-forward",
		},
	}}
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
	}
	for _, tc := range tests {
		binding, found, err := o.LookupRoute(context.Background(), tc.sid, tc.target)
		if err != nil || !found {
			t.Fatalf("LookupRoute(%s, %+v): found=%v err=%v", tc.sid, tc.target, found, err)
		}
		if binding.Kind != tc.wantKind || binding.ExpectedAccessToken != tc.wantToken {
			t.Fatalf("LookupRoute(%s, %+v) = %+v, want kind=%v token=%q", tc.sid, tc.target, binding, tc.wantKind, tc.wantToken)
		}
	}
}

func TestStartingSandboxServesMMDSButNotDataPlane(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "starting", Profile: types.ProfileE2B, State: types.StateStarting,
		TemplateID: "template", FloatingIP: "100.100.0.4", EnvdUDS: "/run/starting/envd.sock",
		EnvdAccessToken: "envd", ForwardAccessToken: "forward", ManifestKey: strings.Repeat("4", 64),
	}
	o.cache(sb)
	if sid, ok := o.ByFloatingIP(sb.FloatingIP); !ok || sid != sb.ID {
		t.Fatalf("ByFloatingIP(starting) = %q ok=%v", sid, ok)
	}
	if templateID, token, ok := o.SandboxInfo(sb.ID); !ok || templateID != sb.TemplateID || token != sb.EnvdAccessToken {
		t.Fatalf("SandboxInfo(starting) = %q %q ok=%v", templateID, token, ok)
	}
	if secret, ok := o.MmdsSecret(sb.ID); !ok || len(secret) == 0 {
		t.Fatalf("MmdsSecret(starting) = %x ok=%v", secret, ok)
	}
	route, err := activateRouteForTest(context.Background(), o, sb.ID, proxy.LegacyTarget(49983))
	if err != nil || route.Kind != proxy.KindNotFound {
		t.Fatalf("Route(starting) = %+v err=%v, want not found until running", route, err)
	}
}

func TestOnWakeDoesNotRepublishDeletedCachedStarting(t *testing.T) {
	o := testOrch(t)
	o.cache(&types.Sandbox{
		ID: "deleted-starting", Profile: types.ProfileE2B, State: types.StateStarting,
		TemplateID: "e2b-snp-" + strings.Repeat("a", 64),
	})
	events, cancel := o.Subscribe()
	defer cancel()

	// Model the post-Kill window in which a caller obtained the old immutable
	// cache snapshot before Delete removed the durable row. Wake must resolve the
	// authoritative row under the lifecycle fence and leave Delete as the last
	// publication, never resurrect the stale starting route.
	o.OnWake(context.Background(), "deleted-starting")
	event := <-events
	if event.Kind != routesync.TypeDelete || event.SID != "deleted-starting" {
		t.Fatalf("OnWake stale starting event = %+v, want Delete", event)
	}
	if cached := o.lookup("deleted-starting"); cached != nil {
		t.Fatalf("OnWake retained deleted starting cache = %+v", cached)
	}
}

func TestInternalKnownExecDoesNotResumePausedSandboxBeforeIssue64(t *testing.T) {
	o := &Orchestrator{reg: map[string]*types.Sandbox{
		"paused": {ID: "paused", Profile: types.ProfileBare, State: types.StatePaused},
	}}
	binding, found, err := o.LookupRoute(context.Background(), "paused", proxy.ConnectTarget{Service: proxy.ConnectServiceExec})
	if err != nil || !found || binding.Kind != proxy.KindDeny {
		t.Fatalf("exec binding = %+v found=%v err=%v, want deny without resume", binding, found, err)
	}
}

func TestRouteReplayUsesFingerprintToken(t *testing.T) {
	o := &Orchestrator{
		routeFP: "fp-test",
		subs:    map[int]chan routesync.Event{},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	o.publish(routesync.Event{Kind: routesync.TypeUpsert, Route: routesync.RouteEntry{SandboxID: "s1"}})
	token := o.CurrentRevToken()
	if token != "fp-test:1" {
		t.Fatalf("token=%q, want fp-test:1", token)
	}
	o.publish(routesync.Event{Kind: routesync.TypeDelete, SID: "s1"})

	var got []routesync.Event
	if err := o.Replay(context.Background(), 1, func(ev routesync.Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != routesync.TypeDelete || got[0].SID != "s1" {
		t.Fatalf("replay=%+v, want delete s1", got)
	}
	if fp := o.SourceFingerprint(); fp != "fp-test" {
		t.Fatalf("fingerprint=%q", fp)
	}
}
