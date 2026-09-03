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
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestRouteEntryProjectsExplicitCredentials(t *testing.T) {
	apiSecret := strings.Repeat("1", 64)
	manifestKey := strings.Repeat("2", 64)
	sb := &types.Sandbox{
		ID: "node-s1", StableIDValue: "stable-s1", Profile: types.ProfileE2B,
		TemplateID: "template", State: types.StateRunning,
		EnvdUDS: "/run/s1/envd.sock", CiUDS: "/run/s1/ci.sock", FloatingIP: "100.100.0.2",
		APISecret: apiSecret, ManifestKey: manifestKey, ServiceSecret: strings.Repeat("3", 64),
		EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		Metadata: map[string]string{
			sandboxcfg.NsTraffic: `{"max_inflight":{"total":32,"forward":0}}`,
		},
		ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("4", 64)},
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
	if got.SandboxID != sb.ID || got.StableID != "stable-s1" ||
		got.APISecret != apiSecret || got.APISecretFingerprint != apiFingerprint ||
		got.ManifestKeyFingerprint != manifestFingerprint || got.ServiceSecret != sb.ServiceSecret ||
		got.EnvdAccessToken != "envd" || got.TrafficAccessToken != "traffic" ||
		got.ForwardAccessToken != "forward" || got.ArtifactLocation != "remote" ||
		got.MmdsSecret != hex.EncodeToString(keys.MmdsSecret(manifestKey, sb.ID)) {
		t.Fatalf("route entry = %+v", got)
	}
	if got.MaxInflightPatch == nil || got.MaxInflightPatch.Total == nil || *got.MaxInflightPatch.Total != 32 ||
		got.MaxInflightPatch.Forward == nil || *got.MaxInflightPatch.Forward != 0 || got.MaxInflightPatch.Exec != nil {
		t.Fatalf("traffic route projection = %+v", got.MaxInflightPatch)
	}
	wire, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(wire), manifestKey) {
		t.Fatal("route entry exposed manifest encryption key")
	}
}

func TestRouteEntryFailsClosedOnCorruptPersistedTraffic(t *testing.T) {
	sb := &types.Sandbox{
		ID: "corrupt", Profile: types.ProfileBare, State: types.StateRunning,
		Metadata: map[string]string{sandboxcfg.NsTraffic: `{"max_inflight":{"total":null}}`},
	}
	entry := (&Orchestrator{log: slog.New(slog.NewTextHandler(io.Discard, nil))}).routeEntry(sb)
	if entry.State != routesync.StateDead || entry.MaxInflightPatch != nil {
		t.Fatalf("corrupt traffic projection = %+v", entry)
	}
}

func TestArtifactLocationTreatsLocatedRefAsRemote(t *testing.T) {
	ref := "file://" + strings.Repeat("a", 64) + ".snapshot@location:source"
	if got := artifactLocation(ref); got != "remote" {
		t.Fatalf("artifactLocation() = %q, want remote", got)
	}
}

func TestStartingSandboxProjectsMMDS(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "starting", Profile: types.ProfileE2B, State: types.StateStarting,
		TemplateID: "template", FloatingIP: "100.100.0.4", EnvdUDS: "/run/starting/envd.sock",
		EnvdAccessToken: "envd", ForwardAccessToken: "forward", ManifestKey: strings.Repeat("4", 64),
	}
	got := o.routeEntry(sb)
	if got.State != string(types.StateStarting) || got.FloatingIP != sb.FloatingIP ||
		got.TemplateID != sb.TemplateID || got.EnvdAccessToken != sb.EnvdAccessToken || got.MmdsSecret == "" {
		t.Fatalf("starting route projection = %+v", got)
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
	o.publishRouteBarrier("ephemeral")
	if got := o.CurrentRevToken(); got != token {
		t.Fatalf("route barrier advanced replay token to %q, want %q", got, token)
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
