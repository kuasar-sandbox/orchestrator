package orch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func mmdsAdminTestOrchestrator(t *testing.T) (*Orchestrator, *types.Sandbox) {
	t.Helper()
	cfg := &config.Config{MMDS: config.MMDSConfig{
		Enabled: true,
		Routes: config.MMDSRoutesConfig{
			Enabled: true, MaxRoutesPerSandbox: 8, MaxNamespaceBytes: 4096,
			MaxStaticBodyBytes: 1024, MaxSecretValueBytes: 8,
		},
	}}
	o := testOrchCfg(t, cfg)
	sb := &types.Sandbox{
		ID: "sandbox-1", Profile: types.ProfileE2B, TemplateID: "template-1",
		State: types.StateRunning, RunID: "run-1",
		APISecret:   "1111111111111111111111111111111111111111111111111111111111111111",
		ManifestKey: "2222222222222222222222222222222222222222222222222222222222222222",
		Metadata:    map[string]string{sandboxcfg.NsMMDS: `{"routes":[{"path":"/secret","type":"secret","secret":"declared","content_type":"application/json"}]}`},
	}
	if err := materializeSandboxCredentials(sb, sandboxcfg.Credentials{}); err != nil {
		t.Fatal(err)
	}
	digest := sandboxcfg.MMDSRoutesDigest(sb.Metadata[sandboxcfg.NsMMDS])
	if err := o.st.InsertSandboxWithMMDSRouteSecretValues(context.Background(), sb, digest, nil); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	return o, sb
}

func receiveMMDSAdminUpsert(t *testing.T, events <-chan routesync.Event) routesync.RouteEntry {
	t.Helper()
	select {
	case event := <-events:
		if event.Kind != routesync.TypeUpsert {
			t.Fatalf("event kind = %q", event.Kind)
		}
		return event.Route
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for route upsert")
		return routesync.RouteEntry{}
	}
}

func assertNoMMDSAdminEvent(t *testing.T, events <-chan routesync.Event) {
	t.Helper()
	select {
	case <-events:
		t.Fatal("unexpected route event")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestMMDSRouteSecretAdminMutationPublishesCommittedValues(t *testing.T) {
	o, sb := mmdsAdminTestOrchestrator(t)
	events, cancel := o.Subscribe()
	defer cancel()
	ctx := context.Background()

	if err := o.PutMMDSRouteSecretValue(ctx, sb.ID, "declared", []byte("one")); err != nil {
		t.Fatal(err)
	}
	first := receiveMMDSAdminUpsert(t, events)
	if first.MMDSRouteSecretValues == nil || string((*first.MMDSRouteSecretValues)["declared"]) != "one" {
		t.Fatal("first committed value was not projected")
	}
	if err := o.PutMMDSRouteSecretValue(ctx, sb.ID, "declared", []byte("two")); err != nil {
		t.Fatal(err)
	}
	second := receiveMMDSAdminUpsert(t, events)
	if second.MMDSRouteSecretValues == nil || string((*second.MMDSRouteSecretValues)["declared"]) != "two" {
		t.Fatal("replacement value was not projected")
	}

	if err := o.DeleteMMDSRouteSecretValue(ctx, sb.ID, "declared"); err != nil {
		t.Fatal(err)
	}
	deleted := receiveMMDSAdminUpsert(t, events)
	if deleted.MMDSRouteSecretValues == nil {
		t.Fatal("successful delete projected an unavailable value view")
	}
	if _, exists := (*deleted.MMDSRouteSecretValues)["declared"]; exists {
		t.Fatal("deleted value remained projected")
	}
	if err := o.DeleteMMDSRouteSecretValue(ctx, sb.ID, "declared"); err != nil {
		t.Fatal(err)
	}
	idempotent := receiveMMDSAdminUpsert(t, events)
	if idempotent.MMDSRouteSecretValues == nil {
		t.Fatal("idempotent delete projected an unavailable value view")
	}
	if _, exists := (*idempotent.MMDSRouteSecretValues)["declared"]; exists {
		t.Fatal("idempotent delete restored the value")
	}
}

func TestMMDSRouteSecretAdminRejectsUndeclaredAndOversizedWithoutPublish(t *testing.T) {
	o, sb := mmdsAdminTestOrchestrator(t)
	events, cancel := o.Subscribe()
	defer cancel()
	if err := o.PutMMDSRouteSecretValue(context.Background(), sb.ID, "other", []byte("x")); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("undeclared error = %v", err)
	}
	assertNoMMDSAdminEvent(t, events)
	if err := o.PutMMDSRouteSecretValue(context.Background(), sb.ID, "declared", []byte("123456789")); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("oversized error = %v", err)
	}
	assertNoMMDSAdminEvent(t, events)
}

func TestMMDSRouteSecretStoreFailureDoesNotPublish(t *testing.T) {
	o, sb := mmdsAdminTestOrchestrator(t)
	events, cancel := o.Subscribe()
	defer cancel()
	if err := o.st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := o.PutMMDSRouteSecretValue(context.Background(), sb.ID, "declared", []byte("x")); err == nil {
		t.Fatal("closed store mutation unexpectedly succeeded")
	}
	assertNoMMDSAdminEvent(t, events)
}

func TestMMDSRouteSecretStoreUsesSandboxOwner(t *testing.T) {
	o, sb := mmdsAdminTestOrchestrator(t)
	if err := o.PutMMDSRouteSecretValue(context.Background(), sb.ID, "declared", []byte("value")); err != nil {
		t.Fatal(err)
	}
	values, _, found, err := o.st.GetMMDSRouteSecretValues(
		context.Background(), store.MMDSRouteSecretOwnerSandbox, sb.ID,
		sandboxcfg.MMDSRoutesDigest(sb.Metadata[sandboxcfg.NsMMDS]),
	)
	if err != nil || !found || string(values["declared"]) != "value" {
		t.Fatalf("sandbox owner lookup metadata mismatch: found=%t err=%v", found, err)
	}
}
