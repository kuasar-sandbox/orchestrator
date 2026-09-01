package orch

import (
	"context"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func activateRouteForTest(ctx context.Context, o *Orchestrator, sid string, target proxy.ConnectTarget) (proxy.Route, error) {
	binding, found, err := o.LookupRoute(ctx, sid, target)
	if err != nil {
		return proxy.Route{}, err
	}
	if !found {
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	if binding.Kind == proxy.KindDeny {
		return proxy.Route{Kind: proxy.KindDeny}, nil
	}
	route, found, err := o.ActivateRoute(ctx, binding)
	if err != nil {
		return proxy.Route{}, err
	}
	if !found {
		return proxy.Route{Kind: proxy.KindNotFound}, nil
	}
	return route, nil
}

func TestLookupRouteIsSideEffectFreeAndActivateRevalidatesBinding(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "paused", Profile: types.ProfileBare, State: types.StatePaused,
		FloatingIP: "192.0.2.10", APISecret: strings.Repeat("1", 64), ManifestKey: strings.Repeat("3", 64),
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	target := proxy.LegacyTarget(8080)
	binding, found, err := o.LookupRoute(context.Background(), sb.ID, target)
	if err != nil || !found || binding.ExpectedAccessToken != sb.ForwardAccessToken {
		t.Fatalf("LookupRoute = %+v found=%v err=%v", binding, found, err)
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || stored.State != types.StatePaused {
		t.Fatalf("LookupRoute changed durable state: %+v err=%v", stored, err)
	}
	if o.lookup(sb.ID) != nil {
		t.Fatal("LookupRoute populated the lifecycle cache")
	}
	if _, active := o.launches.Lookup(sb.ID); active {
		t.Fatal("LookupRoute created a launch owner")
	}

	rotated := cloneSandbox(sb)
	rotated.ServiceSecret = strings.Repeat("6", 64)
	rotated.ForwardAccessToken, err = keys.MintForwardAccessToken(rotated.ServiceSecret, rotated.StableID())
	if err != nil {
		t.Fatal(err)
	}
	if err := o.st.Delete(context.Background(), sb.ID); err != nil {
		t.Fatal(err)
	}
	if err := o.st.Put(context.Background(), rotated); err != nil {
		t.Fatal(err)
	}
	if route, found, err := o.ActivateRoute(context.Background(), binding); err != nil || found || route != (proxy.Route{}) {
		t.Fatalf("ActivateRoute(stale binding) = %+v found=%v err=%v", route, found, err)
	}
	stored, err = o.st.Get(context.Background(), sb.ID)
	if err != nil || stored.State != types.StatePaused {
		t.Fatalf("stale activation changed durable state: %+v err=%v", stored, err)
	}
	if _, active := o.launches.Lookup(sb.ID); active {
		t.Fatal("stale activation created a launch owner")
	}
}

func TestActivateRouteBuildsBackendFromLatestRunningRecord(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID: "running", Profile: types.ProfileBare, State: types.StateRunning,
		FloatingIP: "192.0.2.20", APISecret: strings.Repeat("3", 64), ManifestKey: strings.Repeat("5", 64),
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	target := proxy.LegacyTarget(8080)
	binding, found, err := o.LookupRoute(context.Background(), sb.ID, target)
	if err != nil || !found {
		t.Fatalf("LookupRoute found=%v err=%v", found, err)
	}
	newRecord := cloneSandbox(sb)
	newRecord.FloatingIP = "192.0.2.21"
	if err := o.st.Put(context.Background(), newRecord); err != nil {
		t.Fatal(err)
	}
	route, found, err := o.ActivateRoute(context.Background(), binding)
	if err != nil || !found || route.Kind != proxy.KindTCP || route.Addr != "192.0.2.21:8080" {
		t.Fatalf("ActivateRoute = %+v found=%v err=%v", route, found, err)
	}
}
