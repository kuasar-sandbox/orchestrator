package orch

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestLookupExecIsSideEffectFreeAndUsesStableCredentialSubject(t *testing.T) {
	o := testOrch(t)
	sb := &types.Sandbox{
		ID:                 "node-s1",
		AuthSandboxIDValue: "stable-s1",
		Profile:            types.ProfileBare,
		TemplateID:         types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("1", 64)}.String(),
		State:              types.StatePaused,
		APISecret:          strings.Repeat("2", 64),
		ManifestKey:        strings.Repeat("3", 64),
		RunDir:             filepath.Join(t.TempDir(), "run", "node-s1"),
		BaseDir:            filepath.Join(t.TempDir(), "base", "node-s1"),
		CreatedUnix:        1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(context.Background(), sb); err != nil {
		t.Fatal(err)
	}

	identity, found, err := o.LookupExec(context.Background(), sb.ID)
	if err != nil || !found {
		t.Fatalf("LookupExec() = %+v, %v, %v", identity, found, err)
	}
	want := (proxy.ExecIdentity{
		NodeSandboxID: sb.ID,
		AuthSandboxID: sb.AuthSandboxID(),
		ServiceSecret: sb.ServiceSecret,
	})
	if identity != want {
		t.Fatalf("identity = %+v, want node/auth identity", identity)
	}
	if cached := o.lookup(sb.ID); cached != nil {
		t.Fatal("side-effect-free exec lookup populated the lifecycle cache")
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused {
		t.Fatalf("side-effect-free exec lookup changed sandbox: %+v err=%v", stored, err)
	}
}

func TestActivateExecRequiresExpectedIdentityThenUsesExistingResume(t *testing.T) {
	cfg := &config.Config{}
	started := make(chan struct{}, 2)
	lc := &countingLauncher{started: started}
	o, baseCtx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	ctx, cancel := context.WithTimeout(baseCtx, 5*time.Second)
	defer cancel()

	manifestKey := strings.Repeat("a", 64)
	sid := "exec-paused"
	sb := &types.Sandbox{
		ID:                 sid,
		AuthSandboxIDValue: "stable-exec",
		Profile:            types.ProfileBare,
		TemplateID:         types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:              types.StatePaused,
		APISecret:          deriveTestAPISecret(t, manifestKey),
		ManifestKey:        manifestKey,
		RunDir:             filepath.Join(cfg.Paths.RunRoot, sid),
		BaseDir:            filepath.Join(cfg.Paths.BaseRoot, sid),
		CreatedUnix:        1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	identity, found, err := o.LookupExec(ctx, sid)
	if err != nil || !found {
		t.Fatalf("LookupExec() = %+v, %v, %v", identity, found, err)
	}
	if got := lc.starts.Load(); got != 0 {
		t.Fatalf("lookup launcher starts = %d, want 0", got)
	}

	wrong := identity
	wrong.AuthSandboxID = "different-lineage"
	if got, found, err := o.ActivateExec(ctx, sid, wrong); err != nil || found || got != (proxy.ExecIdentity{}) {
		t.Fatalf("ActivateExec(wrong identity) = %+v, %v, %v", got, found, err)
	}
	if got := lc.starts.Load(); got != 0 {
		t.Fatalf("identity mismatch launcher starts = %d, want 0", got)
	}

	ready, found, err := o.ActivateExec(ctx, sid, identity)
	if err != nil || !found {
		t.Fatalf("ActivateExec(valid identity) = %+v, %v, %v", ready, found, err)
	}
	if ready != identity {
		t.Fatalf("ready identity = %+v, want %+v", ready, identity)
	}
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("authorized activation launcher starts = %d, want 1", got)
	}
	stored, err := o.st.Get(ctx, sid)
	if err != nil || stored == nil || stored.State != types.StateRunning {
		t.Fatalf("authorized activation did not resume sandbox: %+v err=%v", stored, err)
	}
}

func TestLookupExecTreatsDeadAndMissingSandboxesAsAbsent(t *testing.T) {
	o := testOrch(t)
	o.cache(&types.Sandbox{ID: "dead", State: types.StateDead})
	if identity, found, err := o.LookupExec(context.Background(), "dead"); err != nil || found || identity != (proxy.ExecIdentity{}) {
		t.Fatalf("dead LookupExec = %+v, %v, %v", identity, found, err)
	}
	if identity, found, err := o.LookupExec(context.Background(), "missing"); err != nil || found || identity != (proxy.ExecIdentity{}) {
		t.Fatalf("missing LookupExec = %+v, %v, %v", identity, found, err)
	}
}

func TestStartingWithoutLaunchOwnerFailsClosed(t *testing.T) {
	o := testOrch(t)
	manifestKey := strings.Repeat("c", 64)
	sb := &types.Sandbox{
		ID: "ownerless-starting", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(),
		State:      types.StateStarting, FloatingIP: "192.0.2.30",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "base"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.InsertSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)

	route, err := activateRouteForTest(context.Background(), o, sb.ID, proxy.LegacyTarget(8080))
	if err != nil || route.Kind != proxy.KindNotFound {
		t.Fatalf("ownerless starting route = %+v, %v; want fail-closed not found", route, err)
	}
	identity := execIdentity(sb)
	if got, found, err := o.ActivateExec(context.Background(), sb.ID, identity); err != nil || found || got != (proxy.ExecIdentity{}) {
		t.Fatalf("ownerless starting exec = %+v, %v, %v; want absent", got, found, err)
	}
	stored, err := o.st.Get(context.Background(), sb.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != "" {
		t.Fatalf("ownerless starting was mutated or relaunched: %+v, %v", stored, err)
	}
	if _, found := o.launches.Lookup(sb.ID); found {
		t.Fatal("ownerless starting acquired a launch attempt")
	}
}

func TestStartingInternalRouteAndExecWaitHonorCallerCancellation(t *testing.T) {
	o := testOrch(t)
	manifestKey := strings.Repeat("e", 64)
	sb := &types.Sandbox{
		ID: "cancel-starting", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("f", 64)}.String(),
		State:      types.StateStarting, FloatingIP: "192.0.2.31",
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "base"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.InsertSandbox(context.Background(), sb); err != nil {
		t.Fatal(err)
	}
	o.cache(sb)
	attempt, err := o.launches.Claim(context.Background(), sb.ID, launchCreate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { o.launches.Finish(attempt, context.Canceled) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if route, err := activateRouteForTest(ctx, o, sb.ID, proxy.LegacyTarget(8080)); !errors.Is(err, context.Canceled) || route != (proxy.Route{}) {
		t.Fatalf("canceled starting route = %+v, %v", route, err)
	}
	if identity, found, err := o.ActivateExec(ctx, sb.ID, execIdentity(sb)); !errors.Is(err, context.Canceled) || found || identity != (proxy.ExecIdentity{}) {
		t.Fatalf("canceled starting exec = %+v, %v, %v", identity, found, err)
	}
}
