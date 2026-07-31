package orch

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExecSessionMintsTokenForAuthenticatedStableSubject(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("a", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID:                 "stable-g2",
		Profile:            types.ProfileBare,
		AuthSandboxIDValue: "stable",
		TemplateID:         types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:              types.StateRunning,
		APISecret:          deriveTestAPISecret(t, manifestKey),
		ManifestKey:        manifestKey,
		RunDir:             filepath.Join(t.TempDir(), "run"),
		BaseDir:            filepath.Join(t.TempDir(), "lib"),
		CreatedUnix:        1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	before := time.Now().Unix()
	token, err := o.ExecSession(ctx, sb.ID, apiKey, "ignored-for-existing-target", 37)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().Unix()
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(before+36, 0)); err != nil {
		t.Fatalf("minted token before expiry: %v", err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(after+38, 0)); err == nil {
		t.Fatal("minted token remained valid after ttl")
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.ID, time.Unix(before, 0)); err == nil {
		t.Fatal("minted token accepted node-local ID instead of stable AuthSandboxID")
	}
}

func TestExecSessionWithoutTTLIsLongLived(t *testing.T) {
	sb := &types.Sandbox{
		ID: "bare-1", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("2", 64)}.String(), State: types.StateRunning,
		ServiceSecret: strings.Repeat("1", 64),
	}
	token, err := mintExecSessionToken(sb, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.ID, time.Unix(math.MaxInt64, 0)); err != nil {
		t.Fatalf("long-lived token at distant time: %v", err)
	}
}

func TestExecSessionTTLStartsAtSigningAfterTargetPreparation(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("4", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: "signing-time", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("5", 64)}.String(), State: types.StateRunning,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "lib"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	clock := scriptedUnixClock(t, 1_800_000_000, 1_800_000_100)
	token, err := o.execSession(ctx, sb.ID, apiKey, "", 37, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(1_800_000_136, 0)); err != nil {
		t.Fatalf("TTL was consumed before signing: %v", err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.AuthSandboxID(), time.Unix(1_800_000_137, 0)); err == nil {
		t.Fatal("token accepted at signing time + TTL")
	}
}

func TestExecSessionImportsBeforeReturningAndResumesAsynchronously(t *testing.T) {
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Keep the migration snapshot probe local and deterministic.
	t.Setenv("PATH", t.TempDir())

	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	started := make(chan struct{}, 1)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)

	manifestKey := strings.Repeat("c", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{
		APISecret: apiSecret, ManifestKey: manifestKey,
	}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := &types.Sandbox{
		ID: "exec-portable-source", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(), State: types.StatePaused,
		SnapshotRef: "manifest://" + strings.Repeat("e", 64),
		APISecret:   apiSecret, ManifestKey: manifestKey,
		CreatedUnix: 1, DeadlineUnix: 100,
	}
	materializeTestSandboxCredentials(t, source)
	migrationToken, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}

	targetID := "exec-portable-target"
	type result struct {
		token string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		token, err := o.ExecSession(ctx, targetID, apiKey, migrationToken, 37)
		done <- result{token: token, err: err}
	}()

	var issued result
	select {
	case issued = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ExecSession waited for the blocked asynchronous resume")
	}
	if issued.err != nil || issued.token == "" {
		t.Fatalf("ExecSession token = %q, error = %v", issued.token, issued.err)
	}
	imported, err := o.st.Get(ctx, targetID)
	if err != nil || imported == nil || imported.State != types.StatePaused {
		t.Fatalf("synchronously imported target = %+v, %v", imported, err)
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(imported), sandboxCredentials(source))
	if err := keys.VerifyExecAccessToken(
		issued.token, imported.ServiceSecret, imported.AuthSandboxID(), time.Now(),
	); err != nil {
		t.Fatalf("imported target token: %v", err)
	}

	waitForLauncherStart(t, started)
	blocked, err := o.st.Get(ctx, targetID)
	if err != nil || blocked == nil || blocked.State != types.StatePaused {
		t.Fatalf("imported row before launcher release = %+v, %v; want paused", blocked, err)
	}
	close(startGate)
	waitForSandbox(t, o, ctx, targetID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateRunning
	}, "running after asynchronous exec-session resume")
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launcher starts = %d, want 1", got)
	}
}

func TestExecSessionRejectsUnauthorizedAndInvalidTTL(t *testing.T) {
	o := testOrch(t)
	if _, err := o.ExecSession(context.Background(), "missing", "invalid", "", 0); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("missing target error = %v, want not found", err)
	}
	if _, err := o.ExecSession(context.Background(), "overflow-target", "invalid", "kmt1.not-opened", math.MaxInt64); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("overflow request error = %v, want bad request", err)
	}
	if sb, err := o.st.Get(context.Background(), "overflow-target"); err != nil || sb != nil {
		t.Fatalf("overflow request reached import: sandbox=%+v err=%v", sb, err)
	}
	for _, test := range []struct {
		name string
		now  int64
		ttl  int64
	}{
		{name: "negative", now: 1, ttl: -1},
		{name: "overflow", now: math.MaxInt64 - 1, ttl: 2},
		{name: "invalid clock", now: 0, ttl: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := execSessionExpiry(test.now, test.ttl); !errors.Is(err, api.ErrBadRequest) {
				t.Fatalf("execSessionExpiry() error = %v, want bad request", err)
			}
		})
	}
}

func TestExecSessionRejectsDeadAndInconsistentSandbox(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("6", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	dead := &types.Sandbox{
		ID: "dead-exec", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("7", 64)}.String(), State: types.StateDead,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "lib"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, dead)
	if err := o.st.Put(ctx, dead); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ExecSession(ctx, dead.ID, apiKey, "", 0); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("dead sandbox error = %v, want not found", err)
	}
	inconsistent := &types.Sandbox{
		ID: "bad-template", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("8", 64)}.String(), State: types.StateRunning,
		ServiceSecret: strings.Repeat("9", 64),
	}
	if _, err := mintExecSessionToken(inconsistent, 0, 1); err == nil {
		t.Fatal("inconsistent template/profile minted a token")
	}
}

func scriptedUnixClock(t *testing.T, values ...int64) unixClock {
	t.Helper()
	index := 0
	return func() int64 {
		if index >= len(values) {
			t.Fatalf("clock called more than %d times", len(values))
		}
		value := values[index]
		index++
		return value
	}
}
