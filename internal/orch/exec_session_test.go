package orch

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestExecSessionMintsTokenForStableID(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("a", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID:            "stable-g2",
		Profile:       types.ProfileBare,
		StableIDValue: "stable",
		TemplateID:    types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:         types.StateRunning,
		APISecret:     deriveTestAPISecret(t, manifestKey),
		ManifestKey:   manifestKey,
		RunDir:        filepath.Join(t.TempDir(), "run"),
		BaseDir:       filepath.Join(t.TempDir(), "lib"),
		CreatedUnix:   1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	before := time.Now().Unix()
	conditions := []string{"request.argv == ['/bin/true']", "request.cwd == '/'"}
	token, err := o.ExecSession(ctx, sb.ID, apiKey, "ignored-for-existing-target", 37, conditions)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now().Unix()
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Unix(before+36, 0)); err != nil {
		t.Fatalf("minted token before expiry: %v", err)
	}
	claims, err := keys.ParseAndVerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Unix(before, 0))
	if err != nil || len(claims.Conditions) != 2 || claims.Conditions[0] != conditions[0] || claims.Conditions[1] != conditions[1] {
		t.Fatalf("token conditions = %+v, %v", claims, err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Unix(after+38, 0)); err == nil {
		t.Fatal("minted token remained valid after ttl")
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.ID, time.Unix(before, 0)); err == nil {
		t.Fatal("minted token accepted node-local ID instead of StableID")
	}
}

func TestExecSessionMintsTokenWithoutRestartingStartingResume(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("c", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: "starting-exec", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("d", 64)}.String(),
		State:      types.StateStarting, RunID: "starting-run",
		ResumeSource: types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("e", 64)},
		LaunchMode:   types.LaunchMemory,
		APISecret:    deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "lib"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	token, err := o.ExecSession(ctx, sb.ID, apiKey, "", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Now()); err != nil {
		t.Fatalf("starting exec token: %v", err)
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StateStarting || stored.RunID != sb.RunID {
		t.Fatalf("exec-session changed starting resume: %+v err=%v", stored, err)
	}
}

func TestExecSessionWithoutTTLIsLongLived(t *testing.T) {
	sb := &types.Sandbox{
		ID: "bare-1", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("2", 64)}.String(), State: types.StateRunning,
		ServiceSecret: strings.Repeat("1", 64),
	}
	token, err := mintExecSessionToken(sb, 0, nil, 1)
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
	token, err := o.execSession(ctx, sb.ID, apiKey, "", 37, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Unix(1_800_000_136, 0)); err != nil {
		t.Fatalf("TTL was consumed before signing: %v", err)
	}
	if err := keys.VerifyExecAccessToken(token, sb.ServiceSecret, sb.StableID(), time.Unix(1_800_000_137, 0)); err == nil {
		t.Fatal("token accepted at signing time + TTL")
	}
}

func TestExecSessionPreparationWaitsForPreviousCleanupFence(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("b", 64)
	sb := &types.Sandbox{
		ID: "exec-cleanup-fence", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{
			Profile: types.ProfileBare, Kind: types.KindImg,
			Ref: "manifest://" + strings.Repeat("c", 64),
		}.String(),
		State: types.StatePaused, ResumeSource: types.ResumeSource{
			Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("d", 64),
		},
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "lib"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	previous, err := o.launches.Claim(context.Background(), sb.ID, launchResume)
	if err != nil {
		t.Fatal(err)
	}

	validated := make(chan struct{})
	prepared := make(chan struct{})
	var validateOnce, prepareOnce sync.Once
	type result struct {
		sb  *types.Sandbox
		err error
	}
	done := make(chan result, 1)
	go func() {
		current, _, err := o.ensureResumeAcceptedPrepared(ctx, sb.ID, nil, types.ResumeRequest{
			Trigger: types.ResumeTriggerExecSession,
			Mode:    types.ResumeAuto,
		}, func(*types.Sandbox) error {
			validateOnce.Do(func() { close(validated) })
			return nil
		}, func(*types.Sandbox) error {
			prepareOnce.Do(func() { close(prepared) })
			return nil
		})
		done <- result{sb: current, err: err}
	}()
	select {
	case <-validated:
	case <-time.After(2 * time.Second):
		t.Fatal("prepared admission did not reach the previous cleanup fence")
	}

	// Acquiring this lock is a deterministic barrier: the first admission pass
	// has observed the paused row, released the lifecycle fence, and is waiting
	// for the old attempt's terminal cleanup before it can recurse.
	unlock := o.lifecycle.Lock(sb.ID)
	if err := o.st.SetState(ctx, sb.ID, types.StateRunning); err != nil {
		unlock()
		t.Fatal(err)
	}
	unlock()
	select {
	case <-prepared:
		t.Fatal("operation-specific token preparation ran before previous cleanup finished")
	default:
	}
	o.launches.Finish(previous, nil)

	select {
	case got := <-done:
		if got.err != nil || got.sb == nil || got.sb.State != types.StateRunning {
			t.Fatalf("prepared admission = %+v, %v", got.sb, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prepared admission did not continue after cleanup finished")
	}
	select {
	case <-prepared:
	default:
		t.Fatal("operation-specific token preparation did not run after cleanup")
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
		ResumeSource: types.ResumeSource{
			Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("e", 64),
		},
		APISecret: apiSecret, ManifestKey: manifestKey,
		CreatedUnix: 1, DeadlineUnix: 100,
	}
	materializeTestSandboxCredentials(t, source)
	migrationToken, err := o.mintSandboxToken(source, source.ResumeSource)
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
		token, err := o.ExecSession(ctx, targetID, apiKey, migrationToken, 37, nil)
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
	if err != nil || imported == nil || imported.State != types.StateStarting || imported.RunID != "" {
		t.Fatalf("synchronously imported target = %+v, %v", imported, err)
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(imported), sandboxCredentials(source))
	if err := keys.VerifyExecAccessToken(
		issued.token, imported.ServiceSecret, imported.StableID(), time.Now(),
	); err != nil {
		t.Fatalf("imported target token: %v", err)
	}

	waitForLauncherStart(t, started)
	blocked, err := o.st.Get(ctx, targetID)
	if err != nil || blocked == nil || blocked.State != types.StateStarting || blocked.RunID != "" || blocked.VswitchPort != "" {
		t.Fatalf("imported row before launcher release = %+v, %v; want unassigned starting without network ownership", blocked, err)
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
	if _, err := o.ExecSession(context.Background(), "missing", "invalid", "", 0, nil); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("missing target error = %v, want not found", err)
	}
	if _, err := o.ExecSession(context.Background(), "overflow-target", "invalid", "kmt1.not-opened", math.MaxInt64, nil); !errors.Is(err, api.ErrBadRequest) {
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

func TestExecSessionRejectsInvalidConditionsBeforeActivation(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("a", 64)
	_, apiKey := defaultTestCredentials(t, manifestKey)
	sb := &types.Sandbox{
		ID: "paused-invalid-condition", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}.String(),
		State:      types.StatePaused, ResumeSource: types.ResumeSource{
			Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64),
		}, APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "lib"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ExecSession(ctx, sb.ID, apiKey, "", 0, []string{"request.unknown == true"}); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("invalid condition error = %v", err)
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" {
		t.Fatalf("invalid condition changed sandbox = %+v, %v", stored, err)
	}
}

func TestExecSessionAuthenticatesBeforeCompilingAndCompilesBeforeImport(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	manifestKey := strings.Repeat("3", 64)
	apiSecret, apiKey := defaultTestCredentials(t, manifestKey)
	_, foreignKey := defaultTestCredentials(t, strings.Repeat("4", 64))
	existing := &types.Sandbox{
		ID: "exec-auth-order", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("5", 64)}.String(),
		State:      types.StateRunning, APISecret: apiSecret, ManifestKey: manifestKey,
		RunDir: filepath.Join(t.TempDir(), "run"), BaseDir: filepath.Join(t.TempDir(), "lib"), CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, existing)
	if err := o.st.Put(ctx, existing); err != nil {
		t.Fatal(err)
	}
	invalidConditions := []string{"request.unknown == true"}
	if _, err := o.ExecSession(ctx, existing.ID, foreignKey, "", 0, invalidConditions); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("foreign existing-target condition error = %v, want not found before compile", err)
	}
	if _, err := o.ExecSession(ctx, "missing-unallowed", foreignKey, "kmt1.not-opened", 0, invalidConditions); !errors.Is(err, api.ErrNotAllowed) {
		t.Fatalf("unallowlisted import condition error = %v, want not allowed before compile", err)
	}
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{
		APISecret: apiSecret, ManifestKey: manifestKey,
	}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ExecSession(ctx, "missing-invalid-condition", apiKey, "kmt1.not-opened", 0, invalidConditions); !errors.Is(err, api.ErrBadRequest) {
		t.Fatalf("authorized invalid-condition error = %v, want bad request before import", err)
	}
	if imported, err := o.st.Get(ctx, "missing-invalid-condition"); err != nil || imported != nil {
		t.Fatalf("invalid condition reached import: sandbox=%+v err=%v", imported, err)
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
		CreatedUnix: 1,
	}
	materializeTestSandboxCredentials(t, dead)
	if err := o.st.Put(ctx, dead); err != nil {
		t.Fatal(err)
	}
	if _, err := o.ExecSession(ctx, dead.ID, apiKey, "", 0, nil); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("dead sandbox error = %v, want not found", err)
	}
	inconsistent := &types.Sandbox{
		ID: "bad-template", Profile: types.ProfileBare,
		TemplateID: types.TemplateID{Profile: types.ProfileE2B, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("8", 64)}.String(), State: types.StateRunning,
		ServiceSecret: strings.Repeat("9", 64),
	}
	if _, err := mintExecSessionToken(inconsistent, 0, nil, 1); err == nil {
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
