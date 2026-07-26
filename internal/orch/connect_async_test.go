package orch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/secretbox"
	"github.com/kuasar-sandbox/orchestrator/internal/store"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestConnectExistingTargetIgnoresMalformedMigrationToken(t *testing.T) {
	o := testOrch(t)
	ctx := context.Background()
	mk := strings.Repeat("a", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sb := &types.Sandbox{
		ID:           "existing-target",
		Profile:      types.ProfileBare,
		TemplateID:   "bare-img-" + strings.Repeat("b", 64),
		State:        types.StateRunning,
		APISecret:    deriveTestAPISecret(t, mk),
		ManifestKey:  mk,
		RunDir:       filepath.Join(t.TempDir(), "run"),
		BaseDir:      filepath.Join(t.TempDir(), "lib"),
		CreatedUnix:  1,
		DeadlineUnix: 100,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	got, err := o.Connect(ctx, sb.ID, apiKey, "not-a-kmt1-token", 0)
	if err != nil {
		t.Fatalf("Connect existing target: %v", err)
	}
	if got == nil || got.ID != sb.ID || got.State != sb.State || got.DeadlineUnix != sb.DeadlineUnix {
		t.Fatalf("Connect existing target changed the selected sandbox: %+v", got)
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(got), sandboxCredentials(sb))
}

func TestConnectMissingTargetWithoutMigrationTokenReturnsNotFound(t *testing.T) {
	o := testOrch(t)
	_, err := o.Connect(context.Background(), "missing-target", "any-api-key", "", 0)
	if !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("Connect missing target error = %v, want ErrNotFound", err)
	}
}

func TestConnectImportsBeforeReturningAndResumesAsynchronously(t *testing.T) {
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A migration resume probes snapshot metadata before launching. An empty PATH
	// makes that best-effort probe fail immediately and keeps this unit test local.
	t.Setenv("PATH", t.TempDir())

	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	started := make(chan struct{}, 4)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)

	mk := strings.Repeat("c", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{
		APISecret: apiSecret, ManifestKey: mk,
	}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	source := &types.Sandbox{
		ID:           "portable-source",
		Profile:      types.ProfileBare,
		TemplateID:   "bare-img-" + strings.Repeat("d", 64),
		State:        types.StatePaused,
		SnapshotRef:  "manifest://" + strings.Repeat("e", 64),
		APISecret:    apiSecret,
		ManifestKey:  mk,
		CreatedUnix:  1,
		DeadlineUnix: 100,
	}
	materializeTestSandboxCredentials(t, source)
	token, err := o.mintSandboxToken(source, source.SnapshotRef)
	if err != nil {
		t.Fatal(err)
	}

	targetID := "portable-target"
	type connectResult struct {
		sb  *types.Sandbox
		err error
	}
	done := make(chan connectResult, 1)
	go func() {
		sb, err := o.Connect(ctx, targetID, apiKey, token, 0)
		done <- connectResult{sb: sb, err: err}
	}()

	var result connectResult
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Connect waited for the blocked asynchronous resume")
	}
	if result.err != nil || result.sb == nil || result.sb.ID != targetID || result.sb.State != types.StatePaused {
		t.Fatalf("Connect imported result = %+v, %v", result.sb, result.err)
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(result.sb), sandboxCredentials(source))

	readable, err := o.Get(ctx, targetID, apiKey)
	if err != nil || readable == nil {
		t.Fatalf("Get synchronously imported target: %+v, %v", readable, err)
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(readable), sandboxCredentials(source))

	waitForLauncherStart(t, started)
	blocked, err := o.st.Get(ctx, targetID)
	if err != nil || blocked == nil || blocked.State != types.StatePaused {
		t.Fatalf("imported row before launcher release = %+v, %v; want paused", blocked, err)
	}
	close(startGate)
	waitForSandbox(t, o, ctx, targetID, func(sb *types.Sandbox) bool {
		return sb.State == types.StateRunning
	}, "running after asynchronous imported resume")
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launcher starts = %d, want 1", got)
	}
}

func TestConcurrentConnectImportUsesSingleCompleteWinner(t *testing.T) {
	dir := t.TempDir()
	runtimePath := filepath.Join(dir, "runtime.erofs")
	if err := os.WriteFile(runtimePath, []byte("runtime"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", t.TempDir())

	cfg := &config.Config{}
	cfg.Sandbox.Boot.Runtime = runtimePath
	started := make(chan struct{}, 64)
	startGate := make(chan struct{})
	lc := &countingLauncher{started: started, startGate: startGate}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)

	mk := strings.Repeat("7", 64)
	apiSecret, apiKey := defaultTestCredentials(t, mk)
	if _, err := o.st.AddKeyPair(ctx, store.KeyPair{APISecret: apiSecret, ManifestKey: mk}, "", 0, ""); err != nil {
		t.Fatal(err)
	}
	makeSource := func(id, marker string) *types.Sandbox {
		sb := &types.Sandbox{
			ID: id, Profile: types.ProfileBare,
			TemplateID: "bare-img-" + strings.Repeat("8", 64),
			State:      types.StatePaused, SnapshotRef: "manifest://" + strings.Repeat("9", 64),
			APISecret: apiSecret, ManifestKey: mk,
			Metadata:    map[string]string{"winner": marker, "padding": strings.Repeat(marker, 128<<10)},
			CreatedUnix: 1, DeadlineUnix: 100,
		}
		materializeTestSandboxCredentials(t, sb)
		return sb
	}
	sources := []*types.Sandbox{makeSource("source-alpha", "a"), makeSource("source-beta", "b")}
	tokens := make([]string, len(sources))
	for i, source := range sources {
		var err error
		tokens[i], err = o.mintSandboxToken(source, source.SnapshotRef)
		if err != nil {
			t.Fatal(err)
		}
	}

	const callers = 32
	targetID := "concurrent-target"
	release := make(chan struct{})
	type concurrentConnectResult struct {
		sb  *types.Sandbox
		err error
	}
	results := make(chan concurrentConnectResult, callers)
	var ready sync.WaitGroup
	ready.Add(callers)
	for i := 0; i < callers; i++ {
		token := tokens[i%len(tokens)]
		go func() {
			ready.Done()
			<-release
			sb, err := o.Connect(ctx, targetID, apiKey, token, 0)
			results <- concurrentConnectResult{sb: sb, err: err}
		}()
	}
	ready.Wait()
	close(release)

	connected := make([]*types.Sandbox, 0, callers)
	for range callers {
		select {
		case result := <-results:
			if result.err != nil || result.sb == nil {
				t.Fatalf("concurrent Connect = %+v, %v", result.sb, result.err)
			}
			connected = append(connected, result.sb)
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Connect calls did not return")
		}
	}

	winner, err := o.st.Get(ctx, targetID)
	if err != nil || winner == nil {
		t.Fatalf("read import winner: %+v, %v", winner, err)
	}
	var sourceWinner *types.Sandbox
	for _, source := range sources {
		if winner.AuthSandboxID() == source.AuthSandboxID() {
			sourceWinner = source
			break
		}
	}
	if sourceWinner == nil || winner.Metadata["winner"] != sourceWinner.Metadata["winner"] {
		t.Fatalf("import winner mixed token state: %+v", winner)
	}
	assertMigrationCredentialsEqual(t, sandboxCredentials(winner), sandboxCredentials(sourceWinner))
	for _, sb := range connected {
		if sb.AuthSandboxID() != winner.AuthSandboxID() || sb.Metadata["winner"] != winner.Metadata["winner"] {
			t.Fatalf("Connect returned a non-winning record: got=%+v winner=%+v", sb, winner)
		}
		assertMigrationCredentialsEqual(t, sandboxCredentials(sb), sandboxCredentials(winner))
	}

	waitForLauncherStart(t, started)
	close(startGate)
	waitForSandbox(t, o, ctx, targetID, func(sb *types.Sandbox) bool { return sb.State == types.StateRunning }, "running after concurrent import")
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launcher starts = %d, want 1", got)
	}
}

func TestConnectExplicitTimeoutWinsAfterAsyncResume(t *testing.T) {
	cfg := &config.Config{}
	cfg.Sandbox.TimeoutSec = 900
	started := make(chan struct{}, 4)
	lc := &countingLauncher{started: started}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)

	mk := strings.Repeat("f", 64)
	_, apiKey := defaultTestCredentials(t, mk)
	sb := &types.Sandbox{
		ID:           "timeout-target",
		Profile:      types.ProfileBare,
		TemplateID:   "bare-img-" + strings.Repeat("1", 64),
		State:        types.StatePaused,
		APISecret:    deriveTestAPISecret(t, mk),
		ManifestKey:  mk,
		RunDir:       filepath.Join(cfg.Paths.RunRoot, "timeout-target"),
		BaseDir:      filepath.Join(cfg.Paths.BaseRoot, "timeout-target"),
		CreatedUnix:  1,
		DeadlineUnix: 10,
	}
	materializeTestSandboxCredentials(t, sb)
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	before := time.Now().Unix()
	connected, err := o.Connect(ctx, sb.ID, apiKey, "", 37)
	if err != nil {
		t.Fatal(err)
	}
	expectedDeadline := connected.DeadlineUnix
	if expectedDeadline < before+36 || expectedDeadline > time.Now().Unix()+38 {
		t.Fatalf("requested deadline = %d, outside explicit timeout window", expectedDeadline)
	}
	waitForLauncherStart(t, started)
	waitForSandbox(t, o, ctx, sb.ID, func(current *types.Sandbox) bool {
		return current.State == types.StateRunning && current.DeadlineUnix == expectedDeadline
	}, "running with explicit Connect deadline")
	if cached := o.lookup(sb.ID); cached == nil || cached.State != types.StateRunning || cached.DeadlineUnix != expectedDeadline {
		t.Fatalf("cached sandbox did not retain explicit Connect deadline: %+v", cached)
	}
	if got := lc.starts.Load(); got != 1 {
		t.Fatalf("launcher starts = %d, want 1", got)
	}
}

func newAsyncConnectTestOrchestrator(t *testing.T, cfg *config.Config, lc *countingLauncher) (*Orchestrator, context.Context) {
	t.Helper()
	dir := t.TempDir()
	if cfg.Paths.RunRoot == "" {
		cfg.Paths.RunRoot = filepath.Join(dir, "run")
	}
	if cfg.Paths.BaseRoot == "" {
		cfg.Paths.BaseRoot = filepath.Join(dir, "lib")
	}
	if cfg.Sandbox.Network.Bare.InnerIP == "" {
		cfg.Sandbox.Network.Bare.InnerIP = "169.254.1.1/31"
	}
	box, err := secretbox.NewFromColonHex(strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "node.db"), box)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	o := New(cfg, st, lc, stubVS{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	lc.orch = o
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	o.SetClusterContext(ctx)
	if err := o.StartRunPools(ctx); err != nil {
		t.Fatal(err)
	}
	return o, ctx
}

func waitForLauncherStart(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("launcher did not start")
	}
}

func waitForSandbox(
	t *testing.T,
	o *Orchestrator,
	ctx context.Context,
	id string,
	ready func(*types.Sandbox) bool,
	description string,
) *types.Sandbox {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	for {
		sb, err := o.st.Get(ctx, id)
		if err != nil {
			t.Fatalf("get sandbox while waiting for %s: %v", description, err)
		}
		if sb != nil && ready(sb) {
			return sb
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("sandbox did not become %s: %+v", description, sb)
		}
	}
}
