package orch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/nodepath"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestWaitRuntimeReadinessProtocolAndCleanup(t *testing.T) {
	tests := []struct {
		name       string
		wire       string
		noSend     bool
		noConnect  bool
		wantErr    bool
		wantCtxErr bool
	}{
		{name: "exact", wire: "control_ready\nready\n"},
		{name: "EOF before control", wire: "", wantErr: true},
		{name: "EOF before ready", wire: "control_ready\n", wantErr: true},
		{name: "unknown", wire: "unknown\nready\n", wantErr: true},
		{name: "duplicate", wire: "control_ready\ncontrol_ready\n", wantErr: true},
		{name: "reverse", wire: "ready\ncontrol_ready\n", wantErr: true},
		{name: "silent connection timeout", noSend: true, wantErr: true, wantCtxErr: true},
		{name: "accept timeout", noConnect: true, wantErr: true, wantCtxErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := shortOrchestratorTestDir(t)
			runRoot, sid := filepath.Join(dir, "run"), "sid"
			if err := os.MkdirAll(nodepath.SandboxRunDir(runRoot, sid), 0o700); err != nil {
				t.Fatal(err)
			}
			l, err := listenRuntimeReadiness(runRoot, sid)
			if err != nil {
				t.Fatal(err)
			}
			path := configsock.ReadinessSocketPath(runRoot, sid)
			if st, err := os.Stat(path); err != nil || st.Mode().Perm() != 0o600 {
				t.Fatalf("readiness socket mode: stat=%v err=%v", st, err)
			}
			clientDone := make(chan struct{})
			if !tt.noConnect {
				go func() {
					defer close(clientDone)
					conn, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
					if err != nil {
						return
					}
					defer conn.Close()
					if tt.noSend {
						_, _ = io.Copy(io.Discard, conn)
						return
					}
					_, _ = io.WriteString(conn, tt.wire)
				}()
			}

			timeout := time.Second
			if tt.wantCtxErr {
				timeout = 30 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			err = waitRuntimeReadiness(ctx, l)
			cancel()
			l.Close()
			if (err != nil) != tt.wantErr {
				t.Fatalf("waitRuntimeReadiness error = %v, want error=%t", err, tt.wantErr)
			}
			if tt.wantCtxErr && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("error = %v, want deadline exceeded", err)
			}
			if !tt.noConnect {
				select {
				case <-clientDone:
				case <-time.After(time.Second):
					t.Fatal("readiness client did not exit after server cleanup")
				}
			}
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("readiness socket remains after wait: %v", err)
			}
		})
	}
}

func TestListenRuntimeReadinessReplacesStaleSocket(t *testing.T) {
	dir := shortOrchestratorTestDir(t)
	runRoot, sid := filepath.Join(dir, "run"), "stale"
	if err := os.MkdirAll(nodepath.SandboxRunDir(runRoot, sid), 0o700); err != nil {
		t.Fatal(err)
	}
	path := configsock.ReadinessSocketPath(runRoot, sid)
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("stale socket was not retained for test: %v", err)
	}

	l, err := listenRuntimeReadiness(runRoot, sid)
	if err != nil {
		t.Fatalf("replace stale socket: %v", err)
	}
	l.Close()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replacement socket remains after close: %v", err)
	}
}

func TestLaunchBindsBeforeAssignmentAndBareWaitsForRuntime(t *testing.T) {
	assigned := make(chan string, 1)
	connectGate := make(chan struct{})
	lc := &countingLauncher{assigned: assigned, connectGate: connectGate}
	cfg := &config.Config{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileBare, "bind-before-assign")
	if err := os.MkdirAll(sb.RunDir, 0o755); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- runFreshLaunchForTest(t, o, ctx, sb, tmpl) }()
	select {
	case sid := <-assigned:
		if sid != sb.ID {
			t.Fatalf("assigned sid = %q", sid)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not receive assignment")
	}
	readyPath := configsock.ReadinessSocketPath(cfg.Paths.RunRoot, sb.ID)
	if st, err := os.Stat(readyPath); err != nil || st.Mode()&os.ModeSocket == 0 {
		t.Fatalf("listener was not bound when assignment completed: stat=%v err=%v", st, err)
	}
	if st, err := os.Stat(sb.RunDir); err != nil || st.Mode().Perm() != 0o700 {
		t.Fatalf("run directory mode = %v, err=%v; want 0700", st, err)
	}
	select {
	case err := <-done:
		t.Fatalf("bare launch returned before runtime readiness: %v", err)
	default:
	}
	close(connectGate)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("bare launch did not finish after runtime readiness")
	}
	if _, err := os.Lstat(readyPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ready socket remains after successful launch: %v", err)
	}
}

func TestE2BLaunchInitializesEnvdAfterRuntime(t *testing.T) {
	assigned := make(chan string, 1)
	lc := &countingLauncher{assigned: assigned, readinessDelay: 150 * time.Millisecond}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.sandboxReadyTimeout = 2 * time.Second
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "e2b-order")
	events, cancelEvents := o.Subscribe()
	defer cancelEvents()

	done := make(chan error, 1)
	go func() { done <- runFreshLaunchForTest(t, o, ctx, sb, tmpl) }()
	select {
	case <-assigned:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not receive assignment")
	}
	var assignedRunID string
	startingIndex := 0
	for index, wantKind := range []string{routesync.TypeUpsert, routesync.TypeRouteBarrier, routesync.TypeUpsert, routesync.TypeUpsert} {
		select {
		case event := <-events:
			if wantKind == routesync.TypeRouteBarrier {
				if event.Kind != routesync.TypeRouteBarrier {
					t.Fatalf("pre-init launch event %d = %+v, want route barrier", index, event)
				}
				continue
			}
			if event.Kind != routesync.TypeUpsert || event.Route.State != routesync.StateStarting {
				t.Fatalf("pre-init launch event %d = %+v, want starting upsert", index, event)
			}
			if startingIndex < 2 && event.Route.RunID != "" {
				t.Fatalf("pre-assignment launch event %d has run ID %q", index, event.Route.RunID)
			}
			if startingIndex == 2 {
				assignedRunID = event.Route.RunID
				if assignedRunID == "" {
					t.Fatal("assigned starting route did not publish its run ID before envd init")
				}
			}
			startingIndex++
		case <-time.After(time.Second):
			t.Fatalf("missing pre-init starting upsert %d", index)
		}
	}
	requests := make(chan string, 4)
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))

	select {
	case req := <-requests:
		t.Fatalf("envd was queried before runtime ready: %s", req)
	case <-time.After(60 * time.Millisecond):
	}
	select {
	case req := <-requests:
		if req != "POST /init" {
			t.Fatalf("first envd request = %q, want POST /init", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("envd /init was not attempted after runtime readiness")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("E2B launch did not return")
	}
	select {
	case req := <-requests:
		t.Fatalf("unexpected envd request after successful /init: %s", req)
	default:
	}
	select {
	case event := <-events:
		if event.Kind != routesync.TypeUpsert || event.Route.State != routesync.StateRunning || event.Route.RunID != assignedRunID {
			t.Fatalf("successful launch event = %+v, want running upsert for %s", event, assignedRunID)
		}
	case <-time.After(time.Second):
		t.Fatal("successful launch did not publish running")
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StateRunning {
		t.Fatalf("stored sandbox after successful launch = %+v, %v", stored, err)
	}
}

func TestE2BInitMustCompleteWithinLaunchDeadline(t *testing.T) {
	lc := &countingLauncher{}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.sandboxReadyTimeout = 180 * time.Millisecond
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "init-launch-deadline")
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int64
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		<-r.Context().Done()
	}))

	started := time.Now()
	err := runFreshLaunchForTest(t, o, ctx, sb, tmpl)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "envd /init") {
		t.Fatalf("launch error = %v, want mandatory envd /init deadline failure", err)
	}
	if elapsed < 140*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("launch elapsed %s, want the 180ms launch budget", elapsed)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("envd /init attempts = %d, want one attempt before the shared launch deadline", got)
	}
}

func TestE2BRuntimeAndInitShareLaunchDeadline(t *testing.T) {
	lc := &countingLauncher{readinessDelay: 120 * time.Millisecond}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.sandboxReadyTimeout = 220 * time.Millisecond
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "shared-deadline")

	started := time.Now()
	err := runFreshLaunchForTest(t, o, ctx, sb, tmpl)
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "envd /init") {
		t.Fatalf("launch error = %v, want envd /init deadline failure", err)
	}
	if elapsed < 180*time.Millisecond || elapsed > 500*time.Millisecond {
		t.Fatalf("launch elapsed %s; runtime and envd /init did not share the 220ms budget", elapsed)
	}
}

func TestEnvdInitPayloadPreservesMMDSCredentialPolicy(t *testing.T) {
	for _, tt := range []struct {
		name    string
		enabled bool
	}{
		{name: "disabled"},
		{name: "enabled", enabled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.MMDS.Enabled = tt.enabled
			o := &Orchestrator{cfg: cfg}
			sb := &types.Sandbox{
				ID:              "payload-policy",
				EnvdUDS:         filepath.Join(shortOrchestratorTestDir(t), "envd.sock"),
				EnvdAccessToken: "envd-token",
				Env:             map[string]string{"TEST_KEY": "test-value"},
			}
			type observedRequest struct {
				method      string
				path        string
				contentType string
				payload     map[string]any
				err         error
			}
			observed := make(chan observedRequest, 1)
			startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got := observedRequest{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type")}
				got.err = json.NewDecoder(r.Body).Decode(&got.payload)
				observed <- got
				w.WriteHeader(http.StatusNoContent)
			}))

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := o.envdInit(ctx, sb); err != nil {
				t.Fatal(err)
			}
			got := <-observed
			if got.err != nil {
				t.Fatalf("decode /init payload: %v", got.err)
			}
			if got.method != http.MethodPost || got.path != "/init" || got.contentType != "application/json" {
				t.Fatalf("envd request = %s %s content-type=%q", got.method, got.path, got.contentType)
			}
			if got.payload["defaultUser"] != "user" || got.payload["defaultWorkdir"] != "/home/user" {
				t.Fatalf("envd defaults = %+v", got.payload)
			}
			envVars, ok := got.payload["envVars"].(map[string]any)
			if !ok || envVars["TEST_KEY"] != "test-value" {
				t.Fatalf("envVars = %+v", got.payload["envVars"])
			}
			timestamp, ok := got.payload["timestamp"].(string)
			if !ok {
				t.Fatalf("timestamp = %v, want string", got.payload["timestamp"])
			}
			if _, err := time.Parse(time.RFC3339, timestamp); err != nil {
				t.Fatalf("timestamp = %q: %v", timestamp, err)
			}
			token, hasToken := got.payload["accessToken"]
			if tt.enabled && (!hasToken || token != sb.EnvdAccessToken) {
				t.Fatalf("MMDS-enabled accessToken = %v, present=%t", token, hasToken)
			}
			if !tt.enabled && hasToken {
				t.Fatalf("MMDS-disabled payload contains accessToken: %+v", got.payload)
			}
		})
	}
}

func TestEnvdInitAllowsSlowRequestWithinSharedDeadline(t *testing.T) {
	cfg := &config.Config{}
	o := &Orchestrator{cfg: cfg}
	sb := &types.Sandbox{ID: "slow-init", EnvdUDS: filepath.Join(shortOrchestratorTestDir(t), "envd.sock")}
	var attempts atomic.Int64
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		timer := time.NewTimer(100 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := o.envdInit(ctx, sb); err != nil {
		t.Fatal(err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("envd /init attempts = %d, want one request within the shared deadline", got)
	}
}

func TestEnvdInitRetriesOnlyTransportErrors(t *testing.T) {
	cfg := &config.Config{}
	o := &Orchestrator{cfg: cfg}
	sb := &types.Sandbox{ID: "transport-retry", EnvdUDS: filepath.Join(shortOrchestratorTestDir(t), "envd.sock")}
	var attempts atomic.Int64
	serverErrors := make(chan error, 1)
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 3 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				select {
				case serverErrors <- err:
				default:
				}
				return
			}
			_ = conn.Close()
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := o.envdInit(ctx, sb); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverErrors:
		t.Fatalf("force transport failure: %v", err)
	default:
	}
	if got := attempts.Load(); got != 4 {
		t.Fatalf("envd /init attempts = %d, want 4", got)
	}
}

func TestEnvdInitNon204FailsWithoutRetryOrResponseDetail(t *testing.T) {
	cfg := &config.Config{}
	o := &Orchestrator{cfg: cfg}
	sb := &types.Sandbox{ID: "hard-failure", EnvdUDS: filepath.Join(shortOrchestratorTestDir(t), "envd.sock")}
	var attempts atomic.Int64
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "temporary init failure "+strings.Repeat("x", 160))
	}))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := o.envdInit(ctx, sb)
	if err == nil || !strings.Contains(err.Error(), "status 503") || strings.Contains(err.Error(), "temporary init failure") {
		t.Fatalf("envd /init error = %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("non-204 envd /init attempts = %d, want 1", got)
	}
}

func TestEnvdInitRetryDelay(t *testing.T) {
	for _, tt := range []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: 0},
		{failures: 1, want: time.Millisecond},
		{failures: 2, want: 2 * time.Millisecond},
		{failures: 3, want: 4 * time.Millisecond},
		{failures: 4, want: 5 * time.Millisecond},
		{failures: 20, want: 5 * time.Millisecond},
	} {
		if got := envdInitRetryDelay(tt.failures); got != tt.want {
			t.Fatalf("envdInitRetryDelay(%d) = %s, want %s", tt.failures, got, tt.want)
		}
	}
}

func TestFailedCreateEnvdInitTransitionsStartingToDead(t *testing.T) {
	lc := &countingLauncher{}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "init-create-dead")
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "init rejected", http.StatusInternalServerError)
	}))
	events, cancel := o.Subscribe()
	defer cancel()

	launchErr := runFreshLaunchForTest(t, o, ctx, sb, tmpl)
	if launchErr == nil || !strings.Contains(launchErr.Error(), "status 500") {
		t.Fatalf("launch error = %v, want envd /init failure", launchErr)
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored == nil || stored.State != types.StateDead {
		t.Fatalf("stored failed create = %+v, %v; want dead", stored, err)
	}
	if lc.stops.Load() == 0 {
		t.Fatal("failed create did not stop its runner")
	}
	for _, want := range []string{routesync.StateStarting, routesync.TypeRouteBarrier, routesync.StateStarting, routesync.StateStarting, routesync.TypeDelete} {
		select {
		case event := <-events:
			switch want {
			case routesync.TypeRouteBarrier:
				if event.Kind != routesync.TypeRouteBarrier {
					t.Fatalf("failed-create barrier event = %+v", event)
				}
			case routesync.TypeDelete:
				if event.Kind != routesync.TypeDelete || event.SID != sb.ID {
					t.Fatalf("terminal failed-create event = %+v, want delete", event)
				}
			default:
				if event.Kind != routesync.TypeUpsert || event.Route.State != want {
					t.Fatalf("failed-create event = %+v, want upsert %s", event, want)
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("failed create did not publish %s", want)
		}
	}
	var ranged []string
	if err := o.Range(ctx, func(route routesync.RouteEntry) error {
		ranged = append(ranged, route.SandboxID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ranged) != 0 {
		t.Fatalf("dead failed create remained in route snapshot: %v", ranged)
	}
}

func TestFailedCreatePreflightHasNoDurableOrRunnerSideEffects(t *testing.T) {
	lc := &countingLauncher{}
	cfg := &config.Config{}
	cfg.Sandbox.Network.Bare.InnerIP = "invalid"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileBare, "pre-assignment-failure")

	if err := runFreshLaunchForTest(t, o, ctx, sb, tmpl); err == nil {
		t.Fatal("launch with invalid network succeeded")
	}
	stored, err := o.st.Get(ctx, sb.ID)
	if err != nil || stored != nil {
		t.Fatalf("preflight failure persisted sandbox = %+v, %v", stored, err)
	}
	if lc.starts.Load() != 0 {
		t.Fatalf("preflight failure started %d runners", lc.starts.Load())
	}
}

func TestResumeReadinessFailureRollsBackAndTearsDown(t *testing.T) {
	lc := &countingLauncher{readinessWire: []byte("ready\ncontrol_ready\n")}
	cfg := &config.Config{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, _ := launchTestSandbox(t, cfg, types.ProfileBare, "rollback")
	sb.State = types.StatePaused
	sb.LaunchMode = ""
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("b", 64)}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	_, attempt, err := o.ensureResumeAccepted(ctx, sb.ID, nil, types.ResumeRequest{
		Trigger: types.ResumeTriggerConnect,
		Mode:    types.ResumeAuto,
	}, nil)
	if err == nil {
		err = attempt.wait(ctx)
	}
	if err == nil || !strings.Contains(err.Error(), "runtime readiness protocol") {
		t.Fatalf("resume error = %v", err)
	}
	stored, getErr := o.st.Get(ctx, sb.ID)
	if getErr != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" ||
		stored.VswitchPort != "" || stored.RunDir != "" || stored.EnvdUDS != "" || stored.CiUDS != "" {
		t.Fatalf("stored sandbox after failed resume = %+v, %v", stored, getErr)
	}
	if lc.stops.Load() == 0 {
		t.Fatal("readiness failure did not trigger runner teardown")
	}
	if _, statErr := os.Lstat(configsock.ReadinessSocketPath(cfg.Paths.RunRoot, sb.ID)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("ready socket remains after failed resume: %v", statErr)
	}
}

func TestResumeEnvdInitFailurePublishesStartingThenPaused(t *testing.T) {
	lc := &countingLauncher{}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, _ := launchTestSandbox(t, cfg, types.ProfileE2B, "init-rollback")
	sb.State = types.StatePaused
	sb.LaunchMode = ""
	sb.ResumeSource = types.ResumeSource{Kind: types.ResumeSourceSnapshot, Ref: "manifest://" + strings.Repeat("c", 64)}
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int64
	events, cancel := o.Subscribe()
	defer cancel()

	_, attempt, err := o.ensureResumeAccepted(ctx, sb.ID, nil, types.ResumeRequest{
		Trigger: types.ResumeTriggerConnect,
		Mode:    types.ResumeAuto,
	}, nil)
	// Resume admission must remove the old paused RunDir before a new runner
	// owns it. Model the new runner's envd socket only after that cleanup gate.
	startEnvdTestServer(t, sb.EnvdUDS, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "init rejected", http.StatusInternalServerError)
	}))
	if err == nil {
		err = attempt.wait(ctx)
	}
	if err == nil || !strings.Contains(err.Error(), "envd /init") || !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("resume error = %v, want mandatory envd /init failure", err)
	}
	stored, getErr := o.st.Get(ctx, sb.ID)
	if getErr != nil || stored == nil || stored.State != types.StatePaused || stored.RunID != "" ||
		stored.VswitchPort != "" || stored.RunDir != "" || stored.EnvdUDS != "" || stored.CiUDS != "" {
		t.Fatalf("stored sandbox after failed resume = %+v, %v", stored, getErr)
	}
	if lc.stops.Load() == 0 {
		t.Fatal("envd /init failure did not trigger runner teardown")
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("non-204 envd /init attempts = %d, want 1", got)
	}
	for _, want := range []string{routesync.StateStarting, routesync.StateStarting, routesync.StateStarting, routesync.StatePaused} {
		select {
		case event := <-events:
			if event.Kind != routesync.TypeUpsert || event.Route.State != want {
				t.Fatalf("failed resume event = %+v, want upsert %s", event, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("failed resume did not publish %s", want)
		}
	}
	select {
	case event := <-events:
		t.Fatalf("failed resume published an extra route event: %+v", event)
	default:
	}
}

func runFreshLaunchForTest(t *testing.T, o *Orchestrator, ctx context.Context, sb *types.Sandbox, tmpl types.TemplateID) error {
	t.Helper()
	_, attempt, err := o.acceptFreshLaunch(ctx, sb, tmpl, nil)
	if err != nil {
		return err
	}
	return attempt.wait(ctx)
}

func launchTestSandbox(t *testing.T, cfg *config.Config, profile types.Profile, sid string) (*types.Sandbox, types.TemplateID) {
	t.Helper()
	manifestKey := strings.Repeat("a", 64)
	tmpl := types.TemplateID{Profile: profile, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}
	sb := &types.Sandbox{
		ID: sid, Profile: profile, TemplateID: tmpl.String(), State: types.StateStarting, LaunchMode: types.LaunchImage,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: nodepath.SandboxRunDir(cfg.Paths.RunRoot, sid), BaseDir: nodepath.SandboxBaseDir(cfg.Paths.BaseRoot, sid),
		CreatedUnix: 1,
	}
	if profile == types.ProfileE2B {
		sb.EnvdUDS = filepath.Join(sb.RunDir, "envd.sock")
		sb.CiUDS = filepath.Join(sb.RunDir, "ci.sock")
	}
	materializeTestSandboxCredentials(t, sb)
	return sb, tmpl
}

func startEnvdTestServer(t *testing.T, socket string, handler http.Handler) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
}
