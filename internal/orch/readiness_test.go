package orch

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
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
			if err := os.MkdirAll(filepath.Join(runRoot, sid), 0o700); err != nil {
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
	if err := os.MkdirAll(filepath.Join(runRoot, sid), 0o700); err != nil {
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
	go func() { done <- o.launch(ctx, sb, tmpl) }()
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

func TestE2BLaunchChecksEnvdAfterRuntimeAndInitRemainsWarning(t *testing.T) {
	assigned := make(chan string, 1)
	lc := &countingLauncher{assigned: assigned, readinessDelay: 150 * time.Millisecond}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.sandboxReadyTimeout = 2 * time.Second
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "e2b-order")

	done := make(chan error, 1)
	go func() { done <- o.launch(ctx, sb, tmpl) }()
	select {
	case <-assigned:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not receive assignment")
	}
	ln, err := net.Listen("unix", sb.EnvdUDS)
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan string, 4)
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.Path
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// /init failure is deliberately still only a warning.
		w.WriteHeader(http.StatusInternalServerError)
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	select {
	case req := <-requests:
		t.Fatalf("envd was queried before runtime ready: %s", req)
	case <-time.After(60 * time.Millisecond):
	}
	select {
	case req := <-requests:
		if req != "GET /health" {
			t.Fatalf("first envd request = %q", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("envd health was not queried after runtime ready")
	}
	select {
	case req := <-requests:
		if req != "POST /init" {
			t.Fatalf("second envd request = %q", req)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("envd /init was not attempted")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("/init warning changed launch success: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("E2B launch did not return")
	}
}

func TestE2BInitUsesLaunchContextAfterReadinessDeadline(t *testing.T) {
	lc := &countingLauncher{}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.sandboxReadyTimeout = 300 * time.Millisecond
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "init-after-ready-deadline")
	if err := os.MkdirAll(sb.RunDir, 0o700); err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("unix", sb.EnvdUDS)
	if err != nil {
		t.Fatal(err)
	}
	initStarted := make(chan struct{})
	initCanceled := make(chan error, 1)
	releaseInit := make(chan struct{})
	defer func() {
		select {
		case <-releaseInit:
		default:
			close(releaseInit)
		}
	}()
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			w.WriteHeader(http.StatusNoContent)
		case "/init":
			close(initStarted)
			select {
			case <-releaseInit:
				w.WriteHeader(http.StatusNoContent)
			case <-r.Context().Done():
				initCanceled <- r.Context().Err()
			}
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	started := time.Now()
	done := make(chan error, 1)
	go func() { done <- o.launch(ctx, sb, tmpl) }()
	select {
	case <-initStarted:
	case err := <-done:
		t.Fatalf("launch returned before envd /init started: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("envd /init did not start")
	}

	// Hold /init past the runtime/health deadline. It must remain live because
	// warning-only initialization retains the parent launch context.
	remaining := time.Until(started.Add(o.sandboxReadyTimeout + 100*time.Millisecond))
	if remaining > 0 {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		select {
		case err := <-done:
			t.Fatalf("launch returned when readiness context expired during /init: %v", err)
		case err := <-initCanceled:
			t.Fatalf("envd /init inherited readiness cancellation: %v", err)
		case <-timer.C:
		}
	}

	close(releaseInit)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("launch did not finish after envd /init completed")
	}
}

func TestE2BRuntimeAndEnvdShareReadinessDeadline(t *testing.T) {
	lc := &countingLauncher{readinessDelay: 200 * time.Millisecond}
	cfg := &config.Config{}
	cfg.Sandbox.Network.E2B.InnerIP = "169.254.0.21/30"
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	o.sandboxReadyTimeout = 400 * time.Millisecond
	sb, tmpl := launchTestSandbox(t, cfg, types.ProfileE2B, "shared-deadline")

	started := time.Now()
	err := o.launch(ctx, sb, tmpl)
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(err.Error(), "envd not ready") {
		t.Fatalf("launch error = %v, want envd readiness failure", err)
	}
	if elapsed < 350*time.Millisecond || elapsed > 600*time.Millisecond {
		t.Fatalf("launch elapsed %s; runtime and envd did not share the 400ms budget", elapsed)
	}
}

func TestResumeReadinessFailureRollsBackAndTearsDown(t *testing.T) {
	lc := &countingLauncher{readinessWire: []byte("ready\ncontrol_ready\n")}
	cfg := &config.Config{}
	o, ctx := newAsyncConnectTestOrchestrator(t, cfg, lc)
	sb, _ := launchTestSandbox(t, cfg, types.ProfileBare, "rollback")
	sb.State = types.StatePaused
	if err := o.st.Put(ctx, sb); err != nil {
		t.Fatal(err)
	}

	err := o.resume(ctx, sb, false)
	if err == nil || !strings.Contains(err.Error(), "runtime readiness protocol") {
		t.Fatalf("resume error = %v", err)
	}
	stored, getErr := o.st.Get(ctx, sb.ID)
	if getErr != nil || stored == nil || stored.State != types.StatePaused {
		t.Fatalf("stored sandbox after failed resume = %+v, %v", stored, getErr)
	}
	if lc.stops.Load() == 0 {
		t.Fatal("readiness failure did not trigger runner teardown")
	}
	if _, statErr := os.Lstat(configsock.ReadinessSocketPath(cfg.Paths.RunRoot, sb.ID)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("ready socket remains after failed resume: %v", statErr)
	}
}

func launchTestSandbox(t *testing.T, cfg *config.Config, profile types.Profile, sid string) (*types.Sandbox, types.TemplateID) {
	t.Helper()
	manifestKey := strings.Repeat("a", 64)
	tmpl := types.TemplateID{Profile: profile, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("b", 64)}
	sb := &types.Sandbox{
		ID: sid, Profile: profile, TemplateID: tmpl.String(), State: types.StateRunning,
		APISecret: deriveTestAPISecret(t, manifestKey), ManifestKey: manifestKey,
		RunDir: filepath.Join(cfg.Paths.RunRoot, sid), BaseDir: filepath.Join(cfg.Paths.BaseRoot, sid),
		CreatedUnix: 1,
	}
	if profile == types.ProfileE2B {
		sb.EnvdUDS = filepath.Join(sb.RunDir, "envd.sock")
		sb.CiUDS = filepath.Join(sb.RunDir, "ci.sock")
	}
	materializeTestSandboxCredentials(t, sb)
	return sb, tmpl
}
