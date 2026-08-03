package orch

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestWaitReady(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) (*Orchestrator, *types.Sandbox, *http.Client, context.Context)
		wantErr    func(error) bool
		maxElapsed time.Duration
	}{
		{
			name: "retries promptly",
			setup: func(t *testing.T) (*Orchestrator, *types.Sandbox, *http.Client, context.Context) {
				sock := filepath.Join(t.TempDir(), "envd.sock")
				ln, err := net.Listen("unix", sock)
				if err != nil {
					t.Fatal(err)
				}
				var probes atomic.Int32
				srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					if probes.Add(1) == 1 {
						http.Error(w, "not ready", http.StatusServiceUnavailable)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				})}
				go func() { _ = srv.Serve(ln) }()
				t.Cleanup(func() { _ = srv.Close() })
				t.Cleanup(func() {
					if got := probes.Load(); got != 2 {
						t.Errorf("health probes = %d, want 2", got)
					}
				})
				cl := udsClient(sock)
				t.Cleanup(cl.CloseIdleConnections)
				return &Orchestrator{}, &types.Sandbox{ID: "test", EnvdUDS: sock}, cl, context.Background()
			},
			wantErr:    func(err error) bool { return err == nil },
			maxElapsed: 200 * time.Millisecond,
		},
		{
			name: "honors cancellation",
			setup: func(t *testing.T) (*Orchestrator, *types.Sandbox, *http.Client, context.Context) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				sb := &types.Sandbox{ID: "test", EnvdUDS: filepath.Join(t.TempDir(), "missing.sock")}
				cl := udsClient(sb.EnvdUDS)
				t.Cleanup(cl.CloseIdleConnections)
				return &Orchestrator{}, sb, cl, ctx
			},
			wantErr: func(err error) bool { return errors.Is(err, context.Canceled) },
		},
		{
			name: "fails when assigned runner exits",
			setup: func(t *testing.T) (*Orchestrator, *types.Sandbox, *http.Client, context.Context) {
				runRoot := t.TempDir()
				sb := &types.Sandbox{ID: "test", RunID: "sr-dead", EnvdUDS: filepath.Join(runRoot, "missing.sock")}
				cl := udsClient(sb.EnvdUDS)
				t.Cleanup(cl.CloseIdleConnections)
				return &Orchestrator{runnerPool: &runPool{runRoot: runRoot}}, sb, cl, context.Background()
			},
			wantErr: func(err error) bool {
				return err != nil && strings.Contains(err.Error(), "exited before envd was ready")
			},
			maxElapsed: 100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, sb, cl, ctx := tt.setup(t)
			start := time.Now()
			err := o.waitReady(ctx, sb, cl, time.Second)
			if !tt.wantErr(err) {
				t.Fatalf("waitReady error = %v", err)
			}
			if tt.maxElapsed > 0 && time.Since(start) >= tt.maxElapsed {
				t.Fatalf("waitReady took %s, want less than %s", time.Since(start), tt.maxElapsed)
			}
		})
	}
}

func TestWaitReadyAndInitReuseConnection(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "envd.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	var health, init atomic.Int32
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/health":
			health.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case "/init":
			init.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	var dials atomic.Int32
	cl := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials.Add(1)
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	t.Cleanup(cl.CloseIdleConnections)
	o := &Orchestrator{cfg: &config.Config{}}
	sb := &types.Sandbox{ID: "test", EnvdUDS: sock}
	if err := o.waitReady(context.Background(), sb, cl, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := o.envdInit(context.Background(), sb, cl); err != nil {
		t.Fatal(err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("UDS dials = %d, want 1 for health + init", got)
	}
	if health.Load() != 1 || init.Load() != 1 {
		t.Fatalf("requests: health=%d init=%d, want 1 each", health.Load(), init.Load())
	}
}
