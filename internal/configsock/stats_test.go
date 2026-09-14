package configsock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type nativeStatsFunc func(context.Context, conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error)

func (f nativeStatsFunc) ReadStats(ctx context.Context, r conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
	return f(ctx, r)
}

func TestNativeStatsRevocationInterruptsIncompleteRequestBody(t *testing.T) {
	registry := NewRegistry()
	var calls atomic.Int64
	socket, _ := startTestServer(t, Deps{Plugins: registry, RouteSource: nativeStatsSource{}, Stats: nativeStatsFunc(func(context.Context, conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
		calls.Add(1)
		return nil, errors.New("incomplete request reached native source")
	})})
	p := registerStatsPlugin(t, socket, registry)
	conn, err := net.Dial("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// Expect/Continue is emitted when the authorized handler starts reading;
	// it proves the request captured its lease before the test revokes it.
	_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Length: 100\r\nExpect: 100-continue\r\n\r\n", PathTelemetryStats)
	if err != nil {
		t.Fatal(err)
	}
	continueResponse := make([]byte, len("HTTP/1.1 100 Continue\r\n\r\n"))
	if _, err := io.ReadFull(conn, continueResponse); err != nil || string(continueResponse) != "HTTP/1.1 100 Continue\r\n\r\n" {
		t.Fatal("handler did not begin reading", string(continueResponse), err)
	}
	registry.Remove(p)
	// Closing a revoked request must not wait for the missing body. The handler
	// may close HTTP/1's unread-body connection after emitting its 503.
	data, err := io.ReadAll(conn)
	if err != nil || !strings.Contains(string(data), "503 Service Unavailable") {
		t.Fatal("incomplete request held the revoked lease", string(data), err)
	}
	if calls.Load() != 0 {
		t.Fatal("incomplete request reached native source")
	}
}

type nativeStatsSource struct{}

func (nativeStatsSource) Range(context.Context, func(routesync.RouteEntry) error) error { return nil }
func (nativeStatsSource) Subscribe() (<-chan routesync.Event, func()) {
	return make(chan routesync.Event), func() {}
}
func (nativeStatsSource) OnWake(context.Context, string) { panic("stats observer issued wake") }
func (nativeStatsSource) Policy() routesync.Policy       { return routesync.Policy{} }

type nativeStatsSink struct {
	ready chan struct{}
	once  sync.Once
}

func (*nativeStatsSink) BeginSync()                             {}
func (*nativeStatsSink) ApplyUpsert(routesync.RouteEntry) error { return nil }
func (*nativeStatsSink) ApplyDelete(string)                     {}
func (*nativeStatsSink) SetPolicy(routesync.Policy)             {}
func (s *nativeStatsSink) Bookmark()                            { s.once.Do(func() { close(s.ready) }) }

func registerStatsPlugin(t *testing.T, socket string, registry *Registry) *Plugin {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	sink := &nativeStatsSink{ready: make(chan struct{})}
	// A write-only telemetry process has no API UDS, but still holds the same
	// fixed-ID route lease. No tenant API key is present on either connection.
	subscriber := routesync.NewSubscriber(func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}, routesync.TelemetryPluginID, routesync.Register{Subscribe: &routesync.Subscribe{Kind: routesync.KindRoute}}, sink, nil, discardLogger())
	done := make(chan struct{})
	go func() { subscriber.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-sink.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("real plugin registration did not become ready")
	}
	registry.mu.Lock()
	p := registry.m[routesync.TelemetryPluginID]
	registry.mu.Unlock()
	if p == nil || p.peerPID != os.Getpid() {
		t.Fatal("registration did not capture actual SO_PEERCRED")
	}
	return p
}

func TestNativeStatsOtherProcess(t *testing.T) {
	socket := os.Getenv("KUASAR_STATS_PEER_TEST_SOCKET")
	if socket == "" {
		return
	}
	code, body := rawPost(t, HTTPClient(socket), PathTelemetryStats, conductorextension.StatsRequest{SandboxIDs: []string{"exact"}, Sections: []string{"usage"}})
	if code != http.StatusForbidden {
		t.Fatalf("unregistered same-UID process received %d: %s", code, body)
	}
}

func TestNativeStatsActualPeerLeaseAndStrictBounds(t *testing.T) {
	registry := NewRegistry()
	pidfile := filepath.Join(t.TempDir(), "plugins.pid")
	mustWrite(t, pidfile, fmt.Sprint(os.Getpid()))
	var calls atomic.Int32
	reader := nativeStatsFunc(func(_ context.Context, r conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
		calls.Add(1)
		return []conductorextension.SandboxStats{{SandboxID: r.SandboxIDs[0], StableID: "label", Usage: json.RawMessage(`{"enabled":false}`)}}, nil
	})
	socket, client := startTestServer(t, Deps{Plugins: registry, RouteSource: nativeStatsSource{}, PluginPidfile: pidfile, Stats: reader})
	request := conductorextension.StatsRequest{SandboxIDs: []string{"exact"}, Sections: []string{"usage"}}
	if code, _ := rawPost(t, client, PathTelemetryStats, request); code != 403 || calls.Load() != 0 {
		t.Fatal("UDS access alone granted native stats", code, calls.Load())
	}
	p := registerStatsPlugin(t, socket, registry)
	rows, err := ReadNativeStats(context.Background(), socket, request)
	if err != nil || len(rows) != 1 || rows[0].SandboxID != "exact" || string(rows[0].Usage) != `{"enabled":false}` {
		t.Fatal(rows, err)
	}
	for _, body := range []string{`null`, `[]`, `{"sandboxIDs":["exact"],"sections":["usage"],"file":"/private"}`, `{"sandboxIDs":[],"sandboxIDs":["exact"]}`, strings.Repeat(" ", 64<<10) + `{}`} {
		if code, _ := rawPostBytes(t, client, PathTelemetryStats, []byte(body)); code != 400 {
			t.Fatal("invalid or oversized request accepted", code)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeStatsOtherProcess$")
	child.Env = append(os.Environ(), "KUASAR_STATS_PEER_TEST_SOCKET="+socket)
	if output, err := child.CombinedOutput(); err != nil {
		t.Fatalf("actual other process: %v\n%s", err, output)
	}
	// An allowlist change also takes effect for subsequent native reads on an
	// already established plugin lease.
	mustWrite(t, pidfile, "2147483647")
	if code, _ := rawPost(t, client, PathTelemetryStats, request); code != 403 {
		t.Fatal("revoked allowlist entry accepted", code)
	}
	mustWrite(t, pidfile, fmt.Sprint(os.Getpid()))
	registry.Remove(p)
	if code, _ := rawPost(t, client, PathTelemetryStats, request); code != 403 {
		t.Fatal("removed lease accepted", code)
	}
	if calls.Load() != 1 {
		t.Fatal("unauthorized or malformed call reached native reader", calls.Load())
	}
}

func TestNativeStatsLeaseRevocationCancelsRead(t *testing.T) {
	for _, reason := range []string{"disconnect", "replacement", "client"} {
		t.Run(reason, func(t *testing.T) {
			registry := NewRegistry()
			entered, stopped := make(chan struct{}), make(chan struct{})
			reader := nativeStatsFunc(func(ctx context.Context, _ conductorextension.StatsRequest) ([]conductorextension.SandboxStats, error) {
				close(entered)
				<-ctx.Done()
				close(stopped)
				return nil, ctx.Err()
			})
			socket, _ := startTestServer(t, Deps{Plugins: registry, RouteSource: nativeStatsSource{}, Stats: reader})
			p := registerStatsPlugin(t, socket, registry)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := ReadNativeStats(ctx, socket, conductorextension.StatsRequest{SandboxIDs: []string{"exact"}, Sections: []string{"usage"}})
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("native read did not start")
			}
			switch reason {
			case "disconnect":
				registry.Remove(p)
			case "replacement":
				registry.Add(&Plugin{ID: routesync.TelemetryPluginID, peerPID: os.Getpid() + 1})
				registry.Remove(p) // Must not revoke the successor.
			case "client":
				cancel()
			}
			select {
			case err := <-done:
				if err == nil || (reason != "client" && !strings.Contains(err.Error(), "HTTP 503")) {
					t.Fatal("canceled read returned success or incorrect error", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("read retained a revoked lease")
			}
			select {
			case <-stopped:
			case <-time.After(3 * time.Second):
				t.Fatal("native operation not canceled")
			}
		})
	}
}

func TestNativeStatsClientRejectsIncompleteOrOversizedBody(t *testing.T) {
	for _, body := range []string{`null`, `[]`, `[{"sandboxID":"stable"}]`, `[{"sandboxID":"exact"}]`, `[{"sandboxID":"exact","usage":null}]`, strings.Repeat(" ", conductorextension.MaxStatsResponseBytes+1)} {
		socket := telemetryTestSocket(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		if _, err := ReadNativeStats(context.Background(), socket, conductorextension.StatsRequest{SandboxIDs: []string{"exact"}, Sections: []string{"usage"}}); err == nil {
			t.Fatal("incomplete or oversized body accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ReadNativeStats(ctx, "/unreachable.sock", conductorextension.StatsRequest{}); !errors.Is(err, context.Canceled) {
		t.Fatal("client lost cancellation", err)
	}
}
