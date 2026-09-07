package proxy_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxyadmission"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// The limiter is deliberately an always-admit test double: transport behavior
// must be identical for unlimited and admitted limited requests.
type cancellationPolicyRouter struct{ admission proxyadmission.Binding }

func (r cancellationPolicyRouter) LookupRoute(_ context.Context, sid string, target proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	binding := proxy.BindRoute(sid, sid, types.ProfileBare, "", "test-token", target)
	binding.Admission = r.admission
	return binding, true, nil
}

func (r cancellationPolicyRouter) ActivateRoute(_ context.Context, binding proxy.RouteBinding) (proxy.Route, bool, error) {
	return proxy.Route{Kind: binding.Kind, Addr: "127.0.0.1:8080"}, true, nil
}

type cancellationPolicyTracker struct{ closes atomic.Int32 }
type cancellationPolicyFlow struct{ tracker *cancellationPolicyTracker }
type cancellationPolicyConn struct {
	net.Conn
	tracker *cancellationPolicyTracker
	once    sync.Once
}

func (t *cancellationPolicyTracker) BeginParking(string, proxy.ConnectService) proxy.TrafficFlow {
	return cancellationPolicyFlow{tracker: t}
}
func (t *cancellationPolicyTracker) TryBeginParking(string, proxy.ConnectService, proxyadmission.Binding) (proxy.TrafficFlow, error) {
	return cancellationPolicyFlow{tracker: t}, nil
}
func (f cancellationPolicyFlow) AttachBackend(conn net.Conn) net.Conn {
	return &cancellationPolicyConn{Conn: conn, tracker: f.tracker}
}
func (c *cancellationPolicyConn) Close() error {
	var err error
	c.once.Do(func() {
		err = c.Conn.Close()
		c.tracker.closes.Add(1)
	})
	return err
}
func (c *cancellationPolicyConn) CloseWrite() error {
	if closer, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return closer.CloseWrite()
	}
	return nil
}
func (cancellationPolicyFlow) Close() {}

func TestBackendCancellationDoesNotDependOnTrafficPolicy(t *testing.T) {
	for _, limited := range []bool{false, true} {
		for _, private := range []bool{false, true} {
			for _, h2 := range []bool{false, true} {
				t.Run(fmt.Sprintf("limited=%v/private=%v/h2=%v", limited, private, h2), func(t *testing.T) {
					var binding proxyadmission.Binding
					if limited {
						binding = proxyadmission.Binding{Slot: 1, Generation: 1, Limits: config.MaxInflight{Total: 10}}
					}
					backend, peer := net.Pipe()
					defer backend.Close()
					defer peer.Close()
					dialed := make(chan struct{})
					tracker := &cancellationPolicyTracker{}
					p := proxy.NewWithDialer(cancellationPolicyRouter{binding}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
						func(context.Context, proxy.Route) (net.Conn, error) {
							close(dialed)
							return backend, nil
						}, "").WithTrafficTracker(tracker)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					r := httptest.NewRequest(http.MethodGet, "http://sandbox:8080/", nil).WithContext(ctx)
					r.Header.Set(proxy.HeaderSandboxID, "s1")
					r.Header.Set(proxy.HeaderSandboxPort, "8080")
					r.Header.Set(proxy.HeaderAccessToken, "test-token")
					if h2 {
						r.Method, r.Proto, r.ProtoMajor, r.ProtoMinor = http.MethodConnect, "HTTP/2.0", 2, 0
						reader, writer := io.Pipe()
						defer reader.Close()
						defer writer.Close()
						r.Body = reader
					}
					headerRead := make(chan error, 1)
					peerDone := make(chan struct{})
					go func() {
						defer close(peerDone)
						if !h2 {
							request, err := http.ReadRequest(bufio.NewReader(peer))
							if request != nil {
								_ = request.Body.Close()
							}
							headerRead <- err
						}
						_, _ = io.Copy(io.Discard, peer)
					}()
					done := make(chan struct{})
					go func() {
						defer close(done)
						w := httptest.NewRecorder()
						if private {
							p.ForwardAuthorized(w, r, proxy.AuthorizedForwardRequest{SandboxID: "s1", Target: proxy.LegacyTarget(8080)})
						} else {
							p.ServeHTTP(w, r)
						}
					}()
					select {
					case <-dialed:
					case <-time.After(2 * time.Second):
						t.Fatal("backend was not dialed")
					}
					if !h2 {
						select {
						case err := <-headerRead:
							if err != nil {
								t.Fatal(err)
							}
						case <-time.After(2 * time.Second):
							t.Fatal("request did not reach backend")
						}
					}
					cancel()
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						t.Fatal("cancellation left the backend/handler blocked")
					}
					select {
					case <-peerDone:
					case <-time.After(2 * time.Second):
						t.Fatal("backend peer did not observe closure")
					}
					if got := tracker.closes.Load(); got != 1 {
						t.Fatalf("backend final close count=%d, want 1", got)
					}
				})
			}
		}
	}
}
