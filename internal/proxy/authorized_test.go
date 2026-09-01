package proxy_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type orderedEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *orderedEvents) add(event string) {
	e.mu.Lock()
	e.events = append(e.events, event)
	e.mu.Unlock()
}

func (e *orderedEvents) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

type orderedAuthorizedRouter struct {
	events  *orderedEvents
	binding proxy.RouteBinding
	route   proxy.Route
}

func (r orderedAuthorizedRouter) LookupRoute(context.Context, string, proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	r.events.add("lookup")
	return r.binding, true, nil
}

func (r orderedAuthorizedRouter) ActivateRoute(_ context.Context, binding proxy.RouteBinding) (proxy.Route, bool, error) {
	r.events.add("activate")
	return r.route, binding == r.binding, nil
}

type orderedTraffic struct{ events *orderedEvents }
type orderedFlow struct{ events *orderedEvents }

func (t orderedTraffic) BeginParking(string, proxy.ConnectService) proxy.TrafficFlow {
	t.events.add("parking")
	return orderedFlow{events: t.events}
}

func (f orderedFlow) AttachBackend(conn net.Conn) net.Conn {
	f.events.add("attach")
	return conn
}

func (f orderedFlow) Close() { f.events.add("close") }

func TestForwardAuthorizedHTTPOrderRewriteAndNoCoreTokenCheck(t *testing.T) {
	target := proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}
	events := &orderedEvents{}
	binding := proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd-token", "core-token", target)
	server, client := net.Pipe()
	backendDone := make(chan error, 1)
	go func() {
		defer server.Close()
		request, err := http.ReadRequest(bufio.NewReader(server))
		if err != nil {
			backendDone <- err
			return
		}
		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		if err != nil || string(body) != "payload" {
			backendDone <- fmt.Errorf("guest body=%q err=%v", body, err)
			return
		}
		events.add("guest")
		if request.URL.Path != "/guest" || request.Host != "guest.internal" ||
			request.Header.Get("X-Private-Rewrite") != "yes" || request.Header.Get("Connection") != "close" {
			backendDone <- fmt.Errorf("guest request = method=%q host=%q path=%q headers=%v", request.Method, request.Host, request.URL.Path, request.Header)
			return
		}
		_, err = io.WriteString(server, "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
		backendDone <- err
	}()
	px := proxy.NewWithDialer(
		orderedAuthorizedRouter{events: events, binding: binding, route: proxy.Route{Kind: proxy.KindTCP, Addr: "unused:8080"}},
		func() string { return "enforce" }, slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			events.add("dial")
			return client, nil
		}, "",
	).WithTrafficTracker(orderedTraffic{events: events})

	request := httptest.NewRequest(http.MethodPost, "http://private.local/private", strings.NewReader("payload"))
	request.Header.Set("Connection", "keep-alive")
	response := httptest.NewRecorder()
	px.ForwardAuthorized(response, request, proxy.AuthorizedForwardRequest{
		SandboxID: "node-s1",
		Target:    target,
		Revalidate: func(context.Context) error {
			events.add("revalidate")
			return nil
		},
		Rewrite: func(outbound *http.Request) error {
			events.add("rewrite")
			outbound.URL.Path = "/guest"
			outbound.Host = "guest.internal"
			outbound.Header.Set("X-Private-Rewrite", "yes")
			outbound.Header.Set("Connection", "keep-alive")
			return nil
		},
	})
	if err := <-backendDone; err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}
	if request.URL.Path != "/private" || request.Host != "private.local" || request.Header.Get("X-Private-Rewrite") != "" {
		t.Fatalf("original request was mutated: host=%q path=%q headers=%v", request.Host, request.URL.Path, request.Header)
	}
	want := []string{"lookup", "parking", "activate", "revalidate", "dial", "attach", "rewrite", "guest", "close"}
	if got := events.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events=%v want=%v", got, want)
	}
}

func TestForwardAuthorizedRevalidateFailureDoesNotDial(t *testing.T) {
	target := proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}
	router := &admissionRouter{
		binding: proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
		route:   proxy.Route{Kind: proxy.KindTCP, Addr: "unused:8080"}, found: true, activateOK: true,
	}
	traffic := &recordingTrafficTracker{}
	var dials atomic.Int32
	px := proxy.NewWithDialer(router, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
		func(context.Context, proxy.Route) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("must not dial")
		}, "").WithTrafficTracker(traffic)
	response := httptest.NewRecorder()
	px.ForwardAuthorized(response, httptest.NewRequest(http.MethodGet, "http://private/", nil), proxy.AuthorizedForwardRequest{
		SandboxID: "node-s1", Target: target,
		Revalidate: func(context.Context) error { return errors.New("private revision changed") },
	})
	if response.Code != http.StatusConflict || response.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorStale {
		t.Fatalf("response=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
	if dials.Load() != 0 || traffic.begins.Load() != 1 || traffic.attaches.Load() != 0 || traffic.closes.Load() != 1 {
		t.Fatalf("dials=%d traffic begins=%d attaches=%d closes=%d", dials.Load(), traffic.begins.Load(), traffic.attaches.Load(), traffic.closes.Load())
	}
}

func TestForwardAuthorizedRewriteFailureWritesNoGuestBytes(t *testing.T) {
	target := proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}
	router := &admissionRouter{
		binding: proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
		route:   proxy.Route{Kind: proxy.KindTCP, Addr: "unused:8080"}, found: true, activateOK: true,
	}
	traffic := &recordingTrafficTracker{}
	server, client := net.Pipe()
	guestRead := make(chan int, 1)
	go func() {
		defer server.Close()
		_ = server.SetReadDeadline(time.Now().Add(time.Second))
		buffer := make([]byte, 1)
		count, _ := server.Read(buffer)
		guestRead <- count
	}()
	px := proxy.NewWithDialer(router, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil,
		func(context.Context, proxy.Route) (net.Conn, error) { return client, nil }, "").WithTrafficTracker(traffic)
	request := httptest.NewRequest(http.MethodGet, "http://private/original", nil)
	response := httptest.NewRecorder()
	px.ForwardAuthorized(response, request, proxy.AuthorizedForwardRequest{
		SandboxID: "node-s1", Target: target,
		Rewrite: func(outbound *http.Request) error {
			outbound.URL.Path = "/must-not-escape"
			return errors.New("private rewrite rejected")
		},
	})
	if response.Code != http.StatusBadRequest || response.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorBadRequest {
		t.Fatalf("response=%d headers=%v body=%q", response.Code, response.Header(), response.Body.String())
	}
	if count := <-guestRead; count != 0 {
		t.Fatalf("guest received %d bytes before rewrite rejection", count)
	}
	if request.URL.Path != "/original" || traffic.begins.Load() != 1 || traffic.attaches.Load() != 1 || traffic.closes.Load() != 1 {
		t.Fatalf("request path=%q traffic begins=%d attaches=%d closes=%d", request.URL.Path, traffic.begins.Load(), traffic.attaches.Load(), traffic.closes.Load())
	}
}

func TestForwardAuthorizedConnectSkipsRewriteAndOwnsTraffic(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	go func() {
		conn, err := backend.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(conn, conn)
	}()
	target := proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 8080}
	router := &admissionRouter{
		binding: proxy.BindRoute("node-s1", "stable-s1", types.ProfileE2B, "envd", "forward", target),
		route:   proxy.Route{Kind: proxy.KindTCP, Addr: backend.Addr().String()}, found: true, activateOK: true,
	}
	traffic := &recordingTrafficTracker{}
	px := proxy.New(router, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil).WithTrafficTracker(traffic)
	var rewrites atomic.Int32
	var revalidations atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		px.ForwardAuthorized(w, r, proxy.AuthorizedForwardRequest{
			SandboxID: "node-s1", Target: target,
			Revalidate: func(context.Context) error { revalidations.Add(1); return nil },
			Rewrite:    func(*http.Request) error { rewrites.Add(1); return nil },
		})
	}))
	defer server.Close()
	client, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(client, "CONNECT private HTTP/1.1\r\nHost: private\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(client)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status=%d", response.StatusCode)
	}
	if _, err := io.WriteString(client, "ping\n"); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || line != "ping\n" {
		t.Fatalf("echo=%q err=%v", line, err)
	}
	_ = client.Close()
	deadline := time.Now().Add(time.Second)
	for traffic.closes.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if rewrites.Load() != 0 || revalidations.Load() != 1 || traffic.begins.Load() != 1 || traffic.attaches.Load() != 1 || traffic.closes.Load() != 1 {
		t.Fatalf("rewrite=%d revalidate=%d traffic begins=%d attaches=%d closes=%d", rewrites.Load(), revalidations.Load(), traffic.begins.Load(), traffic.attaches.Load(), traffic.closes.Load())
	}
}

func TestForwardAuthorizedRejectsNativeExecBeforeLookup(t *testing.T) {
	router := &admissionRouter{}
	px := proxy.New(router, nil, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)
	response := httptest.NewRecorder()
	px.ForwardAuthorized(response, httptest.NewRequest(http.MethodConnect, "http://private", nil), proxy.AuthorizedForwardRequest{
		SandboxID: "node-s1", Target: proxy.ConnectTarget{Service: proxy.ConnectServiceExec},
	})
	if response.Code != http.StatusNotImplemented || response.Header().Get(proxy.HeaderProxyError) != proxy.ProxyErrorDenied || router.lookupCalls.Load() != 0 {
		t.Fatalf("response=%d headers=%v lookup=%d", response.Code, response.Header(), router.lookupCalls.Load())
	}
}
