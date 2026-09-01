package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/proxystats"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type connectStubRouter struct {
	r      proxy.Route
	token  string
	target *proxy.ConnectTarget
}

func (s connectStubRouter) LookupRoute(_ context.Context, sid string, target proxy.ConnectTarget) (proxy.RouteBinding, bool, error) {
	if s.target != nil {
		*s.target = target
	}
	return proxy.BindRoute(sid, sid, types.ProfileE2B, s.token, s.token, target), true, nil
}

func (s connectStubRouter) ActivateRoute(_ context.Context, _ proxy.RouteBinding) (proxy.Route, bool, error) {
	return s.r, true, nil
}

// TestProxyForwarderConnectRelay drives CONNECT end to end: client ->
// proxyForwarder -> proxy UDS -> backend. It exercises the transparent
// CONNECT relay used by the external proxy fallback.
func TestProxyForwarderConnectRelay(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Backend the worker tunnels to: a byte echo server.
	backLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer backLn.Close()
	go func() {
		for {
			c, err := backLn.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { defer c.Close(); _, _ = io.Copy(c, c) }(c)
		}
	}()

	// Proxy endpoint: serves CONNECT (to the backend) over its UDS (HTTP/1.1).
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	wln, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer wln.Close()
	var workerTarget proxy.ConnectTarget
	workerStats := proxystats.NewWorkerStats()
	masterStats := proxystats.NewMasterStats(metrics.New(), []string{"worker-0"})
	if err := masterStats.BeginWorker("worker-0", 1); err != nil {
		t.Fatal(err)
	}
	statsCtx, cancelStats := context.WithCancel(context.Background())
	defer cancelStats()
	if _, err := workerStats.StartSender(statsCtx, "worker-0", 1, func(frame proxystats.Frame) error {
		return masterStats.Receive("worker-0", 1, frame)
	}); err != nil {
		t.Fatal(err)
	}
	px := proxy.New(connectStubRouter{
		r:      proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String()},
		token:  "tok",
		target: &workerTarget,
	},
		func() string { return "enforce" }, log, workerStats).WithTrafficTracker(workerStats)
	wsrv := &http.Server{Handler: px}
	go wsrv.Serve(wln)
	defer wsrv.Close()

	// Registry with the proxy endpoint registered as a forward target.
	reg := configsock.NewRegistry()
	reg.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})

	pf := newProxyForwarder(reg, metrics.New(), log)
	ts := httptest.NewServer(pf)
	defer ts.Close()
	_, bport, _ := net.SplitHostPort(backLn.Addr().String())

	// Raw CONNECT to the proxy forwarder; it relays to the worker, which tunnels to backend.
	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "CONNECT ignored:%s HTTP/1.1\r\nHost: ignored:%s\r\nE2b-Sandbox-Id: s1\r\nE2b-Sandbox-Service: forward\r\nE2b-Sandbox-Port: %s\r\nX-Access-Token: tok\r\n\r\nping\n", bport, bport, bport)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relayed CONNECT = %d (want 200)", resp.StatusCode)
	}
	line, _ := br.ReadString('\n')
	if strings.TrimSpace(line) != "ping" {
		t.Fatalf("relayed tunnel echo = %q (want ping)", line)
	}
	if want := (proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: mustAtoi(t, bport)}); workerTarget != want {
		t.Fatalf("worker target = %#v, want %#v", workerTarget, want)
	}
	waitForwarderTraffic(t, masterStats, 0, 1)
	if err := c.(*net.TCPConn).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		t.Fatalf("half-close relay read error = %v, want EOF", err)
	}
	waitForwarderTraffic(t, masterStats, 0, 0)
}

func waitForwarderTraffic(t *testing.T, master *proxystats.MasterStats, parking, egress uint64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := master.SandboxTrafficStats(context.Background(), "s1", "run-1", types.ProfileE2B, types.StateRunning)
		if err == nil && stats.Inflight.Parking == parking && stats.Inflight.Egress == egress {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("conductor fallback traffic did not reach parking=%d egress=%d", parking, egress)
}

func TestProxyForwarderPreservesPortlessLogicalService(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	wln, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer wln.Close()

	type observedConnect struct {
		authority string
		sid       string
		service   string
		port      string
		token     string
	}
	observed := make(chan observedConnect, 1)
	wsrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- observedConnect{
			authority: r.Host,
			sid:       r.Header.Get(proxy.HeaderSandboxID),
			service:   r.Header.Get(proxy.HeaderSandboxService),
			port:      r.Header.Get(proxy.HeaderSandboxPort),
			token:     r.Header.Get(proxy.HeaderAccessToken),
		}
		http.Error(w, "not implemented", http.StatusNotImplemented)
	})}
	go wsrv.Serve(wln)
	defer wsrv.Close()

	reg := configsock.NewRegistry()
	reg.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})
	pf := newProxyForwarder(reg, metrics.New(), log)
	ts := httptest.NewServer(pf)
	defer ts.Close()

	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "CONNECT sandbox:443 HTTP/1.1\r\nHost: sandbox:443\r\n%s: s1\r\n%s: exec\r\n%s: tok\r\n\r\n",
		proxy.HeaderSandboxID, proxy.HeaderSandboxService, proxy.HeaderAccessToken)
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotImplemented)
	}
	got := <-observed
	want := observedConnect{authority: "sandbox:443", sid: "s1", service: "exec", token: "tok"}
	if got != want {
		t.Fatalf("worker CONNECT = %#v, want %#v", got, want)
	}
}

func TestProxyForwarderOrdinaryHTTPIsSentDirectlyOnce(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	wln, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer wln.Close()

	type observedRequest struct {
		method  string
		path    string
		service string
		port    string
		body    string
	}
	observed := make(chan observedRequest, 1)
	wsrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		observed <- observedRequest{
			method: r.Method, path: r.URL.RequestURI(), body: string(body),
			service: r.Header.Get(proxy.HeaderSandboxService), port: r.Header.Get(proxy.HeaderSandboxPort),
		}
		w.WriteHeader(http.StatusNoContent)
	})}
	go wsrv.Serve(wln)
	defer wsrv.Close()

	reg := configsock.NewRegistry()
	reg.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})
	pf := newProxyForwarder(reg, metrics.New(), log)
	ts := httptest.NewServer(pf)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/status?probe=1", strings.NewReader("streamed-once"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(proxy.HeaderSandboxID, "s1")
	req.Header.Set(proxy.HeaderSandboxPort, "49983")
	req.Header.Set(proxy.HeaderSandboxService, "application-defined")
	req.Header.Set(proxy.HeaderAccessToken, "tok")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNoContent)
	}
	got := <-observed
	want := observedRequest{
		method: http.MethodPost, path: "/status?probe=1", service: "application-defined",
		port: "49983", body: "streamed-once",
	}
	if got != want {
		t.Fatalf("worker request = %#v, want %#v", got, want)
	}
}

func TestProxyForwarderLetsPrivateRawRequestReachWorker(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	listener, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	observed := make(chan string, 1)
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observed <- r.Header.Get("X-Sandbox-Id") + " " + r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	})}
	go server.Serve(listener)
	defer server.Close()

	registry := configsock.NewRegistry()
	registry.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})
	forwarder := httptest.NewServer(newProxyForwarder(registry, metrics.New(), log))
	defer forwarder.Close()
	request, err := http.NewRequest(http.MethodGet, forwarder.URL+"/private/sandboxes/s1", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "private.invalid"
	request.Header.Set("X-Sandbox-Id", "s1")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted || <-observed != "s1 /private/sandboxes/s1" {
		t.Fatalf("response=%d", response.StatusCode)
	}
}

func TestProxyForwarderAffinityIsStableForCanonicalAndRawRequests(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := configsock.NewRegistry()
	servers := make([]*http.Server, 0, 2)
	for index := 0; index < 2; index++ {
		workerID := strconv.Itoa(index)
		socket := filepath.Join(t.TempDir(), "px.sock")
		listener, err := net.Listen("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Worker-Id", workerID)
			w.WriteHeader(http.StatusNoContent)
		})}
		servers = append(servers, server)
		go server.Serve(listener)
		defer listener.Close()
		registry.Add(&configsock.Plugin{ID: "px" + workerID, Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: socket}}}})
	}
	defer func() {
		for _, server := range servers {
			_ = server.Close()
		}
	}()
	forwarder := httptest.NewServer(newProxyForwarder(registry, metrics.New(), log))
	defer forwarder.Close()

	do := func(path, host string, canonical bool) string {
		t.Helper()
		request, err := http.NewRequest(http.MethodGet, forwarder.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = host
		if canonical {
			request.Header.Set(proxy.HeaderSandboxID, "stable-s1")
			request.Header.Set(proxy.HeaderSandboxPort, "8080")
		} else {
			request.Header.Set("X-Sandbox-Id", "private-s1")
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.Header.Get("X-Worker-Id")
	}
	rawFirst := do("/private?revision=1", "private.example", false)
	rawSecond := do("/private?revision=2", "private.example", false)
	if rawFirst == "" || rawSecond != rawFirst {
		t.Fatalf("raw affinity changed: first=%q second=%q", rawFirst, rawSecond)
	}
	canonicalFirst := do("/one", "one.example", true)
	canonicalSecond := do("/different", "different.example", true)
	if canonicalFirst == "" || canonicalSecond != canonicalFirst {
		t.Fatalf("canonical affinity changed: first=%q second=%q", canonicalFirst, canonicalSecond)
	}
}

func TestProxyForwarderWithoutExtensionKeepsBuiltInParserError(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	listener, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server := &http.Server{Handler: proxy.New(connectStubRouter{}, nil, log, nil)}
	go server.Serve(listener)
	defer server.Close()
	registry := configsock.NewRegistry()
	registry.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})
	forwarder := httptest.NewServer(newProxyForwarder(registry, metrics.New(), log))
	defer forwarder.Close()
	request, err := http.NewRequest(http.MethodGet, forwarder.URL+"/invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "not-canonical"
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusBadRequest || response.Header.Get(proxy.HeaderProxyError) != proxy.ProxyErrorBadRequest || string(body) != "bad sandbox host\n" {
		t.Fatalf("response=%d headers=%v body=%q", response.StatusCode, response.Header, body)
	}
}

func mustAtoi(t *testing.T, value string) int {
	t.Helper()
	port, err := strconv.Atoi(value)
	if err != nil {
		t.Fatal(err)
	}
	return port
}
