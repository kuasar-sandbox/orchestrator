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

// TestProxyForwarderConnectRelay drives a chained CONNECT end to end: client ->
// proxyForwarder -> proxy UDS -> backend. It exercises the explicit chained
// CONNECT path used by the external proxy fallback.
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
	fmt.Fprintf(c, "CONNECT ignored:%s HTTP/1.1\r\nHost: ignored:%s\r\nE2b-Sandbox-Id: s1\r\nE2b-Sandbox-Service: forward\r\nE2b-Sandbox-Port: %s\r\nX-Access-Token: tok\r\n\r\n", bport, bport, bport)
	br := bufio.NewReader(c)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("relayed CONNECT = %d (want 200)", resp.StatusCode)
	}
	if _, err := io.WriteString(c, "ping\n"); err != nil {
		t.Fatal(err)
	}
	line, _ := br.ReadString('\n')
	if strings.TrimSpace(line) != "ping" {
		t.Fatalf("relayed tunnel echo = %q (want ping)", line)
	}
	if want := (proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: mustAtoi(t, bport)}); workerTarget != want {
		t.Fatalf("worker target = %#v, want %#v", workerTarget, want)
	}
	waitForwarderTraffic(t, masterStats, 0, 1)
	if err := c.Close(); err != nil {
		t.Fatal(err)
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

func TestProxyForwarderOrdinaryHTTPKeepsServiceInsideLegacyTunnel(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	wln, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer wln.Close()

	type observedRequest struct {
		outerService string
		outerPort    string
		innerService string
	}
	observed := make(chan observedRequest, 1)
	wsrv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Error("worker response writer cannot hijack")
			return
		}
		conn, rw, err := hj.Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		if _, err := rw.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			t.Error(err)
			return
		}
		if err := rw.Flush(); err != nil {
			t.Error(err)
			return
		}
		inner, err := http.ReadRequest(rw.Reader)
		if err != nil {
			t.Error(err)
			return
		}
		observed <- observedRequest{
			outerService: r.Header.Get(proxy.HeaderSandboxService),
			outerPort:    r.Header.Get(proxy.HeaderSandboxPort),
			innerService: inner.Header.Get(proxy.HeaderSandboxService),
		}
		_, _ = rw.WriteString("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n")
		_ = rw.Flush()
	})}
	go wsrv.Serve(wln)
	defer wsrv.Close()

	reg := configsock.NewRegistry()
	reg.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})
	pf := newProxyForwarder(reg, metrics.New(), log)
	ts := httptest.NewServer(pf)
	defer ts.Close()

	req, err := http.NewRequest(http.MethodGet, ts.URL+"/status", nil)
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
	want := observedRequest{outerPort: "49983", innerService: "application-defined"}
	if got != want {
		t.Fatalf("worker request = %#v, want %#v", got, want)
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
