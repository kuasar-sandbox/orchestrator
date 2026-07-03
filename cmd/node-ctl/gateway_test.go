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
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

type connectStubRouter struct{ r proxy.Route }

func (s connectStubRouter) Route(ctx context.Context, sid string, port int) (proxy.Route, error) {
	return s.r, nil
}

// TestGatewayConnectRelay drives a chained CONNECT end to end: client -> gateway ->
// proxy worker (over its UDS) -> backend. It exercises relayConnect, the path
// httputil.ReverseProxy cannot serve.
func TestGatewayConnectRelay(t *testing.T) {
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

	// Proxy worker: serves CONNECT (to the backend) over its UDS (HTTP/1.1).
	workerSock := filepath.Join(t.TempDir(), "px.sock")
	wln, err := net.Listen("unix", workerSock)
	if err != nil {
		t.Fatal(err)
	}
	defer wln.Close()
	px := proxy.New(connectStubRouter{proxy.Route{Kind: proxy.KindTCP, Addr: backLn.Addr().String(), AccessToken: "tok"}},
		func() string { return "enforce" }, log, nil)
	wsrv := &http.Server{Handler: px}
	go wsrv.Serve(wln)
	defer wsrv.Close()

	// Registry with the worker registered as a proxy forward target.
	reg := configsock.NewRegistry()
	reg.Add(&configsock.Plugin{ID: "px0", Caps: routesync.Register{Proxy: &routesync.Proxy{Socket: routesync.Socket{Path: workerSock}}}})

	gw := newGateway(reg, metrics.New(), log)
	ts := httptest.NewServer(gw)
	defer ts.Close()
	_, bport, _ := net.SplitHostPort(backLn.Addr().String())

	// Raw CONNECT to the gateway; it relays to the worker, which tunnels to backend.
	c, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "CONNECT ignored:%s HTTP/1.1\r\nHost: ignored:%s\r\nE2b-Sandbox-Id: s1\r\nX-Access-Token: tok\r\n\r\n", bport, bport)
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
}
