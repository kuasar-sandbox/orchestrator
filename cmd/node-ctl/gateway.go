package main

import (
	"bufio"
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

// gateway is the external-mode fallback: when a data-plane request lands on the
// orchestrator's own listener (rather than directly on a proxy worker's
// data_listen), it is reverse-proxied to one of the registered proxy workers over
// its UDS. The proxy then serves it from its synced route table exactly like a
// direct request. Workers are picked by sandbox-id hash over the live registered
// set (from the plugin registry) for connection affinity.
type gateway struct {
	reg *configsock.Registry
	rp  *httputil.ReverseProxy
	mx  *metrics.M
	log *slog.Logger
}

type gwSockKey struct{}

func newGateway(reg *configsock.Registry, mx *metrics.M, log *slog.Logger) *gateway {
	g := &gateway{reg: reg, mx: mx, log: log}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			sock, _ := ctx.Value(gwSockKey{}).(string)
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
		MaxIdleConnsPerHost: 64,
	}
	g.rp = &httputil.ReverseProxy{
		Director:      func(req *http.Request) { req.URL.Scheme = "http"; req.URL.Host = "proxy" },
		Transport:     tr,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			g.mx.Inc(`gateway_forward_total{result="error"}`)
			http.Error(w, "proxy worker unreachable", http.StatusBadGateway)
		},
	}
	return g
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sid, _, ok := proxy.ParseSandbox(r)
	if !ok {
		http.Error(w, "bad sandbox host", http.StatusBadRequest)
		return
	}
	targets := g.reg.ProxyTargets()
	if len(targets) == 0 {
		g.mx.Inc(`gateway_forward_total{result="no_worker"}`)
		http.Error(w, "no proxy workers registered", http.StatusBadGateway)
		return
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sid))
	sock := targets[h.Sum32()%uint32(len(targets))]
	if r.Method == http.MethodConnect {
		g.relayConnect(w, r, sock) // tunnels (own metrics); ReverseProxy can't
		return
	}
	g.mx.Inc(`gateway_forward_total{result="ok"}`)
	ctx := context.WithValue(r.Context(), gwSockKey{}, sock)
	g.rp.ServeHTTP(w, r.WithContext(ctx))
}

// relayConnect forwards a CONNECT to a proxy worker over its UDS as a chained
// CONNECT: dial the worker, issue a CONNECT carrying the sandbox identity + access
// token, and on 200 splice the client to the worker (which tunnels onward to the
// sandbox). httputil.ReverseProxy cannot tunnel CONNECT, so this path is explicit.
func (g *gateway) relayConnect(w http.ResponseWriter, r *http.Request, sock string) {
	backend, err := (&net.Dialer{}).DialContext(r.Context(), "unix", sock)
	if err != nil {
		g.mx.Inc(`gateway_forward_total{result="error"}`)
		http.Error(w, "proxy worker unreachable", http.StatusBadGateway)
		return
	}
	if err := writeConnectRequest(backend, r); err != nil {
		backend.Close()
		g.mx.Inc(`gateway_forward_total{result="error"}`)
		http.Error(w, "proxy worker unreachable", http.StatusBadGateway)
		return
	}
	br := bufio.NewReader(backend)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusOK {
		backend.Close()
		g.mx.Inc(`gateway_forward_total{result="error"}`)
		code := http.StatusBadGateway
		if err == nil {
			code = resp.StatusCode // surface the worker's 401/404/501
		}
		http.Error(w, "connect refused by worker", code)
		return
	}
	g.mx.Inc(`gateway_forward_total{result="ok"}`)
	// br may hold tunnel bytes prefetched past the CONNECT response; read through it.
	proxy.Tunnel(w, r, &bufConn{Conn: backend, r: br})
}

// writeConnectRequest issues the chained CONNECT to the worker: the original target
// authority (so the worker reads the port) plus the sandbox identity + access-token
// headers (so it resolves + authorizes). Only headers are written; the tunnel bytes
// flow afterward via Tunnel.
func writeConnectRequest(w io.Writer, r *http.Request) error {
	target := r.Host
	if target == "" {
		target = r.URL.Host
	}
	var b strings.Builder
	fmt.Fprintf(&b, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n", target, target)
	for _, h := range []string{"E2b-Sandbox-Id", "E2b-Sandbox-Port", "X-Access-Token"} {
		if v := r.Header.Get(h); v != "" {
			fmt.Fprintf(&b, "%s: %s\r\n", h, v)
		}
	}
	b.WriteString("\r\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// bufConn is a backend connection whose reads come from a bufio.Reader (which may
// hold tunnel bytes prefetched while reading the CONNECT response).
type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }
