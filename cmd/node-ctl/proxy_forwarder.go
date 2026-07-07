package main

import (
	"hash/fnv"
	"log/slog"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
)

// proxyForwarder is the external-mode fallback: when a data-plane request lands on
// the conductor's own listener (rather than directly on the proxy data listener),
// it is forwarded to a registered proxy UDS. The proxy then serves it from the
// shared route view exactly like a direct request. Each request uses a fresh
// CONNECT tunnel and never reuses a worker connection.
type proxyForwarder struct {
	reg *configsock.Registry
	mx  *metrics.M
	log *slog.Logger
}

func newProxyForwarder(reg *configsock.Registry, mx *metrics.M, log *slog.Logger) *proxyForwarder {
	return &proxyForwarder{reg: reg, mx: mx, log: log}
}

func (pf *proxyForwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	sid, port, ok := proxy.ParseSandbox(r)
	if !ok {
		http.Error(w, "bad sandbox host", http.StatusBadRequest)
		return
	}
	targets := pf.reg.ProxyTargets()
	if len(targets) == 0 {
		pf.mx.Inc(`proxy_forwarder_total{result="no_proxy"}`)
		http.Error(w, "no proxy registered", http.StatusBadGateway)
		return
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sid))
	sock := targets[h.Sum32()%uint32(len(targets))]
	pf.forward(w, r, sock, sid, port)
}

// forward sends a CONNECT to a proxy UDS as a chained
// CONNECT: dial the worker, issue a CONNECT carrying the sandbox identity + access
// token, and on 200 splice the client to the worker (which tunnels onward to the
// sandbox). Ordinary HTTP is then written through the same one-shot tunnel.
func (pf *proxyForwarder) forward(w http.ResponseWriter, r *http.Request, sock, sid string, port int) {
	backend, br, resp, err := proxy.DialSandboxConnect(r.Context(), "unix", sock, sid, port, r.Header.Get(proxy.HeaderAccessToken))
	if err != nil {
		pf.mx.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	if resp.StatusCode != http.StatusOK {
		backend.Close()
		pf.mx.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "connect refused by worker", resp.StatusCode)
		return
	}
	if r.Method == http.MethodConnect {
		pf.mx.Inc(`proxy_forwarder_total{result="ok"}`)
		proxy.TunnelBuffered(w, r, backend, br)
		return
	}
	defer backend.Close()
	innerResp, err := proxy.ForwardHTTPOnce(r, backend, br, nil)
	if err != nil {
		pf.mx.Inc(`proxy_forwarder_total{result="error"}`)
		http.Error(w, "proxy unreachable", http.StatusBadGateway)
		return
	}
	defer innerResp.Body.Close()
	pf.mx.Inc(`proxy_forwarder_total{result="ok"}`)
	proxy.WriteHTTPResponse(w, innerResp)
}
