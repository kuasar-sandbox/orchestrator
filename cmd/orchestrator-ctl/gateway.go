package main

import (
	"context"
	"hash/fnv"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
)

// gateway is the external-mode fallback: when a data-plane request lands on the
// orchestrator's own listener (rather than directly on a proxy worker's
// data_listen), it is reverse-proxied to one of the proxy workers over its UDS.
// The proxy then serves it from its synced route table exactly like a direct
// request. Workers are picked by sandbox-id hash for connection affinity.
type gateway struct {
	sockets []string
	rp      *httputil.ReverseProxy
	mx      *metrics.M
	log     *slog.Logger
}

type gwSockKey struct{}

func newGateway(sockets []string, mx *metrics.M, log *slog.Logger) *gateway {
	g := &gateway{sockets: sockets, mx: mx, log: log}
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
	if len(g.sockets) == 0 {
		http.Error(w, "no proxy workers", http.StatusBadGateway)
		return
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(sid))
	sock := g.sockets[h.Sum32()%uint32(len(g.sockets))]
	g.mx.Inc(`gateway_forward_total{result="ok"}`)
	ctx := context.WithValue(r.Context(), gwSockKey{}, sock)
	g.rp.ServeHTTP(w, r.WithContext(ctx))
}
