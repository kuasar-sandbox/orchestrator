// Package proxy is the L7 sandbox-traffic reverse proxy. It parses
// <port>-<sid>.<domain> (or E2b-Sandbox-* headers), asks the Router where the
// sandbox lives, validates the data-plane access token, and forwards: e2b control
// ports over the sandbox-ctl --connect UDS, everything else to floatingip:port.
// Streaming-safe (no buffering).
//
// The same Proxy serves both deployment modes — only the Router differs:
//   - internal: the orchestrator itself resolves the route (single-flight resume);
//   - external: the proxy worker resolves it from its synced route table (parking
//     a request and prompting a Wake until the orchestrator resumes the sandbox).
package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/config"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/types"
)

// Kind tells the handler how to reach the sandbox.
type Kind int

const (
	KindNotFound Kind = iota // unknown sandbox -> 404
	KindDeny                 // e.g. bare data-plane port -> 501
	KindUDS                  // dial unix socket (e2b control: --connect)
	KindTCP                  // dial floatingip:port
)

type Route struct {
	Kind        Kind
	UDS         string // KindUDS
	Addr        string // KindTCP, host:port
	AccessToken string // expected envd access token ("" = no data-plane auth, e.g. bare)
}

// Router resolves a (sandboxID, port) to a Route. It may block to auto-resume a
// paused sandbox (internal) or park awaiting a route push (external), returning
// KindUDS/KindTCP once up, or KindNotFound if it never came up.
type Router interface {
	Route(ctx context.Context, sandboxID string, port int) (Route, error)
}

// RouteForTarget builds the forwarding decision for a resolved, running sandbox
// from its targets + the requested port. Shared by the internal router (orch) and
// the external route table so both classify ports identically.
func RouteForTarget(profile, envdUDS, ciUDS, floatingIP, accessToken string, port int) Route {
	control := port == 49983 || port == 49999
	if profile == string(types.ProfileE2B) && control {
		uds := envdUDS
		if port == 49999 {
			uds = ciUDS
		}
		return Route{Kind: KindUDS, UDS: uds, AccessToken: accessToken}
	}
	if profile == string(types.ProfileBare) && control {
		return Route{Kind: KindDeny}
	}
	return Route{Kind: KindTCP, Addr: fmt.Sprintf("%s:%d", floatingIP, port), AccessToken: accessToken}
}

// poolKey is the ReverseProxy Transport's idle-connection pool key for a route —
// distinct per backend (floatingip:port for TCP, the UDS path for control) so a
// kept-alive connection is NEVER reused across sandboxes/ports/UDS targets. The
// real dial is DialContext's (from the route in context); this is only the pool
// key (and the addr it ignores). A constant key here is the cross-route-reuse
// bug: a port-forward could reuse an envd-control connection and hit envd.
func poolKey(r Route) string {
	switch r.Kind {
	case KindTCP:
		return r.Addr
	case KindUDS:
		return "uds." + strings.ReplaceAll(strings.Trim(r.UDS, "/"), "/", "-")
	default:
		return "sandbox"
	}
}

type routeKey struct{}

type Proxy struct {
	router   Router
	authMode func() string // config.Auth* (off|log|enforce), read per-request so a
	log      *slog.Logger  // pushed routesync policy can change it centrally
	mx       *metrics.M
	rp       *httputil.ReverseProxy
}

// New builds a proxy over router. authMode is read per request and returns one of
// config.AuthOff/Log/Enforce (nil => enforce). mx may be nil (metrics off).
func New(router Router, authMode func() string, log *slog.Logger, mx *metrics.M) *Proxy {
	if authMode == nil {
		authMode = func() string { return config.AuthEnforce }
	}
	p := &Proxy{router: router, authMode: authMode, log: log, mx: mx}
	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			r, _ := ctx.Value(routeKey{}).(Route)
			return dialRoute(ctx, r)
		},
		ForceAttemptHTTP2:     false, // envd's h2c server also serves h1; h1 carries Connect streams
		MaxIdleConnsPerHost:   64,
		ResponseHeaderTimeout: 0,
	}
	p.rp = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			// Key the idle-connection pool by the RESOLVED route. The Transport
			// pools keep-alive connections by req.URL.Host; a single constant
			// here let one sandbox's request reuse a kept-alive connection that
			// DialContext had opened for a DIFFERENT route (e.g. a port-forward
			// reusing an envd-control UDS connection from a prior exec → the
			// request hits envd and 404s). DialContext still does the real dial
			// from the route in context; the upstream Host header stays req.Host
			// (the client's). Per-route keys confine reuse to the same backend.
			r, _ := req.Context().Value(routeKey{}).(Route)
			req.URL.Host = poolKey(r)
		},
		Transport:     tr,
		FlushInterval: -1, // stream immediately (process.Start / WatchDir / files)
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			p.mx.Inc(`data_requests_total{result="upstream_error"}`)
			http.Error(w, "upstream error", http.StatusBadGateway)
		},
	}
	return p
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.serveConnect(w, r)
		return
	}
	sid, port, ok := ParseSandbox(r)
	if !ok {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		http.Error(w, "bad sandbox host", http.StatusBadRequest)
		return
	}
	route, err := p.router.Route(r.Context(), sid, port)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		http.Error(w, "routing error", http.StatusBadGateway)
		return
	}
	switch route.Kind {
	case KindNotFound:
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		http.Error(w, "sandbox not found", http.StatusNotFound)
	case KindDeny:
		p.mx.Inc(`data_requests_total{result="denied"}`)
		http.Error(w, "data plane not available on this sandbox", http.StatusNotImplemented)
	case KindUDS, KindTCP:
		if !p.authorized(r, route) {
			p.mx.Inc(`data_requests_total{result="unauthorized"}`)
			http.Error(w, "invalid access token", http.StatusUnauthorized)
			return
		}
		p.mx.Inc(`data_requests_total{result="ok"}`)
		ctx := context.WithValue(r.Context(), routeKey{}, route)
		p.rp.ServeHTTP(w, r.WithContext(ctx))
	default:
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		http.Error(w, "routing error", http.StatusBadGateway)
	}
}

// ParseSandbox extracts (sid, port) from the Host header <port>-<sid>.<domain>,
// falling back to the E2b-Sandbox-Id / E2b-Sandbox-Port headers the SDK always
// sets. Exported so the external-mode gateway can shard by sandbox id.
func ParseSandbox(r *http.Request) (sid string, port int, ok bool) {
	if h := r.Header.Get("E2b-Sandbox-Id"); h != "" {
		sid = h
		port, _ = strconv.Atoi(r.Header.Get("E2b-Sandbox-Port"))
		if port == 0 {
			port = 49983
		}
		return sid, port, true
	}
	host := r.Host
	if i := strings.IndexByte(host, ':'); i >= 0 {
		host = host[:i]
	}
	// strip ".<domain>"
	label := host
	if i := strings.IndexByte(host, '.'); i >= 0 {
		label = host[:i]
	}
	// label = <port>-<sid>
	dash := strings.IndexByte(label, '-')
	if dash <= 0 {
		return "", 0, false
	}
	port, err := strconv.Atoi(label[:dash])
	if err != nil {
		return "", 0, false
	}
	sid = label[dash+1:]
	if sid == "" {
		return "", 0, false
	}
	return sid, port, true
}
