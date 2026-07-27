// Package proxy is the L7 sandbox-traffic reverse proxy. It parses
// <port>-<sid>.<domain> (or E2b-Sandbox-* headers), asks the Router where the
// sandbox lives, validates the data-plane access token, and forwards: e2b control
// ports over the sandbox-ctl --connect UDS, everything else to floatingip:port.
// Streaming-safe (no buffering).
//
// The same Proxy serves both deployment modes — only the Router differs:
//   - internal: the orchestrator itself resolves the route (single-flight resume);
//   - external: a proxy worker resolves it from the shared route view written by
//     the proxy master (parking a request and prompting a Wake until the
//     orchestrator resumes the sandbox).
package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/config"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Kind tells the handler how to reach the sandbox.
type Kind int

const (
	KindNotFound Kind = iota // unknown sandbox -> 404
	KindDeny                 // recognized service unsupported by this route -> 501
	KindUDS                  // dial unix socket (e2b control: --connect)
	KindTCP                  // dial floatingip:port
)

type Route struct {
	Kind        Kind
	UDS         string // KindUDS
	Addr        string // KindTCP, host:port
	AccessToken string // expected token selected for this forwarding target
}

const (
	HeaderSandboxID      = "E2b-Sandbox-Id"
	HeaderSandboxPort    = "E2b-Sandbox-Port"
	HeaderSandboxService = "E2b-Sandbox-Service"
	HeaderAccessToken    = "X-Access-Token"
	HeaderProxyError     = "X-Kuasar-Proxy-Error"

	ProxyErrorBadRequest    = "bad_request"
	ProxyErrorRouteError    = "route_error"
	ProxyErrorNotFound      = "not_found"
	ProxyErrorDenied        = "denied"
	ProxyErrorUnauthorized  = "unauthorized"
	ProxyErrorUpstreamError = "upstream_error"
)

// ConnectService is the canonical logical service selected by a CONNECT request.
// The empty value is the legacy profile + raw-port mapping used when the service
// Header is absent.
type ConnectService string

const (
	ConnectServiceLegacy         ConnectService = ""
	ConnectServiceForward        ConnectService = "forward"
	ConnectServiceE2BEnvd        ConnectService = "e2b:envd"
	ConnectServiceE2BInterpreter ConnectService = "e2b:code-interpreter"
	ConnectServiceExec           ConnectService = "exec"
)

// ConnectTarget carries the canonical CONNECT target. Port is optional for
// logical services and required for legacy and explicit forward targets.
type ConnectTarget struct {
	Service ConnectService
	Port    int
}

// LegacyTarget builds the target used by ordinary HTTP and service-less CONNECT.
func LegacyTarget(port int) ConnectTarget {
	return ConnectTarget{Service: ConnectServiceLegacy, Port: port}
}

// Router resolves a (sandboxID, target) to a Route. It may block to auto-resume a
// paused sandbox (internal) or park awaiting a route push (external), returning
// KindUDS/KindTCP once up, or KindNotFound if it never came up.
type Router interface {
	Route(ctx context.Context, sandboxID string, target ConnectTarget) (Route, error)
}

// ExecIdentity is the credential and node-local identity needed to authorize an
// exec tunnel. ServiceSecret is trusted internal state and must never be logged
// or returned to the client.
type ExecIdentity struct {
	NodeSandboxID string
	AuthSandboxID string
	ServiceSecret string
}

// ExecRouter separates the side-effect-free credential lookup from the
// authorized lifecycle transition. ActivateExec may resume a paused sandbox and
// must re-read its identity before returning.
type ExecRouter interface {
	LookupExec(ctx context.Context, sandboxID string) (ExecIdentity, bool, error)
	ActivateExec(ctx context.Context, sandboxID string, expected ExecIdentity) (ExecIdentity, bool, error)
}

// Counter is the narrow metrics surface the proxy needs.
type Counter interface {
	Inc(name string)
}

type noopCounter struct{}

func (noopCounter) Inc(string) {}

// RouteDialer opens a backend connection for a resolved route.
type RouteDialer func(context.Context, Route) (net.Conn, error)

// RouteForTarget builds the forwarding decision for a resolved sandbox. Explicit
// CONNECT services are authoritative; the legacy service retains the profile +
// raw-port mapping. Shared by internal and external node proxies.
func RouteForTarget(profile, envdUDS, ciUDS, floatingIP, envdAccessToken, forwardAccessToken string, target ConnectTarget) Route {
	switch target.Service {
	case ConnectServiceLegacy:
		if profile == string(types.ProfileE2B) && (target.Port == 49983 || target.Port == 49999) {
			uds := envdUDS
			if target.Port == 49999 {
				uds = ciUDS
			}
			return Route{Kind: KindUDS, UDS: uds, AccessToken: envdAccessToken}
		}
		// A bare sandbox has no reserved logical-service ports. In particular,
		// 49983 and 49999 are ordinary guest TCP destinations.
		return Route{Kind: KindTCP, Addr: fmt.Sprintf("%s:%d", floatingIP, target.Port), AccessToken: forwardAccessToken}
	case ConnectServiceForward:
		return Route{Kind: KindTCP, Addr: fmt.Sprintf("%s:%d", floatingIP, target.Port), AccessToken: forwardAccessToken}
	case ConnectServiceE2BEnvd:
		if profile != string(types.ProfileE2B) {
			return Route{Kind: KindDeny}
		}
		return Route{Kind: KindUDS, UDS: envdUDS, AccessToken: envdAccessToken}
	case ConnectServiceE2BInterpreter:
		if profile != string(types.ProfileE2B) {
			return Route{Kind: KindDeny}
		}
		return Route{Kind: KindUDS, UDS: ciUDS, AccessToken: envdAccessToken}
	case ConnectServiceExec:
		// Exec CONNECT is dispatched to the authenticated ctl.sock path before
		// generic route lookup. Keep direct route-selection callers fail-closed.
		return Route{Kind: KindDeny}
	default:
		// HTTP parsing rejects unknown services before route lookup. Keep direct
		// callers fail-closed as an unsupported target.
		return Route{Kind: KindDeny}
	}
}

type Proxy struct {
	router      Router
	authMode    func() string // config.Auth* (off|log|enforce), read per-request so a
	log         *slog.Logger  // pushed routesync policy can change it centrally
	mx          Counter
	dial        RouteDialer
	execRunRoot string
}

// New builds a proxy over router. authMode is read per request and returns one of
// config.AuthOff/Log/Enforce (nil => enforce). mx may be nil (metrics off).
func New(router Router, authMode func() string, log *slog.Logger, mx Counter) *Proxy {
	return NewWithDialer(router, authMode, log, mx, nil, "")
}

// NewWithDialer builds a proxy with an explicit backend dialer. A nil dialer uses
// the process's current network namespace. execRunRoot is the trusted node run
// root used only by a final node proxy; an empty value leaves exec unavailable.
func NewWithDialer(router Router, authMode func() string, log *slog.Logger, mx Counter, dial RouteDialer, execRunRoot string) *Proxy {
	if authMode == nil {
		authMode = func() string { return config.AuthEnforce }
	}
	if mx == nil {
		mx = noopCounter{}
	}
	if dial == nil {
		dial = directDialRoute
	}
	return &Proxy{router: router, authMode: authMode, log: log, mx: mx, dial: dial, execRunRoot: execRunRoot}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.serveConnect(w, r)
		return
	}
	if r.Header.Get(HeaderSandboxService) == string(ConnectServiceExec) {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		w.Header().Set("Allow", http.MethodConnect)
		writeProxyError(w, http.StatusMethodNotAllowed, "exec requires CONNECT", ProxyErrorBadRequest)
		return
	}
	sid, port, ok := ParseSandbox(r)
	if !ok {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, "bad sandbox host", ProxyErrorBadRequest)
		return
	}
	route, err := p.router.Route(r.Context(), sid, LegacyTarget(port))
	if err != nil {
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
		return
	}
	switch route.Kind {
	case KindNotFound:
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorNotFound)
	case KindDeny:
		p.mx.Inc(`data_requests_total{result="denied"}`)
		writeProxyError(w, http.StatusNotImplemented, "data plane not available on this sandbox", ProxyErrorDenied)
	case KindUDS, KindTCP:
		if !p.authorized(r, route, port) {
			p.mx.Inc(`data_requests_total{result="unauthorized"}`)
			writeProxyError(w, http.StatusUnauthorized, "invalid access token", ProxyErrorUnauthorized)
			return
		}
		backend, err := p.dial(r.Context(), route)
		if err != nil {
			p.mx.Inc(`data_requests_total{result="upstream_error"}`)
			writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
			return
		}
		defer backend.Close()
		resp, err := ForwardHTTPOnce(r, backend, nil, nil)
		if err != nil {
			p.mx.Inc(`data_requests_total{result="upstream_error"}`)
			writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
			return
		}
		defer resp.Body.Close()
		p.mx.Inc(`data_requests_total{result="ok"}`)
		WriteHTTPResponse(w, resp)
	default:
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
	}
}

// ParseSandbox extracts (sid, port) from the Host header <port>-<sid>.<domain>,
// falling back to the E2b-Sandbox-Id / E2b-Sandbox-Port headers the SDK always
// sets. Exported so the external-mode proxyForwarder can shard by sandbox id.
func ParseSandbox(r *http.Request) (sid string, port int, ok bool) {
	if h := r.Header.Get(HeaderSandboxID); h != "" {
		sid = h
		port, _ = strconv.Atoi(r.Header.Get(HeaderSandboxPort))
		if !validPort(port) {
			return "", 0, false
		}
		return sid, port, true
	}
	return parseSandboxHost(r.Host)
}

func parseSandboxHost(authority string) (sid string, port int, ok bool) {
	host := authority
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
	if !validPort(port) {
		return "", 0, false
	}
	sid = label[dash+1:]
	if sid == "" {
		return "", 0, false
	}
	return sid, port, true
}

func writeProxyError(w http.ResponseWriter, status int, msg, kind string) {
	if kind != "" {
		w.Header().Set(HeaderProxyError, kind)
	}
	http.Error(w, msg, status)
}

// WriteHTTPResponse copies an upstream response to the client and flushes as data
// arrives, preserving streaming semantics without keeping a reusable upstream
// connection alive.
func WriteHTTPResponse(w http.ResponseWriter, resp *http.Response) {
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if resp.Body != nil {
		_, _ = io.Copy(flushWriter{w}, resp.Body)
	}
}

func copyHeader(dst, src http.Header) {
	for k, vals := range src {
		for _, v := range vals {
			dst.Add(k, v)
		}
	}
}
