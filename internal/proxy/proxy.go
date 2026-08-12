// Package proxy is the L7 sandbox-traffic reverse proxy. It parses
// <port>-<sid>.<domain> (or E2b-Sandbox-* headers), asks the Router where the
// sandbox lives, validates the data-plane access token, and forwards: e2b control
// ports over the sandbox-ctl --connect UDS, everything else to floatingip:port.
// Streaming-safe (no buffering).
//
// The same Proxy serves both deployment modes — only the Router differs:
//   - internal: the orchestrator itself resolves the route (shared launch owner);
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
	Kind Kind
	UDS  string // KindUDS
	Addr string // KindTCP, host:port
}

// RouteBinding is the stable credential subject and exact logical target that an
// ordinary data-plane authorization covers. Lifecycle state, backend addresses,
// run identity, and route revisions deliberately do not participate: they may
// change during a legitimate activation.
type RouteBinding struct {
	SandboxID           string
	AuthSandboxID       string
	Profile             types.Profile
	Target              ConnectTarget
	Kind                Kind
	ExpectedAccessToken string
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

// Router separates side-effect-free ordinary route admission from the authorized
// lifecycle transition. ActivateRoute must revalidate expected before and after
// activation and return a Route built from the latest running record.
type Router interface {
	LookupRoute(ctx context.Context, sandboxID string, target ConnectTarget) (RouteBinding, bool, error)
	ActivateRoute(ctx context.Context, expected RouteBinding) (Route, bool, error)
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
// authorized lifecycle transition. ActivateExec may resume a paused sandbox or
// wait for a starting one and must re-read its identity before returning.
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

// TrafficTracker observes one authorized logical ingress. BeginParking is
// called only after credential admission; the returned handle owns the
// parking→egress→idle state transition for the final backend connection.
type TrafficTracker interface {
	BeginParking(sandboxID string, service ConnectService) TrafficFlow
}

type TrafficFlow interface {
	AttachBackend(net.Conn) net.Conn
	Close()
}

type noopTrafficTracker struct{}
type noopTrafficFlow struct{}

func (noopTrafficTracker) BeginParking(string, ConnectService) TrafficFlow { return noopTrafficFlow{} }
func (noopTrafficFlow) AttachBackend(conn net.Conn) net.Conn               { return conn }
func (noopTrafficFlow) Close()                                             {}

// RouteDialer opens a backend connection for a resolved route.
type RouteDialer func(context.Context, Route) (net.Conn, error)

// BindRoute selects the credential and logical backend kind for an exact target.
// It intentionally contains no dial address.
func BindRoute(sandboxID, authSandboxID string, profile types.Profile, envdAccessToken, forwardAccessToken string, target ConnectTarget) RouteBinding {
	binding := RouteBinding{
		SandboxID:     sandboxID,
		AuthSandboxID: authSandboxID,
		Profile:       profile,
		Target:        target,
		Kind:          routeKindForTarget(profile, target),
	}
	switch binding.Kind {
	case KindUDS:
		binding.ExpectedAccessToken = envdAccessToken
	case KindTCP:
		binding.ExpectedAccessToken = forwardAccessToken
	}
	return binding
}

func routeKindForTarget(profile types.Profile, target ConnectTarget) Kind {
	if profile != types.ProfileE2B && profile != types.ProfileBare {
		return KindDeny
	}
	switch target.Service {
	case ConnectServiceLegacy:
		if profile == types.ProfileE2B && (target.Port == 49983 || target.Port == 49999) {
			return KindUDS
		}
		// A bare sandbox has no reserved logical-service ports. In particular,
		// 49983 and 49999 are ordinary guest TCP destinations.
		return KindTCP
	case ConnectServiceForward:
		return KindTCP
	case ConnectServiceE2BEnvd:
		if profile != types.ProfileE2B {
			return KindDeny
		}
		return KindUDS
	case ConnectServiceE2BInterpreter:
		if profile != types.ProfileE2B {
			return KindDeny
		}
		return KindUDS
	case ConnectServiceExec:
		// Exec CONNECT is dispatched to the authenticated ctl.sock path before
		// generic route lookup. Keep direct route-selection callers fail-closed.
		return KindDeny
	default:
		// HTTP parsing rejects unknown services before route lookup. Keep direct
		// callers fail-closed as an unsupported target.
		return KindDeny
	}
}

// RouteForTarget builds a dial target from a freshly read running sandbox.
func RouteForTarget(profile types.Profile, envdUDS, ciUDS, floatingIP string, target ConnectTarget) Route {
	kind := routeKindForTarget(profile, target)
	switch kind {
	case KindUDS:
		uds := envdUDS
		if target.Service == ConnectServiceE2BInterpreter ||
			(target.Service == ConnectServiceLegacy && target.Port == 49999) {
			uds = ciUDS
		}
		return Route{Kind: kind, UDS: uds}
	case KindTCP:
		return Route{Kind: kind, Addr: fmt.Sprintf("%s:%d", floatingIP, target.Port)}
	default:
		return Route{Kind: kind}
	}
}

type Proxy struct {
	router      Router
	authMode    func() string // config.Auth* (off|log|enforce), read per-request so a
	log         *slog.Logger  // pushed routesync policy can change it centrally
	mx          Counter
	traffic     TrafficTracker
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
	return &Proxy{router: router, authMode: authMode, log: log, mx: mx, traffic: noopTrafficTracker{}, dial: dial, execRunRoot: execRunRoot}
}

// WithTrafficTracker installs per-sandbox traffic accounting and returns p for
// construction-time chaining. A nil tracker restores the no-op implementation.
func (p *Proxy) WithTrafficTracker(tracker TrafficTracker) *Proxy {
	if tracker == nil {
		tracker = noopTrafficTracker{}
	}
	p.traffic = tracker
	return p
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
	route, flow, ok := p.admitRoute(w, r, sid, LegacyTarget(port))
	if !ok {
		return
	}
	defer flow.Close()
	switch route.Kind {
	case KindUDS, KindTCP:
		backend, err := p.dial(r.Context(), route)
		if err != nil {
			p.mx.Inc(`data_requests_total{result="upstream_error"}`)
			writeProxyError(w, http.StatusBadGateway, "upstream error", ProxyErrorUpstreamError)
			return
		}
		backend = flow.AttachBackend(backend)
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
	default: // ActivateRoute returned a non-dialable route despite admission.
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
	}
}

// admitRoute is the common ordinary HTTP/non-exec CONNECT admission sequence:
// side-effect-free lookup, authorization, binding-revalidating activation, then
// a freshly resolved dial route.
func (p *Proxy) admitRoute(w http.ResponseWriter, r *http.Request, sid string, target ConnectTarget) (Route, TrafficFlow, bool) {
	binding, found, err := p.router.LookupRoute(r.Context(), sid, target)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
		return Route{}, nil, false
	}
	if !found {
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorNotFound)
		return Route{}, nil, false
	}
	switch binding.Kind {
	case KindDeny:
		p.mx.Inc(`data_requests_total{result="denied"}`)
		writeProxyError(w, http.StatusNotImplemented, "data plane not available on this sandbox", ProxyErrorDenied)
		return Route{}, nil, false
	case KindUDS, KindTCP:
	default:
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
		return Route{}, nil, false
	}
	if !p.authorized(r, binding) {
		p.mx.Inc(`data_requests_total{result="unauthorized"}`)
		writeProxyError(w, http.StatusUnauthorized, "invalid access token", ProxyErrorUnauthorized)
		return Route{}, nil, false
	}
	flow := p.traffic.BeginParking(binding.SandboxID, trafficService(binding))
	if flow == nil {
		flow = noopTrafficFlow{}
	}
	route, found, err := p.router.ActivateRoute(r.Context(), binding)
	if err != nil {
		flow.Close()
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "sandbox activation failed", ProxyErrorRouteError)
		return Route{}, nil, false
	}
	if !found {
		flow.Close()
		p.mx.Inc(`data_requests_total{result="notfound"}`)
		// Admission already entered parking. Keep the public status, but do not
		// advertise a pre-admission typed stale error to a chained cluster router:
		// retrying this same logical request would create a second ingress.
		writeProxyError(w, http.StatusNotFound, "sandbox not found", ProxyErrorRouteError)
		return Route{}, nil, false
	}
	if route.Kind != binding.Kind || (route.Kind != KindUDS && route.Kind != KindTCP) {
		flow.Close()
		p.mx.Inc(`data_requests_total{result="route_error"}`)
		writeProxyError(w, http.StatusBadGateway, "routing error", ProxyErrorRouteError)
		return Route{}, nil, false
	}
	return route, flow, true
}

func trafficService(binding RouteBinding) ConnectService {
	if binding.Target.Service != ConnectServiceLegacy {
		return binding.Target.Service
	}
	if binding.Profile == types.ProfileE2B {
		switch binding.Target.Port {
		case 49983:
			return ConnectServiceE2BEnvd
		case 49999:
			return ConnectServiceE2BInterpreter
		}
	}
	return ConnectServiceForward
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
