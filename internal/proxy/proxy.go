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
	KindNotFound       Kind = iota // unknown sandbox -> 404
	KindDeny                       // e.g. bare data-plane port -> 501
	KindUDS                        // dial unix socket (e2b control: --connect)
	KindTCP                        // dial floatingip:port
	KindWrongNodeEpoch             // expected node identity/epoch is stale
	KindWrongBinding               // expected Registry History Generation/Binding is stale
	KindRouteInactive              // exact execution exists but cannot currently serve
)

type Route struct {
	Kind        Kind
	UDS         string // KindUDS
	Addr        string // KindTCP, host:port
	AccessToken string // expected envd access token ("" = no data-plane auth, e.g. bare)
}

const (
	HeaderSandboxID          = "E2b-Sandbox-Id"
	HeaderSandboxPort        = "E2b-Sandbox-Port"
	HeaderAccessToken        = "X-Access-Token"
	HeaderProxyError         = "X-Kuasar-Proxy-Error"
	HeaderNodeID             = "X-Kuasar-Node-Id"
	HeaderNodeEpoch          = "X-Kuasar-Node-Epoch"
	HeaderRegistryGeneration = "X-Kuasar-Registry-Generation"
	HeaderBindingDigest      = "X-Kuasar-Binding-Digest"

	ProxyErrorBadRequest     = "bad_request"
	ProxyErrorRouteError     = "route_error"
	ProxyErrorNotFound       = "not_found"
	ProxyErrorDenied         = "denied"
	ProxyErrorUnauthorized   = "unauthorized"
	ProxyErrorUpstreamError  = "upstream_error"
	ProxyErrorWrongNodeEpoch = "wrong_node_epoch"
	ProxyErrorWrongBinding   = "wrong_binding"
	ProxyErrorRouteInactive  = "route_inactive"
)

type RouteRequest struct {
	SandboxID                  string
	Port                       int
	ExpectedNodeID             string
	ExpectedNodeEpoch          uint64
	ExpectedRegistryGeneration string
	ExpectedBindingDigest      string
}

func (r RouteRequest) HasExecutionFence() bool {
	return r.ExpectedNodeID != "" || r.ExpectedNodeEpoch != 0 ||
		r.ExpectedRegistryGeneration != "" || r.ExpectedBindingDigest != ""
}

// Router resolves a (sandboxID, port) to a Route. It may block to auto-resume a
// paused sandbox (internal) or park awaiting a route push (external), returning
// KindUDS/KindTCP once up, or KindNotFound if it never came up.
type Router interface {
	Route(ctx context.Context, request RouteRequest) (Route, error)
}

// RouteFenceFailure validates a Router's expected execution against the node's
// protected current projection. managed reports whether the local object has a
// system-owned ExecutionBinding. Validation happens before wake/resume/dial.
func RouteFenceFailure(
	request RouteRequest,
	managed bool,
	nodeID string,
	nodeEpoch uint64,
	registryGeneration, bindingDigest string,
) (Kind, bool) {
	if !managed {
		if request.HasExecutionFence() {
			return KindWrongBinding, true
		}
		return 0, false
	}
	if request.ExpectedNodeID == "" || request.ExpectedNodeEpoch == 0 ||
		request.ExpectedRegistryGeneration == "" || request.ExpectedBindingDigest == "" {
		return KindWrongBinding, true
	}
	if request.ExpectedNodeID != nodeID || request.ExpectedNodeEpoch != nodeEpoch {
		return KindWrongNodeEpoch, true
	}
	if request.ExpectedRegistryGeneration != registryGeneration || request.ExpectedBindingDigest != bindingDigest {
		return KindWrongBinding, true
	}
	return 0, false
}

// Counter is the narrow metrics surface the proxy needs.
type Counter interface {
	Inc(name string)
}

type noopCounter struct{}

func (noopCounter) Inc(string) {}

// RouteDialer opens a backend connection for a resolved route.
type RouteDialer func(context.Context, Route) (net.Conn, error)

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

type Proxy struct {
	router   Router
	authMode func() string // config.Auth* (off|log|enforce), read per-request so a
	log      *slog.Logger  // pushed routesync policy can change it centrally
	mx       Counter
	dial     RouteDialer
}

// New builds a proxy over router. authMode is read per request and returns one of
// config.AuthOff/Log/Enforce (nil => enforce). mx may be nil (metrics off).
func New(router Router, authMode func() string, log *slog.Logger, mx Counter) *Proxy {
	return NewWithDialer(router, authMode, log, mx, nil)
}

// NewWithDialer builds a proxy with an explicit backend dialer. A nil dialer uses
// the process's current network namespace.
func NewWithDialer(router Router, authMode func() string, log *slog.Logger, mx Counter, dial RouteDialer) *Proxy {
	if authMode == nil {
		authMode = func() string { return config.AuthEnforce }
	}
	if mx == nil {
		mx = noopCounter{}
	}
	if dial == nil {
		dial = directDialRoute
	}
	return &Proxy{router: router, authMode: authMode, log: log, mx: mx, dial: dial}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodConnect {
		p.serveConnect(w, r)
		return
	}
	sid, port, ok := ParseSandbox(r)
	if !ok {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, "bad sandbox host", ProxyErrorBadRequest)
		return
	}
	request, err := RouteRequestFromHTTP(r, sid, port)
	if err != nil {
		p.mx.Inc(`data_requests_total{result="badrequest"}`)
		writeProxyError(w, http.StatusBadRequest, err.Error(), ProxyErrorBadRequest)
		return
	}
	route, err := p.router.Route(r.Context(), request)
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
	case KindWrongNodeEpoch:
		p.mx.Inc(`data_requests_total{result="wrong_node_epoch"}`)
		writeProxyError(w, http.StatusConflict, "wrong node epoch", ProxyErrorWrongNodeEpoch)
	case KindWrongBinding:
		p.mx.Inc(`data_requests_total{result="wrong_binding"}`)
		writeProxyError(w, http.StatusConflict, "wrong execution binding", ProxyErrorWrongBinding)
	case KindRouteInactive:
		p.mx.Inc(`data_requests_total{result="route_inactive"}`)
		writeProxyError(w, http.StatusConflict, "route inactive", ProxyErrorRouteInactive)
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

func RouteRequestFromHTTP(r *http.Request, sid string, port int) (RouteRequest, error) {
	request := RouteRequest{SandboxID: sid, Port: port}
	request.ExpectedNodeID = strings.TrimSpace(r.Header.Get(HeaderNodeID))
	request.ExpectedRegistryGeneration = strings.TrimSpace(r.Header.Get(HeaderRegistryGeneration))
	request.ExpectedBindingDigest = strings.TrimSpace(r.Header.Get(HeaderBindingDigest))
	if raw := strings.TrimSpace(r.Header.Get(HeaderNodeEpoch)); raw != "" {
		epoch, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || epoch == 0 {
			return RouteRequest{}, fmt.Errorf("bad node epoch")
		}
		request.ExpectedNodeEpoch = epoch
	}
	if request.HasExecutionFence() && (request.ExpectedNodeID == "" || request.ExpectedNodeEpoch == 0 ||
		request.ExpectedRegistryGeneration == "" || request.ExpectedBindingDigest == "") {
		return RouteRequest{}, fmt.Errorf("incomplete execution fence")
	}
	return request, nil
}

// ParseSandbox extracts (sid, port) from the Host header <port>-<sid>.<domain>,
// falling back to the E2b-Sandbox-Id / E2b-Sandbox-Port headers the SDK always
// sets. Exported so the external-mode proxyForwarder can shard by sandbox id.
func ParseSandbox(r *http.Request) (sid string, port int, ok bool) {
	if h := r.Header.Get(HeaderSandboxID); h != "" {
		sid = h
		port, _ = strconv.Atoi(r.Header.Get(HeaderSandboxPort))
		if port <= 0 {
			return "", 0, false
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
