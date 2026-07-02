// Package router is the cluster's e2b-compatible unified ingress
// (cluster-router.md): it serves the e2b control plane (api.<domain>) and the
// data plane (<port>-<sid>.<domain>) by Host, reserving sandboxes through the
// registry's control API and forwarding the data plane to the target node (the
// two-hop path: client -> router -> node data endpoint -> guest).
package router

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	proxypkg "github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/registry"
)

// Headers the cluster ingress reads (cluster.md).
const (
	HeaderGroup     = "X-Kuasar-Sandbox-Group"
	HeaderRouteKey  = "X-Kuasar-Route-Key"
	HeaderAPIKey    = "X-API-KEY"
	HeaderAccessTok = "X-Access-Token"
)

// reserveResult / routeResolve mirror registry route_link JSON.
type reserveResult struct {
	NodeID       string `json:"node_id"`
	SID          string `json:"sid"`
	AccessToken  string `json:"access_token"`
	DataEndpoint string `json:"data_endpoint"`
}
type routeResolve struct {
	SID          string `json:"sid"`
	Group        string `json:"group"`
	RouteKey     string `json:"route_key"`
	NodeID       string `json:"node_id"`
	DataEndpoint string `json:"data_endpoint"`
	AccessToken  string `json:"access_token"`
	State        string `json:"state"`
	cachedAt     time.Time
	lastUsed     time.Time
}

type activeRoute struct {
	rr   *routeResolve
	refs int
}

// buildEntry maps a build to its node; RunCleanup evicts entries older than
// buildTTL so the build map doesn't grow unboundedly over the ingress's life.
type buildEntry struct {
	node string
	at   time.Time
}

const buildTTL = time.Hour

// Router is the e2b unified ingress. It uses registry membership to route each
// group-scoped control request to a route owner, then forwards data to nodes.
type Router struct {
	domain        string
	authMode      string // off | log | enforce — caller api_key auth (§8); default enforce
	dataPlaneAuth string // off | log | enforce — data-plane access-token check (§7)
	mx            *metrics.M
	routeLinkBase string       // static base used by New in tests / size-1 local mode
	routeClient   *http.Client // static client used by New in tests / size-1 local mode
	routeRegistry routeRegistry
	log           *slog.Logger

	buildsMu sync.Mutex
	builds   map[string]buildEntry // build_id -> node (a build is node-bound); TTL-evicted

	cacheMu sync.RWMutex
	cache   map[string]*routeResolve // group\x00route_key\x00sid -> resolved data-plane target
	byKey   map[string]string        // group\x00route_key -> full route cache key (no registry watch)
	active  map[string]*activeRoute  // active group/route or sid forwards; survives normal route-cache churn

	reserveMu       sync.Mutex
	reserveInFlight map[string]*reserveFlight // single-flight Reserve per (group,route_key)

	authTTL time.Duration
	authMu  sync.Mutex
	authOK  map[string]time.Time // group\x00api_key -> cached-valid-until (§8)

	routeTTL         time.Duration
	routeIdleTimeout time.Duration

	fwdTransport *http.Transport // pooled transport for node (data/control/build) forwards

}

type reserveFlight struct {
	done chan struct{}
	res  *reserveResult
	err  error
}

type routeRegistry interface {
	RouteCandidates(ctx context.Context, group string) ([]clusterclient.Endpoint, error)
	Refresh(ctx context.Context) error
}

type staticRouteRegistry struct {
	ep clusterclient.Endpoint
}

func (s staticRouteRegistry) RouteCandidates(context.Context, string) ([]clusterclient.Endpoint, error) {
	if s.ep.Client == nil || s.ep.BaseURL == "" {
		return nil, fmt.Errorf("router: route registry endpoint is not configured")
	}
	return []clusterclient.Endpoint{s.ep}, nil
}

func (s staticRouteRegistry) Refresh(context.Context) error { return nil }

// New builds a Router with one static registry endpoint. Production wiring uses
// NewWithRegistry so group operations go through membership owner selection.
func New(routeAddr, domain string, authTTL time.Duration, routeTLS *tls.Config, log *slog.Logger) *Router {
	if authTTL <= 0 {
		authTTL = 60 * time.Second
	}
	rt := &Router{
		domain: domain, log: log, authTTL: authTTL, authMode: "enforce", mx: metrics.New(),
		builds:           map[string]buildEntry{},
		cache:            map[string]*routeResolve{},
		byKey:            map[string]string{},
		active:           map[string]*activeRoute{},
		reserveInFlight:  map[string]*reserveFlight{},
		authOK:           map[string]time.Time{},
		routeTTL:         5 * time.Minute,
		routeIdleTimeout: 2 * time.Minute,
	}
	base, client, err := clusterclient.HTTPBase(routeAddr, routeTLS)
	if err != nil {
		base, client = "http://"+routeAddr, &http.Client{Transport: &http.Transport{}}
	}
	client.Timeout = 60 * time.Second
	rt.routeLinkBase = base
	rt.routeClient = client
	rt.routeRegistry = staticRouteRegistry{ep: clusterclient.Endpoint{MemberID: "static", BaseURL: base, Client: client}}
	// Pooled transport for node forwards: a higher per-host idle cap than the
	// stdlib default of 2 avoids TCP/TLS churn to a busy node at high density.
	rt.fwdTransport = &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}
	return rt
}

func NewWithRegistry(reg *clusterclient.Registry, domain string, authTTL time.Duration, log *slog.Logger) *Router {
	rt := New("127.0.0.1:7700", domain, authTTL, nil, log)
	rt.routeLinkBase = ""
	rt.routeClient = nil
	rt.routeRegistry = reg
	return rt
}

// SetDataPlaneAuth sets the data-plane access-token enforcement mode (off | log |
// enforce); cluster-ctl router sets it from config before serving.
func (rt *Router) SetDataPlaneAuth(mode string) { rt.dataPlaneAuth = mode }

// SetRouteCache configures local route resolution cache retention. Non-positive
// values disable that particular age check; active requests remain protected by
// the active-route map while they are in flight.
func (rt *Router) SetRouteCache(routeTTL, idleTimeout time.Duration) {
	rt.cacheMu.Lock()
	defer rt.cacheMu.Unlock()
	rt.routeTTL = routeTTL
	rt.routeIdleTimeout = idleTimeout
}

// SetAuthMode sets the caller api_key auth mode (off | log | enforce, §8): off
// skips it (front with an external mTLS/JWT gateway), log warns but allows.
func (rt *Router) SetAuthMode(mode string) {
	if mode != "" {
		rt.authMode = mode
	}
}

// Metrics returns the router's metric registry (Prometheus text); cluster-ctl
// serves it on router.metrics_listen.
func (rt *Router) Metrics() *metrics.M { return rt.mx }

// Handler routes by Host: api.<domain> (and any api.* host) -> control plane;
// everything else -> data plane (<port>-<sid>.<domain>).
func (rt *Router) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if i := strings.IndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if host == "api."+rt.domain {
			rt.serveControl(w, r)
			return
		}
		rt.serveData(w, r, host)
	})
}

// --- control plane ---

func (rt *Router) serveControl(w http.ResponseWriter, r *http.Request) {
	rt.mx.Inc(`router_requests_total{plane="control"}`)
	path := r.URL.Path
	switch {
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/sandboxes"):
		// e2b create: reserve by (group, route_key).
		rt.handleCreate(w, r)
	case r.Method == http.MethodPost && strings.HasSuffix(path, "/templates"):
		// e2b build register: reserve a build node, forward, record build_id -> node.
		rt.handleBuildRegister(w, r)
	case strings.Contains(path, "/builds/"):
		// build trigger / status / files: route by build_id to the recorded node.
		rt.handleBuildForward(w, r)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/sandboxes"):
		// list: the group's sandbox shard (no cross-group).
		rt.handleList(w, r)
	case strings.Contains(path, "/sandboxes/"):
		// get / kill / pause / timeout / connect / export: forward to the node by sid.
		rt.handleSandboxVerb(w, r)
	default:
		http.Error(w, "cluster router: control verb not supported", http.StatusNotImplemented)
	}
}

func (rt *Router) handleCreate(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	routeKey := r.Header.Get(HeaderRouteKey)
	if routeKey == "" {
		routeKey = newRouteKey()
	}
	res, err := rt.reserveByKey(r.Context(), group, routeKey)
	if err != nil {
		rt.log.Warn("router: reserve", "group", group, "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	// Minimal e2b create response (the SDK keys off sandboxID; the data plane uses
	// <port>-<sid>.<domain> + X-Access-Token).
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"sandboxID":   res.SID,
		"routeKey":    routeKey,
		"clientID":    res.NodeID,
		"accessToken": res.AccessToken,
		"domain":      rt.domain,
	})
}

// --- build control plane ---

// buildReserveResult mirrors registry.BuildReserveResult (the registry assigns the
// build/template ids + places the build, §7.5).
type buildReserveResult struct {
	BuildID      string `json:"build_id"`
	TemplateID   string `json:"template_id"`
	NodeID       string `json:"node_id"`
	DataEndpoint string `json:"data_endpoint"`
}

// handleBuildRegister reserves + pre-provisions a build via the registry (which
// assigns the ids, resource-aware-places it, RESERVES the node's build pool, and
// sends build_register with the image-pull creds, §7.5), then synthesizes the e2b
// register response from the registry's ids — it does NOT forward the register to
// the node (the node already holds the build).
func (rt *Router) handleBuildRegister(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	var body struct {
		Name     string   `json:"name"`
		Tags     []string `json:"tags"`
		CPUCount int      `json:"cpuCount"`
		MemoryMB int      `json:"memoryMB"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	var resources *buildResources
	if body.CPUCount > 0 || body.MemoryMB > 0 {
		resources = &buildResources{CPU: body.CPUCount * 1000, Mem: int64(body.MemoryMB) << 20}
	}
	res, err := rt.routeLinkReserveBuild(r.Context(), group, resources)
	if err != nil {
		rt.log.Warn("router: reserve-build", "group", group, "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	rt.buildsMu.Lock()
	rt.builds[res.BuildID] = buildEntry{node: res.DataEndpoint, at: time.Now()}
	rt.buildsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"templateID": res.TemplateID, "buildID": res.BuildID,
		"public": false, "names": nonEmptySlice(body.Name), "tags": body.Tags, "aliases": body.Tags,
	})
}

// buildResources mirrors routesync.BuildResources for the control request body.
type buildResources struct {
	CPU     int   `json:"cpu,omitempty"`
	Mem     int64 `json:"mem,omitempty"`
	Storage int64 `json:"storage,omitempty"`
}

func nonEmptySlice(s string) []string {
	if s == "" {
		return []string{}
	}
	return []string{s}
}

// handleBuildForward routes a build trigger/status/files call to the node that
// holds the build (by build_id), resolving via route_link on a local miss
// (router restart: the in-memory build map is lost but route_link build state is
// replicated).
func (rt *Router) handleBuildForward(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	bid := extractBuildID(r.URL.Path)
	rt.buildsMu.Lock()
	e, ok := rt.builds[bid]
	rt.buildsMu.Unlock()
	node := e.node
	if !ok {
		res, rerr := rt.routeLinkResolveBuild(r.Context(), group, bid)
		if rerr != nil || res.DataEndpoint == "" {
			http.Error(w, "unknown build "+bid, http.StatusNotFound)
			return
		}
		node = res.DataEndpoint
		rt.buildsMu.Lock()
		rt.builds[bid] = buildEntry{node: node, at: time.Now()}
		rt.buildsMu.Unlock()
	}
	rt.forwardBuild(w, r, node)
}

// forwardBuild proxies a build control call (trigger / status / files) to the node
// holding the build (Host api.<domain>; the client's X-API-KEY passes through for
// the node's build auth).
func (rt *Router) forwardBuild(w http.ResponseWriter, r *http.Request, dataEndpoint string) {
	target := &url.URL{Scheme: "http", Host: dataEndpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = rt.fwdTransport
	apiHost := "api." + rt.domain
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = dataEndpoint
		req.Host = apiHost
		req.Header.Del(HeaderAccessTok) // builds authorize via X-API-KEY, not a client token
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		rt.log.Warn("router: build forward", "node", dataEndpoint, "err", e)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

// extractBuildID pulls the build id from an e2b build path: the segment after
// "/builds/" (e.g. /v2/templates/<tid>/builds/<bid>[/status]).
func extractBuildID(path string) string {
	i := strings.Index(path, "/builds/")
	if i < 0 {
		return ""
	}
	rest := path[i+len("/builds/"):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

// --- sandbox control verbs (forward by sid / group shard) ---

// handleSandboxVerb forwards a sid-scoped control verb (get/kill/pause/timeout/
// connect/export) to the node holding the sandbox (cluster-router.md); kill's
// teardown propagates back as a route delete, converging the registry.
func (rt *Router) handleSandboxVerb(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	sid := extractSandboxID(r.URL.Path)
	if sid == "" || sid == "import" {
		http.Error(w, "cluster router: unsupported sandbox path", http.StatusNotImplemented)
		return
	}
	routeKey := r.Header.Get(HeaderRouteKey)
	if routeKey == "" {
		http.Error(w, HeaderRouteKey+" required", http.StatusBadRequest)
		return
	}
	// Authenticate the caller against the sandbox's group before forwarding (the
	// node re-authenticates too, but the ingress must not be an open relay / sid
	// oracle — cluster-router.md/§8).
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	rr := rt.resolveRoute(r.Context(), group, routeKey, sid)
	if rr == nil || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	rt.forwardToNode(w, r, rr.DataEndpoint)
}

// handleList returns the group's sandbox shard (cluster-router.md: list is
// group-local — the registry holds every node's sandboxes for the group).
func (rt *Router) handleList(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	path := fmt.Sprintf("%s?group=%s", registry.RouteLinkListPath, url.QueryEscape(group))
	resp, err := rt.routeLinkHTTP(r.Context(), group, http.MethodGet, path, nil, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	for k, vals := range resp.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "application/json")
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// resolveRoute returns a sandbox's resolved route via the local cache, falling
// back to the control API; nil if unknown.
func (rt *Router) resolveRoute(ctx context.Context, group, routeKey, sid string) *routeResolve {
	if rr := rt.cachedRoute(group, routeKey, sid); rr != nil && routeMatchesIdentity(rr, group, routeKey) {
		return rr
	}
	if rr, err := rt.routeLinkRoute(ctx, group, routeKey, sid); err == nil {
		if !routeMatchesIdentity(rr, group, routeKey) {
			rt.evictRoute(group, routeKey, sid)
			return nil
		}
		rt.rememberRoute(rr)
		return rr
	}
	return nil
}

// forwardToNode proxies a control request to a node's e2b control plane (Host
// api.<domain>; the client's X-API-KEY passes through for the node's auth).
func (rt *Router) forwardToNode(w http.ResponseWriter, r *http.Request, dataEndpoint string) {
	target := &url.URL{Scheme: "http", Host: dataEndpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = rt.fwdTransport
	apiHost := "api." + rt.domain
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = dataEndpoint
		req.Host = apiHost
		req.Header.Del(HeaderAccessTok) // control verbs authorize via X-API-KEY, not a client token
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		rt.log.Warn("router: control forward", "node", dataEndpoint, "err", e)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

// extractSandboxID pulls the sid from an e2b sandbox path: the segment after
// "/sandboxes/" (e.g. /sandboxes/<sid>[/pause]).
func extractSandboxID(path string) string {
	i := strings.Index(path, "/sandboxes/")
	if i < 0 {
		return ""
	}
	rest := path[i+len("/sandboxes/"):]
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		return rest[:j]
	}
	return rest
}

// --- data plane ---

func (rt *Router) serveData(w http.ResponseWriter, r *http.Request, host string) {
	sub := strings.TrimSuffix(host, "."+rt.domain)
	if sub == host { // not under our domain
		// by-(group,route-key): business traffic with no prior create directly
		// triggers Reserve, addressed by headers rather than a <port>-<sid> host.
		if g, rk := r.Header.Get(HeaderGroup), r.Header.Get(HeaderRouteKey); g != "" && rk != "" {
			rt.serveDataByKey(w, r, g, rk)
			return
		}
		http.Error(w, "unknown host", http.StatusNotFound)
		return
	}
	// sub = <port>-<sid>
	i := strings.IndexByte(sub, '-')
	if i < 0 {
		if g, rk := r.Header.Get(HeaderGroup), r.Header.Get(HeaderRouteKey); g != "" && rk != "" {
			rt.serveDataByKey(w, r, g, rk)
			return
		}
		http.Error(w, "bad data host (want <port>-<sid>.<domain>)", http.StatusBadRequest)
		return
	}
	port, err := strconv.Atoi(sub[:i])
	if err != nil || port <= 0 {
		http.Error(w, "bad data host (want <port>-<sid>.<domain>)", http.StatusBadRequest)
		return
	}
	sid := sub[i+1:]
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	routeKey := r.Header.Get(HeaderRouteKey)
	if routeKey == "" {
		http.Error(w, HeaderRouteKey+" required", http.StatusBadRequest)
		return
	}
	// Hot path: serve from the local route cache (zero control round-trip, §5). On a
	// miss (cache lagging / cold), fall back to route_link.
	rr := rt.cachedRoute(group, routeKey, sid)
	if rr != nil && !routeMatchesIdentity(rr, group, routeKey) {
		rr = nil
	}
	if rr == nil {
		var err error
		if rr, err = rt.routeLinkRoute(r.Context(), group, routeKey, sid); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if !routeMatchesIdentity(rr, group, routeKey) {
			rt.evictRoute(group, routeKey, sid)
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		rt.rememberRoute(rr)
	}
	// Not ready (paused / lagging): data-plane traffic wakes it via Reserve (§1.4),
	// then forwards to the resumed node.
	if (rr.State != "ready" || rr.DataEndpoint == "") && rr.Group != "" {
		if res, err := rt.reserveByKey(r.Context(), rr.Group, rr.RouteKey); err == nil && res.DataEndpoint != "" {
			rr = &routeResolve{SID: res.SID, Group: rr.Group, RouteKey: rr.RouteKey, NodeID: res.NodeID, DataEndpoint: res.DataEndpoint, AccessToken: res.AccessToken, State: "ready"}
		}
	}
	if rr.State != "ready" || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not ready", http.StatusServiceUnavailable)
		return
	}
	// Data-plane credential enforcement (cluster-router.md): enforce requires
	// either the sandbox's access token or a valid envd /files signature; log warns
	// on mismatch; off (and unset) skips. Token-auth traffic is re-injected for the
	// node, but signed /files traffic stays headerless so node proxy and envd verify
	// the same URL.
	injectAccessToken := true
	if rt.dataPlaneAuth == "enforce" || rt.dataPlaneAuth == "log" {
		auth := envdsign.CheckDataPlaneAuth(r, port, rr.AccessToken, time.Now())
		if auth.OK {
			injectAccessToken = !auth.Signed
		} else {
			if rt.dataPlaneAuth == "enforce" {
				http.Error(w, "invalid access token", http.StatusUnauthorized)
				return
			}
			rt.log.Warn("router: data-plane auth mismatch (log mode)", "sid", sid, "err", auth.Err)
		}
	}
	rt.forwardSandboxData(w, r, rr, r.Host, sid, injectAccessToken)
}

// forwardSandboxData two-hop forwards a data-plane request to the sandbox's node:
// sandboxHost is the <port>-<sid>.<domain> authority the node proxy resolves from
// (preserved for by-sid; synthesized for by-(group,route-key)). Token-auth traffic
// gets the access token injected; signed /files traffic is forwarded without it.
// CONNECT is tunneled (ReverseProxy can't).
func (rt *Router) forwardSandboxData(w http.ResponseWriter, r *http.Request, rr *routeResolve, sandboxHost, sid string, injectAccessToken bool) {
	r.Host = sandboxHost
	doneActive := rt.beginActiveRoute(rr)
	defer doneActive()
	rt.mx.Inc(`router_requests_total{plane="data"}`)
	if r.Method == http.MethodConnect {
		rt.tunnelData(w, r, rr)
		return
	}
	target := &url.URL{Scheme: "http", Host: rr.DataEndpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = rt.fwdTransport
	tok := rr.AccessToken
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = sandboxHost
		if injectAccessToken {
			req.Header.Set(HeaderAccessTok, tok)
		} else {
			req.Header.Del(HeaderAccessTok)
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		// A cached route that fails is likely stale (sandbox moved/gone): evict it so
		// the next request re-resolves via route_link control (§5 stale → fallback).
		rt.evictRoute(rr.Group, rr.RouteKey, sid)
		rt.mx.Inc(`router_requests_total{plane="data",result="bad_gateway"}`)
		rt.log.Warn("router: data forward", "sid", sid, "node", rr.NodeID, "err", e)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

// serveDataByKey handles by-(group,route-key) data-plane addressing (cluster-router.md):
// business traffic with no prior create. The caller authenticates by api_key (the
// per-sandbox token is router-injected, not caller-held); the router Reserves the
// (group, route_key) sandbox and forwards to it on port E2b-Sandbox-Port.
func (rt *Router) serveDataByKey(w http.ResponseWriter, r *http.Request, group, routeKey string) {
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	rr := rt.cachedRouteByKey(group, routeKey)
	if rr == nil || rr.State != "ready" || rr.DataEndpoint == "" {
		res, err := rt.reserveByKey(r.Context(), group, routeKey)
		if err != nil || res.DataEndpoint == "" {
			http.Error(w, "reserve failed", http.StatusServiceUnavailable)
			return
		}
		rr = &routeResolve{SID: res.SID, Group: group, RouteKey: routeKey, NodeID: res.NodeID, DataEndpoint: res.DataEndpoint, AccessToken: res.AccessToken, State: "ready"}
		rt.rememberRoute(rr)
	}
	port := r.Header.Get("E2b-Sandbox-Port")
	if port == "" {
		port = "49983"
	}
	rt.forwardSandboxData(w, r, rr, port+"-"+rr.SID+"."+rt.domain, rr.SID, true)
}

// tunnelData chains a CONNECT to the node's data endpoint (the node tunnels onward
// to the sandbox), carrying the <port>-<sid>.<domain> authority + access token,
// then splices client <-> node (httputil.ReverseProxy can't tunnel CONNECT).
func (rt *Router) tunnelData(w http.ResponseWriter, r *http.Request, rr *routeResolve) {
	backend, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", rr.DataEndpoint)
	if err != nil {
		http.Error(w, "node unreachable", http.StatusBadGateway)
		return
	}
	hdr := "CONNECT " + r.Host + " HTTP/1.1\r\nHost: " + r.Host + "\r\n" + HeaderAccessTok + ": " + rr.AccessToken + "\r\n\r\n"
	if _, err := io.WriteString(backend, hdr); err != nil {
		backend.Close()
		http.Error(w, "node unreachable", http.StatusBadGateway)
		return
	}
	br := bufio.NewReader(backend)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodConnect})
	if err != nil || resp.StatusCode != http.StatusOK {
		backend.Close()
		code := http.StatusBadGateway
		if err == nil {
			code = resp.StatusCode
		}
		rt.evictRoute(rr.Group, rr.RouteKey, rr.SID)
		http.Error(w, "connect refused by node", code)
		return
	}
	// br may hold tunnel bytes prefetched past the CONNECT response; read through it.
	proxypkg.Tunnel(w, r, &bufConn{Conn: backend, r: br})
}

// bufConn reads from a bufio.Reader (which may hold bytes prefetched past the
// CONNECT response) before the underlying conn.
type bufConn struct {
	net.Conn
	r *bufio.Reader
}

func (c *bufConn) Read(p []byte) (int, error) { return c.r.Read(p) }

func (rt *Router) evictRoute(group, routeKey, sid string) {
	rt.cacheMu.Lock()
	key := routeCacheID(group, routeKey, sid)
	if rr := rt.cache[key]; rr != nil {
		if routeCacheID(rr.Group, rr.RouteKey, rr.SID) == key {
			delete(rt.byKey, routeCacheKey(rr.Group, rr.RouteKey))
		}
		for _, key := range activeRouteKeys(rr) {
			delete(rt.active, key)
		}
	}
	delete(rt.active, activeSIDKey(group, routeKey, sid))
	delete(rt.cache, key)
	rt.cacheMu.Unlock()
}

func routeCacheKey(group, routeKey string) string { return group + "\x00" + routeKey }
func routeCacheID(group, routeKey, sid string) string {
	return group + "\x00" + routeKey + "\x00" + sid
}
func activeSIDKey(group, routeKey, sid string) string {
	return "sid\x00" + routeCacheID(group, routeKey, sid)
}

func activeRouteKeys(rr *routeResolve) []string {
	if rr == nil {
		return nil
	}
	keys := make([]string, 0, 2)
	if rr.Group != "" && rr.RouteKey != "" {
		keys = append(keys, routeCacheKey(rr.Group, rr.RouteKey))
	}
	if rr.Group != "" && rr.RouteKey != "" && rr.SID != "" {
		keys = append(keys, activeSIDKey(rr.Group, rr.RouteKey, rr.SID))
	}
	return keys
}

func (rt *Router) beginActiveRoute(rr *routeResolve) func() {
	keys := activeRouteKeys(rr)
	if len(keys) == 0 {
		return func() {}
	}
	cp := *rr
	rt.cacheMu.Lock()
	for _, key := range keys {
		if cur := rt.active[key]; cur != nil {
			cur.refs++
		} else {
			rt.active[key] = &activeRoute{rr: &cp, refs: 1}
		}
	}
	rt.cacheMu.Unlock()
	return func() {
		rt.cacheMu.Lock()
		defer rt.cacheMu.Unlock()
		for _, key := range keys {
			cur := rt.active[key]
			if cur == nil {
				continue
			}
			cur.refs--
			if cur.refs <= 0 {
				delete(rt.active, key)
			}
		}
	}
}

func (rt *Router) rememberRoute(rr *routeResolve) {
	if rr == nil || rr.Group == "" || rr.RouteKey == "" || rr.SID == "" {
		return
	}
	cp := *rr
	now := time.Now()
	cp.cachedAt = now
	cp.lastUsed = now
	key := routeCacheID(cp.Group, cp.RouteKey, cp.SID)
	rt.cacheMu.Lock()
	rt.cache[key] = &cp
	rt.byKey[routeCacheKey(cp.Group, cp.RouteKey)] = key
	rt.cacheMu.Unlock()
}

func routeBelongsToGroup(rr *routeResolve, group string) bool {
	return rr != nil && group != "" && rr.Group == group
}

func routeMatchesIdentity(rr *routeResolve, group, routeKey string) bool {
	return routeBelongsToGroup(rr, group) && routeKey != "" && rr.RouteKey == routeKey
}

func newRouteKey() string {
	return "rk-" + randomHexID()
}

func randomHexID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

func (rt *Router) cachedRouteByKey(group, routeKey string) *routeResolve {
	key := routeCacheKey(group, routeKey)
	rt.cacheMu.Lock()
	defer rt.cacheMu.Unlock()
	if ar := rt.active[key]; ar != nil && ar.rr != nil {
		cp := *ar.rr
		return &cp
	}
	if cacheKey := rt.byKey[key]; cacheKey != "" {
		return rt.cacheRouteLocked(cacheKey, time.Now())
	}
	return nil
}

// --- control client ---

func (rt *Router) routeLinkReserve(ctx context.Context, group, routeKey string) (*reserveResult, error) {
	path := fmt.Sprintf("%s?group=%s&route_key=%s", registry.RouteLinkReservePath, url.QueryEscape(group), url.QueryEscape(routeKey))
	var res reserveResult
	if err := rt.routeLinkCall(ctx, group, http.MethodPost, path, nil, nil, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) reserveByKey(ctx context.Context, group, routeKey string) (*reserveResult, error) {
	key := routeCacheKey(group, routeKey)
	rt.reserveMu.Lock()
	if f := rt.reserveInFlight[key]; f != nil {
		rt.reserveMu.Unlock()
		select {
		case <-f.done:
			return f.res, f.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f := &reserveFlight{done: make(chan struct{})}
	rt.reserveInFlight[key] = f
	rt.reserveMu.Unlock()

	f.res, f.err = rt.routeLinkReserve(ctx, group, routeKey)
	if f.err == nil && f.res != nil {
		rt.rememberRoute(&routeResolve{
			SID: f.res.SID, Group: group, RouteKey: routeKey, NodeID: f.res.NodeID,
			DataEndpoint: f.res.DataEndpoint, AccessToken: f.res.AccessToken, State: "ready",
		})
	}
	close(f.done)

	rt.reserveMu.Lock()
	delete(rt.reserveInFlight, key)
	rt.reserveMu.Unlock()
	return f.res, f.err
}

func (rt *Router) routeLinkReserveBuild(ctx context.Context, group string, resources *buildResources) (*buildReserveResult, error) {
	reqBody, _ := json.Marshal(map[string]any{
		"group": group, "build_id": "bld-" + randomHexID(), "template_id": "transient-" + randomHexID(), "resources": resources,
	})
	resp, err := rt.routeLinkHTTP(ctx, group, http.MethodPost, registry.RouteLinkReserveBuildPath, reqBody, map[string]string{"Content-Type": "application/json"})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("route_link reserve-build: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var res buildReserveResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) routeLinkResolveBuild(ctx context.Context, group, buildID string) (*buildReserveResult, error) {
	path := fmt.Sprintf("%s?group=%s&build_id=%s", registry.RouteLinkBuildPath, url.QueryEscape(group), url.QueryEscape(buildID))
	var res buildReserveResult
	if err := rt.routeLinkCall(ctx, group, http.MethodGet, path, nil, nil, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) routeLinkRoute(ctx context.Context, group, routeKey, sid string) (*routeResolve, error) {
	path := fmt.Sprintf("%s?group=%s&route_key=%s&sid=%s", registry.RouteLinkRoutePath, url.QueryEscape(group), url.QueryEscape(routeKey), url.QueryEscape(sid))
	var rr routeResolve
	if err := rt.routeLinkCall(ctx, group, http.MethodGet, path, nil, nil, &rr); err != nil {
		return nil, err
	}
	return &rr, nil
}

func (rt *Router) routeLinkCall(ctx context.Context, group, method, path string, body []byte, headers map[string]string, out any) error {
	resp, err := rt.routeLinkHTTP(ctx, group, method, path, body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("route_link %s: %s: %s", path, resp.Status, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (rt *Router) routeLinkHTTP(ctx context.Context, group, method, path string, body []byte, headers map[string]string) (*http.Response, error) {
	refreshed := false
	for {
		resp, err, retry := rt.routeLinkHTTPOnce(ctx, group, method, path, body, headers)
		if !retry || refreshed {
			return resp, err
		}
		if refreshErr := rt.routeRegistry.Refresh(ctx); refreshErr != nil {
			if resp != nil || err != nil {
				return resp, err
			}
			return nil, refreshErr
		}
		if resp != nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512))
			resp.Body.Close()
		}
		refreshed = true
	}
}

func (rt *Router) routeLinkHTTPOnce(ctx context.Context, group, method, path string, body []byte, headers map[string]string) (*http.Response, error, bool) {
	eps, err := rt.routeRegistry.RouteCandidates(ctx, group)
	if err != nil {
		return nil, err, false
	}
	var last error
	for i, ep := range eps {
		req, err := http.NewRequestWithContext(ctx, method, ep.BaseURL+path, bytes.NewReader(body))
		if err != nil {
			return nil, err, false
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := ep.Client.Do(req)
		if err != nil {
			last = err
			continue
		}
		if routeLinkRetryableStatus(resp.StatusCode) {
			if i+1 >= len(eps) {
				return resp, nil, true
			}
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			resp.Body.Close()
			last = fmt.Errorf("route_link %s: %s: %s", ep.MemberID, resp.Status, strings.TrimSpace(string(b)))
			continue
		}
		return resp, nil, false
	}
	if last != nil {
		return nil, last, true
	}
	return nil, fmt.Errorf("router: no registry candidates for group %q", group), false
}

func routeLinkRetryableStatus(status int) bool {
	return status == http.StatusConflict || status >= 500
}

// --- local route cache ---

func (rt *Router) cachedRoute(group, routeKey, sid string) *routeResolve {
	cacheKey := routeCacheID(group, routeKey, sid)
	rt.cacheMu.Lock()
	defer rt.cacheMu.Unlock()
	if ar := rt.active[activeSIDKey(group, routeKey, sid)]; ar != nil && ar.rr != nil {
		cp := *ar.rr
		return &cp
	}
	return rt.cacheRouteLocked(cacheKey, time.Now())
}

func (rt *Router) cacheRouteLocked(cacheKey string, now time.Time) *routeResolve {
	rr := rt.cache[cacheKey]
	if rr == nil {
		return nil
	}
	if rt.routeTTL > 0 && !rr.cachedAt.IsZero() && now.Sub(rr.cachedAt) > rt.routeTTL {
		delete(rt.cache, cacheKey)
		if rt.byKey[routeCacheKey(rr.Group, rr.RouteKey)] == cacheKey {
			delete(rt.byKey, routeCacheKey(rr.Group, rr.RouteKey))
		}
		return nil
	}
	if rt.routeIdleTimeout > 0 && !rr.lastUsed.IsZero() && now.Sub(rr.lastUsed) > rt.routeIdleTimeout {
		delete(rt.cache, cacheKey)
		if rt.byKey[routeCacheKey(rr.Group, rr.RouteKey)] == cacheKey {
			delete(rt.byKey, routeCacheKey(rr.Group, rr.RouteKey))
		}
		return nil
	}
	rr.lastUsed = now
	cp := *rr
	return &cp
}

// verifyAuth checks an api key against a group via the control API, caching a
// valid result for authTTL (cluster-router.md). It returns (ok, err): a
// non-nil err means the control API was unreachable (caller → 503); ok==false
// with nil err means the key was rejected (caller → 403).
func (rt *Router) verifyAuth(ctx context.Context, group, apiKey string) (bool, error) {
	if group == "" || apiKey == "" {
		return false, nil
	}
	k := group + "\x00" + apiKey
	rt.authMu.Lock()
	if exp, ok := rt.authOK[k]; ok && time.Now().Before(exp) {
		rt.authMu.Unlock()
		return true, nil
	}
	rt.authMu.Unlock()

	path := fmt.Sprintf("%s?group=%s", registry.RouteLinkVerifyKeyPath, url.QueryEscape(group))
	resp, err := rt.routeLinkHTTP(ctx, group, http.MethodGet, path, nil, map[string]string{HeaderAPIKey: apiKey})
	if err != nil {
		return false, err // route_link unreachable
	}
	resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		rt.authMu.Lock()
		rt.authOK[k] = time.Now().Add(rt.authTTL)
		rt.authMu.Unlock()
		return true, nil
	case http.StatusForbidden:
		return false, nil // the registry rejected the key
	default:
		return false, fmt.Errorf("route_link verify-key: %s", resp.Status)
	}
}

// authorize verifies (group, api_key) and writes the right error on failure: 503
// when the control API is unreachable (don't 403-storm on a transient blip), 403
// when the key is rejected. Returns false on failure.
func (rt *Router) authorize(w http.ResponseWriter, ctx context.Context, group, apiKey string) bool {
	if rt.authMode == "off" {
		return true // caller auth delegated to a front gateway (§8)
	}
	ok, err := rt.verifyAuth(ctx, group, apiKey)
	if err != nil {
		if rt.authMode == "log" {
			rt.log.Warn("router: caller-auth route_link unreachable (log mode, allowing)", "group", group)
			return true
		}
		http.Error(w, "auth temporarily unavailable", http.StatusServiceUnavailable)
		return false
	}
	if !ok {
		if rt.authMode == "log" {
			rt.log.Warn("router: caller-auth reject (log mode, allowing)", "group", group)
			return true
		}
		rt.mx.Inc(`router_requests_total{result="auth_reject"}`)
		http.Error(w, "invalid api key for group", http.StatusForbidden)
		return false
	}
	return true
}

// RunCleanup periodically evicts expired auth-cache and stale route/build entries
// so local ingress state stays bounded over the process lifetime.
func (rt *Router) RunCleanup(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			rt.authMu.Lock()
			for k, exp := range rt.authOK {
				if now.After(exp) {
					delete(rt.authOK, k)
				}
			}
			rt.authMu.Unlock()
			rt.cacheMu.Lock()
			for key, rr := range rt.cache {
				if rr == nil {
					delete(rt.cache, key)
					continue
				}
				expired := rt.routeTTL > 0 && !rr.cachedAt.IsZero() && now.Sub(rr.cachedAt) > rt.routeTTL
				idle := rt.routeIdleTimeout > 0 && !rr.lastUsed.IsZero() && now.Sub(rr.lastUsed) > rt.routeIdleTimeout
				if expired || idle {
					delete(rt.cache, key)
					if rt.byKey[routeCacheKey(rr.Group, rr.RouteKey)] == key {
						delete(rt.byKey, routeCacheKey(rr.Group, rr.RouteKey))
					}
				}
			}
			rt.cacheMu.Unlock()
			rt.buildsMu.Lock()
			for k, e := range rt.builds {
				if now.Sub(e.at) > buildTTL {
					delete(rt.builds, k)
				}
			}
			rt.buildsMu.Unlock()
		}
	}
}
