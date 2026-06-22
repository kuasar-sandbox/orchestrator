// Package router is the cluster's e2b-compatible unified ingress
// (cluster-router.md): it serves the e2b control plane (api.<domain>) and the
// data plane (<port>-<sid>.<domain>) by Host, reserving sandboxes through the
// registry's control API and forwarding the data plane to the placed node (the
// two-hop path: client -> router -> node data endpoint -> guest).
//
// Phase 3 ships create (-> reserve) + data-plane forward by sid; the remaining
// control verbs (pause/kill/list/get forwarded to the node) and the local route
// cache (hot-path, zero-control-roundtrip) build on this.
package router

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/metrics"
	proxypkg "github.com/kuasar-sandbox/sandbox-orchestrator/internal/proxy"
)

// Headers the cluster ingress reads (cluster.md §1.4).
const (
	HeaderGroup     = "X-Kuasar-Sandbox-Group"
	HeaderRouteKey  = "X-Kuasar-Route-Key"
	HeaderAPIKey    = "X-API-KEY"
	HeaderAccessTok = "X-Access-Token"
)

// reserveResult / routeResolve mirror the registry control JSON (decoupled from the
// registry package).
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
}

// buildEntry maps a build to its node; RunCleanup evicts entries older than
// buildTTL so the build map doesn't grow unboundedly over the ingress's life.
type buildEntry struct {
	node string
	at   time.Time
}

const buildTTL = time.Hour

// Router is the e2b unified ingress. It dials the registry control API (UDS or
// TCP) for reserve + route resolution.
type Router struct {
	domain        string
	authMode      string // off | log | enforce — caller api_key auth (§8); default enforce
	dataPlaneAuth string // off | log | enforce — data-plane access-token check (§7)
	mx            *metrics.M
	controlBase   string       // http base for the control API
	opClient      *http.Client // 60s timeout (reserve / route calls)
	watchClient   *http.Client // no timeout (the long-lived route watch stream)
	log           *slog.Logger

	buildsMu sync.Mutex
	builds   map[string]buildEntry // build_id -> node (a build is node-bound); TTL-evicted

	cacheMu  sync.RWMutex
	cache    map[string]*routeResolve // sid -> resolved data-plane target (local route cache, §5)
	keyToSID map[string]string        // sandbox store key -> sid (for delete eviction)

	authTTL time.Duration
	authMu  sync.Mutex
	authOK  map[string]time.Time // group\x00api_key -> cached-valid-until (§8)

	fwdTransport *http.Transport // pooled transport for node (data/control/build) forwards

	watchRev int64 // last applied watch revision (resume point); only the watch loop touches it
}

// New builds a Router. controlAddr is the registry control endpoint: a path ("/run/...")
// for a unix socket, or host:port for TCP. authTTL caches api-key verification
// (<=0 → 60s).
// controlTLS (non-nil) makes a TCP control endpoint dial over (m)TLS h2; nil = plain. A UDS
// controlAddr ("/...") is always plain (local).
func New(controlAddr, domain string, authTTL time.Duration, controlTLS *tls.Config, log *slog.Logger) *Router {
	if authTTL <= 0 {
		authTTL = 60 * time.Second
	}
	rt := &Router{
		domain: domain, log: log, authTTL: authTTL, authMode: "enforce", mx: metrics.New(),
		builds:   map[string]buildEntry{},
		cache:    map[string]*routeResolve{},
		keyToSID: map[string]string{},
		authOK:   map[string]time.Time{},
	}
	var transport http.RoundTripper
	switch {
	case strings.HasPrefix(controlAddr, "/"):
		rt.controlBase = "http://control"
		transport = &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", controlAddr)
			},
		}
	case controlTLS != nil:
		rt.controlBase = "https://" + controlAddr
		transport = &http.Transport{TLSClientConfig: controlTLS, ForceAttemptHTTP2: true}
	default:
		rt.controlBase = "http://" + controlAddr
		transport = &http.Transport{}
	}
	rt.opClient = &http.Client{Timeout: 60 * time.Second, Transport: transport}
	rt.watchClient = &http.Client{Transport: transport} // no timeout: the watch is long-lived
	// Pooled transport for node forwards: a higher per-host idle cap than the
	// stdlib default of 2 avoids TCP/TLS churn to a busy node at high density.
	rt.fwdTransport = &http.Transport{MaxIdleConns: 512, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second}
	return rt
}

// SetDataPlaneAuth sets the data-plane access-token enforcement mode (off | log |
// enforce); cluster-ctl router sets it from config before serving.
func (rt *Router) SetDataPlaneAuth(mode string) { rt.dataPlaneAuth = mode }

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
	res, err := rt.opReserve(r.Context(), group, routeKey)
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
	res, err := rt.opReserveBuild(r.Context(), group, resources)
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
// holds the build (by build_id), resolving via the registry control on a local miss
// (router restart: the in-memory build map is lost but the BuildStore persists).
func (rt *Router) handleBuildForward(w http.ResponseWriter, r *http.Request) {
	bid := extractBuildID(r.URL.Path)
	rt.buildsMu.Lock()
	e, ok := rt.builds[bid]
	rt.buildsMu.Unlock()
	node := e.node
	if !ok {
		res, rerr := rt.opResolveBuild(r.Context(), bid)
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
// connect/export) to the node holding the sandbox (cluster-router.md §6); kill's
// teardown propagates back as a route delete, converging the registry.
func (rt *Router) handleSandboxVerb(w http.ResponseWriter, r *http.Request) {
	sid := extractSandboxID(r.URL.Path)
	if sid == "" || sid == "import" {
		http.Error(w, "cluster router: unsupported sandbox path", http.StatusNotImplemented)
		return
	}
	rr := rt.resolveRoute(r.Context(), sid)
	if rr == nil || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	// Authenticate the caller against the sandbox's group before forwarding (the
	// node re-authenticates too, but the ingress must not be an open relay / sid
	// oracle — cluster-router.md §6/§8).
	if !rt.authorize(w, r.Context(), rr.Group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	rt.forwardToNode(w, r, rr.DataEndpoint)
}

// handleList returns the group's sandbox shard (cluster-router.md §6: list is
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
	u := fmt.Sprintf("%s/control/list?group=%s", rt.controlBase, url.QueryEscape(group))
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	resp, err := rt.opClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.Copy(w, resp.Body)
}

// resolveRoute returns a sandbox's resolved route via the local cache, falling
// back to the control API; nil if unknown.
func (rt *Router) resolveRoute(ctx context.Context, sid string) *routeResolve {
	if rr := rt.cachedRoute(sid); rr != nil {
		return rr
	}
	if rr, err := rt.opRoute(ctx, sid); err == nil {
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
	// by-(group,route-key): business traffic with no prior create directly triggers
	// a Reserve (router §4), addressed by headers rather than a <port>-<sid> host.
	if g, rk := r.Header.Get(HeaderGroup), r.Header.Get(HeaderRouteKey); g != "" && rk != "" {
		rt.serveDataByKey(w, r, g, rk)
		return
	}
	sub := strings.TrimSuffix(host, "."+rt.domain)
	if sub == host { // not under our domain
		http.Error(w, "unknown host", http.StatusNotFound)
		return
	}
	// sub = <port>-<sid>
	i := strings.IndexByte(sub, '-')
	if i < 0 {
		http.Error(w, "bad data host (want <port>-<sid>.<domain>)", http.StatusBadRequest)
		return
	}
	sid := sub[i+1:]
	// Hot path: serve from the local route cache (zero control round-trip, §5). On a
	// miss (cache lagging / cold), fall back to the registry control.
	rr := rt.cachedRoute(sid)
	if rr == nil {
		var err error
		if rr, err = rt.opRoute(r.Context(), sid); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}
	// Not ready (paused / lagging): data-plane traffic wakes it via Reserve (§1.4),
	// then forwards to the resumed node.
	if (rr.State != "ready" || rr.DataEndpoint == "") && rr.Group != "" {
		if res, err := rt.opReserve(r.Context(), rr.Group, rr.RouteKey); err == nil && res.DataEndpoint != "" {
			rr = &routeResolve{SID: res.SID, Group: rr.Group, RouteKey: rr.RouteKey, NodeID: res.NodeID, DataEndpoint: res.DataEndpoint, AccessToken: res.AccessToken, State: "ready"}
		}
	}
	if rr.State != "ready" || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not ready", http.StatusServiceUnavailable)
		return
	}
	// Data-plane token enforcement (cluster-router.md §8): enforce requires the
	// caller to present the sandbox's access token; log warns on mismatch; off (and
	// unset) skips. The token is (re)injected for the node below regardless.
	if rt.dataPlaneAuth == "enforce" || rt.dataPlaneAuth == "log" {
		if r.Header.Get(HeaderAccessTok) != rr.AccessToken {
			if rt.dataPlaneAuth == "enforce" {
				http.Error(w, "invalid access token", http.StatusUnauthorized)
				return
			}
			rt.log.Warn("router: data-plane token mismatch (log mode)", "sid", sid)
		}
	}
	rt.forwardSandboxData(w, r, rr, r.Host, sid)
}

// forwardSandboxData two-hop forwards a data-plane request to the sandbox's node:
// sandboxHost is the <port>-<sid>.<domain> authority the node proxy resolves from
// (preserved for by-sid; synthesized for by-(group,route-key)). The access token
// is injected; CONNECT is tunneled (ReverseProxy can't).
func (rt *Router) forwardSandboxData(w http.ResponseWriter, r *http.Request, rr *routeResolve, sandboxHost, sid string) {
	r.Host = sandboxHost
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
		req.Header.Set(HeaderAccessTok, tok)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		// A cached route that fails is likely stale (sandbox moved/gone): evict it so
		// the next request re-resolves via the watch / control (§5 stale → fallback).
		rt.evictRoute(sid)
		rt.mx.Inc(`router_requests_total{plane="data",result="bad_gateway"}`)
		rt.log.Warn("router: data forward", "sid", sid, "node", rr.NodeID, "err", e)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

// serveDataByKey handles by-(group,route-key) data-plane addressing (router §4):
// business traffic with no prior create. The caller authenticates by api_key (the
// per-sandbox token is router-injected, not caller-held); the router Reserves the
// (group, route_key) sandbox and forwards to it on port E2b-Sandbox-Port.
func (rt *Router) serveDataByKey(w http.ResponseWriter, r *http.Request, group, routeKey string) {
	if !rt.authorize(w, r.Context(), group, r.Header.Get(HeaderAPIKey)) {
		return
	}
	res, err := rt.opReserve(r.Context(), group, routeKey)
	if err != nil || res.DataEndpoint == "" {
		http.Error(w, "reserve failed", http.StatusServiceUnavailable)
		return
	}
	port := r.Header.Get("E2b-Sandbox-Port")
	if port == "" {
		port = "49983"
	}
	rr := &routeResolve{SID: res.SID, Group: group, RouteKey: routeKey, NodeID: res.NodeID, DataEndpoint: res.DataEndpoint, AccessToken: res.AccessToken, State: "ready"}
	rt.forwardSandboxData(w, r, rr, port+"-"+res.SID+"."+rt.domain, res.SID)
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
		rt.evictRoute(rr.SID)
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

func (rt *Router) evictRoute(sid string) {
	rt.cacheMu.Lock()
	delete(rt.cache, sid)
	rt.cacheMu.Unlock()
}

// --- control client ---

func (rt *Router) opReserve(ctx context.Context, group, routeKey string) (*reserveResult, error) {
	u := fmt.Sprintf("%s/control/reserve?group=%s&route_key=%s", rt.controlBase, url.QueryEscape(group), url.QueryEscape(routeKey))
	var res reserveResult
	if err := rt.opCall(ctx, http.MethodPost, u, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) opReserveBuild(ctx context.Context, group string, resources *buildResources) (*buildReserveResult, error) {
	reqBody, _ := json.Marshal(map[string]any{"group": group, "resources": resources})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rt.controlBase+ControlReserveBuildPath, bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := rt.opClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("control reserve-build: %s: %s", resp.Status, strings.TrimSpace(string(b)))
	}
	var res buildReserveResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) opResolveBuild(ctx context.Context, buildID string) (*buildReserveResult, error) {
	u := fmt.Sprintf("%s/control/build?build_id=%s", rt.controlBase, url.QueryEscape(buildID))
	var res buildReserveResult
	if err := rt.opCall(ctx, http.MethodGet, u, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// ControlReserveBuildPath mirrors the registry control path (avoids importing the registry).
const ControlReserveBuildPath = "/control/reserve-build"

func (rt *Router) opRoute(ctx context.Context, sid string) (*routeResolve, error) {
	u := fmt.Sprintf("%s/control/route?sid=%s", rt.controlBase, url.QueryEscape(sid))
	var rr routeResolve
	if err := rt.opCall(ctx, http.MethodGet, u, &rr); err != nil {
		return nil, err
	}
	return &rr, nil
}

func (rt *Router) opCall(ctx context.Context, method, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return err
	}
	resp, err := rt.opClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("control %s: %s: %s", u, resp.Status, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// --- local route cache (control watch) ---

// watchEvent mirrors registry.WatchEvent (decoupled from the registry package).
type watchEvent struct {
	Type  string        `json:"type"` // "put" | "delete" | "bookmark"
	Key   string        `json:"key,omitempty"`
	Route *routeResolve `json:"route,omitempty"`
	Rev   int64         `json:"rev,omitempty"`
}

func (rt *Router) cachedRoute(sid string) *routeResolve {
	rt.cacheMu.RLock()
	defer rt.cacheMu.RUnlock()
	return rt.cache[sid]
}

// RunWatch keeps the local route cache synced from the registry's control watch
// (cluster.md §5): a snapshot then live deltas, reconnecting with capped backoff.
// The data plane serves from the cache (zero control round-trip); a miss falls back to
// the control API. cluster-ctl router runs this in the background.
func (rt *Router) RunWatch(ctx context.Context) {
	backoff := 200 * time.Millisecond
	for ctx.Err() == nil {
		err := rt.watchOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		rt.log.Warn("router: route watch ended; reconnecting", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

func (rt *Router) watchOnce(ctx context.Context) error {
	u := fmt.Sprintf("%s/control/watch?from_rev=%d", rt.controlBase, rt.watchRev)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	resp, err := rt.watchClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("control watch: %s", resp.Status)
	}
	// A "reset" frame starts a full snapshot (first connect, or a compacted resume):
	// accumulate into a shadow map and swap it in atomically at the bookmark, so the
	// data plane never sees a half-built set. A resume (no reset) applies deltas to
	// the live cache, keeping it. watchRev only advances once live (post-bookmark),
	// so a snapshot interrupted before its bookmark re-snapshots on reconnect.
	var snap map[string]*routeResolve
	var snapKey map[string]string
	syncing := false
	for {
		ev, err := readWatchFrame(resp.Body)
		if err != nil {
			return err
		}
		switch ev.Type {
		case "reset":
			snap = map[string]*routeResolve{}
			snapKey = map[string]string{}
			syncing = true
		case "put":
			if ev.Route != nil {
				if syncing {
					snap[ev.Route.SID] = ev.Route
					snapKey[ev.Key] = ev.Route.SID
				} else {
					rt.cacheMu.Lock()
					rt.cache[ev.Route.SID] = ev.Route
					rt.keyToSID[ev.Key] = ev.Route.SID
					rt.cacheMu.Unlock()
				}
			}
		case "delete":
			if !syncing {
				rt.cacheMu.Lock()
				if sid := rt.keyToSID[ev.Key]; sid != "" {
					delete(rt.cache, sid)
				}
				delete(rt.keyToSID, ev.Key)
				rt.cacheMu.Unlock()
			}
		case "bookmark":
			if syncing {
				rt.cacheMu.Lock()
				rt.cache, rt.keyToSID = snap, snapKey
				rt.cacheMu.Unlock()
				syncing = false
			}
		}
		// Advance the resume point only for frames applied to the live cache (the
		// bookmark above flips syncing off first, so its rev is recorded too).
		if !syncing && ev.Rev > rt.watchRev {
			rt.watchRev = ev.Rev
		}
	}
}

// verifyAuth checks an api key against a group via the control API, caching a
// valid result for authTTL (cluster-router.md §8). It returns (ok, err): a
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

	u := fmt.Sprintf("%s/control/verify-key?group=%s", rt.controlBase, url.QueryEscape(group))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set(HeaderAPIKey, apiKey) // api key in a header, never the query string (logged)
	resp, err := rt.opClient.Do(req)
	if err != nil {
		return false, err // control unreachable
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
		return false, fmt.Errorf("control verify-key: %s", resp.Status)
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
			rt.log.Warn("router: caller-auth control unreachable (log mode, allowing)", "group", group)
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

// RunCleanup periodically evicts expired auth-cache and stale build-map entries so
// neither grows unboundedly over the ingress's lifetime; cluster-ctl router runs it.
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

func readWatchFrame(r io.Reader) (*watchEvent, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(hdr[:])
	if n == 0 || n > 1<<20 {
		return nil, fmt.Errorf("router: bad watch frame length %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var ev watchEvent
	if err := json.Unmarshal(buf, &ev); err != nil {
		return nil, err
	}
	return &ev, nil
}
