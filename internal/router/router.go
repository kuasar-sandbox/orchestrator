// Package router is the cluster's e2b-compatible unified ingress
// (cluster-router.md): it serves the e2b control plane (api.<domain>) and the
// data plane (<port>-<sid>.<domain>) by Host, reserving sandboxes through the
// registry's op interface and forwarding the data plane to the placed node (the
// two-hop path: client -> router -> node data endpoint -> guest).
//
// Phase 3 ships create (-> reserve) + data-plane forward by sid; the remaining
// control verbs (pause/kill/list/get forwarded to the node) and the local route
// cache (hot-path, zero-op-roundtrip) build on this.
package router

import (
	"bytes"
	"context"
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
)

// Headers the cluster ingress reads (cluster.md §1.4).
const (
	HeaderGroup     = "X-Kuasar-Sandbox-Group"
	HeaderRouteKey  = "X-Kuasar-Route-Key"
	HeaderAPIKey    = "X-API-KEY"
	HeaderAccessTok = "X-Access-Token"
)

// reserveResult / routeResolve mirror the registry op JSON (decoupled from the
// registry package).
type reserveResult struct {
	NodeID       string `json:"node_id"`
	SID          string `json:"sid"`
	AccessToken  string `json:"access_token"`
	DataEndpoint string `json:"data_endpoint"`
}
type routeResolve struct {
	SID          string `json:"sid"`
	NodeID       string `json:"node_id"`
	DataEndpoint string `json:"data_endpoint"`
	AccessToken  string `json:"access_token"`
	State        string `json:"state"`
}

// Router is the e2b unified ingress. It dials the registry op interface (UDS or
// TCP) for reserve + route resolution.
type Router struct {
	domain   string
	opBase   string // http base for the op interface
	opClient *http.Client
	log      *slog.Logger

	buildsMu sync.Mutex
	builds   map[string]string // build_id -> node data endpoint (a build is node-bound)
}

// New builds a Router. opAddr is the registry op endpoint: a path ("/run/...")
// for a unix socket, or host:port for TCP.
func New(opAddr, domain string, log *slog.Logger) *Router {
	rt := &Router{domain: domain, log: log, builds: map[string]string{}}
	if strings.HasPrefix(opAddr, "/") {
		rt.opBase = "http://op"
		rt.opClient = &http.Client{Timeout: 60 * time.Second, Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", opAddr)
			},
		}}
	} else {
		rt.opBase = "http://" + opAddr
		rt.opClient = &http.Client{Timeout: 60 * time.Second}
	}
	return rt
}

// Handler routes by Host: api.<domain> (and any api.* host) -> control plane;
// everything else -> data plane (<port>-<sid>.<domain>).
func (rt *Router) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if i := strings.IndexByte(host, ':'); i >= 0 {
			host = host[:i]
		}
		if host == "api."+rt.domain || strings.HasPrefix(host, "api.") {
			rt.serveControl(w, r)
			return
		}
		rt.serveData(w, r, host)
	})
}

// --- control plane ---

func (rt *Router) serveControl(w http.ResponseWriter, r *http.Request) {
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
	default:
		http.Error(w, "cluster router: control verb not implemented (Phase 7 adds pause/kill/list/get)", http.StatusNotImplemented)
	}
}

func (rt *Router) handleCreate(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
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

// handleBuildRegister reserves a build node for the group, forwards the register
// to it, and records build_id -> node so the follow-up build calls route there.
func (rt *Router) handleBuildRegister(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	res, err := rt.opReserveBuild(r.Context(), group)
	if err != nil {
		rt.log.Warn("router: reserve-build", "group", group, "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	rt.forwardBuild(w, r, res.DataEndpoint, true)
}

// handleBuildForward routes a build trigger/status/files call to the node that
// holds the build (by build_id from the path).
func (rt *Router) handleBuildForward(w http.ResponseWriter, r *http.Request) {
	bid := extractBuildID(r.URL.Path)
	rt.buildsMu.Lock()
	node := rt.builds[bid]
	rt.buildsMu.Unlock()
	if node == "" {
		http.Error(w, "unknown build "+bid, http.StatusNotFound)
		return
	}
	rt.forwardBuild(w, r, node, false)
}

// forwardBuild proxies a build control call to a node's e2b control plane (Host
// api.<domain>; the client's X-API-KEY passes through for the node's build auth).
// When capture is set it records build_id -> node from the register reply.
func (rt *Router) forwardBuild(w http.ResponseWriter, r *http.Request, dataEndpoint string, capture bool) {
	target := &url.URL{Scheme: "http", Host: dataEndpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	apiHost := "api." + rt.domain
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = dataEndpoint
		req.Host = apiHost
	}
	if capture {
		proxy.ModifyResponse = func(resp *http.Response) error {
			body, _ := io.ReadAll(resp.Body)
			resp.Body = io.NopCloser(bytes.NewReader(body))
			var reg struct {
				BuildID string `json:"buildID"`
			}
			if json.Unmarshal(body, &reg) == nil && reg.BuildID != "" {
				rt.buildsMu.Lock()
				rt.builds[reg.BuildID] = dataEndpoint
				rt.buildsMu.Unlock()
			}
			return nil
		}
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

// --- data plane ---

func (rt *Router) serveData(w http.ResponseWriter, r *http.Request, host string) {
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
	rr, err := rt.opRoute(r.Context(), sid)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	if rr.State != "ready" || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not ready", http.StatusServiceUnavailable)
		return
	}
	// Two-hop forward: preserve the <port>-<sid>.<domain> Host (the node proxy
	// resolves the sandbox from it) and inject the access token.
	target := &url.URL{Scheme: "http", Host: rr.DataEndpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	origHost := r.Host
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host
		req.Host = origHost
		req.Header.Set(HeaderAccessTok, rr.AccessToken)
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		rt.log.Warn("router: data forward", "sid", sid, "node", rr.NodeID, "err", e)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, r)
}

// --- op client ---

func (rt *Router) opReserve(ctx context.Context, group, routeKey string) (*reserveResult, error) {
	u := fmt.Sprintf("%s/op/reserve?group=%s&route_key=%s", rt.opBase, url.QueryEscape(group), url.QueryEscape(routeKey))
	var res reserveResult
	if err := rt.opCall(ctx, http.MethodPost, u, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) opReserveBuild(ctx context.Context, group string) (*reserveResult, error) {
	u := fmt.Sprintf("%s/op/reserve-build?group=%s", rt.opBase, url.QueryEscape(group))
	var res reserveResult
	if err := rt.opCall(ctx, http.MethodPost, u, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) opRoute(ctx context.Context, sid string) (*routeResolve, error) {
	u := fmt.Sprintf("%s/op/route?sid=%s", rt.opBase, url.QueryEscape(sid))
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
		return fmt.Errorf("op %s: %s: %s", u, resp.Status, strings.TrimSpace(string(b)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
