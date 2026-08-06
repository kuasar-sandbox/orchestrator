// Package router is the cluster's e2b-compatible unified ingress
// (cluster-router.md): it serves the e2b control plane (api.<domain>) and the
// data plane (<port>-<sid>.<domain>) by Host, reserving sandboxes through the
// registry's control API and forwarding the data plane to the target node (the
// two-hop path: client -> router -> node data endpoint -> guest).
package router

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/orchestrator/internal/execsession"
	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Headers the cluster ingress reads (cluster.md).
const (
	HeaderGroup       = "X-Kuasar-Sandbox-Group"
	HeaderRouteKey    = "X-Kuasar-Route-Key"
	HeaderRestore     = "X-Kuasar-Sandbox-Restore"
	HeaderCredentials = "X-Kuasar-Sandbox-Credentials"
	HeaderCheckpoint  = "X-Kuasar-Sandbox-Checkpoint"
	HeaderMMDS        = "X-Kuasar-Sandbox-MMDS"
	HeaderAPIKey      = "X-API-KEY"
	HeaderAccessTok   = "X-Access-Token"
	HeaderMigration   = "X-Kuasar-Migration-Token"

	maxClusterCreateBodyBytes  int64 = 16 << 20
	maxClusterConnectBodyBytes int64 = 64 << 10
)

// reserveResult / routeResolve mirror registry route_link JSON.
type reserveResult struct {
	Route       routeResolve       `json:"route"`
	Connect     *connectResult     `json:"connect,omitempty"`
	ExecSession *execSessionResult `json:"exec_session,omitempty"`
}

type connectResult struct {
	NodeSandboxID      string `json:"node_sandbox_id"`
	TemplateID         string `json:"template_id"`
	Profile            string `json:"profile"`
	EnvdAccessToken    string `json:"envd_access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken string `json:"forward_access_token"`
}

type execSessionResult struct {
	ExecAccessToken string `json:"exec_access_token"`
}
type routeResolve struct {
	SandboxID              string `json:"sandbox_id"`
	NodeSandboxID          string `json:"node_sandbox_id"`
	RouteRevision          int64  `json:"route_revision"`
	Group                  string `json:"group"`
	RouteKey               string `json:"route_key"`
	NodeID                 string `json:"node_id"`
	DataEndpoint           string `json:"data_endpoint"`
	Profile                string `json:"profile"`
	TemplateID             string `json:"template_id"`
	AuthSandboxID          string `json:"auth_sandbox_id"`
	APISecret              string `json:"api_secret"`
	APISecretFingerprint   string `json:"api_secret_fingerprint"`
	ManifestKeyFingerprint string `json:"manifest_key_fingerprint"`
	ServiceSecret          string `json:"service_secret"`
	EnvdAccessToken        string `json:"envd_access_token,omitempty"`
	TrafficAccessToken     string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken     string `json:"forward_access_token"`
	TargetPort             int    `json:"target_port,omitempty"`
	State                  string `json:"state"`
	cachedAt               time.Time
	lastUsed               time.Time
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
	dataPlaneAuth string // off | log | enforce — data-plane access-token check (§7)
	mx            *metrics.M
	routeLinkBase string       // static base used by New in tests / size-1 local mode
	routeClient   *http.Client // static client used by New in tests / size-1 local mode
	routeRegistry routeRegistry
	log           *slog.Logger

	buildsMu sync.Mutex
	builds   map[string]buildEntry // group\x00build_id -> node; TTL-evicted

	cacheMu sync.RWMutex
	cache   map[string]*routeResolve // group\x00route_key\x00stable_sid -> current data-plane target
	active  map[string]*activeRoute  // in-flight bookkeeping only; never resolves a new request

	authTTL time.Duration
	authMu  sync.Mutex
	authOK  map[string]time.Time // group\x00api_key -> cached-valid-until (§8)

	routeTTL         time.Duration
	routeIdleTimeout time.Duration

	fwdTransport *http.Transport // pooled transport for node control/build forwards

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
		domain: domain, log: log, authTTL: authTTL, mx: metrics.New(),
		builds:           map[string]buildEntry{},
		cache:            map[string]*routeResolve{},
		active:           map[string]*activeRoute{},
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
// values disable that particular age check. In-flight requests retain their own
// route copy and never depend on the cache entry after forwarding starts.
func (rt *Router) SetRouteCache(routeTTL, idleTimeout time.Duration) {
	rt.cacheMu.Lock()
	defer rt.cacheMu.Unlock()
	rt.routeTTL = routeTTL
	rt.routeIdleTimeout = idleTimeout
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
		if r.Header.Get(proxypkg.HeaderSandboxService) == string(proxypkg.ConnectServiceExec) {
			if r.Method != http.MethodConnect {
				w.Header().Set("Allow", http.MethodConnect)
				http.Error(w, "exec requires CONNECT", http.StatusMethodNotAllowed)
				return
			}
			rt.serveExecData(w, r)
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
	case r.Method == http.MethodGet && path == "/health":
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && isSandboxCollectionPath(path):
		// e2b create: reserve by (group, route_key).
		rt.handleCreate(w, r)
	case r.Method == http.MethodPost && isTemplateCollectionPath(path):
		// e2b build register: reserve a build node, forward, record build_id -> node.
		rt.handleBuildRegister(w, r)
	case strings.Contains(path, "/builds/"):
		// build trigger / status / files: route by build_id to the recorded node.
		rt.handleBuildForward(w, r)
	case r.Method == http.MethodGet && isSandboxCollectionPath(path):
		// list: the group's sandbox shard (no cross-group).
		rt.handleList(w, r)
	case r.Method == http.MethodPost && isSandboxConnectPath(path):
		// connect is completed by operation-aware Reserve: the registry prepares
		// the exact node-local target synchronously and the node resumes it
		// asynchronously.
		rt.handleConnect(w, r)
	case isSandboxExecSessionPath(path):
		// Exec capability issuance is a Registry operation over the current
		// node-local instance; the public request is never forwarded to a node.
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		rt.handleExecSession(w, r)
	case isSandboxVerbPath(path):
		// get / kill / pause / timeout / export: forward to the node by sid.
		rt.handleSandboxVerb(w, r)
	default:
		http.Error(w, "cluster router: control verb not supported", http.StatusNotImplemented)
	}
}

func isSandboxCollectionPath(path string) bool {
	return path == "/sandboxes" || path == "/v2/sandboxes"
}

func isTemplateCollectionPath(path string) bool {
	return path == "/templates" || path == "/v3/templates"
}

func isSandboxVerbPath(path string) bool {
	return strings.HasPrefix(path, "/sandboxes/") || strings.HasPrefix(path, "/v2/sandboxes/")
}

func isSandboxConnectPath(path string) bool {
	if !strings.HasSuffix(path, "/connect") {
		return false
	}
	sid := extractSandboxID(path)
	return sid != "" && sid != "import" && path == sandboxPathPrefix(path)+sid+"/connect"
}

func isSandboxExecSessionPath(path string) bool {
	if !strings.HasPrefix(path, "/sandboxes/") || !strings.HasSuffix(path, "/exec-sessions") {
		return false
	}
	sid := extractSandboxID(path)
	return sid != "" && sid != "import" && path == "/sandboxes/"+sid+"/exec-sessions"
}

func sandboxPathPrefix(path string) string {
	if strings.HasPrefix(path, "/v2/sandboxes/") {
		return "/v2/sandboxes/"
	}
	if strings.HasPrefix(path, "/sandboxes/") {
		return "/sandboxes/"
	}
	return ""
}

func (rt *Router) handleCreate(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	routeKey := r.Header.Get(HeaderRouteKey)
	if routeKey == "" {
		routeKey = newRouteKey()
	}
	createConfig, err := createSandboxMetadata(w, r)
	if err != nil {
		status := http.StatusBadRequest
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			status = http.StatusRequestEntityTooLarge
		}
		http.Error(w, err.Error(), status)
		return
	}
	res, err := rt.routeLinkReserve(
		r.Context(), "create", group, routeKey, "", 0, 0, 0, createConfig,
		map[string]string{HeaderAPIKey: apiKeyFromRequest(r)},
	)
	if err != nil {
		rt.log.Warn("router: reserve", "group", group, "err", err)
		writeRouteLinkError(w, err, http.StatusServiceUnavailable)
		return
	}
	route := &res.Route
	if route.Group != group || route.RouteKey != routeKey || route.SandboxID == "" || route.NodeSandboxID == "" {
		http.Error(w, "registry returned an invalid sandbox route", http.StatusBadGateway)
		return
	}
	rt.rememberRoute(route)
	// Public create responses expose only the profile-specific sandbox tokens.
	// Tenant and sandbox credential roots remain inside the protected route link.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	out := map[string]any{
		"sandboxID":          route.SandboxID,
		"routeKey":           routeKey,
		"clientID":           route.NodeID,
		"forwardAccessToken": route.ForwardAccessToken,
		"domain":             rt.domain,
	}
	if route.Profile == string(types.ProfileE2B) {
		out["envdAccessToken"] = route.EnvdAccessToken
		out["trafficAccessToken"] = route.TrafficAccessToken
	}
	_ = json.NewEncoder(w).Encode(out)
}

// createSandboxMetadata selects request-scoped restore, credential, checkpoint,
// and mmds specification objects from the cluster create request. Restore,
// credential, and mmds headers replace their whole body objects. Checkpoint is
// overlaid fieldwise. The body is still decoded when present so malformed
// lower-priority metadata cannot bypass the shared strict parser.
func createSandboxMetadata(w http.ResponseWriter, r *http.Request) (map[string]string, error) {
	restoreRaw, restoreHeader := createHeaderValue(r.Header, HeaderRestore)
	credentialsRaw, credentialsHeader := createHeaderValue(r.Header, HeaderCredentials)
	checkpointRaw, checkpointHeader := createHeaderValue(r.Header, HeaderCheckpoint)
	mmdsRaw, mmdsHeader := createHeaderValue(r.Header, HeaderMMDS)
	checkpointHeaderPolicy := sandboxcfg.CheckpointPolicy{}
	if checkpointHeader {
		var err error
		checkpointHeaderPolicy, err = sandboxcfg.ParseCheckpointPolicyJSON(checkpointRaw)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", HeaderCheckpoint, err)
		}
	}
	var body struct {
		Metadata map[string]string `json:"metadata"`
	}
	if r.Body != nil {
		if r.ContentLength > maxClusterCreateBodyBytes {
			return nil, &http.MaxBytesError{Limit: maxClusterCreateBodyBytes}
		}
		r.Body = http.MaxBytesReader(w, r.Body, maxClusterCreateBodyBytes)
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("bad create body: %w", err)
		}
		// Decode stops after the first complete JSON value. Drain the bounded
		// reader so trailing bytes count toward the advertised body limit while
		// preserving the endpoint's existing single-value decode semantics.
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			return nil, fmt.Errorf("bad create body: %w", err)
		}
	}
	selected := map[string]string{}
	if raw, ok := body.Metadata[sandboxcfg.NsRestore]; ok {
		selected[sandboxcfg.NsRestore] = raw
	}
	if raw, ok := body.Metadata[sandboxcfg.NsCredentials]; ok {
		selected[sandboxcfg.NsCredentials] = raw
	}
	if raw, ok := body.Metadata[sandboxcfg.NsCheckpoint]; ok {
		selected[sandboxcfg.NsCheckpoint] = raw
	}
	if raw, ok := body.Metadata[sandboxcfg.NsMMDS]; ok {
		selected[sandboxcfg.NsMMDS] = raw
	}
	if restoreHeader {
		selected[sandboxcfg.NsRestore] = restoreRaw
	}
	if credentialsHeader {
		selected[sandboxcfg.NsCredentials] = credentialsRaw
	}
	if checkpointHeader {
		bodyPolicy := sandboxcfg.CheckpointPolicy{}
		if raw, ok := selected[sandboxcfg.NsCheckpoint]; ok {
			var err error
			bodyPolicy, err = sandboxcfg.ParseCheckpointPolicyJSON(raw)
			if err != nil {
				return nil, fmt.Errorf("metadata %s: %w", sandboxcfg.NsCheckpoint, err)
			}
		}
		policy := sandboxcfg.OverlayCheckpointPolicy(bodyPolicy, checkpointHeaderPolicy)
		if policy.Empty() {
			delete(selected, sandboxcfg.NsCheckpoint)
		} else {
			canonical, err := sandboxcfg.MarshalCheckpointPolicyJSON(policy)
			if err != nil {
				return nil, err
			}
			selected[sandboxcfg.NsCheckpoint] = canonical
		}
	}
	if mmdsHeader {
		selected[sandboxcfg.NsMMDS] = mmdsRaw
	}
	if len(selected) == 0 {
		return nil, nil
	}
	selected, err := sandboxcfg.NormalizeRestoreMetadata(selected)
	if err != nil {
		return nil, err
	}
	selected, err = sandboxcfg.NormalizeCheckpointMetadata(selected)
	if err != nil {
		return nil, err
	}
	credentials, cleaned, err := sandboxcfg.ExtractCredentials(selected)
	if err != nil {
		return nil, err
	}
	if _, present := selected[sandboxcfg.NsCredentials]; present {
		canonical, err := json.Marshal(credentials)
		if err != nil {
			return nil, fmt.Errorf("encode credentials: %w", err)
		}
		if cleaned == nil {
			cleaned = map[string]string{}
		}
		cleaned[sandboxcfg.NsCredentials] = string(canonical)
	}
	return cleaned, nil
}

func createHeaderValue(header http.Header, name string) (string, bool) {
	_, present := header[http.CanonicalHeaderKey(name)]
	return header.Get(name), present
}

// --- build control plane ---

// buildReserveResult mirrors registry.BuildReserveResult (the registry assigns the
// build/template ids + places the build, §7.5).
type buildReserveResult struct {
	BuildID      string        `json:"build_id"`
	TemplateID   string        `json:"template_id"`
	NodeID       string        `json:"node_id"`
	DataEndpoint string        `json:"data_endpoint"`
	Profile      types.Profile `json:"profile"`
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
	if !rt.authorize(w, r.Context(), group, apiKeyFromRequest(r)) {
		return
	}
	var body struct {
		Name     string   `json:"name"`
		Tags     []string `json:"tags"`
		Profile  string   `json:"profile"`
		CPUCount int      `json:"cpuCount"`
		MemoryMB int      `json:"memoryMB"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body)
	profile := types.ProfileE2B
	if body.Profile != "" {
		var err error
		profile, err = types.ParseProfile(body.Profile)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	var resources *buildResources
	if body.CPUCount > 0 || body.MemoryMB > 0 {
		resources = &buildResources{CPU: body.CPUCount * 1000, Mem: int64(body.MemoryMB) << 20}
	}
	res, err := rt.routeLinkReserveBuild(r.Context(), group, profile, resources)
	if err != nil {
		rt.log.Warn("router: reserve-build", "group", group, "err", err)
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	rt.buildsMu.Lock()
	rt.builds[buildCacheKey(group, res.BuildID)] = buildEntry{node: res.DataEndpoint, at: time.Now()}
	rt.buildsMu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"templateID": res.TemplateID, "buildID": res.BuildID,
		"public": false, "names": nonEmptySlice(body.Name), "tags": body.Tags, "aliases": body.Tags,
		"profile": res.Profile,
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
	if !rt.authorize(w, r.Context(), group, apiKeyFromRequest(r)) {
		return
	}
	bid := extractBuildID(r.URL.Path)
	cacheKey := buildCacheKey(group, bid)
	rt.buildsMu.Lock()
	e, ok := rt.builds[cacheKey]
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
		rt.builds[cacheKey] = buildEntry{node: node, at: time.Now()}
		rt.buildsMu.Unlock()
	}
	rt.forwardBuild(w, r, node)
}

func buildCacheKey(group, buildID string) string {
	return group + "\x00" + buildID
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

func (rt *Router) handleConnect(w http.ResponseWriter, r *http.Request) {
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
	sandboxID := extractSandboxID(r.URL.Path)
	if sandboxID == "" || sandboxID == "import" {
		http.Error(w, "cluster router: unsupported sandbox path", http.StatusNotImplemented)
		return
	}
	migrationToken := r.Header.Get(HeaderMigration)
	if len(migrationToken) > migrationtoken.MaxWireSize {
		http.Error(w, migrationtoken.ErrTokenTooLarge.Error(), http.StatusRequestHeaderFieldsTooLarge)
		return
	}
	var body struct {
		Timeout int `json:"timeout"`
	}
	if r.ContentLength > maxClusterConnectBodyBytes {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	if r.Body != nil {
		r.Body = http.MaxBytesReader(w, r.Body, maxClusterConnectBodyBytes)
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&body); err != nil && !errors.Is(err, io.EOF) {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "invalid request body", http.StatusBadRequest)
			}
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			} else {
				http.Error(w, "invalid request body", http.StatusBadRequest)
			}
			return
		}
	}
	headers := map[string]string{HeaderAPIKey: apiKeyFromRequest(r)}
	if migrationToken != "" {
		headers[HeaderMigration] = migrationToken
	}
	// Connect never carries config: unlike create, it never allocates a new
	// sandbox. On a cross-node reconnect (import via migration token), the
	// token's own carried kuasar-sandbox.mmds is what survives -- see
	// importSandboxWithKey's cluster branch -- there is no redeclaration
	// mechanism to override it on this request.
	res, err := rt.routeLinkReserve(
		r.Context(), "connect", group, routeKey, sandboxID, 0, body.Timeout, 0, nil, headers,
	)
	if err != nil {
		writeRouteLinkError(w, err, http.StatusServiceUnavailable)
		return
	}
	route := &res.Route
	connected := res.Connect
	if !routeMatchesIdentity(route, group, routeKey, sandboxID) || connected == nil ||
		connected.NodeSandboxID == "" || connected.NodeSandboxID != route.NodeSandboxID ||
		connected.TemplateID == "" || connected.TemplateID != route.TemplateID ||
		connected.Profile == "" || connected.Profile != route.Profile ||
		connected.EnvdAccessToken != route.EnvdAccessToken ||
		connected.TrafficAccessToken != route.TrafficAccessToken ||
		connected.ForwardAccessToken != route.ForwardAccessToken {
		http.Error(w, "registry returned an invalid connect result", http.StatusBadGateway)
		return
	}
	rt.rememberRoute(route)
	out := map[string]any{
		"sandboxID":          sandboxID,
		"templateID":         connected.TemplateID,
		"clientID":           "orchestrator",
		"domain":             rt.domain,
		"envdVersion":        "0.1.0",
		"alias":              "",
		"forwardAccessToken": connected.ForwardAccessToken,
	}
	if connected.Profile == string(types.ProfileE2B) {
		out["envdVersion"] = "0.6.1"
		out["envdAccessToken"] = connected.EnvdAccessToken
		out["trafficAccessToken"] = connected.TrafficAccessToken
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (rt *Router) handleExecSession(w http.ResponseWriter, r *http.Request) {
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
	sandboxID := extractSandboxID(r.URL.Path)
	if sandboxID == "" || sandboxID == "import" {
		http.Error(w, "cluster router: unsupported sandbox path", http.StatusNotImplemented)
		return
	}
	apiKey := r.Header.Get(HeaderAPIKey)
	if apiKey == "" {
		http.Error(w, HeaderAPIKey+" required", http.StatusUnauthorized)
		return
	}
	migrationToken := r.Header.Get(HeaderMigration)
	if len(migrationToken) > migrationtoken.MaxWireSize {
		http.Error(w, migrationtoken.ErrTokenTooLarge.Error(), http.StatusRequestHeaderFieldsTooLarge)
		return
	}
	request, err := execsession.DecodeRequest(r.Body, r.ContentLength)
	if err != nil {
		if errors.Is(err, execsession.ErrRequestTooLarge) {
			http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, execsession.ErrInvalidRequest.Error(), http.StatusBadRequest)
		return
	}
	headers := map[string]string{HeaderAPIKey: apiKey}
	if migrationToken != "" {
		headers[HeaderMigration] = migrationToken
	}
	// See handleConnect: exec-session never carries config either.
	res, err := rt.routeLinkReserve(
		r.Context(), "exec-session", group, routeKey, sandboxID, 0, 0, request.TTLSeconds, nil, headers,
	)
	if err != nil {
		writeExecSessionRouteLinkError(w, err)
		return
	}
	route := &res.Route
	result := res.ExecSession
	if !routeMatchesIdentity(route, group, routeKey, sandboxID) || result == nil || result.ExecAccessToken == "" {
		http.Error(w, "exec session unavailable", http.StatusServiceUnavailable)
		return
	}
	rt.rememberRoute(route)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(struct {
		ExecAccessToken string `json:"execAccessToken"`
	}{ExecAccessToken: result.ExecAccessToken})
}

// handleSandboxVerb forwards a sid-scoped control verb (get/kill/pause/timeout/
// export) to the node holding the sandbox (cluster-router.md); kill's
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
	if !rt.authorize(w, r.Context(), group, apiKeyFromRequest(r)) {
		return
	}
	if r.Method == http.MethodDelete {
		status, msg, err := rt.routeLinkDelete(r.Context(), group, routeKey, sid)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if status != http.StatusNoContent {
			if msg == "" {
				msg = http.StatusText(status)
			}
			http.Error(w, msg, status)
			return
		}
		rt.evictRoute(group, routeKey, sid)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	rr := rt.resolveRoute(r.Context(), group, routeKey, sid)
	if rr == nil || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	nodePath, ok := rewriteSandboxPath(r.URL.Path, sid, rr.NodeSandboxID)
	if !ok {
		http.Error(w, "cluster router: unsupported sandbox path", http.StatusNotImplemented)
		return
	}
	rt.forwardToNode(w, r, rr, nodePath, r.Method == http.MethodGet && isSandboxResourcePath(r.URL.Path))
}

// handleList returns the group's sandbox shard (cluster-router.md: list is
// group-local — the registry holds every node's sandboxes for the group).
func (rt *Router) handleList(w http.ResponseWriter, r *http.Request) {
	group := r.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return
	}
	if !rt.authorize(w, r.Context(), group, apiKeyFromRequest(r)) {
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
func (rt *Router) resolveRoute(ctx context.Context, group, routeKey, sandboxID string) *routeResolve {
	if rr := rt.cachedRoute(group, routeKey, sandboxID); rr != nil && routeMatchesIdentity(rr, group, routeKey, sandboxID) {
		return rr
	}
	if rr, err := rt.routeLinkRoute(ctx, group, routeKey, sandboxID); err == nil {
		if !routeMatchesIdentity(rr, group, routeKey, sandboxID) {
			rt.evictRoute(group, routeKey, sandboxID)
			return nil
		}
		rt.rememberRoute(rr)
		return rr
	}
	return nil
}

// forwardToNode proxies a control request to a node's e2b control plane (Host
// api.<domain>; the client's X-API-KEY passes through for the node's auth).
func (rt *Router) forwardToNode(w http.ResponseWriter, r *http.Request, rr *routeResolve, nodePath string, adaptSandboxIdentity bool) {
	target := &url.URL{Scheme: "http", Host: rr.DataEndpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = rt.fwdTransport
	apiHost := "api." + rt.domain
	proxy.Director = func(req *http.Request) {
		req.URL.Scheme = "http"
		req.URL.Host = rr.DataEndpoint
		req.URL.Path = nodePath
		req.URL.RawPath = ""
		req.Host = apiHost
		req.Header.Del(HeaderAccessTok) // control verbs authorize via X-API-KEY, not a client token
	}
	proxy.ModifyResponse = func(resp *http.Response) error {
		if resp.StatusCode == http.StatusNotFound {
			rt.evictRouteIfCurrent(rr.Group, rr.RouteKey, rr.SandboxID, rr.NodeSandboxID)
		}
		if adaptSandboxIdentity {
			return rewriteSandboxIdentityResponse(resp, rr.SandboxID, rr.NodeSandboxID)
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, e error) {
		rt.evictRouteIfCurrent(rr.Group, rr.RouteKey, rr.SandboxID, rr.NodeSandboxID)
		rt.log.Warn("router: control forward", "node", rr.DataEndpoint, "err", e)
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

func rewriteSandboxPath(path, sandboxID, nodeSandboxID string) (string, bool) {
	i := strings.Index(path, "/sandboxes/")
	if i < 0 || sandboxID == "" || nodeSandboxID == "" {
		return path, false
	}
	start := i + len("/sandboxes/")
	end := len(path)
	if j := strings.IndexByte(path[start:], '/'); j >= 0 {
		end = start + j
	}
	if path[start:end] != sandboxID {
		return path, false
	}
	return path[:start] + nodeSandboxID + path[end:], true
}

func isSandboxResourcePath(path string) bool {
	i := strings.Index(path, "/sandboxes/")
	if i < 0 {
		return false
	}
	rest := path[i+len("/sandboxes/"):]
	return rest != "" && !strings.ContainsRune(rest, '/')
}

func rewriteSandboxIdentityResponse(resp *http.Response, sandboxID, nodeSandboxID string) error {
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read sandbox response: %w", err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("decode sandbox response: %w", err)
	}
	var returnedID string
	rawID, found := envelope["sandboxID"]
	if !found || json.Unmarshal(rawID, &returnedID) != nil || returnedID != nodeSandboxID {
		return errors.New("node returned an unexpected sandbox identity")
	}
	envelope["sandboxID"], _ = json.Marshal(sandboxID)
	body, err = json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("encode sandbox response: %w", err)
	}
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return nil
}

// --- data plane ---

// serveExecData handles the cluster-facing logical exec service. Route lookup is
// read-only; a KAT must validate against the stable route identity before the
// Router is allowed to call Reserve(data). The final node receives the same
// service, optional port, and token with only the sandbox ID rewritten.
func (rt *Router) serveExecData(w http.ResponseWriter, r *http.Request) {
	sid, target, ok := proxypkg.ParseConnect(r)
	if !ok || target.Service != proxypkg.ConnectServiceExec {
		http.Error(w, "bad connect target", http.StatusBadRequest)
		return
	}
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

	rr := rt.cachedRoute(group, routeKey, sid)
	if rr != nil && !routeMatchesIdentity(rr, group, routeKey, sid) {
		rr = nil
	}
	if rr == nil {
		var err error
		if rr, err = rt.routeLinkRoute(r.Context(), group, routeKey, sid); err != nil {
			writeExecDataRouteError(w, err)
			return
		}
		if !routeMatchesIdentity(rr, group, routeKey, sid) {
			rt.evictRoute(group, routeKey, sid)
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		rt.rememberRoute(rr)
	}

	token := r.Header.Get(HeaderAccessTok)
	if err := keys.VerifyExecAccessToken(token, rr.ServiceSecret, rr.AuthSandboxID, time.Now()); err != nil {
		http.Error(w, "invalid access token", http.StatusUnauthorized)
		return
	}

	if rr.State != "ready" || rr.DataEndpoint == "" {
		res, err := rt.routeLinkReserve(
			r.Context(), "data", rr.Group, rr.RouteKey, sid, target.Port, 0, 0, nil,
			map[string]string{
				HeaderAccessTok:               token,
				proxypkg.HeaderSandboxService: string(target.Service),
			},
		)
		if err != nil {
			writeExecDataReserveError(w, err)
			return
		}
		current := &res.Route
		if !routeMatchesIdentity(current, rr.Group, rr.RouteKey, sid) {
			http.Error(w, "sandbox not ready", http.StatusServiceUnavailable)
			return
		}
		rt.rememberRoute(current)
		rr = current
	}
	if rr.State != "ready" || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not ready", http.StatusServiceUnavailable)
		return
	}
	rt.forwardSandboxConnect(w, r, rr, sid, target, token)
}

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
	port, err := strconv.Atoi(sub[:i])
	if err != nil || port <= 0 {
		http.Error(w, "bad data host (want <port>-<sid>.<domain>)", http.StatusBadRequest)
		return
	}
	explicitPort, hasExplicitPort, err := requestedDataPort(r, port, true)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	if rr != nil && !routeMatchesIdentity(rr, group, routeKey, sid) {
		rr = nil
	}
	if rr == nil {
		var err error
		if rr, err = rt.routeLinkRoute(r.Context(), group, routeKey, sid); err != nil {
			var routeErr *routeLinkCallError
			if errors.As(err, &routeErr) && routeErr.status == http.StatusNotFound {
				http.Error(w, "sandbox not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if !routeMatchesIdentity(rr, group, routeKey, sid) {
			rt.evictRoute(group, routeKey, sid)
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		rt.rememberRoute(rr)
	}
	effectivePort, err := effectiveDataPort(explicitPort, hasExplicitPort, rr.TargetPort)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Authenticate before Reserve so an invalid request cannot wake a paused
	// sandbox. A signed envd /files URL has no X-Access-Token for the outer node
	// CONNECT: verify the signature first, then use the protected EnvdAccessToken
	// only for that hop. The tunneled HTTP request remains unchanged so envd
	// verifies the same signature independently. Other requests reuse the client's
	// token on both hops; the node proxy performs the final target-specific check.
	connectToken := r.Header.Get(HeaderAccessTok)
	if envdsign.IsFileSignatureCandidate(r.Method, r.URL.Path, effectivePort) {
		if connectToken != "" {
			// An explicit token selects token authentication in every router auth
			// mode. A bad token must not fall back to an otherwise valid signed URL.
			if auth := envdsign.CheckDataPlaneAuth(r, effectivePort, expectedDataAccessToken(rr, effectivePort), time.Now()); !auth.OK {
				http.Error(w, "invalid access token", http.StatusUnauthorized)
				return
			}
		} else {
			if rr.Profile != string(types.ProfileE2B) || rr.EnvdAccessToken == "" ||
				envdsign.ValidateFileRequest(r, rr.EnvdAccessToken, time.Now()) != nil {
				http.Error(w, "invalid file signature", http.StatusUnauthorized)
				return
			}
			connectToken = rr.EnvdAccessToken
		}
	} else if rt.dataPlaneAuth == "enforce" || rt.dataPlaneAuth == "log" {
		auth := envdsign.CheckDataPlaneAuth(r, effectivePort, expectedDataAccessToken(rr, effectivePort), time.Now())
		if !auth.OK {
			if rt.dataPlaneAuth == "enforce" {
				http.Error(w, "invalid access token", http.StatusUnauthorized)
				return
			}
			rt.log.Warn("router: data-plane auth mismatch (log mode)", "sid", sid, "err", auth.Err)
		}
	}
	// Not ready (paused / lagging): operation-aware Reserve authenticates this
	// exact stable sandbox and prepares its current node route. A signed /files
	// request has already been verified above, so its protected EnvdAccessToken is
	// used only for Reserve and the outer CONNECT; the inner HTTP request remains
	// unchanged.
	if (rr.State != "ready" || rr.DataEndpoint == "") && rr.Group != "" {
		res, err := rt.routeLinkReserve(
			r.Context(), "data", rr.Group, rr.RouteKey, sid, effectivePort, 0, 0, nil,
			map[string]string{HeaderAccessTok: connectToken},
		)
		if err != nil {
			writeRouteLinkError(w, err, http.StatusServiceUnavailable)
			return
		}
		resumed := &res.Route
		if routeMatchesIdentity(resumed, rr.Group, rr.RouteKey, sid) {
			rt.rememberRoute(resumed)
			rr = resumed
		}
	}
	if rr.State != "ready" || rr.DataEndpoint == "" {
		http.Error(w, "sandbox not ready", http.StatusServiceUnavailable)
		return
	}
	rt.forwardSandboxData(w, r, rr, sid, effectivePort, connectToken)
}

func (rt *Router) forwardSandboxConnect(w http.ResponseWriter, r *http.Request, rr *routeResolve, sandboxID string, target proxypkg.ConnectTarget, token string) {
	doneActive := rt.beginActiveRoute(rr)
	defer doneActive()
	rt.mx.Inc(`router_requests_total{plane="data"}`)
	backend, br, resp, err := proxypkg.DialSandboxConnect(
		r.Context(), "tcp", rr.DataEndpoint, rr.NodeSandboxID, target, token,
	)
	if err != nil {
		rt.evictRouteIfCurrent(rr.Group, rr.RouteKey, sandboxID, rr.NodeSandboxID)
		rt.mx.Inc(`router_requests_total{plane="data",result="bad_gateway"}`)
		rt.log.Warn("router: data connect", "sid", sandboxID, "node", rr.NodeID, "err", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	if resp.StatusCode != http.StatusOK {
		backend.Close()
		if staleProxyError(resp.Header.Get(proxypkg.HeaderProxyError)) {
			rt.evictRouteIfCurrent(rr.Group, rr.RouteKey, sandboxID, rr.NodeSandboxID)
		}
		rt.mx.Inc(`router_requests_total{plane="data",result="connect_refused"}`)
		http.Error(w, "connect refused by node", resp.StatusCode)
		return
	}
	proxypkg.TunnelBuffered(w, r, backend, br)
}

func expectedDataAccessToken(route *routeResolve, port int) string {
	if route != nil && route.Profile == string(types.ProfileE2B) &&
		(port == 49983 || port == 49999) {
		return route.EnvdAccessToken
	}
	if route == nil {
		return ""
	}
	return route.ForwardAccessToken
}

// forwardSandboxData two-hop forwards a data-plane request to the sandbox's node:
// sandboxID is the stable public identity used to address the route. connectToken is
// used only for the outer node CONNECT. Every ordinary HTTP request is written
// unchanged inside that one-shot tunnel, apart from rewriting known identity carriers
// to the current node-local identity.
func (rt *Router) forwardSandboxData(w http.ResponseWriter, r *http.Request, rr *routeResolve, sandboxID string, port int, connectToken string) {
	nodeSandboxHost := fmt.Sprintf("%d-%s.%s", port, rr.NodeSandboxID, rt.domain)
	doneActive := rt.beginActiveRoute(rr)
	defer doneActive()
	rt.mx.Inc(`router_requests_total{plane="data"}`)
	backend, br, resp, err := proxypkg.DialSandboxConnect(r.Context(), "tcp", rr.DataEndpoint, rr.NodeSandboxID, proxypkg.LegacyTarget(port), connectToken)
	if err != nil {
		rt.evictRouteIfCurrent(rr.Group, rr.RouteKey, sandboxID, rr.NodeSandboxID)
		rt.mx.Inc(`router_requests_total{plane="data",result="bad_gateway"}`)
		rt.log.Warn("router: data connect", "sid", sandboxID, "node", rr.NodeID, "err", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	if resp.StatusCode != http.StatusOK {
		backend.Close()
		if staleProxyError(resp.Header.Get(proxypkg.HeaderProxyError)) {
			rt.evictRouteIfCurrent(rr.Group, rr.RouteKey, sandboxID, rr.NodeSandboxID)
		}
		rt.mx.Inc(`router_requests_total{plane="data",result="connect_refused"}`)
		http.Error(w, "connect refused by node", resp.StatusCode)
		return
	}
	if r.Method == http.MethodConnect {
		proxypkg.TunnelBuffered(w, r, backend, br)
		return
	}
	defer backend.Close()
	resp, err = proxypkg.ForwardHTTPOnce(r, backend, br, func(req *http.Request) {
		req.Host = nodeSandboxHost
		req.URL.Host = nodeSandboxHost
		if _, present := req.Header[http.CanonicalHeaderKey(proxypkg.HeaderSandboxID)]; present {
			req.Header.Set(proxypkg.HeaderSandboxID, rr.NodeSandboxID)
		}
	})
	if err != nil {
		rt.mx.Inc(`router_requests_total{plane="data",result="bad_gateway"}`)
		rt.log.Warn("router: data forward", "sid", sandboxID, "node", rr.NodeID, "err", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	proxypkg.WriteHTTPResponse(w, resp)
}

func staleProxyError(kind string) bool {
	return kind == proxypkg.ProxyErrorNotFound || kind == proxypkg.ProxyErrorUnauthorized
}

func requestedDataPort(r *http.Request, hostPort int, hasHostPort bool) (int, bool, error) {
	var port int
	var found bool
	add := func(p int, source string) error {
		if p <= 0 {
			return fmt.Errorf("bad sandbox port from %s", source)
		}
		if found && port != p {
			return fmt.Errorf("conflicting sandbox ports")
		}
		port = p
		found = true
		return nil
	}
	if hasHostPort {
		if err := add(hostPort, "host"); err != nil {
			return 0, false, err
		}
	}
	if h := r.Header.Get(proxypkg.HeaderSandboxPort); h != "" {
		p, err := strconv.Atoi(h)
		if err != nil {
			return 0, false, fmt.Errorf("bad %s", proxypkg.HeaderSandboxPort)
		}
		if err := add(p, proxypkg.HeaderSandboxPort); err != nil {
			return 0, false, err
		}
	}
	if r.Method == http.MethodConnect {
		if p, ok, err := connectAuthorityPort(r); err != nil {
			return 0, false, err
		} else if ok {
			if err := add(p, "CONNECT authority"); err != nil {
				return 0, false, err
			}
		}
	}
	return port, found, nil
}

func connectAuthorityPort(r *http.Request) (int, bool, error) {
	target := r.URL.Host
	if target == "" {
		target = r.Host
	}
	if target == "" {
		return 0, false, nil
	}
	_, ps, err := net.SplitHostPort(target)
	if err != nil {
		return 0, false, fmt.Errorf("bad CONNECT authority")
	}
	p, err := strconv.Atoi(ps)
	if err != nil || p <= 0 {
		return 0, false, fmt.Errorf("bad CONNECT authority")
	}
	return p, true, nil
}

func effectiveDataPort(explicit int, hasExplicit bool, targetPort int) (int, error) {
	if targetPort > 0 {
		if hasExplicit && explicit != targetPort {
			return 0, fmt.Errorf("target port mismatch")
		}
		return targetPort, nil
	}
	if hasExplicit {
		return explicit, nil
	}
	return 0, fmt.Errorf("target port required")
}

func (rt *Router) evictRoute(group, routeKey, sid string) {
	rt.cacheMu.Lock()
	rt.evictRouteLocked(group, routeKey, sid)
	rt.cacheMu.Unlock()
}

func (rt *Router) evictRouteIfCurrent(group, routeKey, sandboxID, nodeSandboxID string) {
	rt.cacheMu.Lock()
	if rr := rt.cache[routeCacheID(group, routeKey, sandboxID)]; rr != nil && rr.NodeSandboxID != nodeSandboxID {
		rt.cacheMu.Unlock()
		return
	}
	rt.evictRouteLocked(group, routeKey, sandboxID)
	rt.cacheMu.Unlock()
}

func (rt *Router) evictRouteLocked(group, routeKey, sid string) {
	key := routeCacheID(group, routeKey, sid)
	if rr := rt.cache[key]; rr != nil {
		for _, key := range activeRouteKeys(rr) {
			delete(rt.active, key)
		}
	}
	delete(rt.active, activeSIDKey(group, routeKey, sid))
	delete(rt.cache, key)
}

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
	keys := make([]string, 0, 1)
	if rr.Group != "" && rr.RouteKey != "" && rr.SandboxID != "" {
		keys = append(keys, activeSIDKey(rr.Group, rr.RouteKey, rr.SandboxID))
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
	if rr == nil || rr.Group == "" || rr.RouteKey == "" || rr.SandboxID == "" || rr.NodeSandboxID == "" {
		return
	}
	cp := *rr
	now := time.Now()
	cp.cachedAt = now
	cp.lastUsed = now
	key := routeCacheID(cp.Group, cp.RouteKey, cp.SandboxID)
	rt.cacheMu.Lock()
	if current := rt.cache[key]; current != nil {
		if cp.RouteRevision < current.RouteRevision ||
			(cp.RouteRevision == current.RouteRevision && cp.NodeSandboxID != current.NodeSandboxID) {
			rt.cacheMu.Unlock()
			return
		}
	}
	rt.cache[key] = &cp
	rt.cacheMu.Unlock()
}

func routeBelongsToGroup(rr *routeResolve, group string) bool {
	return rr != nil && group != "" && rr.Group == group
}

func routeMatchesIdentity(rr *routeResolve, group, routeKey, sandboxID string) bool {
	return routeBelongsToGroup(rr, group) && routeKey != "" && rr.RouteKey == routeKey &&
		sandboxID != "" && rr.SandboxID == sandboxID && rr.NodeSandboxID != ""
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

// --- control client ---

func (rt *Router) routeLinkReserve(ctx context.Context, operation, group, routeKey, sandboxID string, port, timeout int, ttlSeconds int64, config map[string]string, headers map[string]string) (*reserveResult, error) {
	query := url.Values{
		"operation": []string{operation},
		"group":     []string{group},
		"route_key": []string{routeKey},
	}
	if sandboxID != "" {
		query.Set("sid", sandboxID)
	}
	if port > 0 {
		query.Set("port", strconv.Itoa(port))
	}
	if timeout != 0 {
		query.Set("timeout", strconv.Itoa(timeout))
	}
	if ttlSeconds > 0 {
		query.Set("ttl_seconds", strconv.FormatInt(ttlSeconds, 10))
	}
	path := registry.RouteLinkReservePath + "?" + query.Encode()
	var body []byte
	if operation == "create" {
		body, _ = json.Marshal(registry.SandboxReserveReq{Config: config})
		if headers == nil {
			headers = map[string]string{}
		}
		headers["Content-Type"] = "application/json"
	}
	var res reserveResult
	if err := rt.routeLinkCallWithConflictRetry(ctx, group, http.MethodPost, path, body, headers, &res, operation != "connect"); err != nil {
		return nil, err
	}
	return &res, nil
}

func (rt *Router) routeLinkReserveBuild(ctx context.Context, group string, profile types.Profile, resources *buildResources) (*buildReserveResult, error) {
	reqBody, _ := json.Marshal(map[string]any{
		"group": group, "build_id": "bld-" + randomHexID(), "template_id": "transient-" + randomHexID(), "profile": profile, "resources": resources,
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

func (rt *Router) routeLinkDelete(ctx context.Context, group, routeKey, sid string) (int, string, error) {
	path := fmt.Sprintf("%s?group=%s&route_key=%s&sid=%s", registry.RouteLinkDeletePath, url.QueryEscape(group), url.QueryEscape(routeKey), url.QueryEscape(sid))
	resp, err := rt.routeLinkHTTP(ctx, group, http.MethodDelete, path, nil, nil)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return resp.StatusCode, strings.TrimSpace(string(b)), nil
}

func (rt *Router) routeLinkCall(ctx context.Context, group, method, path string, body []byte, headers map[string]string, out any) error {
	return rt.routeLinkCallWithConflictRetry(ctx, group, method, path, body, headers, out, true)
}

func (rt *Router) routeLinkCallWithConflictRetry(ctx context.Context, group, method, path string, body []byte, headers map[string]string, out any, retryConflict bool) error {
	resp, err := rt.routeLinkHTTPWithConflictRetry(ctx, group, method, path, body, headers, retryConflict)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &routeLinkCallError{status: resp.StatusCode, path: path, response: resp.Status, body: strings.TrimSpace(string(b))}
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

type routeLinkCallError struct {
	status   int
	path     string
	response string
	body     string
}

func (e *routeLinkCallError) Error() string {
	return fmt.Sprintf("route_link %s: %s: %s", e.path, e.response, e.body)
}

func writeRouteLinkError(w http.ResponseWriter, err error, fallbackStatus int) {
	status := fallbackStatus
	message := err.Error()
	var routeErr *routeLinkCallError
	if errors.As(err, &routeErr) {
		status = routeErr.status
		message = routeErr.body
		if message == "" {
			message = http.StatusText(status)
		}
	}
	http.Error(w, message, status)
}

// writeExecSessionRouteLinkError preserves the public exec-session status
// contract without forwarding Registry or node-link error details.
func writeExecSessionRouteLinkError(w http.ResponseWriter, err error) {
	status := http.StatusServiceUnavailable
	message := "exec session unavailable"
	var routeErr *routeLinkCallError
	if errors.As(err, &routeErr) {
		switch routeErr.status {
		case http.StatusBadRequest:
			status = http.StatusBadRequest
			message = "invalid exec session request"
		case http.StatusUnauthorized:
			status = http.StatusUnauthorized
			message = "unauthorized"
		case http.StatusForbidden:
			status = http.StatusForbidden
			message = "exec session credential not allowed"
		case http.StatusNotFound:
			status = http.StatusNotFound
			message = "not found"
		}
	}
	http.Error(w, message, status)
}

func writeExecDataRouteError(w http.ResponseWriter, err error) {
	var routeErr *routeLinkCallError
	if errors.As(err, &routeErr) && routeErr.status == http.StatusNotFound {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	http.Error(w, "routing unavailable", http.StatusServiceUnavailable)
}

func writeExecDataReserveError(w http.ResponseWriter, err error) {
	var routeErr *routeLinkCallError
	if errors.As(err, &routeErr) {
		switch routeErr.status {
		case http.StatusUnauthorized:
			http.Error(w, "invalid access token", http.StatusUnauthorized)
			return
		case http.StatusNotFound:
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
	}
	http.Error(w, "sandbox activation failed", http.StatusServiceUnavailable)
}

func (rt *Router) routeLinkHTTP(ctx context.Context, group, method, path string, body []byte, headers map[string]string) (*http.Response, error) {
	return rt.routeLinkHTTPWithConflictRetry(ctx, group, method, path, body, headers, true)
}

func (rt *Router) routeLinkHTTPWithConflictRetry(ctx context.Context, group, method, path string, body []byte, headers map[string]string, retryConflict bool) (*http.Response, error) {
	refreshed := false
	for {
		resp, err, retry := rt.routeLinkHTTPOnce(ctx, group, method, path, body, headers, retryConflict)
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

func (rt *Router) routeLinkHTTPOnce(ctx context.Context, group, method, path string, body []byte, headers map[string]string, retryConflict bool) (*http.Response, error, bool) {
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
		if routeLinkRetryableStatus(resp.StatusCode, retryConflict) {
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

func routeLinkRetryableStatus(status int, retryConflict bool) bool {
	return status >= 500 || retryConflict && status == http.StatusConflict
}

// --- local route cache ---

func (rt *Router) cachedRoute(group, routeKey, sid string) *routeResolve {
	cacheKey := routeCacheID(group, routeKey, sid)
	rt.cacheMu.Lock()
	defer rt.cacheMu.Unlock()
	return rt.cacheRouteLocked(cacheKey, time.Now())
}

func (rt *Router) cacheRouteLocked(cacheKey string, now time.Time) *routeResolve {
	rr := rt.cache[cacheKey]
	if rr == nil {
		return nil
	}
	if rt.routeTTL > 0 && !rr.cachedAt.IsZero() && now.Sub(rr.cachedAt) > rt.routeTTL {
		delete(rt.cache, cacheKey)
		return nil
	}
	if rt.routeIdleTimeout > 0 && !rr.lastUsed.IsZero() && now.Sub(rr.lastUsed) > rt.routeIdleTimeout {
		delete(rt.cache, cacheKey)
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

func apiKeyFromRequest(r *http.Request) string {
	if key := r.Header.Get(HeaderAPIKey); key != "" {
		return key
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if len(auth) > len("Bearer ") && strings.EqualFold(auth[:len("Bearer ")], "Bearer ") {
		return strings.TrimSpace(auth[len("Bearer "):])
	}
	return ""
}

// authorize verifies (group, api_key) and writes the right error on failure: 503
// when the control API is unreachable (don't 403-storm on a transient blip), 403
// when the key is rejected. Returns false on failure.
func (rt *Router) authorize(w http.ResponseWriter, ctx context.Context, group, apiKey string) bool {
	ok, err := rt.verifyAuth(ctx, group, apiKey)
	if err != nil {
		http.Error(w, "auth temporarily unavailable", http.StatusServiceUnavailable)
		return false
	}
	if !ok {
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
