package finalrouter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/envdsign"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	proxypkg "github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/routeclient"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

const (
	HeaderGroup     = "X-Kuasar-Sandbox-Group"
	HeaderRouteKey  = "X-Kuasar-Route-Key"
	HeaderAPIKey    = "X-API-KEY"
	HeaderAccessTok = "X-Access-Token"

	pendingBuildPollInterval = 50 * time.Millisecond
	pendingBuildForwardWait  = 30 * time.Second
)

var errRouteNotFound = errors.New("finalrouter: Route does not exist")

type ControlPlane interface {
	CurrentServeIdentity(bool) (routeapi.RegistryServeIdentity, error)
	CacheAuthorized(routeapi.RegistryServeIdentity) bool
	ReserveSandbox(context.Context, string, string, uint64, routeapi.SandboxInput) (routeclient.RouteMutationResult, error)
	ResumeSandbox(context.Context, string, string, uint64) (routeclient.RouteMutationResult, error)
	DeleteSandbox(context.Context, string, string, uint64) (routeclient.RouteMutationResult, error)
	ReadRoute(context.Context, string, string, uint64) (routeclient.RouteReadResult, error)
	ReadAddressableRoute(context.Context, string, string, uint64) (routeclient.RouteReadResult, error)
	RegisterBuild(context.Context, string, string, uint64, routeapi.BuildInput) (routeclient.BuildMutationResult, error)
	ReadBuild(context.Context, string, string, uint64) (routeclient.BuildReadResult, error)
	ListRoutesPage(context.Context, string, clusterstate.RouteWorkflowState, uint32, string) (routeclient.RouteListPageResult, error)
}

type CallerAuthorizer interface {
	Verify(context.Context, string, string) (bool, error)
}

type routeEntry struct {
	Route         clusterstate.ReadyRoute
	State         clusterstate.RouteWorkflowState
	Group         string
	RouteKey      string
	Revision      uint64
	ServeIdentity routeapi.RegistryServeIdentity
	CachedAt      time.Time
	LastUsed      time.Time
}

type routeRevisionFloor struct {
	Revision      uint64
	InactiveUntil time.Time
}

type buildEntry struct {
	Build         clusterstate.BuildProjection
	Group         string
	Revision      uint64
	ServeIdentity routeapi.RegistryServeIdentity
	CachedAt      time.Time
}

type reserveFlight struct {
	done        chan struct{}
	inputDigest [sha256.Size]byte
	route       *routeEntry
	err         error
}

var (
	errRoutePending       = errors.New("finalrouter: Route mutation is pending")
	errRouteInputConflict = errors.New("finalrouter: Route is reserved with different immutable input")
	errBuildRejected      = errors.New("finalrouter: Build registration was rejected")
)

type Router struct {
	control    ControlPlane
	authorizer CallerAuthorizer
	domain     string
	log        *slog.Logger
	mx         *metrics.M

	authMode      string
	dataPlaneAuth string
	authTTL       time.Duration
	authMu        sync.Mutex
	authOK        map[string]time.Time

	routeTTL          time.Duration
	routeIdleTimeout  time.Duration
	cacheMu           sync.Mutex
	routes            map[string]*routeEntry
	minimumRevisions  map[string]routeRevisionFloor
	routeRevisionRefs map[string]uint32
	builds            map[string]*buildEntry

	reserveMu sync.Mutex
	flights   map[string]*reserveFlight

	forward *http.Transport
	nodeTLS *tls.Config
}

func New(control ControlPlane, authorizer CallerAuthorizer, domain string, authTTL time.Duration, log *slog.Logger) (*Router, error) {
	if control == nil || authorizer == nil || domain == "" {
		return nil, errors.New("router: control plane, Provider authorizer, and domain are required")
	}
	if authTTL <= 0 {
		authTTL = time.Minute
	}
	if log == nil {
		log = slog.Default()
	}
	return &Router{
		control: control, authorizer: authorizer, domain: domain, log: log, mx: metrics.New(),
		authMode: "enforce", dataPlaneAuth: "enforce", authTTL: authTTL,
		authOK: make(map[string]time.Time), routeTTL: 5 * time.Minute, routeIdleTimeout: 2 * time.Minute,
		routes:            make(map[string]*routeEntry),
		minimumRevisions:  make(map[string]routeRevisionFloor),
		routeRevisionRefs: make(map[string]uint32), builds: make(map[string]*buildEntry),
		flights: make(map[string]*reserveFlight),
		forward: &http.Transport{
			MaxIdleConns: 512, MaxIdleConnsPerHost: 64, IdleConnTimeout: 90 * time.Second,
		},
	}, nil
}

func (r *Router) SetNodeTLS(config *tls.Config) error {
	if config == nil || len(config.Certificates) == 0 || config.RootCAs == nil {
		return errors.New("router: node mTLS requires a client certificate and CA")
	}
	r.nodeTLS = config.Clone()
	r.forward.TLSClientConfig = config.Clone()
	r.forward.ForceAttemptHTTP2 = true
	return nil
}

func (r *Router) Metrics() *metrics.M { return r.mx }

func (r *Router) SetAuthMode(mode string) {
	if mode == "off" || mode == "log" || mode == "enforce" {
		r.authMode = mode
	}
}

func (r *Router) SetDataPlaneAuth(mode string) {
	if mode == "off" || mode == "log" || mode == "enforce" {
		r.dataPlaneAuth = mode
	}
}

func (r *Router) SetRouteCache(routeTTL, idle time.Duration) {
	if routeTTL > 0 {
		r.routeTTL = routeTTL
	}
	if idle > 0 {
		r.routeIdleTimeout = idle
	}
}

func (r *Router) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		host := request.Host
		if index := strings.IndexByte(host, ':'); index >= 0 {
			host = host[:index]
		}
		if host == "api."+r.domain {
			r.serveControl(w, request)
			return
		}
		r.serveData(w, request, host)
	})
}

func (r *Router) serveControl(w http.ResponseWriter, request *http.Request) {
	r.mx.Inc(`router_requests_total{plane="control"}`)
	path := request.URL.Path
	switch {
	case request.Method == http.MethodGet && path == "/health":
		w.WriteHeader(http.StatusNoContent)
	case request.Method == http.MethodPost && sandboxCollection(path):
		r.createSandbox(w, request)
	case request.Method == http.MethodGet && sandboxCollection(path):
		r.listSandboxes(w, request)
	case request.Method == http.MethodPost && templateCollection(path):
		r.registerBuild(w, request)
	case buildOperation(path):
		r.forwardBuild(w, request)
	case sandboxVerb(path):
		r.sandboxControl(w, request)
	default:
		http.Error(w, "cluster router: operation is not supported", http.StatusNotImplemented)
	}
}

func sandboxCollection(path string) bool  { return path == "/sandboxes" || path == "/v2/sandboxes" }
func templateCollection(path string) bool { return path == "/templates" || path == "/v3/templates" }
func buildOperation(path string) bool {
	return strings.Contains(path, "/builds/") || strings.Contains(path, "/files/")
}
func sandboxVerb(path string) bool {
	return strings.HasPrefix(path, "/sandboxes/") || strings.HasPrefix(path, "/v2/sandboxes/")
}

func nodeRequestEnvelope(request *http.Request) (clusterstate.NodeRequestEnvelopeV1, error) {
	raw, err := io.ReadAll(io.LimitReader(request.Body, clusterstate.MaxNodeRequestBodyBytes+1))
	if err != nil {
		return clusterstate.NodeRequestEnvelopeV1{}, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return clusterstate.NodeRequestEnvelopeV1{}, errors.New("request body must be a JSON object")
	}
	return clusterstate.NewNodeRequestEnvelopeV1(
		request.Method, request.URL.Path, request.URL.RawQuery, request.Header, raw,
	)
}

func sandboxCreateInput(request *http.Request) (routeapi.SandboxInput, error) {
	envelope, err := nodeRequestEnvelope(request)
	if err != nil {
		return routeapi.SandboxInput{}, err
	}
	create, err := api.ParseSandboxCreateEnvelope(envelope)
	if err != nil {
		return routeapi.SandboxInput{}, err
	}
	envelope, err = api.RewriteSandboxCreateEnvelope(
		envelope, create.TemplateID, create.TimeoutSec, create.Metadata,
	)
	if err != nil {
		return routeapi.SandboxInput{}, err
	}
	input := routeapi.SandboxInput{
		TemplateRef: create.TemplateID, Config: create.Metadata, TimeoutSeconds: create.TimeoutSec,
		Demand: placement.SandboxDemand{SlotUnits: 1}, Request: envelope,
	}
	return input, input.Validate()
}

func defaultSandboxInput() (routeapi.SandboxInput, error) {
	envelope, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/sandboxes", "", nil, []byte("{}"),
	)
	if err != nil {
		return routeapi.SandboxInput{}, err
	}
	input := routeapi.SandboxInput{
		Demand: placement.SandboxDemand{SlotUnits: 1}, Request: envelope,
	}
	return input, input.Validate()
}

func firstOrEmpty(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func (r *Router) createSandbox(w http.ResponseWriter, request *http.Request) {
	group, ok := r.authorizedGroup(w, request)
	if !ok {
		return
	}
	routeKey := request.Header.Get(HeaderRouteKey)
	if routeKey == "" {
		routeKey = "rk-" + randomID()
	} else if !validRouteKey(routeKey) {
		http.Error(w, "invalid Sandbox route key", http.StatusBadRequest)
		return
	}
	input, err := sandboxCreateInput(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	entry, err := r.reserve(request.Context(), group, routeKey, input)
	if err != nil {
		if errors.Is(err, errRoutePending) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"sandboxID": routeKey, "routeKey": routeKey, "state": "pending",
			})
			return
		}
		if errors.Is(err, errRouteInputConflict) {
			http.Error(w, "route key is reserved with different input", http.StatusConflict)
			return
		}
		r.log.Warn("router reserve", "group", group, "route_key", routeKey, "err", err)
		http.Error(w, "sandbox is not ready", http.StatusServiceUnavailable)
		return
	}
	route := entry.Route
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"sandboxID": routeKey, "routeKey": routeKey, "clientID": route.NodeID,
		"accessToken": route.AccessToken, "envdAccessToken": route.AccessToken,
		"trafficAccessToken": route.TrafficAccessToken, "domain": r.domain,
		"templateID": route.TemplateRef, "envdVersion": route.Presentation.EnvdVersion,
	})
}

func (r *Router) reserve(ctx context.Context, group, routeKey string, input routeapi.SandboxInput) (*routeEntry, error) {
	key := routeKeyID(group, routeKey)
	rawInput, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	inputDigest := sha256.Sum256(rawInput)
	r.reserveMu.Lock()
	if flight := r.flights[key]; flight != nil {
		if flight.inputDigest != inputDigest {
			r.reserveMu.Unlock()
			return nil, errRouteInputConflict
		}
		r.reserveMu.Unlock()
		select {
		case <-flight.done:
			return flight.route, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &reserveFlight{done: make(chan struct{}), inputDigest: inputDigest}
	r.flights[key] = flight
	r.reserveMu.Unlock()

	minimum, releaseRevision := r.beginRouteRevision(group, routeKey)
	defer releaseRevision()
	result, err := r.control.ReserveSandbox(ctx, group, routeKey, minimum, input)
	if errors.Is(err, routeclient.ErrMutationOutcomeUnknown) {
		err = errors.Join(errRoutePending, err)
	}
	if err == nil {
		response := result.Response
		if response.Outcome == routeapi.MutationPending {
			err = errRoutePending
		} else if response.Outcome != routeapi.MutationReady || response.Route == nil {
			err = fmt.Errorf("Route mutation ended as %s: %s", response.Outcome, response.Reason)
		} else {
			flight.route = &routeEntry{
				Route: *response.Route, State: clusterstate.WorkflowRouteReady, Group: group, RouteKey: routeKey,
				Revision: response.RouteRevision, ServeIdentity: result.ServeIdentity,
			}
			if !r.control.CacheAuthorized(result.ServeIdentity) {
				err = errors.New("Router Serve Permit changed before Route cache install")
				flight.route = nil
			} else if !r.rememberRoute(flight.route) {
				err = errors.New("Route revision was fenced before cache install")
				flight.route = nil
			}
		}
	}
	flight.err = err
	close(flight.done)
	r.reserveMu.Lock()
	delete(r.flights, key)
	r.reserveMu.Unlock()
	return flight.route, flight.err
}

func (r *Router) listSandboxes(w http.ResponseWriter, request *http.Request) {
	group, ok := r.authorizedGroup(w, request)
	if !ok {
		return
	}
	query := request.URL.Query()
	state, knownState := routeListState(query.Get("state"))
	if !knownState {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "[]\n")
		return
	}
	result, err := r.control.ListRoutesPage(
		request.Context(), group, state, routeListLimit(query.Get("limit")), query.Get("nextToken"),
	)
	if errors.Is(err, routeclient.ErrInvalidRouteListToken) {
		http.Error(w, "invalid nextToken", http.StatusBadRequest)
		return
	}
	if err != nil || !r.control.CacheAuthorized(result.ServeIdentity) {
		http.Error(w, "Route list unavailable", http.StatusServiceUnavailable)
		return
	}
	items := make([]map[string]any, 0, len(result.Routes))
	for _, route := range result.Routes {
		state := "running"
		if route.State == clusterstate.WorkflowRoutePaused {
			state = "paused"
		}
		items = append(items, map[string]any{
			"sandboxID": route.RouteKey, "clientID": route.NodeID,
			"templateID": route.TemplateRef, "state": state,
			"cpuCount": route.Presentation.CPUCount, "memoryMB": route.Presentation.MemoryMB,
			"diskSizeMB": route.Presentation.DiskSizeMB, "envdVersion": route.Presentation.EnvdVersion,
			"startedAt": presentationTime(route.Presentation.StartedAt),
			"endAt":     presentationTime(route.Presentation.EndAt), "metadata": route.Presentation.Metadata,
		})
	}
	if result.NextToken != "" {
		w.Header().Set("x-next-token", result.NextToken)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(items)
}

func routeListState(value string) (clusterstate.RouteWorkflowState, bool) {
	switch value {
	case "":
		return "", true
	case "running":
		return clusterstate.WorkflowRouteReady, true
	case "paused":
		return clusterstate.WorkflowRoutePaused, true
	default:
		return "", false
	}
}

func routeListLimit(value string) uint32 {
	if value == "" {
		return 100
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 {
		return 100
	}
	if parsed > 1000 {
		return 1000
	}
	return uint32(parsed)
}

func presentationTime(unixSeconds int64) string {
	return time.Unix(unixSeconds, 0).UTC().Format(time.RFC3339)
}

func (r *Router) sandboxControl(w http.ResponseWriter, request *http.Request) {
	group, ok := r.authorizedGroup(w, request)
	if !ok {
		return
	}
	if strings.Contains(request.URL.Path, "/import") || strings.Contains(request.URL.Path, "/export") {
		http.Error(w, "import/export is reserved for explicit serveIdentity recovery", http.StatusNotImplemented)
		return
	}
	routeKey, decoded := escapedPathObjectID(request.URL.EscapedPath(), "/sandboxes/")
	if !decoded || routeKey == "" {
		http.Error(w, "Sandbox route key is required", http.StatusBadRequest)
		return
	}
	if !validRouteKey(routeKey) {
		http.Error(w, "invalid Sandbox route key", http.StatusBadRequest)
		return
	}
	if header := request.Header.Get(HeaderRouteKey); header != "" && header != routeKey {
		http.Error(w, "conflicting Sandbox route key", http.StatusBadRequest)
		return
	}
	if request.Method == http.MethodDelete {
		exactRouteKey, exact := escapedPathObjectIDExact(request.URL.EscapedPath(), "/sandboxes/")
		if !exact || exactRouteKey != routeKey {
			http.Error(w, "DELETE is only valid for the Sandbox resource", http.StatusMethodNotAllowed)
			return
		}
		minimum, releaseRevision := r.beginRouteRevision(group, routeKey)
		result, deleteErr := r.control.DeleteSandbox(request.Context(), group, routeKey, minimum)
		releaseRevision()
		if deleteErr != nil {
			http.Error(w, "delete unavailable", http.StatusServiceUnavailable)
			return
		}
		switch result.Response.Outcome {
		case routeapi.MutationTerminal:
			r.evictRoute(group, routeKey)
			w.WriteHeader(http.StatusNoContent)
		case routeapi.MutationPending:
			r.evictRoute(group, routeKey)
			w.WriteHeader(http.StatusAccepted)
		case routeapi.MutationConflict:
			http.Error(w, result.Response.Reason, http.StatusConflict)
		default:
			http.Error(w, "delete unavailable", http.StatusServiceUnavailable)
		}
		return
	}
	var entry *routeEntry
	var err error
	connectRouteKey, exactConnect := escapedSandboxAction(request.URL.EscapedPath(), "connect")
	if request.Method == http.MethodPost && exactConnect && connectRouteKey == routeKey {
		minimum, releaseRevision := r.beginRouteRevision(group, routeKey)
		result, resumeErr := r.control.ResumeSandbox(request.Context(), group, routeKey, minimum)
		if resumeErr == nil && result.Response.Outcome == routeapi.MutationReady && result.Response.Route != nil {
			entry = &routeEntry{Route: *result.Response.Route, Group: group, RouteKey: routeKey,
				State: clusterstate.WorkflowRouteReady, Revision: result.Response.RouteRevision, ServeIdentity: result.ServeIdentity}
			if !r.rememberRoute(entry) {
				entry = nil
				err = errors.New("resumed Route revision was already fenced")
			}
		} else {
			err = errors.Join(resumeErr, errors.New("Route resume did not become READY"))
		}
		releaseRevision()
	} else {
		entry, err = r.resolveControlRoute(request.Context(), group, routeKey)
	}
	if err != nil || entry == nil {
		if errors.Is(err, errRouteNotFound) {
			http.Error(w, "sandbox not found", http.StatusNotFound)
			return
		}
		http.Error(w, "sandbox unavailable", http.StatusServiceUnavailable)
		return
	}
	r.forwardNodeControl(w, request, entry, nil)
}

func (r *Router) registerBuild(w http.ResponseWriter, request *http.Request) {
	group, ok := r.authorizedGroup(w, request)
	if !ok {
		return
	}
	envelope, err := nodeRequestEnvelope(request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	register, err := api.ParseBuildRegisterEnvelope(envelope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	buildID := "bld-" + randomID()
	templateID := types.TransientPrefix + buildID
	names := nonEmpty(register.Name)
	aliases := canonicalStrings(register.Tags)
	register.Name, register.Tags = firstOrEmpty(names), append([]string(nil), aliases...)
	envelope, err = api.RewriteBuildRegisterEnvelope(envelope, register)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	demand, err := checkedBuildDemand(register.CPUCount, register.MemoryMB)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	input := routeapi.BuildInput{
		TemplateID: templateID, Profile: register.Profile, Names: names, Aliases: aliases,
		Metadata: register.Metadata, CPUCount: register.CPUCount, MemoryMB: register.MemoryMB,
		Request: envelope, Demand: demand,
	}
	result, err := r.control.RegisterBuild(request.Context(), group, buildID, 0, input)
	if errors.Is(err, routeclient.ErrPermitUnavailable) {
		http.Error(w, "Build registration unavailable", http.StatusServiceUnavailable)
		return
	}
	if err != nil {
		if !errors.Is(err, routeclient.ErrMutationOutcomeUnknown) {
			http.Error(w, "Build registration unavailable", http.StatusServiceUnavailable)
			return
		}
		r.log.Warn("Build registration outcome is unknown after request delivery", "group", group, "build_id", buildID, "err", err)
	}
	if err != nil || result.Response.Outcome == routeapi.MutationPending {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"templateID": templateID, "buildID": buildID, "public": false,
			"names": names, "tags": aliases, "aliases": aliases, "profile": register.Profile,
			"state": "pending",
		})
		return
	}
	if result.Response.Outcome != routeapi.MutationReady || result.Response.Build == nil {
		http.Error(w, "Build registration unavailable", http.StatusServiceUnavailable)
		return
	}
	entry := &buildEntry{
		Build: *result.Response.Build, Group: group, Revision: result.Response.BuildRevision,
		ServeIdentity: result.ServeIdentity,
	}
	if !r.control.CacheAuthorized(entry.ServeIdentity) {
		http.Error(w, "Build serveIdentity changed", http.StatusServiceUnavailable)
		return
	}
	r.rememberBuild(entry)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"templateID": templateID, "buildID": buildID, "public": false,
		"names": names, "tags": aliases, "aliases": aliases, "profile": register.Profile,
	})
}

func (r *Router) forwardBuild(w http.ResponseWriter, request *http.Request) {
	group, ok := r.authorizedGroup(w, request)
	if !ok {
		return
	}
	buildID := pathObjectID(request.URL.Path, "/builds/")
	if buildID == "" {
		templateID := pathObjectID(request.URL.Path, "/templates/")
		buildID = strings.TrimPrefix(templateID, types.TransientPrefix)
		if buildID == templateID {
			buildID = ""
		}
	}
	if buildID == "" {
		http.Error(w, "Build ID is required", http.StatusBadRequest)
		return
	}
	entry := r.cachedBuild(group, buildID)
	if entry == nil {
		result, err := r.control.ReadBuild(request.Context(), group, buildID, 0)
		if err != nil {
			http.Error(w, "Build lookup unavailable", http.StatusServiceUnavailable)
			return
		}
		if result.Response.Outcome == routeapi.ReadConflict && result.Response.Pending != nil {
			pending := result.Response.Pending
			templateID := pathObjectID(request.URL.Path, "/templates/")
			if templateID == "" || templateID != pending.TemplateRef {
				http.Error(w, "Build template does not match registration", http.StatusConflict)
				return
			}
			if !r.control.CacheAuthorized(result.ServeIdentity) {
				http.Error(w, "Router Serve Permit expired", http.StatusServiceUnavailable)
				return
			}
			if request.Method == http.MethodGet && strings.HasSuffix(strings.TrimSuffix(request.URL.Path, "/"), "/status") {
				writePendingBuildStatus(w, *pending)
				return
			}
			entry, err = r.awaitBuildRegistration(request.Context(), group, buildID)
			if err != nil {
				if errors.Is(err, errBuildRejected) {
					http.Error(w, "Build registration was rejected", http.StatusConflict)
					return
				}
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Build registration is pending", http.StatusServiceUnavailable)
				return
			}
		} else if result.Response.Outcome == routeapi.ReadReady && result.Response.Build != nil {
			entry = &buildEntry{
				Build: *result.Response.Build, Group: group, Revision: result.Response.BuildRevision,
				ServeIdentity: result.ServeIdentity,
			}
			r.rememberBuild(entry)
		} else {
			http.Error(w, "unknown build", http.StatusNotFound)
			return
		}
	}
	if templateID := pathObjectID(request.URL.Path, "/templates/"); templateID == "" || templateID != entry.Build.TemplateRef {
		http.Error(w, "Build template does not match registration", http.StatusConflict)
		return
	}
	r.forwardNodeControl(w, request, nil, entry)
}

func checkedBuildDemand(cpuCount, memoryMB int) (placement.BuildDemand, error) {
	if cpuCount <= 0 || memoryMB <= 0 {
		return placement.BuildDemand{}, errors.New("Build CPU and memory ceilings must be positive")
	}
	const bytesPerMiB = uint64(1 << 20)
	maximum := ^uint64(0)
	cpu, memory := uint64(cpuCount), uint64(memoryMB)
	if cpu > maximum/1000 || memory > maximum/bytesPerMiB {
		return placement.BuildDemand{}, errors.New("Build CPU or memory ceiling exceeds admission limits")
	}
	return placement.BuildDemand{Slots: 1, CPU: cpu * 1000, Memory: memory * bytesPerMiB}, nil
}

func writePendingBuildStatus(w http.ResponseWriter, pending routeapi.PendingBuildProjection) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"templateID": pending.TemplateRef,
		"buildID":    pending.BuildID,
		"profile":    pending.Profile,
		"status":     "building",
		"logs":       []string{},
		"logEntries": []any{},
	})
}

func (r *Router) awaitBuildRegistration(ctx context.Context, group, buildID string) (*buildEntry, error) {
	waitCtx, cancel := context.WithTimeout(ctx, pendingBuildForwardWait)
	defer cancel()
	ticker := time.NewTicker(pendingBuildPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-waitCtx.Done():
			return nil, waitCtx.Err()
		case <-ticker.C:
			result, err := r.control.ReadBuild(waitCtx, group, buildID, 0)
			if err != nil {
				continue
			}
			if result.Response.Outcome == routeapi.ReadConflict && result.Response.Pending != nil {
				continue
			}
			if result.Response.Outcome != routeapi.ReadReady || result.Response.Build == nil {
				return nil, errBuildRejected
			}
			entry := &buildEntry{
				Build: *result.Response.Build, Group: group, Revision: result.Response.BuildRevision,
				ServeIdentity: result.ServeIdentity,
			}
			if !r.control.CacheAuthorized(entry.ServeIdentity) {
				return nil, routeclient.ErrPermitUnavailable
			}
			r.rememberBuild(entry)
			return entry, nil
		}
	}
}

func (r *Router) forwardNodeControl(w http.ResponseWriter, request *http.Request, route *routeEntry, build *buildEntry) {
	if (route == nil) == (build == nil) {
		http.Error(w, "invalid direct execution target", http.StatusInternalServerError)
		return
	}
	var endpoint, group, objectID, kind, serveIdentity, bindingDigest, nodeID, routeKey string
	var nodeEpoch uint64
	if route != nil {
		_, releaseRevision := r.beginRouteRevision(route.Group, route.RouteKey)
		defer releaseRevision()
		if !r.control.CacheAuthorized(route.ServeIdentity) {
			http.Error(w, "Router Serve Permit expired", http.StatusServiceUnavailable)
			return
		}
		endpoint, group, objectID, kind = route.Route.DataEndpoint, route.Group, route.Route.SandboxID, "sandbox"
		serveIdentity, bindingDigest, nodeID, nodeEpoch, routeKey = route.Route.RegistryGeneration,
			route.Route.BindingDigest, route.Route.NodeID, route.Route.NodeEpoch, route.RouteKey
	} else {
		if !r.control.CacheAuthorized(build.ServeIdentity) {
			http.Error(w, "Router Serve Permit expired", http.StatusServiceUnavailable)
			return
		}
		endpoint, group, objectID, kind = build.Build.DataEndpoint, build.Group, build.Build.BuildID, "build"
		serveIdentity, bindingDigest, nodeID, nodeEpoch = build.Build.RegistryGeneration,
			build.Build.BindingDigest, build.Build.NodeID, build.Build.NodeEpoch
	}
	scheme := "http"
	if r.nodeTLS != nil {
		scheme = "https"
	}
	target := &url.URL{Scheme: scheme, Host: endpoint}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = r.forward
	proxy.Director = func(next *http.Request) {
		next.URL.Scheme = scheme
		next.URL.Host = endpoint
		next.Host = "api." + r.domain
		if route != nil {
			escaped := next.URL.EscapedPath()
			if strings.HasPrefix(escaped, "/v2/sandboxes/") {
				escaped = strings.TrimPrefix(escaped, "/v2")
			}
			escaped = rewritePathObjectID(escaped, "/sandboxes/", objectID)
			if decoded, err := url.PathUnescape(escaped); err == nil {
				next.URL.Path, next.URL.RawPath = decoded, escaped
			}
		}
		next.Header.Del(HeaderAccessTok)
		clearDirectFence(next.Header)
		next.Header.Set(clusterstate.DirectHeaderExecutionKind, kind)
		next.Header.Set(clusterstate.DirectHeaderObjectID, objectID)
		next.Header.Set(clusterstate.DirectHeaderGroup, group)
		if routeKey != "" {
			next.Header.Set(clusterstate.DirectHeaderRouteKey, routeKey)
		}
		next.Header.Set(proxypkg.HeaderNodeID, nodeID)
		next.Header.Set(proxypkg.HeaderNodeEpoch, strconv.FormatUint(nodeEpoch, 10))
		next.Header.Set(proxypkg.HeaderRegistryGeneration, serveIdentity)
		next.Header.Set(proxypkg.HeaderBindingDigest, bindingDigest)
	}
	proxy.ModifyResponse = func(response *http.Response) error {
		proxyError := response.Header.Get(proxypkg.HeaderProxyError)
		if proxyError == proxypkg.ProxyErrorWrongBinding || proxyError == proxypkg.ProxyErrorWrongNodeEpoch {
			if route != nil {
				r.rejectStaleRoute(route)
			} else {
				r.evictBuild(build.Group, build.Build.BuildID)
			}
		}
		if route != nil && strings.Contains(response.Header.Get("Content-Type"), "application/json") {
			return rewriteSandboxResponse(response, route.RouteKey)
		}
		return nil
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	proxy.ServeHTTP(w, request)
}

func clearDirectFence(header http.Header) {
	for _, name := range []string{
		clusterstate.DirectHeaderExecutionKind, clusterstate.DirectHeaderObjectID,
		clusterstate.DirectHeaderGroup, clusterstate.DirectHeaderRouteKey,
		proxypkg.HeaderNodeID, proxypkg.HeaderNodeEpoch,
		proxypkg.HeaderRegistryGeneration, proxypkg.HeaderBindingDigest,
	} {
		header.Del(name)
	}
}

func rewriteSandboxResponse(response *http.Response, routeKey string) error {
	if response.Body == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	response.Body.Close()
	if err != nil {
		return err
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	value = rewriteSandboxJSON(value, routeKey)
	raw, err = json.Marshal(value)
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	response.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	return nil
}

func rewriteSandboxJSON(value any, routeKey string) any {
	rewriteIdentity := func(object map[string]any) {
		if _, present := object["sandboxID"]; present {
			object["sandboxID"] = routeKey
		}
	}
	switch typed := value.(type) {
	case map[string]any:
		rewriteIdentity(typed)
		return typed
	case []any:
		for index := range typed {
			if object, ok := typed[index].(map[string]any); ok {
				rewriteIdentity(object)
			}
		}
		return typed
	}
	return value
}

func (r *Router) serveData(w http.ResponseWriter, request *http.Request, host string) {
	group := request.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" is required", http.StatusBadRequest)
		return
	}
	routeKey, hostPort, hasHostPort, err := parseDataHost(
		host, r.domain, request.Header.Get(proxypkg.HeaderSandboxID),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if header := request.Header.Get(HeaderRouteKey); header != "" {
		if routeKey != "" && routeKey != header {
			http.Error(w, "conflicting Sandbox route key", http.StatusBadRequest)
			return
		}
		routeKey = header
	}
	if routeKey == "" {
		http.Error(w, "Sandbox route key is required", http.StatusBadRequest)
		return
	}
	if !validRouteKey(routeKey) {
		http.Error(w, "invalid Sandbox route key", http.StatusBadRequest)
		return
	}
	var (
		port                   int
		injectToken            bool
		consumedProviderHeader string
		authenticated          bool
	)
	entry, err := r.resolveRoute(request.Context(), group, routeKey)
	if err != nil {
		addressable, addressErr := r.resolveControlRoute(request.Context(), group, routeKey)
		if addressErr == nil && addressable != nil && addressable.State == clusterstate.WorkflowRoutePaused {
			var ok bool
			port, injectToken, consumedProviderHeader, ok = r.authorizeDataRequest(
				w, request, group, addressable.Route, hostPort, hasHostPort,
			)
			if !ok {
				return
			}
			authenticated = true
			entry, err = r.resumeRoute(request.Context(), group, routeKey)
		} else if addressErr == nil && addressable != nil && addressable.State == clusterstate.WorkflowRouteReady {
			entry, err = addressable, nil
		} else {
			// Creation by logical key always requires caller authorization.
			key, header := providerCredential(request)
			if !r.authorize(w, request.Context(), group, key) {
				return
			}
			consumedProviderHeader = header
			input, inputErr := defaultSandboxInput()
			if inputErr != nil {
				err = inputErr
			} else {
				entry, err = r.reserve(request.Context(), group, routeKey, input)
				if err == nil && entry != nil {
					port, err = requestedPort(request, hostPort, hasHostPort, entry.Route.TargetPort)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					// Provider authorization creates the Route. The caller cannot
					// pre-present the capability minted by that same mutation, so the
					// Router injects it for this first trusted forward.
					injectToken, authenticated = true, true
				}
			}
		}
	}
	if err != nil || entry == nil || !r.control.CacheAuthorized(entry.ServeIdentity) {
		if err == nil {
			if entry == nil {
				err = errors.New("Route resolution returned no entry")
			} else {
				err = routeclient.ErrPermitUnavailable
			}
		}
		r.log.Warn("router data route", "group", group, "route_key", routeKey, "err", err)
		http.Error(w, "sandbox route unavailable", http.StatusServiceUnavailable)
		return
	}
	if !authenticated {
		var ok bool
		port, injectToken, consumedProviderHeader, ok = r.authorizeDataRequest(
			w, request, group, entry.Route, hostPort, hasHostPort,
		)
		if !ok {
			return
		}
	}
	r.forwardData(w, request, entry, port, injectToken, consumedProviderHeader)
}

func (r *Router) authorizeDataRequest(
	w http.ResponseWriter,
	request *http.Request,
	group string,
	route clusterstate.ReadyRoute,
	hostPort int,
	hasHostPort bool,
) (int, bool, string, bool) {
	port, err := requestedPort(request, hostPort, hasHostPort, route.TargetPort)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return 0, false, "", false
	}
	var providerErr error
	if key, header := providerCredential(request); key != "" {
		verified, err := r.verifyCaller(request.Context(), group, key)
		if verified {
			return port, true, header, true
		}
		if err != nil && r.authMode == "enforce" {
			providerErr = err
		} else if r.authMode == "log" {
			r.log.Warn("Router data request Provider authorization rejected in log mode", "group", group, "err", err)
		}
	}
	injectToken := true
	if r.dataPlaneAuth == "enforce" || r.dataPlaneAuth == "log" {
		auth := envdsign.CheckDataPlaneAuth(request, port, route.AccessToken, time.Now())
		if !auth.OK && r.dataPlaneAuth == "enforce" {
			if providerErr != nil {
				http.Error(w, "caller authorization unavailable", http.StatusServiceUnavailable)
				return 0, false, "", false
			}
			http.Error(w, "invalid access token", http.StatusUnauthorized)
			return 0, false, "", false
		}
		if auth.OK {
			injectToken = !auth.Signed
		}
	}
	return port, injectToken, "", true
}

func (r *Router) resumeRoute(ctx context.Context, group, routeKey string) (*routeEntry, error) {
	minimum, releaseRevision := r.beginRouteRevision(group, routeKey)
	defer releaseRevision()
	result, err := r.control.ResumeSandbox(ctx, group, routeKey, minimum)
	if err != nil || result.Response.Outcome != routeapi.MutationReady || result.Response.Route == nil {
		return nil, errors.Join(err, errors.New("Route resume did not become READY"))
	}
	entry := &routeEntry{
		Route: *result.Response.Route, State: clusterstate.WorkflowRouteReady, Group: group, RouteKey: routeKey,
		Revision: result.Response.RouteRevision, ServeIdentity: result.ServeIdentity,
	}
	if !r.control.CacheAuthorized(result.ServeIdentity) {
		return nil, errors.New("Router Serve Permit changed before resumed Route cache install")
	}
	if !r.rememberRoute(entry) {
		return nil, errors.New("resumed Route revision was already fenced")
	}
	return entry, nil
}

func parseDataHost(host, domain, explicitRouteKey string) (string, int, bool, error) {
	if host == "data."+domain {
		return explicitRouteKey, 0, false, nil
	}
	suffix := "." + domain
	if !strings.HasSuffix(host, suffix) {
		return "", 0, false, errors.New("invalid sandbox host")
	}
	subdomain := strings.TrimSuffix(host, suffix)
	index := strings.IndexByte(subdomain, '-')
	if index <= 0 || index+1 >= len(subdomain) {
		return "", 0, false, errors.New("invalid sandbox host")
	}
	port, err := strconv.Atoi(subdomain[:index])
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, false, errors.New("invalid sandbox host")
	}
	routeKey := subdomain[index+1:]
	if explicitRouteKey != "" && explicitRouteKey != routeKey {
		return "", 0, false, errors.New("conflicting sandbox identity")
	}
	return routeKey, port, true, nil
}

func (r *Router) forwardData(
	w http.ResponseWriter,
	request *http.Request,
	entry *routeEntry,
	port int,
	injectToken bool,
	consumedProviderHeader string,
) {
	_, releaseRevision := r.beginRouteRevision(entry.Group, entry.RouteKey)
	defer releaseRevision()
	if !r.control.CacheAuthorized(entry.ServeIdentity) {
		http.Error(w, "Router Serve Permit expired", http.StatusServiceUnavailable)
		return
	}
	route := entry.Route
	host := fmt.Sprintf("%d-%s.%s", port, route.SandboxID, r.domain)
	request.Host = host
	connectRequest := proxypkg.SandboxConnectRequest{
		RouteRequest: proxypkg.RouteRequest{
			SandboxID: route.SandboxID, Port: port, ExpectedNodeID: route.NodeID,
			ExpectedNodeEpoch: route.NodeEpoch, ExpectedRegistryGeneration: route.RegistryGeneration,
			ExpectedBindingDigest: route.BindingDigest,
		},
		AccessToken: route.AccessToken,
	}
	backend, reader, response, err := r.dialSandboxConnect(request.Context(), route.DataEndpoint, connectRequest)
	if err != nil {
		r.evictRoute(entry.Group, entry.RouteKey)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	if response.StatusCode != http.StatusOK {
		backend.Close()
		kind := response.Header.Get(proxypkg.HeaderProxyError)
		if kind != "" {
			w.Header().Set(proxypkg.HeaderProxyError, kind)
		}
		if proxyErrorRequiresNewerRoute(kind) {
			r.rejectStaleRoute(entry)
			// Refresh the Route key under the new revision fence, but never replay
			// the failed data request against a different execution.
			_, _ = r.resolveRoute(request.Context(), entry.Group, entry.RouteKey)
		} else if staleProxyError(kind) {
			r.evictRoute(entry.Group, entry.RouteKey)
		}
		http.Error(w, "connect refused by node", response.StatusCode)
		return
	}
	if request.Method == http.MethodConnect {
		proxypkg.TunnelBuffered(w, request, backend, reader)
		return
	}
	defer backend.Close()
	response, err = proxypkg.ForwardHTTPOnce(request, backend, reader, func(next *http.Request) {
		prepareDataBackendRequest(next, host, route.AccessToken, injectToken, consumedProviderHeader)
	})
	if err != nil {
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}
	defer response.Body.Close()
	proxypkg.WriteHTTPResponse(w, response)
}

func prepareDataBackendRequest(
	request *http.Request,
	host string,
	accessToken string,
	injectToken bool,
	consumedProviderHeader string,
) {
	request.Host = host
	// X-API-KEY is Router-reserved and must never reach tenant code, including
	// when Provider verification is unavailable and access-token auth succeeds.
	request.Header.Del(HeaderAPIKey)
	if consumedProviderHeader != "" && !strings.EqualFold(consumedProviderHeader, HeaderAPIKey) {
		request.Header.Del(consumedProviderHeader)
	}
	if injectToken {
		request.Header.Set(HeaderAccessTok, accessToken)
	} else {
		request.Header.Del(HeaderAccessTok)
	}
}

func (r *Router) dialSandboxConnect(
	ctx context.Context,
	endpoint string,
	request proxypkg.SandboxConnectRequest,
) (net.Conn, *bufio.Reader, *http.Response, error) {
	if r.nodeTLS != nil {
		return proxypkg.DialSandboxConnectTLS(ctx, endpoint, request, r.nodeTLS)
	}
	return proxypkg.DialSandboxConnect(ctx, "tcp", endpoint, request)
}

func (r *Router) resolveRoute(ctx context.Context, group, routeKey string) (*routeEntry, error) {
	if cached := r.cachedRoute(group, routeKey); cached != nil {
		return cached, nil
	}
	minimum, releaseRevision := r.beginRouteRevision(group, routeKey)
	defer releaseRevision()
	result, err := r.control.ReadRoute(ctx, group, routeKey, minimum)
	if err != nil || result.Response.Outcome != routeapi.ReadReady || result.Response.Route == nil {
		return nil, errors.New("Route is not READY")
	}
	entry := &routeEntry{
		Route: *result.Response.Route, State: clusterstate.WorkflowRouteReady, Group: group, RouteKey: routeKey,
		Revision: result.Response.RouteRevision, ServeIdentity: result.ServeIdentity,
	}
	if !r.control.CacheAuthorized(entry.ServeIdentity) {
		return nil, errors.New("Route serveIdentity is no longer permitted")
	}
	if !r.rememberRoute(entry) {
		return nil, errors.New("Route revision was already fenced")
	}
	return entry, nil
}

func (r *Router) resolveControlRoute(ctx context.Context, group, routeKey string) (*routeEntry, error) {
	if cached := r.cachedRoute(group, routeKey); cached != nil {
		return cached, nil
	}
	minimum, releaseRevision := r.beginRouteRevision(group, routeKey)
	defer releaseRevision()
	result, err := r.control.ReadAddressableRoute(ctx, group, routeKey, minimum)
	if err != nil {
		return nil, fmt.Errorf("read addressable Route: %w", err)
	}
	if result.Response.Outcome == routeapi.ReadNotFound {
		return nil, errRouteNotFound
	}
	if result.Response.Outcome != routeapi.ReadReady || result.Response.Route == nil {
		return nil, errors.New("Route has no addressable execution")
	}
	entry := &routeEntry{
		Route: *result.Response.Route, State: result.Response.State, Group: group, RouteKey: routeKey,
		Revision: result.Response.RouteRevision, ServeIdentity: result.ServeIdentity,
	}
	if !r.control.CacheAuthorized(entry.ServeIdentity) {
		return nil, errors.New("Route serveIdentity is no longer permitted")
	}
	if result.Response.State == clusterstate.WorkflowRouteReady {
		if !r.rememberRoute(entry) {
			return nil, errors.New("Route revision was already fenced")
		}
	} else if !r.routeRevisionCurrent(entry) {
		return nil, errors.New("addressable Route revision was already fenced")
	}
	return entry, nil
}

func (r *Router) rememberRoute(entry *routeEntry) bool {
	if entry == nil || entry.Group == "" || entry.RouteKey == "" || entry.Route.SandboxID == "" {
		return false
	}
	copy := *entry
	if copy.State == "" {
		copy.State = clusterstate.WorkflowRouteReady
	}
	if copy.State != clusterstate.WorkflowRouteReady {
		return false
	}
	copy.CachedAt, copy.LastUsed = time.Now(), time.Now()
	key := routeKeyID(copy.Group, copy.RouteKey)
	r.cacheMu.Lock()
	floor := r.routeRevisionFloorLocked(key, copy.CachedAt)
	if copy.Revision < floor.Revision {
		r.cacheMu.Unlock()
		return false
	}
	r.routes[key] = &copy
	if copy.Revision > floor.Revision {
		floor.Revision = copy.Revision
	}
	floor.InactiveUntil = time.Time{}
	r.minimumRevisions[key] = floor
	r.cacheMu.Unlock()
	return true
}

func (r *Router) routeRevisionCurrent(entry *routeEntry) bool {
	if entry == nil {
		return false
	}
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	return entry.Revision >= r.routeRevisionFloorLocked(
		routeKeyID(entry.Group, entry.RouteKey), time.Now(),
	).Revision
}

func (r *Router) minimumRouteRevision(group, routeKey string) uint64 {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	return r.routeRevisionFloorLocked(routeKeyID(group, routeKey), time.Now()).Revision
}

func (r *Router) beginRouteRevision(group, routeKey string) (uint64, func()) {
	key := routeKeyID(group, routeKey)
	r.cacheMu.Lock()
	floor := r.routeRevisionFloorLocked(key, time.Now())
	r.routeRevisionRefs[key]++
	minimum := floor.Revision
	if floor.Revision != 0 {
		floor.InactiveUntil = time.Time{}
		r.minimumRevisions[key] = floor
	}
	r.cacheMu.Unlock()

	var once sync.Once
	return minimum, func() {
		once.Do(func() {
			r.cacheMu.Lock()
			if r.routeRevisionRefs[key] <= 1 {
				delete(r.routeRevisionRefs, key)
				r.releaseInactiveRouteRevisionLocked(key, time.Now())
			} else {
				r.routeRevisionRefs[key]--
			}
			r.cacheMu.Unlock()
		})
	}
}

func (r *Router) releaseInactiveRouteRevisionLocked(key string, now time.Time) {
	if r.routes[key] != nil || r.routeRevisionRefs[key] != 0 {
		return
	}
	floor, found := r.minimumRevisions[key]
	if !found {
		return
	}
	floor.InactiveUntil = now.Add(r.routeTTL)
	r.minimumRevisions[key] = floor
}

func (r *Router) routeRevisionFloorLocked(key string, now time.Time) routeRevisionFloor {
	floor, found := r.minimumRevisions[key]
	if found && r.routes[key] == nil && r.routeRevisionRefs[key] == 0 &&
		!floor.InactiveUntil.IsZero() && !now.Before(floor.InactiveUntil) {
		delete(r.minimumRevisions, key)
		return routeRevisionFloor{}
	}
	return floor
}

func (r *Router) rejectStaleRoute(entry *routeEntry) {
	if entry == nil {
		return
	}
	minimum := entry.Revision
	if minimum < ^uint64(0) {
		minimum++
	}
	keyID := routeKeyID(entry.Group, entry.RouteKey)
	r.cacheMu.Lock()
	floor := r.routeRevisionFloorLocked(keyID, time.Now())
	delete(r.routes, keyID)
	if minimum > floor.Revision {
		floor.Revision = minimum
	}
	floor.InactiveUntil = time.Time{}
	r.minimumRevisions[keyID] = floor
	r.releaseInactiveRouteRevisionLocked(keyID, time.Now())
	r.cacheMu.Unlock()
}

func (r *Router) cachedRoute(group, routeKey string) *routeEntry {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	return r.cachedRouteLocked(routeKeyID(group, routeKey), time.Now())
}

func (r *Router) cachedRouteLocked(key string, now time.Time) *routeEntry {
	entry := r.routes[key]
	if entry == nil {
		return nil
	}
	if !r.control.CacheAuthorized(entry.ServeIdentity) || now.Sub(entry.CachedAt) > r.routeTTL ||
		now.Sub(entry.LastUsed) > r.routeIdleTimeout {
		delete(r.routes, key)
		r.releaseInactiveRouteRevisionLocked(key, now)
		return nil
	}
	entry.LastUsed = now
	copy := *entry
	return &copy
}

func (r *Router) evictRoute(group, routeKey string) {
	key := routeKeyID(group, routeKey)
	r.cacheMu.Lock()
	delete(r.routes, key)
	r.releaseInactiveRouteRevisionLocked(key, time.Now())
	r.cacheMu.Unlock()
}

func (r *Router) rememberBuild(entry *buildEntry) {
	copy := *entry
	copy.CachedAt = time.Now()
	r.cacheMu.Lock()
	r.builds[buildID(entry.Group, entry.Build.BuildID)] = &copy
	r.cacheMu.Unlock()
}

func (r *Router) cachedBuild(group, id string) *buildEntry {
	key := buildID(group, id)
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()
	entry := r.builds[key]
	if entry == nil {
		return nil
	}
	if !r.control.CacheAuthorized(entry.ServeIdentity) || time.Since(entry.CachedAt) > time.Hour {
		delete(r.builds, key)
		return nil
	}
	copy := *entry
	return &copy
}

func (r *Router) evictBuild(group, id string) {
	r.cacheMu.Lock()
	delete(r.builds, buildID(group, id))
	r.cacheMu.Unlock()
}

func (r *Router) authorizedGroup(w http.ResponseWriter, request *http.Request) (string, bool) {
	group := request.Header.Get(HeaderGroup)
	if group == "" {
		http.Error(w, HeaderGroup+" required", http.StatusBadRequest)
		return "", false
	}
	return group, r.authorize(w, request.Context(), group, apiKey(request))
}

func (r *Router) authorize(w http.ResponseWriter, ctx context.Context, group, key string) bool {
	if r.authMode == "off" {
		return true
	}
	ok, err := r.verifyCaller(ctx, group, key)
	if err != nil {
		http.Error(w, "caller authorization unavailable", http.StatusServiceUnavailable)
		return false
	}
	if ok {
		return true
	}
	if r.authMode == "log" {
		r.log.Warn("Router caller authorization rejected in log mode", "group", group)
		return true
	}
	http.Error(w, "forbidden", http.StatusForbidden)
	return false
}

func (r *Router) verifyCaller(ctx context.Context, group, key string) (bool, error) {
	cacheKey := group + "\x00" + key
	r.authMu.Lock()
	deadline, cached := r.authOK[cacheKey]
	r.authMu.Unlock()
	if cached && time.Now().Before(deadline) {
		return true, nil
	}
	ok, err := r.authorizer.Verify(ctx, group, key)
	if err != nil {
		return false, err
	}
	if ok {
		r.authMu.Lock()
		r.authOK[cacheKey] = time.Now().Add(r.authTTL)
		r.authMu.Unlock()
		return true, nil
	}
	return false, nil
}

func (r *Router) RunCleanup(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r.cleanupCaches(now)
		}
	}
}

func (r *Router) cleanupCaches(now time.Time) {
	r.authMu.Lock()
	for key, deadline := range r.authOK {
		if !now.Before(deadline) {
			delete(r.authOK, key)
		}
	}
	r.authMu.Unlock()
	r.cacheMu.Lock()
	for key, entry := range r.routes {
		if !r.control.CacheAuthorized(entry.ServeIdentity) || now.Sub(entry.CachedAt) > r.routeTTL ||
			now.Sub(entry.LastUsed) > r.routeIdleTimeout {
			delete(r.routes, key)
			r.releaseInactiveRouteRevisionLocked(key, now)
		}
	}
	for key, floor := range r.minimumRevisions {
		if r.routes[key] == nil && r.routeRevisionRefs[key] == 0 &&
			!floor.InactiveUntil.IsZero() && !now.Before(floor.InactiveUntil) {
			delete(r.minimumRevisions, key)
		}
	}
	for key, entry := range r.builds {
		if !r.control.CacheAuthorized(entry.ServeIdentity) || now.Sub(entry.CachedAt) > time.Hour {
			delete(r.builds, key)
		}
	}
	r.cacheMu.Unlock()
}

func apiKey(request *http.Request) string {
	key, _ := providerCredential(request)
	return key
}

func providerCredential(request *http.Request) (string, string) {
	if key := request.Header.Get(HeaderAPIKey); key != "" {
		return key, HeaderAPIKey
	}
	if auth := request.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[len("bearer "):]), "Authorization"
	}
	return "", ""
}

func requestedPort(request *http.Request, hostPort int, hasHostPort bool, targetPort int) (int, error) {
	port := hostPort
	if raw := request.Header.Get(proxypkg.HeaderSandboxPort); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 || hasHostPort && value != hostPort {
			return 0, errors.New("conflicting or invalid sandbox port")
		}
		port, hasHostPort = value, true
	}
	if request.Method == http.MethodConnect {
		target := request.URL.Host
		if target == "" {
			target = request.Host
		}
		if target != "" {
			_, raw, err := net.SplitHostPort(target)
			value, parseErr := strconv.Atoi(raw)
			if err != nil || parseErr != nil || value <= 0 || hasHostPort && value != port {
				return 0, errors.New("conflicting or invalid CONNECT port")
			}
			port, hasHostPort = value, true
		}
	}
	if targetPort > 0 {
		if hasHostPort && port != targetPort {
			return 0, errors.New("target port mismatch")
		}
		return targetPort, nil
	}
	if !hasHostPort {
		return 0, errors.New("target port required")
	}
	return port, nil
}

func staleProxyError(kind string) bool {
	return kind == proxypkg.ProxyErrorNotFound || kind == proxypkg.ProxyErrorUnauthorized ||
		kind == proxypkg.ProxyErrorWrongNodeEpoch || kind == proxypkg.ProxyErrorWrongBinding ||
		kind == proxypkg.ProxyErrorRouteInactive
}

func proxyErrorRequiresNewerRoute(kind string) bool {
	return kind == proxypkg.ProxyErrorNotFound || kind == proxypkg.ProxyErrorWrongNodeEpoch ||
		kind == proxypkg.ProxyErrorWrongBinding || kind == proxypkg.ProxyErrorRouteInactive
}

func pathObjectID(path, marker string) string {
	index := strings.Index(path, marker)
	if index < 0 {
		return ""
	}
	value := path[index+len(marker):]
	if slash := strings.IndexByte(value, '/'); slash >= 0 {
		value = value[:slash]
	}
	return value
}

func escapedPathObjectID(path, marker string) (string, bool) {
	encoded := pathObjectID(path, marker)
	if encoded == "" {
		return "", false
	}
	decoded, err := url.PathUnescape(encoded)
	if err != nil || url.PathEscape(decoded) != encoded {
		return "", false
	}
	return decoded, true
}

func escapedPathObjectIDExact(path, marker string) (string, bool) {
	index := strings.Index(path, marker)
	if index < 0 {
		return "", false
	}
	encoded := path[index+len(marker):]
	if encoded == "" || strings.Contains(encoded, "/") {
		return "", false
	}
	decoded, err := url.PathUnescape(encoded)
	if err != nil || url.PathEscape(decoded) != encoded {
		return "", false
	}
	return decoded, true
}

func escapedSandboxAction(path, action string) (string, bool) {
	if action == "" {
		return "", false
	}
	for _, prefix := range []string{"/sandboxes/", "/v2/sandboxes/"} {
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		value := strings.TrimPrefix(path, prefix)
		suffix := "/" + action
		if !strings.HasSuffix(value, suffix) {
			return "", false
		}
		encoded := strings.TrimSuffix(value, suffix)
		if encoded == "" || strings.Contains(encoded, "/") {
			return "", false
		}
		decoded, err := url.PathUnescape(encoded)
		if err != nil || url.PathEscape(decoded) != encoded {
			return "", false
		}
		return decoded, true
	}
	return "", false
}

func validRouteKey(routeKey string) bool {
	if routeKey == "" || routeKey == "." || routeKey == ".." || !utf8.ValidString(routeKey) {
		return false
	}
	for _, value := range routeKey {
		if value < 0x20 || value == 0x7f {
			return false
		}
	}
	return true
}

func rewritePathObjectID(path, marker, objectID string) string {
	index := strings.Index(path, marker)
	if index < 0 {
		return path
	}
	start := index + len(marker)
	end := len(path)
	if slash := strings.IndexByte(path[start:], '/'); slash >= 0 {
		end = start + slash
	}
	return path[:start] + url.PathEscape(objectID) + path[end:]
}

func canonicalStrings(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value != "" {
			set[value] = struct{}{}
		}
	}
	result := make([]string, 0, len(set))
	for value := range set {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func nonEmpty(value string) []string {
	if value == "" {
		return nil
	}
	return []string{value}
}

func randomID() string {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(value[:])
}

func routeKeyID(group, routeKey string) string { return group + "\x00" + routeKey }
func buildID(group, id string) string          { return group + "\x00" + id }
