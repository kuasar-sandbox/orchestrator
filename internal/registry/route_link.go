package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// route_link paths. Routers dial this link for group-scoped route/build
// operations and API-key verification.
const (
	RouteLinkReservePath      = "/route-link/reserve"       // POST ?group=&route_key=&operation=... -> ReserveResult
	RouteLinkRoutePath        = "/route-link/route"         // GET  ?group=&route_key=&sid= -> RouteResolve
	RouteLinkDeletePath       = "/route-link/delete"        // DELETE ?group=&route_key=&sid= -> node-link CmdDelete
	RouteLinkReserveBuildPath = "/route-link/reserve-build" // POST {group,build_id,template_id,profile,resources,metadata} -> BuildReserveResult
	RouteLinkBuildPath        = "/route-link/build"         // GET  ?group=&build_id=  -> BuildReserveResult (resolve)
	RouteLinkListPath         = "/route-link/list"          // GET  ?group=            -> the group's sandbox shard
	RouteLinkVerifyKeyPath    = "/route-link/verify-key"    // GET  ?group=&api_key=   -> 200 valid / 403 invalid
)

// RouteResolve is the data-plane forwarding target the router needs for a sid
// (the hot path: client -> router -> node DataEndpoint -> guest).
type RouteResolve struct {
	SandboxID              string `json:"sandbox_id"`
	NodeSandboxID          string `json:"node_sandbox_id"`
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
	RouteRevision          int64  `json:"route_revision"`
}

// SandboxReserveReq is the router-to-registry create payload. Config remains a
// map because it follows the existing placement command carrier. Cluster ingress
// admits only the request-scoped restore, credentials, and checkpoint
// namespaces into it.
type SandboxReserveReq struct {
	Config map[string]string `json:"config,omitempty"`
}

// ServeRouteLink mounts the router/admin-facing route_link API.
func (r *Registry) ServeRouteLink(mux *http.ServeMux) {
	mux.HandleFunc(RouteLinkReservePath, r.serveReserve)
	mux.HandleFunc(RouteLinkRoutePath, r.serveRoute)
	mux.HandleFunc(RouteLinkDeletePath, r.serveDelete)
	mux.HandleFunc(RouteLinkReserveBuildPath, r.serveReserveBuild)
	mux.HandleFunc(RouteLinkBuildPath, r.serveBuild) // resolve build_id -> node (router restart)
	mux.HandleFunc(RouteLinkListPath, r.serveList)
	mux.HandleFunc(RouteLinkVerifyKeyPath, r.serveVerifyKey)
}

// serveVerifyKey verifies an api key through the placer-owned group provider
// view. Registry route owners do not consult the group provider; they only fail over across
// ready placers. A 403 hides both a bad key and an unknown group.
func (r *Registry) serveVerifyKey(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	replicas, minReady, timeout := r.scalePolicy()
	ok, err := r.VerifyAPIKeyWithMinReady(req.Context(), q.Get("group"), req.Header.Get("X-API-KEY"), replicas, minReady, timeout)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable) // key provider unavailable → 503
		return
	}
	if !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ListItem is one row of a group's sandbox listing (cluster-router.md: list is
// served from the group's SandboxStore shard, no cross-group).
type ListItem struct {
	SandboxID  string `json:"sandboxID"`
	State      string `json:"state"`
	TemplateID string `json:"templateID,omitempty"`
	ClientID   string `json:"clientID,omitempty"`
}

func (r *Registry) serveList(w http.ResponseWriter, req *http.Request) {
	group := req.URL.Query().Get("group")
	if group == "" {
		http.Error(w, "group is required", http.StatusBadRequest)
		return
	}
	out := []ListItem{}
	if err := r.stores.RangeSandboxes(req.Context(), group, func(s *SandboxRecord) error {
		if s.State == StateReady || s.State == StatePaused {
			out = append(out, ListItem{SandboxID: s.SandboxID, State: string(s.State), TemplateID: s.TemplateID, ClientID: s.NodeID})
		}
		return nil
	}); err != nil {
		http.Error(w, err.Error(), routeLinkStatus(err))
		return
	}
	writeJSON(w, out)
}

func (r *Registry) serveReserveBuild(w http.ResponseWriter, req *http.Request) {
	var br BuildReserveReq
	if err := json.NewDecoder(req.Body).Decode(&br); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if br.Group == "" {
		br.Group = req.URL.Query().Get("group")
	}
	if br.Group == "" {
		http.Error(w, "group is required", http.StatusBadRequest)
		return
	}
	if !br.Profile.Valid() {
		http.Error(w, fmt.Sprintf("unknown build profile %q", br.Profile), http.StatusBadRequest)
		return
	}
	res, err := r.ReserveBuild(req.Context(), br)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, res)
}

// serveBuild resolves a build_id to its node (router restart recovery, §7.5).
func (r *Registry) serveBuild(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	res, found := r.ResolveBuild(req.Context(), q.Get("group"), q.Get("build_id"))
	if !found {
		http.Error(w, "build not found", http.StatusNotFound)
		return
	}
	writeJSON(w, res)
}

func (r *Registry) serveReserve(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	group, routeKey := q.Get("group"), q.Get("route_key")
	if group == "" || routeKey == "" {
		http.Error(w, "group and route_key are required", http.StatusBadRequest)
		return
	}
	operation := ReserveOperation(q.Get("operation"))
	if !operation.Valid() {
		http.Error(w, "operation must be create, connect, exec-session, or data", http.StatusBadRequest)
		return
	}
	port, err := reserveQueryInt(q.Get("port"), "port")
	if err != nil || port > 65535 {
		http.Error(w, "port must be an integer between 0 and 65535", http.StatusBadRequest)
		return
	}
	timeoutSeconds, err := reserveQueryInt(q.Get("timeout"), "timeout")
	if err != nil || int64(timeoutSeconds) > routesync.MaxConnectTimeoutSeconds {
		http.Error(w, fmt.Sprintf("timeout must be an integer between 0 and %d", routesync.MaxConnectTimeoutSeconds), http.StatusBadRequest)
		return
	}
	ttlSeconds, err := reserveQueryInt64(q.Get("ttl_seconds"), "ttl_seconds")
	if err != nil {
		http.Error(w, "ttl_seconds must be a non-negative integer", http.StatusBadRequest)
		return
	}
	var body SandboxReserveReq
	if operation == ReserveCreate && req.Body != nil {
		err = json.NewDecoder(req.Body).Decode(&body)
		if err != nil && !errors.Is(err, io.EOF) {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	} else if req.Body != nil {
		content, readErr := io.ReadAll(io.LimitReader(req.Body, 1))
		if readErr != nil {
			http.Error(w, readErr.Error(), http.StatusBadRequest)
			return
		}
		if len(content) != 0 {
			http.Error(w, "reserve body is only valid for create", http.StatusBadRequest)
			return
		}
	}
	for key := range body.Config {
		if key != sandboxcfg.NsRestore && key != sandboxcfg.NsCredentials && key != sandboxcfg.NsCheckpoint {
			http.Error(w, fmt.Sprintf("unsupported sandbox reserve config %q", key), http.StatusBadRequest)
			return
		}
	}
	res, err := r.ReserveSandbox(req.Context(), SandboxReserveRequest{
		Operation:         operation,
		Group:             group,
		RouteKey:          routeKey,
		ExpectedSandboxID: q.Get("sid"),
		Port:              port,
		TimeoutSeconds:    timeoutSeconds,
		TTLSeconds:        ttlSeconds,
		APIKey:            req.Header.Get("X-API-KEY"),
		AccessToken:       req.Header.Get("X-Access-Token"),
		Service:           req.Header.Get("E2b-Sandbox-Service"),
		MigrationToken:    req.Header.Get("X-Kuasar-Migration-Token"),
		Config:            body.Config,
	})
	if err != nil {
		http.Error(w, err.Error(), routeLinkStatus(err))
		return
	}
	writeJSON(w, res)
}

func reserveQueryInt(value, field string) (int, error) {
	if value == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("registry: invalid %s", field)
	}
	return n, nil
}

func reserveQueryInt64(value, field string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("registry: invalid %s", field)
	}
	return n, nil
}

func (r *Registry) serveRoute(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	rr, found, err := r.ResolveSID(req.Context(), q.Get("group"), q.Get("route_key"), q.Get("sid"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	writeJSON(w, rr)
}

func (r *Registry) serveDelete(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	deleted, err := r.DeleteSandboxRoute(req.Context(), q.Get("group"), q.Get("route_key"), q.Get("sid"))
	if err != nil {
		http.Error(w, err.Error(), routeLinkStatus(err))
		return
	}
	if !deleted {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ResolveSID maps a group-scoped (route_key, sid) pair to its data-plane
// forwarding target. The lookup is exact and quorum-backed; it does not trust
// this process's local sid index or scan the whole group.
func (r *Registry) ResolveSID(ctx context.Context, group, routeKey, sid string) (*RouteResolve, bool, error) {
	if group == "" || routeKey == "" || sid == "" {
		return nil, false, nil
	}
	rec, rev, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !found {
		return nil, found, err
	}
	if rec.SandboxID != sid {
		return nil, false, nil
	}
	if rec.State == StateReady || rec.State == StatePaused || hasRouteCredentials(rec) {
		if _, _, err := replacementCredentials(rec); err != nil {
			return nil, false, err
		}
	}
	return &RouteResolve{
		SandboxID: rec.SandboxID, NodeSandboxID: rec.NodeSandboxID,
		Group: rec.Group, RouteKey: rec.RouteKey, NodeID: rec.NodeID,
		DataEndpoint: r.nodeDataEndpoint(ctx, rec.NodeID), Profile: rec.Profile, TemplateID: rec.TemplateID,
		AuthSandboxID: rec.AuthSandboxID, APISecret: rec.APISecret,
		APISecretFingerprint: rec.APISecretFingerprint, ManifestKeyFingerprint: rec.ManifestKeyFingerprint,
		ServiceSecret: rec.ServiceSecret, EnvdAccessToken: rec.EnvdAccessToken,
		TrafficAccessToken: rec.TrafficAccessToken, ForwardAccessToken: rec.ForwardAccessToken,
		TargetPort: rec.TargetPort,
		State:      string(rec.State), RouteRevision: rev,
	}, true, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func routeLinkStatus(err error) int {
	var rejected *nodeCommandRejection
	if errors.As(err, &rejected) {
		return rejected.status
	}
	switch {
	case errors.Is(err, ErrReserveBadRequest), errors.Is(err, errInvalidSandboxConfig):
		return http.StatusBadRequest
	case errors.Is(err, ErrReserveUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrReserveForbidden):
		return http.StatusForbidden
	case errors.Is(err, ErrSandboxNotFound):
		return http.StatusNotFound
	case errors.Is(err, clusterstate.ErrQuorum):
		return http.StatusServiceUnavailable
	default:
		return http.StatusServiceUnavailable
	}
}
