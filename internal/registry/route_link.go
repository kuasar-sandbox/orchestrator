package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

// route_link paths. Routers dial this link for group-scoped route/build
// operations and API-key verification.
const (
	RouteLinkReservePath      = "/route-link/reserve"       // POST ?group=&route_key= -> ReserveResult
	RouteLinkRoutePath        = "/route-link/route"         // GET  ?group=&route_key=&sid= -> RouteResolve
	RouteLinkDeletePath       = "/route-link/delete"        // DELETE ?group=&route_key=&sid= -> node-link CmdDelete
	RouteLinkReserveBuildPath = "/route-link/reserve-build" // POST {group,build_id,template_id,profile,resources,metadata} -> BuildReserveResult
	RouteLinkBuildPath        = "/route-link/build"         // GET  ?group=&build_id=  -> BuildReserveResult (resolve)
	RouteLinkListPath         = "/route-link/list"          // GET  ?group=            -> the group's sandbox shard
	RouteLinkVerifyKeyPath    = "/route-link/verify-key"    // GET  ?group=&api_key=   -> 200 valid / 403 invalid
)

// MaxSandboxConfigBytes bounds one normalized per-sandbox config before it is
// nested in placement/route-command envelopes and base64-encoded as a shardkv
// Record.Value. The raw public control body has its own, larger limit.
const MaxSandboxConfigBytes = 512 << 10

var ErrSandboxConfigTooLarge = errors.New("registry: sandbox config too large")

// ValidateSandboxConfigSize is shared by the public router and route owner so
// Header-only requests, direct route-link calls, and group-merged effective
// configs all obey the same single-record transport budget.
func ValidateSandboxConfigSize(config map[string]string) error {
	wire, err := json.Marshal(config)
	if err != nil {
		return err
	}
	if len(wire) > MaxSandboxConfigBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrSandboxConfigTooLarge, len(wire), MaxSandboxConfigBytes)
	}
	return nil
}

// RouteResolve is the data-plane forwarding target the router needs for a sid
// (the hot path: client -> router -> node DataEndpoint -> guest).
type RouteResolve struct {
	SID                string `json:"sid"`
	Group              string `json:"group"`
	RouteKey           string `json:"route_key"`
	NodeID             string `json:"node_id"`
	DataEndpoint       string `json:"data_endpoint"`
	AccessToken        string `json:"access_token"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	TargetPort         int    `json:"target_port,omitempty"`
	State              string `json:"state"`
}

// ReserveSandboxRequest carries per-create sandbox configuration from the
// cluster router to the route owner. Config is already normalized from request
// metadata and X-Kuasar-Sandbox-* headers; the route owner validates it again at
// its own HTTP trust boundary.
type ReserveSandboxRequest struct {
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
// view. Registry route owners do not read auth_key; they only fail over across
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
			out = append(out, ListItem{SandboxID: s.SID, State: string(s.State), TemplateID: s.TemplateID, ClientID: s.NodeID})
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
	if err := ValidateSandboxConfigSize(br.Metadata); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	q := req.URL.Query()
	group, routeKey := q.Get("group"), q.Get("route_key")
	if group == "" || routeKey == "" {
		http.Error(w, "group and route_key are required", http.StatusBadRequest)
		return
	}
	var body ReserveSandboxRequest
	if req.Body != nil {
		dec := json.NewDecoder(req.Body)
		if err := dec.Decode(&body); err != nil && err != io.EOF {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		} else if err == nil {
			if err := dec.Decode(&struct{}{}); err != io.EOF {
				http.Error(w, "request body contains trailing content", http.StatusBadRequest)
				return
			}
		}
	}
	if err := ValidateSandboxConfigSize(body.Config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, err := sandboxcfg.ParseSpec(body.Config); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	res, err := r.ReserveSandbox(req.Context(), group, routeKey, body.Config)
	if err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, ErrSandboxConfigTooLarge) {
			status = http.StatusBadRequest
		}
		http.Error(w, err.Error(), status)
		return
	}
	writeJSON(w, res)
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
	rec, _, found, err := r.stores.GetSandbox(ctx, group, routeKey)
	if err != nil || !found {
		return nil, found, err
	}
	if rec.SID != sid {
		return nil, false, nil
	}
	return &RouteResolve{
		SID: rec.SID, Group: rec.Group, RouteKey: rec.RouteKey, NodeID: rec.NodeID,
		DataEndpoint: r.nodeDataEndpoint(ctx, rec.NodeID), AccessToken: rec.AccessToken,
		TrafficAccessToken: rec.TrafficAccessToken, TargetPort: rec.TargetPort,
		State: string(rec.State),
	}, true, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func routeLinkStatus(err error) int {
	switch {
	case errors.Is(err, clusterstate.ErrQuorum):
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadRequest
	}
}
