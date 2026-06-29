package registry

import (
	"context"
	"encoding/json"
	"net/http"
)

// route_link paths. Routers and operator tools dial this link for group-scoped
// route/build operations, API-key verification, and import/export.
const (
	RouteLinkReservePath      = "/route-link/reserve"       // POST ?group=&route_key= -> ReserveResult
	RouteLinkRoutePath        = "/route-link/route"         // GET  ?group=&route_key=&sid= -> RouteResolve
	RouteLinkReserveBuildPath = "/route-link/reserve-build" // POST {group,resources,metadata} -> BuildReserveResult
	RouteLinkBuildPath        = "/route-link/build"         // GET  ?group=&build_id=  -> BuildReserveResult (resolve)
	RouteLinkListPath         = "/route-link/list"          // GET  ?group=            -> the group's sandbox shard
	RouteLinkVerifyKeyPath    = "/route-link/verify-key"    // GET  ?group=&api_key=   -> 200 valid / 403 invalid
)

// RouteResolve is the data-plane forwarding target the router needs for a sid
// (the hot path: client -> router -> node DataEndpoint -> guest).
type RouteResolve struct {
	SID          string `json:"sid"`
	Group        string `json:"group"`
	RouteKey     string `json:"route_key"`
	NodeID       string `json:"node_id"`
	DataEndpoint string `json:"data_endpoint"`
	AccessToken  string `json:"access_token"`
	State        string `json:"state"`
}

// ServeRouteLink mounts the router/admin-facing route_link API.
func (r *Registry) ServeRouteLink(mux *http.ServeMux) {
	mux.HandleFunc(RouteLinkReservePath, r.serveReserve)
	mux.HandleFunc(RouteLinkRoutePath, r.serveRoute)
	mux.HandleFunc(RouteLinkReserveBuildPath, r.serveReserveBuild)
	mux.HandleFunc(RouteLinkBuildPath, r.serveBuild) // resolve build_id -> node (router restart)
	mux.HandleFunc(RouteLinkListPath, r.serveList)
	mux.HandleFunc(RouteLinkVerifyKeyPath, r.serveVerifyKey)
	mux.HandleFunc(RouteLinkExportPath, r.serveExport) // operator JSONL export
	mux.HandleFunc(RouteLinkImportPath, r.serveImport) // operator JSONL import
}

// serveVerifyKey verifies an api key through the scaler-owned group provider
// view. Registry route owners do not read auth_key; they only fail over across
// ready scalers. A 403 hides both a bad key and an unknown group.
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

// ListItem is one row of a group's sandbox listing (cluster-router.md §6: list is
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
		http.Error(w, err.Error(), http.StatusBadRequest)
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
	res, err := r.ReserveSandbox(req.Context(), q.Get("group"), q.Get("route_key"), nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
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
		State: string(rec.State),
	}, true, nil
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
