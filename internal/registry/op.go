package registry

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/apikey"
)

// Op interface paths the router (and later the scaler) dial (cluster.md §5.2).
// Phase 3 ships reserve + route resolution for the router; the scaler's view
// subscription + the resumable watch land with later phases.
const (
	OpReservePath      = "/op/reserve"       // POST ?group=&route_key= -> ReserveResult
	OpRoutePath        = "/op/route"         // GET  ?sid=              -> RouteResolve
	OpReserveBuildPath = "/op/reserve-build" // POST ?group=            -> ReserveResult (build node)
	OpGroupPath        = "/op/group"         // POST ?group=&template_ref= -> set template_ref
	OpListPath         = "/op/list"          // GET  ?group=            -> the group's sandbox shard
	OpVerifyKeyPath    = "/op/verify-key"    // GET  ?group=&api_key=   -> 200 valid / 403 invalid
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

// ServeOp mounts the op interface (reserve + route resolution) on a mux. The
// registry serves it on op.listen for router/scaler.
func (r *Registry) ServeOp(mux *http.ServeMux) {
	mux.HandleFunc(OpReservePath, r.serveReserve)
	mux.HandleFunc(OpRoutePath, r.serveRoute)
	mux.HandleFunc(OpReserveBuildPath, r.serveReserveBuild)
	mux.HandleFunc(OpGroupPath, r.serveGroup)
	mux.HandleFunc(OpListPath, r.serveList)
	mux.HandleFunc(OpVerifyKeyPath, r.serveVerifyKey)
	mux.HandleFunc(OpWatchPath, r.serveWatch)
	mux.HandleFunc(OpNodeWatchPath, r.serveNodeWatch)   // standalone scaler view (§5.2)
	mux.HandleFunc(OpGroupWatchPath, r.serveGroupWatch) // standalone scaler view (§5.2)
}

// serveVerifyKey verifies an api key against a group's manifest key (the router's
// auth check, cached for router.auth_cache_ttl — cluster-router.md §8). A 403
// hides both a bad key and an unknown group.
func (r *Registry) serveVerifyKey(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	g, found, err := r.stores.GetGroupByID(req.Context(), q.Get("group"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found || g.ManifestKey == "" {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	p, err := apikey.Parse(req.Header.Get("X-API-KEY")) // in a header, never the query (logged)
	if err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	raw, err := hex.DecodeString(g.ManifestKey)
	if err != nil || !apikey.Verify(p, raw) {
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
	out := []ListItem{}
	_ = r.stores.RangeSandboxes(req.Context(), group, func(s *SandboxRecord) error {
		if s.State == StateReady || s.State == StatePaused {
			out = append(out, ListItem{SandboxID: s.SID, State: string(s.State), TemplateID: s.TemplateID, ClientID: s.NodeID})
		}
		return nil
	})
	writeJSON(w, out)
}

func (r *Registry) serveReserveBuild(w http.ResponseWriter, req *http.Request) {
	res, err := r.ReserveBuild(req.Context(), req.URL.Query().Get("group"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, res)
}

func (r *Registry) serveGroup(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	if err := r.SetGroupTemplate(req.Context(), q.Get("group"), q.Get("template_ref")); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
	rr, found, err := r.ResolveSID(req.Context(), req.URL.Query().Get("sid"))
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

// ResolveSID maps a sid to its data-plane forwarding target (router hot path),
// via the sid index built from the route stream.
func (r *Registry) ResolveSID(ctx context.Context, sid string) (*RouteResolve, bool, error) {
	r.mu.Lock()
	kp, ok := r.sidKeys[sid]
	r.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	rec, _, found, err := r.stores.GetSandbox(ctx, kp[0], kp[1])
	if err != nil || !found {
		return nil, found, err
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
