package registry

import (
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// RouteIdentity is common to the two explicit RouteLink v2 projections.
type RouteIdentity struct {
	SandboxID     string           `json:"sandbox_id"`
	NodeSandboxID string           `json:"node_sandbox_id"`
	Group         string           `json:"group"`
	RouteKey      string           `json:"route_key"`
	NodeID        string           `json:"node_id"`
	Profile       string           `json:"profile"`
	TemplateID    string           `json:"template_id"`
	State         string           `json:"state"`
	RouteRevision metadata.Version `json:"route_revision"`
}

// ControlRoute follows full object APIKey authentication. It exposes public
// Create tokens but neither tenant roots nor the data-plane signing root.
type ControlRoute struct {
	RouteIdentity
	APIEndpoint        string `json:"api_endpoint"`
	EnvdAccessToken    string `json:"envd_access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken string `json:"forward_access_token"`
}

// DataRoute is a protected Router-only projection used to verify data tokens and
// signed file URLs before any wake. Those URLs may omit X-Access-Token. It does
// not grant control access and carries no APISecret or Node control endpoint.
type DataRoute struct {
	RouteIdentity
	DataEndpoint       string `json:"data_endpoint"`
	StableID           string `json:"stable_id"`
	ServiceSecret      string `json:"service_secret"`
	EnvdAccessToken    string `json:"envd_access_token,omitempty"`
	TrafficAccessToken string `json:"traffic_access_token,omitempty"`
	ForwardAccessToken string `json:"forward_access_token"`
	TargetPort         int    `json:"target_port,omitempty"`
}

func routeIdentity(route RouteResolve) RouteIdentity {
	return RouteIdentity{SandboxID: route.SandboxID, NodeSandboxID: route.NodeSandboxID, Group: route.Group, RouteKey: route.RouteKey, NodeID: route.NodeID, Profile: route.Profile, TemplateID: route.TemplateID, State: route.State, RouteRevision: route.RouteRevision}
}
func controlRoute(route RouteResolve) ControlRoute {
	return ControlRoute{RouteIdentity: routeIdentity(route), APIEndpoint: route.APIEndpoint, EnvdAccessToken: route.EnvdAccessToken, TrafficAccessToken: route.TrafficAccessToken, ForwardAccessToken: route.ForwardAccessToken}
}
func dataRoute(route RouteResolve) DataRoute {
	return DataRoute{RouteIdentity: routeIdentity(route), DataEndpoint: route.DataEndpoint, StableID: route.StableID, ServiceSecret: route.ServiceSecret, EnvdAccessToken: route.EnvdAccessToken, TrafficAccessToken: route.TrafficAccessToken, ForwardAccessToken: route.ForwardAccessToken, TargetPort: route.TargetPort}
}

func (r *Registry) serveControlRoute(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	route, found, err := r.ResolveSID(req.Context(), q.Get("group"), q.Get("route_key"), q.Get("sid"))
	if err != nil {
		http.Error(w, err.Error(), routeLinkStatus(err))
		return
	}
	if !found {
		http.Error(w, "sandbox not found", http.StatusNotFound)
		return
	}
	if err := r.authenticateSandboxAPIKey(req.Context(), &SandboxRecord{Group: route.Group, APISecret: route.APISecret, APISecretFingerprint: route.APISecretFingerprint}, req.Header.Get("X-API-KEY")); err != nil {
		http.Error(w, err.Error(), routeLinkStatus(err))
		return
	}
	writeJSON(w, controlRoute(*route))
}

func writeReserveResponse(w http.ResponseWriter, op ReserveOperation, result *ReserveResult) {
	var route any = controlRoute(result.Route)
	if op == ReserveData {
		route = dataRoute(result.Route)
	}
	writeJSON(w, struct {
		Route       any                          `json:"route"`
		Connect     *routesync.ConnectResult     `json:"connect,omitempty"`
		ExecSession *routesync.ExecSessionResult `json:"exec_session,omitempty"`
	}{Route: route, Connect: result.Connect, ExecSession: result.ExecSession})
}
