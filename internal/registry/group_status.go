package registry

import (
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	requestplacement "github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// GroupRegistrationStatus identifies an observed registration. Fence is an
// opaque comparison token, not a credential or authority to send commands.
type GroupRegistrationStatus struct {
	NodeID    string           `json:"node_id"`
	LinkOwner string           `json:"link_owner"`
	Fence     metadata.Version `json:"fence"`
}

// GroupStatus contains only a safe projection of one published Group candidate.
// It never includes root material, the Node's private context, resume tokens or
// API/data endpoints. Eligibility describes default/explicit-template Create
// selection without request constraints; it does not reserve Node admission.
type GroupStatus struct {
	Group             string                   `json:"group"`
	NodeID            string                   `json:"node_id"`
	StorageReadable   bool                     `json:"storage_readable"`
	Published         bool                     `json:"published"`
	Registration      *GroupRegistrationStatus `json:"registration,omitempty"`
	GroupVersion      int64                    `json:"group_version,omitempty"`
	Eligible          bool                     `json:"eligible"`
	Reason            string                   `json:"reason"`
	RuntimeMatch      bool                     `json:"runtime_match"`
	State             metadata.NodeState       `json:"state,omitempty"`
	Zone              string                   `json:"zone,omitempty"`
	Draining          bool                     `json:"draining,omitempty"`
	LastHeartbeatUnix int64                    `json:"last_heartbeat_unix,omitempty"`
}

// serveGroupStatus uses the same full-key authority and completed publication
// as selection. Incomplete or superseded snapshots cannot satisfy this read.
// It is intentionally a bounded, requested-node read rather than an inventory
// endpoint available to every Group key holder.
func (r *Registry) serveGroupStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := req.URL.Query()
	group, nodeID := q.Get("group"), q.Get("node_id")
	if nodegroup.ValidateName(group) != nil || nodeID == "" || len(nodeID) > 4096 || strings.IndexByte(nodeID, 0) >= 0 {
		http.Error(w, "valid group and node_id are required", http.StatusBadRequest)
		return
	}
	var template types.TemplateID
	if q.Has("template_id") {
		var err error
		template, err = types.ParseTemplateID(q.Get("template_id"))
		if err != nil {
			http.Error(w, "invalid template_id", http.StatusBadRequest)
			return
		}
	}
	key := req.Header.Get("X-API-KEY")
	ok, err := r.VerifyGroupAPIKey(req.Context(), group, key)
	if err != nil {
		http.Error(w, "metadata read unavailable", http.StatusServiceUnavailable)
		return
	}
	if !ok {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	selected, err := r.stores.groups.ReadGroupSupport(req.Context(), group, nodeID)
	if err != nil {
		http.Error(w, "metadata read unavailable", http.StatusServiceUnavailable)
		return
	}
	out := GroupStatus{Group: group, NodeID: nodeID, StorageReadable: true, Reason: "support_unavailable"}
	if !selected.Support.Exists {
		writeJSON(w, out)
		return
	}
	// A key retained for another Node or an existing object does not authorize
	// inspection of support published under a different root in the same Group.
	parsed, _ := apikey.Parse(key) // VerifyGroupAPIKey already validated the key.
	authorized := false
	for _, entry := range selected.Support.Value.Entries {
		auth, err := r.stores.groups.ReadGroupAuth(req.Context(), group, entry.AuthRef)
		if err != nil {
			http.Error(w, "metadata read unavailable", http.StatusServiceUnavailable)
			return
		}
		raw, decodeErr := hex.DecodeString(auth.Value.APISecret)
		valid := auth.Exists && decodeErr == nil && len(raw) == 32 && apikey.Verify(parsed, raw)
		clear(raw)
		if valid {
			authorized = true
			break
		}
	}
	if !authorized {
		writeJSON(w, out)
		return
	}
	reg, node := selected.Node.Registration, selected.Node.Value
	out.Published = true
	out.Registration = &GroupRegistrationStatus{NodeID: reg.NodeID, LinkOwner: reg.LinkOwner, Fence: reg.Fence}
	out.GroupVersion = selected.Support.Value.GroupVersion
	out.State, out.Zone, out.Draining = node.State, node.Zone, node.Draining
	out.LastHeartbeatUnix = node.LastHeartbeatUnix
	if !q.Has("template_id") && selected.Support.Value.DefaultContext != nil {
		template, err = types.ParseTemplateID(selected.Support.Value.DefaultContext.TemplateID)
		if err != nil {
			http.Error(w, "published template unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	hasTemplate := q.Has("template_id") || selected.Support.Value.DefaultContext != nil
	if hasTemplate {
		out.RuntimeMatch = requestplacement.RuntimeMatches(template, node.Labels)
	}
	r.mu.Lock()
	zoneMax := r.placementZoneMax
	r.mu.Unlock()
	switch {
	case !selected.Node.Exists || selected.Node.Reaping || node.State == metadata.NodeDead || node.State == metadata.NodeDrained:
		out.Reason = "node_unavailable"
	case node.Draining:
		out.Reason = "draining"
	case placementZoneRank(node.Zone) > placementZoneRank(zoneMax):
		out.Reason = "zone_rejected"
	case !hasTemplate:
		out.Reason = "default_template_missing"
	case !out.RuntimeMatch:
		out.Reason = "runtime_unavailable"
	default:
		out.Eligible, out.Reason = true, "eligible"
	}
	writeJSON(w, out)
}
