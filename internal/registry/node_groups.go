package registry

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const PlacerLinkNodeGroupPath = "/placer-link/node-group"

// ImportLease authorizes a new forwarding attempt, not execution on the Node.
// The Node still authenticates the full business key and performs its own CAS.
type ImportLease struct {
	SourceID string `json:"source_id"`
	OwnerID  string `json:"owner_id"`
	RunID    string `json:"run_id"`
	Term     uint64 `json:"term"`
}

type NodeGroupRequest struct {
	NodeID string                 `json:"node_id"`
	Kind   string                 `json:"kind"`
	Group  routesync.GroupCommand `json:"group"`
	Lease  ImportLease            `json:"lease"`
}

type NodeGroupResponse struct {
	NodeID       string                `json:"node_id"`
	Registration metadata.Registration `json:"registration"`
	Ack          *routesync.CmdAck     `json:"ack"`
}

func (r *Registry) serveNodeGroup(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in NodeGroupRequest
	if err := decodeOptionalReserveBody(req.Body, 1<<20, &in); err != nil {
		http.Error(w, "invalid Group relay request", http.StatusBadRequest)
		return
	}
	switch in.Kind {
	case routesync.CmdGroupGet, routesync.CmdGroupList, routesync.CmdGroupPut, routesync.CmdGroupDelete:
	default:
		http.Error(w, "invalid Group command", http.StatusBadRequest)
		return
	}
	if in.NodeID == "" || in.Lease.SourceID == "" || in.Lease.OwnerID == "" || in.Lease.RunID == "" || in.Lease.Term == 0 {
		http.Error(w, "node and import source lease are required", http.StatusBadRequest)
		return
	}
	profile, err := r.stores.nodes.ReadNodeProfile(req.Context(), in.NodeID)
	if err != nil || !profile.Exists || profile.Reaping || r.nodeOwner == nil {
		http.Error(w, "Node unavailable", http.StatusServiceUnavailable)
		return
	}
	registration := profile.Registration
	cmd := &routesync.Command{CmdID: newID(), Kind: in.Kind, Group: &in.Group}
	if err := routesync.ValidateMessage(&routesync.Msg{Type: routesync.TypeCommand, Cmd: cmd}); err != nil {
		http.Error(w, "Group command exceeds NodeLink budget", http.StatusRequestEntityTooLarge)
		return
	}
	// Recheck after all profile reads and encoding work, immediately before
	// forwarding. A lease can expire while a metadata read is blocked.
	valid, err := r.stores.imports.Check(req.Context(), in.Lease.SourceID, in.Lease.OwnerID, in.Lease.RunID, in.Lease.Term)
	if err != nil {
		http.Error(w, "cannot check import source lease", http.StatusServiceUnavailable)
		return
	}
	if !valid {
		http.Error(w, "stale import source lease", http.StatusConflict)
		return
	}
	ack, err := r.nodeOwner.SendCommandAndWait(withNodeRegistration(req.Context(), registration), in.NodeID, cmd, 5*time.Second)
	if err != nil || ack == nil || ack.CmdID != cmd.CmdID {
		http.Error(w, "Group result unknown; inspect the same Node", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(NodeGroupResponse{NodeID: in.NodeID, Registration: registration, Ack: ack})
}
