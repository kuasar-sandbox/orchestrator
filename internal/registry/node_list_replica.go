package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	clusterstate "github.com/kuasar-sandbox/sandbox-orchestrator/internal/cluster"
)

const NodeListReplicaRPCPath = "/internal/registry-member/node-list-replica"

type NodeListReplica = clusterstate.NodeListReplica

type nodeListReplicaRequest struct {
	Op     string                     `json:"op"`
	NodeID string                     `json:"node_id,omitempty"`
	Entry  clusterstate.NodeListEntry `json:"entry,omitempty"`
	Ballot clusterstate.Ballot        `json:"ballot,omitempty"`
}

type nodeListReplicaResponse struct {
	Entry   clusterstate.NodeListEntry   `json:"entry,omitempty"`
	Entries []clusterstate.NodeListEntry `json:"entries,omitempty"`
	Found   bool                         `json:"found,omitempty"`
	OK      bool                         `json:"ok,omitempty"`
	Ballot  clusterstate.Ballot          `json:"ballot,omitempty"`
	Error   string                       `json:"error,omitempty"`
}

func ServeNodeListReplica(w http.ResponseWriter, req *http.Request, replica NodeListReplica) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in nodeListReplicaRequest
	if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var out nodeListReplicaResponse
	var err error
	switch in.Op {
	case "read":
		out.Entry, out.Found, err = replica.Read(req.Context(), in.NodeID)
	case "prepare":
		out.Entry, out.Found, out.OK, err = replica.Prepare(req.Context(), in.NodeID, in.Ballot)
	case "accept":
		out.OK, err = replica.Accept(req.Context(), in.NodeID, in.Entry, in.Ballot)
	case "repair":
		err = replica.Repair(req.Context(), in.NodeID, in.Entry)
		out.OK = err == nil
	case "max_ballot":
		out.Ballot, err = replica.MaxBallot(req.Context(), in.NodeID)
	case "list":
		out.Entries, err = replica.List(req.Context())
		out.OK = err == nil
	default:
		err = fmt.Errorf("registry: unknown node_list replica op %q", in.Op)
	}
	if err == nil && in.Op != "prepare" && in.Op != "accept" {
		out.OK = true
	}
	if err != nil {
		out.Error = err.Error()
		w.WriteHeader(http.StatusConflict)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(&out)
}

type HTTPNodeListReplica struct {
	endpoint string
	client   *http.Client
}

func NewHTTPNodeListReplica(endpoint string, client *http.Client) *HTTPNodeListReplica {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPNodeListReplica{endpoint: strings.TrimRight(endpoint, "/"), client: client}
}

func (r *HTTPNodeListReplica) Read(ctx context.Context, nodeID string) (clusterstate.NodeListEntry, bool, error) {
	out, err := r.call(ctx, nodeListReplicaRequest{Op: "read", NodeID: nodeID})
	return out.Entry, out.Found, err
}

func (r *HTTPNodeListReplica) Prepare(ctx context.Context, nodeID string, ballot clusterstate.Ballot) (clusterstate.NodeListEntry, bool, bool, error) {
	out, err := r.call(ctx, nodeListReplicaRequest{Op: "prepare", NodeID: nodeID, Ballot: ballot})
	return out.Entry, out.Found, out.OK, err
}

func (r *HTTPNodeListReplica) Accept(ctx context.Context, nodeID string, entry clusterstate.NodeListEntry, ballot clusterstate.Ballot) (bool, error) {
	out, err := r.call(ctx, nodeListReplicaRequest{Op: "accept", NodeID: nodeID, Entry: entry, Ballot: ballot})
	return out.OK, err
}

func (r *HTTPNodeListReplica) Repair(ctx context.Context, nodeID string, entry clusterstate.NodeListEntry) error {
	_, err := r.call(ctx, nodeListReplicaRequest{Op: "repair", NodeID: nodeID, Entry: entry})
	return err
}

func (r *HTTPNodeListReplica) MaxBallot(ctx context.Context, nodeID string) (clusterstate.Ballot, error) {
	out, err := r.call(ctx, nodeListReplicaRequest{Op: "max_ballot", NodeID: nodeID})
	return out.Ballot, err
}

func (r *HTTPNodeListReplica) List(ctx context.Context) ([]clusterstate.NodeListEntry, error) {
	out, err := r.call(ctx, nodeListReplicaRequest{Op: "list"})
	return out.Entries, err
}

func (r *HTTPNodeListReplica) call(ctx context.Context, in nodeListReplicaRequest) (nodeListReplicaResponse, error) {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(&in); err != nil {
		return nodeListReplicaResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint+NodeListReplicaRPCPath, &body)
	if err != nil {
		return nodeListReplicaResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return nodeListReplicaResponse{}, err
	}
	defer resp.Body.Close()
	var out nodeListReplicaResponse
	if resp.StatusCode/100 == 2 {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
			return nodeListReplicaResponse{}, err
		}
		return out, nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return nodeListReplicaResponse{}, fmt.Errorf("node_list replica: %s: %s", resp.Status, strings.TrimSpace(string(b)))
}
