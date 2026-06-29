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

type NodeListReplica interface {
	ApplyNodeListPut(ctx context.Context, entry clusterstate.NodeListEntry) error
	ApplyNodeListDelete(ctx context.Context, nodeID string) error
}

type nodeListReplicaRequest struct {
	Op     string                     `json:"op"`
	NodeID string                     `json:"node_id,omitempty"`
	Entry  clusterstate.NodeListEntry `json:"entry,omitempty"`
}

type nodeListReplicaResponse struct {
	OK    bool   `json:"ok,omitempty"`
	Error string `json:"error,omitempty"`
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
	var err error
	switch in.Op {
	case "put":
		err = replica.ApplyNodeListPut(req.Context(), in.Entry)
	case "delete":
		err = replica.ApplyNodeListDelete(req.Context(), in.NodeID)
	default:
		err = fmt.Errorf("registry: unknown node_list replica op %q", in.Op)
	}
	out := nodeListReplicaResponse{OK: err == nil}
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

func (r *HTTPNodeListReplica) ApplyNodeListPut(ctx context.Context, entry clusterstate.NodeListEntry) error {
	return r.call(ctx, nodeListReplicaRequest{Op: "put", Entry: entry})
}

func (r *HTTPNodeListReplica) ApplyNodeListDelete(ctx context.Context, nodeID string) error {
	return r.call(ctx, nodeListReplicaRequest{Op: "delete", NodeID: nodeID})
}

func (r *HTTPNodeListReplica) call(ctx context.Context, in nodeListReplicaRequest) error {
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(&in); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint+NodeListReplicaRPCPath, &body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 == 2 {
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Errorf("node_list replica: %s: %s", resp.Status, strings.TrimSpace(string(b)))
}
