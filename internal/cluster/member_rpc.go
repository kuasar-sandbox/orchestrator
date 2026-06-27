package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const (
	RouteReplicaRPCPath = "/internal/registry-member/route-replica"
	NodeReplicaRPCPath  = "/internal/registry-member/node-replica"
)

type replicaRequest struct {
	Op     string      `json:"op"`
	Key    string      `json:"key,omitempty"`
	Ballot Ballot      `json:"ballot,omitempty"`
	Route  RouteRecord `json:"route,omitempty"`
	Node   NodeRecord  `json:"node,omitempty"`
}

type replicaResponse struct {
	Route  RouteRecord `json:"route,omitempty"`
	Node   NodeRecord  `json:"node,omitempty"`
	Found  bool        `json:"found,omitempty"`
	OK     bool        `json:"ok,omitempty"`
	Ballot Ballot      `json:"ballot,omitempty"`
	Keys   []string    `json:"keys,omitempty"`
	Error  string      `json:"error,omitempty"`
}

func ServeRouteReplica(w http.ResponseWriter, req *http.Request, replica RouteReplica) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in replicaRequest
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var out replicaResponse
	var err error
	switch in.Op {
	case "read":
		out.Route, out.Found, err = replica.Read(req.Context(), in.Key)
	case "prepare":
		out.Route, out.Found, out.OK, err = replica.Prepare(req.Context(), in.Key, in.Ballot)
	case "accept":
		out.OK, err = replica.Accept(req.Context(), in.Key, in.Route, in.Ballot)
	case "repair":
		err = replica.Repair(req.Context(), in.Key, in.Route)
		out.OK = err == nil
	case "max_ballot":
		out.Ballot, err = replica.MaxBallot(req.Context(), in.Key)
	case "keys":
		out.Keys = replica.Keys(req.Context())
		out.OK = true
	default:
		err = fmt.Errorf("cluster: unknown route replica op %q", in.Op)
	}
	writeReplicaResponse(w, out, err)
}

func ServeNodeReplica(w http.ResponseWriter, req *http.Request, replica NodeReplica) {
	if req.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in replicaRequest
	if err := json.NewDecoder(req.Body).Decode(&in); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var out replicaResponse
	var err error
	switch in.Op {
	case "read":
		out.Node, out.Found, err = replica.Read(req.Context(), in.Key)
	case "prepare":
		out.Node, out.Found, out.OK, err = replica.Prepare(req.Context(), in.Key, in.Ballot)
	case "accept":
		out.OK, err = replica.Accept(req.Context(), in.Key, in.Node, in.Ballot)
	case "repair":
		err = replica.Repair(req.Context(), in.Key, in.Node)
		out.OK = err == nil
	case "max_ballot":
		out.Ballot, err = replica.MaxBallot(req.Context(), in.Key)
	case "keys":
		out.Keys = replica.Keys(req.Context())
		out.OK = true
	default:
		err = fmt.Errorf("cluster: unknown node replica op %q", in.Op)
	}
	writeReplicaResponse(w, out, err)
}

func writeReplicaResponse(w http.ResponseWriter, out replicaResponse, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		out.Error = err.Error()
		w.WriteHeader(http.StatusConflict)
	}
	_ = json.NewEncoder(w).Encode(&out)
}

type HTTPRouteReplica struct {
	endpoint string
	client   *http.Client
}

func NewHTTPRouteReplica(endpoint string, client *http.Client) *HTTPRouteReplica {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPRouteReplica{endpoint: strings.TrimRight(endpoint, "/"), client: client}
}

func (r *HTTPRouteReplica) Read(ctx context.Context, key string) (RouteRecord, bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "read", Key: key})
	return out.Route, out.Found, err
}

func (r *HTTPRouteReplica) Prepare(ctx context.Context, key string, ballot Ballot) (RouteRecord, bool, bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "prepare", Key: key, Ballot: ballot})
	return out.Route, out.Found, out.OK, err
}

func (r *HTTPRouteReplica) Accept(ctx context.Context, key string, rec RouteRecord, ballot Ballot) (bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "accept", Key: key, Route: rec, Ballot: ballot})
	return out.OK, err
}

func (r *HTTPRouteReplica) Repair(ctx context.Context, key string, rec RouteRecord) error {
	_, err := r.call(ctx, replicaRequest{Op: "repair", Key: key, Route: rec})
	return err
}

func (r *HTTPRouteReplica) MaxBallot(ctx context.Context, key string) (Ballot, error) {
	out, err := r.call(ctx, replicaRequest{Op: "max_ballot", Key: key})
	return out.Ballot, err
}

func (r *HTTPRouteReplica) Keys(ctx context.Context) []string {
	out, err := r.call(ctx, replicaRequest{Op: "keys"})
	if err != nil {
		return nil
	}
	return out.Keys
}

func (r *HTTPRouteReplica) call(ctx context.Context, in replicaRequest) (replicaResponse, error) {
	return postReplica(ctx, r.client, r.endpoint+RouteReplicaRPCPath, in)
}

type HTTPNodeReplica struct {
	endpoint string
	client   *http.Client
}

func NewHTTPNodeReplica(endpoint string, client *http.Client) *HTTPNodeReplica {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPNodeReplica{endpoint: strings.TrimRight(endpoint, "/"), client: client}
}

func (r *HTTPNodeReplica) Read(ctx context.Context, key string) (NodeRecord, bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "read", Key: key})
	return out.Node, out.Found, err
}

func (r *HTTPNodeReplica) Prepare(ctx context.Context, key string, ballot Ballot) (NodeRecord, bool, bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "prepare", Key: key, Ballot: ballot})
	return out.Node, out.Found, out.OK, err
}

func (r *HTTPNodeReplica) Accept(ctx context.Context, key string, rec NodeRecord, ballot Ballot) (bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "accept", Key: key, Node: rec, Ballot: ballot})
	return out.OK, err
}

func (r *HTTPNodeReplica) Repair(ctx context.Context, key string, rec NodeRecord) error {
	_, err := r.call(ctx, replicaRequest{Op: "repair", Key: key, Node: rec})
	return err
}

func (r *HTTPNodeReplica) MaxBallot(ctx context.Context, key string) (Ballot, error) {
	out, err := r.call(ctx, replicaRequest{Op: "max_ballot", Key: key})
	return out.Ballot, err
}

func (r *HTTPNodeReplica) Keys(ctx context.Context) []string {
	out, err := r.call(ctx, replicaRequest{Op: "keys"})
	if err != nil {
		return nil
	}
	return out.Keys
}

func (r *HTTPNodeReplica) call(ctx context.Context, in replicaRequest) (replicaResponse, error) {
	return postReplica(ctx, r.client, r.endpoint+NodeReplicaRPCPath, in)
}

func postReplica(ctx context.Context, client *http.Client, url string, in replicaRequest) (replicaResponse, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(&in); err != nil {
		return replicaResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return replicaResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return replicaResponse{}, err
	}
	defer resp.Body.Close()
	var out replicaResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return replicaResponse{}, err
	}
	if out.Error != "" {
		return out, fmt.Errorf("%s", out.Error)
	}
	if resp.StatusCode >= 300 {
		return out, fmt.Errorf("cluster: replica rpc status %s", resp.Status)
	}
	return out, nil
}
