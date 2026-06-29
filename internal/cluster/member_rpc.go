package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	RouteReplicaRPCPath     = "/internal/registry-member/route-replica"
	NodeReplicaRPCPath      = "/internal/registry-member/node-replica"
	ScaleLinkReplicaRPCPath = "/internal/registry-member/scale-link-replica"
)

type replicaRequest struct {
	Op     string          `json:"op"`
	Key    string          `json:"key,omitempty"`
	Group  string          `json:"group,omitempty"`
	Ballot Ballot          `json:"ballot,omitempty"`
	Route  RouteRecord     `json:"route,omitempty"`
	Node   NodeRecord      `json:"node,omitempty"`
	Scale  ScaleLinkRecord `json:"scale,omitempty"`
}

type replicaResponse struct {
	Route  RouteRecord     `json:"route,omitempty"`
	Routes []RouteRecord   `json:"routes,omitempty"`
	Node   NodeRecord      `json:"node,omitempty"`
	Scale  ScaleLinkRecord `json:"scale,omitempty"`
	Found  bool            `json:"found,omitempty"`
	OK     bool            `json:"ok,omitempty"`
	Ballot Ballot          `json:"ballot,omitempty"`
	Error  string          `json:"error,omitempty"`
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
	case "list_group":
		out.Routes, err = replica.ListGroup(req.Context(), in.Group)
		out.OK = err == nil
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
	default:
		err = fmt.Errorf("cluster: unknown node replica op %q", in.Op)
	}
	writeReplicaResponse(w, out, err)
}

func ServeScaleLinkReplica(w http.ResponseWriter, req *http.Request, replica ScaleLinkReplica) {
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
		out.Scale, out.Found, err = replica.Read(req.Context(), in.Key)
	case "prepare":
		out.Scale, out.Found, out.OK, err = replica.Prepare(req.Context(), in.Key, in.Ballot)
	case "accept":
		out.OK, err = replica.Accept(req.Context(), in.Key, in.Scale, in.Ballot)
	case "repair":
		err = replica.Repair(req.Context(), in.Key, in.Scale)
		out.OK = err == nil
	case "max_ballot":
		out.Ballot, err = replica.MaxBallot(req.Context(), in.Key)
	default:
		err = fmt.Errorf("cluster: unknown scale task replica op %q", in.Op)
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
	health   ReplicaAvailability
}

func NewHTTPRouteReplica(endpoint string, client *http.Client) *HTTPRouteReplica {
	return NewHTTPRouteReplicaWithHealth(endpoint, client, nil)
}

func NewHTTPRouteReplicaWithHealth(endpoint string, client *http.Client, health ReplicaAvailability) *HTTPRouteReplica {
	if client == nil {
		client = http.DefaultClient
	}
	if health == nil {
		health = NewReplicaHealth(2 * time.Second)
	}
	return &HTTPRouteReplica{endpoint: strings.TrimRight(endpoint, "/"), client: client, health: health}
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

func (r *HTTPRouteReplica) ListGroup(ctx context.Context, group string) ([]RouteRecord, error) {
	out, err := r.call(ctx, replicaRequest{Op: "list_group", Group: group})
	return out.Routes, err
}

func (r *HTTPRouteReplica) call(ctx context.Context, in replicaRequest) (replicaResponse, error) {
	return postReplica(ctx, r.client, r.endpoint+RouteReplicaRPCPath, in, r.health)
}

type HTTPNodeReplica struct {
	endpoint string
	client   *http.Client
	health   ReplicaAvailability
}

func NewHTTPNodeReplica(endpoint string, client *http.Client) *HTTPNodeReplica {
	return NewHTTPNodeReplicaWithHealth(endpoint, client, nil)
}

func NewHTTPNodeReplicaWithHealth(endpoint string, client *http.Client, health ReplicaAvailability) *HTTPNodeReplica {
	if client == nil {
		client = http.DefaultClient
	}
	if health == nil {
		health = NewReplicaHealth(2 * time.Second)
	}
	return &HTTPNodeReplica{endpoint: strings.TrimRight(endpoint, "/"), client: client, health: health}
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

func (r *HTTPNodeReplica) call(ctx context.Context, in replicaRequest) (replicaResponse, error) {
	return postReplica(ctx, r.client, r.endpoint+NodeReplicaRPCPath, in, r.health)
}

type HTTPScaleLinkReplica struct {
	endpoint string
	client   *http.Client
	health   ReplicaAvailability
}

func NewHTTPScaleLinkReplicaWithHealth(endpoint string, client *http.Client, health ReplicaAvailability) *HTTPScaleLinkReplica {
	if client == nil {
		client = http.DefaultClient
	}
	if health == nil {
		health = NewReplicaHealth(2 * time.Second)
	}
	return &HTTPScaleLinkReplica{endpoint: strings.TrimRight(endpoint, "/"), client: client, health: health}
}

func (r *HTTPScaleLinkReplica) Read(ctx context.Context, key string) (ScaleLinkRecord, bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "read", Key: key})
	return out.Scale, out.Found, err
}

func (r *HTTPScaleLinkReplica) Prepare(ctx context.Context, key string, ballot Ballot) (ScaleLinkRecord, bool, bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "prepare", Key: key, Ballot: ballot})
	return out.Scale, out.Found, out.OK, err
}

func (r *HTTPScaleLinkReplica) Accept(ctx context.Context, key string, rec ScaleLinkRecord, ballot Ballot) (bool, error) {
	out, err := r.call(ctx, replicaRequest{Op: "accept", Key: key, Scale: rec, Ballot: ballot})
	return out.OK, err
}

func (r *HTTPScaleLinkReplica) Repair(ctx context.Context, key string, rec ScaleLinkRecord) error {
	_, err := r.call(ctx, replicaRequest{Op: "repair", Key: key, Scale: rec})
	return err
}

func (r *HTTPScaleLinkReplica) MaxBallot(ctx context.Context, key string) (Ballot, error) {
	out, err := r.call(ctx, replicaRequest{Op: "max_ballot", Key: key})
	return out.Ballot, err
}

func (r *HTTPScaleLinkReplica) call(ctx context.Context, in replicaRequest) (replicaResponse, error) {
	return postReplica(ctx, r.client, r.endpoint+ScaleLinkReplicaRPCPath, in, r.health)
}

var ErrReplicaUnavailable = errors.New("cluster: replica unavailable")

type ReplicaHealth struct {
	mu             sync.Mutex
	cooldown       time.Duration
	unhealthyUntil time.Time
}

type ReplicaAvailability interface {
	Available() bool
	MarkSuccess()
	MarkFailure()
}

type FuncReplicaAvailability struct {
	available func() bool
	cooldown  *ReplicaHealth
}

func NewFuncReplicaAvailability(available func() bool, cooldown time.Duration) *FuncReplicaAvailability {
	return &FuncReplicaAvailability{available: available, cooldown: NewReplicaHealth(cooldown)}
}

func (h *FuncReplicaAvailability) Available() bool {
	if h == nil {
		return true
	}
	if h.available != nil && !h.available() {
		return false
	}
	return h.cooldown == nil || h.cooldown.Available()
}

func (h *FuncReplicaAvailability) MarkSuccess() {
	if h != nil && h.cooldown != nil {
		h.cooldown.MarkSuccess()
	}
}

func (h *FuncReplicaAvailability) MarkFailure() {
	if h != nil && h.cooldown != nil {
		h.cooldown.MarkFailure()
	}
}

func NewReplicaHealth(cooldown time.Duration) *ReplicaHealth {
	if cooldown <= 0 {
		cooldown = 2 * time.Second
	}
	return &ReplicaHealth{cooldown: cooldown}
}

func (h *ReplicaHealth) Available() bool {
	if h == nil {
		return true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	return time.Now().After(h.unhealthyUntil)
}

func (h *ReplicaHealth) MarkSuccess() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.unhealthyUntil = time.Time{}
	h.mu.Unlock()
}

func (h *ReplicaHealth) MarkFailure() {
	if h == nil {
		return
	}
	h.mu.Lock()
	h.unhealthyUntil = time.Now().Add(h.cooldown)
	h.mu.Unlock()
}

func postReplica(ctx context.Context, client *http.Client, url string, in replicaRequest, health ReplicaAvailability) (replicaResponse, error) {
	if health != nil && !health.Available() {
		return replicaResponse{}, ErrReplicaUnavailable
	}
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
		if health != nil {
			health.MarkFailure()
		}
		return replicaResponse{}, err
	}
	defer resp.Body.Close()
	var out replicaResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		if health != nil {
			health.MarkFailure()
		}
		return replicaResponse{}, err
	}
	if out.Error != "" {
		if health != nil {
			health.MarkFailure()
		}
		return out, fmt.Errorf("%s", out.Error)
	}
	if resp.StatusCode >= 300 {
		if health != nil {
			health.MarkFailure()
		}
		return out, fmt.Errorf("cluster: replica rpc status %s", resp.Status)
	}
	if health != nil {
		health.MarkSuccess()
	}
	return out, nil
}
