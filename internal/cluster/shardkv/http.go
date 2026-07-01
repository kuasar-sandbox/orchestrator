package shardkv

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

const HTTPPath = "/internal/registry-member/shardkv"

func ServeHTTP(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var in Request
		if err := json.NewDecoder(io.LimitReader(req.Body, 1<<20)).Decode(&in); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if in.Label == "" {
			http.Error(w, "shardkv: request label is required", http.StatusBadRequest)
			return
		}
		out, err := store.Handle(req.Context(), in)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			out.Error = err.Error()
			w.WriteHeader(http.StatusConflict)
		}
		_ = json.NewEncoder(w).Encode(out)
	}
}

type HTTPEndpointResolver interface {
	Endpoint(member MemberID) (string, bool)
}

type HTTPEndpointResolverFunc func(member MemberID) (string, bool)

func (f HTTPEndpointResolverFunc) Endpoint(member MemberID) (string, bool) {
	return f(member)
}

type HTTPTransport struct {
	client    *http.Client
	endpoints HTTPEndpointResolver
	peers     HTTPPeerResolver
}

func NewHTTPTransport(client *http.Client, endpoints HTTPEndpointResolver) *HTTPTransport {
	if client == nil {
		client = http.DefaultClient
	}
	return &HTTPTransport{client: client, endpoints: endpoints}
}

type HTTPPeer struct {
	Endpoint string
	Client   *http.Client
}

type HTTPPeerResolver interface {
	Peer(member MemberID) (HTTPPeer, bool)
}

type HTTPPeerResolverFunc func(member MemberID) (HTTPPeer, bool)

func (f HTTPPeerResolverFunc) Peer(member MemberID) (HTTPPeer, bool) {
	return f(member)
}

func NewHTTPPeerTransport(peers HTTPPeerResolver) *HTTPTransport {
	return &HTTPTransport{peers: peers}
}

func (t *HTTPTransport) Call(ctx context.Context, member MemberID, in Request) (Response, error) {
	if t == nil {
		return Response{}, ErrReplicaUnavailable
	}
	client := t.client
	var endpoint string
	if t.peers != nil {
		peer, ok := t.peers.Peer(member)
		if !ok || peer.Endpoint == "" {
			return Response{}, ErrReplicaUnavailable
		}
		endpoint = peer.Endpoint
		client = peer.Client
	} else {
		if t.endpoints == nil {
			return Response{}, ErrReplicaUnavailable
		}
		var ok bool
		endpoint, ok = t.endpoints.Endpoint(member)
		if !ok || endpoint == "" {
			return Response{}, ErrReplicaUnavailable
		}
	}
	if client == nil {
		client = http.DefaultClient
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(in); err != nil {
		return Response{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(endpoint, "/")+HTTPPath, &body)
	if err != nil {
		return Response{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	var out Response
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return Response{}, err
	}
	if resp.StatusCode/100 != 2 {
		if out.Error != "" {
			return out, fmt.Errorf("shardkv http %s: %s", resp.Status, out.Error)
		}
		return out, fmt.Errorf("shardkv http %s", resp.Status)
	}
	return out, nil
}
