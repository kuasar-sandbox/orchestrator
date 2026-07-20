package controlplane

import (
	"context"
	"errors"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

const (
	ReplicaCatchUpPath  = "/internal/raft-transition/catch-up"
	ReplicaAppliedPath  = "/internal/raft-transition/applied"
	ReplicaPromotedPath = "/internal/raft-transition/promoted"
)

type replicaTransitionRuntime interface {
	ProveLocalReplicaCaughtUp(context.Context, raftstore.ReplicaCatchUpRequest) (raftstore.ReplicaCatchUpProof, error)
	ProveLocalReplicaApplied(context.Context, raftstore.ReplicaAppliedRequest) (raftstore.ReplicaAppliedProof, error)
	ConfirmLocalReplicaPromoted(context.Context, raftstore.ReplicaPromotionRequest) error
}

type ReplicaTransitionHandler struct {
	runtime replicaTransitionRuntime
}

func NewReplicaTransitionHandler(runtime replicaTransitionRuntime) (*ReplicaTransitionHandler, error) {
	if runtime == nil {
		return nil, errors.New("controlplane: replica transition handler requires a runtime")
	}
	return &ReplicaTransitionHandler{runtime: runtime}, nil
}

func (h *ReplicaTransitionHandler) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST "+ReplicaCatchUpPath, h.catchUp)
	mux.HandleFunc("POST "+ReplicaAppliedPath, h.applied)
	mux.HandleFunc("POST "+ReplicaPromotedPath, h.promoted)
}

func (h *ReplicaTransitionHandler) catchUp(w http.ResponseWriter, request *http.Request) {
	var input raftstore.ReplicaCatchUpRequest
	if err := decodeBoundedJSON(request.Body, &input, maximumSessionRPC); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, replicaCatchUpResponse{Error: "invalid catch-up request"})
		return
	}
	proof, err := h.runtime.ProveLocalReplicaCaughtUp(request.Context(), input)
	response := replicaCatchUpResponse{Proof: &proof}
	if err != nil {
		response.Proof, response.Error = nil, err.Error()
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (h *ReplicaTransitionHandler) applied(w http.ResponseWriter, request *http.Request) {
	var input raftstore.ReplicaAppliedRequest
	if err := decodeBoundedJSON(request.Body, &input, maximumSessionRPC); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, replicaAppliedResponse{Error: "invalid applied-index request"})
		return
	}
	proof, err := h.runtime.ProveLocalReplicaApplied(request.Context(), input)
	response := replicaAppliedResponse{Proof: &proof}
	if err != nil {
		response.Proof, response.Error = nil, err.Error()
	}
	writeSessionJSON(w, http.StatusOK, response)
}

func (h *ReplicaTransitionHandler) promoted(w http.ResponseWriter, request *http.Request) {
	var input raftstore.ReplicaPromotionRequest
	if err := decodeBoundedJSON(request.Body, &input, maximumSessionRPC); err != nil {
		writeSessionJSON(w, http.StatusBadRequest, replicaPromotionResponse{Error: "invalid promotion request"})
		return
	}
	response := replicaPromotionResponse{}
	if err := h.runtime.ConfirmLocalReplicaPromoted(request.Context(), input); err != nil {
		response.Error = err.Error()
	}
	writeSessionJSON(w, http.StatusOK, response)
}

type ReplicaTransitionClient struct {
	peers map[string]SessionPeer
}

func NewReplicaTransitionClient(peers []SessionPeer) (*ReplicaTransitionClient, error) {
	byID := make(map[string]SessionPeer, len(peers))
	for _, peer := range peers {
		if peer.MemberID == "" || peer.Endpoint == "" || peer.Client == nil {
			return nil, errors.New("controlplane: incomplete replica transition peer")
		}
		if _, duplicate := byID[peer.MemberID]; duplicate {
			return nil, errors.New("controlplane: duplicate replica transition peer")
		}
		byID[peer.MemberID] = peer
	}
	if len(byID) == 0 {
		return nil, errors.New("controlplane: replica transition peers are required")
	}
	return &ReplicaTransitionClient{peers: byID}, nil
}

func (c *ReplicaTransitionClient) ProbeReplicaCatchUp(
	ctx context.Context,
	request raftstore.ReplicaCatchUpRequest,
) (raftstore.ReplicaCatchUpProof, error) {
	peer, found := c.peers[request.MemberID]
	if !found {
		return raftstore.ReplicaCatchUpProof{}, errors.New("controlplane: catch-up target is absent from the signed registryLayout")
	}
	var response replicaCatchUpResponse
	if err := postBoundedJSON(ctx, peer, ReplicaCatchUpPath, request, &response, maximumSessionRPC); err != nil {
		return raftstore.ReplicaCatchUpProof{}, err
	}
	if response.Error != "" || response.Proof == nil {
		return raftstore.ReplicaCatchUpProof{}, errors.New(response.Error)
	}
	return *response.Proof, nil
}

func (c *ReplicaTransitionClient) ProbeReplicaApplied(
	ctx context.Context,
	request raftstore.ReplicaAppliedRequest,
) (raftstore.ReplicaAppliedProof, error) {
	peer, found := c.peers[request.MemberID]
	if !found {
		return raftstore.ReplicaAppliedProof{}, errors.New("controlplane: applied-index target is absent from the signed registryLayout")
	}
	var response replicaAppliedResponse
	if err := postBoundedJSON(ctx, peer, ReplicaAppliedPath, request, &response, maximumSessionRPC); err != nil {
		return raftstore.ReplicaAppliedProof{}, err
	}
	if response.Error != "" || response.Proof == nil {
		return raftstore.ReplicaAppliedProof{}, errors.New(response.Error)
	}
	return *response.Proof, nil
}

func (c *ReplicaTransitionClient) ConfirmReplicaPromoted(
	ctx context.Context,
	request raftstore.ReplicaPromotionRequest,
) error {
	peer, found := c.peers[request.MemberID]
	if !found {
		return errors.New("controlplane: promotion target is absent from the signed registryLayout")
	}
	var response replicaPromotionResponse
	if err := postBoundedJSON(ctx, peer, ReplicaPromotedPath, request, &response, maximumSessionRPC); err != nil {
		return err
	}
	if response.Error != "" {
		return errors.New(response.Error)
	}
	return nil
}

type replicaCatchUpResponse struct {
	Proof *raftstore.ReplicaCatchUpProof `json:"proof,omitempty"`
	Error string                         `json:"error,omitempty"`
}

type replicaAppliedResponse struct {
	Proof *raftstore.ReplicaAppliedProof `json:"proof,omitempty"`
	Error string                         `json:"error,omitempty"`
}

type replicaPromotionResponse struct {
	Error string `json:"error,omitempty"`
}
