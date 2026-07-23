package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/session"
)

const (
	OperatorEnrollNodePath                 = "/internal/operator/node/enroll"
	OperatorRetireNodePath                 = "/internal/operator/node/retire"
	OperatorBeginRecoveryPath              = "/internal/operator/recovery/begin"
	OperatorResolveRecoveryNodePath        = "/internal/operator/recovery/node/resolve"
	OperatorFenceRoutePath                 = "/internal/operator/route/fence"
	OperatorCloseRegistryGenerationPath    = "/internal/operator/registry-generation/close"
	OperatorConfirmDrainPath               = "/internal/operator/registry-generation/confirm-predecessor-drain"
	OperatorActivateRegistryGenerationPath = "/internal/operator/registry-generation/activate"
	OperatorSystemStatePath                = "/internal/operator/system/state"
)

type EnrollNodeRequest struct {
	routeapi.RegistryServeIdentity
	NodeID       string `json:"node_id"`
	EnrollmentID string `json:"enrollment_id"`
	NodeEpoch    uint64 `json:"node_epoch"`
	DataEndpoint string `json:"data_endpoint"`
}

type RetireNodeRequest struct {
	routeapi.RegistryServeIdentity
	NodeID        string `json:"node_id"`
	EnrollmentID  string `json:"enrollment_id"`
	LastNodeEpoch uint64 `json:"last_node_epoch"`
}

type BeginRecoveryRequest struct {
	routeapi.RegistryServeIdentity
}

type ResolveRecoveryNodeRequest struct {
	routeapi.RegistryServeIdentity
	NodeID      string `json:"node_id"`
	Resolution  string `json:"resolution"`
	ProofDigest string `json:"proof_digest"`
	Reason      string `json:"reason"`
}

type FenceRouteRequest struct {
	routeapi.RegistryServeIdentity
	Group                 string `json:"group"`
	RouteKey              string `json:"route_key"`
	SandboxID             string `json:"sandbox_id"`
	NodeID                string `json:"node_id"`
	NodeEpoch             uint64 `json:"node_epoch"`
	BindingDigest         string `json:"binding_digest"`
	ExpectedRouteRevision uint64 `json:"expected_route_revision"`
	ProofDigest           string `json:"proof_digest"`
	Reason                string `json:"reason"`
}

type ActivateRegistryGenerationRequest struct {
	routeapi.RegistryServeIdentity
}

type CloseRegistryGenerationRequest struct {
	routeapi.RegistryServeIdentity
	Successor raftstore.RegistryLayout `json:"successor"`
}

type ConfirmPredecessorDrainRequest struct {
	routeapi.RegistryServeIdentity
	EvidenceDigest string `json:"evidence_digest"`
}

type CloseRegistryGenerationResponse struct {
	PredecessorProof raftstore.PredecessorProof `json:"predecessor_proof"`
}

type OperatorService struct {
	store            *RaftStore
	retirer          IdentityRetirer
	recoveryResolver RecoveryNodeResolver
}

type IdentityRetirer interface {
	RetireIdentity(context.Context, session.IdentityRetirement) (bool, error)
}

type RecoveryNodeResolver interface {
	ResolveRecoveryNode(context.Context, ResolveRecoveryNodeRequest) error
}

func NewOperatorService(
	store *RaftStore,
	retirer IdentityRetirer,
	recoveryResolver RecoveryNodeResolver,
) (*OperatorService, error) {
	if store == nil || retirer == nil || recoveryResolver == nil {
		return nil, errors.New("controlplane: operator service requires consensus, Session retirement, and recovery resolution")
	}
	return &OperatorService{store: store, retirer: retirer, recoveryResolver: recoveryResolver}, nil
}

func (s *OperatorService) EnrollNode(ctx context.Context, request EnrollNodeRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil {
		return err
	}
	current, err := s.store.RegistryServeIdentity()
	if err != nil || current != request.RegistryServeIdentity {
		return errors.New("controlplane: node enrollment targets another serving Registry History Generation")
	}
	return s.store.EnrollNode(ctx, session.NodeEnrollment{
		NodeID: request.NodeID, EnrollmentID: request.EnrollmentID,
		NodeEpoch: request.NodeEpoch, DataEndpoint: request.DataEndpoint,
	})
}

func (s *OperatorService) RetireNode(ctx context.Context, request RetireNodeRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil {
		return err
	}
	current, err := s.store.RegistryServeIdentity()
	if err != nil || current != request.RegistryServeIdentity {
		return errors.New("controlplane: node retirement targets another serving Registry History Generation")
	}
	retired, err := s.retirer.RetireIdentity(ctx, session.IdentityRetirement{
		NodeID: request.NodeID, EnrollmentID: request.EnrollmentID, LastNodeEpoch: request.LastNodeEpoch,
	})
	if err != nil {
		return err
	}
	if !retired {
		return errors.New("controlplane: exact node identity was not retired")
	}
	return nil
}

func (s *OperatorService) BeginRecovery(ctx context.Context, request BeginRecoveryRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil {
		return err
	}
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	if !operatorRegistryServeIdentityMatches(state, request.RegistryServeIdentity) {
		return errors.New("controlplane: recovery begin targets another Registry History Generation")
	}
	_, err = s.store.BeginRecovery(ctx)
	return err
}

func (s *OperatorService) ResolveRecoveryNode(ctx context.Context, request ResolveRecoveryNodeRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil || request.NodeID == "" ||
		request.ProofDigest == "" || request.Reason == "" {
		return errors.New("controlplane: incomplete recovery node resolution")
	}
	switch request.Resolution {
	case string(raftstore.RecoveryNodeMissing), string(raftstore.RecoveryNodeQuarantined):
	default:
		return errors.New("controlplane: recovery resolution must be MISSING or QUARANTINED")
	}
	return s.recoveryResolver.ResolveRecoveryNode(ctx, request)
}

func (s *OperatorService) FenceRoute(ctx context.Context, request FenceRouteRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil || request.Group == "" || request.RouteKey == "" ||
		request.SandboxID == "" || request.NodeID == "" || request.NodeEpoch == 0 || request.BindingDigest == "" ||
		request.ExpectedRouteRevision == 0 || request.ProofDigest == "" || request.Reason == "" || len(request.Reason) > 1024 {
		return errors.New("controlplane: incomplete external Route fence request")
	}
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	if !operatorRegistryServeIdentityMatches(state, request.RegistryServeIdentity) {
		return errors.New("controlplane: external Route fence targets another Registry History Generation")
	}
	record, err := s.store.ReadRouteWorkflow(ctx, request.Group, request.RouteKey)
	if err != nil {
		return err
	}
	if record == nil {
		return errors.New("controlplane: external Route fence target is not found")
	}
	execution, found := routeBoundExecution(*record)
	if !found || execution.SandboxID != request.SandboxID || execution.NodeID != request.NodeID ||
		execution.NodeEpoch != request.NodeEpoch || execution.RegistryGeneration != request.RegistryGeneration ||
		execution.BindingDigest != request.BindingDigest {
		return errors.New("controlplane: external Route fence does not identify the current execution")
	}
	proof := clusterstate.TerminalProof{
		Kind: clusterstate.ProofExternalFence, ProofDigest: request.ProofDigest,
		FencedNodeID: request.NodeID, FencedNodeEpoch: request.NodeEpoch,
	}
	if record.State != clusterstate.WorkflowRouteTombstone && record.Revision.LogIndex != request.ExpectedRouteRevision {
		return errors.New("controlplane: external Route fence revision conflict")
	}
	_, err = s.store.FenceRouteExecution(ctx, *record, proof, request.Reason)
	return err
}

func (s *OperatorService) ActivateRegistryGeneration(ctx context.Context, request ActivateRegistryGenerationRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil {
		return err
	}
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	if !operatorRegistryServeIdentityMatches(state, request.RegistryServeIdentity) || state.Recovery != nil || state.Retired ||
		!state.PredecessorDrainComplete || state.HasPredecessor && state.RecoveryCompletion == nil {
		return errors.New("controlplane: Registry History Generation has not satisfied final cutover gates")
	}
	if _, err := s.store.SetServingGates(ctx, raftstore.GateUpdate{Serve: true, Write: true, Cutover: true}); err != nil {
		return err
	}
	_, err = s.store.RefreshPermitGrant(ctx)
	return err
}

func (s *OperatorService) CloseRegistryGeneration(
	ctx context.Context,
	request CloseRegistryGenerationRequest,
) (CloseRegistryGenerationResponse, error) {
	if err := request.RegistryServeIdentity.Validate(); err != nil {
		return CloseRegistryGenerationResponse{}, err
	}
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return CloseRegistryGenerationResponse{}, err
	}
	if !operatorRegistryServeIdentityMatches(state, request.RegistryServeIdentity) {
		return CloseRegistryGenerationResponse{}, errors.New("controlplane: Registry History Generation closure targets another Registry History Generation")
	}
	closed, err := s.store.CloseRegistryGeneration(ctx, request.Successor)
	if err != nil {
		return CloseRegistryGenerationResponse{}, err
	}
	proof, err := closed.ConsensusPredecessorProof()
	if err != nil {
		return CloseRegistryGenerationResponse{}, err
	}
	return CloseRegistryGenerationResponse{PredecessorProof: proof}, nil
}

func (s *OperatorService) ConfirmPredecessorDrain(ctx context.Context, request ConfirmPredecessorDrainRequest) error {
	if err := request.RegistryServeIdentity.Validate(); err != nil || request.EvidenceDigest == "" {
		return errors.New("controlplane: incomplete predecessor drain confirmation")
	}
	state, err := s.store.ReadSystem(ctx)
	if err != nil {
		return err
	}
	if !operatorRegistryServeIdentityMatches(state, request.RegistryServeIdentity) {
		return errors.New("controlplane: predecessor drain confirmation targets another Registry History Generation")
	}
	_, err = s.store.ConfirmPredecessorPermitDrain(ctx, request.EvidenceDigest)
	return err
}

func NewOperatorHandler(service *OperatorService, trust func(*http.Request) error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+OperatorEnrollNodePath, func(w http.ResponseWriter, request *http.Request) {
		var input EnrollNodeRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil {
			http.Error(w, "invalid node enrollment request", http.StatusBadRequest)
			return
		}
		if err := service.EnrollNode(request.Context(), input); err != nil {
			http.Error(w, "node enrollment rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+OperatorRetireNodePath, func(w http.ResponseWriter, request *http.Request) {
		var input RetireNodeRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil {
			http.Error(w, "invalid node retirement request", http.StatusBadRequest)
			return
		}
		if err := service.RetireNode(request.Context(), input); err != nil {
			http.Error(w, "node retirement rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+OperatorBeginRecoveryPath, func(w http.ResponseWriter, request *http.Request) {
		var input BeginRecoveryRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil || service.BeginRecovery(request.Context(), input) != nil {
			http.Error(w, "recovery begin rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+OperatorResolveRecoveryNodePath, func(w http.ResponseWriter, request *http.Request) {
		var input ResolveRecoveryNodeRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil || service.ResolveRecoveryNode(request.Context(), input) != nil {
			http.Error(w, "recovery node resolution rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+OperatorFenceRoutePath, func(w http.ResponseWriter, request *http.Request) {
		var input FenceRouteRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil {
			http.Error(w, "invalid external Route fence request", http.StatusBadRequest)
			return
		}
		if err := service.FenceRoute(request.Context(), input); err != nil {
			http.Error(w, "external Route fence rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+OperatorActivateRegistryGenerationPath, func(w http.ResponseWriter, request *http.Request) {
		var input ActivateRegistryGenerationRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil || service.ActivateRegistryGeneration(request.Context(), input) != nil {
			http.Error(w, "Registry History Generation activation rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST "+OperatorCloseRegistryGenerationPath, func(w http.ResponseWriter, request *http.Request) {
		var input CloseRegistryGenerationRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil {
			http.Error(w, "invalid Registry History Generation closure request", http.StatusBadRequest)
			return
		}
		response, err := service.CloseRegistryGeneration(request.Context(), input)
		if err != nil {
			http.Error(w, "Registry History Generation closure rejected", http.StatusConflict)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(response)
	})
	mux.HandleFunc("POST "+OperatorConfirmDrainPath, func(w http.ResponseWriter, request *http.Request) {
		var input ConfirmPredecessorDrainRequest
		if !operatorAuthorized(w, request, trust) {
			return
		}
		if err := decodeOperatorRequest(request, &input); err != nil {
			http.Error(w, "invalid predecessor drain request", http.StatusBadRequest)
			return
		}
		if err := service.ConfirmPredecessorDrain(request.Context(), input); err != nil {
			http.Error(w, "predecessor drain confirmation rejected", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET "+OperatorSystemStatePath, func(w http.ResponseWriter, request *http.Request) {
		if !operatorAuthorized(w, request, trust) {
			return
		}
		state, err := service.store.ReadSystem(request.Context())
		if err != nil {
			http.Error(w, "System Group unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(state)
	})
	return mux
}

func operatorRegistryServeIdentityMatches(state raftstore.SystemState, identity routeapi.RegistryServeIdentity) bool {
	return state.ClusterID == identity.ClusterID && state.RegistryGeneration == identity.RegistryGeneration &&
		state.SystemEpoch == identity.SystemEpoch && state.ActiveRegistryLayoutDigest == identity.RegistryLayoutDigest
}

func operatorAuthorized(w http.ResponseWriter, request *http.Request, trust func(*http.Request) error) bool {
	if trust == nil || trust(request) != nil {
		http.Error(w, "untrusted operator identity", http.StatusForbidden)
		return false
	}
	return true
}

func decodeOperatorRequest(request *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(request.Body, 128<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("controlplane: operator request has trailing data")
	}
	return nil
}
