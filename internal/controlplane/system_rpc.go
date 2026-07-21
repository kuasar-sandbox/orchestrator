package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
)

const (
	SystemRuntimePath = "/internal/system-runtime/operation"
	maximumSystemRPC  = 64 << 20
)

type systemOperation string

const (
	systemRead         systemOperation = "READ"
	systemApply        systemOperation = "APPLY"
	systemPermit       systemOperation = "REFRESH_PERMIT"
	systemClose        systemOperation = "CLOSE_GENERATION"
	systemDrain        systemOperation = "CONFIRM_PREDECESSOR_DRAIN"
	systemBeginRecover systemOperation = "BEGIN_RECOVERY"
	systemRecoverDrain systemOperation = "CONFIRM_RECOVERY_DRAIN"
	systemAdvance      systemOperation = "ADVANCE_RECOVERY"
	systemGates        systemOperation = "SET_GATES"
)

type systemRPCRequest struct {
	Operation      systemOperation           `json:"operation"`
	Command        *raftstore.SystemCommand  `json:"command,omitempty"`
	Successor      *raftstore.RegistryLayout `json:"successor,omitempty"`
	EvidenceDigest string                    `json:"evidence_digest,omitempty"`
	RecoveryFrom   raftstore.RecoveryPhase   `json:"recovery_from,omitempty"`
	RecoveryTo     raftstore.RecoveryPhase   `json:"recovery_to,omitempty"`
	Gates          *raftstore.GateUpdate     `json:"gates,omitempty"`
}

type systemRPCResponse struct {
	State  *raftstore.SystemState       `json:"state,omitempty"`
	Apply  *raftstore.SystemApplyResult `json:"apply,omitempty"`
	Permit *raftstore.PermitGrant       `json:"permit,omitempty"`
	Error  string                       `json:"error,omitempty"`
}

type systemReplicaRuntime interface {
	ReadSystemStrong(context.Context) (raftstore.SystemState, error)
	ApplySystem(context.Context, raftstore.SystemCommand) (raftstore.SystemApplyResult, error)
	RefreshPermit(context.Context) (raftstore.PermitGrant, error)
	CloseRegistryGeneration(context.Context, raftstore.RegistryLayout) (raftstore.SystemState, error)
	ConfirmPredecessorPermitDrain(context.Context, string) (raftstore.SystemState, error)
	BeginRecovery(context.Context) (raftstore.SystemState, error)
	ConfirmRecoveryPermitDrain(context.Context) (raftstore.SystemState, error)
	AdvanceRecovery(context.Context, raftstore.RecoveryPhase, raftstore.RecoveryPhase) (raftstore.SystemState, error)
	SetServingGates(context.Context, raftstore.GateUpdate) (raftstore.SystemState, error)
}

func NewSystemRuntimeHandler(runtime systemReplicaRuntime) (http.Handler, error) {
	if runtime == nil {
		return nil, errors.New("controlplane: System runtime handler requires a local replica")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != SystemRuntimePath {
			http.NotFound(w, request)
			return
		}
		var input systemRPCRequest
		if err := decodeBoundedJSON(request.Body, &input, maximumSystemRPC); err != nil {
			writeSessionJSON(w, http.StatusBadRequest, systemRPCResponse{Error: "invalid System runtime request"})
			return
		}
		response := executeSystemOperation(request.Context(), runtime, input)
		writeSessionJSON(w, http.StatusOK, response)
	}), nil
}

func executeSystemOperation(ctx context.Context, runtime systemReplicaRuntime, request systemRPCRequest) systemRPCResponse {
	var (
		state  raftstore.SystemState
		apply  raftstore.SystemApplyResult
		permit raftstore.PermitGrant
		err    error
	)
	switch request.Operation {
	case systemRead:
		state, err = runtime.ReadSystemStrong(ctx)
	case systemApply:
		if request.Command == nil {
			err = errors.New("System apply command is required")
		} else {
			apply, err = runtime.ApplySystem(ctx, *request.Command)
		}
	case systemPermit:
		permit, err = runtime.RefreshPermit(ctx)
	case systemClose:
		if request.Successor == nil {
			err = errors.New("successor registryLayout is required")
		} else {
			state, err = runtime.CloseRegistryGeneration(ctx, *request.Successor)
		}
	case systemDrain:
		state, err = runtime.ConfirmPredecessorPermitDrain(ctx, request.EvidenceDigest)
	case systemBeginRecover:
		state, err = runtime.BeginRecovery(ctx)
	case systemRecoverDrain:
		state, err = runtime.ConfirmRecoveryPermitDrain(ctx)
	case systemAdvance:
		state, err = runtime.AdvanceRecovery(ctx, request.RecoveryFrom, request.RecoveryTo)
	case systemGates:
		if request.Gates == nil {
			err = errors.New("serving gates are required")
		} else {
			state, err = runtime.SetServingGates(ctx, *request.Gates)
		}
	default:
		err = errors.New("unsupported System runtime operation")
	}
	if err != nil {
		return systemRPCResponse{Error: err.Error()}
	}
	response := systemRPCResponse{}
	switch request.Operation {
	case systemApply:
		response.Apply = &apply
	case systemPermit:
		response.Permit = &permit
	default:
		response.State = &state
	}
	return response
}

type RemoteSystemClient struct {
	peers []SessionPeer
}

func NewRemoteSystemClient(peers []SessionPeer) (*RemoteSystemClient, error) {
	if len(peers) != int(raftstore.DefaultReplication) {
		return nil, errors.New("controlplane: remote System client requires the exact three replicas")
	}
	seen := make(map[string]struct{}, len(peers))
	copyPeers := make([]SessionPeer, len(peers))
	for index, peer := range peers {
		peer.Endpoint = strings.TrimRight(peer.Endpoint, "/")
		if peer.MemberID == "" || peer.Endpoint == "" || peer.Client == nil {
			return nil, errors.New("controlplane: incomplete remote System replica")
		}
		if _, duplicate := seen[peer.MemberID]; duplicate {
			return nil, errors.New("controlplane: duplicate remote System replica")
		}
		seen[peer.MemberID] = struct{}{}
		copyPeers[index] = peer
	}
	return &RemoteSystemClient{peers: copyPeers}, nil
}

func (c *RemoteSystemClient) ReadSystemStrong(ctx context.Context) (raftstore.SystemState, error) {
	response, err := c.call(ctx, systemRPCRequest{Operation: systemRead})
	if err != nil || response.State == nil {
		return raftstore.SystemState{}, errors.Join(err, errors.New("controlplane: System read returned no state"))
	}
	return *response.State, nil
}

func (c *RemoteSystemClient) ApplySystem(ctx context.Context, command raftstore.SystemCommand) (raftstore.SystemApplyResult, error) {
	response, err := c.call(ctx, systemRPCRequest{Operation: systemApply, Command: &command})
	if err != nil || response.Apply == nil {
		return raftstore.SystemApplyResult{}, errors.Join(err, errors.New("controlplane: System apply returned no result"))
	}
	return *response.Apply, nil
}

func (c *RemoteSystemClient) RefreshPermit(ctx context.Context) (raftstore.PermitGrant, error) {
	response, err := c.call(ctx, systemRPCRequest{Operation: systemPermit})
	if err != nil || response.Permit == nil {
		return raftstore.PermitGrant{}, errors.Join(err, errors.New("controlplane: System refresh returned no Permit"))
	}
	return *response.Permit, nil
}

func (c *RemoteSystemClient) CloseRegistryGeneration(ctx context.Context, successor raftstore.RegistryLayout) (raftstore.SystemState, error) {
	return c.stateCall(ctx, systemRPCRequest{Operation: systemClose, Successor: &successor})
}

func (c *RemoteSystemClient) ConfirmPredecessorPermitDrain(ctx context.Context, digest string) (raftstore.SystemState, error) {
	return c.stateCall(ctx, systemRPCRequest{Operation: systemDrain, EvidenceDigest: digest})
}

func (c *RemoteSystemClient) BeginRecovery(ctx context.Context) (raftstore.SystemState, error) {
	return c.stateCall(ctx, systemRPCRequest{Operation: systemBeginRecover})
}

func (c *RemoteSystemClient) ConfirmRecoveryPermitDrain(ctx context.Context) (raftstore.SystemState, error) {
	return c.stateCall(ctx, systemRPCRequest{Operation: systemRecoverDrain})
}

func (c *RemoteSystemClient) AdvanceRecovery(ctx context.Context, from, to raftstore.RecoveryPhase) (raftstore.SystemState, error) {
	return c.stateCall(ctx, systemRPCRequest{Operation: systemAdvance, RecoveryFrom: from, RecoveryTo: to})
}

func (c *RemoteSystemClient) SetServingGates(ctx context.Context, gates raftstore.GateUpdate) (raftstore.SystemState, error) {
	return c.stateCall(ctx, systemRPCRequest{Operation: systemGates, Gates: &gates})
}

func (c *RemoteSystemClient) stateCall(ctx context.Context, request systemRPCRequest) (raftstore.SystemState, error) {
	response, err := c.call(ctx, request)
	if err != nil || response.State == nil {
		return raftstore.SystemState{}, errors.Join(err, errors.New("controlplane: System operation returned no state"))
	}
	return *response.State, nil
}

func (c *RemoteSystemClient) call(ctx context.Context, input systemRPCRequest) (systemRPCResponse, error) {
	if c == nil || len(c.peers) == 0 {
		return systemRPCResponse{}, errors.New("controlplane: remote System replicas are unavailable")
	}
	var joined error
	for _, peer := range c.peers {
		var response systemRPCResponse
		if err := postBoundedJSON(ctx, peer, SystemRuntimePath, input, &response, maximumSystemRPC); err != nil {
			joined = errors.Join(joined, fmt.Errorf("member %s: %w", peer.MemberID, err))
			continue
		}
		if response.Error != "" {
			joined = errors.Join(joined, fmt.Errorf("member %s: %s", peer.MemberID, response.Error))
			continue
		}
		return response, nil
	}
	return systemRPCResponse{}, errors.Join(errors.New("controlplane: System Group RPC failed on every replica"), joined)
}

func postBoundedJSON(ctx context.Context, peer SessionPeer, path string, input, output any, limit int64) error {
	raw, err := json.Marshal(input)
	if err != nil {
		return err
	}
	if int64(len(raw)) > limit {
		return errors.New("controlplane: internal RPC request exceeds its byte limit")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, peer.Endpoint+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := peer.Client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return fmt.Errorf("controlplane: internal RPC returned %s", response.Status)
	}
	return decodeBoundedJSON(response.Body, output, limit)
}

func decodeBoundedJSON(reader io.Reader, target any, limit int64) error {
	decoder := json.NewDecoder(io.LimitReader(reader, limit+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("controlplane: internal RPC has trailing JSON")
	}
	return nil
}
