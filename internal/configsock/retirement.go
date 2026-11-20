package configsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
)

const FeatureCheckpointHolderRetirementV1 = "checkpoint_holder_retirement_v1"

var ErrRunRetirementUnsupported = errors.New("runner retirement unsupported")

// RunRetirementEvent identifies one committed retirement, not a reusable
// request to stop whichever sandbox currently occupies a shared path.
type RunRetirementEvent struct {
	OperationID  string `json:"operation_id"`
	SandboxID    string `json:"sandbox_id"`
	InstanceID   string `json:"instance_id"`
	RunID        string `json:"run_id"`
	CheckpointID string `json:"checkpoint_id"`
	MemoryID     string `json:"memory_id"`
	Epoch        uint64 `json:"epoch"`
}

func (e RunRetirementEvent) Valid() bool {
	return e.OperationID != "" && e.SandboxID != "" && e.InstanceID != "" && e.RunID != "" && e.CheckpointID != "" && e.MemoryID != "" && e.Epoch != 0
}

type RunRoleRequest struct {
	Kind       string              `json:"kind"`
	RunID      string              `json:"run_id"`
	Retirement *RunRetirementEvent `json:"retirement,omitempty"` // nil queries; present acknowledges
}

type RunRoleResponse struct {
	Pending      bool                `json:"pending,omitempty"`
	Retirement   *RunRetirementEvent `json:"retirement,omitempty"`
	Acknowledged bool                `json:"acknowledged,omitempty"`
	Error        string              `json:"error,omitempty"`
}

// Optional extension keeps embedding providers without managed holders on the
// original session protocol. A missing extension never advertises capability.
type RunRetirementProvider interface {
	RegisterRunSessionFeatures(context.Context, string, string, []string) (RunSessionRegistration, bool, error)
	RunRetirementRole(context.Context, string, string, *RunRetirementEvent) (RunRoleResponse, error)
}

type RunRetirementSession interface {
	Retirements() <-chan RunRetirementEvent
}

func (s *Server) handleRunRole(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok {
		writeJSON(w, http.StatusForbidden, RunRoleResponse{Error: "no peer credentials"})
		return
	}
	var req RunRoleRequest
	if status := decodeTaskRequest(w, r, 8192, &req); status != 0 || req.Kind == "" || req.RunID == "" {
		writeJSON(w, http.StatusBadRequest, RunRoleResponse{Error: "bad request"})
		return
	}
	pidFile, known := s.deps.Provider.RunPidFile(req.Kind, req.RunID)
	if !known || !s.taskAuthed("run-session:"+req.Kind+":"+req.RunID, pidFile, peer) {
		writeJSON(w, http.StatusForbidden, RunRoleResponse{Error: "not authorized"})
		return
	}
	provider, ok := s.deps.Provider.(RunRetirementProvider)
	if !ok {
		writeJSON(w, http.StatusNotImplemented, RunRoleResponse{Error: "runner retirement unsupported"})
		return
	}
	if req.Retirement != nil && (!req.Retirement.Valid() || req.Retirement.RunID != req.RunID) {
		writeJSON(w, http.StatusBadRequest, RunRoleResponse{Error: "invalid retirement identity"})
		return
	}
	out, err := provider.RunRetirementRole(r.Context(), req.Kind, req.RunID, req.Retirement)
	if err != nil {
		writeJSON(w, http.StatusConflict, RunRoleResponse{Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func ReadRunRole(ctx context.Context, socket, kind, runID string) (RunRoleResponse, error) {
	return runRoleRequest(ctx, socket, RunRoleRequest{Kind: kind, RunID: runID})
}

func AcknowledgeRunRetirement(ctx context.Context, socket, kind string, event RunRetirementEvent) error {
	out, err := runRoleRequest(ctx, socket, RunRoleRequest{Kind: kind, RunID: event.RunID, Retirement: &event})
	if err == nil && (!out.Acknowledged || out.Retirement == nil || *out.Retirement != event) {
		return errors.New("configsock: retirement acknowledgement identity mismatch")
	}
	return err
}

func runRoleRequest(ctx context.Context, socket string, input RunRoleRequest) (RunRoleResponse, error) {
	body, _ := json.Marshal(input)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+PathRunSession, bytes.NewReader(body))
	if err != nil {
		return RunRoleResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := HTTPClient(socket).Do(req)
	if err != nil {
		return RunRoleResponse{}, &transportError{err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed || resp.StatusCode == http.StatusNotImplemented {
		return RunRoleResponse{}, ErrRunRetirementUnsupported
	}
	var out RunRoleResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return out, &transportError{err: err}
	}
	if resp.StatusCode >= http.StatusBadRequest || out.Error != "" {
		return out, buildResponseError(resp.StatusCode, out.Error)
	}
	return out, nil
}
