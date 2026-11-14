package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"

	"github.com/kuasar-sandbox/orchestrator/internal/strictjson"
	"github.com/kuasar-sandbox/orchestrator/internal/workspace"
)

const RunGenerationHeader = "X-Kuasar-Run-Generation"

var (
	ErrWorkspaceConflict    = errors.New("conflicting workspace state or source")
	ErrWorkspaceGeneration  = errors.New("sandbox run generation changed")
	ErrWorkspaceUnavailable = errors.New("workspace state temporarily unavailable")
)

type WorkspaceStatus struct {
	Name      string            `json:"name"`
	Mounts    []workspace.Mount `json:"mounts"`
	State     string            `json:"state"`
	SizeBytes *int64            `json:"sizeBytes,omitempty"`
}
type WorkspaceList struct {
	RunGeneration string            `json:"runGeneration,omitempty"`
	Workspaces    []WorkspaceStatus `json:"workspaces"`
}
type workspaceCore interface {
	ListWorkspaces(context.Context, string, string) (WorkspaceList, error)
	AttachWorkspace(context.Context, string, string, string, string, json.RawMessage) error
	ExportWorkspace(context.Context, string, string, string) (string, error)
}

func (a *API) listWorkspaces(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	out, err := a.core.ListWorkspaces(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()))
	if err != nil {
		a.fail(w, err)
		return
	}
	if out.Workspaces == nil {
		out.Workspaces = []WorkspaceStatus{}
	}
	writeJSON(w, http.StatusOK, out)
}

func workspaceName(w http.ResponseWriter, r *http.Request) (string, bool) {
	q, err := url.ParseQuery(r.URL.RawQuery)
	names := q["name"]
	if err != nil || len(q) != 1 || len(names) != 1 || names[0] == "" {
		writeErr(w, 400, "one workspace name is required")
		return "", false
	}
	return names[0], true
}

func decodeWorkspaceBody(w http.ResponseWriter, r *http.Request, out any, limit int64) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		writeErr(w, 413, "request body too large")
		return false
	}
	if err != nil || strictjson.Decode(raw, out) != nil {
		writeErr(w, 400, "invalid workspace request body")
		return false
	}
	return true
}

func (a *API) attachWorkspace(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	name, ok := workspaceName(w, r)
	if !ok {
		return
	}
	gen, err := singleOptionalHeader(r.Header, RunGenerationHeader)
	if err != nil || (gen != nil && *gen == "") {
		writeErr(w, 400, "invalid run generation")
		return
	}
	generation := ""
	if gen != nil {
		generation = *gen
	}
	var body map[string]json.RawMessage
	if !decodeWorkspaceBody(w, r, &body, maxCreateRequestBytes) {
		return
	}
	disk := bytes.TrimSpace(body["disk"])
	if len(body) != 1 || len(disk) == 0 || disk[0] != '{' || workspace.ValidateDisk(disk) != nil {
		writeErr(w, 400, "invalid workspace disk")
		return
	}
	if err = a.core.AttachWorkspace(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), name, generation, disk); err != nil {
		a.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) exportWorkspace(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	name, ok := workspaceName(w, r)
	if !ok {
		return
	}
	if !decodeWorkspaceBody(w, r, &struct{}{}, maxPauseRequestBytes) {
		return
	}
	token, err := a.core.ExportWorkspace(r.Context(), r.PathValue("id"), apiKeyFrom(r.Context()), name)
	if err != nil {
		a.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}
