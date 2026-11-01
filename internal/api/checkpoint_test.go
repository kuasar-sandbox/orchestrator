package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestPauseReturnsRetainedCheckpointID(t *testing.T) {
	core := &checkpointCoreStub{checkpointID: "capture-1", pause: func(_ context.Context, _, _ string, request sandboxcfg.CaptureRequest) error {
		if !request.Checkpoint || request.IfIdle || request.Kind != types.CaptureSandbox {
			t.Fatalf("request=%+v", request)
		}
		return nil
	}}
	handler, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(`{"memory":false,"checkpoint":true}`), nil)
	var body map[string]string
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if response.Code != 200 || len(body) != 1 || body["checkpointID"] != "capture-1" {
		t.Fatalf("response=%d %+v", response.Code, body)
	}
}

type retainedCheckpointCore struct {
	Core
	exportID, deleteSID, deleteID string
}

func (c *retainedCheckpointCore) DeleteCheckpoint(_ context.Context, _ string, sid, id string) error {
	c.deleteSID, c.deleteID = sid, id
	return nil
}
func (c *retainedCheckpointCore) ExportSandbox(_ context.Context, _, _ string, _, _ bool, id string) (types.ExportResult, error) {
	c.exportID = id
	return types.ExportResult{}, ErrNotFound
}

func TestCheckpointEndpointSelectionAndDeletion(t *testing.T) {
	core := &retainedCheckpointCore{}
	handler, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/export", strings.NewReader(`{"checkpointID":"capture-1","toTemplate":true}`), nil)
	if response.Code != 404 || core.exportID != "capture-1" {
		t.Fatalf("export=%d %q", response.Code, core.exportID)
	}
	response = migrationRequest(t, handler, key, http.MethodDelete, "/sandboxes/sid/checkpoints/capture-1", nil, nil)
	if response.Code != 204 || core.deleteSID != "sid" || core.deleteID != "capture-1" {
		t.Fatalf("delete=%d %s %s", response.Code, core.deleteSID, core.deleteID)
	}
	core.exportID = ""
	response = migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/export", strings.NewReader(`{"checkpointID":123}`), nil)
	if response.Code != 400 || core.exportID != "" {
		t.Fatal("invalid checkpoint selector fell through to parent export")
	}
}
