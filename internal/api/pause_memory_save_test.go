package api

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestPausePreviousMemoryWaitOptionAndConflict(t *testing.T) {
	for _, noWait := range []bool{false, true} {
		body := `{}`
		if noWait {
			body = `{"noWaitForPreviousMemorySave":true}`
		}
		core := &checkpointCoreStub{pause: func(_ context.Context, _, _ string, request sandboxcfg.CaptureRequest) error {
			if request.NoWaitForPreviousMemorySave != noWait {
				t.Fatalf("no-wait request = %v", request.NoWaitForPreviousMemorySave)
			}
			return ErrPreviousMemorySavePending
		}}
		handler, key := newMigrationTestHandler(t, core)
		response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(body), nil)
		if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "previous checkpoint memory save") {
			t.Fatalf("pending response = %d %s", response.Code, response.Body.String())
		}
	}
}
