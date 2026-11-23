package api

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
)

func TestPauseRejectsRemovedMemoryWaitOption(t *testing.T) {
	for _, field := range []string{"noWaitForPreviousMemorySave", "NoWaitForPreviousMemorySave", "NOWAITFORPREVIOUSMEMORYSAVE"} {
		for _, value := range []string{"true", "false", "null"} {
			t.Run(field+"/"+value, func(t *testing.T) {
				called := false
				core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CaptureRequest) error {
					called = true
					return nil
				}}
				handler, key := newMigrationTestHandler(t, core)
				body := fmt.Sprintf(`{%q:%s}`, field, value)
				response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(body), nil)
				if response.Code != http.StatusBadRequest || called {
					t.Fatalf("removed option response = %d %s, called=%t", response.Code, response.Body.String(), called)
				}
			})
		}
	}
}

func TestPausePreviousMemorySaveConflict(t *testing.T) {
	core := &checkpointCoreStub{pause: func(context.Context, string, string, sandboxcfg.CaptureRequest) error {
		return ErrPreviousMemorySavePending
	}}
	handler, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/sid/pause", strings.NewReader(`{}`), nil)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "previous checkpoint memory save") {
		t.Fatalf("pending response = %d %s", response.Code, response.Body.String())
	}
}
