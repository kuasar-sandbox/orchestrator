package api

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

type importOptionsCore struct {
	Core
	call func(ImportOptions) (string, error)
}

func (c *importOptionsCore) ImportSandbox(_ context.Context, _, _, _ string, options ImportOptions) (string, error) {
	return c.call(options)
}

func TestImportPreservesMemoryPresence(t *testing.T) {
	for _, tc := range []struct {
		name, body     string
		present, value bool
		status         int
	}{
		{"absent", `{"token":"kmt1.test"}`, false, false, http.StatusOK},
		{"false", `{"token":"kmt1.test","memory":false}`, true, false, http.StatusOK},
		{"true", `{"token":"kmt1.test","memory":true}`, true, true, http.StatusOK},
		{"invalid", `{"token":"kmt1.test","memory":"false"}`, false, false, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			core := &importOptionsCore{call: func(options ImportOptions) (string, error) {
				called = true
				if (options.Memory != nil) != tc.present || (options.Memory != nil && *options.Memory != tc.value) {
					t.Fatalf("memory presence/value changed: %+v", options)
				}
				return "target", nil
			}}
			handler, key := newMigrationTestHandler(t, core)
			response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/import", strings.NewReader(tc.body), nil)
			if response.Code != tc.status || called != (tc.status == http.StatusOK) {
				t.Fatalf("status=%d called=%t body=%s", response.Code, called, response.Body.String())
			}
		})
	}
}

func TestImportMemoryUnavailableIsConflict(t *testing.T) {
	core := &importOptionsCore{call: func(ImportOptions) (string, error) { return "", types.ErrMemoryUnavailable }}
	handler, key := newMigrationTestHandler(t, core)
	response := migrationRequest(t, handler, key, http.MethodPost, "/sandboxes/import", strings.NewReader(`{"token":"kmt1.test","memory":true}`), nil)
	if response.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
}
