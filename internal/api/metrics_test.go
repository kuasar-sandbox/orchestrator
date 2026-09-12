package api

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

// Every lifecycle method is a nil embedded interface: invoking one panics.
type metricsOwnershipCore struct {
	Core
	key  string
	gets int
}

func (c *metricsOwnershipCore) Get(_ context.Context, id, key string) (*types.Sandbox, error) {
	c.gets++
	if id != "sandbox-id" || key != c.key {
		return nil, ErrNotFound
	}
	return &types.Sandbox{ID: id, State: types.StatePaused}, nil
}

func TestMetricsAuthenticationOwnershipAndPausedRead(t *testing.T) {
	key, err := apikey.Mint([]byte("owner"))
	if err != nil {
		t.Fatal(err)
	}
	other, err := apikey.Mint([]byte("other"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, id, key string
		want          int
		gets          int
	}{
		{"missing-auth", "sandbox-id", "", 401, 0},
		{"bad-auth", "sandbox-id", "malformed", 401, 0},
		{"wrong-owner", "sandbox-id", other, 404, 1},
		{"missing-sandbox", "missing", key, 404, 1},
		{"no-stable-fallback", "stable-id", key, 404, 1},
		{"paused", "sandbox-id", key, 206, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			core := &metricsOwnershipCore{key: key}
			handler := New(core, "test", Resources{}, slog.New(slog.NewTextHandler(io.Discard, nil))).WithMetricsProxy(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.PathValue("id") != "sandbox-id" || r.URL.RawQuery != "start=invalid&end=opaque" {
					t.Fatal("API interpreted metrics query")
				}
				w.WriteHeader(206)
				_, _ = w.Write([]byte("opaque"))
			})).Handler()
			req := httptest.NewRequest(http.MethodGet, "/sandboxes/"+tc.id+"/metrics?start=invalid&end=opaque", nil)
			req.Header.Set("X-API-KEY", tc.key)
			req.Header.Set(MigrationTokenHeader, "must-not-import-or-resume")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != tc.want || core.gets != tc.gets {
				t.Fatalf("status=%d gets=%d", response.Code, core.gets)
			}
		})
	}
	core := &metricsOwnershipCore{key: key}
	handler := New(core, "test", Resources{}, slog.Default()).Handler()
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/sandbox-id/metrics", nil)
	req.Header.Set("Authorization", "Bearer "+key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	if response.Code != 503 || core.gets != 1 {
		t.Fatalf("no telemetry: %d gets=%d", response.Code, core.gets)
	}
}
