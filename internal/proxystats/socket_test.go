package proxystats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStatsServerRequiresReadySyncedExactRouteIdentity(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	synced := false
	identity := RouteIdentity{RunID: "run-1", Profile: types.ProfileBare, State: types.StatePaused}
	server := NewStatsServer(master, func() bool { return synced }, func(sandboxID string) (RouteIdentity, bool) {
		return identity, sandboxID == "s1"
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	do := func(query TrafficQuery) *httptest.ResponseRecorder {
		body, err := json.Marshal(BatchRequest{Sandboxes: []TrafficQuery{query}})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, BatchPath, bytes.NewReader(body))
		resp := httptest.NewRecorder()
		server.Handler().ServeHTTP(resp, req)
		return resp
	}
	query := TrafficQuery{SandboxID: "s1", RunID: "run-1", Profile: types.ProfileBare}
	if resp := do(query); resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("unsynced status=%d, want 503", resp.Code)
	}
	synced = true
	wrongRun := query
	wrongRun.RunID = "run-old"
	if resp := do(wrongRun); resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("RunID mismatch status=%d, want 503", resp.Code)
	}
	missingRun := query
	missingRun.RunID = ""
	if resp := do(missingRun); resp.Code != http.StatusBadRequest {
		t.Fatalf("missing RunID status=%d, want 400", resp.Code)
	}
	resp := do(query)
	if resp.Code != http.StatusOK || resp.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("valid status=%d cache=%q body=%s", resp.Code, resp.Header().Get("Cache-Control"), resp.Body.String())
	}
	var result BatchResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Sandboxes) != 1 || result.Sandboxes[0].SandboxID != "s1" || result.Sandboxes[0].Stats == nil {
		t.Fatalf("batch response = %+v", result)
	}
	if result.Sandboxes[0].Stats.State != string(types.StatePaused) || result.Sandboxes[0].Stats.IdleSince != nil {
		t.Fatalf("paused stats = %+v", result.Sandboxes[0].Stats)
	}
}

func TestQuerySocketRoundTrip(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	socket := filepath.Join(t.TempDir(), "stats.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewStatsServer(master, func() bool { return true }, func(sandboxID string) (RouteIdentity, bool) {
		return RouteIdentity{RunID: "run-1", Profile: types.ProfileE2B, State: types.StateRunning}, sandboxID == "s1"
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()

	results, err := QuerySocket(context.Background(), socket, []TrafficQuery{{
		SandboxID: "s1", RunID: "run-1", Profile: types.ProfileE2B,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Stats == nil || len(results[0].Stats.Services) != 4 {
		t.Fatalf("query results = %+v", results)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stats server did not stop")
	}
}
