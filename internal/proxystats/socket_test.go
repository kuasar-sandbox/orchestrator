package proxystats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	publicconfig "github.com/kuasar-sandbox/orchestrator/config"
	"github.com/kuasar-sandbox/orchestrator/internal/api"
	"github.com/kuasar-sandbox/orchestrator/internal/metrics"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestStatsServerRequiresReadySyncedExactRouteIdentity(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	readyWorker(t, master, "w0", 1)
	synced := false
	identity := RouteIdentity{
		RunID: "run-1", Profile: types.ProfileBare, State: types.StatePaused,
		MaxInflight: publicconfig.MaxInflight{Total: 32, Forward: 8, Exec: 2},
	}
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
	query := TrafficQuery{SandboxID: "s1", RunID: "run-1", Profile: types.ProfileBare, State: types.StatePaused}
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
	if resp := do(missingRun); resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("missing RunID mismatch status=%d, want 503", resp.Code)
	}
	identity = RouteIdentity{Profile: types.ProfileBare, State: types.StateStarting}
	missingRun.State = types.StateStarting
	observeZero(master, "s1", "", types.StateStarting)
	resp := do(missingRun)
	if resp.Code != http.StatusOK {
		t.Fatalf("pre-assignment starting status=%d, want 200: %s", resp.Code, resp.Body.String())
	}
	var startingResult BatchResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &startingResult); err != nil {
		t.Fatal(err)
	}
	if got := startingResult.Sandboxes[0].Stats; got.State != string(types.StateStarting) || got.IdleSince != nil {
		t.Fatalf("starting stats = %+v", got)
	}
	identity = RouteIdentity{
		RunID: "run-1", Profile: types.ProfileBare, State: types.StatePaused,
		MaxInflight: publicconfig.MaxInflight{Total: 32, Forward: 8, Exec: 2},
	}
	observeZero(master, "s1", "run-1", types.StatePaused)
	staleState := query
	staleState.State = types.StateRunning
	if resp := do(staleState); resp.Code != http.StatusServiceUnavailable {
		t.Fatalf("state mismatch status=%d, want 503", resp.Code)
	}
	resp = do(query)
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
	if result.Sandboxes[0].Stats.MaxInflight != identity.MaxInflight {
		t.Fatalf("effective maxInflight = %+v, want %+v", result.Sandboxes[0].Stats.MaxInflight, identity.MaxInflight)
	}
}

func TestQuerySocketRoundTrip(t *testing.T) {
	master := NewMasterStats(metrics.New(), []string{"w0"})
	var restarting atomic.Bool
	master.InplaceRestart = restarting.Load
	readyWorker(t, master, "w0", 1)
	observeZero(master, "s1", "run-1", types.StateRunning)
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
		SandboxID: "s1", RunID: "run-1", Profile: types.ProfileE2B, State: types.StateRunning,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].PhysicalOnly || results[0].Stats == nil || len(results[0].Stats.Services) != 5 {
		t.Fatalf("query results = %+v", results)
	}
	for _, active := range []bool{true, false} {
		restarting.Store(active)
		results, err := QuerySocket(ctx, socket, []TrafficQuery{{SandboxID: "s1", RunID: "run-1", Profile: types.ProfileE2B, State: types.StateRunning}})
		if err != nil {
			t.Fatal(err)
		}
		if results[0].Stats.InplaceRestart != active {
			t.Fatalf("restart flag=%v want=%v", results[0].Stats.InplaceRestart, active)
		}
	}
	results, err = QuerySocket(WithPhysicalTelemetry(ctx), socket, []TrafficQuery{
		{SandboxID: "s1", RunID: "run-1", Profile: types.ProfileE2B, State: types.StateRunning},
	})
	if err != nil || len(results) != 1 || !results[0].PhysicalOnly || len(results[0].Stats.Services) != 4 {
		t.Fatalf("acknowledged physical query = %+v, err=%v", results, err)
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

func TestPhysicalTelemetryUsesBootOrderDuringPositiveHold(t *testing.T) {
	m := NewMasterStats(nil, []string{"w"})
	wall := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	m.now = func() timePoint { return timePoint{wall: wall, bootNS: 10} }
	m.trustSince = m.now()
	readyWorker(t, m, "w", 1)
	route := routesync.RouteEntry{SandboxID: "s", RunID: "r", State: routesync.StateRunning, Hold: &routesync.HoldActivity{Value: 3}}
	m.ObserveRoute(route)
	// exec closed first. The wall clock stepped back before forward closed.
	earlier := wall.Add(-time.Minute)
	if err := m.Receive("w", 1, Frame{Type: TypeUpdate, Version: Version, Epoch: 1, Sequence: 2, Traffic: []SandboxSnapshot{{
		SandboxID: "s", Services: map[string]ServiceSnapshot{
			"exec": {IdleSince: &wall, IdleSinceBootNS: 20}, "forward": {IdleSince: &earlier, IdleSinceBootNS: 30},
		},
	}}}); err != nil {
		t.Fatal(err)
	}
	server := NewStatsServer(m, func() bool { return true }, func(string) (RouteIdentity, bool) {
		return RouteIdentity{RunID: "r", Profile: types.ProfileBare, State: types.StateRunning}, true
	}, nil)
	request := func(view string) *httptest.ResponseRecorder {
		body := bytes.NewBufferString(`{"sandboxes":[{"sandboxID":"s","runID":"r","profile":"bare","state":"running"}]}`)
		r := httptest.NewRequest(http.MethodPost, BatchPath, body)
		if view != "" {
			r.Header.Set(trafficViewHeader, view)
		}
		w := httptest.NewRecorder()
		server.Handler().ServeHTTP(w, r)
		return w
	}
	readStats := func(view string) *api.TrafficStats {
		t.Helper()
		w := request(view)
		var batch BatchResponse
		if w.Code != http.StatusOK || w.Header().Get(trafficViewHeader) != view || json.Unmarshal(w.Body.Bytes(), &batch) != nil || len(batch.Sandboxes) != 1 {
			t.Fatalf("view %q: HTTP %d, header %q, body %s", view, w.Code, w.Header().Get(trafficViewHeader), w.Body)
		}
		return batch.Sandboxes[0].Stats
	}
	if physical := readStats(physicalTrafficView); len(physical.Services) != 2 || physical.Inflight.Connected != 0 || physical.IdleSince == nil || !physical.IdleSince.Equal(earlier) {
		t.Fatalf("physical snapshot used wall order or counted hold: %+v", physical)
	}
	if logical := readStats(""); logical.Services["hold"].Connected != 3 || logical.Inflight.Connected != 3 || logical.IdleSince != nil {
		t.Fatalf("logical snapshot lost active hold: %+v", logical)
	}
	m.InplaceRestart = func() bool { return true }
	if partial := readStats(physicalTrafficView); !partial.InplaceRestart || partial.IdleSince != nil {
		t.Fatalf("partial physical snapshot claimed complete idle: %+v", partial)
	}
	route.Hold = nil
	m.ObserveRoute(route)
	if w := request(physicalTrafficView); w.Code != http.StatusServiceUnavailable || w.Header().Get(trafficViewHeader) != "" {
		t.Fatalf("untrusted negotiated route: HTTP %d, header %v", w.Code, w.Header())
	}
}

func TestQuerySocketPhysicalTelemetryLegacyProxy(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "stats.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(trafficViewHeader) != physicalTrafficView {
			t.Error("physical query did not request its view")
		}
		// Released Proxy decodes the existing JSON strictly and ignores the header.
		var batch BatchRequest
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&batch); err != nil || len(batch.Sandboxes) != 1 {
			http.Error(w, "invalid v11 request", http.StatusBadRequest)
			return
		}
		_, _ = io.WriteString(w, `{"sandboxes":[{"sandboxID":"s","stats":{"services":{"forward":{"connected":1}}}}]}`)
	})}
	go server.Serve(listener)
	defer server.Close()
	rows, err := QuerySocket(WithPhysicalTelemetry(t.Context()), socket, []TrafficQuery{
		{SandboxID: "s", RunID: "r", Profile: types.ProfileBare, State: types.StateRunning},
	})
	if err != nil || len(rows) != 1 || rows[0].PhysicalOnly || rows[0].Stats.Services["forward"].Connected != 1 {
		t.Fatalf("legacy response = %+v, err=%v", rows, err)
	}
}

func TestQuerySocketTransportFailureIsUnavailable(t *testing.T) {
	_, err := QuerySocket(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), []TrafficQuery{{
		SandboxID: "s1", RunID: "run-1", Profile: types.ProfileE2B, State: types.StateRunning,
	}})
	if !errors.Is(err, api.ErrStatsUnavailable) {
		t.Fatalf("transport failure error = %v, want ErrStatsUnavailable", err)
	}
}
