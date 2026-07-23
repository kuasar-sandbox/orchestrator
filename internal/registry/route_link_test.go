package registry

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServeRouteLinkOmitsRuntimeSnapshotEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	New(NewStores(), nil, 0, nil).ServeRouteLink(mux)

	for _, tc := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/route-link/export"},
		{method: http.MethodPost, path: "/route-link/import"},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		if _, pattern := mux.Handler(req); pattern != "" {
			t.Fatalf("%s remains mounted as %q", tc.path, pattern)
		}
	}

	req := httptest.NewRequest(http.MethodGet, RouteLinkListPath, nil)
	if _, pattern := mux.Handler(req); pattern != RouteLinkListPath {
		t.Fatalf("normal route-link API pattern=%q, want %q", pattern, RouteLinkListPath)
	}
}

func TestServeReserveRejectsInvalidRestoreBeforeReservation(t *testing.T) {
	mux := http.NewServeMux()
	New(NewStores(), nil, 0, nil).ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.restore":"{\"prefetch\":\"disk\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", rec.Code, rec.Body.String())
	}
}

func TestServeReserveRejectsNonRestoreConfigBeforeReservation(t *testing.T) {
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return &Placement{NodeID: "n1"}, nil
	}), 0, nil)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}","kuasar-sandbox.network":"{}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", rec.Code, rec.Body.String())
	}
	if placements != 0 {
		t.Fatalf("unsupported config reached placement %d times", placements)
	}
	if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
		t.Fatalf("unsupported config wrote route state: found=%v err=%v", found, err)
	}
}

func TestServeReserveReportsConcurrentRestoreConflict(t *testing.T) {
	reg := New(NewStores(), nil, 0, nil)
	call := &reserveCall{done: make(chan struct{}), restoreMode: "off"}
	close(call.done)
	reg.inflight[flightKey("/g", "rk")] = call
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status=%d body=%q, want 409", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(RouteLinkErrorHeader); got != RouteLinkRestoreConflict {
		t.Fatalf("%s=%q, want %q", RouteLinkErrorHeader, got, RouteLinkRestoreConflict)
	}
}
