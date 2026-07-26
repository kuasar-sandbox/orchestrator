package registry

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
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
		return &Placement{NodeID: "n1", APISecretFingerprint: testAPIFingerprint}, nil
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

func TestServeReserveAcceptsCredentialsWithoutSendingThemToPlacer(t *testing.T) {
	placements := 0
	reg := New(NewStores(), placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		if _, found := req.Config[sandboxcfg.NsCredentials]; found {
			t.Fatalf("credentials leaked into placement config: %+v", req.Config)
		}
		return nil, ErrNoNode
	}), 0, nil)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.credentials":"{\"service_secret\":\"` + strings.Repeat("1", 64) + `\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%q, want 503 after accepted config reached placement", rec.Code, rec.Body.String())
	}
	if placements != 1 {
		t.Fatalf("accepted credentials reached placement %d times, want 1", placements)
	}
}

func TestServeReserveRejectsInvalidCredentialsBeforePlacement(t *testing.T) {
	placements := 0
	reg := New(NewStores(), placementFunc(func(context.Context, PlaceRequest) (*Placement, error) {
		placements++
		return nil, ErrNoNode
	}), 0, nil)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.credentials":"{\"unknown\":\"value\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", rec.Code, rec.Body.String())
	}
	if placements != 0 {
		t.Fatalf("invalid credentials reached placement %d times", placements)
	}
}

func TestServeReserveRejectsCredentialsInvalidForPlacedProfile(t *testing.T) {
	reg := New(NewStores(), placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		if _, found := req.Config[sandboxcfg.NsCredentials]; found {
			t.Fatalf("credentials leaked into placement config: %+v", req.Config)
		}
		return &Placement{
			NodeID: "n1", TemplateRef: "bare-img-" + strings.Repeat("a", 64),
			APISecretFingerprint: testAPIFingerprint,
		}, nil
	}), 0, nil)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.credentials":"{\"envd_access_token\":\"e2b-only\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", rec.Code, rec.Body.String())
	}
	if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
		t.Fatalf("invalid profile credentials wrote route state: found=%v err=%v", found, err)
	}
}
