package registry

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/migrationtoken"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestServeRouteReturnsProtectedExplicitCredentials(t *testing.T) {
	ctx := context.Background()
	reg := New(NewStores(), nil, 0, nil)
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:8443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1"})
	want := testE2BSandboxRecord("/g", "rk", "sb-route", "n1", StateReady)
	if _, err := reg.stores.PutSandbox(ctx, want); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	req := httptest.NewRequest(http.MethodGet, RouteLinkRoutePath+"?group=/g&route_key=rk&sid=sb-route", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	if bytes.Contains(rec.Body.Bytes(), []byte(`"access_token":`)) {
		t.Fatal("protected route retained generic access_token")
	}
	var got RouteResolve
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.SandboxID != want.SandboxID || got.NodeSandboxID != want.NodeSandboxID ||
		got.Profile != want.Profile || got.DataEndpoint != "127.0.0.1:8443" ||
		got.RouteRevision <= 0 ||
		got.AuthSandboxID != want.AuthSandboxID || got.APISecret != want.APISecret ||
		got.APISecretFingerprint != want.APISecretFingerprint ||
		got.ManifestKeyFingerprint != want.ManifestKeyFingerprint ||
		got.ServiceSecret != want.ServiceSecret || got.EnvdAccessToken != want.EnvdAccessToken ||
		got.TrafficAccessToken != want.TrafficAccessToken ||
		got.ForwardAccessToken != want.ForwardAccessToken {
		t.Fatal("protected route omitted or changed an explicit credential field")
	}
}

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
	reg := New(NewStores(), nil, 0, nil)
	enableTestCreateAuth(t, reg)
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.restore":"{\"prefetch\":\"disk\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
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
	enableTestCreateAuth(t, reg)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.restore":"{\"prefetch\":\"memory\"}","kuasar-sandbox.network":"{}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
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

func TestServeReserveAcceptsMMDSConfig(t *testing.T) {
	placements := 0
	reg := New(NewStores(), placementFunc(func(_ context.Context, req PlaceRequest) (*Placement, error) {
		placements++
		if _, found := req.Config[sandboxcfg.NsMMDS]; !found {
			t.Fatalf("mmds specification did not reach placement config: %+v", req.Config)
		}
		return nil, ErrNoNode
	}), 0, nil)
	enableTestCreateAuth(t, reg)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.mmds":"{\"version\":1,\"routes\":[{\"path\":\"/x\",\"type\":\"static\",\"data\":\"d\"}]}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	// A pre-Phase-1 allowlist rejected any config key other than restore/
	// credentials with 400 before reservation ever ran -- confirm the mmds
	// specification now reaches placement (503 from ErrNoNode) instead.
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%q, want 503 after accepted config reached placement", rec.Code, rec.Body.String())
	}
	if placements != 1 {
		t.Fatalf("accepted mmds config reached placement %d times, want 1", placements)
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
	enableTestCreateAuth(t, reg)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.credentials":"{\"service_secret\":\"` + strings.Repeat("1", 64) + `\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
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
	enableTestCreateAuth(t, reg)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.credentials":"{\"unknown\":\"credential-secret-sentinel\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "credential-secret-sentinel") {
		t.Fatalf("invalid credentials value leaked into error: %q", rec.Body.String())
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
			NodeID: "n1", TemplateRef: types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String(),
			APISecretFingerprint: testAPIFingerprint,
		}, nil
	}), 0, nil)
	enableTestCreateAuth(t, reg)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	body := []byte(`{"config":{"kuasar-sandbox.credentials":"{\"envd_access_token\":\"e2b-only\"}"}}`)
	req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "e2b-only") {
		t.Fatalf("profile-invalid credentials leaked into error: %q", rec.Body.String())
	}
	if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
		t.Fatalf("invalid profile credentials wrote route state: found=%v err=%v", found, err)
	}
}

func TestServeReserveMapsOperationErrorsWithoutLifecycleSideEffects(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-route", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)

	tests := []struct {
		name       string
		path       string
		body       string
		apiKey     string
		access     string
		migration  string
		wantStatus int
	}{
		{name: "missing operation", path: "?group=/g&route_key=rk", wantStatus: http.StatusBadRequest},
		{name: "missing create credential", path: "?group=/g&route_key=new&operation=create", wantStatus: http.StatusUnauthorized},
		{name: "wrong create credential", path: "?group=/g&route_key=new&operation=create", apiKey: "e2b_bad", wantStatus: http.StatusForbidden},
		{name: "expected identity mismatch", path: "?group=/g&route_key=rk&operation=connect&sid=sb-other", apiKey: testAPIKeyValue(), wantStatus: http.StatusNotFound},
		{name: "connected node unavailable", path: "?group=/g&route_key=rk&operation=connect&sid=sb-route", apiKey: testAPIKeyValue(), wantStatus: http.StatusServiceUnavailable},
		{name: "wrong data credential", path: "?group=/g&route_key=rk&operation=data&sid=sb-route&port=8080", access: "wrong", wantStatus: http.StatusUnauthorized},
		{name: "connect body", path: "?group=/g&route_key=rk&operation=connect&sid=sb-route", body: `{}`, apiKey: testAPIKeyValue(), wantStatus: http.StatusBadRequest},
		{name: "overflowing connect timeout", path: fmt.Sprintf("?group=/g&route_key=rk&operation=connect&sid=sb-route&timeout=%d", routesync.MaxConnectTimeoutSeconds+1), apiKey: testAPIKeyValue(), wantStatus: http.StatusBadRequest},
		{name: "oversized migration token", path: "?group=/g&route_key=rk&operation=connect&sid=sb-route", apiKey: testAPIKeyValue(), migration: strings.Repeat("x", migrationtoken.MaxWireSize+1), wantStatus: http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+tc.path, body)
			req.Header.Set("X-API-KEY", tc.apiKey)
			req.Header.Set("X-Access-Token", tc.access)
			req.Header.Set("X-Kuasar-Migration-Token", tc.migration)
			resp := httptest.NewRecorder()
			mux.ServeHTTP(resp, req)
			if resp.Code != tc.wantStatus {
				t.Fatalf("status=%d body=%q, want %d", resp.Code, resp.Body.String(), tc.wantStatus)
			}
		})
	}
	stored, _, found, err := reg.stores.GetSandbox(ctx, record.Group, record.RouteKey)
	if err != nil || !found || stored.State != StatePaused || stored.NodeSandboxID != record.NodeSandboxID {
		t.Fatalf("failed route-link operations changed route: stored=%+v found=%v err=%v", stored, found, err)
	}
}

func TestServeReserveConnectReturnsNestedRouteAndTypedResult(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-route", "n1", StateReserved)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(record, cmd.SID),
		})
	}})
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	req := httptest.NewRequest(http.MethodPost,
		RouteLinkReservePath+"?group=/g&route_key=rk&operation=connect&sid=sb-route&timeout=37", nil)
	req.Header.Set("X-API-KEY", testAPIKeyValue())
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q", resp.Code, resp.Body.String())
	}
	var result ReserveResult
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Connect == nil || result.Connect.NodeSandboxID != record.NodeSandboxID ||
		result.Route.SandboxID != record.SandboxID || result.Route.NodeSandboxID != record.NodeSandboxID ||
		result.Route.RouteRevision <= 0 {
		t.Fatalf("reserve result=%+v", result)
	}
}

func TestServeReserveConnectPropagatesTypedNodeRejection(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	record := testE2BSandboxRecord("/g", "rk", "sb-route", "n1", StatePaused)
	if _, err := reg.stores.PutSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if err := reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckRejected,
			Reason: "migration credential not allowed", HTTPStatus: http.StatusForbidden,
		})
	}})
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	req := httptest.NewRequest(http.MethodPost,
		RouteLinkReservePath+"?group=/g&route_key=rk&operation=connect&sid=sb-route", nil)
	req.Header.Set("X-API-KEY", testAPIKeyValue())
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden || strings.TrimSpace(resp.Body.String()) != "migration credential not allowed" {
		t.Fatalf("status=%d body=%q, want typed node rejection", resp.Code, resp.Body.String())
	}
}
