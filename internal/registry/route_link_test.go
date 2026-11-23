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
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestServeDataRouteReturnsOnlyDataCredentials(t *testing.T) {
	ctx := context.Background()
	reg := New(newTestStores(t), 0, nil)
	if err := putTestNode(reg.stores, ctx, &NodeRecord{NodeID: "n1", APIEndpoint: "127.0.0.1:7443", DataEndpoint: "127.0.0.1:8443"}); err != nil {
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
		got.Profile != want.Profile || got.APIEndpoint != "" || got.DataEndpoint != "127.0.0.1:8443" ||
		!got.RouteRevision.IsCommitted() ||
		got.StableID != want.StableID || got.APISecret != "" ||
		got.APISecretFingerprint != "" || got.ManifestKeyFingerprint != "" ||
		got.ServiceSecret != want.ServiceSecret || got.EnvdAccessToken != want.EnvdAccessToken ||
		got.TrafficAccessToken != want.TrafficAccessToken ||
		got.ForwardAccessToken != want.ForwardAccessToken {
		t.Fatal("protected route omitted or changed an explicit credential field")
	}
}

func TestServeRouteLinkOmitsRuntimeSnapshotEndpoints(t *testing.T) {
	mux := http.NewServeMux()
	New(newTestStores(t), 0, nil).ServeRouteLink(mux)

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

func TestServeReserveBuildOrdinaryNegativeACKPreservesIntent(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			reg := testReg(t)
			seedTestNode(t, reg.stores, "n1")
			publishTestGroups(t, reg, "n1", "/g")
			reg.SetNodeOwner(&recordingNodeOwner{allow: true, ack: &routesync.CmdAck{
				Status: routesync.AckRejected, HTTPStatus: status, Reason: "ordinary node rejection",
			}})
			mux := http.NewServeMux()
			reg.ServeRouteLink(mux)
			body, err := json.Marshal(BuildReserveReq{
				Group: "/g", BuildID: "negative-ack", Profile: types.ProfileE2B, Resources: testWireBuildResources(),
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, RouteLinkReserveBuildPath, bytes.NewReader(body))
			req.Header.Set("X-API-KEY", testAPIKeyValue())
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "ordinary node rejection") {
				t.Fatalf("response = %d %q, want ambiguous result", rec.Code, rec.Body.String())
			}
			if record, found, err := reg.stores.GetBuildInGroup(t.Context(), "/g", "negative-ack"); err != nil || !found || record.State != BuildStarting {
				t.Fatalf("negative ACK erased ambiguous intent: %v %v", found, err)
			}
			if _, found, err := reg.stores.GetNodeBuildRef(t.Context(), "n1", "negative-ack"); err != nil || !found {
				t.Fatalf("negative ACK lost cleanup responsibility: %v %v", found, err)
			}
		})
	}
}

func TestServeReserveRejectsInvalidRestoreBeforeReservation(t *testing.T) {
	mux := http.NewServeMux()
	reg := New(newTestStores(t), 0, nil)
	if err := putTestNode(reg.stores, t.Context(), &NodeRecord{NodeID: "fixture", Draining: true}); err != nil {
		t.Fatal(err)
	}
	publishTestGroups(t, reg, "fixture", "/g")
	reads := countTestGroupReads(reg)
	_ = reads
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

func TestServeReserveConnectStrictlyBoundsTypedBody(t *testing.T) {
	reg := New(newTestStores(t), 0, nil)
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	for _, test := range []struct {
		name string
		body string
		want int
	}{
		{name: "unknown field", body: `{"unknown":true}`, want: http.StatusBadRequest},
		{name: "duplicate field", body: `{"memory":true,"memory":false}`, want: http.StatusBadRequest},
		{name: "malformed memory", body: `{"memory":"false"}`, want: http.StatusBadRequest},
		{name: "trailing value", body: `{ } { }`, want: http.StatusBadRequest},
		{name: "array", body: `[]`, want: http.StatusBadRequest},
		{name: "chunked over limit", body: strings.Repeat(" ", maxConnectReserveBody+1), want: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost,
				RouteLinkReservePath+"?group=/g&route_key=rk&operation=connect&sid=sb-route",
				strings.NewReader(test.body),
			)
			request.ContentLength = -1
			request.Header.Set("X-API-KEY", testAPIKeyValue())
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status=%d body=%q, want %d", response.Code, response.Body.String(), test.want)
			}
		})
	}
}

func TestServeReserveAcceptsResourceAndTrafficConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config map[string]string
	}{
		{name: "resource", config: map[string]string{sandboxcfg.NsResource: `{"capacity":{"cpu":2}}`}},
		{name: "traffic", config: map[string]string{sandboxcfg.NsTraffic: `{"max_inflight":{"total":3}}`}},
		{name: "both", config: map[string]string{
			sandboxcfg.NsResource: `{"allocatable":{"memory":"512MiB"}}`,
			sandboxcfg.NsTraffic:  `{"max_inflight":{"forward":0,"exec":2}}`,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := New(newTestStores(t), 0, nil)
			if err := putTestNode(reg.stores, t.Context(), &NodeRecord{NodeID: "fixture", Draining: true}); err != nil {
				t.Fatal(err)
			}
			publishTestGroups(t, reg, "fixture", "/g")
			reads := countTestGroupReads(reg)
			_ = reads
			mux := http.NewServeMux()
			reg.ServeRouteLink(mux)
			body, err := json.Marshal(SandboxReserveReq{Config: tc.config})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, RouteLinkReservePath+"?group=/g&route_key=rk&operation=create", bytes.NewReader(body))
			req.Header.Set("X-API-KEY", testAPIKeyValue())
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusServiceUnavailable || reads.calls != 1 {
				t.Fatalf("status=%d body=%q reads.calls=%d; want accepted config reaching unavailable placement once", rec.Code, rec.Body.String(), reads.calls)
			}
			if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
				t.Fatalf("unavailable placement wrote route state: found=%v err=%v", found, err)
			}
		})
	}
}

func TestServeReserveRejectsInvalidResourceAndTrafficBeforePlacement(t *testing.T) {
	for _, tc := range []struct {
		name, operation, key, value string
	}{
		{"invalid resource", "create", sandboxcfg.NsResource, `{"control":{}}`},
		{"invalid traffic", "create", sandboxcfg.NsTraffic, `{"max_inflight":{"total":-1}}`},
		{"connect resource", "connect", sandboxcfg.NsResource, `{"capacity":{"cpu":2}}`},
		{"connect traffic", "connect", sandboxcfg.NsTraffic, `{"max_inflight":{"total":3}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := New(newTestStores(t), 0, nil)
			if err := putTestNode(reg.stores, t.Context(), &NodeRecord{NodeID: "fixture", Draining: true}); err != nil {
				t.Fatal(err)
			}
			publishTestGroups(t, reg, "fixture", "/g")
			reads := countTestGroupReads(reg)
			_ = reads
			mux := http.NewServeMux()
			reg.ServeRouteLink(mux)
			body, err := json.Marshal(SandboxReserveReq{Config: map[string]string{tc.key: tc.value}})
			if err != nil {
				t.Fatal(err)
			}
			path := RouteLinkReservePath + "?group=/g&route_key=rk&operation=" + tc.operation
			if tc.operation == "connect" {
				path += "&sid=sb-route"
			}
			req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
			req.Header.Set("X-API-KEY", testAPIKeyValue())
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest || reads.calls != 0 {
				t.Fatalf("status=%d body=%q reads.calls=%d; want 400 before placement", rec.Code, rec.Body.String(), reads.calls)
			}
			if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
				t.Fatalf("invalid config wrote route state: found=%v err=%v", found, err)
			}
		})
	}
}

func TestServeReserveRejectsUnsupportedConfigBeforeReservation(t *testing.T) {
	reg := New(newTestStores(t), 0, nil)
	if err := putTestNode(reg.stores, t.Context(), &NodeRecord{NodeID: "fixture", Draining: true}); err != nil {
		t.Fatal(err)
	}
	publishTestGroups(t, reg, "fixture", "/g")
	reads := countTestGroupReads(reg)
	_ = reads
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
	if reads.calls != 0 {
		t.Fatalf("unsupported config reached placement %d times", reads.calls)
	}
	if _, _, found, err := reg.stores.GetSandbox(context.Background(), "/g", "rk"); err != nil || found {
		t.Fatalf("unsupported config wrote route state: found=%v err=%v", found, err)
	}
}

func TestServeReserveAcceptsCredentialsBeforeCapacityFailure(t *testing.T) {
	reg := New(newTestStores(t), 0, nil)
	if err := putTestNode(reg.stores, t.Context(), &NodeRecord{NodeID: "fixture", Draining: true}); err != nil {
		t.Fatal(err)
	}
	publishTestGroups(t, reg, "fixture", "/g")
	reads := countTestGroupReads(reg)
	_ = reads
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
	if reads.calls != 1 {
		t.Fatalf("accepted credentials reached placement %d times, want 1", reads.calls)
	}
}

func TestServeReserveRejectsInvalidCredentialsBeforePlacement(t *testing.T) {
	reg := New(newTestStores(t), 0, nil)
	if err := putTestNode(reg.stores, t.Context(), &NodeRecord{NodeID: "fixture", Draining: true}); err != nil {
		t.Fatal(err)
	}
	publishTestGroups(t, reg, "fixture", "/g")
	reads := countTestGroupReads(reg)
	_ = reads
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
	if reads.calls != 0 {
		t.Fatalf("invalid credentials reached placement %d times", reads.calls)
	}
}

func TestServeReserveRejectsCredentialsInvalidForPlacedProfile(t *testing.T) {
	reg := New(newTestStores(t), 0, nil)

	bare := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindImg, Ref: "manifest://" + strings.Repeat("a", 64)}.String()
	publishTestSupport(t, reg, "fixture", nodegroup.Support{Group: "/g", Version: 1, Entries: []nodegroup.Entry{{AuthRef: testAPIFingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}}, DefaultContext: &nodegroup.DefaultContext{TemplateID: bare}})
	reads := countTestGroupReads(reg)
	_ = reads
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
		{name: "connect create fields", path: "?group=/g&route_key=rk&operation=connect&sid=sb-route", body: `{"config":{"kuasar-sandbox.restore":"{}"}}`, apiKey: testAPIKeyValue(), wantStatus: http.StatusBadRequest},
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
	if err := putTestNode(reg.stores, ctx, &NodeRecord{NodeID: "n1", DataEndpoint: "127.0.0.1:9443"}); err != nil {
		t.Fatal(err)
	}
	reg.addNode(&fakeConn{nodeID: "n1", onCmd: func(cmd *routesync.Command) {
		if cmd.Memory == nil || *cmd.Memory {
			t.Errorf("connect memory = %v, want explicit false", cmd.Memory)
		}
		go reg.ackCommand(&routesync.CmdAck{
			CmdID: cmd.CmdID, Status: routesync.AckAccepted,
			Connect: testConnectResult(record, cmd.SID),
		})
	}})
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	req := httptest.NewRequest(http.MethodPost,
		RouteLinkReservePath+"?group=/g&route_key=rk&operation=connect&sid=sb-route&timeout=37", strings.NewReader(`{"memory":false}`))
	req.Header.Set("X-API-KEY", testAPIKeyValue())
	req.Header.Set("Content-Type", "application/json")
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
		!result.Route.RouteRevision.IsCommitted() {
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
	if err := putTestNode(reg.stores, ctx, &NodeRecord{NodeID: "n1"}); err != nil {
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
