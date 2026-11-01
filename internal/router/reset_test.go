package router

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestResetTerminalManagementUsesNodeAPI(t *testing.T) {
	for _, state := range []string{"dead", "deleting"} {
		for _, operation := range []struct{ method, path, body string }{
			{http.MethodGet, "", ""},
			{http.MethodPost, "/reset", ""},
			{http.MethodPost, "/export", `{"checkpointID":"old-A","toTemplate":true}`},
			{http.MethodDelete, "/checkpoints/old-A", ""},
		} {
			t.Run(state+"/"+operation.method+operation.path, func(t *testing.T) {
				var nodeHits int
				node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					nodeHits++
					body, _ := io.ReadAll(r.Body)
					if r.Method != operation.method || r.URL.Path != "/sandboxes/retained-g0"+operation.path || string(body) != operation.body || r.Header.Get(HeaderAPIKey) != "e2b_test" {
						t.Errorf("forwarded request=%s %s %s", r.Method, r.URL.Path, body)
					}
					if operation.method == http.MethodGet {
						_ = json.NewEncoder(w).Encode(map[string]any{"sandboxID": "retained-g0", "state": state, "autoReset": true, "deadAt": "2026-11-01T00:00:00Z", "autoResetError": "disk unavailable"})
						return
					}
					w.WriteHeader(http.StatusNoContent)
				}))
				defer node.Close()
				control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/route-link/verify-key" {
						return
					}
					if r.URL.Path != "/route-link/route" {
						t.Errorf("management unexpectedly used %s", r.URL.Path)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					route := routerTestRouteResolve(t, "retained", "/g", "rk", strings.TrimPrefix(node.URL, "http://"), types.ProfileE2B)
					route.State, route.DataEndpoint = state, ""
					_ = json.NewEncoder(w).Encode(route)
				}))
				defer control.Close()
				rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
				req := httptest.NewRequest(operation.method, "http://api.test.local/sandboxes/retained"+operation.path, strings.NewReader(operation.body))
				req.Header.Set(HeaderGroup, "/g")
				req.Header.Set(HeaderRouteKey, "rk")
				req.Header.Set(HeaderAPIKey, "e2b_test")
				w := httptest.NewRecorder()
				rt.Handler().ServeHTTP(w, req)
				wantStatus := http.StatusNoContent
				if operation.method == http.MethodGet {
					wantStatus = http.StatusOK
					var view map[string]any
					if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || view["sandboxID"] != "retained" || view["state"] != state || view["autoReset"] != true || view["autoResetError"] != "disk unavailable" || view["deadAt"] != "2026-11-01T00:00:00Z" {
						t.Fatalf("terminal fields lost through cluster: %s / %v", w.Body.String(), err)
					}
				}
				if w.Code != wantStatus || nodeHits != 1 {
					t.Fatalf("terminal management status=%d hits=%d body=%s", w.Code, nodeHits, w.Body.String())
				}
			})
		}
	}
}

func TestResetCreatePolicyParsing(t *testing.T) {
	for _, tc := range []struct {
		body                    string
		present, value, invalid bool
	}{
		{body: `{}`},
		{body: `{"autoReset":null}`},
		{body: `{"autoReset":false}`, present: true},
		{body: `{"autoReset":true}`, present: true, value: true},
		{body: `{"autoReset":"true"}`, invalid: true},
	} {
		req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(tc.body))
		var autoReset *bool
		_, err := createSandboxMetadataWithAutoPauseMemory(httptest.NewRecorder(), req, nil, &autoReset)
		if (err != nil) != tc.invalid || (!tc.invalid && ((autoReset != nil) != tc.present || (autoReset != nil && *autoReset != tc.value))) {
			t.Fatalf("body=%s policy=%v err=%v", tc.body, autoReset, err)
		}
	}
}
