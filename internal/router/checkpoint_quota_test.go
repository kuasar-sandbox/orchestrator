package router

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/clusterclient"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func checkpointQuotaBody(raw string) string {
	body, _ := json.Marshal(map[string]any{"metadata": map[string]string{sandboxcfg.NsCheckpoint: raw}})
	return string(body)
}

func TestCreateCheckpointQuotaPresenceAndValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, header string
		valid              bool
		quota              int // Zero means omitted, not an accepted quota.
		snapshot           string
	}{
		{name: "omitted", body: `{}`, valid: true},
		{name: "metadata", body: checkpointQuotaBody(`{"max_checkpoints":17}`), valid: true, quota: 17},
		{name: "header", body: `{}`, header: `{"max_checkpoints":4}`, valid: true, quota: 4},
		{name: "null", body: checkpointQuotaBody(`{"max_checkpoints":null}`), valid: true},
		{name: "header overlays leaves", body: checkpointQuotaBody(`{"max_checkpoints":2,"merge_ref":true,"drop_caches":false}`), header: `{"max_checkpoints":4,"merge_ref":false}`, valid: true, quota: 4, snapshot: `{"merge_ref":false,"drop_caches":false}`},
		{name: "null header inherits", body: checkpointQuotaBody(`{"max_checkpoints":2,"merge_ref":true}`), header: `{"max_checkpoints":null,"drop_caches":false}`, valid: true, quota: 2, snapshot: `{"merge_ref":true,"drop_caches":false}`},
		{name: "zero", body: checkpointQuotaBody(`{"max_checkpoints":0}`)},
		{name: "negative", body: `{}`, header: `{"max_checkpoints":-1}`},
		{name: "fraction", body: checkpointQuotaBody(`{"max_checkpoints":1.5}`)},
		{name: "string", body: checkpointQuotaBody(`{"max_checkpoints":"3"}`)},
		{name: "duplicate", body: checkpointQuotaBody(`{"max_checkpoints":1,"max_checkpoints":3}`)},
		{name: "header cannot hide invalid metadata", body: checkpointQuotaBody(`{"max_checkpoints":0}`), header: `{"max_checkpoints":4}`},
		{name: "removed top level", body: `{"max_checkpoints":1}`},
		{name: "removed top level null", body: `{"max_checkpoints":null}`},
		{name: "removed top level casing", body: `{"MAX_CHECKPOINTS":4}`},
		{name: "namespace cannot hide removed field", body: `{"max_checkpoints":null}`, header: `{"max_checkpoints":4}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(tc.body))
			if tc.header != "" {
				req.Header.Set(HeaderCheckpoint, tc.header)
			}
			stale := 99
			quota := &stale
			meta, err := createSandboxPoliciesWithQuota(httptest.NewRecorder(), req, nil, nil, &quota)
			if (err == nil) != tc.valid {
				t.Fatalf("validation: %v", err)
			}
			if !tc.valid {
				return
			}
			if (quota != nil) != (tc.quota != 0) || quota != nil && *quota != tc.quota {
				t.Fatalf("quota=%v, want %d (zero means omitted)", quota, tc.quota)
			}
			if got := meta[sandboxcfg.NsCheckpoint]; got != tc.snapshot {
				t.Fatalf("snapshot defaults=%q, want %q; quota must not remain in metadata", got, tc.snapshot)
			}
		})
	}
}

func TestCreateCheckpointQuotaReachesReservation(t *testing.T) {
	for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
		for _, tc := range []struct {
			name, body, header string
			status, quota      int
			snapshot           string
		}{
			{name: "default", body: `{}`, status: http.StatusCreated},
			{name: "metadata", body: checkpointQuotaBody(`{"max_checkpoints":2}`), status: http.StatusCreated, quota: 2},
			{name: "header overlay", body: checkpointQuotaBody(`{"max_checkpoints":2,"merge_ref":true}`), header: `{"max_checkpoints":4}`, status: http.StatusCreated, quota: 4, snapshot: `{"merge_ref":true}`},
			{name: "removed top level", body: `{"max_checkpoints":null}`, header: `{"max_checkpoints":4}`, status: http.StatusBadRequest},
			{name: "invalid metadata", body: checkpointQuotaBody(`{"max_checkpoints":0}`), header: `{"max_checkpoints":4}`, status: http.StatusBadRequest},
		} {
			t.Run(path+"/"+tc.name, func(t *testing.T) {
				calls := 0
				var wire registry.SandboxReserveReq
				client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.URL.Path == registry.RouteLinkVerifyKeyPath {
						return textResponse(http.StatusOK, ""), nil
					}
					if r.URL.Path != "/route-link/v2/reserve" {
						t.Fatalf("unexpected request %s", r.URL.Path)
					}
					calls++
					if err := json.NewDecoder(r.Body).Decode(&wire); err != nil {
						t.Fatal(err)
					}
					body, _ := json.Marshal(routerTestReserveResult(t, "s1", "/g", "rk", "node:1", types.ProfileBare))
					return textResponse(http.StatusOK, string(body)), nil
				})}
				rt := New("unused:80", "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
				rt.routeRegistry = routeRegistryFunc(func(context.Context, string) ([]clusterclient.Endpoint, error) {
					return []clusterclient.Endpoint{{MemberID: "r1", BaseURL: "http://r1", Client: client}}, nil
				})
				req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
				req.Host = "api.test.local"
				req.Header.Set(HeaderGroup, "/g")
				req.Header.Set(HeaderRouteKey, "rk")
				req.Header.Set(HeaderAPIKey, "key")
				if tc.header != "" {
					req.Header.Set(HeaderCheckpoint, tc.header)
				}
				w := httptest.NewRecorder()
				rt.Handler().ServeHTTP(w, req)
				if w.Code != tc.status {
					t.Fatalf("status=%d body=%s, want %d", w.Code, w.Body, tc.status)
				}
				if tc.status == http.StatusBadRequest {
					if calls != 0 {
						t.Fatalf("invalid request reached reservation %d times", calls)
					}
					return
				}
				if calls != 1 || (wire.MaxCheckpoints != nil) != (tc.quota != 0) || wire.MaxCheckpoints != nil && *wire.MaxCheckpoints != tc.quota {
					t.Fatalf("reservation calls=%d quota=%v, want quota %d", calls, wire.MaxCheckpoints, tc.quota)
				}
				if wire.Config[sandboxcfg.NsCheckpoint] != tc.snapshot {
					t.Fatalf("reservation snapshot defaults=%q, want %q", wire.Config[sandboxcfg.NsCheckpoint], tc.snapshot)
				}
			})
		}
	}
}
