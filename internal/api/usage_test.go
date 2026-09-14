package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	conductorextension "github.com/kuasar-sandbox/orchestrator/app/conductor/extension"
)

type usageStatsCoreStub struct {
	Core
	query conductorextension.UsageQuery
	calls int
	err   error
}

func (c *usageStatsCoreStub) UsageStats(_ context.Context, id, _ string, q conductorextension.UsageQuery) (json.RawMessage, error) {
	c.calls++
	c.query = q
	if id != "exact-sid" {
		return nil, ErrNotFound
	}
	return json.RawMessage(`{"enabled":false,"saved_end":"9007199254740993","saving":false,"unknown_tail":true}`), c.err
}
func TestUsageStatsRouteQueryPrecisionAndErrors(t *testing.T) {
	core := &usageStatsCoreStub{}
	handler, key := newSandboxContractHandler(t, core, Resources{})
	for _, tc := range []struct {
		path string
		code int
	}{
		{"/sandboxes/exact-sid/stats/usage", 200},
		{"/sandboxes/exact-sid/stats/usage?view=saved", 200},
		{"/sandboxes/exact-sid/stats/usage?view=history&cursor=9007199254740993&limit=1", 200},
		{"/sandboxes/stable-alias/stats/usage", 404},
		{"/sandboxes/exact-sid/usage", 404},
		{"/sandboxes/exact-sid/stats/network", 404},
		{"/v2/sandboxes/stats", 404},
		{"/sandboxes/exact-sid/stats/usage?view=other", 400},
		{"/sandboxes/exact-sid/stats/usage?view=history&limit=0", 400},
		{"/sandboxes/exact-sid/stats/usage?view=history&limit=101", 400},
		{"/sandboxes/exact-sid/stats/usage?view=history&cursor=-1", 400},
		{"/sandboxes/exact-sid/stats/usage?view=history&cursor=9223372036854775808", 400},
		{"/sandboxes/exact-sid/stats/usage?view=saved&cursor=0", 400},
		{"/sandboxes/exact-sid/stats/usage?view=current&view=saved", 400},
		{"/sandboxes/exact-sid/stats/usage?file=/tmp/other.usage", 400},
		{"/sandboxes/exact-sid/stats/usage?view=%ZZ", 400},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set("X-API-KEY", key)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != tc.code {
			t.Fatalf("%s: %d %s", tc.path, res.Code, res.Body.String())
		}
		if tc.code == 200 {
			if res.Header().Get("Cache-Control") != "no-store" || !strings.Contains(res.Body.String(), `"saved_end":"9007199254740993"`) {
				t.Fatal("native JSON lost", res.Body.String())
			}
			if strings.Contains(tc.path, "cursor=") && core.query.Cursor != 9007199254740993 {
				t.Fatal("lossy cursor", core.query)
			}
		}
	}
	before := core.calls
	req := httptest.NewRequest(http.MethodGet, "/sandboxes/exact-sid/stats/usage", nil)
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != 401 || core.calls != before {
		t.Fatal("unauthenticated native read", res.Code)
	}
	core.err = ErrStatsUnavailable
	req.Header.Set("X-API-KEY", key)
	res = httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	if res.Code != 503 || strings.Contains(res.Body.String(), "saved_end") {
		t.Fatal("unavailable became data", res.Body.String())
	}
}
