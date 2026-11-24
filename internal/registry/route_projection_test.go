package registry

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
)

func TestRouteV2SeparatesObjectControlAndProtectedData(t *testing.T) {
	r := New(newTestStores(t), time.Second, nil)
	raw, _ := hex.DecodeString(testAPISecret)
	key, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	entry := nodegroup.Entry{AuthRef: testAPIFingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}
	reg := reportRegistryGroup(t, r, "node", nil, nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{entry}})
	r.SetNodeOwner(&recordingNodeOwner{})
	rec := testE2BSandboxRecord("g", "rk", "sb-object", "node", StateReady)
	rec.GroupVersion, rec.TemplateSource, rec.CreateTemplateID = 1, "explicit", testTemplateRef
	if _, err := r.stores.PutSandbox(t.Context(), rec); err != nil {
		t.Fatal(err)
	}
	if err := r.stores.groups.ApplyGroupSupport(t.Context(), reg, 2, nil, "g"); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	r.ServeRouteLink(mux)
	request := func(path, apiKey string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path+"?group=g&route_key=rk&sid=sb-object", nil)
		if apiKey != "" {
			req.Header.Set("X-API-KEY", apiKey)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		return w
	}
	for _, path := range []string{RouteLinkControlRoutePath, RouteLinkListPath} {
		if w := request(path, ""); w.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated control=%d %s", w.Code, w.Body.String())
		}
		if w := request(path, key); w.Code != http.StatusOK {
			t.Fatalf("retained object inaccessible after Group withdrawal: %d %s", w.Code, w.Body.String())
		}
	}
	control := request(RouteLinkControlRoutePath, key)
	data := request(RouteLinkRoutePath, "") // Signed file URL lookup has no APIKey/access token.
	if data.Code != http.StatusOK {
		t.Fatalf("data lookup=%d %s", data.Code, data.Body.String())
	}
	for _, tc := range []struct {
		body      string
		forbidden []string
	}{
		{control.Body.String(), []string{"api_secret", "service_secret", "data_endpoint"}},
		{data.Body.String(), []string{"api_secret", "api_endpoint"}},
	} {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal([]byte(tc.body), &fields); err != nil {
			t.Fatal(err)
		}
		for _, field := range tc.forbidden {
			if _, found := fields[field]; found {
				t.Fatalf("projection leaked %s", field)
			}
		}
	}
	if !strings.Contains(data.Body.String(), `"service_secret"`) {
		t.Fatal("data signature verification material lost")
	}
	if w := request("/route-link/route", key); w.Code != http.StatusNotFound {
		t.Fatal("old protocol still mounted")
	}
}
