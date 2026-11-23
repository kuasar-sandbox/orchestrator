package router

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBuildRegisterMissingEndpointRecoversWithoutReregister(t *testing.T) {
	for _, staleEmptyEntry := range []bool{false, true} {
		name := "registration"
		if staleEmptyEntry {
			name = "existing-empty-cache"
		}
		t.Run(name, func(t *testing.T) {
			const buildID = "endpoint-recovery"
			const templateID = "transient-endpoint-recovery"
			var reserveCalls, resolveCalls, apiHits, dataHits atomic.Int32
			var endpointAvailable atomic.Bool
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiHits.Add(1)
				if r.Host != "api.test.local" || r.Header.Get(HeaderAPIKey) != "e2b_test" || r.Header.Get(HeaderAccessTok) != "" {
					t.Errorf("forwarded control identity: host=%q key=%q access=%q", r.Host, r.Header.Get(HeaderAPIKey), r.Header.Get(HeaderAccessTok))
				}
				_ = json.NewEncoder(w).Encode(map[string]string{"buildID": buildID, "templateID": templateID})
			}))
			defer api.Close()
			data := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				dataHits.Add(1)
				http.Error(w, "wrong endpoint", http.StatusTeapot)
			}))
			defer data.Close()
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/v2/verify-key":
					w.WriteHeader(http.StatusOK)
				case "/route-link/v2/reserve-build":
					reserveCalls.Add(1)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"build_id": buildID, "template_id": templateID, "node_id": "n1", "profile": "e2b",
						"api_endpoint": "", "data_endpoint": strings.TrimPrefix(data.URL, "http://"),
					})
				case "/route-link/v2/build":
					if r.URL.Query().Get("group") != "/g" || r.URL.Query().Get("build_id") != buildID {
						t.Errorf("lookup changed accepted identity: %s", r.URL.RawQuery)
					}
					resolveCalls.Add(1)
					if !endpointAvailable.Load() {
						http.Error(w, "build node API endpoint is unavailable", http.StatusServiceUnavailable)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"build_id": buildID, "template_id": templateID, "node_id": "n1", "profile": "e2b",
						"api_endpoint": strings.TrimPrefix(api.URL, "http://"), "data_endpoint": strings.TrimPrefix(data.URL, "http://"),
					})
				default:
					http.NotFound(w, r)
				}
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger())
			front := httptest.NewServer(rt.Handler())
			defer front.Close()
			request := func(method, path, body string) (int, []byte) {
				t.Helper()
				req, err := http.NewRequest(method, front.URL+path, strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				req.Host = "api.test.local"
				req.Header.Set(HeaderGroup, "/g")
				req.Header.Set(HeaderAPIKey, "e2b_test")
				req.Header.Set(HeaderAccessTok, "must-not-forward")
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				raw, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				return resp.StatusCode, raw
			}
			status, raw := request(http.MethodPost, "/v3/templates", `{"name":"test","cpuCount":1,"memoryMB":128}`)
			if status != http.StatusAccepted {
				t.Fatalf("registration status=%d body=%s", status, raw)
			}
			var registered map[string]any
			if err := json.Unmarshal(raw, &registered); err != nil {
				t.Fatal(err)
			}
			if registered["buildID"] != buildID || registered["templateID"] != templateID {
				t.Fatalf("registration lost accepted IDs: %s", raw)
			}
			rt.buildsMu.Lock()
			entry, cached := rt.builds[buildCacheKey("/g", buildID)]
			if staleEmptyEntry {
				rt.builds[buildCacheKey("/g", buildID)] = buildEntry{node: "", at: time.Now()}
			}
			rt.buildsMu.Unlock()
			if !staleEmptyEntry && cached && entry.node == "" {
				t.Error("registration cached an unavailable endpoint")
			}

			path := "/v2/templates/" + templateID + "/builds/" + buildID + "/status"
			for attempt, want := range []int{http.StatusServiceUnavailable, http.StatusOK, http.StatusOK} {
				got, body := request(http.MethodGet, path, "")
				if got != want {
					t.Errorf("status attempt %d: got=%d want=%d body=%s", attempt+1, got, want, body)
				}
				if attempt == 0 {
					endpointAvailable.Store(true)
				}
			}
			if reserveCalls.Load() != 1 || resolveCalls.Load() != 4 || apiHits.Load() != 2 || dataHits.Load() != 0 {
				t.Errorf("calls reserve=%d resolve=%d api=%d data=%d; want 1/4/2/0", reserveCalls.Load(), resolveCalls.Load(), apiHits.Load(), dataHits.Load())
			}
		})
	}
}
