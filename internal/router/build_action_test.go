package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/api"
)

func TestBuildDeleteForwardsExactActionInputsAfterRouterRestart(t *testing.T) {
	for _, tc := range []struct {
		query, header string
		code          int
		cancel        bool
	}{
		{"cancel=true", "", 202, true}, {"", `{"cancel":true}`, 202, true},
		{"cancel=true", `{}`, 202, true}, {"cancel=true", `{"cancel":false}`, 409, false},
		{"cancel=false", `{"cancel":true}`, 202, true}, {"cancel=1", `{"cancel":true}`, 400, false},
		{"cancel=true&cancel=false", `{"cancel":true}`, 400, false},
		{"cancel=true", `{"cancel":null}`, 400, false},
	} {
		t.Run(tc.query+tc.header, func(t *testing.T) {
			var nodeHits atomic.Int32
			const tid = "transient-abc0123456789"
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				nodeHits.Add(1)
				if r.Method != "DELETE" || r.URL.Path != "/templates/"+tid || r.URL.RawQuery != tc.query || r.Header.Get("X-Kuasar-Sandbox-Builder") != tc.header {
					t.Errorf("changed DELETE: %s %s %v", r.Method, r.URL, r.Header)
				}
				opts, err := api.ParseDeleteBuildOptions(r)
				if err != nil {
					w.WriteHeader(400)
					return
				}
				if opts.Cancel != tc.cancel {
					t.Errorf("forwarded cancel=%v", opts.Cancel)
				}
				if opts.Cancel {
					w.Header().Set("Location", "/templates/"+tid+"/builds/b1/status")
					w.WriteHeader(202)
				} else {
					w.WriteHeader(409)
				}
			}))
			defer node.Close()
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/verify-key":
					w.WriteHeader(200)
				case "/route-link/build":
					if r.URL.Query().Get("group") != "/g" || r.URL.Query().Get("template_id") != tid || r.URL.Query().Get("build_id") != "" {
						t.Errorf("wrong resolve: %s", r.URL)
					}
					_ = json.NewEncoder(w).Encode(map[string]string{"build_id": "b1", "template_id": tid, "node_id": "n1", "api_endpoint": strings.TrimPrefix(node.URL, "http://")})
				default:
					t.Errorf("unexpected registry operation %s", r.URL)
					w.WriteHeader(500)
				}
			}))
			defer control.Close()
			for i := 0; i < 2; i++ {
				front := httptest.NewServer(New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger()).Handler())
				req, _ := http.NewRequest("DELETE", front.URL+"/templates/"+tid+"?"+tc.query, nil)
				req.Host = "api.test.local"
				req.Header.Set(HeaderGroup, "/g")
				req.Header.Set(HeaderAPIKey, "e2b_test")
				if tc.header != "" {
					req.Header.Set("X-Kuasar-Sandbox-Builder", tc.header)
				}
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					front.Close()
					t.Fatal(err)
				}
				resp.Body.Close()
				front.Close()
				if resp.StatusCode != tc.code {
					t.Fatalf("status=%d want %d", resp.StatusCode, tc.code)
				}
				if tc.code == 202 && resp.Header.Get("Location") != "/templates/"+tid+"/builds/b1/status" {
					t.Fatal("Location lost")
				}
			}
			if nodeHits.Load() != 2 {
				t.Fatalf("node hits=%d", nodeHits.Load())
			}
		})
	}
}

func TestBuildDeleteAuthorityFailureNeverLooksLikeCompletion(t *testing.T) {
	for _, code := range []int{404, 503, 500} {
		control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/route-link/verify-key" {
				w.WriteHeader(200)
				return
			}
			w.WriteHeader(code)
		}))
		front := httptest.NewServer(New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger()).Handler())
		req, _ := http.NewRequest("DELETE", front.URL+"/templates/transient-abc?cancel=true", nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderAPIKey, "e2b_test")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		front.Close()
		control.Close()
		want := 503
		if code == 404 {
			want = 404
		}
		if resp.StatusCode != want {
			t.Fatalf("registry %d became %d", code, resp.StatusCode)
		}
	}
}

func TestBuildRegistrationRejectsCancelBeforeClusterDispatch(t *testing.T) {
	control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/route-link/verify-key" {
			w.WriteHeader(200)
			return
		}
		t.Errorf("invalid registration reached dispatch: %s", r.URL)
		w.WriteHeader(500)
	}))
	defer control.Close()
	front := httptest.NewServer(New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, discardRouterLogger()).Handler())
	defer front.Close()
	for _, tc := range []struct{ body, query, header string }{
		{`{"cancel":true}`, "", ""}, {`{"cancel":false}`, "", ""}, {`{"cancel":null}`, "", ""}, {`{"cancel":"false"}`, "", ""},
		{`{}`, "cancel=false", ""}, {`{}`, "cancel=", ""}, {`{}`, "", `{"cancel":true}`},
	} {
		req, _ := http.NewRequest("POST", front.URL+"/v3/templates?"+tc.query, strings.NewReader(tc.body))
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "/g")
		req.Header.Set(HeaderAPIKey, "e2b_test")
		if tc.header != "" {
			req.Header.Set(HeaderBuilder, tc.header)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != 400 {
			t.Fatalf("%+v status=%d", tc, resp.StatusCode)
		}
	}
}
