package router

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/apikey"
	"github.com/kuasar-sandbox/orchestrator/internal/cluster/metadata"
	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/testutil/metadatastore"
)

func TestPublicListForwardsPaginationStateAndScope(t *testing.T) {
	for _, path := range []string{"/sandboxes", "/v2/sandboxes"} {
		t.Run(path, func(t *testing.T) {
			control := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/route-link/v2/verify-key":
					w.WriteHeader(http.StatusOK)
				case "/route-link/v2/list":
					want := url.Values{"group": {"/g"}, "limit": {"17"}, "nextToken": {"opaque+/cursor="}, "state": {"paused"}}
					if !reflect.DeepEqual(r.URL.Query(), want) || r.Header.Get(HeaderAPIKey) != "e2b_test" {
						t.Errorf("forwarded list query=%v, want %v", r.URL.Query(), want)
					}
					w.Header().Set("x-next-token", "next-page")
					w.Header().Set("Cache-Control", "no-store")
					_, _ = io.WriteString(w, "[]\n") // filtered empty pages must pass through
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer control.Close()
			rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			query := url.Values{"group": {"untrusted-override"}, "limit": {"17"}, "nextToken": {"opaque+/cursor="}, "state": {"paused"}, "ignored": {"x"}}
			req := httptest.NewRequest(http.MethodGet, path+"?"+query.Encode(), nil)
			req.Host = "api.test.local"
			req.Header.Set(HeaderGroup, "/g")
			req.Header.Set(HeaderAPIKey, "e2b_test")
			w := httptest.NewRecorder()
			rt.Handler().ServeHTTP(w, req)
			if w.Code != http.StatusOK || w.Body.String() != "[]\n" || w.Header().Get("x-next-token") != "next-page" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("public list lost continuation: status=%d headers=%v body=%s", w.Code, w.Header(), w.Body.String())
			}
		})
	}
}

func TestPublicListTraversesRegistryFilteredPages(t *testing.T) {
	db := metadatastore.New(t)
	repos := db.Repositories()
	stores, err := registry.NewStores(repos, metadatastore.Owners{ID: "registry"})
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New(stores, 0, nil)
	raw, _ := hex.DecodeString(strings.Repeat("1", 64))
	secret, fingerprint := hex.EncodeToString(raw), hex.EncodeToString(apikey.FullFingerprint(raw))
	key, err := apikey.Mint(raw)
	if err != nil {
		t.Fatal(err)
	}
	node, err := repos.Nodes.Register(t.Context(), metadata.Version{}, metadata.NodeProfileRecord{NodeID: "node", LinkOwner: "registry", APIEndpoint: "node:7443", DataEndpoint: "node:8080"})
	if err != nil {
		t.Fatal(err)
	}
	group := nodegroup.Support{Group: "g", Version: 1, Entries: []nodegroup.Entry{{AuthRef: fingerprint, AuthMaterial: nodegroup.AuthMaterial{APISecret: secret}}}}
	if err := repos.Groups.BeginGroupSupport(t.Context(), node.Registration, "list-test", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := repos.Groups.WriteGroupSupportBatch(t.Context(), node.Registration, "list-test", 0, []nodegroup.Support{group}); err != nil {
		t.Fatal(err)
	}
	if done, err := repos.Groups.FinishGroupSupport(t.Context(), node.Registration, "list-test"); err != nil || !done {
		t.Fatalf("group publication: done=%v err=%v", done, err)
	}
	for i := range 213 {
		sid := fmt.Sprintf("sb-%04d", i)
		record := metadata.SandboxRecord{Group: "g", RouteKey: fmt.Sprintf("route-%04d", i), SandboxID: sid, NodeSandboxID: sid + "-g0", NextSandboxGeneration: 1, NodeID: "node", Profile: "bare", State: metadata.StatePaused, APISecretFingerprint: fingerprint, APISecret: secret}
		if i < 100 {
			record.APISecret = strings.Repeat("2", 64)
		}
		if _, err := repos.Routes.CompareAndPutRoute(t.Context(), metadata.RouteKey{Group: "g", RouteKey: record.RouteKey}, metadata.Version{}, record); err != nil {
			t.Fatal(err)
		}
	}
	mux := http.NewServeMux()
	reg.ServeRouteLink(mux)
	control := httptest.NewServer(mux)
	defer control.Close()
	rt := New(strings.TrimPrefix(control.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	next, count := "", 0
	seen := map[string]bool{}
	for page := 0; ; page++ {
		query := url.Values{"state": {"paused"}}
		if next != "" {
			query.Set("nextToken", next)
			query.Set("limit", "61") // changing only the page size is allowed
		}
		req := httptest.NewRequest(http.MethodGet, "/v2/sandboxes?"+query.Encode(), nil)
		req.Host = "api.test.local"
		req.Header.Set(HeaderGroup, "g")
		req.Header.Set(HeaderAPIKey, key)
		w := httptest.NewRecorder()
		rt.Handler().ServeHTTP(w, req)
		var rows []registry.ListItem
		if err := json.Unmarshal(w.Body.Bytes(), &rows); err != nil || w.Code != http.StatusOK {
			t.Fatalf("public page %d: status=%d body=%s error=%v", page, w.Code, w.Body.String(), err)
		}
		if page == 0 && (len(rows) != 0 || w.Header().Get("x-next-token") == "") {
			t.Fatal("public empty page lost its opaque continuation")
		}
		for _, item := range rows {
			want := fmt.Sprintf("sb-%04d", count+100)
			if item.SandboxID != want || seen[item.SandboxID] || item.State != "paused" {
				t.Fatalf("public list lost authorization/order/identity: item=%+v want=%s", item, want)
			}
			seen[item.SandboxID] = true
			count++
		}
		next = w.Header().Get("x-next-token")
		if next == "" {
			break
		}
		if page > 10 {
			t.Fatal("public list failed to finish")
		}
	}
	if count != 113 {
		t.Fatalf("public list missed authorized routes: %d", count)
	}
}
