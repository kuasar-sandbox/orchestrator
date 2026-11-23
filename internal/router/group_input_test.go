package router

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/registry"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestGroupCreateInputKeepsTemplateAndStripsPlacement(t *testing.T) {
	id := types.TemplateID{Profile: types.ProfileBare, Kind: types.KindSnp, Ref: "manifest://" + strings.Repeat("a", 64) + "@runtime:runtime.bundle:digest:" + strings.Repeat("b", 64)}.String()
	body, _ := json.Marshal(map[string]any{"templateID": id, "metadata": map[string]string{placement.Metadata: `{"node_selectors":[{"zone":"body"}]}`, sandboxcfg.NsCheckpoint: `{"max_checkpoints":7}`}})
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(string(body)))
	req.Header.Set(placement.Header, `{}`)
	var selection createWireOptions
	var quota *int
	clean, err := createSandboxInputWithQuota(httptest.NewRecorder(), req, nil, nil, &quota, &selection)
	if err != nil {
		t.Fatal(err)
	}
	if selection.templateID == nil || *selection.templateID != id || len(selection.placement.NodeSelectors) != 0 {
		t.Fatalf("lost request semantics: %+v", selection)
	}
	if _, found := clean[placement.Metadata]; found {
		t.Fatal("placement leaked into Node metadata")
	}
	if quota == nil || *quota != 7 {
		t.Fatalf("checkpoint quota lost: %v", quota)
	}
}

func TestGroupCreateInputRejectsMalformedLowerLayerAndTemplate(t *testing.T) {
	for _, body := range []string{`{"templateID":""}`, `{"templateID":7}`, `{"templateID":"alias"}`, `{"templateID":null,"templateID":null}`, `{"metadata":{"kuasar-sandbox.placement":"null"}}`, `{"metadata":{"kuasar-sandbox.placement":"{\"shuffle_labels\":[\"a\"]}"}}`} {
		req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
		req.Header.Set(placement.Header, `{}`)
		if _, err := createSandboxInputWithQuota(httptest.NewRecorder(), req, nil, nil, nil, &createWireOptions{}); err == nil {
			t.Fatalf("accepted %s", body)
		}
	}
	for _, body := range []string{`{}`, `{"templateID":null}`} {
		req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(body))
		var input createWireOptions
		if _, err := createSandboxInputWithQuota(httptest.NewRecorder(), req, nil, nil, nil, &input); err != nil || input.templateID != nil {
			t.Fatalf("default input rejected: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, "/sandboxes", strings.NewReader(`{}`))
	req.Header.Set(placement.Header, strings.Repeat(" ", placement.MaxBytes+1))
	if _, err := createSandboxMetadata(httptest.NewRecorder(), req); !errors.Is(err, placement.ErrHeaderTooLarge) {
		t.Fatalf("header limit lost: %v", err)
	}
}

func TestGroupBuildPlacementReachesRegistryWithoutMetadataLeak(t *testing.T) {
	var received registry.BuildReserveReq
	var key string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case registry.RouteLinkVerifyKeyPath:
			w.WriteHeader(http.StatusOK)
		case registry.RouteLinkReserveBuildPath:
			key = r.Header.Get(HeaderAPIKey)
			if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
				t.Error(err)
			}
			_ = json.NewEncoder(w).Encode(registry.BuildReserveResult{BuildID: "bld-test", TemplateID: "transient-test", Profile: types.ProfileBare})
		default:
			t.Errorf("unexpected RouteLink: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	router := New(server.URL, "test", time.Second, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	body := `{"profile":"bare","cpuCount":2,"memoryMB":256,"metadata":{"kuasar-sandbox.placement":"{\"node_selectors\":[{\"pool\":\"build\"}]}"}}`
	req := httptest.NewRequest(http.MethodPost, "http://api.test/templates", strings.NewReader(body))
	req.Header.Set(HeaderGroup, "g")
	req.Header.Set(HeaderAPIKey, "caller-key")
	w := httptest.NewRecorder()
	router.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Fatalf("Build=%d %s", w.Code, w.Body.String())
	}
	if key != "caller-key" || !received.Placement.Matches(map[string]string{"pool": "build"}) || received.Placement.Matches(nil) {
		t.Fatalf("lost selection/auth: %+v", received.Placement)
	}
	if _, exists := received.Metadata[placement.Metadata]; exists {
		t.Fatal("request-only Placement was persisted")
	}
}
