package controlplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/clustercfg"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/placer"
)

type plannerTestProvider struct {
	record clusterstate.SandboxGroupRecord
}

func (p plannerTestProvider) GetRecord(context.Context, string) (clusterstate.SandboxGroupRecord, bool, error) {
	return p.record, true, nil
}
func (p plannerTestProvider) Get(context.Context, string) (clusterstate.SandboxGroup, bool, error) {
	return clusterstate.SandboxGroup{Group: p.record.Group}, true, nil
}
func (plannerTestProvider) GetPlacementHint(context.Context, string) (clusterstate.PlacementHint, bool, error) {
	return clusterstate.PlacementHint{}, true, nil
}
func (p plannerTestProvider) GetManifestKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return p.record.ManifestKey, true, nil
}
func (p plannerTestProvider) GetAuthKey(context.Context, string) (clusterstate.Secret, bool, error) {
	return p.record.AuthKey, true, nil
}

func TestHTTPPlacementPlannerPagesAndRecoversCatalogCache(t *testing.T) {
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	provider := plannerTestProvider{record: clusterstate.SandboxGroupRecord{
		Group: "/group", TemplateRef: templateRef,
		AuthKey:     clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("a", 64)},
		ManifestKey: clusterstate.Secret{Type: clusterstate.SecretInline, Value: strings.Repeat("b", 64)},
	}}
	newService := func() *placer.FinalService {
		service, err := placer.NewFinalService(provider, clustercfg.PlacementConfig{})
		if err != nil {
			t.Fatal(err)
		}
		return service
	}
	var mu sync.RWMutex
	current := http.Handler(newService())
	requestCounts := make(map[string]int)
	maximumBody := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		raw, err := io.ReadAll(request.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request.Body = io.NopCloser(bytes.NewReader(raw))
		mu.Lock()
		requestCounts[request.URL.Path]++
		maximumBody[request.URL.Path] = max(maximumBody[request.URL.Path], len(raw))
		handler := current
		mu.Unlock()
		handler.ServeHTTP(w, request)
	}))
	defer server.Close()

	nodes := make([]placement.CatalogNode, 5000)
	for index := range nodes {
		nodes[index] = placement.CatalogNode{
			NodeID: fmt.Sprintf("node-%05d", index), SandboxSlotCapacity: 1,
			Capabilities: map[string]bool{"sandbox": true},
			Labels:       map[string]string{"catalog-padding": strings.Repeat("x", 1024)},
		}
	}
	catalog, err := placement.NewCatalogSnapshot(
		"cluster-1", "generation-1", 1, strings.Repeat("d", 64), 11, nodes,
	)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := clusterstate.NewNodeRequestEnvelopeV1(
		http.MethodPost, "/sandboxes", "", nil, []byte(`{"future":true}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := placer.PlanRequest{
		Kind: placer.PlanSandbox, Group: "/group", RouteKey: "route-1",
		Sandbox: &placer.SandboxPlanInput{
			SandboxID: "sandbox-1", TimeoutSeconds: 30,
			Demand: placement.SandboxDemand{SlotUnits: 1}, Request: envelope,
		},
	}
	planner, err := NewHTTPPlacementPlanner([]PlannerEndpoint{{
		Name: "placer-a", Endpoint: server.URL, Client: server.Client(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := planner.Plan(context.Background(), catalog, request)
		if err != nil || len(response.Candidates) != placement.DefaultCandidateCount {
			t.Fatalf("Plan attempt %d = %+v, %v", attempt, response, err)
		}
	}
	mu.RLock()
	firstSyncCount := requestCounts[placer.FinalCatalogSyncPath]
	planBodyBytes := maximumBody[placer.FinalPlanPath]
	maximumSyncBytes := maximumBody[placer.FinalCatalogSyncPath]
	mu.RUnlock()
	if firstSyncCount <= 1 || planBodyBytes >= 4<<20 || maximumSyncBytes > placer.MaximumCatalogSyncRequestBytes {
		t.Fatalf("catalog transport = syncs=%d plan_bytes=%d max_sync_bytes=%d", firstSyncCount, planBodyBytes, maximumSyncBytes)
	}

	mu.Lock()
	current = newService()
	mu.Unlock()
	response, err := planner.Plan(context.Background(), catalog, request)
	if err != nil || len(response.Candidates) != placement.DefaultCandidateCount {
		t.Fatalf("Plan after Placer restart = %+v, %v", response, err)
	}
	mu.RLock()
	restartedSyncCount := requestCounts[placer.FinalCatalogSyncPath]
	mu.RUnlock()
	if restartedSyncCount <= firstSyncCount {
		t.Fatalf("Placer cache loss did not trigger catalog resync: before=%d after=%d", firstSyncCount, restartedSyncCount)
	}
}

func TestCatalogSyncPageSplitsOnEncodedByteLimit(t *testing.T) {
	labels := make(map[string]string, 32)
	for index := 0; index < 32; index++ {
		labels[fmt.Sprintf("label-%02d", index)] = strings.Repeat("x", 256)
	}
	nodes := make([]placement.CatalogNode, 300)
	for index := range nodes {
		nodes[index] = placement.CatalogNode{
			NodeID: fmt.Sprintf("node-%04d", index), Labels: labels, SandboxSlotCapacity: 1,
		}
	}
	catalog, err := placement.NewCatalogSnapshot(
		"cluster-1", "generation-1", 1, strings.Repeat("d", 64), 12, nodes,
	)
	if err != nil {
		t.Fatal(err)
	}
	start := uint32(0)
	pages := 0
	for start < catalog.Reference.NodeCount {
		page, err := nextCatalogSyncPage(catalog, start)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(page)
		if err != nil {
			t.Fatal(err)
		}
		if len(raw) > placer.MaximumCatalogSyncRequestBytes {
			t.Fatalf("catalog page bytes = %d", len(raw))
		}
		if pages == 0 && len(page.Nodes) >= placer.MaxCatalogSyncPageNodes {
			t.Fatalf("first byte-bounded page retained %d nodes", len(page.Nodes))
		}
		start += uint32(len(page.Nodes))
		pages++
	}
	if pages < 2 {
		t.Fatalf("catalog sync pages = %d", pages)
	}
}
