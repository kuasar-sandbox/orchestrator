package routeapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
)

type fakeService struct {
	route ReadRouteResponse
	build ReadBuildResponse
	err   error
}

func (s fakeService) ReadRoute(context.Context, ReadRouteRequest) (ReadRouteResponse, error) {
	return s.route, s.err
}

func (s fakeService) ReadBuild(context.Context, ReadBuildRequest) (ReadBuildResponse, error) {
	return s.build, s.err
}

func TestReplicaLocalRouteReadNeverReturnsFinalNegative(t *testing.T) {
	request := routeRequest(false)
	if err := (ReadRouteResponse{Outcome: ReadNotFound}).ValidateFor(request); err == nil {
		t.Fatal("replica-local NOT_FOUND accepted")
	}
	request.Strong = true
	if err := (ReadRouteResponse{Outcome: ReadNotFound}).ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	request.Strong = false
	request.MinRouteRevision = 12
	if err := (ReadRouteResponse{Outcome: ReadReady, Route: testReadyRoute(), RouteRevision: 11}).ValidateFor(request); err == nil {
		t.Fatal("READY below min_route_revision accepted")
	}
	response := ReadRouteResponse{
		Outcome:    ReadNeedLeader,
		LeaderHint: &LeaderHint{MemberID: "r1", Endpoint: "https://r1.internal", Term: 3},
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
}

func TestTrustedHandlerRequiresInternalTransportIdentity(t *testing.T) {
	service := fakeService{route: ReadRouteResponse{Outcome: ReadReady, Route: testReadyRoute(), RouteRevision: 12}}
	body, _ := json.Marshal(routeRequest(false))
	untrusted := httptest.NewRequest(http.MethodPost, ReadRoutePath, bytes.NewReader(body))
	w := httptest.NewRecorder()
	NewHandler(service, nil).ServeHTTP(w, untrusted)
	if w.Code != http.StatusForbidden {
		t.Fatalf("untrusted status = %d", w.Code)
	}

	trusted := httptest.NewRequest(http.MethodPost, ReadRoutePath, bytes.NewReader(body))
	w = httptest.NewRecorder()
	NewHandler(service, func(*http.Request) error { return nil }).ServeHTTP(w, trusted)
	if w.Code != http.StatusOK {
		t.Fatalf("trusted status = %d body=%s", w.Code, w.Body.String())
	}
	var response ReadRouteResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil || response.Outcome != ReadReady {
		t.Fatalf("response = %+v err=%v", response, err)
	}
}

func TestTrustedHandlerPreservesUnavailableOutcome(t *testing.T) {
	body, _ := json.Marshal(routeRequest(true))
	request := httptest.NewRequest(http.MethodPost, ReadRoutePath, bytes.NewReader(body))
	w := httptest.NewRecorder()
	NewHandler(fakeService{err: errors.New("permit expired")}, func(*http.Request) error { return nil }).ServeHTTP(w, request)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", w.Code)
	}
	var response ReadRouteResponse
	if err := json.NewDecoder(w.Body).Decode(&response); err != nil || response.Outcome != ReadUnavailable {
		t.Fatalf("response = %+v err=%v", response, err)
	}
}

func TestRouterStateCarriesMonotonicRevisionAndLeaderHint(t *testing.T) {
	state, err := NewRouterState(routeRequest(false).RequestIdentity)
	if err != nil {
		t.Fatal(err)
	}
	request := state.RouteRequest("/g", "rk", "s1", 7, false)
	if request.MinRouteRevision != 0 {
		t.Fatalf("initial minimum = %d", request.MinRouteRevision)
	}
	if err := state.ObserveRoute(request, ReadRouteResponse{
		Outcome: ReadReady, Route: testReadyRoute(), RouteRevision: 12,
	}); err != nil {
		t.Fatal(err)
	}
	request = state.RouteRequest("/g", "rk", "s1", 7, false)
	if request.MinRouteRevision != 12 {
		t.Fatalf("minimum = %d", request.MinRouteRevision)
	}
	hint := LeaderHint{MemberID: "r1", Endpoint: "https://r1.internal", Term: 4}
	if err := state.ObserveRoute(request, ReadRouteResponse{Outcome: ReadNeedLeader, LeaderHint: &hint}); err != nil {
		t.Fatal(err)
	}
	old := LeaderHint{MemberID: "old", Endpoint: "https://old.internal", Term: 3}
	if err := state.ObserveRoute(request, ReadRouteResponse{Outcome: ReadNeedLeader, LeaderHint: &old}); err != nil {
		t.Fatal(err)
	}
	if got, ok := state.LeaderHint(7); !ok || got != hint {
		t.Fatalf("leader hint = %+v ok=%v", got, ok)
	}
}

func routeRequest(strong bool) ReadRouteRequest {
	return ReadRouteRequest{
		RequestIdentity: RequestIdentity{
			ClusterID: "cluster-1", StorageGeneration: "g1", SystemEpoch: 2,
			ManifestDigest: "manifest-digest", ShardID: 7,
		},
		Group: "/g", RouteKey: "rk", SandboxID: "s1", Strong: strong,
	}
}

func testReadyRoute() *clusterstate.ReadyRoute {
	digest := sha256.Sum256([]byte("binding"))
	return &clusterstate.ReadyRoute{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		AccessToken: "token", TemplateRef: "e2b-snp-t1", StorageGeneration: "g1",
		BindingDigest: hex.EncodeToString(digest[:]), LastEventSeq: 3,
	}
}
