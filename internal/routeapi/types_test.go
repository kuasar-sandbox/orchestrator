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
	if err := (ReadRouteResponse{Outcome: ReadReady, Group: "/g", RouteKey: "rk", Route: testReadyRoute(), RouteRevision: 11}).ValidateFor(request); err == nil {
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
	service := fakeService{route: ReadRouteResponse{Outcome: ReadReady, Group: "/g", RouteKey: "rk", Route: testReadyRoute(), RouteRevision: 12}}
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

func TestPositiveReadsRequireExactTableKeyIdentity(t *testing.T) {
	request := routeRequest(false)
	response := ReadRouteResponse{
		Outcome: ReadReady, Group: request.Group, RouteKey: request.RouteKey,
		Route: testReadyRoute(), RouteRevision: 1,
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	response.RouteKey = "another-route"
	if err := response.ValidateFor(request); err == nil {
		t.Fatal("READY projection for another Route key was accepted")
	}

	buildRequest := ReadBuildRequest{RequestIdentity: request.RequestIdentity, Group: "/g", BuildID: "b1"}
	build := &clusterstate.BuildProjection{
		BuildID: "b1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1",
		BindingDigest: testReadyRoute().BindingDigest, LastEventSeq: 1,
	}
	buildResponse := ReadBuildResponse{
		Outcome: ReadReady, Group: "/g", Build: build,
		BuildState: clusterstate.BuildBuilding, BuildRevision: 1,
	}
	if err := buildResponse.ValidateFor(buildRequest); err != nil {
		t.Fatal(err)
	}
	buildResponse.Group = "/another-group"
	if err := buildResponse.ValidateFor(buildRequest); err == nil {
		t.Fatal("positive Build projection for another Group was accepted")
	}
}

func TestBuildReadResponseEnforcesOutcomeUnionAndReadyArtifact(t *testing.T) {
	request := routeRequest(true)
	buildRequest := ReadBuildRequest{RequestIdentity: request.RequestIdentity, Group: "/g", BuildID: "b1", Strong: true}
	build := &clusterstate.BuildProjection{
		BuildID: "b1", NodeID: "n1", NodeEpoch: 7, RegistryGeneration: "g1",
		BindingDigest: testReadyRoute().BindingDigest, LastEventSeq: 1,
	}

	missing := ReadBuildResponse{Outcome: ReadNotFound, Build: build}
	if err := missing.ValidateFor(buildRequest); err == nil {
		t.Fatal("NOT_FOUND response carrying a Build projection was accepted")
	}
	ready := ReadBuildResponse{
		Outcome: ReadReady, Group: "/g", Build: build,
		BuildState: clusterstate.BuildReady, BuildRevision: 1,
	}
	if err := ready.ValidateFor(buildRequest); err == nil {
		t.Fatal("READY Build without an artifact was accepted")
	}
	ready.Build.ArtifactRef = "manifest://artifact"
	if err := ready.ValidateFor(buildRequest); err != nil {
		t.Fatalf("READY Build with artifact: %v", err)
	}
	errorResponse := ReadBuildResponse{
		Outcome: ReadReady, Group: "/g", Build: build,
		BuildState: clusterstate.BuildError, BuildRevision: 1,
	}
	errorResponse.Build.ArtifactRef = ""
	if err := errorResponse.ValidateFor(buildRequest); err == nil {
		t.Fatal("BUILD_ERROR without a reason was accepted")
	}
	errorResponse.Build.Reason = "builder failed"
	if err := errorResponse.ValidateFor(buildRequest); err != nil {
		t.Fatalf("BUILD_ERROR with reason: %v", err)
	}
}

func TestFinalNegativeReadsRejectLeaderHints(t *testing.T) {
	hint := &LeaderHint{MemberID: "r1", Endpoint: "https://r1.internal", Term: 1}
	routeRequest := routeRequest(true)
	for _, outcome := range []string{ReadNotFound, ReadConflict, ReadUnavailable} {
		if err := (ReadRouteResponse{Outcome: outcome, LeaderHint: hint}).ValidateFor(routeRequest); err == nil {
			t.Fatalf("Route outcome %s accepted a leader hint", outcome)
		}
		buildRequest := ReadBuildRequest{RequestIdentity: routeRequest.RequestIdentity, Group: "/g", BuildID: "b1", Strong: true}
		if err := (ReadBuildResponse{Outcome: outcome, LeaderHint: hint}).ValidateFor(buildRequest); err == nil {
			t.Fatalf("Build outcome %s accepted a leader hint", outcome)
		}
	}
}

func routeRequest(strong bool) ReadRouteRequest {
	return ReadRouteRequest{
		RequestIdentity: RequestIdentity{
			ClusterID: "cluster-1", RegistryGeneration: "g1", SystemEpoch: 2,
			RegistryLayoutDigest: "manifest-digest", ShardID: 7,
		},
		Group: "/g", RouteKey: "rk", SandboxID: "s1", Strong: strong,
	}
}

func testReadyRoute() *clusterstate.ReadyRoute {
	digest := sha256.Sum256([]byte("binding"))
	return &clusterstate.ReadyRoute{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		AccessToken: "token", TemplateRef: "e2b-snp-t1", RegistryGeneration: "g1",
		BindingDigest: hex.EncodeToString(digest[:]), LastEventSeq: 3,
	}
}
