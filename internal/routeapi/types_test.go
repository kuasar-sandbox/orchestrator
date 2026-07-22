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
	"strings"
	"testing"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
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
	if err := (ReadRouteResponse{Outcome: ReadReady, Group: "/g", RouteKey: "rk", State: clusterstate.WorkflowRouteReady, Route: testReadyRoute(), RouteRevision: 11}).ValidateFor(request); err == nil {
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

func TestLeaderHintRequiresCanonicalHTTPSEndpoint(t *testing.T) {
	hint := LeaderHint{MemberID: "r1", Endpoint: "http://r1.internal", Term: 1}
	if err := hint.Validate(); err == nil {
		t.Fatal("non-HTTPS leader endpoint accepted")
	}
	hint.Endpoint = "https://r1.internal/"
	if err := hint.Validate(); err == nil {
		t.Fatal("non-canonical leader endpoint accepted")
	}
	hint.Endpoint = "https://r1.internal"
	if err := hint.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTrustedHandlerRejectsOversizedBodyAfterValidJSON(t *testing.T) {
	body, err := json.Marshal(routeRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	body = append(body, bytes.Repeat([]byte(" "), maxBodyBytes-len(body)+1)...)
	request := httptest.NewRequest(http.MethodPost, ReadRoutePath, bytes.NewReader(body))
	w := httptest.NewRecorder()
	NewHandler(fakeService{}, func(*http.Request) error { return nil }).ServeHTTP(w, request)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized request status = %d", w.Code)
	}
}

func TestTrustedHandlerRequiresInternalTransportIdentity(t *testing.T) {
	service := fakeService{route: ReadRouteResponse{Outcome: ReadReady, Group: "/g", RouteKey: "rk", State: clusterstate.WorkflowRouteReady, Route: testReadyRoute(), RouteRevision: 12}}
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
		State: clusterstate.WorkflowRouteReady, Route: testReadyRoute(), RouteRevision: 1,
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatal(err)
	}
	response.RouteKey = "another-route"
	if err := response.ValidateFor(request); err == nil {
		t.Fatal("READY projection for another Route key was accepted")
	}

	buildRequest := ReadBuildRequest{RequestIdentity: request.RequestIdentity, Group: "/g", BuildID: "b1"}
	build := testBuildProjection()
	buildResponse := ReadBuildResponse{
		Outcome: ReadReady, Group: "/g", Build: build,
		BuildState: clusterstate.BuildRegistered, BuildRevision: 1,
	}
	if err := buildResponse.ValidateFor(buildRequest); err != nil {
		t.Fatal(err)
	}
	buildResponse.Group = "/another-group"
	if err := buildResponse.ValidateFor(buildRequest); err == nil {
		t.Fatal("positive Build projection for another Group was accepted")
	}
}

func TestBuildReadResponseOnlyExposesRegistrationBinding(t *testing.T) {
	request := routeRequest(true)
	buildRequest := ReadBuildRequest{RequestIdentity: request.RequestIdentity, Group: "/g", BuildID: "b1", Strong: true}
	build := testBuildProjection()

	missing := ReadBuildResponse{Outcome: ReadNotFound, Build: build}
	if err := missing.ValidateFor(buildRequest); err == nil {
		t.Fatal("NOT_FOUND response carrying a Build projection was accepted")
	}
	registered := ReadBuildResponse{
		Outcome: ReadReady, Group: "/g", Build: build,
		BuildState: clusterstate.BuildRegistered, BuildRevision: 1,
	}
	if err := registered.ValidateFor(buildRequest); err != nil {
		t.Fatalf("registered Build binding: %v", err)
	}
	registered.BuildState = clusterstate.BuildStarting
	if err := registered.ValidateFor(buildRequest); err == nil {
		t.Fatal("unbound BUILD_STARTING projection was exposed as a positive read")
	}
	pending := ReadBuildResponse{
		Outcome: ReadConflict, Group: "/g", BuildState: clusterstate.BuildStarting, BuildRevision: 2,
		Pending: &PendingBuildProjection{BuildID: "b1", TemplateRef: "transient-b1", Profile: types.ProfileE2B},
	}
	if err := pending.ValidateFor(buildRequest); err != nil {
		t.Fatalf("strong pending Build projection: %v", err)
	}
	buildRequest.Strong = false
	if err := pending.ValidateFor(buildRequest); err == nil {
		t.Fatal("replica-local read accepted a pending Build projection")
	}
}

func TestPausedProjectionRequiresExplicitStrongAddressableRead(t *testing.T) {
	request := routeRequest(false)
	request.Addressable = true
	if err := request.Validate(); err == nil {
		t.Fatal("replica-local addressable projection request was accepted")
	}
	request.Strong = true
	response := ReadRouteResponse{
		Outcome: ReadReady, Group: request.Group, RouteKey: request.RouteKey,
		State: clusterstate.WorkflowRoutePaused, Route: testReadyRoute(), RouteRevision: 12,
	}
	if err := response.ValidateFor(request); err != nil {
		t.Fatalf("strong addressable PAUSED projection: %v", err)
	}
	request.Addressable = false
	if err := response.ValidateFor(request); err == nil {
		t.Fatal("ordinary strong read accepted a PAUSED projection")
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
		Group: "/g", RouteKey: "rk", Strong: strong,
	}
}

func testReadyRoute() *clusterstate.ReadyRoute {
	digest := sha256.Sum256([]byte("binding"))
	templateRef := "e2b-img-" + strings.Repeat("c", 64)
	spec, _ := clusterstate.MarshalSandboxDispatchSpec(clusterstate.SandboxDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateRef: templateRef,
		AuthKeyFingerprint: strings.Repeat("a", 24), ManifestKeyFingerprint: strings.Repeat("b", 24),
		AccessToken: "token", TargetPort: 3000,
		Request: clusterstate.NodeRequestEnvelopeV1{
			Version: clusterstate.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/sandboxes",
			Body: []byte(`{"metadata":null,"templateID":"` + templateRef + `","timeout":0}`),
		},
	})
	intent, _ := clusterstate.NewDispatchIntent([]byte("demand"), spec, "provider-v1")
	return &clusterstate.ReadyRoute{
		SandboxID: "s1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		TargetPort: 3000, AccessToken: "token", TrafficAccessToken: "traffic-token",
		TemplateRef: templateRef, RegistryGeneration: "g1",
		BindingDigest: hex.EncodeToString(digest[:]), LastEventSeq: 3, Intent: intent,
	}
}

func testBuildProjection() *clusterstate.BuildProjection {
	digest := sha256.Sum256([]byte("build-binding"))
	spec, _ := clusterstate.MarshalBuildDispatchSpec(clusterstate.BuildDispatchSpecV1{
		Version: clusterstate.DispatchSpecVersionV1, TemplateID: "template-1",
		AuthKeyFingerprint: strings.Repeat("b", 24), ManifestKeyFingerprint: strings.Repeat("c", 24),
		Profile: types.ProfileBare, CPUCount: 1, MemoryMB: 512,
		Request: clusterstate.NodeRequestEnvelopeV1{Version: clusterstate.NodeRequestEnvelopeVersionV1, Method: "POST", Path: "/v3/templates", Body: []byte(`{"cpuCount":1,"memoryMB":512,"metadata":null,"name":"","profile":"bare","tags":null}`)},
	})
	intent, _ := clusterstate.NewDispatchIntent([]byte("demand"), spec, "provider-v1")
	return &clusterstate.BuildProjection{
		BuildID: "b1", NodeID: "n1", NodeEpoch: 7, DataEndpoint: "10.0.0.1:8443",
		RegistryGeneration: "g1", BindingDigest: hex.EncodeToString(digest[:]),
		Intent: intent, TemplateRef: "template-1",
	}
}
