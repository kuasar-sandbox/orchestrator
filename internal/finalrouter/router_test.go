package finalrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/placement"
	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
	"github.com/kuasar-sandbox/orchestrator/internal/routeclient"
)

type revisionControl struct {
	serveIdentity  routeapi.RegistryServeIdentity
	route          clusterstate.ReadyRoute
	routeState     clusterstate.RouteWorkflowState
	revision       uint64
	reserveMins    []uint64
	readMins       []uint64
	reserveStarted chan struct{}
	reserveRelease chan struct{}
	reserveCalls   int
	reserveErr     error
	resumeCalls    int
	register       func(string, string, routeapi.BuildInput) (routeclient.BuildMutationResult, error)
	readBuild      func(context.Context, string, string, uint64) (routeclient.BuildReadResult, error)
}

func (c *revisionControl) CurrentServeIdentity(bool) (routeapi.RegistryServeIdentity, error) {
	return c.serveIdentity, nil
}
func (c *revisionControl) CacheAuthorized(identity routeapi.RegistryServeIdentity) bool {
	return identity == c.serveIdentity
}
func (c *revisionControl) ReserveSandbox(_ context.Context, group, routeKey string, minimum uint64, _ routeapi.SandboxInput) (routeclient.RouteMutationResult, error) {
	c.reserveMins = append(c.reserveMins, minimum)
	c.reserveCalls++
	if c.reserveStarted != nil {
		close(c.reserveStarted)
		<-c.reserveRelease
	}
	if c.reserveErr != nil {
		return routeclient.RouteMutationResult{}, c.reserveErr
	}
	return routeclient.RouteMutationResult{ServeIdentity: c.serveIdentity, Response: routeapi.RouteMutationResponse{
		Outcome: routeapi.MutationReady, Group: group, RouteKey: routeKey,
		State: clusterstate.WorkflowRouteReady, Route: &c.route, RouteRevision: c.revision,
	}}, nil
}
func (c *revisionControl) ResumeSandbox(_ context.Context, group, routeKey string, _ uint64) (routeclient.RouteMutationResult, error) {
	c.resumeCalls++
	return routeclient.RouteMutationResult{ServeIdentity: c.serveIdentity, Response: routeapi.RouteMutationResponse{
		Outcome: routeapi.MutationReady, Group: group, RouteKey: routeKey,
		State: clusterstate.WorkflowRouteReady, Route: &c.route, RouteRevision: c.revision,
	}}, nil
}
func (*revisionControl) DeleteSandbox(context.Context, string, string, uint64) (routeclient.RouteMutationResult, error) {
	return routeclient.RouteMutationResult{}, errors.New("unexpected DeleteSandbox")
}
func (c *revisionControl) ReadRoute(_ context.Context, group, routeKey string, minimum uint64) (routeclient.RouteReadResult, error) {
	c.readMins = append(c.readMins, minimum)
	if c.routeState == clusterstate.WorkflowRoutePaused {
		return routeclient.RouteReadResult{ServeIdentity: c.serveIdentity, Response: routeapi.ReadRouteResponse{
			Outcome: routeapi.ReadConflict, Reason: string(clusterstate.WorkflowRoutePaused),
		}}, nil
	}
	return routeclient.RouteReadResult{ServeIdentity: c.serveIdentity, Response: routeapi.ReadRouteResponse{
		Outcome: routeapi.ReadReady, Group: group, RouteKey: routeKey,
		State: clusterstate.WorkflowRouteReady, Route: &c.route, RouteRevision: c.revision,
	}}, nil
}
func (c *revisionControl) ReadAddressableRoute(_ context.Context, group, routeKey string, minimum uint64) (routeclient.RouteReadResult, error) {
	c.readMins = append(c.readMins, minimum)
	state := c.routeState
	if state == "" {
		state = clusterstate.WorkflowRouteReady
	}
	return routeclient.RouteReadResult{ServeIdentity: c.serveIdentity, Response: routeapi.ReadRouteResponse{
		Outcome: routeapi.ReadReady, Group: group, RouteKey: routeKey,
		State: state, Route: &c.route, RouteRevision: c.revision,
	}}, nil
}
func (c *revisionControl) RegisterBuild(_ context.Context, group, buildID string, _ uint64, input routeapi.BuildInput) (routeclient.BuildMutationResult, error) {
	if c.register == nil {
		return routeclient.BuildMutationResult{}, errors.New("unexpected RegisterBuild")
	}
	return c.register(group, buildID, input)
}
func (c *revisionControl) ReadBuild(ctx context.Context, group, buildID string, revision uint64) (routeclient.BuildReadResult, error) {
	if c.readBuild == nil {
		return routeclient.BuildReadResult{}, errors.New("unexpected ReadBuild")
	}
	return c.readBuild(ctx, group, buildID, revision)
}
func (*revisionControl) ListRoutes(context.Context, string) (routeclient.RouteListResult, error) {
	return routeclient.RouteListResult{}, errors.New("unexpected ListRoutes")
}

type allowCaller struct{}

func (allowCaller) Verify(context.Context, string, string) (bool, error) { return true, nil }

func TestStaleProxyFailureRequiresNewerRouteRevision(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity, revision: 8, route: clusterstate.ReadyRoute{
		SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 8, DataEndpoint: "node-1:8443",
		RegistryGeneration: serveIdentity.RegistryGeneration, BindingDigest: "binding-new",
	}}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	old := &routeEntry{
		Route: clusterstate.ReadyRoute{SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 7},
		Group: "/group", RouteKey: "route-1", Revision: 7, ServeIdentity: serveIdentity,
	}
	router.rememberRoute(old)
	router.rejectStaleRoute(old)
	if cached := router.cachedRoute(old.Group, old.RouteKey); cached != nil {
		t.Fatalf("stale Route remained cached: %+v", cached)
	}
	if minimum := router.minimumRouteRevision(old.Group, old.RouteKey); minimum != 8 {
		t.Fatalf("minimum Route revision = %d, want 8", minimum)
	}

	resolved, err := router.resolveRoute(context.Background(), old.Group, old.RouteKey)
	if err != nil || resolved == nil || resolved.Revision != 8 {
		t.Fatalf("resolve newer Route = %+v, %v", resolved, err)
	}
	if len(control.readMins) != 1 || control.readMins[0] != 8 {
		t.Fatalf("ReadRoute minimums = %v", control.readMins)
	}
	if minimum := router.minimumRouteRevision(old.Group, old.RouteKey); minimum != 0 {
		t.Fatalf("satisfied minimum Route revision remained at %d", minimum)
	}
}

func TestProxyErrorsThatFenceRouteRevision(t *testing.T) {
	for _, kind := range []string{
		proxy.ProxyErrorNotFound, proxy.ProxyErrorWrongNodeEpoch,
		proxy.ProxyErrorWrongBinding, proxy.ProxyErrorRouteInactive,
	} {
		if !proxyErrorRequiresNewerRoute(kind) {
			t.Errorf("%q did not fence the cached Route revision", kind)
		}
	}
	for _, kind := range []string{proxy.ProxyErrorUnauthorized, proxy.ProxyErrorRouteError, proxy.ProxyErrorUpstreamError, ""} {
		if proxyErrorRequiresNewerRoute(kind) {
			t.Errorf("%q incorrectly advanced the Route revision fence", kind)
		}
	}
}

func TestParseDataHost(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		explicitID string
		wantID     string
		wantPort   int
		wantHost   bool
		wantError  bool
	}{
		{name: "route-key ingress", host: "data.example.test"},
		{name: "route-key ingress with exact ID", host: "data.example.test", explicitID: "sandbox-1", wantID: "sandbox-1"},
		{name: "sandbox host", host: "49983-sandbox-1.example.test", wantID: "sandbox-1", wantPort: 49983, wantHost: true},
		{name: "matching explicit ID", host: "49983-sandbox-1.example.test", explicitID: "sandbox-1", wantID: "sandbox-1", wantPort: 49983, wantHost: true},
		{name: "foreign host", host: "data.other.test", wantError: true},
		{name: "missing ID", host: "49983-.example.test", wantError: true},
		{name: "invalid port", host: "70000-sandbox-1.example.test", wantError: true},
		{name: "conflicting ID", host: "49983-sandbox-1.example.test", explicitID: "sandbox-2", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			id, port, hasHostPort, err := parseDataHost(test.host, "example.test", test.explicitID)
			if (err != nil) != test.wantError || id != test.wantID || port != test.wantPort || hasHostPort != test.wantHost {
				t.Fatalf("parseDataHost() = (%q, %d, %v, %v)", id, port, hasHostPort, err)
			}
		})
	}
}

func TestCreateExposesRouteKeyInsteadOfConcreteSandboxID(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity, revision: 8, route: clusterstate.ReadyRoute{
		SandboxID: "node-local-sandbox", NodeID: "node-1", NodeEpoch: 7,
		DataEndpoint: "node-1:8443", RegistryGeneration: serveIdentity.RegistryGeneration,
		BindingDigest: "binding-1", TemplateRef: "template-1",
	}}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://api.example.test/sandboxes", bytes.NewBufferString(`{}`))
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	request.Header.Set(HeaderRouteKey, "route-1")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	var body map[string]any
	if response.Code != http.StatusCreated || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatalf("create = %d %s", response.Code, response.Body.String())
	}
	if body["sandboxID"] != "route-1" || body["routeKey"] != "route-1" {
		t.Fatalf("northbound Sandbox identity = %+v", body)
	}
	if strings.Contains(response.Body.String(), "node-local-sandbox") {
		t.Fatalf("create leaked concrete SID: %s", response.Body.String())
	}
}

func TestConcurrentReserveRejectsDifferentImmutableInput(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{
		serveIdentity: serveIdentity, revision: 8,
		route:          clusterstate.ReadyRoute{SandboxID: "sandbox-1", RegistryGeneration: "generation-1"},
		reserveStarted: make(chan struct{}), reserveRelease: make(chan struct{}),
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, reserveErr := router.reserve(context.Background(), "/group", "route-1", routeapi.SandboxInput{
			Demand: placement.SandboxDemand{SlotUnits: 1}, Config: map[string]string{"mode": "first"},
		})
		firstDone <- reserveErr
	}()
	<-control.reserveStarted
	_, conflict := router.reserve(context.Background(), "/group", "route-1", routeapi.SandboxInput{
		Demand: placement.SandboxDemand{SlotUnits: 1}, Config: map[string]string{"mode": "second"},
	})
	if !errors.Is(conflict, errRouteInputConflict) {
		t.Fatalf("conflicting in-flight reserve = %v", conflict)
	}
	close(control.reserveRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if control.reserveCalls != 1 {
		t.Fatalf("Registry reserve calls = %d, want 1", control.reserveCalls)
	}
}

func TestBuildRegistrationPendingReturnsGeneratedStableIDs(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	for _, test := range []struct {
		name   string
		result routeclient.BuildMutationResult
		err    error
	}{
		{name: "committed pending", result: routeclient.BuildMutationResult{Response: routeapi.BuildMutationResponse{Outcome: routeapi.MutationPending}}},
		{name: "ambiguous response", err: errors.Join(routeclient.ErrMutationOutcomeUnknown, errors.New("response lost"))},
	} {
		t.Run(test.name, func(t *testing.T) {
			var capturedBuildID, capturedTemplateID string
			control := &revisionControl{serveIdentity: serveIdentity}
			control.register = func(_ string, buildID string, input routeapi.BuildInput) (routeclient.BuildMutationResult, error) {
				capturedBuildID, capturedTemplateID = buildID, input.TemplateID
				return test.result, test.err
			}
			router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "http://api.example.test/v3/templates",
				bytes.NewBufferString(`{"cpuCount":1,"memoryMB":512}`))
			request.Host = "api.example.test"
			request.Header.Set(HeaderGroup, "/group")
			response := httptest.NewRecorder()
			router.Handler().ServeHTTP(response, request)
			var body map[string]any
			if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &body) != nil {
				t.Fatalf("registration = %d %s", response.Code, response.Body.String())
			}
			if body["buildID"] != capturedBuildID || body["templateID"] != capturedTemplateID || body["state"] != "pending" {
				t.Fatalf("pending identity = %+v, captured=(%q,%q)", body, capturedBuildID, capturedTemplateID)
			}
		})
	}
}

func TestBuildRegistrationDefinitiveDeliveryFailureDoesNotPublishIDs(t *testing.T) {
	control := &revisionControl{}
	control.register = func(_ string, _ string, _ routeapi.BuildInput) (routeclient.BuildMutationResult, error) {
		return routeclient.BuildMutationResult{}, errors.New("all Registry dials failed")
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://api.example.test/v3/templates",
		bytes.NewBufferString(`{"cpuCount":1,"memoryMB":512}`))
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || strings.Contains(response.Body.String(), "buildID") {
		t.Fatalf("definitive registration failure = %d %s", response.Code, response.Body.String())
	}
}

func TestUnknownRouteMutationPublishesStableRouteKey(t *testing.T) {
	control := &revisionControl{reserveErr: errors.Join(routeclient.ErrMutationOutcomeUnknown, errors.New("response lost"))}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://api.example.test/sandboxes", bytes.NewBufferString(`{}`))
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	var body map[string]any
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &body) != nil || body["routeKey"] == "" {
		t.Fatalf("unknown Route mutation = %d %s", response.Code, response.Body.String())
	}
}

func TestPendingBuildStatusUsesCommittedStartingProjection(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity}
	control.readBuild = func(_ context.Context, group, buildID string, _ uint64) (routeclient.BuildReadResult, error) {
		return routeclient.BuildReadResult{ServeIdentity: serveIdentity, Response: routeapi.ReadBuildResponse{
			Outcome: routeapi.ReadConflict, Group: group, BuildState: clusterstate.BuildStarting, BuildRevision: 7,
			Pending: &routeapi.PendingBuildProjection{BuildID: buildID, TemplateRef: "transient-" + buildID, Profile: "e2b"},
		}}, nil
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet,
		"http://api.example.test/templates/transient-build-1/builds/build-1/status", nil)
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	var body map[string]any
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil ||
		body["status"] != "building" || body["buildID"] != "build-1" || body["templateID"] != "transient-build-1" {
		t.Fatalf("pending Build status = %d %s", response.Code, response.Body.String())
	}
}

func TestPendingBuildTriggerWaitsForRegistrationBinding(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get(clusterstate.DirectHeaderObjectID) != "build-1" {
			t.Errorf("forwarded Build ID = %q", request.Header.Get(clusterstate.DirectHeaderObjectID))
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	control := &revisionControl{serveIdentity: serveIdentity}
	reads := 0
	control.readBuild = func(_ context.Context, group, buildID string, _ uint64) (routeclient.BuildReadResult, error) {
		reads++
		response := routeapi.ReadBuildResponse{
			Outcome: routeapi.ReadConflict, Group: group, BuildState: clusterstate.BuildStarting, BuildRevision: 7,
			Pending: &routeapi.PendingBuildProjection{BuildID: buildID, TemplateRef: "transient-" + buildID, Profile: "e2b"},
		}
		if reads > 1 {
			response = routeapi.ReadBuildResponse{
				Outcome: routeapi.ReadReady, Group: group, BuildState: clusterstate.BuildRegistered, BuildRevision: 8,
				Build: &clusterstate.BuildProjection{
					BuildID: buildID, NodeID: "node-1", NodeEpoch: 1,
					DataEndpoint: strings.TrimPrefix(backend.URL, "http://"), RegistryGeneration: "generation-1",
					BindingDigest: "binding-1", TemplateRef: "transient-" + buildID,
				},
			}
		}
		return routeclient.BuildReadResult{ServeIdentity: serveIdentity, Response: response}, nil
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost,
		"http://api.example.test/v2/templates/transient-build-1/builds/build-1", bytes.NewBufferString(`{"fromImage":"base"}`))
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted || reads < 2 {
		t.Fatalf("pending Build trigger = %d reads=%d body=%s", response.Code, reads, response.Body.String())
	}
}

func TestPausedDataRouteAuthenticatesBeforeResume(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{
		serveIdentity: serveIdentity, routeState: clusterstate.WorkflowRoutePaused, revision: 8,
		route: clusterstate.ReadyRoute{
			SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 1, DataEndpoint: "127.0.0.1:1",
			RegistryGeneration: "generation-1", BindingDigest: "binding-1", AccessToken: "secret", TargetPort: 49983,
		},
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://49983-route-1.example.test/", nil)
	request.Host = "49983-route-1.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || control.resumeCalls != 0 {
		t.Fatalf("unauthorized paused route = %d resumes=%d body=%s", response.Code, control.resumeCalls, response.Body.String())
	}
}

func TestBuildRegistrationWithoutServePermitFailsBeforePublishingIDs(t *testing.T) {
	control := &revisionControl{}
	control.register = func(_ string, _ string, _ routeapi.BuildInput) (routeclient.BuildMutationResult, error) {
		return routeclient.BuildMutationResult{}, routeclient.ErrPermitUnavailable
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://api.example.test/v3/templates",
		bytes.NewBufferString(`{"cpuCount":1,"memoryMB":512}`))
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("registration = %d %s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "buildID") {
		t.Fatalf("definitive preflight failure published a Build ID: %s", response.Body.String())
	}
}

func TestPausedSandboxControlResolvesBoundSIDAfterCacheMiss(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/sandboxes/node-local-sandbox" ||
			request.Header.Get(clusterstate.DirectHeaderExecutionKind) != "sandbox" ||
			request.Header.Get(clusterstate.DirectHeaderObjectID) != "node-local-sandbox" ||
			request.Header.Get(clusterstate.DirectHeaderGroup) != "/group" ||
			request.Header.Get(clusterstate.DirectHeaderRouteKey) != "route-1" ||
			request.Header.Get(proxy.HeaderNodeID) != "node-1" ||
			request.Header.Get(proxy.HeaderNodeEpoch) != "7" ||
			request.Header.Get(proxy.HeaderRegistryGeneration) != "serveIdentity-1" ||
			request.Header.Get(proxy.HeaderBindingDigest) != "binding-1" {
			t.Errorf("direct Sandbox request = %s headers=%v", request.URL.Path, request.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"sandboxID": "node-local-sandbox", "nested": []string{"node-local-sandbox"},
		})
	}))
	defer backend.Close()
	control := &revisionControl{serveIdentity: serveIdentity, revision: 8, routeState: clusterstate.WorkflowRoutePaused, route: clusterstate.ReadyRoute{
		SandboxID: "node-local-sandbox", NodeID: "node-1", NodeEpoch: 7,
		DataEndpoint: strings.TrimPrefix(backend.URL, "http://"), RegistryGeneration: serveIdentity.RegistryGeneration,
		BindingDigest: "binding-1", TemplateRef: "template-1",
	}}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://api.example.test/sandboxes/route-1", nil)
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "node-local-sandbox") ||
		!strings.Contains(response.Body.String(), "route-1") {
		t.Fatalf("rewritten Sandbox response = %d %s", response.Code, response.Body.String())
	}
}

func TestBuildForwardUsesOnlyCommittedRegistrationFence(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v2/templates/transient-build-1/builds/build-1" ||
			request.Header.Get(clusterstate.DirectHeaderExecutionKind) != "build" ||
			request.Header.Get(clusterstate.DirectHeaderObjectID) != "build-1" ||
			request.Header.Get(clusterstate.DirectHeaderGroup) != "/group" ||
			request.Header.Get(clusterstate.DirectHeaderRouteKey) != "" ||
			request.Header.Get(proxy.HeaderNodeID) != "node-2" ||
			request.Header.Get(proxy.HeaderNodeEpoch) != "9" ||
			request.Header.Get(proxy.HeaderRegistryGeneration) != "serveIdentity-1" ||
			request.Header.Get(proxy.HeaderBindingDigest) != "build-binding" {
			t.Errorf("direct Build request = %s headers=%v", request.URL.Path, request.Header)
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer backend.Close()
	control := &revisionControl{serveIdentity: serveIdentity}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	router.rememberBuild(&buildEntry{
		Group: "/group", Revision: 11, ServeIdentity: serveIdentity,
		Build: clusterstate.BuildProjection{
			BuildID: "build-1", NodeID: "node-2", NodeEpoch: 9,
			DataEndpoint: strings.TrimPrefix(backend.URL, "http://"), RegistryGeneration: "serveIdentity-1",
			BindingDigest: "build-binding", TemplateRef: "transient-build-1",
		},
	})
	request := httptest.NewRequest(http.MethodPost,
		"http://api.example.test/v2/templates/transient-build-1/builds/build-1", bytes.NewBufferString(`{"fromImage":"base"}`))
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	request.Header.Set(clusterstate.DirectHeaderRouteKey, "forged-route")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("Build forward = %d %s", response.Code, response.Body.String())
	}
}
