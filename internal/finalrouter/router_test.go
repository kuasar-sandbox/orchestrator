package finalrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	serveIdentity   routeapi.RegistryServeIdentity
	route           clusterstate.ReadyRoute
	routeState      clusterstate.RouteWorkflowState
	revision        uint64
	reserveMins     []uint64
	readMins        []uint64
	reserveStarted  chan struct{}
	reserveRelease  chan struct{}
	reserveCalls    int
	reserveErr      error
	resumeCalls     int
	resume          func(string, string, uint64) (routeclient.RouteMutationResult, error)
	deleteCalls     int
	deleteRoute     func(string, string, uint64) (routeclient.RouteMutationResult, error)
	readRoute       func(string, string, uint64) (routeclient.RouteReadResult, error)
	readAddressable func(string, string, uint64) (routeclient.RouteReadResult, error)
	register        func(string, string, routeapi.BuildInput) (routeclient.BuildMutationResult, error)
	readBuild       func(context.Context, string, string, uint64) (routeclient.BuildReadResult, error)
	listedRoutes    []routeapi.ListedRoute
	listNextToken   string
	listState       clusterstate.RouteWorkflowState
	listLimit       uint32
	listToken       string
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
func (c *revisionControl) ResumeSandbox(_ context.Context, group, routeKey string, minimum uint64) (routeclient.RouteMutationResult, error) {
	c.resumeCalls++
	if c.resume != nil {
		return c.resume(group, routeKey, minimum)
	}
	return routeclient.RouteMutationResult{ServeIdentity: c.serveIdentity, Response: routeapi.RouteMutationResponse{
		Outcome: routeapi.MutationReady, Group: group, RouteKey: routeKey,
		State: clusterstate.WorkflowRouteReady, Route: &c.route, RouteRevision: c.revision,
	}}, nil
}
func (c *revisionControl) DeleteSandbox(_ context.Context, group, routeKey string, minimum uint64) (routeclient.RouteMutationResult, error) {
	c.deleteCalls++
	if c.deleteRoute == nil {
		return routeclient.RouteMutationResult{}, errors.New("unexpected DeleteSandbox")
	}
	return c.deleteRoute(group, routeKey, minimum)
}
func (c *revisionControl) ReadRoute(_ context.Context, group, routeKey string, minimum uint64) (routeclient.RouteReadResult, error) {
	c.readMins = append(c.readMins, minimum)
	if c.readRoute != nil {
		return c.readRoute(group, routeKey, minimum)
	}
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
	if c.readAddressable != nil {
		return c.readAddressable(group, routeKey, minimum)
	}
	state := c.routeState
	if state == "" {
		state = clusterstate.WorkflowRouteReady
	}
	return routeclient.RouteReadResult{ServeIdentity: c.serveIdentity, Response: routeapi.ReadRouteResponse{
		Outcome: routeapi.ReadReady, Group: group, RouteKey: routeKey,
		State: state, Route: &c.route, RouteRevision: c.revision,
	}}, nil
}

func TestSandboxControlDistinguishesDefinitiveNotFoundFromUnavailable(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		control *revisionControl
		status  int
	}{
		{
			name: "strong not found", path: "/sandboxes/missing", status: http.StatusNotFound,
			control: &revisionControl{readAddressable: func(group, routeKey string, minimum uint64) (routeclient.RouteReadResult, error) {
				return routeclient.RouteReadResult{Response: routeapi.ReadRouteResponse{
					Outcome: routeapi.ReadNotFound, Group: group, RouteKey: routeKey,
				}}, nil
			}},
		},
		{
			name: "read unavailable", path: "/sandboxes/missing", status: http.StatusServiceUnavailable,
			control: &revisionControl{readAddressable: func(string, string, uint64) (routeclient.RouteReadResult, error) {
				return routeclient.RouteReadResult{}, errors.New("permit expired")
			}},
		},
		{
			name: "resume pending", path: "/sandboxes/paused/connect", status: http.StatusServiceUnavailable,
			control: &revisionControl{resume: func(group, routeKey string, minimum uint64) (routeclient.RouteMutationResult, error) {
				return routeclient.RouteMutationResult{Response: routeapi.RouteMutationResponse{
					Outcome: routeapi.MutationPending, Group: group, RouteKey: routeKey,
				}}, nil
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router, err := New(test.control, allowCaller{}, "example.test", time.Minute, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodGet, "http://api.example.test"+test.path, nil)
			request.Host = "api.example.test"
			request.Header.Set(HeaderGroup, "/group")
			response := httptest.NewRecorder()
			router.Handler().ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status = %d, want %d: %s", response.Code, test.status, response.Body.String())
			}
		})
	}
}

func TestDeleteRequiresExactSandboxResourcePath(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity}
	control.deleteRoute = func(group, routeKey string, _ uint64) (routeclient.RouteMutationResult, error) {
		return routeclient.RouteMutationResult{ServeIdentity: serveIdentity, Response: routeapi.RouteMutationResponse{
			Outcome: routeapi.MutationTerminal, Group: group, RouteKey: routeKey,
			State: clusterstate.WorkflowRouteTombstone, RouteRevision: 9,
		}}, nil
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"/v2/sandboxes/route-1/pause",
		"/v2/sandboxes/route-1/connect",
		"/v2/sandboxes/route-1/timeout",
	} {
		request := httptest.NewRequest(http.MethodDelete, "http://api.example.test"+path, nil)
		request.Host = "api.example.test"
		request.Header.Set(HeaderGroup, "/group")
		response := httptest.NewRecorder()
		router.Handler().ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("DELETE %s = %d, want %d", path, response.Code, http.StatusMethodNotAllowed)
		}
	}
	if control.deleteCalls != 0 {
		t.Fatalf("child-resource DELETE calls = %d, want 0", control.deleteCalls)
	}

	request := httptest.NewRequest(http.MethodDelete, "http://api.example.test/v2/sandboxes/nested%2Froute", nil)
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || control.deleteCalls != 1 {
		t.Fatalf("exact DELETE = %d calls=%d body=%s", response.Code, control.deleteCalls, response.Body.String())
	}
}

func TestDeleteFencesConcurrentStaleRouteReadAtCommittedRevision(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	for _, test := range []struct {
		name    string
		outcome string
		state   clusterstate.RouteWorkflowState
		status  int
	}{
		{name: "terminal", outcome: routeapi.MutationTerminal, state: clusterstate.WorkflowRouteTombstone, status: http.StatusNoContent},
		{name: "pending", outcome: routeapi.MutationPending, state: clusterstate.WorkflowRouteDeleting, status: http.StatusAccepted},
	} {
		t.Run(test.name, func(t *testing.T) {
			control := &revisionControl{serveIdentity: serveIdentity}
			control.deleteRoute = func(group, routeKey string, _ uint64) (routeclient.RouteMutationResult, error) {
				return routeclient.RouteMutationResult{ServeIdentity: serveIdentity, Response: routeapi.RouteMutationResponse{
					Outcome: test.outcome, Group: group, RouteKey: routeKey,
					State: test.state, RouteRevision: 9,
				}}, nil
			}
			router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
			if err != nil {
				t.Fatal(err)
			}

			_, releaseStaleRead := router.beginRouteRevision("/group", "route-1")
			request := httptest.NewRequest(http.MethodDelete, "http://api.example.test/v2/sandboxes/route-1", nil)
			request.Host = "api.example.test"
			request.Header.Set(HeaderGroup, "/group")
			response := httptest.NewRecorder()
			router.Handler().ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("DELETE status = %d, want %d: %s", response.Code, test.status, response.Body.String())
			}

			stale := &routeEntry{
				Route: clusterstate.ReadyRoute{SandboxID: "sandbox-1"},
				State: clusterstate.WorkflowRouteReady, Group: "/group", RouteKey: "route-1",
				Revision: 8, ServeIdentity: serveIdentity,
			}
			if router.rememberRoute(stale) {
				t.Fatal("pre-delete READY read reinstalled below the committed delete revision")
			}
			if minimum := router.minimumRouteRevision("/group", "route-1"); minimum != 9 {
				t.Fatalf("minimum Route revision = %d, want 9", minimum)
			}
			releaseStaleRead()
		})
	}
}

func TestFirstDataRequestUsesCallerAuthorizationForCreatedRoute(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{
		serveIdentity: serveIdentity, revision: 8,
		route: clusterstate.ReadyRoute{
			SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 1, DataEndpoint: "127.0.0.1:0",
			RegistryGeneration: "generation-1", BindingDigest: "binding-1", AccessToken: "minted-token",
			TargetPort: 49983,
		},
		readRoute: func(group, routeKey string, _ uint64) (routeclient.RouteReadResult, error) {
			return routeclient.RouteReadResult{ServeIdentity: serveIdentity, Response: routeapi.ReadRouteResponse{
				Outcome: routeapi.ReadNotFound, Group: group, RouteKey: routeKey,
			}}, nil
		},
		readAddressable: func(group, routeKey string, _ uint64) (routeclient.RouteReadResult, error) {
			return routeclient.RouteReadResult{ServeIdentity: serveIdentity, Response: routeapi.ReadRouteResponse{
				Outcome: routeapi.ReadNotFound, Group: group, RouteKey: routeKey,
			}}, nil
		},
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://49983-route-1.example.test/", nil)
	request.Host = "49983-route-1.example.test"
	request.Header.Set(HeaderGroup, "/group")
	request.Header.Set(HeaderAPIKey, "caller-key")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway || control.reserveCalls != 1 {
		t.Fatalf("first data request = %d reserves=%d body=%s", response.Code, control.reserveCalls, response.Body.String())
	}
}

func TestExistingDataRouteAcceptsProviderAPIKey(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity, revision: 8, route: clusterstate.ReadyRoute{
		SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 1, DataEndpoint: "127.0.0.1:1",
		RegistryGeneration: "generation-1", BindingDigest: "binding-1", AccessToken: "minted-token",
		TargetPort: 49983,
	}}
	authorizer := &recordingAuthorizer{allow: true}
	router, err := New(control, authorizer, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://49983-route-1.example.test/", nil)
	request.Host = "49983-route-1.example.test"
	request.Header.Set(HeaderGroup, "/group")
	request.Header.Set(HeaderAPIKey, "caller-key")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway || authorizer.calls != 1 {
		t.Fatalf("existing data request = %d auth_calls=%d body=%s", response.Code, authorizer.calls, response.Body.String())
	}
}

func TestDataPlaneEnforceRejectsUnverifiedProviderKey(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	for _, mode := range []string{"off", "log"} {
		t.Run(mode, func(t *testing.T) {
			authorizer := &recordingAuthorizer{}
			router, err := New(&revisionControl{
				serveIdentity: serveIdentity, revision: 8,
				route: clusterstate.ReadyRoute{
					SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 1, DataEndpoint: "127.0.0.1:1",
					RegistryGeneration: "generation-1", BindingDigest: "binding-1",
					AccessToken: "minted-token", TargetPort: 49983,
				},
			}, authorizer, "example.test", time.Minute, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			router.SetAuthMode(mode)
			request := httptest.NewRequest(http.MethodGet, "http://49983-route-1.example.test/", nil)
			request.Host = "49983-route-1.example.test"
			request.Header.Set(HeaderGroup, "/group")
			request.Header.Set(HeaderAPIKey, "unverified-key")
			response := httptest.NewRecorder()
			router.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusUnauthorized || authorizer.calls != 1 {
				t.Fatalf("data request = %d auth_calls=%d body=%s", response.Code, authorizer.calls, response.Body.String())
			}
		})
	}
}

func TestPrepareDataBackendRequestStripsOnlyConsumedProviderCredential(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://sandbox.test/", nil)
	request.Header.Set(HeaderAPIKey, "tenant-wide-secret")
	request.Header.Set("Authorization", "Bearer application-token")
	prepareDataBackendRequest(request, "49983-sandbox-1.example.test", "execution-token", true, "")
	if request.Header.Get(HeaderAPIKey) != "" {
		t.Fatal("Provider API key was forwarded to the Sandbox")
	}
	if request.Header.Get("Authorization") != "Bearer application-token" {
		t.Fatal("unconsumed application Authorization header was removed")
	}
	if request.Header.Get(HeaderAccessTok) != "execution-token" {
		t.Fatal("execution access token was not injected")
	}

	bearer := httptest.NewRequest(http.MethodGet, "http://sandbox.test/", nil)
	bearer.Header.Set("Authorization", "Bearer provider-secret")
	prepareDataBackendRequest(bearer, "49983-sandbox-1.example.test", "execution-token", true, "Authorization")
	if bearer.Header.Get("Authorization") != "" {
		t.Fatal("consumed Provider bearer credential was forwarded to the Sandbox")
	}
}

func TestSandboxResumeRequiresExactPostConnectAction(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity, revision: 8, route: clusterstate.ReadyRoute{
		SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 1, DataEndpoint: "127.0.0.1:1",
		RegistryGeneration: "generation-1", BindingDigest: "binding-1",
	}}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		method string
		path   string
		calls  int
	}{
		{method: http.MethodGet, path: "/sandboxes/route-1/connect", calls: 0},
		{method: http.MethodPost, path: "/sandboxes/route-1/extra/connect", calls: 0},
		{method: http.MethodPost, path: "/v2/sandboxes/route-1/connect", calls: 1},
	} {
		request := httptest.NewRequest(test.method, "http://api.example.test"+test.path, nil)
		request.Host = "api.example.test"
		request.Header.Set(HeaderGroup, "/group")
		response := httptest.NewRecorder()
		router.Handler().ServeHTTP(response, request)
		if control.resumeCalls != test.calls {
			t.Fatalf("%s %s resume calls = %d, want %d", test.method, test.path, control.resumeCalls, test.calls)
		}
	}
}

func TestV2SandboxItemPathIsNormalizedForNodeAPI(t *testing.T) {
	seenPath := ""
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		seenPath = request.URL.EscapedPath()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer node.Close()
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity, revision: 8, route: clusterstate.ReadyRoute{
		SandboxID: "sandbox-1", NodeID: "node-1", NodeEpoch: 1,
		DataEndpoint: strings.TrimPrefix(node.URL, "http://"), RegistryGeneration: "generation-1",
		BindingDigest: "binding-1",
	}}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://api.example.test/v2/sandboxes/route-1", nil)
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || seenPath != "/sandboxes/sandbox-1" {
		t.Fatalf("v2 forward = %d path=%q body=%s", response.Code, seenPath, response.Body.String())
	}
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
func (c *revisionControl) ListRoutesPage(
	_ context.Context,
	_ string,
	state clusterstate.RouteWorkflowState,
	limit uint32,
	nextToken string,
) (routeclient.RouteListPageResult, error) {
	if c.listedRoutes == nil {
		return routeclient.RouteListPageResult{}, errors.New("unexpected ListRoutesPage")
	}
	c.listState, c.listLimit, c.listToken = state, limit, nextToken
	return routeclient.RouteListPageResult{
		Routes: c.listedRoutes, NextToken: c.listNextToken, ServeIdentity: c.serveIdentity,
	}, nil
}

type allowCaller struct{}

func (allowCaller) Verify(context.Context, string, string) (bool, error) { return true, nil }

type recordingAuthorizer struct {
	calls int
	allow bool
}

func (a *recordingAuthorizer) Verify(context.Context, string, string) (bool, error) {
	a.calls++
	return a.allow, nil
}

func routerTestPresentation() clusterstate.SandboxPresentationV1 {
	return clusterstate.SandboxPresentationV1{
		CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1",
		StartedAt: 1, EndAt: 2, Metadata: map[string]string{"tenant": "value"},
	}
}

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
	_, releaseRevision := router.beginRouteRevision(old.Group, old.RouteKey)
	router.rememberRoute(old)
	router.rejectStaleRoute(old)
	if cached := router.cachedRoute(old.Group, old.RouteKey); cached != nil {
		t.Fatalf("stale Route remained cached: %+v", cached)
	}
	if router.rememberRoute(old) {
		t.Fatal("Route below the revision fence was reinstalled")
	}
	if cached := router.cachedRoute(old.Group, old.RouteKey); cached != nil {
		t.Fatalf("fenced Route was reinstalled: %+v", cached)
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
	if minimum := router.minimumRouteRevision(old.Group, old.RouteKey); minimum != 8 {
		t.Fatalf("satisfied minimum Route revision = %d, want cached floor 8", minimum)
	}
	releaseRevision()
	router.evictRoute(old.Group, old.RouteKey)
	if minimum := router.minimumRouteRevision(old.Group, old.RouteKey); minimum != 8 {
		t.Fatalf("inactive Route revision floor = %d, want retained during grace period", minimum)
	}
	router.cleanupCaches(time.Now().Add(router.routeTTL + time.Second))
	if _, err := router.resolveRoute(context.Background(), old.Group, old.RouteKey); err != nil {
		t.Fatalf("resolve Route after cache eviction: %v", err)
	}
	if len(control.readMins) != 2 || control.readMins[1] != 0 {
		t.Fatalf("ReadRoute minimums after cache eviction = %v", control.readMins)
	}
}

func TestInactiveRouteRevisionFencesAreBoundedByCache(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	router, err := New(&revisionControl{serveIdentity: serveIdentity}, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 1000; index++ {
		routeKey := fmt.Sprintf("route-%d", index)
		entry := &routeEntry{
			Route: clusterstate.ReadyRoute{SandboxID: fmt.Sprintf("sandbox-%d", index)},
			Group: "/group", RouteKey: routeKey, Revision: uint64(index + 1), ServeIdentity: serveIdentity,
		}
		if !router.rememberRoute(entry) {
			t.Fatalf("remember Route %d", index)
		}
		router.evictRoute(entry.Group, entry.RouteKey)
	}
	if len(router.minimumRevisions) != 1000 {
		t.Fatalf("inactive revision floors were not retained during grace period: %d", len(router.minimumRevisions))
	}
	router.cleanupCaches(time.Now().Add(router.routeTTL + time.Second))
	if len(router.minimumRevisions) != 0 || len(router.routeRevisionRefs) != 0 {
		t.Fatalf("inactive revision state leaked: minimums=%d refs=%d", len(router.minimumRevisions), len(router.routeRevisionRefs))
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
		BindingDigest: "binding-1", TemplateRef: "template-1", Presentation: routerTestPresentation(),
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
	if body["sandboxID"] != "route-1" || body["routeKey"] != "route-1" || body["envdVersion"] != "0.6.1" {
		t.Fatalf("northbound Sandbox identity = %+v", body)
	}
	if strings.Contains(response.Body.String(), "node-local-sandbox") {
		t.Fatalf("create leaked concrete SID: %s", response.Body.String())
	}
}

func TestListRendersCompleteSDKProjectionWithoutNodeFanout(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{serveIdentity: serveIdentity, listedRoutes: []routeapi.ListedRoute{{
		RouteKey: "route-1", State: clusterstate.WorkflowRoutePaused, NodeID: "node-1",
		TemplateRef: "template-1", Presentation: routerTestPresentation(),
	}}}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://api.example.test/v2/sandboxes", nil)
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	var body []map[string]any
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil || len(body) != 1 {
		t.Fatalf("list = %d %s", response.Code, response.Body.String())
	}
	item := body[0]
	if item["sandboxID"] != "route-1" || item["state"] != "paused" || item["cpuCount"] != float64(2) ||
		item["memoryMB"] != float64(2048) || item["diskSizeMB"] != float64(64) ||
		item["envdVersion"] != "0.6.1" || item["startedAt"] != "1970-01-01T00:00:01Z" ||
		item["endAt"] != "1970-01-01T00:00:02Z" {
		t.Fatalf("SDK list projection = %+v", item)
	}
}

func TestListPassesStateLimitAndContinuation(t *testing.T) {
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	control := &revisionControl{
		serveIdentity: serveIdentity, listNextToken: "next-page",
		listedRoutes: []routeapi.ListedRoute{{
			RouteKey: "route-1", State: clusterstate.WorkflowRoutePaused, NodeID: "node-1",
			TemplateRef: "template-1", Presentation: routerTestPresentation(),
		}},
	}
	router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(
		http.MethodGet, "http://api.example.test/v2/sandboxes?state=paused&limit=7&nextToken=previous-page", nil,
	)
	request.Host = "api.example.test"
	request.Header.Set(HeaderGroup, "/group")
	response := httptest.NewRecorder()
	router.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("x-next-token") != "next-page" {
		t.Fatalf("paged list = %d next=%q body=%s", response.Code, response.Header().Get("x-next-token"), response.Body.String())
	}
	if control.listState != clusterstate.WorkflowRoutePaused || control.listLimit != 7 || control.listToken != "previous-page" {
		t.Fatalf("list query = state=%q limit=%d token=%q", control.listState, control.listLimit, control.listToken)
	}
}

func TestCreatePreservesEscapableRouteKeys(t *testing.T) {
	for _, routeKey := range []string{"nested/route", "escaped%2Froute", "route key", "路由"} {
		t.Run(routeKey, func(t *testing.T) {
			control := &revisionControl{serveIdentity: routeapi.RegistryServeIdentity{
				ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
				RegistryLayoutDigest: "registry-layout-1",
			}, route: clusterstate.ReadyRoute{SandboxID: "sandbox-1", RegistryGeneration: "generation-1"}}
			router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "http://api.example.test/sandboxes", bytes.NewBufferString(`{}`))
			request.Host = "api.example.test"
			request.Header.Set(HeaderGroup, "/group")
			request.Header.Set(HeaderRouteKey, routeKey)
			response := httptest.NewRecorder()
			router.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusCreated || control.reserveCalls != 1 {
				t.Fatalf("create = %d calls=%d body=%s", response.Code, control.reserveCalls, response.Body.String())
			}
			var body map[string]any
			if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body["routeKey"] != routeKey {
				t.Fatalf("Route key changed: body=%+v err=%v", body, err)
			}
		})
	}
}

func TestCreateRejectsUnaddressableRouteKeys(t *testing.T) {
	for _, routeKey := range []string{".", "..", "bad\x00key", "bad\x7fkey", string([]byte{0xff})} {
		t.Run(routeKey, func(t *testing.T) {
			control := &revisionControl{serveIdentity: routeapi.RegistryServeIdentity{
				ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
				RegistryLayoutDigest: "registry-layout-1",
			}}
			router, err := New(control, allowCaller{}, "example.test", time.Minute, slog.Default())
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequest(http.MethodPost, "http://api.example.test/sandboxes", bytes.NewBufferString(`{}`))
			request.Host = "api.example.test"
			request.Header.Set(HeaderGroup, "/group")
			request.Header[HeaderRouteKey] = []string{routeKey}
			response := httptest.NewRecorder()
			router.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || control.reserveCalls != 0 {
				t.Fatalf("create = %d calls=%d body=%s", response.Code, control.reserveCalls, response.Body.String())
			}
		})
	}
}

func TestEscapedRouteKeyPathRoundTrip(t *testing.T) {
	routeKey, ok := escapedPathObjectID("/v2/sandboxes/user1%2Fsession1/connect", "/sandboxes/")
	if !ok || routeKey != "user1/session1" {
		t.Fatalf("decoded Route key = %q, %v", routeKey, ok)
	}
	escaped := rewritePathObjectID(
		strings.TrimPrefix("/v2/sandboxes/user1%2Fsession1/connect", "/v2"),
		"/sandboxes/", "node-local-sandbox",
	)
	if escaped != "/sandboxes/node-local-sandbox/connect" {
		t.Fatalf("rewritten path = %q", escaped)
	}
}

func TestControlPathClassificationUsesEscapedRouteKeyBoundaries(t *testing.T) {
	path := "/sandboxes/team%2Ffiles%2Frun"
	if !sandboxVerb(path) || buildOperation(path) || sandboxMigrationOperation(http.MethodGet, path) {
		t.Fatalf("escaped Sandbox route was misclassified: sandbox=%v build=%v migration=%v",
			sandboxVerb(path), buildOperation(path), sandboxMigrationOperation(http.MethodGet, path))
	}
	for _, test := range []struct {
		method string
		path   string
		want   bool
	}{
		{method: http.MethodGet, path: "/sandboxes/export"},
		{method: http.MethodGet, path: "/sandboxes/team%2Fexport"},
		{method: http.MethodPost, path: "/sandboxes/import", want: true},
		{method: http.MethodPost, path: "/sandboxes/route-1/export", want: true},
	} {
		if got := sandboxMigrationOperation(test.method, test.path); got != test.want {
			t.Errorf("sandboxMigrationOperation(%q, %q) = %v, want %v", test.method, test.path, got, test.want)
		}
	}
}

func TestForwardNodeControlCanonicalizesBearerAndFencesSuccessfulPause(t *testing.T) {
	var authorization, path string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		authorization, path = request.Header.Get("Authorization"), request.URL.EscapedPath()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backend.Close()
	serveIdentity := routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}
	router, err := New(&revisionControl{serveIdentity: serveIdentity}, allowCaller{}, "example.test", time.Minute, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	entry := &routeEntry{
		Group: "/group", RouteKey: "team/route", Revision: 7, ServeIdentity: serveIdentity,
		Route: clusterstate.ReadyRoute{
			SandboxID: "node-local-sandbox", NodeID: "node-1", NodeEpoch: 1,
			DataEndpoint: strings.TrimPrefix(backend.URL, "http://"), RegistryGeneration: "generation-1",
			BindingDigest: "binding-1",
		},
	}
	if !router.rememberRoute(entry) {
		t.Fatal("remember Route")
	}
	request := httptest.NewRequest(http.MethodPost, "http://api.example.test/sandboxes/team%2Froute/pause", nil)
	request.Header.Set("Authorization", "bearer provider-secret")
	response := httptest.NewRecorder()
	router.forwardNodeControl(response, request, entry, nil)
	if response.Code != http.StatusNoContent || authorization != "Bearer provider-secret" ||
		path != "/sandboxes/node-local-sandbox/pause" {
		t.Fatalf("forwarded pause = %d auth=%q path=%q body=%s", response.Code, authorization, path, response.Body.String())
	}
	if cached := router.cachedRoute(entry.Group, entry.RouteKey); cached != nil {
		t.Fatalf("successful pause retained READY cache: %+v", cached)
	}
	if minimum := router.minimumRouteRevision(entry.Group, entry.RouteKey); minimum != entry.Revision+1 {
		t.Fatalf("pause revision floor = %d, want %d", minimum, entry.Revision+1)
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

func TestBuildLookupFailureReturnsUnavailable(t *testing.T) {
	control := &revisionControl{serveIdentity: routeapi.RegistryServeIdentity{
		ClusterID: "cluster-1", RegistryGeneration: "generation-1", SystemEpoch: 1,
		RegistryLayoutDigest: "registry-layout-1",
	}}
	control.readBuild = func(context.Context, string, string, uint64) (routeclient.BuildReadResult, error) {
		return routeclient.BuildReadResult{}, errors.New("Registry unavailable")
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
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("Build lookup failure status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestCheckedBuildDemandRejectsOverflow(t *testing.T) {
	maximum := ^uint64(0)
	maxCPU := int(maximum / 1000)
	maxMemory := int(maximum / (1 << 20))
	if demand, err := checkedBuildDemand(maxCPU, maxMemory); err != nil ||
		demand.CPU != uint64(maxCPU)*1000 || demand.Memory != uint64(maxMemory)*(1<<20) {
		t.Fatalf("maximum Build demand = %+v, %v", demand, err)
	}
	if _, err := checkedBuildDemand(maxCPU+1, 1); err == nil {
		t.Fatal("overflowing CPU ceiling was accepted")
	}
	if _, err := checkedBuildDemand(1, maxMemory+1); err == nil {
		t.Fatal("overflowing memory ceiling was accepted")
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
			"sandboxID": "node-local-sandbox",
			"metadata":  map[string]any{"sandboxID": "external-id", "value": "node-local-sandbox"},
			"nested":    []string{"node-local-sandbox"},
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
	var body map[string]any
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatalf("rewritten Sandbox response = %d %s", response.Code, response.Body.String())
	}
	metadata, _ := body["metadata"].(map[string]any)
	nested, _ := body["nested"].([]any)
	if body["sandboxID"] != "route-1" || metadata["sandboxID"] != "external-id" ||
		metadata["value"] != "node-local-sandbox" || len(nested) != 1 || nested[0] != "node-local-sandbox" {
		t.Fatalf("Sandbox identity rewrite changed opaque fields: %+v", body)
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
