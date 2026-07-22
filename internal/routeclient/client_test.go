package routeclient

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"reflect"
	"strings"
	"testing"
	"time"

	clusterstate "github.com/kuasar-sandbox/orchestrator/internal/cluster"
	"github.com/kuasar-sandbox/orchestrator/internal/raftstore"
	"github.com/kuasar-sandbox/orchestrator/internal/routeapi"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestGenerationAuthorizationRequiresEveryFinalServeGate(t *testing.T) {
	client := testClient(t)
	base := routeapi.PermitResponse{
		ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1", SystemEpoch: 1,
		RegistryLayoutDigest: client.digest, CommitIndex: 7, MaxLifetimeMillis: 5000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}
	identity := serveIdentity(base)
	for _, test := range []struct {
		name   string
		mutate func(*routeapi.PermitResponse)
	}{
		{name: "serve", mutate: func(p *routeapi.PermitResponse) { p.ServeGate = false }},
		{name: "cutover", mutate: func(p *routeapi.PermitResponse) { p.CutoverGate = false }},
		{name: "recovery", mutate: func(p *routeapi.PermitResponse) { p.RecoveryClosed = false }},
	} {
		t.Run(test.name, func(t *testing.T) {
			permit := base
			test.mutate(&permit)
			client.permit = &cachedPermit{response: permit, expires: time.Now().Add(time.Minute)}
			if _, err := client.CurrentServeIdentity(false); err == nil || client.CacheAuthorized(identity) {
				t.Fatal("closed final gate authorized Router reads or cache forwarding")
			}
		})
	}
	client.permit = &cachedPermit{response: base, expires: time.Now().Add(time.Minute)}
	if got, err := client.CurrentServeIdentity(true); err != nil || got != identity || !client.CacheAuthorized(identity) {
		t.Fatalf("open serveIdentity = %+v, %v cache=%v", got, err, client.CacheAuthorized(identity))
	}
	base.WriteGate = false
	client.permit = &cachedPermit{response: base, expires: time.Now().Add(time.Minute)}
	if _, err := client.CurrentServeIdentity(true); err == nil {
		t.Fatal("closed write gate authorized a mutation")
	}
	if _, err := client.CurrentServeIdentity(false); err != nil {
		t.Fatalf("write gate incorrectly blocked reads: %v", err)
	}
}

func TestLocalReadsUseReplicaSpreadRendezvousWithoutLeaderBias(t *testing.T) {
	client := testClient(t)
	client.leaders[0] = routeapi.LeaderHint{
		MemberID: "registry-a", Endpoint: client.registryLayout.Members[0].InternalEndpoint, Term: 10,
	}
	primaries := make(map[string]int)
	for index := 0; index < 256; index++ {
		endpoints := client.localReadEndpoints(0, string(rune(index)))
		if len(endpoints) != 3 {
			t.Fatalf("local endpoints=%d, want 3", len(endpoints))
		}
		primaries[endpoints[0].MemberID]++
	}
	if len(primaries) != 3 {
		t.Fatalf("rendezvous primaries were not spread across replicas: %v", primaries)
	}
	writeOrder := client.shardEndpoints(0)
	if len(writeOrder) != 3 || writeOrder[0].MemberID != "registry-a" {
		t.Fatalf("leader hint was not retained for strong/write paths: %+v", writeOrder)
	}
}

func TestPostJSONPreservesBoundedMemberFailureDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "data shard proposal timed out", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	err := postJSON(context.Background(), Endpoint{
		MemberID: "registry-a", BaseURL: server.URL, Client: server.Client(),
	}, "/mutation", struct{}{}, &struct{}{})
	if err == nil || !strings.Contains(err.Error(), "registry-a returned 503 Service Unavailable: data shard proposal timed out") {
		t.Fatalf("member failure = %v", err)
	}
}

func TestPostJSONTreatsClientRejectionsAsDefinitive(t *testing.T) {
	for _, test := range []struct {
		status  int
		unknown bool
	}{
		{status: http.StatusBadRequest},
		{status: http.StatusForbidden},
		{status: http.StatusNotFound},
		{status: http.StatusMethodNotAllowed},
		{status: http.StatusServiceUnavailable, unknown: true},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "rejected", test.status)
			}))
			defer server.Close()
			err := postJSON(context.Background(), Endpoint{
				MemberID: "registry-a", BaseURL: server.URL, Client: server.Client(),
			}, "/mutation", struct{}{}, &struct{}{})
			if err == nil || requestMayHaveReached(err) != test.unknown {
				t.Fatalf("status %d error = %v, unknown=%v", test.status, err, requestMayHaveReached(err))
			}
		})
	}
}

func TestMutationErrorsDistinguishDefinitiveAndUnknownDelivery(t *testing.T) {
	for _, test := range []struct {
		name    string
		wrote   bool
		unknown bool
	}{
		{name: "dial failure is definitive"},
		{name: "written request is unknown", wrote: true, unknown: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := testClient(t)
			transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
				if test.wrote {
					trace := httptrace.ContextClientTrace(request.Context())
					trace.WroteRequest(httptrace.WroteRequestInfo{})
				}
				return nil, errors.New("transport failed")
			})
			for memberID, endpoint := range client.endpoints {
				endpoint.Client = &http.Client{Transport: transport}
				client.endpoints[memberID] = endpoint
			}
			_, err := client.buildMutation(context.Background(), 0, routeapi.RegisterBuildPath, routeapi.RegisterBuildRequest{})
			if err == nil || errors.Is(err, ErrMutationOutcomeUnknown) != test.unknown {
				t.Fatalf("mutation error = %v, unknown=%v", err, test.unknown)
			}
		})
	}
}

func TestAddressableStrongReadImmediatelyFollowsVerifiedLeaderHint(t *testing.T) {
	client := testClient(t)
	client.permit = &cachedPermit{response: routeapi.PermitResponse{
		ClusterID: client.registryLayout.ClusterID, RegistryGeneration: client.registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: client.digest, CommitIndex: 1, MaxLifetimeMillis: 5000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, expires: time.Now().Add(time.Minute)}
	var calls []string
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		calls = append(calls, request.URL.Hostname())
		var response routeapi.ReadRouteResponse
		switch request.URL.Hostname() {
		case "registry-a.test":
			response = routeapi.ReadRouteResponse{Outcome: routeapi.ReadNeedLeader, LeaderHint: &routeapi.LeaderHint{
				MemberID: "registry-c", Endpoint: "https://registry-c.test:9443", Term: 2,
			}}
		case "registry-c.test":
			response = routeapi.ReadRouteResponse{Outcome: routeapi.ReadNotFound}
		default:
			return nil, errors.New("unhinted replica was contacted")
		}
		body, _ := json.Marshal(response)
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(string(body))), Request: request,
		}, nil
	})
	for memberID, endpoint := range client.endpoints {
		endpoint.Client = &http.Client{Transport: transport}
		client.endpoints[memberID] = endpoint
	}
	result, err := client.ReadAddressableRoute(context.Background(), "/g", "route-1", 0)
	if err != nil || result.Response.Outcome != routeapi.ReadNotFound {
		t.Fatalf("addressable read = %+v, %v", result, err)
	}
	if !reflect.DeepEqual(calls, []string{"registry-a.test", "registry-c.test"}) {
		t.Fatalf("strong read calls = %v", calls)
	}
}

func TestListRoutesRestartsBucketWhenSnapshotChangesBetweenPages(t *testing.T) {
	client := testClient(t)
	client.permit = &cachedPermit{response: routeapi.PermitResponse{
		ClusterID: client.registryLayout.ClusterID, RegistryGeneration: client.registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: client.digest, CommitIndex: 1, MaxLifetimeMillis: 5000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, expires: time.Now().Add(time.Minute)}
	var firstBucketCursors []string
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		var input routeapi.ListRoutesRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			return nil, err
		}
		response := routeapi.ListRoutesResponse{Bucket: input.Bucket, SnapshotRevision: 11}
		if input.Bucket == 0 {
			firstBucketCursors = append(firstBucketCursors, input.AfterRouteKey)
			entry := func(key string) routeapi.ListedRoute {
				return routeapi.ListedRoute{
					RouteKey: key, State: clusterstate.WorkflowRouteReady,
					NodeID: "node-1", TemplateRef: "template-1",
					Presentation: clusterstate.SandboxPresentationV1{
						CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1", StartedAt: 1, EndAt: 2,
					},
				}
			}
			switch len(firstBucketCursors) {
			case 1:
				response.SnapshotRevision = 10
				response.Routes = []routeapi.ListedRoute{entry("route-a")}
				response.NextRouteKey = "route-a"
			case 2:
				response.Routes = []routeapi.ListedRoute{entry("route-b")}
			case 3:
				response.Routes = []routeapi.ListedRoute{entry("route-a")}
				response.NextRouteKey = "route-a"
			default:
				response.Routes = []routeapi.ListedRoute{entry("route-b")}
			}
		}
		body, _ := json.Marshal(response)
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(string(body))), Request: request,
		}, nil
	})
	for memberID, endpoint := range client.endpoints {
		endpoint.Client = &http.Client{Transport: transport}
		client.endpoints[memberID] = endpoint
	}
	result, err := client.ListRoutes(context.Background(), "/group")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Routes) != 2 || result.Routes[0].RouteKey != "route-a" || result.Routes[1].RouteKey != "route-b" ||
		result.BucketRevisions[0] != 11 {
		t.Fatalf("stable Route list = %+v", result)
	}
	wantCursors := []string{"", "route-a", "", "route-a"}
	if !reflect.DeepEqual(firstBucketCursors, wantCursors) {
		t.Fatalf("bucket cursors = %v, want %v", firstBucketCursors, wantCursors)
	}
}

func TestListRoutesPageUsesBoundedBucketCursor(t *testing.T) {
	client := testClient(t)
	client.permit = &cachedPermit{response: routeapi.PermitResponse{
		ClusterID: client.registryLayout.ClusterID, RegistryGeneration: client.registryLayout.RegistryGeneration,
		SystemEpoch: 1, RegistryLayoutDigest: client.digest, CommitIndex: 1, MaxLifetimeMillis: 5000,
		ServeGate: true, WriteGate: true, CutoverGate: true, RecoveryClosed: true,
	}, expires: time.Now().Add(time.Minute)}
	entry := func(key string) routeapi.ListedRoute {
		return routeapi.ListedRoute{
			RouteKey: key, State: clusterstate.WorkflowRoutePaused, NodeID: "node-1", TemplateRef: "template-1",
			Presentation: clusterstate.SandboxPresentationV1{
				CPUCount: 2, MemoryMB: 2048, DiskSizeMB: 64, EnvdVersion: "0.6.1", StartedAt: 1, EndAt: 2,
			},
		}
	}
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		var input routeapi.ListRoutesRequest
		if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
			return nil, err
		}
		if input.State != clusterstate.WorkflowRoutePaused || input.Limit > 3 {
			return nil, fmt.Errorf("unexpected list request: %+v", input)
		}
		response := routeapi.ListRoutesResponse{Bucket: input.Bucket, SnapshotRevision: 11}
		switch input.Bucket {
		case 0:
			if input.AfterRouteKey == "" {
				response.Routes = []routeapi.ListedRoute{entry("route-a")}
			}
		case 1:
			if input.AfterRouteKey == "" {
				response.Routes = []routeapi.ListedRoute{entry("route-b"), entry("route-c")}
			} else if input.AfterRouteKey == "route-b" {
				response.Routes = []routeapi.ListedRoute{entry("route-c")}
			}
		}
		body, _ := json.Marshal(response)
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(string(body))), Request: request,
		}, nil
	})
	for memberID, endpoint := range client.endpoints {
		endpoint.Client = &http.Client{Transport: transport}
		client.endpoints[memberID] = endpoint
	}
	first, err := client.ListRoutesPage(context.Background(), "/group", clusterstate.WorkflowRoutePaused, 2, "")
	if err != nil || len(first.Routes) != 2 || first.Routes[0].RouteKey != "route-a" ||
		first.Routes[1].RouteKey != "route-b" || first.NextToken == "" {
		t.Fatalf("first Route page = %+v, %v", first, err)
	}
	cursor, err := decodeRouteListCursor(first.NextToken)
	if err != nil || cursor.Bucket != 1 || cursor.AfterRouteKey != "route-b" {
		t.Fatalf("Route page cursor = %+v, %v", cursor, err)
	}
	second, err := client.ListRoutesPage(
		context.Background(), "/group", clusterstate.WorkflowRoutePaused, 2, first.NextToken,
	)
	if err != nil || len(second.Routes) != 1 || second.Routes[0].RouteKey != "route-c" || second.NextToken != "" {
		t.Fatalf("second Route page = %+v, %v", second, err)
	}
	if _, err := client.ListRoutesPage(
		context.Background(), "/other", clusterstate.WorkflowRoutePaused, 2, first.NextToken,
	); !errors.Is(err, ErrInvalidRouteListToken) {
		t.Fatalf("cross-group cursor error = %v", err)
	}
}

func testClient(t *testing.T) *Client {
	t.Helper()
	registryLayout := testRouteRegistryLayout()
	digest, err := registryLayout.Digest()
	if err != nil {
		t.Fatal(err)
	}
	endpoints := make([]Endpoint, len(registryLayout.Members))
	for index, member := range registryLayout.Members {
		endpoints[index] = Endpoint{MemberID: member.MemberID, BaseURL: member.InternalEndpoint, Client: &http.Client{}}
	}
	client, err := New(registryLayout, digest, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func testRouteRegistryLayout() raftstore.RegistryLayout {
	members := []raftstore.RegistryMember{
		{MemberID: "registry-a", InternalEndpoint: "https://registry-a.test:9443", RaftEndpoint: "127.0.0.1:63001"},
		{MemberID: "registry-b", InternalEndpoint: "https://registry-b.test:9443", RaftEndpoint: "127.0.0.1:63002"},
		{MemberID: "registry-c", InternalEndpoint: "https://registry-c.test:9443", RaftEndpoint: "127.0.0.1:63003"},
	}
	replicas := []raftstore.ReplicaPlacement{
		{MemberID: "registry-a", ReplicaID: 1},
		{MemberID: "registry-b", ReplicaID: 2},
		{MemberID: "registry-c", ReplicaID: 3},
	}
	shards := make([]raftstore.ShardPlacement, 16)
	for shardID := range shards {
		shards[shardID] = raftstore.ShardPlacement{
			ShardID: uint32(shardID), Replicas: append([]raftstore.ReplicaPlacement(nil), replicas...),
		}
	}
	bootstrap := sha256.Sum256([]byte("bootstrap"))
	return raftstore.RegistryLayout{
		FormatVersion: raftstore.RegistryLayoutFormatV1, ClusterID: "cluster-1", RegistryGeneration: "serveIdentity-1",
		RegistryLayoutVersion: 1, SchemaVersion: 1, ProtocolVersion: 1, HashVersion: "ShardHashV1",
		VirtualShardCount: 16, RouteBucketCount: 16, BuildBucketCount: 16,
		ReplicationFactor: raftstore.DefaultReplication, ServePermitMaxMillis: 5000,
		BootstrapTokenDigest: hex.EncodeToString(bootstrap[:]), Members: members,
		SystemReplicas: append([]raftstore.ReplicaPlacement(nil), replicas...), DataShards: shards,
	}
}
