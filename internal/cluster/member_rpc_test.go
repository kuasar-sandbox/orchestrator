package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouteQuorumOverHTTPReplicas(t *testing.T) {
	ctx := context.Background()
	local := NewMemoryRouteReplica()
	remoteA := NewMemoryRouteReplica()
	remoteB := NewMemoryRouteReplica()

	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeRouteReplica(w, r, remoteA)
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeRouteReplica(w, r, remoteB)
	}))
	defer srvB.Close()

	q := NewRouteQuorum("writer-a", local, NewHTTPRouteReplica(srvA.URL, srvA.Client()), NewHTTPRouteReplica(srvB.URL, srvB.Client()))
	rec, err := q.CAS(ctx, "/g", "rk", 0, func(RouteRecord, bool) (RouteRecord, bool, error) {
		return RouteRecord{Group: "/g", RouteKey: "rk", SandboxID: "sb-http", State: RouteReady, NodeID: "n1"}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Meta.Rev == 0 {
		t.Fatalf("missing rev: %+v", rec)
	}
	got, found, err := q.Get(ctx, "/g", "rk")
	if err != nil || !found || got.SandboxID != "sb-http" {
		t.Fatalf("get route = %+v found=%v err=%v", got, found, err)
	}
}

func TestNodeQuorumOverHTTPReplicas(t *testing.T) {
	ctx := context.Background()
	local := NewMemoryNodeReplica()
	remoteA := NewMemoryNodeReplica()
	remoteB := NewMemoryNodeReplica()

	srvA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeNodeReplica(w, r, remoteA)
	}))
	defer srvA.Close()
	srvB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeNodeReplica(w, r, remoteB)
	}))
	defer srvB.Close()

	q := NewNodeQuorum("writer-a", local, NewHTTPNodeReplica(srvA.URL, srvA.Client()), NewHTTPNodeReplica(srvB.URL, srvB.Client()))
	rec, err := q.CAS(ctx, "n-http", 0, func(NodeRecord, bool) (NodeRecord, bool, error) {
		return NodeRecord{NodeID: "n-http", State: NodeLive, DataEndpoint: "10.0.0.1:8443"}, true, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if rec.Meta.Rev == 0 {
		t.Fatalf("missing rev: %+v", rec)
	}
	got, found, err := q.Get(ctx, "n-http")
	if err != nil || !found || got.DataEndpoint != "10.0.0.1:8443" {
		t.Fatalf("get node = %+v found=%v err=%v", got, found, err)
	}
}
