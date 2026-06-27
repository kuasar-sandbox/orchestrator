package cluster

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMemoryRouteLinkCASAndGroupList(t *testing.T) {
	ctx := context.Background()
	k := NewMemoryKernel("m1")
	b1 := k.NextBallot(Ballot{})
	rec, err := k.PutRoute(ctx, RouteRecord{Group: "/g", RouteKey: "r1", SandboxID: "sb-1", State: RouteReserved, NodeID: "n1"}, 0, b1)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Meta.Rev != 1 || rec.Meta.Ballot != b1 {
		t.Fatalf("meta = %+v, want rev=1 ballot=%+v", rec.Meta, b1)
	}
	if _, err := k.PutRoute(ctx, RouteRecord{Group: "/g", RouteKey: "r1", State: RouteReady}, 0, k.NextBallot(b1)); !errors.Is(err, ErrConflict) {
		t.Fatalf("create over existing route err=%v, want ErrConflict", err)
	}
	ready, err := k.PutRoute(ctx, RouteRecord{Group: "/g", RouteKey: "r1", SandboxID: "sb-1", State: RouteReady, NodeID: "n1"}, rec.Meta.Rev, k.NextBallot(b1))
	if err != nil {
		t.Fatal(err)
	}
	if ready.Meta.Rev != 2 || ready.State != RouteReady {
		t.Fatalf("ready = %+v", ready)
	}
	_, err = k.PutRoute(ctx, RouteRecord{Group: "/g", RouteKey: "r1", State: RoutePaused}, ready.Meta.Rev, b1)
	if !errors.Is(err, ErrStale) {
		t.Fatalf("stale ballot err=%v, want ErrStale", err)
	}
	if _, err := k.PutRoute(ctx, RouteRecord{Group: "/other", RouteKey: "r2", State: RouteReady}, 0, Ballot{Round: 1, Writer: "m1"}); err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := k.ListRoutes(ctx, "/g", func(r RouteRecord) error {
		got = append(got, r.RouteKey)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "r1" {
		t.Fatalf("group list = %v, want [r1]", got)
	}
}

func TestMemoryNodeLinkProjectsNodeList(t *testing.T) {
	ctx := context.Background()
	k := NewMemoryKernel("m1")
	node, err := k.PutNode(ctx, NodeRecord{
		NodeID: "n1", State: NodeLive, Labels: map[string]string{"zone": "a"},
		Capacity: 10, DataEndpoint: "127.0.0.1:49983", RuntimeDigest: "rt1", Allocated: 100, Pool: 1000, Counts: 3,
	}, 0, Ballot{Round: 1, Writer: "m1"})
	if err != nil {
		t.Fatal(err)
	}
	if node.Meta.Rev != 1 {
		t.Fatalf("node rev=%d, want 1", node.Meta.Rev)
	}
	view, found, err := k.GetNodeList(ctx, "n1")
	if err != nil || !found {
		t.Fatalf("node_list found=%v err=%v", found, err)
	}
	if view.NodeID != "n1" || view.Labels["zone"] != "a" || view.DataEndpoint == "" {
		t.Fatalf("node_list projection = %+v", view)
	}
	if _, ok := reflect.TypeOf(NodeListEntry{}).FieldByName("Allocated"); ok {
		t.Fatal("node_list must not expose high-frequency allocation")
	}
	node.Allocated = 900
	node.Labels["zone"] = "mutated"
	again, _, _ := k.GetNode(ctx, "n1")
	if again.Allocated != 100 || again.Labels["zone"] != "a" {
		t.Fatalf("stored node mutated through returned copy: %+v", again)
	}
	if err := k.DeleteNode(ctx, "n1", node.Meta.Rev, Ballot{Round: 2, Writer: "m1"}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := k.GetNodeList(ctx, "n1"); err != nil || found {
		t.Fatalf("node_list after delete found=%v err=%v", found, err)
	}
}

func TestMemoryKernelNamespaceScanner(t *testing.T) {
	ctx := context.Background()
	k := NewMemoryKernel("scan")
	if _, err := k.PutRoute(ctx, RouteRecord{Group: "/g", RouteKey: "rk", State: RouteReady}, 0, k.NextBallot(Ballot{})); err != nil {
		t.Fatal(err)
	}
	if _, err := k.PutNode(ctx, NodeRecord{NodeID: "n1", State: NodeLive}, 0, k.NextBallot(Ballot{})); err != nil {
		t.Fatal(err)
	}
	var routeKeys, nodeKeys, nodeListKeys []string
	if err := k.ListKeys(ctx, NamespaceRouteLink, func(key string) error {
		routeKeys = append(routeKeys, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := k.ListKeys(ctx, NamespaceNodeLink, func(key string) error {
		nodeKeys = append(nodeKeys, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := k.ListKeys(ctx, NamespaceNodeList, func(key string) error {
		nodeListKeys = append(nodeListKeys, key)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(routeKeys, []string{RouteKey("/g", "rk")}) {
		t.Fatalf("route keys=%v", routeKeys)
	}
	if !reflect.DeepEqual(nodeKeys, []string{"n1"}) || !reflect.DeepEqual(nodeListKeys, []string{"n1"}) {
		t.Fatalf("node keys=%v node_list=%v", nodeKeys, nodeListKeys)
	}

	from := MemberView{Version: 1, Members: []string{"r1", "r2", "r3"}}
	to := MemberView{Version: 2, Members: []string{"r1", "r2", "r3", "r4"}}
	if _, err := PlanNamespaceHandoff(ctx, from, to, 3, NamespaceRouteLink, k); err != nil {
		t.Fatal(err)
	}
}
