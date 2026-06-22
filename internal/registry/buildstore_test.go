package registry

import (
	"context"
	"testing"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// TestReserveBuildResourceAware drives the §7.5 core: a build reserves its node's
// build pool IMMEDIATELY (RESERVED occupies), a second build that would
// oversubscribe is refused, and a terminal build event releases the pool.
func TestReserveBuildResourceAware(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	// n1 has a 2-core build pool.
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1", BuildCapacity: &routesync.BuildResources{CPU: 2000, Mem: 8 << 30, Storage: 100 << 30}})
	reg.addNode(&fakeConn{nodeID: "n1"})

	// First build (1.5 cores) fits + is committed RESERVED on n1.
	r1, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 1500}})
	if err != nil || r1.NodeID != "n1" || r1.BuildID == "" {
		t.Fatalf("first build: %+v err=%v", r1, err)
	}
	if rec, found, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID); !found || rec.State != BuildRegistered || !rec.occupies() {
		t.Fatalf("first build not RESERVED in BuildStore: %+v found=%v", rec, found)
	}

	// Second build (1.5 cores) would oversubscribe (1.5+1.5 > 2.0) → refused now,
	// before any heartbeat — RESERVED occupies immediately (the user's invariant).
	if _, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 1500}}); err != ErrNoNode {
		t.Fatalf("oversubscribing build should be refused (ErrNoNode), got %v", err)
	}

	// The first build finishes → releases the pool → a second build now fits.
	reg.applyBuildEvent(ctx, &routesync.BuildEvent{BuildID: r1.BuildID, Group: "/g", State: "ready", TemplateID: "e2b-img-x"})
	if rec, _, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID); rec.occupies() {
		t.Fatal("a ready build should not occupy the pool")
	}
	if r3, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g", Resources: &routesync.BuildResources{CPU: 1500}}); err != nil || r3.NodeID != "n1" {
		t.Fatalf("after release, build should fit: %+v err=%v", r3, err)
	}
}

// TestReserveBuildDefaultResources: an unspecified request uses the default pool
// footprint (cluster.md §7.5 "不指定则使用默认").
func TestReserveBuildDefaultResources(t *testing.T) {
	ctx := context.Background()
	reg := testReg(t)
	reg.stores.PutNode(ctx, &NodeRecord{NodeID: "n1"}) // no declared build pool → unconstrained
	reg.addNode(&fakeConn{nodeID: "n1"})
	r1, err := reg.ReserveBuild(ctx, BuildReserveReq{Group: "/g"})
	if err != nil || r1.BuildID == "" || r1.TemplateID == "" {
		t.Fatalf("default-resources build: %+v err=%v", r1, err)
	}
	rec, found, _ := reg.stores.GetBuildInGroup(ctx, "/g", r1.BuildID)
	if !found || rec.Resources == nil || rec.Resources.CPU != defaultBuildResources.CPU {
		t.Fatalf("default resources not applied: %+v", rec)
	}
}
