package nodectl

import (
	"context"
	"github.com/kuasar-sandbox/sandboxer/pkg/resource"
	"testing"
	"time"
)

// The actual UDS protocol carries cache amounts independently from Guest
// memory, and heartbeat never turns a reclaim target into a physical grant.
func TestServerCacheBudgetProtocolAndGuestIsolation(t *testing.T) {
	srv, client, cleanup := startTestServer(t, 8<<30)
	defer cleanup()
	const mb = uint64(1 << 20)
	resp, err := client.Admit(AdmitParams{
		SandboxID: "cache-protocol", CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes: 128 << 20, StartupBudgetMemory: 128 << 20,
		CacheMaximum: 512 << 20, ClientFeatures: []string{FeatureCacheBudgetV1, resource.CacheMaximumFeature(512 << 20)},
	})
	if err != nil || resp == nil || resp.Status != StatusAdmitted || !resp.CacheSupported {
		t.Fatalf("cache negotiation: %+v %v", resp, err)
	}
	grant, err := client.RequestCacheBudgetContext(context.Background(), 0, 8*mb, 512<<20)
	if err != nil || grant.Granted != 8*mb || grant.Reservation != 8*mb {
		t.Fatalf("first cache grant: %+v %v", grant, err)
	}
	charge := srv.State.ResourceSnapshot().Reserved.MemoryBytes
	if charge != 128*mb || srv.State.ResourceSnapshot().CacheAllocated != 8*mb {
		t.Fatalf("host cache not independently charged: %d", charge)
	}
	hb, err := client.Heartbeat(0, 0, 0, 0)
	if err != nil || hb.NewAllocatable != 128*mb || hb.CacheReservation != 8*mb {
		t.Fatalf("heartbeat mixed Guest and cache: %+v %v", hb, err)
	}
	shrunk, err := client.RequestCacheBudgetContext(context.Background(), 0, 0, 512<<20)
	if err != nil || shrunk.Reservation != 0 || shrunk.Granted != 0 {
		t.Fatalf("cache return: %+v %v", shrunk, err)
	}
	if got := srv.State.ResourceSnapshot().Reserved.MemoryBytes; got != 128*mb {
		t.Fatalf("cache budget not returned: %d", got)
	}
	if _, err := client.RequestCacheBudgetContext(context.Background(), 8*mb, 0, 512<<20); err == nil {
		t.Fatal("node accepted a stale physical baseline")
	}
	if _, err := client.RequestCacheBudgetContext(context.Background(), 0, 8*mb, 1<<30); err == nil {
		t.Fatal("node accepted an unapproved maximum")
	}
}

func TestServerLegacyCacheCapabilityNotAdvertised(t *testing.T) {
	_, client, cleanup := startTestServer(t, 8<<30)
	defer cleanup()
	result, err := client.Admit(AdmitParams{
		SandboxID: "legacy-cache", CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes: 128 << 20, StartupBudgetMemory: 128 << 20,
	})
	if err != nil || result.Status != StatusAdmitted {
		t.Fatalf("legacy admit: %+v %v", result, err)
	}
	if result.CacheSupported {
		t.Fatal("legacy client negotiated unsupported cache grants")
	}
	if _, err := client.RequestCacheBudgetContext(context.Background(), 0, 8<<20, 32<<20); err == nil {
		t.Fatal("legacy session obtained cache grants without feature")
	}
}

func TestServerCacheInitialAdmitChargesAtomically(t *testing.T) {
	srv, client, cleanup := startTestServer(t, 8<<30)
	defer cleanup()
	const miB = uint64(1 << 20)
	result, err := client.Admit(AdmitParams{
		SandboxID: "initial-cache", CapacityMemoryBytes: 1 << 30,
		FloorMemoryBytes: 128 << 20, StartupBudgetMemory: 128 << 20,
		CacheMaximum: 1 << 30, CacheInitial: 8 * miB,
		ClientFeatures: []string{FeatureCacheBudgetV1, resource.CacheMaximumFeature(1 << 30)},
	})
	if err != nil || result.Status != StatusAdmitted || !result.CacheSupported || result.GrantedInitialCache != 8*miB {
		t.Fatalf("atomic Admit = %+v %v", result, err)
	}
	if r := reservationByTokenForTest(t, srv.State, client.Token()); r.CacheReservation != 8*miB || r.ReservationMemory != 128*miB {
		t.Fatalf("wrong initial separate charges: %+v", r)
	}
	if s := srv.State.ResourceSnapshot(); s.Reserved.MemoryBytes != 128*miB || s.CacheAllocated != 8*miB || s.StartupInFlight != 128*miB {
		t.Fatalf("initial cache incorrectly counted in startup pool: %+v", s)
	}
	if budget, err := client.RequestCacheBudgetContext(context.Background(), 8*miB, 0, 1<<30); err != nil || budget.Reservation != 8*miB || budget.Granted != 0 {
		t.Fatalf("initial cache double granted: %+v %v", budget, err)
	}
}

func TestAdmissionCacheInitialRespectsIndependentPoolLimit(t *testing.T) {
	s := makeState(4<<30, 0)
	s.Wm.StartupFactor = 0.5
	s.CachePoolCapacity = 4 << 20
	launch := LaunchAdmission{Operation: OperationCreate}
	if got := s.AnalyzeLaunchWithCache("first", 256<<20, 8<<20, launch); got.Status != OutcomePreCheckReject {
		t.Fatalf("first cache budget exceeded cache cap without blocking: %+v", got)
	}
	if got := s.AnalyzeLaunchWithCache("first", 256<<20, 0, launch); got.Status != OutcomeAdmitted {
		t.Fatalf("legacy admission affected by cache cap: %+v", got)
	}
}

func TestServerCacheReclaimDeliveredByHeartbeatAndGrantedAfterReturn(t *testing.T) {
	srv, a, cleanup := startTestServer(t, 8<<30)
	defer cleanup()
	srv.State.CachePoolCapacity = 64 << 20
	b := &Client{SocketPath: srv.Path}
	if err := b.Connect(); err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	admit := func(c *Client, sid string) {
		t.Helper()
		resp, err := c.Admit(AdmitParams{
			SandboxID: sid, CapacityMemoryBytes: 1 << 30,
			FloorMemoryBytes: 128 << 20, StartupBudgetMemory: 128 << 20,
			CacheMaximum: 512 << 20, CacheInitial: 0,
			ClientFeatures: []string{FeatureCacheBudgetV1, resource.CacheMaximumFeature(512 << 20)},
		})
		if err != nil || resp.Status != StatusAdmitted || !resp.CacheSupported {
			t.Fatalf("Admit %s: %+v %v", sid, resp, err)
		}
	}
	admit(a, "donor")
	admit(b, "waiter")
	request := func(c *Client, current, amount uint64) resource.CacheBudgetResult {
		t.Helper()
		res, err := c.RequestCacheBudgetContext(context.Background(), current, amount, 512<<20)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if g := request(a, 0, 48<<20); g.Granted != 48<<20 {
		t.Fatal(g)
	}
	if g := request(b, 0, 16<<20); g.Granted != 16<<20 {
		t.Fatal(g)
	}
	if g := request(b, 16<<20, 16<<20); g.Granted != 0 {
		t.Fatalf("unexpected grant before return %+v", g)
	}
	// Persistent demand (not the first denied burst) may request a donor
	// to trim clean cache. The command alone never releases the pool grant.
	time.Sleep(450 * time.Millisecond)
	if g := request(b, 16<<20, 16<<20); g.Granted != 0 {
		t.Fatalf("unexpected regrant before return %+v", g)
	}
	hb, err := a.Heartbeat(0, 0, 0, 0)
	if err != nil || hb.CacheReclaimTarget != 32<<20 || hb.CacheReservation != 48<<20 {
		t.Fatalf("donor did not receive reclaim via heartbeat: %+v %v", hb, err)
	}
	if got := srv.State.reservedCache; got != 64<<20 {
		t.Fatalf("reclaim directive incorrectly released reservation: %d", got)
	}
	if g := request(a, 32<<20, 0); g.Reservation != 32<<20 {
		t.Fatalf("donor physical return: %+v", g)
	}
	if g := request(b, 16<<20, 16<<20); g.Granted != 16<<20 || g.Reservation != 32<<20 {
		t.Fatalf("regrant after donor return: %+v", g)
	}
	if got := srv.State.reservedCache; got != 64<<20 {
		t.Fatalf("cached aggregate after transfer %d", got)
	}
}
