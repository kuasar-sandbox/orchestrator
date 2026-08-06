package proxyshm

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestMMDSRoutesUpsertAndGet(t *testing.T) {
	m := NewMMDSRoutes()
	m.Upsert("sbx-1", `{"version":1}`)
	got, ok := m.Get("sbx-1")
	if !ok || got != `{"version":1}` {
		t.Fatalf("Get() = %q, %t", got, ok)
	}
	if _, ok := m.Get("sbx-2"); ok {
		t.Fatal("expected sbx-2 to be absent")
	}
}

func TestMMDSRoutesUpsertEmptyClears(t *testing.T) {
	m := NewMMDSRoutes()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Upsert("sbx-1", "") // a route entry with no MMDS specification
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected an empty upsert to clear the entry")
	}
}

func TestMMDSRoutesDelete(t *testing.T) {
	m := NewMMDSRoutes()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Delete("sbx-1")
	if _, ok := m.Get("sbx-1"); ok {
		t.Fatal("expected the entry to be deleted")
	}
}

// TestMasterViewApplyUpsertGatesMMDSRoutesOnRunningState proves external-mode
// specified MMDS routes are only servable while running, matching
// Table.SandboxInfo's existing gate on the built-in root instance-info path
// (internal mode's equivalent is Orchestrator.MMDSRoute). publishUpsert
// carries the sandbox's MMDSRoutes JSON on every upsert regardless of state
// (orch/routes.go), so without this ApplyUpsert would keep it queryable via
// WorkerView.MMDSRoute -> mmdsrpc while paused, even though a token minted
// before pause remains verifiable (Table.MmdsSecret is not state-gated).
func TestMasterViewApplyUpsertGatesMMDSRoutesOnRunningState(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 0, nil)
	master.BeginSync()

	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "sbx-1", Profile: "e2b", State: routesync.StateRunning,
		MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`,
	})
	if _, ok := master.MMDSRoutes().Get("sbx-1"); !ok {
		t.Fatal("expected the running sandbox's MMDS routes to be queryable")
	}

	master.ApplyUpsert(routesync.RouteEntry{
		SandboxID: "sbx-1", Profile: "e2b", State: routesync.StatePaused,
		MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`,
	})
	if _, ok := master.MMDSRoutes().Get("sbx-1"); ok {
		t.Fatal("expected the paused sandbox's MMDS routes to fail closed (not found)")
	}
}

// TestMasterViewMMDSRouteRejectsStaleHeapEntryDuringApplyUpsertWindow proves
// MMDSRoute (the mmdsrpc-server-side resolver) is safe even if it races into
// the narrow window between ApplyUpsert's two separate writes (v.table then
// v.mmds): it re-checks v.table itself rather than trusting the heap store
// alone, so a request landing after the table is marked paused but before the
// heap entry is pruned still fails closed instead of serving stale static
// route data to a token minted before pause.
func TestMasterViewMMDSRouteRejectsStaleHeapEntryDuringApplyUpsertWindow(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 0, nil)
	master.BeginSync()

	running := routesync.RouteEntry{
		SandboxID: "sbx-1", Profile: "e2b", State: routesync.StateRunning,
		MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`,
	}
	master.ApplyUpsert(running)
	if _, ok := master.MMDSRoute("sbx-1", "/x"); !ok {
		t.Fatal("expected the running sandbox's route to resolve")
	}

	// Simulate landing exactly between ApplyUpsert's two writes: the table
	// already reflects pause, but the heap store has not been pruned yet.
	paused := running
	paused.State = routesync.StatePaused
	if err := tbl.Upsert(paused); err != nil {
		t.Fatal(err)
	}
	if _, ok := master.MMDSRoutes().Get("sbx-1"); !ok {
		t.Fatal("test setup: expected the heap store to still be stale")
	}

	if _, ok := master.MMDSRoute("sbx-1", "/x"); ok {
		t.Fatal("expected MMDSRoute to fail closed on the mid-transition table state, not trust the stale heap entry")
	}
}

// TestMasterViewApplyUpsertPopulatesMMDSBeforeTableBecomesRunning proves the
// write order ApplyUpsert now uses: populating the MMDS heap store happens
// before the table row is published as running. Checking this only after a
// single ApplyUpsert call returns can't distinguish the two write orders --
// both leave the same final state -- so this races a reader goroutine
// against a writer goroutine repeatedly calling ApplyUpsert(running), each
// time for a fresh, never-before-touched sid (a first-ever create/resume,
// matching the review's "just-created/resumed sandbox" scenario exactly --
// reusing/pausing a sid between calls would leave the *previous* call's
// table row stale-but-still-Running while its heap entry is deleted first,
// which is a real but different, already-covered window, not this one).
// The reader checks directly against the table and heap store, bypassing
// MMDSRoute's own defensive re-check, which exists precisely to paper over
// this window and would otherwise mask a regression here. Any observation of
// table.State == Running with no heap entry for a given sid reproduces the
// bug the P2 review comment described: a worker satisfying a token PUT off
// the already-published running row, then immediately GETting a declared
// static route and finding it not there yet.
func TestMasterViewApplyUpsertPopulatesMMDSBeforeTableBecomesRunning(t *testing.T) {
	const iterations = 20000
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), iterations+16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 0, nil)
	master.BeginSync()

	var nextIndex atomic.Int64
	nextIndex.Store(-1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < iterations; i++ {
			nextIndex.Store(int64(i)) // published before the call: the reader may poll it mid-flight
			master.ApplyUpsert(routesync.RouteEntry{
				SandboxID: fmt.Sprintf("sbx-%d", i), Profile: "e2b", State: routesync.StateRunning,
				MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`,
			})
		}
	}()

	var badObservations int
	for {
		select {
		case <-done:
			if badObservations != 0 {
				t.Fatalf("observed table.State==Running with no MMDS heap entry %d times", badObservations)
			}
			return
		default:
		}
		if i := nextIndex.Load(); i >= 0 {
			sid := fmt.Sprintf("sbx-%d", i)
			if rec, ok := tbl.Lookup(sid); ok && rec.State == routesync.StateRunning {
				if _, ok := master.MMDSRoutes().Get(sid); !ok {
					badObservations++
				}
			}
		}
	}
}

// TestMasterViewApplyUpsertDoesNotClobberExistingMMDSOnRejectedUpdate proves
// a rejected update (malformed field data Table.Upsert itself would refuse)
// never touches the MMDS heap store for that sid -- in particular, it must
// not delete an existing, still-valid heap entry for a sandbox that is
// already running just because a later malformed update to it was rejected.
func TestMasterViewApplyUpsertDoesNotClobberExistingMMDSOnRejectedUpdate(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 0, nil)
	master.BeginSync()

	running := routesync.RouteEntry{
		SandboxID: "sbx-1", Profile: "e2b", State: routesync.StateRunning,
		MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`,
	}
	master.ApplyUpsert(running)
	if _, ok := master.MMDSRoute("sbx-1", "/x"); !ok {
		t.Fatal("expected the running sandbox's route to resolve")
	}

	// A malformed update to the same sid: Profile exceeds Table.Upsert's own
	// field-length limit, so it must be rejected before mutating anything.
	malformed := running
	malformed.Profile = strings.Repeat("p", 17) // maxProfile is 16
	malformed.MMDSRoutes = `{"version":1,"routes":[{"path":"/y","type":"static","data":"e"}]}`
	master.ApplyUpsert(malformed)

	if rec, ok := tbl.Lookup("sbx-1"); !ok || rec.Profile != "e2b" {
		t.Fatalf("table row = %+v ok=%v, want the original row untouched by the rejected update", rec, ok)
	}
	if route, ok := master.MMDSRoute("sbx-1", "/x"); !ok || route.Data != "d" {
		t.Fatalf("MMDSRoute(sbx-1, /x) = %+v ok=%v, want the original route still intact", route, ok)
	}
	if _, ok := master.MMDSRoute("sbx-1", "/y"); ok {
		t.Fatal("the rejected update's route must not have been applied")
	}
}

// TestMasterViewApplyUpsertRollsBackMMDSOnTableCapacityFailure proves the one
// remaining Table.Upsert failure mode that can't be pre-checked -- the table
// being full -- rolls the MMDS heap write back instead of leaving an orphaned
// entry for a sid the table never actually published. Capacity failure can
// only affect a sid with no existing slot (findSlot always reuses an
// already-occupied sid's own slot), so this never risks the clobber the
// rejected-update test above guards against.
func TestMasterViewApplyUpsertRollsBackMMDSOnTableCapacityFailure(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 1) // capacity for exactly one sid
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 0, nil)
	master.BeginSync()

	first := routesync.RouteEntry{SandboxID: "sbx-1", Profile: "e2b", State: routesync.StateRunning}
	master.ApplyUpsert(first)
	if _, ok := tbl.Lookup("sbx-1"); !ok {
		t.Fatal("test setup: expected the first sid to occupy the table's only slot")
	}

	second := routesync.RouteEntry{
		SandboxID: "sbx-2", Profile: "e2b", State: routesync.StateRunning,
		MMDSRoutes: `{"version":1,"routes":[{"path":"/x","type":"static","data":"d"}]}`,
	}
	master.ApplyUpsert(second)

	if _, ok := tbl.Lookup("sbx-2"); ok {
		t.Fatal("test setup: expected the second sid to be rejected (table full)")
	}
	if _, ok := master.MMDSRoutes().Get("sbx-2"); ok {
		t.Fatal("MMDS heap entry for a sid the table rejected (capacity) was not rolled back")
	}
}

func TestMMDSRoutesBookmarkDropsUnaffirmedEntries(t *testing.T) {
	m := NewMMDSRoutes()
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Upsert("sbx-2", `{"version":1}`)
	m.Bookmark()

	// Reconnect: a fresh sync generation only re-affirms sbx-1 (sbx-2 was
	// deleted while disconnected).
	m.BeginSync()
	m.Upsert("sbx-1", `{"version":1}`)
	m.Bookmark()

	if _, ok := m.Get("sbx-1"); !ok {
		t.Fatal("expected sbx-1 (re-affirmed) to remain")
	}
	if _, ok := m.Get("sbx-2"); ok {
		t.Fatal("expected sbx-2 (not re-affirmed) to be dropped at Bookmark")
	}
}
