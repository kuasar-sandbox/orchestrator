package proxyshm

import (
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	connectorvswitch "github.com/kuasar-sandbox/connector/pkg/vswitch"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func newMMDSSourceTestViews(tb testing.TB, capacity int) (*Table, *Table, *MasterView, *WorkerView) {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "routes.shm")
	masterTable, err := Create(path, capacity)
	if err != nil {
		tb.Fatal(err)
	}
	workerTable, err := Open(path)
	if err != nil {
		masterTable.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		_ = workerTable.Close()
		_ = masterTable.Close()
	})
	master := NewMasterView(masterTable, time.Second, nil)
	worker := NewWorkerView(workerTable, nil, nil, time.Second)
	return masterTable, workerTable, master, worker
}

func testMMDSSourceBase() uint32 {
	base, ok := parseMMDSSourceIPv4("100.73.19.7")
	if !ok {
		panic("invalid test MMDS source base")
	}
	if base%uint32(connectorvswitch.MaxPorts) == 0 {
		base++
	}
	return base
}

func testMMDSSourceRoute(sid, state string, ipv4 uint32) routesync.RouteEntry {
	return routesync.RouteEntry{
		SandboxID:  sid,
		State:      state,
		FloatingIP: mmdsSourceIPv4String(ipv4),
		TemplateID: "template-" + sid,
		RunID:      "run-" + sid,
	}
}

func applyMMDSSourceRoute(t *testing.T, master *MasterView, route routesync.RouteEntry) {
	t.Helper()
	if err := master.ApplyUpsert(route); err != nil {
		t.Fatal(err)
	}
}

func assertMMDSSource(t *testing.T, worker *WorkerView, ip, wantSID string) {
	t.Helper()
	sid, found := worker.ByFloatingIP(ip)
	if wantSID == "" {
		if found || sid != "" {
			t.Fatalf("ByFloatingIP(%q) = %q found=%v, want miss", ip, sid, found)
		}
		return
	}
	if !found || sid != wantSID {
		t.Fatalf("ByFloatingIP(%q) = %q found=%v, want %q", ip, sid, found, wantSID)
	}
}

func TestMMDSSourceLayoutUsesConnectorCapacity(t *testing.T) {
	if connectorvswitch.MaxPorts == 0 {
		t.Fatal("connector MaxPorts is zero")
	}
	if mmdsSourceSlotCount != int(connectorvswitch.MaxPorts) {
		t.Fatalf("source slots = %d, connector MaxPorts = %d", mmdsSourceSlotCount, connectorvswitch.MaxPorts)
	}
	if got, want := mmdsSourceSlotSize, int(unsafe.Sizeof(mmapMMDSSourceSlot{})); got != want {
		t.Fatalf("source slot size = %d, want %d", got, want)
	}
	if got, want := mmdsSourceSlotSize, 2*int(unsafe.Sizeof(uint32(0)))+maxSandboxID; got != want {
		t.Fatalf("source slot has padding or unexpected fields: size=%d want=%d", got, want)
	}
	if got := unsafe.Offsetof(mmapHeader{}.MMDSSourceSeq) % unsafe.Alignof(uint64(0)); got != 0 {
		t.Fatalf("MMDSSourceSeq is not uint64-aligned: remainder=%d", got)
	}

	wantRegion := mmdsSourceSlotCount * mmdsSourceSlotSize
	for _, capacity := range []int{1, 7, 113} {
		gotRegion := Size(capacity) - headerSize - capacity*recordSize - terminalCapacity(capacity)*terminalRecordSize
		if gotRegion != wantRegion {
			t.Fatalf("capacity %d source region = %d, want fixed %d", capacity, gotRegion, wantRegion)
		}
	}

	path := filepath.Join(t.TempDir(), "routes.shm")
	master, err := Create(path, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if got := int(master.header.MMDSSourceCap); got != mmdsSourceSlotCount {
		t.Fatalf("created source capacity = %d, want %d", got, mmdsSourceSlotCount)
	}
	if got := len(master.mmdsSources); got != mmdsSourceSlotCount {
		t.Fatalf("created source slice = %d, want %d", got, mmdsSourceSlotCount)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := info.Size(), int64(Size(7)); got != want {
		t.Fatalf("mmap file size = %d, want %d", got, want)
	}
	worker, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	if !worker.readonly || len(worker.mmdsSources) != mmdsSourceSlotCount {
		t.Fatalf("opened worker readonly=%v source slots=%d", worker.readonly, len(worker.mmdsSources))
	}
}

func TestOpenRejectsMMDSSourceSchemaCapacityAndTruncation(t *testing.T) {
	tests := []struct {
		name string
		edit func(*mmapHeader)
		want string
	}{
		{"magic", func(h *mmapHeader) { h.Magic++ }, "invalid magic"},
		{"schema", func(h *mmapHeader) { h.Schema = schema - 1 }, "unsupported schema"},
		{"primary-capacity", func(h *mmapHeader) { h.Capacity = 0 }, "invalid primary capacity"},
		{"terminal-capacity", func(h *mmapHeader) { h.TerminalCap++ }, "invalid terminal capacity"},
		{"mmds-source-capacity", func(h *mmapHeader) { h.MMDSSourceCap-- }, "invalid MMDS source capacity"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.shm")
			table, err := Create(path, 4)
			if err != nil {
				t.Fatal(err)
			}
			defer table.Close()
			tc.edit(table.header)
			opened, err := Open(path)
			if opened != nil {
				opened.Close()
				t.Fatal("Open accepted incompatible header")
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Open error = %v, want %q", err, tc.want)
			}
		})
	}

	t.Run("truncated-source-region", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "routes.shm")
		table, err := Create(path, 4)
		if err != nil {
			t.Fatal(err)
		}
		if err := table.Close(); err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(path, int64(Size(4)-1)); err != nil {
			t.Fatal(err)
		}
		opened, err := Open(path)
		if opened != nil {
			opened.Close()
			t.Fatal("Open accepted truncated source region")
		}
		if err == nil || !strings.Contains(err.Error(), "smaller than expected") {
			t.Fatalf("Open error = %v, want truncated mmap rejection", err)
		}
	})
}

func TestMMDSSourceModuloDirectAddressingAndExactAliasRejection(t *testing.T) {
	base := testMMDSSourceBase()
	if base%uint32(connectorvswitch.MaxPorts) == 0 {
		t.Fatal("test base unexpectedly aligned to connector MaxPorts")
	}
	seen := make([]bool, mmdsSourceSlotCount)
	for offset := uint32(0); offset < uint32(connectorvswitch.MaxPorts); offset++ {
		idx := mmdsSourceSlotIndex(base + offset)
		if seen[idx] {
			t.Fatalf("duplicate modulo slot %d at offset %d", idx, offset)
		}
		seen[idx] = true
	}
	for idx, found := range seen {
		if !found {
			t.Fatalf("modulo slot %d was not covered", idx)
		}
	}
	alias := base + uint32(connectorvswitch.MaxPorts)
	if mmdsSourceSlotIndex(base) != mmdsSourceSlotIndex(alias) {
		t.Fatal("expected base and base+MaxPorts to share a physical slot")
	}

	_, _, master, worker := newMMDSSourceTestViews(t, 4)
	master.BeginSync()
	applyMMDSSourceRoute(t, master, testMMDSSourceRoute("owner", routesync.StateRunning, base))
	master.Bookmark()
	assertMMDSSource(t, worker, mmdsSourceIPv4String(base), "owner")
	assertMMDSSource(t, worker, mmdsSourceIPv4String(alias), "")
}

func TestMMDSSourceInputParsing(t *testing.T) {
	ipv4 := testMMDSSourceBase()
	_, _, master, worker := newMMDSSourceTestViews(t, 4)
	master.BeginSync()
	applyMMDSSourceRoute(t, master, testMMDSSourceRoute("sandbox", routesync.StateStarting, ipv4))
	master.Bookmark()

	addr := netip.MustParseAddr(mmdsSourceIPv4String(ipv4))
	assertMMDSSource(t, worker, addr.String(), "sandbox")
	assertMMDSSource(t, worker, "::ffff:"+addr.String(), "sandbox")
	for _, input := range []string{"", "not-an-ip", "2001:db8::1", "127.0.0.1:80"} {
		assertMMDSSource(t, worker, input, "")
	}
}

func TestMMDSSourceLifecycle(t *testing.T) {
	ipA := testMMDSSourceBase()
	ipB := ipA + 17
	_, _, master, worker := newMMDSSourceTestViews(t, 8)
	master.BeginSync()
	route := testMMDSSourceRoute("sandbox", routesync.StateStarting, ipA)
	applyMMDSSourceRoute(t, master, route)
	master.Bookmark()
	assertMMDSSource(t, worker, route.FloatingIP, route.SandboxID)

	route.State = routesync.StateRunning
	applyMMDSSourceRoute(t, master, route)
	assertMMDSSource(t, worker, route.FloatingIP, route.SandboxID)

	route.FloatingIP = mmdsSourceIPv4String(ipB)
	applyMMDSSourceRoute(t, master, route)
	assertMMDSSource(t, worker, mmdsSourceIPv4String(ipA), "")
	assertMMDSSource(t, worker, route.FloatingIP, route.SandboxID)

	for _, inactive := range []string{routesync.StatePaused, routesync.StateDead} {
		route.State = inactive
		applyMMDSSourceRoute(t, master, route)
		assertMMDSSource(t, worker, route.FloatingIP, "")
		route.State = routesync.StateRunning
		applyMMDSSourceRoute(t, master, route)
		assertMMDSSource(t, worker, route.FloatingIP, route.SandboxID)
	}

	master.ApplyDelete(route.SandboxID)
	assertMMDSSource(t, worker, route.FloatingIP, "")
}

func TestMMDSSourceInvalidRouteDoesNotBreakPrimaryRouting(t *testing.T) {
	masterTable, _, master, worker := newMMDSSourceTestViews(t, 4)
	master.BeginSync()
	route := routesync.RouteEntry{SandboxID: "invalid-ip", State: routesync.StateRunning, FloatingIP: "not-an-ip"}
	applyMMDSSourceRoute(t, master, route)
	master.Bookmark()
	if got, found := masterTable.Lookup(route.SandboxID); !found || got.FloatingIP != route.FloatingIP {
		t.Fatalf("primary route = %+v found=%v, want malformed floating IP retained", got, found)
	}
	assertMMDSSource(t, worker, route.FloatingIP, "")
}

func TestMMDSSourceOwnerReuseProtectsNewOwner(t *testing.T) {
	for _, delayed := range []string{"delete", routesync.StatePaused, routesync.StateDead} {
		t.Run(delayed, func(t *testing.T) {
			ipv4 := testMMDSSourceBase()
			_, _, master, worker := newMMDSSourceTestViews(t, 8)
			master.BeginSync()
			routeA := testMMDSSourceRoute("sandbox-a", routesync.StateRunning, ipv4)
			applyMMDSSourceRoute(t, master, routeA)
			master.Bookmark()
			routeB := testMMDSSourceRoute("sandbox-b", routesync.StateRunning, ipv4)
			applyMMDSSourceRoute(t, master, routeB)
			assertMMDSSource(t, worker, routeB.FloatingIP, routeB.SandboxID)

			if delayed == "delete" {
				master.ApplyDelete(routeA.SandboxID)
			} else {
				routeA.State = delayed
				applyMMDSSourceRoute(t, master, routeA)
			}
			assertMMDSSource(t, worker, routeB.FloatingIP, routeB.SandboxID)

			applyMMDSSourceRoute(t, master, routeB)
			assertMMDSSource(t, worker, routeB.FloatingIP, routeB.SandboxID)
			alias := ipv4 + uint32(connectorvswitch.MaxPorts)
			assertMMDSSource(t, worker, mmdsSourceIPv4String(alias), "")
		})
	}
}

func TestMMDSSourcePrimaryValidationFailsClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Table, routesync.RouteEntry, uint32) error
	}{
		{"source-sid-missing", func(table *Table, route routesync.RouteEntry, ipv4 uint32) error {
			startHeaderWrite(&table.header.MMDSSourceSeq)
			_, _ = table.publishMMDSSource(ipv4, "missing-sandbox")
			finishHeaderWrite(&table.header.MMDSSourceSeq)
			return nil
		}},
		{"primary-paused", func(table *Table, route routesync.RouteEntry, _ uint32) error {
			route.State = routesync.StatePaused
			return table.Upsert(route)
		}},
		{"primary-dead", func(table *Table, route routesync.RouteEntry, _ uint32) error {
			route.State = routesync.StateDead
			return table.Upsert(route)
		}},
		{"primary-ip-mismatch", func(table *Table, route routesync.RouteEntry, ipv4 uint32) error {
			route.FloatingIP = mmdsSourceIPv4String(ipv4 + 1)
			return table.Upsert(route)
		}},
		{"source-exact-ip-mismatch", func(table *Table, route routesync.RouteEntry, ipv4 uint32) error {
			alias := ipv4 + uint32(connectorvswitch.MaxPorts)
			startHeaderWrite(&table.header.MMDSSourceSeq)
			_, _ = table.publishMMDSSource(alias, route.SandboxID)
			finishHeaderWrite(&table.header.MMDSSourceSeq)
			return nil
		}},
		{"source-empty-sid", func(table *Table, _ routesync.RouteEntry, ipv4 uint32) error {
			idx := mmdsSourceSlotIndex(ipv4)
			startHeaderWrite(&table.header.MMDSSourceSeq)
			table.mmdsSources[idx] = mmapMMDSSourceSlot{IPv4: ipv4, Present: statusPresent}
			finishHeaderWrite(&table.header.MMDSSourceSeq)
			return nil
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ipv4 := testMMDSSourceBase()
			masterTable, _, master, worker := newMMDSSourceTestViews(t, 8)
			master.BeginSync()
			route := testMMDSSourceRoute("sandbox", routesync.StateRunning, ipv4)
			applyMMDSSourceRoute(t, master, route)
			master.Bookmark()
			if err := tc.mutate(masterTable, route, ipv4); err != nil {
				t.Fatal(err)
			}
			assertMMDSSource(t, worker, route.FloatingIP, "")
		})
	}
}

const (
	rollbackOldRoutes = `{"routes":[{"path":"/static","data":"old"},{"path":"/secret","type":"secret","secret":"key"},{"path":"/svc","type":"service","service":"old-service"}]}`
	rollbackNewRoutes = `{"routes":[{"path":"/static","data":"new"},{"path":"/secret","type":"secret","secret":"key"},{"path":"/svc","type":"service","service":"new-service"}]}`
)

func rollbackMMDSSourceRoute(sid, state string, ipv4 uint32, routes, secret string) routesync.RouteEntry {
	route := testMMDSSourceRoute(sid, state, ipv4)
	route.MMDSRoutes = routes
	route.MMDSRouteSecretValues = routeValues(map[string][]byte{"key": []byte(secret)})
	return route
}

func TestMMDSSourcePrimaryFailureRestoresSourceAndConfidentialHeap(t *testing.T) {
	errPrimary := errors.New("injected primary upsert failure")
	base := testMMDSSourceBase()
	tests := []struct {
		name     string
		initial  *routesync.RouteEntry
		incoming routesync.RouteEntry
	}{
		{
			name: "active-to-active",
			initial: func() *routesync.RouteEntry {
				r := rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackOldRoutes, "old-secret")
				return &r
			}(),
			incoming: rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackNewRoutes, "new-secret"),
		},
		{
			name: "active-to-inactive",
			initial: func() *routesync.RouteEntry {
				r := rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackOldRoutes, "old-secret")
				return &r
			}(),
			incoming: rollbackMMDSSourceRoute("sandbox", routesync.StatePaused, base, rollbackNewRoutes, "new-secret"),
		},
		{
			name: "ip-change",
			initial: func() *routesync.RouteEntry {
				r := rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackOldRoutes, "old-secret")
				return &r
			}(),
			incoming: rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base+19, rollbackNewRoutes, "new-secret"),
		},
		{
			name: "modulo-alias-ip-change",
			initial: func() *routesync.RouteEntry {
				r := rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackOldRoutes, "old-secret")
				return &r
			}(),
			incoming: rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base+uint32(connectorvswitch.MaxPorts), rollbackNewRoutes, "new-secret"),
		},
		{
			name: "inactive-to-active",
			initial: func() *routesync.RouteEntry {
				r := rollbackMMDSSourceRoute("sandbox", routesync.StatePaused, base, rollbackOldRoutes, "old-secret")
				return &r
			}(),
			incoming: rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackNewRoutes, "new-secret"),
		},
		{
			name:     "new-route",
			incoming: rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackNewRoutes, "new-secret"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			masterTable, _, master, worker := newMMDSSourceTestViews(t, 8)
			master.BeginSync()
			master.SetPolicy(routesync.Policy{MMDS: &routesync.MMDSProxyPolicy{Services: map[string]string{
				"old-service": "unix:///run/old-service.sock",
				"new-service": "unix:///run/new-service.sock",
			}}})
			if tc.initial != nil {
				applyMMDSSourceRoute(t, master, *tc.initial)
			}
			master.Bookmark()

			beforePrimary, beforeFound := masterTable.Lookup(tc.incoming.SandboxID)
			beforeHeap := master.mmds.snapshotEntry(tc.incoming.SandboxID)
			affected := make([]uint32, 0, 2)
			if tc.initial != nil {
				if ipv4, ok := parseMMDSSourceIPv4(tc.initial.FloatingIP); ok {
					affected = append(affected, ipv4)
				}
			}
			if ipv4, ok := parseMMDSSourceIPv4(tc.incoming.FloatingIP); ok {
				affected = append(affected, ipv4)
			}
			beforeSources := masterTable.snapshotMMDSSourceSlots(affected...)
			if tc.name == "modulo-alias-ip-change" && len(beforeSources) != 1 {
				t.Fatalf("aliased affected slots = %d, want one deduplicated snapshot", len(beforeSources))
			}

			master.upsertPrimary = func(routesync.RouteEntry) error { return errPrimary }
			if err := master.ApplyUpsert(tc.incoming); !errors.Is(err, errPrimary) {
				t.Fatalf("ApplyUpsert error = %v, want injected primary failure", err)
			}

			afterPrimary, afterFound := masterTable.Lookup(tc.incoming.SandboxID)
			if beforeFound != afterFound || beforePrimary != afterPrimary {
				t.Fatalf("primary changed on rollback: before=%+v/%v after=%+v/%v",
					beforePrimary, beforeFound, afterPrimary, afterFound)
			}
			afterSources := masterTable.snapshotMMDSSourceSlots(affected...)
			if !reflect.DeepEqual(afterSources, beforeSources) {
				t.Fatalf("source slots changed on rollback:\nbefore=%+v\nafter=%+v", beforeSources, afterSources)
			}
			afterHeap := master.mmds.snapshotEntry(tc.incoming.SandboxID)
			if !reflect.DeepEqual(afterHeap, beforeHeap) {
				t.Fatalf("MMDS heap changed on rollback:\nbefore=%+v\nafter=%+v", beforeHeap, afterHeap)
			}

			if tc.initial != nil && activeMMDSSourceRoute(*tc.initial) {
				assertMMDSSource(t, worker, tc.initial.FloatingIP, tc.initial.SandboxID)
				if got := master.ResolveMMDS(tc.initial.SandboxID, "/static"); !got.Found || string(got.Body) != "old" {
					t.Fatalf("old static route not restored: %+v", got)
				}
				if got := master.ResolveMMDS(tc.initial.SandboxID, "/secret"); !got.Found || !got.Present || string(got.Body) != "old-secret" {
					t.Fatalf("old secret route not restored: %+v", got)
				}
				if got := master.ResolveMMDS(tc.initial.SandboxID, "/svc"); !got.Found || got.Service != "old-service" || got.ServiceSocket != "/run/old-service.sock" {
					t.Fatalf("old service route not restored: %+v", got)
				}
			} else {
				assertMMDSSource(t, worker, tc.incoming.FloatingIP, "")
				if got := master.ResolveMMDS(tc.incoming.SandboxID, "/static"); got.Found || got.Unavailable {
					t.Fatalf("new confidential heap entry survived rollback: %+v", got)
				}
			}
		})
	}
}

func TestMMDSSourceHeapFailureRestoresOldEntryBeforePrimaryWrite(t *testing.T) {
	base := testMMDSSourceBase()
	masterTable, _, master, worker := newMMDSSourceTestViews(t, 4)
	master.BeginSync()
	old := rollbackMMDSSourceRoute("sandbox", routesync.StateRunning, base, rollbackOldRoutes, "old-secret")
	applyMMDSSourceRoute(t, master, old)
	master.Bookmark()
	beforePrimary, _ := masterTable.Lookup(old.SandboxID)
	beforeSource := masterTable.snapshotMMDSSourceSlots(base)
	beforeHeap := master.mmds.snapshotEntry(old.SandboxID)
	var primaryCalls atomic.Int32
	master.upsertPrimary = func(route routesync.RouteEntry) error {
		primaryCalls.Add(1)
		return masterTable.Upsert(route)
	}
	incoming := old
	incoming.MMDSRoutes = `{"routes":[`
	if err := master.ApplyUpsert(incoming); err == nil {
		t.Fatal("malformed MMDS heap update succeeded")
	}
	if primaryCalls.Load() != 0 {
		t.Fatalf("primary upsert called %d times after heap failure", primaryCalls.Load())
	}
	afterPrimary, _ := masterTable.Lookup(old.SandboxID)
	if afterPrimary != beforePrimary || !reflect.DeepEqual(masterTable.snapshotMMDSSourceSlots(base), beforeSource) ||
		!reflect.DeepEqual(master.mmds.snapshotEntry(old.SandboxID), beforeHeap) {
		t.Fatal("heap failure did not preserve primary/source/confidential state")
	}
	assertMMDSSource(t, worker, old.FloatingIP, old.SandboxID)
	if got := master.ResolveMMDS(old.SandboxID, "/secret"); !got.Found || string(got.Body) != "old-secret" {
		t.Fatalf("old secret was not restored after heap failure: %+v", got)
	}
}

func TestMMDSSourceTableFullRollsBackNewPublication(t *testing.T) {
	base := testMMDSSourceBase()
	masterTable, _, master, worker := newMMDSSourceTestViews(t, 1)
	master.BeginSync()
	first := testMMDSSourceRoute("first", routesync.StateRunning, base)
	applyMMDSSourceRoute(t, master, first)
	master.Bookmark()
	second := rollbackMMDSSourceRoute("second", routesync.StateRunning, base+1, rollbackNewRoutes, "second-secret")
	if err := master.ApplyUpsert(second); err == nil || !strings.Contains(err.Error(), "route table full") {
		t.Fatalf("second ApplyUpsert error = %v, want table full", err)
	}
	if _, found := masterTable.Lookup(second.SandboxID); found {
		t.Fatal("table-full route remained in primary table")
	}
	assertMMDSSource(t, worker, first.FloatingIP, first.SandboxID)
	assertMMDSSource(t, worker, second.FloatingIP, "")
	if got := master.ResolveMMDS(second.SandboxID, "/static"); got.Found || got.Unavailable {
		t.Fatalf("table-full route remained in MMDS heap: %+v", got)
	}
}

func TestMMDSSourceFullSyncRebuildAndReadOnlyReopen(t *testing.T) {
	base := testMMDSSourceBase()
	masterTable, workerTable, master, worker := newMMDSSourceTestViews(t, 8)
	master.BeginSync()
	keep := testMMDSSourceRoute("keep", routesync.StateRunning, base)
	stale := testMMDSSourceRoute("stale", routesync.StateRunning, base+1)
	applyMMDSSourceRoute(t, master, keep)
	applyMMDSSourceRoute(t, master, stale)
	master.Bookmark()
	assertMMDSSource(t, worker, keep.FloatingIP, keep.SandboxID)

	keepIPv4, _ := parseMMDSSourceIPv4(keep.FloatingIP)
	startHeaderWrite(&masterTable.header.MMDSSourceSeq)
	_, _ = masterTable.publishMMDSSource(keepIPv4, "corrupt-owner")
	finishHeaderWrite(&masterTable.header.MMDSSourceSeq)
	assertMMDSSource(t, worker, keep.FloatingIP, "")

	master.BeginSync()
	if worker.MMDSAvailable() {
		t.Fatal("BeginSync left MMDS available")
	}
	if slot := masterTable.mmdsSources[mmdsSourceSlotIndex(keepIPv4)]; slot.Present != 0 {
		t.Fatalf("BeginSync retained source slot %+v", slot)
	}
	applyMMDSSourceRoute(t, master, keep)
	assertMMDSSource(t, worker, keep.FloatingIP, "")
	// Corrupt the incremental source publication. Bookmark must ignore this
	// derivative and reconstruct it solely from the primary table.
	startHeaderWrite(&masterTable.header.MMDSSourceSeq)
	masterTable.clearMMDSSources()
	finishHeaderWrite(&masterTable.header.MMDSSourceSeq)
	master.Bookmark()
	assertMMDSSource(t, worker, keep.FloatingIP, keep.SandboxID)
	assertMMDSSource(t, worker, stale.FloatingIP, "")
	if _, found := masterTable.Lookup(stale.SandboxID); found {
		t.Fatal("stale primary route survived Bookmark")
	}

	reopened, err := Open(masterTable.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedWorker := NewWorkerView(reopened, nil, nil, time.Second)
	assertMMDSSource(t, reopenedWorker, keep.FloatingIP, keep.SandboxID)
	if !workerTable.readonly || !reopened.readonly {
		t.Fatal("worker source view was not read-only")
	}
}

func TestMMDSSourceConcurrentReuseNeverReturnsAliasedOwner(t *testing.T) {
	base := testMMDSSourceBase()
	alias := base + uint32(connectorvswitch.MaxPorts)
	_, _, master, worker := newMMDSSourceTestViews(t, 8)
	master.BeginSync()
	routeA := testMMDSSourceRoute("sandbox-a", routesync.StateRunning, base)
	routeB := testMMDSSourceRoute("sandbox-b", routesync.StateRunning, alias)
	applyMMDSSourceRoute(t, master, routeA)
	applyMMDSSourceRoute(t, master, routeB)
	master.Bookmark()

	var done atomic.Bool
	errCh := make(chan error, 1)
	var readers sync.WaitGroup
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for !done.Load() {
				if sid, found := worker.ByFloatingIP(routeA.FloatingIP); found && sid != routeA.SandboxID {
					select {
					case errCh <- fmt.Errorf("source A resolved to %q", sid):
					default:
					}
					return
				}
				if sid, found := worker.ByFloatingIP(routeB.FloatingIP); found && sid != routeB.SandboxID {
					select {
					case errCh <- fmt.Errorf("source B resolved to %q", sid):
					default:
					}
					return
				}
			}
		}()
	}

	for i := 0; i < 500; i++ {
		applyMMDSSourceRoute(t, master, routeA)
		applyMMDSSourceRoute(t, master, routeB)
		routeA.State = routesync.StatePaused
		applyMMDSSourceRoute(t, master, routeA)
		routeA.State = routesync.StateRunning
		applyMMDSSourceRoute(t, master, routeA)
		master.ApplyDelete(routeB.SandboxID)
		applyMMDSSourceRoute(t, master, routeB)
	}
	done.Store(true)
	readers.Wait()
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
}

func benchmarkMMDSSourceReady(b *testing.B, capacity, active int, readonly bool) (*Table, *WorkerView, string) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "routes.shm")
	table, err := Create(path, capacity)
	if err != nil {
		b.Fatal(err)
	}
	master := NewMasterView(table, time.Second, nil)
	master.BeginSync()
	base := testMMDSSourceBase()
	for i := 0; i < active; i++ {
		route := testMMDSSourceRoute(fmt.Sprintf("sandbox-%d", i), routesync.StateRunning, base+uint32(i))
		if err := master.ApplyUpsert(route); err != nil {
			table.Close()
			b.Fatal(err)
		}
	}
	master.Bookmark()
	lookupTable := table
	if readonly {
		lookupTable, err = Open(path)
		if err != nil {
			table.Close()
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = lookupTable.Close() })
	}
	b.Cleanup(func() { _ = table.Close() })
	lookup := mmdsSourceIPv4String(base)
	if active == 0 {
		lookup = mmdsSourceIPv4String(base + 1)
	}
	return table, NewWorkerView(lookupTable, nil, nil, time.Second), lookup
}

func BenchmarkMMDSSourceEmptyMiss(b *testing.B) {
	_, worker, ip := benchmarkMMDSSourceReady(b, defaultCapacity, 0, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.ByFloatingIP(ip)
	}
}

func BenchmarkMMDSSourceHit(b *testing.B) {
	_, worker, ip := benchmarkMMDSSourceReady(b, defaultCapacity, 1, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.ByFloatingIP(ip)
	}
}

func BenchmarkMMDSSourceManyActiveHit(b *testing.B) {
	_, worker, ip := benchmarkMMDSSourceReady(b, defaultCapacity, mmdsSourceSlotCount, false)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.ByFloatingIP(ip)
	}
}

func BenchmarkMMDSSourceReadOnlyWorkerHit(b *testing.B) {
	_, worker, ip := benchmarkMMDSSourceReady(b, defaultCapacity, 1, true)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.ByFloatingIP(ip)
	}
}

func BenchmarkMMDSSourceLifecyclePublishRemove(b *testing.B) {
	table, _, _, _ := newMMDSSourceTestViews(b, 4)
	route := testMMDSSourceRoute("sandbox", routesync.StateRunning, testMMDSSourceBase())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.updateMMDSSources(routesync.RouteEntry{}, false, route)
		table.removeRouteMMDSSource(route, true)
	}
}

func BenchmarkMMDSSourceRebuild(b *testing.B) {
	path := filepath.Join(b.TempDir(), "routes.shm")
	table, err := Create(path, mmdsSourceSlotCount*2)
	if err != nil {
		b.Fatal(err)
	}
	defer table.Close()
	base := testMMDSSourceBase()
	for i := 0; i < mmdsSourceSlotCount; i++ {
		if err := table.Upsert(testMMDSSourceRoute(fmt.Sprintf("sandbox-%d", i), routesync.StateRunning, base+uint32(i))); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		table.rebuildMMDSSources(nil)
	}
	b.ReportMetric(float64(mmdsSourceSlotCount*mmdsSourceSlotSize), "source_region_bytes")
}
