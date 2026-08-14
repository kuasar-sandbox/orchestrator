package proxyshm

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func TestTableSharedLookupAndDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	master, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	worker, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()

	entry := routesync.RouteEntry{
		SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning,
		TemplateID: "tmpl", EnvdUDS: "/run/s1/envd.sock", CiUDS: "/run/s1/ci.sock",
		FloatingIP: "100.100.0.2", AuthSandboxID: "stable-s1",
		APISecret: strings.Repeat("1", 64), APISecretFingerprint: strings.Repeat("2", 64),
		ManifestKeyFingerprint: strings.Repeat("3", 64), ServiceSecret: strings.Repeat("4", 64),
		EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		SnapshotLocation: "remote", MmdsSecret: hex.EncodeToString([]byte("mmds")),
	}
	master.BeginSync()
	if err := master.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	master.Bookmark()

	got, ok := worker.Lookup("s1")
	if !ok || got != entry {
		t.Fatalf("lookup = %+v ok=%v", got, ok)
	}
	_, _, liveRev := worker.LookupRevision("s1")
	idx, ok := master.findSlot("s1", false)
	if !ok {
		t.Fatal("route slot not found before delete")
	}
	if !master.Delete("s1") {
		t.Fatal("delete returned false")
	}
	if _, ok := worker.Lookup("s1"); ok {
		t.Fatal("worker still sees deleted route")
	}
	if deleted, found, rev := worker.LookupRevision("s1"); found || deleted != (routesync.RouteEntry{}) || rev <= liveRev {
		t.Fatalf("deleted lookup = %+v found=%v rev=%d, live rev=%d", deleted, found, rev, liveRev)
	}
	deleted, status, ok := readRecord(&master.records[idx])
	if !ok || status != statusEmpty {
		t.Fatalf("deleted record status=%d ok=%v", status, ok)
	}
	if deleted.SandboxID != "" || deleted.AuthSandboxID != "" || deleted.APISecret != "" || deleted.APISecretFingerprint != "" ||
		deleted.ManifestKeyFingerprint != "" || deleted.ServiceSecret != "" ||
		deleted.EnvdAccessToken != "" || deleted.TrafficAccessToken != "" ||
		deleted.ForwardAccessToken != "" || deleted.MmdsSecret != "" {
		t.Fatalf("deleted record retained credential material: %+v", deleted)
	}
}

func TestTableLookupRevisionTracksLiveAndDeletedSnapshots(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	route := routesync.RouteEntry{SandboxID: "revision-snapshot", State: routesync.StatePaused}
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	paused, found, pausedRev := tbl.LookupRevision(route.SandboxID)
	if !found || paused.State != routesync.StatePaused || pausedRev == 0 {
		t.Fatalf("paused snapshot = %+v, found=%v rev=%d", paused, found, pausedRev)
	}

	route.State = routesync.StateStarting
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	starting, found, startingRev := tbl.LookupRevision(route.SandboxID)
	if !found || starting.State != routesync.StateStarting || startingRev <= pausedRev {
		t.Fatalf("starting snapshot = %+v, found=%v rev=%d; paused rev=%d", starting, found, startingRev, pausedRev)
	}
	if got := tbl.RouteRev(route.SandboxID); got != startingRev {
		t.Fatalf("RouteRev = %d, want atomic lookup rev %d", got, startingRev)
	}

	if !tbl.Delete(route.SandboxID) {
		t.Fatal("delete returned false")
	}
	deleted, found, deletedRev := tbl.LookupRevision(route.SandboxID)
	if found || deleted != (routesync.RouteEntry{}) || deletedRev <= startingRev {
		t.Fatalf("deleted snapshot = %+v, found=%v rev=%d; starting rev=%d", deleted, found, deletedRev, startingRev)
	}

	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	running, found, runningRev := tbl.LookupRevision(route.SandboxID)
	if !found || running.State != routesync.StateRunning || runningRev <= deletedRev {
		t.Fatalf("running snapshot = %+v, found=%v rev=%d; deleted rev=%d", running, found, runningRev, deletedRev)
	}
}

func TestTableByFloatingIPUsesActiveRoutesOnly(t *testing.T) {
	tests := []struct {
		name    string
		state   string
		routeIP string
		queryIP string
		wantSID string
		wantOK  bool
	}{
		{name: "starting", state: routesync.StateStarting, routeIP: "100.100.0.2", queryIP: "100.100.0.2", wantSID: "starting", wantOK: true},
		{name: "running", state: routesync.StateRunning, routeIP: "100.100.0.3", queryIP: "100.100.0.3", wantSID: "running", wantOK: true},
		{name: "paused", state: routesync.StatePaused, routeIP: "100.100.0.4", queryIP: "100.100.0.4", wantOK: false},
		{name: "missing", state: routesync.StateRunning, routeIP: "100.100.0.5", queryIP: "100.100.0.99", wantOK: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			if err := tbl.Upsert(routesync.RouteEntry{
				SandboxID: tc.name, State: tc.state, FloatingIP: tc.routeIP,
			}); err != nil {
				t.Fatal(err)
			}

			gotSID, gotOK := tbl.ByFloatingIP(tc.queryIP)
			if gotSID != tc.wantSID || gotOK != tc.wantOK {
				t.Fatalf("ByFloatingIP(%q) = %q, %v; want %q, %v", tc.queryIP, gotSID, gotOK, tc.wantSID, tc.wantOK)
			}
			if gotSID, gotOK := tbl.ByFloatingIP(""); gotSID != "" || gotOK {
				t.Fatalf("ByFloatingIP(empty) = %q, %v; want empty, false", gotSID, gotOK)
			}
		})
	}
}

func TestTableFloatingIPIndexLifecycle(t *testing.T) {
	const capacity = 8
	byBucket := make(map[uint64]string)
	var ip1, ip2 string
	for i := 2; i < 255 && ip2 == ""; i++ {
		ip := fmt.Sprintf("100.100.0.%d", i)
		bucket := hashSID(ip) % (capacity * 2)
		if prior, ok := byBucket[bucket]; ok {
			ip1, ip2 = prior, ip
			break
		}
		byBucket[bucket] = ip
	}
	if ip1 == "" || ip2 == "" {
		t.Fatal("failed to find floating-IP hash collision")
	}

	tests := []struct {
		name       string
		updated    routesync.RouteEntry
		queryIP    string
		wantOld    bool
		wantNewSID string
		deleteSID  string
		wantRemain string
	}{
		{
			name:       "state transition removes mapping",
			updated:    routesync.RouteEntry{SandboxID: "s1", State: routesync.StatePaused, FloatingIP: ip1},
			queryIP:    ip2,
			wantOld:    false,
			wantNewSID: "s2",
			wantRemain: "s2",
		},
		{
			name:       "IP change replaces mapping",
			updated:    routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning, FloatingIP: "100.100.1.1"},
			queryIP:    "100.100.1.1",
			wantOld:    false,
			wantNewSID: "s1",
			wantRemain: "s2",
		},
		{
			name:       "delete leaves collision chain",
			updated:    routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning, FloatingIP: ip1},
			queryIP:    ip1,
			wantOld:    false,
			deleteSID:  "s1",
			wantRemain: "s2",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.shm")
			tbl, err := Create(path, capacity)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			for _, route := range []routesync.RouteEntry{
				{SandboxID: "s1", State: routesync.StateRunning, FloatingIP: ip1},
				{SandboxID: "s2", State: routesync.StateRunning, FloatingIP: ip2},
			} {
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
			}
			worker, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer worker.Close()

			if err := tbl.Upsert(tc.updated); err != nil {
				t.Fatal(err)
			}
			if tc.deleteSID != "" && !tbl.Delete(tc.deleteSID) {
				t.Fatalf("Delete(%q) returned false", tc.deleteSID)
			}
			gotOld, oldOK := worker.ByFloatingIP(ip1)
			if oldOK != tc.wantOld || (oldOK && gotOld != "s1") {
				t.Fatalf("old IP = %q, %v; want s1, %v", gotOld, oldOK, tc.wantOld)
			}
			gotNew, newOK := worker.ByFloatingIP(tc.queryIP)
			if (tc.wantNewSID == "") != !newOK || (newOK && gotNew != tc.wantNewSID) {
				t.Fatalf("updated IP = %q, %v; want %q", gotNew, newOK, tc.wantNewSID)
			}
			gotRemain, remainOK := worker.ByFloatingIP(ip2)
			if !remainOK || gotRemain != tc.wantRemain {
				t.Fatalf("remaining collision IP = %q, %v; want %q, true", gotRemain, remainOK, tc.wantRemain)
			}
		})
	}
}

// TestTableUpsertDuplicateFloatingIP documents the overwrite contract for two
// active routes sharing a floating IP. connector allocates unique IPs per
// switch, so this only happens on a control-plane bug or a crash-cleanup race;
// see the floatingIndexUpsert comment for why overwrite is preferred over
// rejecting the second route.
func TestTableUpsertDuplicateFloatingIP(t *testing.T) {
	const ip = "100.100.0.9"
	tests := []struct {
		name        string
		retireFirst bool
		sameSID     bool
		wantSID     string
	}{
		{name: "overwrite while original active", wantSID: "s2"},
		{name: "original paused first", retireFirst: true, wantSID: "s2"},
		{name: "same sandbox replay", sameSID: true, wantSID: "s1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			route := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning, FloatingIP: ip}
			if err := tbl.Upsert(route); err != nil {
				t.Fatal(err)
			}

			if tc.sameSID {
				route.State = routesync.StateStarting
				if err := tbl.Upsert(route); err != nil {
					t.Fatal(err)
				}
			} else {
				if tc.retireFirst {
					route.State = routesync.StatePaused
					if err := tbl.Upsert(route); err != nil {
						t.Fatal(err)
					}
				}
				if err := tbl.Upsert(routesync.RouteEntry{
					SandboxID: "s2", State: routesync.StateRunning, FloatingIP: ip,
				}); err != nil {
					t.Fatal(err)
				}
			}
			if got, ok := tbl.ByFloatingIP(ip); !ok || got != tc.wantSID {
				t.Fatalf("ByFloatingIP(%q) = %q, %v; want %q", ip, got, ok, tc.wantSID)
			}

			if !tc.sameSID {
				// Deleting the overwriter leaves the overwritten original
				// unindexed: the documented consequence of the duplicate.
				if !tbl.Delete("s2") {
					t.Fatal("Delete(s2) returned false")
				}
				if got, ok := tbl.ByFloatingIP(ip); ok || got != "" {
					t.Fatalf("post-delete ByFloatingIP(%q) = %q, %v; want miss", ip, got, ok)
				}
			}
		})
	}
}

// TestTableUpsertRollsBackWhenFloatingIndexFull forces the defended index-full
// failure that the 2x-capacity sizing makes unreachable through the public
// API: the in-process index is replaced by a single slot holding one route,
// and the second sandbox is chosen to rebuild after the first, so the retry
// after rebuild is guaranteed to find the index full.
func TestTableUpsertRollsBackWhenFloatingIndexFull(t *testing.T) {
	// Pick two SIDs whose primary home slots order first before second: the
	// rebuild scans records in slot order, so first wins the only index slot
	// and second deterministically fails both the initial probe and the retry.
	first, second := "first", "second"
	for i := 0; hashSID(first)%8 > hashSID(second)%8; i++ {
		first, second = fmt.Sprintf("first-%d", i), fmt.Sprintf("second-%d", i)
	}
	ip1, ip2 := "100.100.0.2", "100.100.0.3"

	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: first, State: routesync.StateRunning, FloatingIP: ip1,
	}); err != nil {
		t.Fatal(err)
	}

	full := tbl.floating
	writeFloatingRecord(&full[0], ip1, first, hashSID(ip1), floatingPresent)
	tbl.floating = full[:1]
	err = tbl.Upsert(routesync.RouteEntry{
		SandboxID: second, State: routesync.StateRunning, FloatingIP: ip2,
	})
	if err == nil || !strings.Contains(err.Error(), "floating IP index full") {
		t.Fatalf("Upsert error = %v; want floating IP index full", err)
	}
	if _, found := tbl.Lookup(second); found {
		t.Fatalf("%s primary record must be rolled back", second)
	}
	if got, ok := tbl.ByFloatingIP(ip1); !ok || got != first {
		t.Fatalf("ByFloatingIP(%q) = %q, %v; want %q", ip1, got, ok, first)
	}

	// With the real index capacity restored the same upsert succeeds.
	tbl.floating = full
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: second, State: routesync.StateRunning, FloatingIP: ip2,
	}); err != nil {
		t.Fatal(err)
	}
	if got, ok := tbl.ByFloatingIP(ip2); !ok || got != second {
		t.Fatalf("restored ByFloatingIP = %q, %v; want %q", got, ok, second)
	}
}

// TestBookmarkRebuildsFloatingIndexAfterDuplicateStrand covers the
// self-heal path: a duplicate floating IP whose overwriter is deleted strands
// the original route, an identical replay does not re-index it, and the
// full-sync Bookmark rebuilds the index from the primary table.
func TestBookmarkRebuildsFloatingIndexAfterDuplicateStrand(t *testing.T) {
	const ip = "100.100.0.9"
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	first := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning, FloatingIP: ip}
	for _, route := range []routesync.RouteEntry{
		first,
		{SandboxID: "s2", State: routesync.StateRunning, FloatingIP: ip},
	} {
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
	}
	if !tbl.Delete("s2") {
		t.Fatal("Delete(s2) returned false")
	}
	if _, found := tbl.Lookup("s1"); !found {
		t.Fatal("s1 must remain active in the primary table")
	}
	if _, ok := tbl.ByFloatingIP(ip); ok {
		t.Fatal("expected stranded miss after duplicate overwrite and delete")
	}

	// An identical replay adopts the route into the new generation but must
	// not heal the index; only the Bookmark rebuild does.
	tbl.BeginSync()
	if err := tbl.Upsert(first); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.ByFloatingIP(ip); ok {
		t.Fatal("identical replay must not re-index the stranded route")
	}

	tbl.Bookmark()
	if got, ok := tbl.ByFloatingIP(ip); !ok || got != "s1" {
		t.Fatalf("post-Bookmark ByFloatingIP(%q) = %q, %v; want s1", ip, got, ok)
	}
}

// TestMasterViewApplyUpsertRestoresMMDSOnRollback forces the defended
// index-full failure through MasterView.ApplyUpsert and asserts the MMDS heap
// view of the rolled-back route survives: the variable MMDS routes live only
// in the heap, not in shared memory.
func TestMasterViewApplyUpsertRestoresMMDSOnRollback(t *testing.T) {
	// Order the helper's primary home slot before the target's so the
	// post-rebuild index retry deterministically finds the index full.
	helper, target := "helper", "target"
	for i := 0; hashSID(helper)%8 > hashSID(target)%8; i++ {
		helper, target = fmt.Sprintf("helper-%d", i), fmt.Sprintf("target-%d", i)
	}
	ip1, ip2 := "100.100.0.2", "100.100.0.3"

	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 8)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	mv := NewMasterView(tbl, time.Second, nil)

	route := routesync.RouteEntry{
		SandboxID: target, State: routesync.StateRunning, FloatingIP: ip1, RunID: "run-1",
		MMDSRoutes: testMMDSRoutes, MMDSRouteSecretValues: routeValues(map[string][]byte{"key": {0, 1, 2}}),
	}
	mv.ApplyUpsert(route)
	mv.ApplyUpsert(routesync.RouteEntry{SandboxID: helper, State: routesync.StateRunning, FloatingIP: ip2})
	mv.Bookmark()

	full := tbl.floating
	writeFloatingRecord(&full[0], ip2, helper, hashSID(ip2), floatingPresent)
	tbl.floating = full[:1]

	updated := route
	updated.RunID = "run-2"
	mv.ApplyUpsert(updated)

	if got, found := tbl.Lookup(target); !found || got.RunID != "run-1" || got.FloatingIP != ip1 {
		t.Fatalf("rolled-back route = %+v found=%v; want run-1 on %s", got, found, ip1)
	}
	secret := mv.mmds.Resolve(target, "/secret")
	if !secret.Found || !secret.Present || !bytes.Equal(secret.Body, []byte{0, 1, 2}) {
		t.Fatalf("heap view after rollback = %+v; want the pre-update secret route", secret)
	}
}

func BenchmarkTableByFloatingIPMissing(b *testing.B) {
	tbl, err := Create(filepath.Join(b.TempDir(), "routes.shm"), defaultCapacity)
	if err != nil {
		b.Fatal(err)
	}
	defer tbl.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if sid, ok := tbl.ByFloatingIP("100.100.255.254"); ok || sid != "" {
			b.Fatalf("missing lookup = %q, %v", sid, ok)
		}
	}
}

func BenchmarkTableByFloatingIPHit(b *testing.B) {
	tbl, err := Create(filepath.Join(b.TempDir(), "routes.shm"), defaultCapacity)
	if err != nil {
		b.Fatal(err)
	}
	defer tbl.Close()
	const routes = 16384
	for i := 0; i < routes; i++ {
		if err := tbl.Upsert(routesync.RouteEntry{
			SandboxID:  fmt.Sprintf("bench-sid-%d", i),
			State:      routesync.StateRunning,
			FloatingIP: fmt.Sprintf("10.0.%d.%d", i/253, i%253+1),
		}); err != nil {
			b.Fatal(err)
		}
	}
	idx := routes / 2
	ip := fmt.Sprintf("10.0.%d.%d", idx/253, idx%253+1)
	want := fmt.Sprintf("bench-sid-%d", idx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if sid, ok := tbl.ByFloatingIP(ip); !ok || sid != want {
			b.Fatalf("hit lookup = %q, %v; want %q", sid, ok, want)
		}
	}
}

func BenchmarkTableFloatingIndexChurn(b *testing.B) {
	tbl, err := Create(filepath.Join(b.TempDir(), "routes.shm"), 256)
	if err != nil {
		b.Fatal(err)
	}
	defer tbl.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sid := fmt.Sprintf("churn-%d", i)
		ip := fmt.Sprintf("10.1.%d.%d", i/253, i%253+1)
		if err := tbl.Upsert(routesync.RouteEntry{
			SandboxID: sid, State: routesync.StateRunning, FloatingIP: ip,
		}); err != nil {
			b.Fatal(err)
		}
		if !tbl.Delete(sid) {
			b.Fatalf("Delete(%q) returned false", sid)
		}
	}
}

func TestTableIdenticalUpsertKeepsLifecycleRevision(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	route := routesync.RouteEntry{SandboxID: "duplicate-paused", State: routesync.StatePaused}
	tbl.BeginSync()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	_, found, routeRev := tbl.LookupRevision(route.SandboxID)
	if !found || routeRev == 0 {
		t.Fatalf("initial route found=%v rev=%d", found, routeRev)
	}
	globalRev := tbl.Rev()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if _, found, got := tbl.LookupRevision(route.SandboxID); !found || got != routeRev || tbl.Rev() != globalRev {
		t.Fatalf("same-generation replay found=%v route_rev=%d global_rev=%d; want %d/%d",
			found, got, tbl.Rev(), routeRev, globalRev)
	}

	// An identical route in a new Range generation must refresh SyncGen so the
	// Bookmark retains it, but that replay still is not a lifecycle transition.
	tbl.BeginSync()
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if _, found, got := tbl.LookupRevision(route.SandboxID); !found || got != routeRev || tbl.Rev() != globalRev {
		t.Fatalf("new-generation replay found=%v route_rev=%d global_rev=%d; want %d/%d",
			found, got, tbl.Rev(), routeRev, globalRev)
	}
	tbl.Bookmark()
	if got, found := tbl.Lookup(route.SandboxID); !found || got != route {
		t.Fatalf("identical replay was swept at Bookmark: %+v found=%v", got, found)
	}
}

func TestTableDeleteBackshiftPreservesCollidingRoutes(t *testing.T) {
	const capacity = 16
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	byHome := make(map[uint64][]string)
	var colliding []string
	for i := 0; len(colliding) < 6; i++ {
		sid := fmt.Sprintf("collision-%d", i)
		home := hashSID(sid) % capacity
		byHome[home] = append(byHome[home], sid)
		if len(byHome[home]) == 6 {
			colliding = byHome[home]
		}
	}
	for _, sid := range colliding {
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateRunning}); err != nil {
			t.Fatal(err)
		}
	}
	deletedSID := colliding[2]
	if !tbl.Delete(deletedSID) {
		t.Fatal("delete returned false")
	}
	if _, found := tbl.Lookup(deletedSID); found {
		t.Fatalf("deleted colliding route %q remains live", deletedSID)
	}
	for _, sid := range append(colliding[:2], colliding[3:]...) {
		if got, found := tbl.Lookup(sid); !found || got.SandboxID != sid {
			t.Fatalf("colliding route %q lost after backshift: %+v found=%v", sid, got, found)
		}
	}
}

func TestTableDeleteChurnLeavesLiveProbeTableReclaimable(t *testing.T) {
	const capacity = 16
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	for i := 0; i < 1000; i++ {
		sid := fmt.Sprintf("churn-%d", i)
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateStarting}); err != nil {
			t.Fatalf("upsert %d: %v", i, err)
		}
		if !tbl.Delete(sid) {
			t.Fatalf("delete %d returned false", i)
		}
		// Unknown-Wake terminal correlation also stays entirely outside the live
		// probe table, even under valid-length random-SID churn.
		tbl.Delete(fmt.Sprintf("unknown-%d", i))
	}
	for i := range tbl.records {
		if status := atomic.LoadUint32(&tbl.records[i].Status); status != statusEmpty {
			t.Fatalf("live record %d retained status %d after churn", i, status)
		}
	}
	if len(tbl.terminals) != capacity {
		t.Fatalf("terminal cache capacity = %d, want bounded %d", len(tbl.terminals), capacity)
	}
	if got := terminalCapacity(defaultCapacity); got != maxTerminalRevisions {
		t.Fatalf("default terminal cache capacity = %d, want bounded %d", got, maxTerminalRevisions)
	}
}

func TestTableBackshiftMatchesMapUnderRandomChurn(t *testing.T) {
	const capacity = 17
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	rng := rand.New(rand.NewSource(135))
	want := make(map[string]routesync.RouteEntry)
	allSIDs := make([]string, capacity*4)
	for i := range allSIDs {
		allSIDs[i] = fmt.Sprintf("model-%d", i)
	}
	for step := 0; step < 10000; step++ {
		sid := allSIDs[rng.Intn(len(allSIDs))]
		if _, found := want[sid]; found || len(want) == capacity || rng.Intn(3) == 0 {
			tbl.Delete(sid)
			delete(want, sid)
		} else {
			route := routesync.RouteEntry{
				SandboxID: sid,
				State:     []string{routesync.StateStarting, routesync.StateRunning, routesync.StatePaused}[step%3],
				Profile:   fmt.Sprintf("p-%d", step),
			}
			if err := tbl.Upsert(route); err != nil {
				t.Fatalf("step %d upsert %q with %d live routes: %v", step, sid, len(want), err)
			}
			want[sid] = route
		}
		for _, probe := range allSIDs {
			got, found := tbl.Lookup(probe)
			expected, expectedFound := want[probe]
			if found != expectedFound || found && got != expected {
				t.Fatalf("step %d lookup %q = %+v found=%v, want %+v found=%v",
					step, probe, got, found, expected, expectedFound)
			}
		}
	}
}

func TestTableLookupRevisionNeverMixesConcurrentRouteAndRevision(t *testing.T) {
	tbl, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	const sid = "revision-race"
	firstWritten := make(chan struct{})
	continueWriter := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		route := routesync.RouteEntry{SandboxID: sid, State: routesync.StateStarting}
		for i := 0; i < 5000; i++ {
			nextRev := tbl.Rev() + 1
			route.TemplateID = strconv.FormatUint(nextRev, 10)
			if i%2 == 0 {
				route.State = routesync.StateStarting
			} else {
				route.State = routesync.StateRunning
			}
			if err := tbl.Upsert(route); err != nil {
				done <- err
				return
			}
			if i == 0 {
				close(firstWritten)
				<-continueWriter
			}
			runtime.Gosched()
		}
		done <- nil
	}()
	select {
	case <-firstWritten:
	case err := <-done:
		t.Fatalf("first route write: %v", err)
	}
	firstRoute, found, firstRev := tbl.LookupRevision(sid)
	close(continueWriter)
	if !found || firstRoute.TemplateID != strconv.FormatUint(firstRev, 10) {
		writerErr := <-done
		if writerErr != nil {
			t.Fatal(writerErr)
		}
		t.Fatalf("invalid first route/revision snapshot: route=%+v found=%v rev=%d", firstRoute, found, firstRev)
	}

	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
		route, found, rev := tbl.LookupRevision(sid)
		if found && route.TemplateID != strconv.FormatUint(rev, 10) {
			writerErr := <-done
			if writerErr != nil {
				t.Fatal(writerErr)
			}
			t.Fatalf("mixed route/revision snapshot: route=%+v rev=%d", route, rev)
		}
		runtime.Gosched()
	}
}

func TestTableSupportsPortableTemplateID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	templateID := types.TemplateID{
		Profile: types.ProfileE2B,
		Kind:    types.KindSnp,
		Ref: "file://" + strings.Repeat("a", 64) + ".snapshot@location:" +
			"019fb713-6fd5-71f0-b9bc-4a2a20147e53",
	}.String()
	if len(templateID) <= 128 {
		t.Fatalf("portable template ID length = %d, want >128", len(templateID))
	}
	entry := routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateRunning, TemplateID: templateID,
	}
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	got, ok := tbl.Lookup("s1")
	if !ok || got.TemplateID != templateID {
		t.Fatalf("template ID round-trip = %q ok=%v", got.TemplateID, ok)
	}
}

func TestTableCredentialFieldBoundariesAndOverwrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()

	tests := []struct {
		name string
		max  int
		set  func(*routesync.RouteEntry, string)
		get  func(routesync.RouteEntry) string
	}{
		{"auth_sandbox_id", maxSandboxID, func(r *routesync.RouteEntry, v string) { r.AuthSandboxID = v }, func(r routesync.RouteEntry) string { return r.AuthSandboxID }},
		{"api_secret", maxSecret, func(r *routesync.RouteEntry, v string) { r.APISecret = v }, func(r routesync.RouteEntry) string { return r.APISecret }},
		{"api_secret_fingerprint", maxFingerprint, func(r *routesync.RouteEntry, v string) { r.APISecretFingerprint = v }, func(r routesync.RouteEntry) string { return r.APISecretFingerprint }},
		{"manifest_key_fingerprint", maxFingerprint, func(r *routesync.RouteEntry, v string) { r.ManifestKeyFingerprint = v }, func(r routesync.RouteEntry) string { return r.ManifestKeyFingerprint }},
		{"service_secret", maxSecret, func(r *routesync.RouteEntry, v string) { r.ServiceSecret = v }, func(r routesync.RouteEntry) string { return r.ServiceSecret }},
		{"envd_access_token", maxAccessToken, func(r *routesync.RouteEntry, v string) { r.EnvdAccessToken = v }, func(r routesync.RouteEntry) string { return r.EnvdAccessToken }},
		{"traffic_access_token", maxAccessToken, func(r *routesync.RouteEntry, v string) { r.TrafficAccessToken = v }, func(r routesync.RouteEntry) string { return r.TrafficAccessToken }},
		{"forward_access_token", maxAccessToken, func(r *routesync.RouteEntry, v string) { r.ForwardAccessToken = v }, func(r routesync.RouteEntry) string { return r.ForwardAccessToken }},
		{"mmds_secret", maxMmdsSecret, func(r *routesync.RouteEntry, v string) { r.MmdsSecret = v }, func(r routesync.RouteEntry) string { return r.MmdsSecret }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			route := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning}
			exact := strings.Repeat("x", tc.max)
			tc.set(&route, exact)
			if err := tbl.Upsert(route); err != nil {
				t.Fatalf("exact boundary rejected: %v", err)
			}
			got, ok := tbl.Lookup("s1")
			if !ok || tc.get(got) != exact {
				t.Fatalf("exact boundary round-trip length=%d ok=%v", len(tc.get(got)), ok)
			}

			short := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning}
			tc.set(&short, "y")
			if err := tbl.Upsert(short); err != nil {
				t.Fatal(err)
			}
			got, ok = tbl.Lookup("s1")
			if !ok || tc.get(got) != "y" {
				t.Fatalf("short overwrite = %q ok=%v", tc.get(got), ok)
			}

			overlong := routesync.RouteEntry{SandboxID: "s1", State: routesync.StateRunning}
			tc.set(&overlong, strings.Repeat("z", tc.max+1))
			if err := tbl.Upsert(overlong); err == nil {
				t.Fatal("overlong value accepted")
			}
		})
	}
}

func TestBookmarkSweepsMissingRoutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	for _, sid := range []string{"s1", "s2"} {
		if err := tbl.Upsert(routesync.RouteEntry{SandboxID: sid, State: routesync.StateRunning}); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark()
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{SandboxID: "s2", State: routesync.StateRunning}); err != nil {
		t.Fatal(err)
	}
	if _, ok := tbl.Lookup("s1"); !ok {
		t.Fatal("route swept before bookmark")
	}
	tbl.Bookmark()
	if _, ok := tbl.Lookup("s1"); ok {
		t.Fatal("route not swept at bookmark")
	}
}

func TestWorkerLookupRouteIsPassiveAndActivationUsesFreshRoute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	updates := &Updates{ch: make(chan struct{})}
	wakeSeen := make(chan string, 1)
	worker := NewWorkerView(tbl, updates, func(sid string) { wakeSeen <- sid }, time.Second)
	tbl.BeginSync()
	tbl.Bookmark()

	type lookupResult struct {
		binding proxy.RouteBinding
		found   bool
		err     error
	}
	lookupDone := make(chan lookupResult, 1)
	go func() {
		binding, found, err := worker.LookupRoute(context.Background(), "s1", proxy.LegacyTarget(49983))
		lookupDone <- lookupResult{binding: binding, found: found, err: err}
	}()
	select {
	case sid := <-wakeSeen:
		t.Fatalf("side-effect-free lookup woke %q", sid)
	case <-time.After(20 * time.Millisecond):
	}
	entry := routesync.RouteEntry{
		SandboxID: "s1", AuthSandboxID: "stable-s1", Profile: "e2b", State: routesync.StatePaused,
		EnvdUDS: "/run/s1/old.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
	}
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	lookup := <-lookupDone
	if lookup.err != nil || !lookup.found || lookup.binding.ExpectedAccessToken != "envd" {
		t.Fatalf("LookupRoute = %+v found=%v err=%v", lookup.binding, lookup.found, lookup.err)
	}
	select {
	case sid := <-wakeSeen:
		t.Fatalf("lookup woke %q after route propagation", sid)
	default:
	}

	type activateResult struct {
		route proxy.Route
		found bool
		err   error
	}
	activateDone := make(chan activateResult, 1)
	go func() {
		route, found, err := worker.ActivateRoute(context.Background(), lookup.binding)
		activateDone <- activateResult{route: route, found: found, err: err}
	}()
	select {
	case sid := <-wakeSeen:
		if sid != "s1" {
			t.Fatalf("wake sid = %q", sid)
		}
	case <-time.After(time.Second):
		t.Fatal("authorized activation did not wake paused route")
	}
	entry.State = routesync.StateStarting
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	entry.State = routesync.StateRunning
	entry.EnvdUDS = "/run/s1/fresh.sock"
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	result := <-activateDone
	if result.err != nil || !result.found || result.route.Kind != proxy.KindUDS || result.route.UDS != entry.EnvdUDS {
		t.Fatalf("ActivateRoute = %+v found=%v err=%v", result.route, result.found, result.err)
	}
}

func TestWorkerLookupRouteInitialMissTimesOutWithoutWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	worker := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, 50*time.Millisecond)
	tbl.BeginSync()
	tbl.Bookmark()

	type lookupResult struct {
		binding proxy.RouteBinding
		found   bool
		err     error
	}
	done := make(chan lookupResult, 1)
	go func() {
		binding, found, err := worker.LookupRoute(context.Background(), "never-published", proxy.LegacyTarget(49983))
		done <- lookupResult{binding: binding, found: found, err: err}
	}()
	select {
	case got := <-done:
		t.Fatalf("initial miss returned before passive propagation timeout: %+v", got)
	case <-time.After(10 * time.Millisecond):
	}
	select {
	case got := <-done:
		if got.err != nil || got.found || got.binding != (proxy.RouteBinding{}) {
			t.Fatalf("LookupRoute after propagation timeout = %+v, %v, %v", got.binding, got.found, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("initial miss did not honor the propagation timeout")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("passive propagation timeout emitted %d wakes", got)
	}
}

func TestWorkerLookupRouteExistingTerminalReturnsImmediatelyWithoutWake(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	updates := &Updates{ch: make(chan struct{})}
	var wakes atomic.Int32
	worker := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)
	tbl.BeginSync()
	tbl.Delete("deleted")
	tbl.Bookmark()

	type lookupResult struct {
		binding proxy.RouteBinding
		found   bool
		err     error
	}
	done := make(chan lookupResult, 1)
	go func() {
		binding, found, err := worker.LookupRoute(context.Background(), "deleted", proxy.LegacyTarget(49983))
		done <- lookupResult{binding: binding, found: found, err: err}
	}()
	select {
	case got := <-done:
		if got.err != nil || got.found || got.binding != (proxy.RouteBinding{}) {
			t.Fatalf("LookupRoute after terminal revision = %+v, %v, %v", got.binding, got.found, got.err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("LookupRoute consumed its park timeout after an existing terminal revision")
	}
	if got := wakes.Load(); got != 0 {
		t.Fatalf("terminal route lookup emitted %d wakes", got)
	}
}

func TestWorkerActivateRouteFailsClosedOnBindingChange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	updates := &Updates{ch: make(chan struct{})}
	wakeSeen := make(chan string, 1)
	worker := NewWorkerView(tbl, updates, func(sid string) { wakeSeen <- sid }, time.Second)
	entry := routesync.RouteEntry{
		SandboxID: "s1", AuthSandboxID: "stable-s1", Profile: "e2b", State: routesync.StatePaused,
		EnvdUDS: "/run/s1/envd.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
	}
	tbl.BeginSync()
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	binding, found, err := worker.LookupRoute(context.Background(), entry.SandboxID, proxy.LegacyTarget(49983))
	if err != nil || !found {
		t.Fatalf("LookupRoute found=%v err=%v", found, err)
	}

	done := make(chan bool, 1)
	go func() {
		_, found, _ := worker.ActivateRoute(context.Background(), binding)
		done <- found
	}()
	select {
	case <-wakeSeen:
	case <-time.After(time.Second):
		t.Fatal("activation did not reach Wake")
	}
	entry.State = routesync.StateStarting
	entry.EnvdAccessToken = "rotated"
	if err := tbl.Upsert(entry); err != nil {
		t.Fatal(err)
	}
	updates.bump()
	select {
	case found := <-done:
		if found {
			t.Fatal("activation accepted a binding that changed after Wake")
		}
	case <-time.After(time.Second):
		t.Fatal("binding change did not terminate activation")
	}
}

func TestWorkerStartingActivationWaitsWithoutWakeAndFailsOnRollback(t *testing.T) {
	for _, terminal := range []string{routesync.StateRunning, routesync.StatePaused, routesync.StateDead} {
		t.Run(terminal, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.shm")
			tbl, err := Create(path, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			updates := &Updates{ch: make(chan struct{})}
			var wakes atomic.Int32
			worker := NewWorkerView(tbl, updates, func(string) { wakes.Add(1) }, time.Second)
			entry := routesync.RouteEntry{
				SandboxID: "s1", AuthSandboxID: "stable-s1", Profile: "e2b", State: routesync.StateStarting,
				EnvdUDS: "/run/s1/envd.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward",
			}
			tbl.BeginSync()
			if err := tbl.Upsert(entry); err != nil {
				t.Fatal(err)
			}
			tbl.Bookmark()
			binding, found, err := worker.LookupRoute(context.Background(), entry.SandboxID, proxy.LegacyTarget(49983))
			if err != nil || !found {
				t.Fatalf("LookupRoute found=%v err=%v", found, err)
			}
			done := make(chan bool, 1)
			go func() {
				_, found, _ := worker.ActivateRoute(context.Background(), binding)
				done <- found
			}()
			select {
			case found := <-done:
				t.Fatalf("starting activation returned early: found=%v", found)
			case <-time.After(20 * time.Millisecond):
			}
			if terminal == routesync.StateDead {
				tbl.Delete(entry.SandboxID)
			} else {
				entry.State = terminal
				if err := tbl.Upsert(entry); err != nil {
					t.Fatal(err)
				}
			}
			updates.bump()
			select {
			case found := <-done:
				if found != (terminal == routesync.StateRunning) {
					t.Fatalf("terminal %s found=%v", terminal, found)
				}
			case <-time.After(time.Second):
				t.Fatalf("activation did not observe terminal %s", terminal)
			}
			if got := wakes.Load(); got != 0 {
				t.Fatalf("starting activation emitted %d wakes", got)
			}
		})
	}
}

func TestWorkerRouteBindingSelectsPurposeSpecificAccessToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	tbl.BeginSync()
	for _, route := range []routesync.RouteEntry{
		{
			SandboxID: "e2b", AuthSandboxID: "e2b", Profile: "e2b", State: routesync.StateRunning,
			EnvdUDS: "/run/e2b/envd.sock", CiUDS: "/run/e2b/ci.sock", FloatingIP: "100.100.0.2",
			EnvdAccessToken: "envd", TrafficAccessToken: "traffic", ForwardAccessToken: "forward",
		},
		{
			SandboxID: "bare", AuthSandboxID: "bare", Profile: "bare", State: routesync.StateRunning,
			FloatingIP: "100.100.0.3", EnvdAccessToken: "unused-envd",
			TrafficAccessToken: "unused-traffic", ForwardAccessToken: "bare-forward",
		},
	} {
		if err := tbl.Upsert(route); err != nil {
			t.Fatal(err)
		}
	}
	tbl.Bookmark()
	view := NewWorkerView(tbl, nil, nil, time.Second)

	tests := []struct {
		sid       string
		target    proxy.ConnectTarget
		wantKind  proxy.Kind
		wantToken string
	}{
		{"e2b", proxy.LegacyTarget(49983), proxy.KindUDS, "envd"},
		{"e2b", proxy.LegacyTarget(49999), proxy.KindUDS, "envd"},
		{"e2b", proxy.LegacyTarget(8080), proxy.KindTCP, "forward"},
		{"bare", proxy.LegacyTarget(49983), proxy.KindTCP, "bare-forward"},
		{"bare", proxy.LegacyTarget(49999), proxy.KindTCP, "bare-forward"},
		{"bare", proxy.LegacyTarget(8080), proxy.KindTCP, "bare-forward"},
		{"e2b", proxy.ConnectTarget{Service: proxy.ConnectServiceForward, Port: 49983}, proxy.KindTCP, "forward"},
		{"e2b", proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd, Port: 8080}, proxy.KindUDS, "envd"},
		{"bare", proxy.ConnectTarget{Service: proxy.ConnectServiceE2BEnvd}, proxy.KindDeny, ""},
		{"e2b", proxy.ConnectTarget{Service: proxy.ConnectServiceExec}, proxy.KindDeny, ""},
	}
	for _, tc := range tests {
		binding, found, err := view.LookupRoute(context.Background(), tc.sid, tc.target)
		if err != nil || !found {
			t.Fatalf("LookupRoute(%s, %+v): found=%v err=%v", tc.sid, tc.target, found, err)
		}
		if binding.Kind != tc.wantKind || binding.ExpectedAccessToken != tc.wantToken {
			t.Fatalf("LookupRoute(%s, %+v) = %+v, want kind=%v token=%q", tc.sid, tc.target, binding, tc.wantKind, tc.wantToken)
		}
	}
}

func TestWorkerKnownUnsupportedServiceDoesNotWakePausedRoute(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	master := NewMasterView(tbl, 50*time.Millisecond, nil)
	tbl.BeginSync()
	if err := tbl.Upsert(routesync.RouteEntry{
		SandboxID: "s1", AuthSandboxID: "s1", Profile: "e2b", State: routesync.StatePaused,
	}); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	worker := NewWorkerView(tbl, nil, master.Wake, 50*time.Millisecond)

	binding, found, err := worker.LookupRoute(context.Background(), "s1", proxy.ConnectTarget{Service: proxy.ConnectServiceExec})
	if err != nil || !found || binding.Kind != proxy.KindDeny {
		t.Fatalf("exec binding = %+v found=%v err=%v, want deny", binding, found, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if sid, ok := master.NextWake(ctx); ok {
		t.Fatalf("unsupported service woke paused sandbox %q", sid)
	}
}

func TestMMDSSourceFromSharedTable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routes.shm")
	tbl, err := Create(path, 16)
	if err != nil {
		t.Fatal(err)
	}
	defer tbl.Close()
	secret := []byte("secret")
	tbl.BeginSync()
	route := routesync.RouteEntry{
		SandboxID: "s1", State: routesync.StateStarting, TemplateID: "tmpl",
		FloatingIP: "100.100.0.3", EnvdAccessToken: "envd", MmdsSecret: hex.EncodeToString(secret),
	}
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	tbl.Bookmark()
	tbl.SetMMDSSynced(true)
	view := NewWorkerView(tbl, nil, nil, time.Second)
	assertSource := func(state string) {
		t.Helper()
		if sid, ok := view.ByFloatingIP("100.100.0.3"); !ok || sid != "s1" {
			t.Fatalf("ByFloatingIP(%s) = %q ok=%v", state, sid, ok)
		}
		if tid, tok, ok := view.SandboxInfo("s1"); !ok || tid != "tmpl" || tok != "envd" {
			t.Fatalf("SandboxInfo(%s) = %q %q ok=%v", state, tid, tok, ok)
		}
	}
	assertSource(routesync.StateStarting)
	route.State = routesync.StateRunning
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	assertSource(routesync.StateRunning)
	if got, ok := view.MmdsSecret("s1"); !ok || string(got) != string(secret) {
		t.Fatalf("MmdsSecret = %x ok=%v", got, ok)
	}
	route.State = routesync.StatePaused
	if err := tbl.Upsert(route); err != nil {
		t.Fatal(err)
	}
	if sid, ok := view.ByFloatingIP(route.FloatingIP); ok || sid != "" {
		t.Fatalf("paused ByFloatingIP = %q ok=%v", sid, ok)
	}
	if tid, tok, ok := view.SandboxInfo(route.SandboxID); ok || tid != "" || tok != "" {
		t.Fatalf("paused SandboxInfo = %q %q ok=%v", tid, tok, ok)
	}
}

func TestMMDSSourceFloatingIPReflectsRouteUpdates(t *testing.T) {
	tests := []struct {
		name        string
		initial     string
		wantInitial bool
		updated     string
		wantUpdate  bool
	}{
		{name: "active to paused", initial: routesync.StateStarting, wantInitial: true, updated: routesync.StatePaused, wantUpdate: false},
		{name: "running remains active", initial: routesync.StateRunning, wantInitial: true, updated: routesync.StateRunning, wantUpdate: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "routes.shm")
			tbl, err := Create(path, 16)
			if err != nil {
				t.Fatal(err)
			}
			defer tbl.Close()
			tbl.BeginSync()
			route := routesync.RouteEntry{
				SandboxID: "s1", State: tc.initial, TemplateID: "tmpl",
				FloatingIP: "100.100.0.3", EnvdAccessToken: "envd", MmdsSecret: "736563726574",
			}
			if err := tbl.Upsert(route); err != nil {
				t.Fatal(err)
			}
			tbl.Bookmark()
			tbl.SetMMDSSynced(true)
			view := NewWorkerView(tbl, nil, nil, time.Second)

			if sid, ok := view.ByFloatingIP(route.FloatingIP); ok != tc.wantInitial || (ok && sid != route.SandboxID) {
				t.Fatalf("initial ByFloatingIP = %q ok=%v, want found=%v", sid, ok, tc.wantInitial)
			}

			route.State = tc.updated
			if err := tbl.Upsert(route); err != nil {
				t.Fatal(err)
			}
			if sid, ok := view.ByFloatingIP(route.FloatingIP); ok != tc.wantUpdate || (ok && sid != route.SandboxID) {
				t.Fatalf("updated ByFloatingIP = %q ok=%v, want found=%v", sid, ok, tc.wantUpdate)
			}
		})
	}
}
