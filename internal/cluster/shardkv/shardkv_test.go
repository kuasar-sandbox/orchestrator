package shardkv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testNS Namespace = "test"
const testRS RecordSetName = "records"

func TestMaglevResolverRejectsUnknownNamespace(t *testing.T) {
	resolver := newTestResolver(t, "m1", []MemberID{"m1", "m2", "m3"}, 2)
	if _, err := resolver.ResolveShard("missing", "s1"); !errors.Is(err, ErrUnknownNamespace) {
		t.Fatalf("ResolveShard unknown err=%v, want ErrUnknownNamespace", err)
	}
	view, err := resolver.ResolveShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.WriteSets) != 1 || len(view.WriteSets[0].Members) != 2 || view.WriteSets[0].Quorum != 2 {
		t.Fatalf("view=%+v, want 2 members quorum 2", view)
	}
}

func TestShardCASGetDeleteSizeOne(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)

	rec, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS create rec=%+v ok=%v err=%v", rec, ok, err)
	}
	if rec.Meta.Rev != 1 {
		t.Fatalf("rev=%d, want 1", rec.Meta.Rev)
	}
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("bad")); err != nil || ok {
		t.Fatalf("CAS stale ok=%v err=%v, want conflict without error", ok, err)
	}
	got, found, err := sh.Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("Get got=%q found=%v err=%v", string(got.Value), found, err)
	}
	if _, ok, err := sh.Delete(ctx, "k1", got.Meta.Rev); err != nil || !ok {
		t.Fatalf("Delete ok=%v err=%v", ok, err)
	}
	if tombstone, found, err := sh.GetRecord(ctx, "k1"); err != nil || !found || !tombstone.Deleted {
		t.Fatalf("GetRecord after delete tombstone=%+v found=%v err=%v, want deleted record", tombstone, found, err)
	}
	if got, found, err := sh.Get(ctx, "k1"); err != nil || found {
		t.Fatalf("Get after delete got=%+v found=%v err=%v", got, found, err)
	}
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v2")); err != nil || !ok {
		t.Fatalf("CAS recreate ok=%v err=%v", ok, err)
	}
}

func TestRecordSetReadLocalRequiresReadyView(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)

	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS create ok=%v err=%v", ok, err)
	}
	if _, _, err := sh.Get(ctx, "k1", ReadOptions{Policy: ReadLocal}); !errors.Is(err, ErrLocalViewBehind) {
		t.Fatalf("ReadLocal before ready err=%v, want ErrLocalViewBehind", err)
	}
	if _, err := sh.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	got, found, err := sh.Get(ctx, "k1", ReadOptions{Policy: ReadLocal})
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("ReadLocal after ready got=%q found=%v err=%v", string(got.Value), found, err)
	}
}

func TestRecordSetReadDefaultMinRevUsesReadyLocalView(t *testing.T) {
	ctx := context.Background()
	ready := newReadyMap()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, ready)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)

	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS k1 ok=%v err=%v", ok, err)
	}
	if _, err := sh.Snapshot(ctx); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if _, ok, err := sh.CAS(ctx, "k2", 0, []byte("v2")); err != nil || !ok {
		t.Fatalf("CAS k2 ok=%v err=%v", ok, err)
	}
	ready.set("m2", false)
	ready.set("m3", false)

	got, found, err := sh.Get(ctx, "k1", ReadOptions{MinRev: 1})
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("ReadDefault+MinRev local got=%q found=%v err=%v", string(got.Value), found, err)
	}
	if _, _, err := sh.Get(ctx, "k2", ReadOptions{Policy: ReadLocal, MinRev: 1}); !errors.Is(err, ErrLocalViewBehind) {
		t.Fatalf("ReadLocal saw record beyond ready rev err=%v, want ErrLocalViewBehind", err)
	}
	if _, _, err := sh.Get(ctx, "k2", ReadOptions{MinRev: 2}); !errors.Is(err, ErrQuorum) {
		t.Fatalf("ReadDefault+MinRev fallback err=%v, want ErrQuorum", err)
	}
}

func TestRecordSetCASConcurrentDifferentKeys(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)
	const writers = 24
	start := make(chan struct{})
	errCh := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		go func() {
			<-start
			key := RecordKey(fmt.Sprintf("k%02d", i))
			_, ok, err := sh.CAS(ctx, key, 0, []byte(fmt.Sprintf("v%02d", i)))
			if err != nil {
				errCh <- err
				return
			}
			if !ok {
				errCh <- fmt.Errorf("CAS %s returned ok=false", key)
				return
			}
			errCh <- nil
		}()
	}
	close(start)
	for i := 0; i < writers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("concurrent CAS failed: %v", err)
		}
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Rev != writers || len(snap.Records) != writers {
		t.Fatalf("snapshot rev=%d records=%d, want %d/%d", snap.Rev, len(snap.Records), writers, writers)
	}
}

func TestShardRepairsLaggingReplica(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS ok=%v err=%v", ok, err)
	}
	lagging, err := cluster["m3"].getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	laggingSet := lagging.recordSet(testRS, time.Now())
	laggingSet.mu.Lock()
	delete(laggingSet.records, "k1")
	laggingSet.mu.Unlock()

	got, found, err := sh.Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("Get got=%q found=%v err=%v", string(got.Value), found, err)
	}
	repaired := mustRecordSet(t, cluster["m3"], testNS, "s1", testRS)
	got, found, err = repaired.Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("lagging replica after repair got=%q found=%v err=%v", string(got.Value), found, err)
	}
}

func TestShardGetRetriesWhenQuorumMissesHigherPromisedRecord(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	high := Ballot{Round: 10, Writer: "m3"}
	rec := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1", Value: []byte("v-high"),
		Meta: RecordMeta{Ballot: high, Rev: 1, UpdatedAt: time.Now()},
	}
	installLocalSnapshot(t, cluster["m2"], "s1", testRS, []Record{rec}, 1)
	installLocalSnapshot(t, cluster["m3"], "s1", testRS, []Record{rec}, 1)

	got, found, err := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS).Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v-high" {
		t.Fatalf("Get got=%q found=%v err=%v, want high promised record", string(got.Value), found, err)
	}
	for _, member := range []MemberID{"m1", "m2", "m3"} {
		rec, found, err := mustRecordSet(t, cluster[member], testNS, "s1", testRS).Get(ctx, "k1")
		if err != nil || !found || string(rec.Value) != "v-high" {
			t.Fatalf("%s repaired rec=%q found=%v err=%v", member, string(rec.Value), found, err)
		}
	}
}

func TestRecordSetGetDoesNotPromoteSingleReplicaAheadOfQuorum(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	partial := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1", Value: []byte("partial"),
		Meta: RecordMeta{Ballot: Ballot{Round: 1, Writer: "m1"}, Rev: 1, UpdatedAt: time.Now()},
	}
	installLocalSnapshot(t, cluster["m1"], "s1", testRS, []Record{partial}, 1)

	got, found, err := mustRecordSet(t, cluster["m2"], testNS, "s1", testRS).Get(ctx, "k1")
	if err != nil || found {
		t.Fatalf("Get partial got=%+v found=%v err=%v, want not found", got, found, err)
	}
	for member, store := range cluster {
		snap, err := mustRecordSet(t, store, testNS, "s1", testRS).Snapshot(ctx)
		if err != nil {
			t.Fatalf("%s snapshot: %v", member, err)
		}
		if snap.Rev != 0 || len(snap.Records) != 0 {
			t.Fatalf("%s snapshot after cleanup=%+v, want empty rev 0", member, snap)
		}
	}
}

func TestMemberReadyFailFastDoesNotChangeQuorum(t *testing.T) {
	ctx := context.Background()
	ready := newReadyMap()
	ready.set("m3", false)
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, ready)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS with one not-ready member ok=%v err=%v", ok, err)
	}

	ready.set("m2", false)
	if _, _, err := sh.CAS(ctx, "k2", 0, []byte("v2")); !errors.Is(err, ErrQuorum) {
		t.Fatalf("CAS with two not-ready members err=%v, want ErrQuorum", err)
	}
}

func TestRecordSetCommitCertificateRestoresAvailabilityAfterDifferentReplicaFailure(t *testing.T) {
	ctx := context.Background()
	ready := newReadyMap()
	ready.set("m3", false)
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, ready)

	if _, ok, err := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS).CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS k1 with m3 down ok=%v err=%v", ok, err)
	}

	ready.set("m3", true)
	ready.set("m1", false)
	rec, ok, err := mustRecordSet(t, cluster["m2"], testNS, "s1", testRS).CAS(ctx, "k2", 0, []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("CAS k2 with m1 down after m3 returns ok=%v err=%v", ok, err)
	}
	if rec.Meta.Rev != 2 {
		t.Fatalf("k2 rev=%d, want 2", rec.Meta.Rev)
	}

	snap, err := mustRecordSet(t, cluster["m2"], testNS, "s1", testRS).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[RecordKey]string{}
	for _, rec := range snap.Records {
		keys[rec.Key] = string(rec.Value)
	}
	if snap.Rev != 2 || keys["k1"] != "v1" || keys["k2"] != "v2" {
		t.Fatalf("snapshot=%+v keys=%+v, want k1/k2 at rev 2", snap, keys)
	}
}

func TestRecordSetJointViewAcceptsCommittedHeadFromActiveCertificate(t *testing.T) {
	ctx := context.Background()
	oldView := ClusterView{Version: 1, Label: "v1", Members: []MemberID{"m1", "m2", "m3"}}
	newView := ClusterView{Version: 2, Label: "v2", Members: []MemberID{"m3", "m4", "m5"}}
	stores := map[MemberID]*Store{}
	transport := TransportFunc(func(ctx context.Context, member MemberID, req Request) (Response, error) {
		store := stores[member]
		if store == nil {
			return Response{}, ErrReplicaUnavailable
		}
		return store.Handle(ctx, req)
	})
	for _, member := range oldView.Members {
		resolver := newTestResolverWithViews(t, member, []ClusterView{oldView}, 3)
		store, err := NewStore(StoreOptions{
			Local: member, Resolver: resolver, Transport: transport, Epoch: "epoch-" + string(member),
		})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
	}

	oldCoordinator := mustRecordSet(t, stores["m1"], testNS, "s-joint", testRS)
	if _, ok, err := oldCoordinator.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("old CAS ok=%v err=%v", ok, err)
	}

	jointViews := []ClusterView{oldView, newView}
	for _, member := range []MemberID{"m1", "m2", "m3", "m4", "m5"} {
		resolver := newTestResolverWithViews(t, member, jointViews, 3)
		if store := stores[member]; store != nil {
			if err := store.Configure(resolver, transport, nil, 10000); err != nil {
				t.Fatalf("configure %s: %v", member, err)
			}
			continue
		}
		store, err := NewStore(StoreOptions{
			Local: member, Resolver: resolver, Transport: transport, Epoch: "epoch-" + string(member),
		})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
	}

	jointCoordinator := mustRecordSet(t, stores["m4"], testNS, "s-joint", testRS)
	rec, ok, err := jointCoordinator.CAS(ctx, "k2", 0, []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("joint CAS ok=%v err=%v", ok, err)
	}
	if rec.Meta.Rev != 2 {
		t.Fatalf("joint CAS rev=%d, want 2", rec.Meta.Rev)
	}
	snap, err := mustRecordSet(t, stores["m5"], testNS, "s-joint", testRS).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[RecordKey]string{}
	for _, rec := range snap.Records {
		keys[rec.Key] = string(rec.Value)
	}
	if snap.Rev != 2 || keys["k1"] != "v1" || keys["k2"] != "v2" {
		t.Fatalf("joint snapshot=%+v keys=%+v, want old and new records at rev 2", snap, keys)
	}
}

func TestRecordSetCutoverReadsColdOldGraceHead(t *testing.T) {
	ctx := context.Background()
	oldView := ClusterView{Version: 1, Label: "v1", Members: []MemberID{"m1", "m2", "m3"}}
	newView := ClusterView{Version: 2, Label: "v2", Members: []MemberID{"m3", "m4", "m5"}}
	stores := map[MemberID]*Store{}
	transport := TransportFunc(func(ctx context.Context, member MemberID, req Request) (Response, error) {
		store := stores[member]
		if store == nil {
			return Response{}, ErrReplicaUnavailable
		}
		return store.Handle(ctx, req)
	})
	for _, member := range oldView.Members {
		resolver := newTestResolverWithViews(t, member, []ClusterView{oldView}, 3)
		store, err := NewStore(StoreOptions{
			Local: member, Resolver: resolver, Transport: transport, Epoch: "epoch-" + string(member),
		})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
	}
	if _, ok, err := mustRecordSet(t, stores["m1"], testNS, "s-cold-cutover", testRS).CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("old CAS ok=%v err=%v", ok, err)
	}

	cutoverViews := []ClusterView{
		{Version: oldView.Version, Label: oldView.Label, Members: oldView.Members, ReadOnly: true},
		newView,
	}
	for _, member := range []MemberID{"m1", "m2", "m3", "m4", "m5"} {
		resolver := newTestResolverWithViews(t, member, cutoverViews, 3)
		if store := stores[member]; store != nil {
			if err := store.Configure(resolver, transport, nil, 10000); err != nil {
				t.Fatalf("configure %s: %v", member, err)
			}
			continue
		}
		store, err := NewStore(StoreOptions{
			Local: member, Resolver: resolver, Transport: transport, Epoch: "epoch-" + string(member),
		})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
	}

	got, found, err := mustRecordSet(t, stores["m4"], testNS, "s-cold-cutover", testRS).Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("cold cutover read got=%q found=%v err=%v, want v1", string(got.Value), found, err)
	}
	snap, err := mustRecordSet(t, stores["m5"], testNS, "s-cold-cutover", testRS).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Rev != 1 || len(snap.Records) != 1 || snap.Records[0].Key != "k1" || string(snap.Records[0].Value) != "v1" {
		t.Fatalf("new owner snapshot after cold repair=%+v, want k1=v1 rev 1", snap)
	}

	delete(stores, "m1")
	delete(stores, "m2")
	got, found, err = mustRecordSet(t, stores["m5"], testNS, "s-cold-cutover", testRS).Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("repaired read without old_grace quorum got=%q found=%v err=%v, want v1", string(got.Value), found, err)
	}
	rec, ok, err := mustRecordSet(t, stores["m4"], testNS, "s-cold-cutover", testRS).CAS(ctx, "k2", 0, []byte("v2"))
	if err != nil || !ok || rec.Meta.Rev != 2 {
		t.Fatalf("active write after cold repair without old_grace quorum rec=%+v ok=%v err=%v, want rev 2", rec, ok, err)
	}
}

func TestRecordSetWatchDoesNotEmitUncommittedAccept(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stores := map[MemberID]*Store{}
	transport := TransportFunc(func(ctx context.Context, member MemberID, req Request) (Response, error) {
		if req.Op == OpAccept {
			return Response{}, ErrReplicaUnavailable
		}
		store := stores[member]
		if store == nil {
			return Response{}, ErrReplicaUnavailable
		}
		return store.Handle(ctx, req)
	})
	for _, member := range []MemberID{"m1", "m2", "m3"} {
		resolver := newTestResolver(t, member, []MemberID{"m1", "m2", "m3"}, 3)
		store, err := NewStore(StoreOptions{
			Local: member, Resolver: resolver, Transport: transport, Epoch: "epoch-" + string(member),
		})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
	}

	sh := mustRecordSet(t, stores["m1"], testNS, "s1", testRS)
	w, err := sh.Watch(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, w.Events, EventReset, "")
	assertEvent(t, w.Events, EventBookmark, "")

	if _, _, err := sh.CAS(ctx, "k1", 0, []byte("v1")); !errors.Is(err, ErrQuorum) {
		t.Fatalf("CAS with remote accept failures err=%v, want ErrQuorum", err)
	}
	select {
	case ev := <-w.Events:
		t.Fatalf("watch emitted uncommitted event: %+v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestNonOwnerCanCoordinateWritesButCannotServeLocalView(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 1, nil)
	shard, owner := shardNotOwnedBy(t, cluster["m3"], "m3")

	coordinator := mustRecordSet(t, cluster["m3"], testNS, shard, testRS)
	if _, ok, err := coordinator.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("non-owner CAS coordinator ok=%v err=%v", ok, err)
	}
	got, found, err := mustRecordSet(t, cluster[owner], testNS, shard, testRS).Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("owner Get got=%q found=%v err=%v", string(got.Value), found, err)
	}
	if _, err := coordinator.Snapshot(ctx); !errors.Is(err, ErrInvalidView) {
		t.Fatalf("non-owner Snapshot err=%v, want ErrInvalidView", err)
	}
	if _, err := coordinator.Watch(ctx, ""); !errors.Is(err, ErrInvalidView) {
		t.Fatalf("non-owner Watch err=%v, want ErrInvalidView", err)
	}
	if _, err := coordinator.WatchSince(ctx, 0); !errors.Is(err, ErrInvalidView) {
		t.Fatalf("non-owner WatchSince err=%v, want ErrInvalidView", err)
	}
}

func TestShardKVOverHTTPTransport(t *testing.T) {
	ctx := context.Background()
	members := []MemberID{"m1", "m2", "m3"}
	stores := map[MemberID]*Store{}
	servers := map[MemberID]*httptest.Server{}
	endpoints := map[MemberID]string{}
	for _, member := range members {
		resolver := newTestResolver(t, member, members, 3)
		store, err := NewStore(StoreOptions{Local: member, Resolver: resolver, Epoch: "epoch-" + string(member)})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ServeHTTP(store)(w, req)
		}))
		t.Cleanup(server.Close)
		servers[member] = server
		endpoints[member] = server.URL
	}
	transport := NewHTTPTransport(servers["m1"].Client(), HTTPEndpointResolverFunc(func(member MemberID) (string, bool) {
		endpoint, ok := endpoints[member]
		return endpoint, ok
	}))
	for _, store := range stores {
		store.transport = transport
	}

	sh := mustRecordSet(t, stores["m1"], testNS, "s-http", testRS)
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS over http ok=%v err=%v", ok, err)
	}
	got, found, err := mustRecordSet(t, stores["m2"], testNS, "s-http", testRS).Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("Get over http got=%q found=%v err=%v", string(got.Value), found, err)
	}
}

func TestShardKVRejectsStaleViewLabel(t *testing.T) {
	ctx := context.Background()
	resolver, err := NewMaglevResolver(MaglevResolverConfig{
		Local: "m1",
		Views: []ClusterView{{Version: 2, Label: "registry.2.hash", Members: []MemberID{"m1"}}},
		Layout: Layout{Namespaces: map[Namespace]NamespaceSpec{
			testNS: {ShardMemberCount: 1, TombstoneRetention: time.Hour, WatchRetention: 100},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(StoreOptions{Local: "m1", Resolver: resolver, Epoch: "epoch-m1"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.Handle(ctx, Request{
		Op: OpRepair, Label: "registry.1.hash", Namespace: testNS, Shard: "s1", RecordSet: testRS,
		Record: Record{Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1", Value: []byte("old")},
	})
	if !errors.Is(err, ErrInvalidView) {
		t.Fatalf("stale label repair err=%v, want ErrInvalidView", err)
	}
}

func TestShardKVHTTPRequiresViewLabel(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	srv := httptest.NewServer(ServeHTTP(store))
	defer srv.Close()

	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(Request{Op: OpSnapshot, Namespace: testNS, Shard: "s1", RecordSet: testRS}); err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+HTTPPath, &body)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", resp.StatusCode)
	}
}

func TestWatchResetAndIncrementalEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS ok=%v err=%v", ok, err)
	}
	w, err := sh.Watch(ctx, "bad-token")
	if err != nil {
		t.Fatal(err)
	}
	if !w.Reset {
		t.Fatal("watch reset=false, want true")
	}
	assertEvent(t, w.Events, EventReset, "")
	assertEvent(t, w.Events, EventPut, "k1")
	assertEvent(t, w.Events, EventBookmark, "")

	if _, ok, err := sh.CAS(ctx, "k2", 0, []byte("v2")); err != nil || !ok {
		t.Fatalf("CAS k2 ok=%v err=%v", ok, err)
	}
	live := assertEvent(t, w.Events, EventPut, "k2")
	token, ok := parseWatchToken(live.Token)
	if !ok || token.Label != "v1" {
		t.Fatalf("live token=%q parsed=%+v ok=%v, want label v1", live.Token, token, ok)
	}
}

func TestWatchSurvivesSnapshotInstallReset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	w, err := sh.Watch(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, w.Events, EventReset, "")
	assertEvent(t, w.Events, EventBookmark, "")

	ls, err := store.getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	localSet := ls.recordSet(testRS, time.Now())
	installed := localSet.install([]Record{{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1", Value: []byte("v1"),
		Meta: RecordMeta{Ballot: Ballot{Round: 1, Writer: "repair"}, Rev: 1, UpdatedAt: time.Now()},
	}}, 1, CommitCertificate{}, time.Now())
	if !installed {
		t.Fatal("install snapshot failed")
	}
	assertEvent(t, w.Events, EventReset, "")
	assertEvent(t, w.Events, EventPut, "k1")
	assertEvent(t, w.Events, EventBookmark, "")

	if _, ok, err := sh.CAS(ctx, "k2", 0, []byte("v2")); err != nil || !ok {
		t.Fatalf("CAS k2 ok=%v err=%v", ok, err)
	}
	assertEvent(t, w.Events, EventPut, "k2")
}

func TestWatchResetStreamsLargeSnapshotWithoutBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	ls, err := store.getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	localSet := ls.recordSet(testRS, time.Now())
	records := make([]Record, 0, 1500)
	for i := 0; i < 1500; i++ {
		key := RecordKey(fmt.Sprintf("k-%04d", i))
		records = append(records, Record{
			Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: key, Value: []byte("v"),
			Meta: RecordMeta{Ballot: Ballot{Round: uint64(i + 1), Writer: "seed"}, Rev: uint64(i + 1), UpdatedAt: time.Now()},
		})
	}
	if !localSet.install(records, 1500, CommitCertificate{}, time.Now()) {
		t.Fatal("install large snapshot failed")
	}

	type watchResult struct {
		w   Watch
		err error
	}
	done := make(chan watchResult, 1)
	go func() {
		w, err := sh.Watch(ctx, "bad-token")
		done <- watchResult{w: w, err: err}
	}()
	var w Watch
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatal(res.err)
		}
		w = res.w
	case <-time.After(time.Second):
		t.Fatal("Watch blocked while preparing a large reset snapshot")
	}
	assertEvent(t, w.Events, EventReset, "")
	count := 0
	for {
		ev := assertAnyEvent(t, w.Events)
		if ev.Type == EventBookmark {
			break
		}
		if ev.Type != EventPut {
			t.Fatalf("event=%+v, want put/bookmark", ev)
		}
		count++
	}
	if count != 1500 {
		t.Fatalf("reset put count=%d, want 1500", count)
	}
}

func TestRecordSetUsesGlobalCommitRevisionAcrossRecords(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)

	r1, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS k1 ok=%v err=%v", ok, err)
	}
	if r1.Meta.Rev != 1 {
		t.Fatalf("k1 rev=%d, want 1", r1.Meta.Rev)
	}
	r2, ok, err := sh.CAS(ctx, "k2", 0, []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("CAS k2 ok=%v err=%v", ok, err)
	}
	if r2.Meta.Rev != 2 {
		t.Fatalf("k2 rev=%d, want recordSet commit rev 2", r2.Meta.Rev)
	}

	snap, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Rev != 2 {
		t.Fatalf("snapshot rev=%d, want 2", snap.Rev)
	}
	revs := map[RecordKey]uint64{}
	for _, rec := range snap.Records {
		revs[rec.Key] = rec.Meta.Rev
	}
	if revs["k1"] != 1 || revs["k2"] != 2 {
		t.Fatalf("record revs=%+v, want k1=1 k2=2", revs)
	}

	w, err := sh.Watch(ctx, snap.Token)
	if err != nil {
		t.Fatal(err)
	}
	r3, ok, err := sh.CAS(ctx, "k3", 0, []byte("v3"))
	if err != nil || !ok {
		t.Fatalf("CAS k3 ok=%v err=%v", ok, err)
	}
	if r3.Meta.Rev != 3 {
		t.Fatalf("k3 rev=%d, want 3", r3.Meta.Rev)
	}
	ev := assertEvent(t, w.Events, EventPut, "k3")
	if ev.Rev != 3 || ev.Record.Meta.Rev != 3 {
		t.Fatalf("watch event rev=%d record rev=%d, want 3", ev.Rev, ev.Record.Meta.Rev)
	}
}

func TestRecordSetRevisionsAreIndependentWithinShard(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	a := mustRecordSet(t, store, testNS, "s1", "a")
	b := mustRecordSet(t, store, testNS, "s1", "b")

	a1, ok, err := a.CAS(ctx, "k1", 0, []byte("a1"))
	if err != nil || !ok {
		t.Fatalf("CAS a/k1 ok=%v err=%v", ok, err)
	}
	a2, ok, err := a.CAS(ctx, "k2", 0, []byte("a2"))
	if err != nil || !ok {
		t.Fatalf("CAS a/k2 ok=%v err=%v", ok, err)
	}
	b1, ok, err := b.CAS(ctx, "k1", 0, []byte("b1"))
	if err != nil || !ok {
		t.Fatalf("CAS b/k1 ok=%v err=%v", ok, err)
	}
	if a1.Meta.Rev != 1 || a2.Meta.Rev != 2 || b1.Meta.Rev != 1 {
		t.Fatalf("revs a1=%d a2=%d b1=%d, want 1/2/1", a1.Meta.Rev, a2.Meta.Rev, b1.Meta.Rev)
	}
	aSnap, err := a.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	bWatch, err := b.Watch(ctx, aSnap.Token)
	if err != nil {
		t.Fatal(err)
	}
	if !bWatch.Reset {
		t.Fatal("cross-recordSet token did not force reset")
	}
}

func TestRecordSetGetDoesNotRepairUncommittedPartialAccept(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	partial := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1", Value: []byte("partial"),
		Meta: RecordMeta{Ballot: Ballot{Round: 1, Writer: "m1"}, Rev: 1, UpdatedAt: time.Now()},
	}
	committed := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k2", Value: []byte("committed"),
		Meta: RecordMeta{Ballot: Ballot{Round: 2, Writer: "m2"}, Rev: 1, UpdatedAt: time.Now()},
	}
	installLocalSnapshot(t, cluster["m1"], "s1", testRS, []Record{partial}, 1)
	installLocalSnapshot(t, cluster["m2"], "s1", testRS, []Record{committed}, 1)
	installLocalSnapshot(t, cluster["m3"], "s1", testRS, []Record{committed}, 1)

	got, found, err := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS).Get(ctx, "k1")
	if err != nil || found {
		t.Fatalf("Get partial got=%+v found=%v err=%v, want not found", got, found, err)
	}
	snap, err := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Rev != 1 || len(snap.Records) != 1 || snap.Records[0].Key != "k2" {
		t.Fatalf("snapshot after partial cleanup=%+v, want only committed k2 at rev 1", snap)
	}
}

func TestRecordSetCASCatchesUpConflictingHeadBeforeNextRevision(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	partial := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1", Value: []byte("partial"),
		Meta: RecordMeta{Ballot: Ballot{Round: 1, Writer: "m1"}, Rev: 1, UpdatedAt: time.Now()},
	}
	committed := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k2", Value: []byte("committed"),
		Meta: RecordMeta{Ballot: Ballot{Round: 2, Writer: "m2"}, Rev: 1, UpdatedAt: time.Now()},
	}
	installLocalSnapshot(t, cluster["m1"], "s1", testRS, []Record{partial}, 1)
	installLocalSnapshot(t, cluster["m2"], "s1", testRS, []Record{committed}, 1)
	installLocalSnapshot(t, cluster["m3"], "s1", testRS, []Record{committed}, 1)

	rec, ok, err := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS).CAS(ctx, "k3", 0, []byte("v3"))
	if err != nil || !ok {
		t.Fatalf("CAS after conflicting head ok=%v err=%v", ok, err)
	}
	if rec.Meta.Rev != 2 {
		t.Fatalf("CAS rev=%d, want 2", rec.Meta.Rev)
	}
	for member, store := range cluster {
		snap, err := mustRecordSet(t, store, testNS, "s1", testRS).Snapshot(ctx)
		if err != nil {
			t.Fatalf("%s snapshot: %v", member, err)
		}
		keys := map[RecordKey]bool{}
		for _, rec := range snap.Records {
			keys[rec.Key] = true
		}
		if snap.Rev != 2 || keys["k1"] || !keys["k2"] || !keys["k3"] {
			t.Fatalf("%s snapshot=%+v, want rev 2 with k2/k3 and no partial k1", member, snap)
		}
	}
}

func TestShardCASCatchesUpReplicaHeadBeforeAllocatingNextRevision(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS k1 ok=%v err=%v", ok, err)
	}
	r2, ok, err := sh.CAS(ctx, "k2", 0, []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("CAS k2 ok=%v err=%v", ok, err)
	}
	if r2.Meta.Rev != 2 {
		t.Fatalf("k2 rev=%d, want 2", r2.Meta.Rev)
	}

	ls, err := cluster["m3"].getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	localSet := ls.recordSet(testRS, time.Now())
	localSet.mu.Lock()
	delete(localSet.records, "k2")
	localSet.rev = 1
	localSet.log = nil
	localSet.mu.Unlock()

	r3, ok, err := sh.CAS(ctx, "k3", 0, []byte("v3"))
	if err != nil || !ok {
		t.Fatalf("CAS k3 ok=%v err=%v", ok, err)
	}
	if r3.Meta.Rev != 3 {
		t.Fatalf("k3 rev=%d, want 3 after catch-up", r3.Meta.Rev)
	}
	for _, member := range []MemberID{"m1", "m2", "m3"} {
		snap, err := mustRecordSet(t, cluster[member], testNS, "s1", testRS).Snapshot(ctx)
		if err != nil {
			t.Fatalf("%s snapshot: %v", member, err)
		}
		if snap.Rev != 3 {
			t.Fatalf("%s snapshot rev=%d, want 3", member, snap.Rev)
		}
	}
}

func TestShardRejectsStaleAcceptThatWouldRegressRecord(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	r1, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS k1 v1 ok=%v err=%v", ok, err)
	}
	r2, ok, err := sh.CAS(ctx, "k1", r1.Meta.Rev, []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("CAS k1 v2 ok=%v err=%v", ok, err)
	}
	stale := r1
	stale.Meta.Ballot = Ballot{Round: r2.Meta.Ballot.Round + 10, Writer: "stale"}
	resp, err := store.Handle(ctx, Request{
		Op: OpAccept, Label: "v1", Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k1",
		Ballot: stale.Meta.Ballot, Record: stale,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("stale accept ok=true, want reject")
	}
	got, found, err := sh.Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v2" || got.Meta.Rev != r2.Meta.Rev {
		t.Fatalf("record regressed got=%q rev=%d found=%v err=%v", string(got.Value), got.Meta.Rev, found, err)
	}
}

func TestShardRejectsStaleInstallThatWouldRegressRecordSet(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	_, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS k1 ok=%v err=%v", ok, err)
	}
	old, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshot, err := store.Handle(ctx, Request{
		Op: OpSnapshot, Label: "v1", Namespace: testNS, Shard: "s1", RecordSet: testRS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := sh.CAS(ctx, "k2", 0, []byte("v2")); err != nil || !ok {
		t.Fatalf("CAS k2 ok=%v err=%v", ok, err)
	}

	resp, err := store.Handle(ctx, Request{
		Op: OpInstall, Label: "v1", Namespace: testNS, Shard: "s1", RecordSet: testRS,
		Records: oldSnapshot.Records, Rev: old.Rev, Certificate: oldSnapshot.Certificate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("stale install ok=true, want reject")
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[RecordKey]string{}
	for _, rec := range snap.Records {
		keys[rec.Key] = string(rec.Value)
	}
	if snap.Rev != 2 || keys["k1"] != "v1" || keys["k2"] != "v2" {
		t.Fatalf("snapshot regressed rev=%d keys=%+v, want rev 2 with k1/k2", snap.Rev, keys)
	}
}

func TestShardRejectsSameRevDifferentInstall(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	r1, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS k1 ok=%v err=%v", ok, err)
	}
	conflict := Record{
		Namespace: testNS, Shard: "s1", RecordSet: testRS, Key: "k2", Value: []byte("v2"),
		Meta: RecordMeta{Ballot: Ballot{Round: r1.Meta.Ballot.Round + 1, Writer: "other"}, Rev: r1.Meta.Rev, UpdatedAt: time.Now()},
	}
	view, err := sh.View(ctx)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := makeCommitCertificate(r1.Meta.Rev, []Record{conflict}, conflict.Meta.Ballot, map[MemberID]bool{"m1": true}, view.WriteSets)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := store.Handle(ctx, Request{
		Op: OpInstall, Label: "v1", Namespace: testNS, Shard: "s1", RecordSet: testRS,
		Records: []Record{conflict}, Rev: r1.Meta.Rev, Certificate: certificate,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.OK {
		t.Fatal("same-rev different install ok=true, want reject")
	}
	snap, err := sh.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Rev != 1 || len(snap.Records) != 1 || snap.Records[0].Key != "k1" || string(snap.Records[0].Value) != "v1" {
		t.Fatalf("snapshot changed to conflicting same rev: %+v", snap)
	}
}

func TestCompactTombstoneRequiresReadyMembers(t *testing.T) {
	ctx := context.Background()
	ready := newReadyMap()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, ready)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)
	rec, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS ok=%v err=%v", ok, err)
	}
	if _, ok, err := sh.Delete(ctx, "k1", rec.Meta.Rev); err != nil || !ok {
		t.Fatalf("Delete ok=%v err=%v", ok, err)
	}
	for _, store := range cluster {
		markTombstoneOld(t, store, "s1", "k1", 2*time.Hour)
	}
	ready.set("m3", false)
	stats, err := cluster["m1"].Compact(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tombstones != 0 {
		t.Fatalf("compact with not-ready member stats=%+v, want no tombstone compact", stats)
	}
	ready.set("m3", true)
	stats, err = cluster["m1"].Compact(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tombstones != 1 {
		t.Fatalf("compact stats=%+v, want one tombstone", stats)
	}
}

func TestCompactedTombstoneSnapshotRemainsCertificateEquivalent(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	sh := mustRecordSet(t, cluster["m1"], testNS, "s1", testRS)
	rec, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS ok=%v err=%v", ok, err)
	}
	if _, ok, err := sh.Delete(ctx, "k1", rec.Meta.Rev); err != nil || !ok {
		t.Fatalf("Delete ok=%v err=%v", ok, err)
	}
	for _, store := range cluster {
		markTombstoneOld(t, store, "s1", "k1", 2*time.Hour)
	}
	compactLocalTombstones(t, cluster["m1"], "s1", testRS, time.Hour)
	compactLocalTombstones(t, cluster["m2"], "s1", testRS, time.Hour)

	snap, err := mustRecordSet(t, cluster["m3"], testNS, "s1", testRS).Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot after mixed tombstone compaction: %v", err)
	}
	if snap.Rev != 2 || len(snap.Records) != 0 {
		t.Fatalf("snapshot=%+v, want rev 2 without live records", snap)
	}
	recreated, ok, err := sh.CAS(ctx, "k1", 0, []byte("v2"))
	if err != nil || !ok {
		t.Fatalf("CAS recreate after mixed tombstone compaction ok=%v err=%v", ok, err)
	}
	if recreated.Meta.Rev != 3 {
		t.Fatalf("recreate rev=%d, want 3", recreated.Meta.Rev)
	}
}

func TestCompactEmptyShardGC(t *testing.T) {
	ctx := context.Background()
	resolver, err := NewMaglevResolver(MaglevResolverConfig{
		Local: "m1",
		Views: []ClusterView{{Version: 1, Label: "v1", Members: []MemberID{"m1"}}},
		Layout: Layout{Namespaces: map[Namespace]NamespaceSpec{
			testNS: {ShardMemberCount: 1, TombstoneRetention: time.Hour, IdleShardTTL: time.Hour},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(StoreOptions{Local: "m1", Resolver: resolver, Epoch: "epoch-m1"})
	if err != nil {
		t.Fatal(err)
	}
	sh := mustRecordSet(t, store, testNS, "s1", testRS)
	rec, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1"))
	if err != nil || !ok {
		t.Fatalf("CAS ok=%v err=%v", ok, err)
	}
	if _, ok, err := sh.Delete(ctx, "k1", rec.Meta.Rev); err != nil || !ok {
		t.Fatalf("Delete ok=%v err=%v", ok, err)
	}
	ls, err := store.getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	markTombstoneOld(t, store, "s1", "k1", 2*time.Hour)
	ls.mu.Lock()
	ls.lastAccess = time.Now().Add(-2 * time.Hour)
	ls.mu.Unlock()
	stats, err := store.Compact(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Tombstones != 1 || stats.Shards != 1 {
		t.Fatalf("compact stats=%+v, want tombstone and shard GC", stats)
	}
}

func TestRecordSetCoordinatorStateIsBounded(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	if len(store.stripes) != defaultRecordSetStripes {
		t.Fatalf("recordSet stripes=%d, want %d", len(store.stripes), defaultRecordSetStripes)
	}
	for i := 0; i < 128; i++ {
		rs := mustRecordSet(t, store, testNS, ShardKey(fmt.Sprintf("s-%d", i)), RecordSetName(fmt.Sprintf("rs-%d", i)))
		rec, ok, err := rs.CAS(ctx, "k", 0, []byte("v"))
		if err != nil || !ok {
			t.Fatalf("CAS %d ok=%v err=%v", i, ok, err)
		}
		if _, ok, err := rs.Delete(ctx, "k", rec.Meta.Rev); err != nil || !ok {
			t.Fatalf("Delete %d ok=%v err=%v", i, ok, err)
		}
	}
	if len(store.stripes) != defaultRecordSetStripes {
		t.Fatalf("recordSet stripes grew to %d, want %d", len(store.stripes), defaultRecordSetStripes)
	}
}

func assertEvent(t *testing.T, ch <-chan WatchEvent, typ EventType, key RecordKey) WatchEvent {
	t.Helper()
	ev := assertAnyEvent(t, ch)
	if ev.Type != typ || ev.Key != key {
		t.Fatalf("event=%+v, want type=%s key=%s", ev, typ, key)
	}
	return ev
}

func assertAnyEvent(t *testing.T, ch <-chan WatchEvent) WatchEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for watch event")
	}
	return WatchEvent{}
}

func newSingleStore(t *testing.T, local MemberID) *Store {
	t.Helper()
	resolver := newTestResolver(t, local, []MemberID{local}, 1)
	store, err := NewStore(StoreOptions{Local: local, Resolver: resolver, Epoch: "epoch-" + string(local)})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func newTestCluster(t *testing.T, members []MemberID, count int, ready MemberReadyProvider) map[MemberID]*Store {
	t.Helper()
	stores := map[MemberID]*Store{}
	transport := TransportFunc(func(ctx context.Context, member MemberID, req Request) (Response, error) {
		store := stores[member]
		if store == nil {
			return Response{}, ErrReplicaUnavailable
		}
		return store.Handle(ctx, req)
	})
	for _, member := range members {
		resolver := newTestResolver(t, member, members, count)
		store, err := NewStore(StoreOptions{
			Local: member, Resolver: resolver, Transport: transport, Ready: ready,
			Epoch: "epoch-" + string(member),
		})
		if err != nil {
			t.Fatal(err)
		}
		stores[member] = store
	}
	return stores
}

func newTestResolver(t *testing.T, local MemberID, members []MemberID, count int) *MaglevResolver {
	t.Helper()
	return newTestResolverWithViews(t, local, []ClusterView{{Version: 1, Label: "v1", Members: members}}, count)
}

func newTestResolverWithViews(t *testing.T, local MemberID, views []ClusterView, count int) *MaglevResolver {
	t.Helper()
	resolver, err := NewMaglevResolver(MaglevResolverConfig{
		Local: local,
		Views: views,
		Layout: Layout{Namespaces: map[Namespace]NamespaceSpec{
			testNS: {ShardMemberCount: count, TombstoneRetention: time.Hour, WatchRetention: 100},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return resolver
}

func mustShard(t *testing.T, store *Store, ns Namespace, shard ShardKey) *Shard {
	t.Helper()
	sh, err := store.Shard(ns, shard)
	if err != nil {
		t.Fatal(err)
	}
	return sh
}

func mustRecordSet(t *testing.T, store *Store, ns Namespace, shard ShardKey, recordSet RecordSetName) *RecordSet {
	t.Helper()
	sh := mustShard(t, store, ns, shard)
	rs, err := sh.RecordSet(recordSet)
	if err != nil {
		t.Fatal(err)
	}
	return rs
}

func shardNotOwnedBy(t *testing.T, store *Store, local MemberID) (ShardKey, MemberID) {
	t.Helper()
	for i := 0; i < 1000; i++ {
		shard := ShardKey(fmt.Sprintf("s-non-owner-%d", i))
		view, err := store.resolver.ResolveShard(testNS, shard)
		if err != nil {
			t.Fatal(err)
		}
		members := writeMembers(view)
		owned := false
		for _, member := range members {
			if member == local {
				owned = true
				break
			}
		}
		if !owned && len(members) > 0 {
			return shard, members[0]
		}
	}
	t.Fatalf("could not find shard not owned by %s", local)
	return "", ""
}

func markTombstoneOld(t *testing.T, store *Store, shard ShardKey, key RecordKey, age time.Duration) {
	t.Helper()
	ls, err := store.getLocalShard(testNS, shard)
	if err != nil {
		t.Fatal(err)
	}
	rs := ls.recordSet(testRS, time.Now())
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rec, ok := rs.records[key]
	if !ok || !rec.Deleted {
		t.Fatalf("record %s is not an existing tombstone: %+v", key, rec)
	}
	rec.Meta.UpdatedAt = time.Now().Add(-age)
	rs.records[key] = rec
	rs.lastAccess = time.Now().Add(-age)
}

func compactLocalTombstones(t *testing.T, store *Store, shard ShardKey, recordSet RecordSetName, retention time.Duration) {
	t.Helper()
	ls, err := store.getLocalShard(testNS, shard)
	if err != nil {
		t.Fatal(err)
	}
	rs := ls.recordSetNoTouch(recordSet)
	if rs == nil {
		t.Fatalf("recordSet %s/%s not found", shard, recordSet)
	}
	if n := rs.compactTombstones(time.Now(), retention); n != 1 {
		t.Fatalf("compact local tombstone count=%d, want 1", n)
	}
}

func installLocalSnapshot(t *testing.T, store *Store, shard ShardKey, recordSet RecordSetName, records []Record, rev uint64) {
	t.Helper()
	ls, err := store.getLocalShard(testNS, shard)
	if err != nil {
		t.Fatal(err)
	}
	rs := ls.recordSet(recordSet, time.Now())
	if !rs.install(records, rev, CommitCertificate{}, time.Now()) {
		t.Fatalf("install local snapshot %s/%s rev %d failed", shard, recordSet, rev)
	}
}

type readyMap struct {
	values map[MemberID]bool
}

func newReadyMap() *readyMap {
	return &readyMap{values: map[MemberID]bool{}}
}

func (r *readyMap) set(member MemberID, ready bool) {
	r.values[member] = ready
}

func (r *readyMap) Ready(_ string, member MemberID) bool {
	ready, ok := r.values[member]
	if !ok {
		return true
	}
	return ready
}
