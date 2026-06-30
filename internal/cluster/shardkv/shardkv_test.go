package shardkv

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const testNS Namespace = "test"

func TestMaglevResolverRejectsUnknownNamespace(t *testing.T) {
	resolver := newTestResolver(t, "m1", []MemberID{"m1", "m2", "m3"}, 2)
	if _, err := resolver.ResolveShard("missing", "s1"); !errors.Is(err, ErrUnknownNamespace) {
		t.Fatalf("ResolveShard unknown err=%v, want ErrUnknownNamespace", err)
	}
	view, err := resolver.ResolveShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Sets) != 1 || len(view.Sets[0].Members) != 2 || view.Sets[0].Quorum != 2 {
		t.Fatalf("view=%+v, want 2 members quorum 2", view)
	}
}

func TestShardCASGetDeleteSizeOne(t *testing.T) {
	ctx := context.Background()
	store := newSingleStore(t, "m1")
	sh := mustShard(t, store, testNS, "s1")

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
	if got, found, err := sh.Get(ctx, "k1"); err != nil || found {
		t.Fatalf("Get after delete got=%+v found=%v err=%v", got, found, err)
	}
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v2")); err != nil || !ok {
		t.Fatalf("CAS recreate ok=%v err=%v", ok, err)
	}
}

func TestShardRepairsLaggingReplica(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	sh := mustShard(t, cluster["m1"], testNS, "s1")
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS ok=%v err=%v", ok, err)
	}
	lagging, err := cluster["m3"].getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	lagging.mu.Lock()
	delete(lagging.records, "k1")
	lagging.mu.Unlock()

	got, found, err := sh.Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("Get got=%q found=%v err=%v", string(got.Value), found, err)
	}
	repaired := mustShard(t, cluster["m3"], testNS, "s1")
	got, found, err = repaired.Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("lagging replica after repair got=%q found=%v err=%v", string(got.Value), found, err)
	}
}

func TestShardGetRetriesWhenQuorumMissesHigherPromisedRecord(t *testing.T) {
	ctx := context.Background()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, nil)
	ls, err := cluster["m3"].getLocalShard(testNS, "s1")
	if err != nil {
		t.Fatal(err)
	}
	high := Ballot{Round: 10, Writer: "m3"}
	ls.mu.Lock()
	ls.records["k1"] = Record{
		Namespace: testNS, Shard: "s1", Key: "k1", Value: []byte("v-high"),
		Meta: RecordMeta{Ballot: high, Rev: 1, UpdatedAt: time.Now()},
	}
	ls.promised["k1"] = high
	ls.mu.Unlock()

	got, found, err := mustShard(t, cluster["m1"], testNS, "s1").Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v-high" {
		t.Fatalf("Get got=%q found=%v err=%v, want high promised record", string(got.Value), found, err)
	}
	for _, member := range []MemberID{"m1", "m2", "m3"} {
		rec, found, err := mustShard(t, cluster[member], testNS, "s1").Get(ctx, "k1")
		if err != nil || !found || string(rec.Value) != "v-high" {
			t.Fatalf("%s repaired rec=%q found=%v err=%v", member, string(rec.Value), found, err)
		}
	}
}

func TestMemberReadyFailFastDoesNotChangeQuorum(t *testing.T) {
	ctx := context.Background()
	ready := newReadyMap()
	ready.set("m3", false)
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, ready)
	sh := mustShard(t, cluster["m1"], testNS, "s1")
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS with one not-ready member ok=%v err=%v", ok, err)
	}

	ready.set("m2", false)
	if _, _, err := sh.CAS(ctx, "k2", 0, []byte("v2")); !errors.Is(err, ErrQuorum) {
		t.Fatalf("CAS with two not-ready members err=%v, want ErrQuorum", err)
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

	sh := mustShard(t, stores["m1"], testNS, "s-http")
	if _, ok, err := sh.CAS(ctx, "k1", 0, []byte("v1")); err != nil || !ok {
		t.Fatalf("CAS over http ok=%v err=%v", ok, err)
	}
	got, found, err := mustShard(t, stores["m2"], testNS, "s-http").Get(ctx, "k1")
	if err != nil || !found || string(got.Value) != "v1" {
		t.Fatalf("Get over http got=%q found=%v err=%v", string(got.Value), found, err)
	}
}

func TestWatchResetAndIncrementalEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := newSingleStore(t, "m1")
	sh := mustShard(t, store, testNS, "s1")
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
	assertEvent(t, w.Events, EventPut, "k2")
}

func TestCompactTombstoneRequiresReadyMembers(t *testing.T) {
	ctx := context.Background()
	ready := newReadyMap()
	cluster := newTestCluster(t, []MemberID{"m1", "m2", "m3"}, 3, ready)
	sh := mustShard(t, cluster["m1"], testNS, "s1")
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
	sh := mustShard(t, store, testNS, "s1")
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

func assertEvent(t *testing.T, ch <-chan WatchEvent, typ EventType, key RecordKey) {
	t.Helper()
	select {
	case ev := <-ch:
		if ev.Type != typ || ev.Key != key {
			t.Fatalf("event=%+v, want type=%s key=%s", ev, typ, key)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for %s %s", typ, key)
	}
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
	resolver, err := NewMaglevResolver(MaglevResolverConfig{
		Local: local,
		Views: []ClusterView{{Version: 1, Label: "v1", Members: members}},
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

func markTombstoneOld(t *testing.T, store *Store, shard ShardKey, key RecordKey, age time.Duration) {
	t.Helper()
	ls, err := store.getLocalShard(testNS, shard)
	if err != nil {
		t.Fatal(err)
	}
	ls.mu.Lock()
	defer ls.mu.Unlock()
	rec, ok := ls.records[key]
	if !ok || !rec.Deleted {
		t.Fatalf("record %s is not an existing tombstone: %+v", key, rec)
	}
	rec.Meta.UpdatedAt = time.Now().Add(-age)
	ls.records[key] = rec
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
