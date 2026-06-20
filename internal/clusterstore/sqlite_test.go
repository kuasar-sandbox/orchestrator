package clusterstore

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T, retention int) *sqliteStore {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "reg.db"), retention)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s.(*sqliteStore)
}

func recv(t *testing.T, ch <-chan Event) (Event, bool) {
	t.Helper()
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event")
		return Event{}, false
	}
}

func TestPutGetDelete(t *testing.T) {
	s := openTest(t, 0)
	ctx := context.Background()

	r1, _ := s.Put(ctx, "node/a", []byte("v1"))
	if r1 != 1 {
		t.Fatalf("first put rev=%d, want 1", r1)
	}
	kv, found, _ := s.Get(ctx, "node/a")
	if !found || string(kv.Value) != "v1" || kv.ModRev != 1 {
		t.Fatalf("get=%v found=%v", kv, found)
	}
	r2, _ := s.Put(ctx, "node/a", []byte("v2"))
	if r2 != 2 {
		t.Fatalf("second put rev=%d, want 2", r2)
	}
	r3, _ := s.Delete(ctx, "node/a")
	if r3 != 3 {
		t.Fatalf("delete rev=%d, want 3", r3)
	}
	if _, found, _ := s.Get(ctx, "node/a"); found {
		t.Fatal("key still present after delete")
	}
	// delete of absent key is a no-op (rev 0)
	if r, _ := s.Delete(ctx, "node/a"); r != 0 {
		t.Fatalf("delete absent rev=%d, want 0", r)
	}
}

func TestCAS(t *testing.T) {
	s := openTest(t, 0)
	ctx := context.Background()

	// create-only on absent key
	rev, ok, _ := s.CAS(ctx, "k", 0, []byte("a"))
	if !ok || rev != 1 {
		t.Fatalf("create CAS ok=%v rev=%d", ok, rev)
	}
	// create-only on present key fails
	if _, ok, _ := s.CAS(ctx, "k", 0, []byte("b")); ok {
		t.Fatal("create CAS on existing key should fail")
	}
	// wrong expected rev fails
	if _, ok, _ := s.CAS(ctx, "k", 99, []byte("c")); ok {
		t.Fatal("CAS with stale rev should fail")
	}
	// correct expected rev succeeds
	rev2, ok, _ := s.CAS(ctx, "k", 1, []byte("d"))
	if !ok || rev2 != 2 {
		t.Fatalf("CAS match ok=%v rev=%d", ok, rev2)
	}
	if kv, _, _ := s.Get(ctx, "k"); string(kv.Value) != "d" {
		t.Fatalf("value=%s, want d", kv.Value)
	}
}

func TestRangePrefix(t *testing.T) {
	s := openTest(t, 0)
	ctx := context.Background()
	for _, k := range []string{"sandbox/g1/y", "node/b", "node/a", "sandbox/g1/x"} {
		s.Put(ctx, k, []byte("v"))
	}
	var got []string
	s.Range(ctx, "node/", func(kv KV) error { got = append(got, kv.Key); return nil })
	if len(got) != 2 || got[0] != "node/a" || got[1] != "node/b" {
		t.Fatalf("node/ range = %v, want [node/a node/b]", got)
	}
	got = nil
	s.Range(ctx, "sandbox/g1/", func(kv KV) error { got = append(got, kv.Key); return nil })
	if len(got) != 2 || got[0] != "sandbox/g1/x" {
		t.Fatalf("sandbox/g1/ range = %v", got)
	}
	got = nil
	s.Range(ctx, "", func(kv KV) error { got = append(got, kv.Key); return nil })
	if len(got) != 4 {
		t.Fatalf("full range len=%d, want 4", len(got))
	}
}

func TestWatchLiveAndCatchup(t *testing.T) {
	s := openTest(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Put(ctx, "k/1", []byte("a")) // rev1
	s.Put(ctx, "k/2", []byte("b")) // rev2
	s.Put(ctx, "k/3", []byte("c")) // rev3

	// catch-up from rev1 → replay rev2, rev3, then live rev4
	ch, err := s.Watch(ctx, "k/", 1)
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	if ev, _ := recv(t, ch); ev.Rev != 2 || ev.Key != "k/2" || ev.Type != EventPut {
		t.Fatalf("replay[0]=%+v, want put k/2 rev2", ev)
	}
	if ev, _ := recv(t, ch); ev.Rev != 3 || ev.Key != "k/3" {
		t.Fatalf("replay[1]=%+v, want k/3 rev3", ev)
	}
	s.Delete(ctx, "k/1") // rev4 (live)
	if ev, _ := recv(t, ch); ev.Rev != 4 || ev.Type != EventDelete || ev.Key != "k/1" {
		t.Fatalf("live=%+v, want delete k/1 rev4", ev)
	}

	// a watch on a different prefix must not see k/ events
	other, _ := s.Watch(ctx, "node/", 0)
	s.Put(ctx, "k/9", []byte("z")) // not under node/
	s.Put(ctx, "node/x", []byte("n"))
	if ev, _ := recv(t, other); ev.Key != "node/x" {
		t.Fatalf("prefix isolation broken: got %+v", ev)
	}
}

func TestWatchCompacted(t *testing.T) {
	s := openTest(t, 3) // retain only 3 changelog entries
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		s.Put(ctx, "k/x", []byte("v"))
	}
	// rev is 10; retention 3 → revs <= 7 pruned. Watching from rev1 must fail.
	if _, err := s.Watch(ctx, "k/", 1); err != ErrCompacted {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
	// watching from a retained rev works
	if _, err := s.Watch(ctx, "k/", 8); err != nil {
		t.Fatalf("watch from retained rev: %v", err)
	}
}

func TestLeaseRevokeAndExpiry(t *testing.T) {
	s := openTest(t, 0)
	ctx := context.Background()

	// Revoke deletes all leased keys.
	l1, _ := s.Grant(ctx, 100)
	s.PutLeased(ctx, "node/a", []byte("a"), l1)
	s.PutLeased(ctx, "node/b", []byte("b"), l1)
	if err := s.Revoke(ctx, l1); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, found, _ := s.Get(ctx, "node/a"); found {
		t.Fatal("leased key survived revoke")
	}

	// Expiry: drive the clock forward and sweep.
	fixed := int64(1000)
	s.now = func() int64 { return fixed }
	l2, _ := s.Grant(ctx, 30) // expires at 1030
	s.PutLeased(ctx, "node/c", []byte("c"), l2)
	watch, _ := s.Watch(ctx, "node/", 0)
	fixed = 1031 // past expiry
	s.sweepExpired()
	if ev, _ := recv(t, watch); ev.Type != EventDelete || ev.Key != "node/c" {
		t.Fatalf("expiry event=%+v, want delete node/c", ev)
	}
	if _, found, _ := s.Get(ctx, "node/c"); found {
		t.Fatal("expired leased key survived sweep")
	}
	// KeepAlive on a revoked/expired lease errors.
	if err := s.KeepAlive(ctx, l2, 30); err == nil {
		t.Fatal("KeepAlive on gone lease should error")
	}
}
