package clusterstore

import (
	"context"
	"testing"
	"time"
)

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

func TestMemoryStoreBasicAndWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := OpenMemory(10)
	defer s.Close()

	if rev, err := s.Put(ctx, "node/a", []byte("a")); err != nil || rev != 1 {
		t.Fatalf("put rev=%d err=%v", rev, err)
	}
	if _, ok, err := s.CAS(ctx, "node/a", 0, []byte("x")); err != nil || ok {
		t.Fatalf("create CAS over existing ok=%v err=%v", ok, err)
	}
	if rev, ok, err := s.CAS(ctx, "node/a", 1, []byte("b")); err != nil || !ok || rev != 2 {
		t.Fatalf("CAS rev=%d ok=%v err=%v", rev, ok, err)
	}
	kv, found, err := s.Get(ctx, "node/a")
	if err != nil || !found || string(kv.Value) != "b" || kv.ModRev != 2 {
		t.Fatalf("get=%+v found=%v err=%v", kv, found, err)
	}

	ch, err := s.Watch(ctx, "node/", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ev, _ := recv(t, ch); ev.Rev != 2 || ev.Key != "node/a" || string(ev.Value) != "b" {
		t.Fatalf("replay=%+v", ev)
	}
	s.Delete(ctx, "node/a")
	if ev, _ := recv(t, ch); ev.Type != EventDelete || ev.Key != "node/a" {
		t.Fatalf("delete event=%+v", ev)
	}
}

func TestMemoryStoreLeaseRevoke(t *testing.T) {
	ctx := context.Background()
	s := OpenMemory(10)
	defer s.Close()
	lease, err := s.Grant(ctx, 60)
	if err != nil {
		t.Fatal(err)
	}
	s.PutLeased(ctx, "k/a", []byte("a"), lease)
	s.PutLeased(ctx, "k/b", []byte("b"), lease)
	if err := s.Revoke(ctx, lease); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := s.Get(ctx, "k/a"); found {
		t.Fatal("leased key survived revoke")
	}
	if err := s.KeepAlive(ctx, lease, 60); err == nil {
		t.Fatal("KeepAlive on revoked lease should fail")
	}
}
