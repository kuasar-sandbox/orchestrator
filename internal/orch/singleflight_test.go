package orch

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// TestFlightGroupCollapses proves the resume dedupe: while one call for a key is
// in flight, concurrent calls for the same key collapse onto it (fn runs once).
func TestFlightGroupCollapses(t *testing.T) {
	var g flightGroup
	var calls atomic.Int64
	inDo := make(chan struct{})
	release := make(chan struct{})

	go g.Do("k", func() error {
		calls.Add(1)
		close(inDo)
		<-release
		return nil
	})
	<-inDo // first call is now inside fn (holding the key)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = g.Do("k", func() error { calls.Add(1); return nil })
		}()
	}
	time.Sleep(80 * time.Millisecond) // let followers enqueue as waiters
	close(release)
	wg.Wait()

	if n := calls.Load(); n != 1 {
		t.Fatalf("fn ran %d times, want 1 (single-flight collapse failed)", n)
	}
}

// TestFlightGroupDistinctKeys: different keys do not collapse.
func TestFlightGroupDistinctKeys(t *testing.T) {
	var g flightGroup
	var calls atomic.Int64
	var wg sync.WaitGroup
	for _, k := range []string{"a", "b", "c"} {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			_ = g.Do(k, func() error { calls.Add(1); return nil })
		}(k)
	}
	wg.Wait()
	if n := calls.Load(); n != 3 {
		t.Fatalf("fn ran %d times, want 3 (distinct keys must not collapse)", n)
	}
}

// TestPublishToSubscriber: an upsert event reaches a live subscriber; a lagging
// subscriber is dropped + closed (so its routesync client reconnects/resnapshots).
func TestPublishToSubscriber(t *testing.T) {
	o := &Orchestrator{subs: map[int]chan routesync.Event{}}
	ch, cancel := o.Subscribe()
	defer cancel()
	o.publish(routesync.Event{Kind: routesync.TypeDelete, SID: "x"})
	select {
	case ev := <-ch:
		if ev.SID != "x" {
			t.Fatalf("event = %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive event")
	}
}
