package orch

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestRouteReplayUsesFingerprintToken(t *testing.T) {
	o := &Orchestrator{
		routeFP: "fp-test",
		subs:    map[int]chan routesync.Event{},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	o.publish(routesync.Event{Kind: routesync.TypeUpsert, Route: routesync.RouteEntry{SandboxID: "s1"}})
	token := o.CurrentRevToken()
	if token != "fp-test:1" {
		t.Fatalf("token=%q, want fp-test:1", token)
	}
	o.publish(routesync.Event{Kind: routesync.TypeDelete, Delete: routesync.RouteDelete{SandboxID: "s1"}})

	var got []routesync.Event
	if err := o.Replay(context.Background(), 1, func(ev routesync.Event) error {
		got = append(got, ev)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != routesync.TypeDelete || got[0].Delete.SandboxID != "s1" ||
		got[0].Delete.AuthorityRevision != 2 {
		t.Fatalf("replay=%+v, want delete s1", got)
	}
	if fp := o.SourceFingerprint(); fp != "fp-test" {
		t.Fatalf("fingerprint=%q", fp)
	}
}

func TestConcurrentRoutePublishPreservesRevisionOrder(t *testing.T) {
	const count = 512
	o := &Orchestrator{
		routeFP: "fp-test",
		subs:    map[int]chan routesync.Event{},
		log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	events, cancel := o.Subscribe()
	defer cancel()

	var publishers sync.WaitGroup
	publishers.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer publishers.Done()
			o.publish(routesync.Event{
				Kind:  routesync.TypeUpsert,
				Route: routesync.RouteEntry{SandboxID: "route", State: routesync.StateRunning},
			})
		}()
	}
	publishers.Wait()
	for want := uint64(1); want <= count; want++ {
		event := <-events
		if event.Route.AuthorityRevision != want {
			t.Fatalf("delivered authority revision %d, want %d", event.Route.AuthorityRevision, want)
		}
	}
}
