package nodelink

import (
	"context"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/nodegroup"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

const testNodeTemplate = "e2b-img-bWFuaWZlc3Q6Ly9hYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFh"

func (n *fakeNode) GroupSnapshot(context.Context) (int64, []nodegroup.Support, error) {
	return 1, []nodegroup.Support{{Group: "/cell/proj/app/g1", Version: 1, Entries: []nodegroup.Entry{{AuthRef: testFingerprint(testAPISecret), AuthMaterial: nodegroup.AuthMaterial{APISecret: testAPISecret}}}, DefaultContext: &nodegroup.DefaultContext{TemplateID: testNodeTemplate}}}, nil
}
func (n *fakeNode) SubscribeGroups() (<-chan routesync.GroupEvent, func()) {
	return make(chan routesync.GroupEvent), func() {}
}

type groupTestSource struct {
	events   chan routesync.GroupEvent
	snapshot func() (int64, []nodegroup.Support, error)
}

func (s *groupTestSource) SubscribeGroups() (<-chan routesync.GroupEvent, func()) {
	return s.events, func() {}
}
func (s *groupTestSource) GroupSnapshot(context.Context) (int64, []nodegroup.Support, error) {
	return s.snapshot()
}

func TestGroupStreamSnapshotBoundaryAndGap(t *testing.T) {
	source := &groupTestSource{events: make(chan routesync.GroupEvent, 4)}
	source.snapshot = func() (int64, []nodegroup.Support, error) {
		source.events <- routesync.GroupEvent{Sequence: 4, Deleted: "already-in-snapshot"}
		source.events <- routesync.GroupEvent{Sequence: 5, Deleted: "later"}
		return 4, []nodegroup.Support{{Group: "g", Version: 4}}, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := make(chan *routesync.Msg, 8)
	done := make(chan error, 1)
	go func() { done <- streamGroups(ctx, source, out) }()
	for _, kind := range []string{routesync.TypeGroupBegin, routesync.TypeGroupBatch, routesync.TypeGroupEnd, routesync.TypeGroupDelete} {
		select {
		case msg := <-out:
			if msg.Type != kind {
				t.Fatalf("report %s want %s", msg.Type, kind)
			}
		case <-ctx.Done():
			t.Fatal("report stalled")
		}
	}
	source.events <- routesync.GroupEvent{Sequence: 7, Deleted: "gap"}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("gap did not end session")
		}
	case <-ctx.Done():
		t.Fatal("gap was not detected")
	}
}

func TestGroupStreamOverflowRequiresReconnect(t *testing.T) {
	source := &groupTestSource{events: make(chan routesync.GroupEvent), snapshot: func() (int64, []nodegroup.Support, error) { return 0, nil, nil }}
	close(source.events)
	out := make(chan *routesync.Msg, 4)
	if err := streamGroups(context.Background(), source, out); err == nil {
		t.Fatal("closed report source accepted")
	}
}
