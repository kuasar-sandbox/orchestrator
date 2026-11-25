package nodelink

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
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

func TestBatchGroupSnapshotBudgetsAndOrder(t *testing.T) {
	for _, test := range []struct {
		name    string
		groups  int
		entries int
		want    []int
	}{
		{name: "rows", groups: 129, want: []int{128, 1}},
		{name: "entries", groups: 65, entries: 2, want: []int{64, 1}},
		{name: "full entry record", groups: 2, entries: 128, want: []int{1, 1}},
		{name: "empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			groups := make([]nodegroup.Support, test.groups)
			for i := range groups {
				groups[i] = nodegroup.Support{Group: "/g/" + strconv.Itoa(i), Version: 1, Entries: make([]nodegroup.Entry, test.entries)}
			}
			var lengths []int
			next := 0
			err := batchGroupSnapshot(groups, func(offset int, batch []nodegroup.Support) error {
				if offset != next || !reflect.DeepEqual(batch, groups[offset:offset+len(batch)]) {
					t.Fatalf("batch reordered or skipped records at offset %d, want %d", offset, next)
				}
				next += len(batch)
				lengths = append(lengths, len(batch))
				return nil
			})
			if err != nil || next != len(groups) || !reflect.DeepEqual(lengths, test.want) {
				t.Fatalf("batches=%v, consumed=%d, error=%v; want %v", lengths, next, err, test.want)
			}
		})
	}
}

func TestBatchGroupSnapshotExactJSONByteBudget(t *testing.T) {
	// These encoder fixtures deliberately use long names to reach an exact
	// array byte boundary independently of other message validation budgets.
	groups := []nodegroup.Support{
		{Group: "/<>&\"\\\n\u2028中", Version: 1},
		{Group: "/second", Version: 2, Entries: []nodegroup.Entry{}},
	}
	raw, err := json.Marshal(groups)
	if err != nil {
		t.Fatal(err)
	}
	groups[1].Group += strings.Repeat("x", (256<<10)-len(raw))
	raw, err = json.Marshal(groups)
	if err != nil || len(raw) != 256<<10 {
		t.Fatalf("fixture size=%d, error=%v", len(raw), err)
	}
	check := func(want []int) {
		t.Helper()
		var lengths []int
		if err := batchGroupSnapshot(groups, func(_ int, batch []nodegroup.Support) error {
			raw, err := json.Marshal(batch)
			if err != nil || len(raw) > 256<<10 {
				t.Fatalf("oversized emitted array: size=%d, error=%v", len(raw), err)
			}
			lengths = append(lengths, len(batch))
			return nil
		}); err != nil || !reflect.DeepEqual(lengths, want) {
			t.Fatalf("batches=%v, error=%v; want %v", lengths, err, want)
		}
	}
	check([]int{2}) // Exactly 256 KiB is allowed.
	groups[1].Group += "x"
	check([]int{1, 1}) // The array comma and JSON escaping count toward the cap.
}

func TestBatchGroupSnapshotErrors(t *testing.T) {
	for name, oversized := range map[string]nodegroup.Support{
		"entries": {Group: "/large", Entries: make([]nodegroup.Entry, 129)},
		"bytes":   {Group: strings.Repeat("x", 256<<10)},
	} {
		t.Run(name, func(t *testing.T) {
			for _, prefix := range []int{0, 1, 128} {
				groups := make([]nodegroup.Support, prefix, prefix+1)
				for i := range groups {
					groups[i] = nodegroup.Support{Group: "/small/" + strconv.Itoa(i), Version: 1}
				}
				groups = append(groups, oversized)
				sent := 0
				err := batchGroupSnapshot(groups, func(offset int, batch []nodegroup.Support) error {
					if offset != 0 || len(batch) != prefix {
						t.Fatalf("oversized record was sent: offset=%d, rows=%d", offset, len(batch))
					}
					sent += len(batch)
					return nil
				})
				if !errors.Is(err, nodegroup.ErrTooLarge) || sent != prefix {
					t.Fatalf("error=%v, sent=%d; want ErrTooLarge after %d rows", err, sent, prefix)
				}
			}
		})
	}
	wantErr := errors.New("send stopped")
	calls := 0
	err := batchGroupSnapshot(make([]nodegroup.Support, 129), func(int, []nodegroup.Support) error {
		calls++
		return wantErr
	})
	if !errors.Is(err, wantErr) || calls != 1 {
		t.Fatalf("send failure: error=%v, calls=%d", err, calls)
	}
}
