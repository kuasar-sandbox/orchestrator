package routesync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type snapshotChanges struct {
	Sink
	applied int
}

func (s *snapshotChanges) ApplyUpsert(RouteEntry) error { s.applied++; return nil }
func (s *snapshotChanges) ApplyDelete(string)           { s.applied++ }
func (s *snapshotChanges) Bookmark()                    { s.applied++ }

func TestIncompleteOrOversizedSnapshotDoesNotPublishPendingRoutes(t *testing.T) {
	for _, overflow := range []bool{false, true} {
		sink := new(snapshotChanges)
		sub := Subscriber{sink: sink}
		var stream bytes.Buffer
		add := func(id string) {
			t.Helper()
			if err := WriteMsg(&stream, &Msg{Type: TypeUpsert, Route: &RouteEntry{SandboxID: id, State: StatePaused}}); err != nil {
				t.Fatal(err)
			}
		}
		add("reset-B")
		if overflow {
			add("new-C")
		}
		err := sub.reconcileSnapshot(context.Background(), &stream, nil, []string{"old-A"}, 1)
		if err == nil || (overflow && !strings.Contains(err.Error(), "exceeds route capacity")) || (!overflow && !errors.Is(err, io.EOF)) {
			t.Fatalf("overflow=%v: unexpected snapshot result %v", overflow, err)
		}
		if sink.applied != 0 {
			t.Fatalf("overflow=%v: published %d changes before snapshot completion", overflow, sink.applied)
		}
	}
}
