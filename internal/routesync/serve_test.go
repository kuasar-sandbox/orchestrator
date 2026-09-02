package routesync

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"
)

func TestStreamAuthorityAllowsNilLoggerAtReadEnd(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		StreamAuthority(
			context.Background(),
			io.Discard,
			func() {},
			bytes.NewReader(nil),
			nil,
			Register{},
			nil,
			nil,
			nil,
		)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("authority did not stop after the reader reached EOF")
	}
}

type buildSnapshotSource struct {
	routeEvents chan Event
	buildEvents chan BuildEvent
	subscribed  bool
}

func (s *buildSnapshotSource) Range(context.Context, func(RouteEntry) error) error { return nil }
func (s *buildSnapshotSource) Subscribe() (<-chan Event, func()) {
	return s.routeEvents, func() {}
}
func (*buildSnapshotSource) OnWake(context.Context, string) {}
func (*buildSnapshotSource) Policy() Policy                 { return Policy{} }
func (s *buildSnapshotSource) SubscribeBuilds() (<-chan BuildEvent, func()) {
	s.subscribed = true
	return s.buildEvents, func() {}
}
func (s *buildSnapshotSource) RangeBuilds(ctx context.Context, fn func(BuildEvent) error) error {
	if !s.subscribed {
		return io.ErrUnexpectedEOF
	}
	if err := fn(BuildEvent{BuildID: "registered", State: "registered"}); err != nil {
		return err
	}
	// This deletion races the snapshot. Because SubscribeBuilds was installed
	// first, it must be delivered after the complete snapshot generation.
	s.buildEvents <- BuildEvent{Kind: BuildDelete, BuildID: "registered"}
	return fn(BuildEvent{BuildID: "ready", State: "ready", TemplateID: "e2b:img:manifest://ready"})
}

func TestStreamAuthorityBuildSnapshotAndRacingDeleteAreOrdered(t *testing.T) {
	source := &buildSnapshotSource{
		routeEvents: make(chan Event),
		buildEvents: make(chan BuildEvent, 1),
	}
	downReader, downWriter := io.Pipe()
	upReader, upWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer upWriter.Close()
	go StreamAuthority(ctx, downWriter, func() {}, upReader, source,
		Register{Subscribe: &Subscribe{Kind: KindRegistry}}, nil, nil, nil)

	wantTypes := []string{
		TypeBuildSyncBegin,
		TypeBuildUpsert,
		TypeBuildUpsert,
		TypeBuildSyncEnd,
		TypeBookmark,
		TypeBuildDelete,
	}
	var got []*Msg
	for range wantTypes {
		got = append(got, readTestMessage(t, downReader))
	}
	for index, want := range wantTypes {
		if got[index].Type != want {
			t.Fatalf("message %d type = %q, want %q; stream=%+v", index, got[index].Type, want, got)
		}
	}
	if got[1].Build == nil || got[1].Build.BuildID != "registered" {
		t.Fatalf("registered snapshot = %+v", got[1])
	}
	if got[2].Build == nil || got[2].Build.BuildID != "ready" || got[2].Build.TemplateID == "" {
		t.Fatalf("ready snapshot = %+v", got[2])
	}
	if got[5].Build == nil || got[5].Build.BuildID != "registered" {
		t.Fatalf("racing deletion = %+v", got[5])
	}
}
