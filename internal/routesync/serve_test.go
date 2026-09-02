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

type ackDuringBuildSnapshotSource struct {
	routeEvents      chan Event
	buildEvents      chan BuildEvent
	outbox           chan<- *Msg
	continueSnapshot <-chan struct{}
}

func (s *ackDuringBuildSnapshotSource) Range(context.Context, func(RouteEntry) error) error {
	return nil
}

func (s *ackDuringBuildSnapshotSource) Subscribe() (<-chan Event, func()) {
	return s.routeEvents, func() {}
}

func (*ackDuringBuildSnapshotSource) OnWake(context.Context, string) {}
func (*ackDuringBuildSnapshotSource) Policy() Policy                 { return Policy{} }

func (s *ackDuringBuildSnapshotSource) SubscribeBuilds() (<-chan BuildEvent, func()) {
	return s.buildEvents, func() {}
}

func (s *ackDuringBuildSnapshotSource) RangeBuilds(ctx context.Context, fn func(BuildEvent) error) error {
	// Model a command that completes while a retained Build range is still in
	// progress. The ACK must use the same stream writer before BuildSyncEnd.
	s.outbox <- &Msg{Type: TypeCmdAck, Ack: &CmdAck{CmdID: "during-snapshot", Status: AckAccepted}}
	if err := fn(BuildEvent{BuildID: "first", State: "ready"}); err != nil {
		return err
	}
	select {
	case <-s.continueSnapshot:
	case <-ctx.Done():
		return ctx.Err()
	}
	return fn(BuildEvent{BuildID: "second", State: "ready"})
}

func TestStreamAuthorityDrainsCommandAckDuringBuildSnapshot(t *testing.T) {
	outbox := make(chan *Msg, 1)
	continueSnapshot := make(chan struct{})
	source := &ackDuringBuildSnapshotSource{
		routeEvents: make(chan Event), buildEvents: make(chan BuildEvent),
		outbox: outbox, continueSnapshot: continueSnapshot,
	}
	downReader, downWriter := io.Pipe()
	upReader, upWriter := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer upWriter.Close()
	go StreamAuthority(ctx, downWriter, func() {}, upReader, source,
		Register{Subscribe: &Subscribe{Kind: KindRegistry}}, nil, outbox, nil)

	begin := readTestMessage(t, downReader)
	first := readTestMessage(t, downReader)
	ack := readTestMessage(t, downReader)
	if begin.Type != TypeBuildSyncBegin || first.Type != TypeBuildUpsert || first.Build == nil || first.Build.BuildID != "first" {
		t.Fatalf("snapshot prefix = begin %+v, first %+v", begin, first)
	}
	if ack.Type != TypeCmdAck || ack.Ack == nil || ack.Ack.CmdID != "during-snapshot" {
		t.Fatalf("interleaved ACK = %+v", ack)
	}

	close(continueSnapshot)
	wantTypes := []string{TypeBuildUpsert, TypeBuildSyncEnd, TypeBookmark}
	for index, want := range wantTypes {
		if got := readTestMessage(t, downReader); got.Type != want {
			t.Fatalf("snapshot suffix message %d type = %q, want %q", index, got.Type, want)
		}
	}
}
