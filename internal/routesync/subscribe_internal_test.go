package routesync

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

type bookmarkSink struct{}

func (bookmarkSink) BeginSync()              {}
func (bookmarkSink) ApplyUpsert(RouteEntry)  {}
func (bookmarkSink) ApplyDelete(RouteDelete) {}
func (bookmarkSink) Bookmark(bool)           {}
func (bookmarkSink) SetPolicy(Policy)        {}

func TestSubscriberReusesLatestBookmarkToken(t *testing.T) {
	subscriber := NewSubscriber(
		nil, "proxy", Register{Subscribe: &Subscribe{Kind: KindRoute}, ResumeFrom: "stale"},
		bookmarkSink{}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	subscriber.apply(&Msg{Type: TypeBookmark, RevToken: "source:42", FullSync: true})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var wire bytes.Buffer
	if err := subscriber.writeUp(ctx, &wire); !errors.Is(err, context.Canceled) {
		t.Fatalf("write registration: %v", err)
	}
	message, err := ReadMsg(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if message.Register == nil || message.Register.ResumeFrom != "source:42" {
		t.Fatalf("reconnect registration = %+v", message.Register)
	}

	// An authority without resumable history sends an empty token after its
	// fallback full sync. Do not retry an older configured token forever.
	subscriber.apply(&Msg{Type: TypeBookmark, FullSync: true})
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	wire.Reset()
	if err := subscriber.writeUp(ctx, &wire); !errors.Is(err, context.Canceled) {
		t.Fatalf("write fallback registration: %v", err)
	}
	message, err = ReadMsg(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if message.Register == nil || message.Register.ResumeFrom != "" {
		t.Fatalf("fallback registration retained a stale token: %+v", message.Register)
	}
}
