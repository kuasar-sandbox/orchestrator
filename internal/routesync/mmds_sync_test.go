package routesync_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

// fakeSink's MmdsSink implementation.
func (f *fakeSink) BeginMmdsSync(generation string)               { f.mmdsBegin <- generation }
func (f *fakeSink) ApplyMmdsUpsert(e routesync.MmdsEndpointEntry) { f.mmdsUp <- e }
func (f *fakeSink) ApplyMmdsDelete(k routesync.MmdsEndpointKey)   { f.mmdsDel <- k }
func (f *fakeSink) MmdsBookmark(generation string)                { f.mmdsBook <- generation }

// fakeSource's MmdsSource implementation.
func (s *fakeSource) MmdsGeneration() string { return s.mmdsGen }
func (s *fakeSource) RangeMmds(ctx context.Context, fn func(routesync.MmdsEndpointEntry) error) error {
	for _, e := range s.mmdsEntries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}
func (s *fakeSource) SubscribeMmds() (<-chan routesync.MmdsEvent, func()) {
	return s.mmdsSub, func() {}
}

// mmdsTestHarness spins up a real h2c server (routesync.ServeStream) over a
// UDS and a real routesync.Subscriber dialing it — the same round-trip style
// as TestRouteSyncRoundtrip, extended with MMDS capability negotiation.
func mmdsTestHarness(t *testing.T, src *fakeSource, mmdsEndpoints bool) (*fakeSink, *fakeSource) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sock := filepath.Join(t.TempDir(), "cfg.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	httpSrv := &http.Server{Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reg, err := routesync.ReadRegister(r.Body)
		if err != nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		routesync.ServeStream(r.Context(), w, r.Body, src, reg, log)
	}), &http2.Server{})}
	go httpSrv.Serve(ln)
	t.Cleanup(func() { httpSrv.Close() })

	sink := newFakeSink()
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}
	reg := routesync.Register{
		Subscribe:     &routesync.Subscribe{Kind: routesync.KindRoute},
		MmdsEndpoints: mmdsEndpoints,
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var mmdsSink routesync.MmdsSink
	if mmdsEndpoints {
		mmdsSink = sink
	}
	go routesync.NewSubscriber(dial, "px-mmds", reg, sink, mmdsSink, nil, log).Run(ctx)
	return sink, src
}

func TestMmdsSyncFullGenerationStream(t *testing.T) {
	src := &fakeSource{
		sub:     make(chan routesync.Event, 4),
		woke:    make(chan string, 4),
		mmdsGen: "gen-1",
		mmdsEntries: []routesync.MmdsEndpointEntry{
			{SandboxID: "s1", Name: "creds", Path: "/latest/meta-data/credentials", BackendType: "relay", Revision: 1, ValuePresent: true, SecretPlaintext: "auth-value"},
			{SandboxID: "s1", Name: "user-data", Path: "/latest/user-data", BackendType: "store", Revision: 0, ValuePresent: false},
		},
		mmdsSub: make(chan routesync.MmdsEvent, 4),
	}
	sink, _ := mmdsTestHarness(t, src, true)

	// Route sync happens first (unrelated to MMDS): drain it before asserting
	// on MMDS frames, since both streams share one connection/writer.
	recv(t, sink.begin, "begin-sync")
	recv(t, sink.up, "initial route upsert")
	recv(t, sink.book, "route bookmark")

	gen := recv(t, sink.mmdsBegin, "mmds sync begin")
	if gen != "gen-1" {
		t.Fatalf("mmds sync begin generation = %q, want gen-1", gen)
	}
	e1 := recv(t, sink.mmdsUp, "mmds upsert 1")
	e2 := recv(t, sink.mmdsUp, "mmds upsert 2")
	got := map[string]routesync.MmdsEndpointEntry{e1.Name: e1, e2.Name: e2}
	if got["creds"].BackendType != "relay" || got["creds"].SecretPlaintext != "auth-value" {
		t.Fatalf("creds entry = %+v", got["creds"])
	}
	if got["user-data"].BackendType != "store" || got["user-data"].ValuePresent {
		t.Fatalf("user-data entry = %+v", got["user-data"])
	}
	bookGen := recv(t, sink.mmdsBook, "mmds bookmark")
	if bookGen != "gen-1" {
		t.Fatalf("mmds bookmark generation = %q, want gen-1", bookGen)
	}
}

func TestMmdsSyncLiveUpsertAndDelete(t *testing.T) {
	src := &fakeSource{
		sub:     make(chan routesync.Event, 4),
		woke:    make(chan string, 4),
		mmdsGen: "gen-1",
		mmdsSub: make(chan routesync.MmdsEvent, 4),
	}
	sink, _ := mmdsTestHarness(t, src, true)

	recv(t, sink.begin, "begin-sync")
	recv(t, sink.up, "initial route upsert")
	recv(t, sink.book, "route bookmark")
	recv(t, sink.mmdsBegin, "mmds sync begin")
	recv(t, sink.mmdsBook, "mmds bookmark") // no entries this time

	src.mmdsSub <- routesync.MmdsEvent{
		Kind:  routesync.TypeMmdsUpsert,
		Entry: routesync.MmdsEndpointEntry{SandboxID: "s2", Name: "a", Revision: 1, ValuePresent: true, SecretPlaintext: "v"},
	}
	up := recv(t, sink.mmdsUp, "live mmds upsert")
	if up.SandboxID != "s2" || up.Name != "a" || up.SecretPlaintext != "v" {
		t.Fatalf("live upsert = %+v", up)
	}

	src.mmdsSub <- routesync.MmdsEvent{Kind: routesync.TypeMmdsDelete, Key: routesync.MmdsEndpointKey{SandboxID: "s2", Name: "a"}}
	del := recv(t, sink.mmdsDel, "live mmds delete")
	if del.SandboxID != "s2" || del.Name != "a" {
		t.Fatalf("live delete = %+v", del)
	}
}

// TestMmdsSyncCapabilityNegotiation: a subscriber that does NOT declare
// Register.MmdsEndpoints receives no MMDS frames at all, even though the
// source implements MmdsSource and has entries to offer — there is no
// downgrade path, it simply keeps configurable endpoints unavailable for a
// subscriber that never asked.
func TestMmdsSyncCapabilityNegotiation(t *testing.T) {
	src := &fakeSource{
		sub:         make(chan routesync.Event, 4),
		woke:        make(chan string, 4),
		mmdsGen:     "gen-1",
		mmdsEntries: []routesync.MmdsEndpointEntry{{SandboxID: "s1", Name: "a", Revision: 1, ValuePresent: true}},
		mmdsSub:     make(chan routesync.MmdsEvent, 4),
	}
	sink, _ := mmdsTestHarness(t, src, false) // MmdsEndpoints: false

	recv(t, sink.begin, "begin-sync")
	recv(t, sink.up, "initial route upsert")
	recv(t, sink.book, "route bookmark")

	// A live route delta proves the connection is still alive and the
	// reader loop is running normally — if it arrives, and no MMDS frame
	// ever does, capability negotiation is working.
	src.sub <- routesync.Event{Kind: routesync.TypeUpsert, Route: routesync.RouteEntry{SandboxID: "s3", State: routesync.StateRunning}}
	recv(t, sink.up, "second route upsert")

	select {
	case gen := <-sink.mmdsBegin:
		t.Fatalf("received an mmds_sync_begin frame (generation %q) despite MmdsEndpoints=false", gen)
	default:
	}
}
