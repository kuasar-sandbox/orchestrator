package routesync_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/kuasar-sandbox/sandbox-orchestrator/internal/routesync"
)

// fakeSink records what the proxy side receives.
type fakeSink struct {
	begin chan struct{}
	up    chan routesync.RouteEntry
	del   chan string
	book  chan struct{}
	pol   chan routesync.Policy
}

func newFakeSink() *fakeSink {
	return &fakeSink{
		begin: make(chan struct{}, 4),
		up:    make(chan routesync.RouteEntry, 4),
		del:   make(chan string, 4),
		book:  make(chan struct{}, 4),
		pol:   make(chan routesync.Policy, 4),
	}
}

func (f *fakeSink) BeginSync()                         { f.begin <- struct{}{} }
func (f *fakeSink) ApplyUpsert(r routesync.RouteEntry) { f.up <- r }
func (f *fakeSink) ApplyDelete(sid string)             { f.del <- sid }
func (f *fakeSink) Bookmark()                          { f.book <- struct{}{} }
func (f *fakeSink) SetPolicy(p routesync.Policy)       { f.pol <- p }

type fakeWakes struct{ ch chan string }

func (f *fakeWakes) NextWake(ctx context.Context) (string, bool) {
	select {
	case s := <-f.ch:
		return s, true
	case <-ctx.Done():
		return "", false
	}
}

// fakeSource is the orchestrator side.
type fakeSource struct {
	sub  chan routesync.Event
	woke chan string
	pol  routesync.Policy
}

func (s *fakeSource) Range(ctx context.Context, fn func(routesync.RouteEntry) error) error {
	return fn(routesync.RouteEntry{SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning})
}
func (s *fakeSource) Subscribe() (<-chan routesync.Event, func()) { return s.sub, func() {} }
func (s *fakeSource) OnWake(ctx context.Context, sid string)      { s.woke <- sid }
func (s *fakeSource) Policy() routesync.Policy                    { return s.pol }

func recv[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for %s", what)
		var zero T
		return zero
	}
}

// TestRouteSyncRoundtrip drives the real bidi h2c protocol end to end: handshake +
// policy + initial route stream + bookmark + a delta downstream, and a wake upstream.
func TestRouteSyncRoundtrip(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sock := filepath.Join(t.TempDir(), "p.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	sink := newFakeSink()
	wakes := &fakeWakes{ch: make(chan string, 1)}
	srv := routesync.NewServer(sink, wakes, log)
	httpSrv := &http.Server{Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(routesync.SyncHeader) == "1" {
			srv.ServeSync(w, r)
			return
		}
		http.Error(w, "unexpected", http.StatusNotFound)
	}), &http2.Server{})}
	go httpSrv.Serve(ln)
	defer httpSrv.Close()

	src := &fakeSource{
		sub:  make(chan routesync.Event, 4),
		woke: make(chan string, 4),
		pol:  routesync.Policy{Domain: "d", AuthMode: "enforce", ParkTimeoutMS: 1234},
	}
	cl := routesync.NewClient([]string{sock}, src, log)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cl.Run(ctx)

	// Handshake policy, then the initial route set streams as upsert(s1) + bookmark.
	if p := recv(t, sink.pol, "policy"); p.AuthMode != "enforce" || p.ParkTimeoutMS != 1234 {
		t.Fatalf("policy = %+v", p)
	}
	recv(t, sink.begin, "begin-sync")
	if r := recv(t, sink.up, "initial upsert"); r.SandboxID != "s1" {
		t.Fatalf("initial upsert = %+v", r)
	}
	recv(t, sink.book, "bookmark")

	// A delta published by the source is delivered as an upsert.
	src.sub <- routesync.Event{Kind: routesync.TypeUpsert, Route: routesync.RouteEntry{SandboxID: "s2", State: routesync.StateRunning}}
	if up := recv(t, sink.up, "upsert"); up.SandboxID != "s2" {
		t.Fatalf("upsert = %+v", up)
	}

	// A delete too.
	src.sub <- routesync.Event{Kind: routesync.TypeDelete, SID: "s1"}
	if del := recv(t, sink.del, "delete"); del != "s1" {
		t.Fatalf("delete = %q", del)
	}

	// A wake from the proxy reaches the source's OnWake.
	wakes.ch <- "s9"
	if got := recv(t, src.woke, "wake"); got != "s9" {
		t.Fatalf("wake = %q", got)
	}
}
