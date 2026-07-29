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

// runMMDSSecretsGateFixture drives one full h2c registration + stream against
// a source whose initial snapshot AND one live delta both carry
// RouteEntry.MMDSSecrets, for a subscriber registered with the given
// MMDSSecrets capability. It returns what the subscriber's sink actually
// received on its Range-derived initial upsert and the live-delta upsert.
func runMMDSSecretsGateFixture(t *testing.T, subscriberWantsSecrets bool) (initialUpsert, deltaUpsert routesync.RouteEntry) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sock := filepath.Join(t.TempDir(), "cfg.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	src := &fakeSource{
		sub:  make(chan routesync.Event, 4),
		woke: make(chan string, 4),
		pol:  routesync.Policy{AuthMode: "enforce"},
		rangeEntry: routesync.RouteEntry{
			SandboxID: "s1", Profile: "e2b", State: routesync.StateRunning,
			MMDSSecrets: `{"version":1,"values":{"key1":{"body_base64":"aGVsbG8=","content_type":"text/plain"}}}`,
		},
	}
	httpSrv := &http.Server{Handler: h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reg, err := routesync.ReadRegister(r.Body)
		if err != nil {
			http.Error(w, "bad register", http.StatusBadRequest)
			return
		}
		routesync.ServeStream(r.Context(), w, r.Body, src, reg, log)
	}), &http2.Server{})}
	go httpSrv.Serve(ln)
	defer httpSrv.Close()

	sink := newFakeSink()
	wakes := &fakeWakes{ch: make(chan string, 1)}
	dial := func(ctx context.Context) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}
	reg := routesync.Register{
		Subscribe:   &routesync.Subscribe{Kind: routesync.KindRouteWake},
		Proxy:       &routesync.Proxy{Socket: routesync.Socket{Path: "/x"}},
		MMDSSecrets: subscriberWantsSecrets,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go routesync.NewSubscriber(dial, "px0", reg, sink, wakes, log).Run(ctx)

	recv(t, sink.pol, "policy")
	recv(t, sink.begin, "begin-sync")
	initialUpsert = recv(t, sink.up, "initial upsert")
	recv(t, sink.book, "bookmark")

	src.sub <- routesync.Event{Kind: routesync.TypeUpsert, Route: routesync.RouteEntry{
		SandboxID: "s2", State: routesync.StateRunning,
		MMDSSecrets: `{"version":1,"values":{"key2":{"body_base64":"d29ybGQ=","content_type":"text/plain"}}}`,
	}}
	deltaUpsert = recv(t, sink.up, "delta upsert")
	return initialUpsert, deltaUpsert
}

// TestRouteSyncMMDSSecretsCarriedForGatedSubscriber proves a subscriber that
// registers with MMDSSecrets: true receives the real secret-blob plaintext on
// both the initial Range snapshot and a live delta.
func TestRouteSyncMMDSSecretsCarriedForGatedSubscriber(t *testing.T) {
	initial, delta := runMMDSSecretsGateFixture(t, true)
	if initial.MMDSSecrets == "" {
		t.Fatal("gated subscriber: initial snapshot upsert is missing MMDSSecrets")
	}
	if delta.MMDSSecrets == "" {
		t.Fatal("gated subscriber: live delta upsert is missing MMDSSecrets")
	}
}

// TestRouteSyncMMDSSecretsClearedForUngatedSubscriber proves a subscriber
// that does NOT register with MMDSSecrets: true never receives the field,
// on either the initial Range snapshot or a live delta -- even though the
// source's underlying RouteEntry carries it.
func TestRouteSyncMMDSSecretsClearedForUngatedSubscriber(t *testing.T) {
	initial, delta := runMMDSSecretsGateFixture(t, false)
	if initial.MMDSSecrets != "" {
		t.Fatalf("ungated subscriber: initial snapshot upsert leaked MMDSSecrets: %q", initial.MMDSSecrets)
	}
	if delta.MMDSSecrets != "" {
		t.Fatalf("ungated subscriber: live delta upsert leaked MMDSSecrets: %q", delta.MMDSSecrets)
	}
}
