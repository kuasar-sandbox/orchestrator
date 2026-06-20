package router

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func writeTestFrame(t *testing.T, w io.Writer, ev *watchEvent) {
	t.Helper()
	b, _ := json.Marshal(ev)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(b)))
	w.Write(hdr[:])
	w.Write(b)
}

// TestRouteCacheFromWatch checks the Phase 7e hot path: the router syncs its
// route cache from /op/watch and serves the data plane from it (no /op/route).
func TestRouteCacheFromWatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var reached bool
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")

	var routeHits int
	op := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/op/watch":
			fl := w.(http.Flusher)
			writeTestFrame(t, w, &watchEvent{Type: "put", Key: "sandbox//g/rk", Route: &routeResolve{SID: "sb-1", DataEndpoint: nodeHost, AccessToken: "tok", State: "ready"}})
			writeTestFrame(t, w, &watchEvent{Type: "bookmark", Rev: 1})
			fl.Flush()
			// Return after the snapshot; RunWatch reconnects + re-snapshots, which
			// keeps the cache warm without the handler holding the stream (and
			// avoids a Close()-vs-context-cancel deadlock in the test teardown).
		case "/op/route":
			routeHits++
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer op.Close()

	rt := New(strings.TrimPrefix(op.URL, "http://"), "test.local", slog.New(slog.NewTextHandler(io.Discard, nil)))
	go rt.RunWatch(ctx)

	// Wait for the cache to populate from the watch snapshot.
	deadline := time.Now().Add(2 * time.Second)
	for rt.cachedRoute("sb-1") == nil {
		if time.Now().After(deadline) {
			t.Fatal("route cache never populated from the watch")
		}
		time.Sleep(10 * time.Millisecond)
	}

	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	req.Host = "49983-sb-1.test.local"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("data plane status=%d, want 204", resp.StatusCode)
	}
	if !reached {
		t.Fatal("request did not reach the node via the cached route")
	}
	if routeHits != 0 {
		t.Fatalf("op /route was hit %d times; the cache should serve the hot path", routeHits)
	}
}
