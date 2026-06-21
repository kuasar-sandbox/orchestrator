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

// TestAuthRejectsBadKey checks the Phase 7g router auth: a create whose api key
// the registry rejects is 403'd at the router, before any reserve.
func TestAuthRejectsBadKey(t *testing.T) {
	op := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/op/verify-key" {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer op.Close()
	rt := New(strings.TrimPrefix(op.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/sandboxes", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderGroup, "/g")
	req.Header.Set(HeaderAPIKey, "e2b_bad")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("create with a rejected key status=%d, want 403", resp.StatusCode)
	}
}

// TestSandboxVerbForward checks the Phase 7f control plane: a sid-scoped verb is
// forwarded to the node that holds the sandbox (resolved via the op interface).
func TestSandboxVerbForward(t *testing.T) {
	var gotPath, gotMethod string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotMethod = r.URL.Path, r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer node.Close()
	nodeHost := strings.TrimPrefix(node.URL, "http://")

	op := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/op/route":
			_ = json.NewEncoder(w).Encode(routeResolve{SID: "sb-1", Group: "/g", DataEndpoint: nodeHost, State: "ready"})
		case "/op/verify-key":
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer op.Close()

	rt := New(strings.TrimPrefix(op.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv := httptest.NewServer(rt.Handler())
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/sandboxes/sb-1", nil)
	req.Host = "api.test.local"
	req.Header.Set(HeaderAPIKey, "e2b_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("verb status=%d, want 200", resp.StatusCode)
	}
	if gotPath != "/sandboxes/sb-1" || gotMethod != http.MethodDelete {
		t.Fatalf("node saw %s %s, want DELETE /sandboxes/sb-1", gotMethod, gotPath)
	}
}

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

	rt := New(strings.TrimPrefix(op.URL, "http://"), "test.local", 0, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
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
