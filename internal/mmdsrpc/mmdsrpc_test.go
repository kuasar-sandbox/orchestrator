package mmdsrpc

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"
)

func newPair(t *testing.T, handler Handler) (*Client, *Server) {
	t.Helper()
	a, b := net.Pipe()
	client := NewClient(a)
	server := NewServer(b, handler, nil)
	go server.Serve()
	t.Cleanup(func() { client.Close() })
	return client, server
}

func TestClientResolveFound(t *testing.T) {
	client, _ := newPair(t, func(sid, path string) (Route, bool) {
		if sid == "sbx-1" && path == "/x" {
			return Route{Type: "static", ContentType: "text/plain", Body: "hi"}, true
		}
		return Route{}, false
	})

	resp, err := client.Resolve(context.Background(), "sbx-1", "/x")
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Found || resp.Type != "static" || resp.Body != "hi" || resp.ContentType != "text/plain" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestClientResolveNotFound(t *testing.T) {
	client, _ := newPair(t, func(sid, path string) (Route, bool) {
		return Route{}, false
	})

	resp, err := client.Resolve(context.Background(), "unknown-sid", "/x")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Found {
		t.Fatalf("expected not-found, got %+v", resp)
	}
}

func TestClientConcurrentMultiplexedRequests(t *testing.T) {
	client, _ := newPair(t, func(sid, path string) (Route, bool) {
		// Echo sid back in the body so each response is distinguishable.
		return Route{Type: "static", Body: sid}, true
	})

	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	bodies := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sid := "sbx-" + string(rune('a'+i%26))
			resp, err := client.Resolve(context.Background(), sid, "/x")
			errs[i] = err
			if err == nil {
				bodies[i] = resp.Body
			}
			if err == nil && resp.Body != sid {
				t.Errorf("request %d: got body %q for sid %q (cross-talk between multiplexed requests)", i, resp.Body, sid)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
}

func TestClientResolveAfterCloseFails(t *testing.T) {
	client, _ := newPair(t, func(sid, path string) (Route, bool) { return Route{}, false })
	client.Close()

	if _, err := client.Resolve(context.Background(), "sid", "/x"); err == nil {
		t.Fatal("expected an error after Close")
	}
}

func TestClientResolveContextCancellation(t *testing.T) {
	a, b := net.Pipe()
	client := NewClient(a)
	defer client.Close()

	// Drain requests on b without ever responding, so Resolve's write
	// succeeds and it blocks waiting for a response that never comes.
	go func() {
		for {
			var req EndpointRequest
			if err := readFrame(b, &req); err != nil {
				return
			}
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Resolve(ctx, "sid", "/x"); err == nil {
		t.Fatal("expected a context deadline error")
	}
}
