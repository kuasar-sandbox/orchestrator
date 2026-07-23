package mmdsrpc

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// fakeTable is a minimal EndpointTable stub for server-side tests.
type fakeTable struct {
	mu    sync.Mutex
	calls int
	block chan struct{} // if non-nil, ServeStore blocks on this or ctx.Done()

	byPath map[string][2]string // sid+"\x00"+path -> {name, backendType}
	store  map[string][]byte    // sid+"\x00"+name -> value ("store" backend)

	lookupErr error // if set, Lookup returns this error unconditionally
}

func newFakeTable() *fakeTable {
	return &fakeTable{byPath: map[string][2]string{}, store: map[string][]byte{}}
}

func (f *fakeTable) Lookup(_ context.Context, sid, path string) (string, string, bool, error) {
	if f.lookupErr != nil {
		return "", "", false, f.lookupErr
	}
	v, ok := f.byPath[sid+"\x00"+path]
	if !ok {
		return "", "", false, nil
	}
	return v[0], v[1], true, nil
}

func (f *fakeTable) ServeStore(ctx context.Context, sid, name string) ([]byte, string, int64, bool, error) {
	f.mu.Lock()
	f.calls++
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, "", 0, false, nil
		}
	}
	v, ok := f.store[sid+"\x00"+name]
	return v, "text/plain", 1, ok, nil
}

func (f *fakeTable) ServeRelay(context.Context, string, string) (int, string, []byte, bool, error) {
	return 0, "", nil, false, nil
}

// harness wires a real Client (worker side) to a real Server (master side)
// over net.Pipe(), mirroring the production socketpair topology closely
// enough to exercise the actual wire protocol.
func harness(t *testing.T, table EndpointTable, maxInflight int) (*Client, func()) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	srv := NewServer(table, maxInflight, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ctx, serverConn)
		close(done)
	}()
	client := NewClient(clientConn)
	cleanup := func() {
		cancel()
		_ = clientConn.Close()
		_ = serverConn.Close()
		<-done
	}
	return client, cleanup
}

func TestLookupServeStoreRoundTrip(t *testing.T) {
	table := newFakeTable()
	table.byPath["s1\x00/latest/a"] = [2]string{"a", "store"}
	table.store["s1\x00a"] = []byte("hello")

	client, cleanup := harness(t, table, 128)
	defer cleanup()

	name, backend, found, err := client.Lookup(context.Background(), "s1", "/latest/a")
	if err != nil || !found || name != "a" || backend != "store" {
		t.Fatalf("Lookup = name=%q backend=%q found=%t err=%v", name, backend, found, err)
	}
	value, contentType, revision, present, err := client.ServeStore(context.Background(), "s1", "a")
	if err != nil || !present || string(value) != "hello" || contentType != "text/plain" || revision != 1 {
		t.Fatalf("ServeStore = value=%q contentType=%q revision=%d present=%t err=%v", value, contentType, revision, present, err)
	}
}

func TestLookupNotFound(t *testing.T) {
	table := newFakeTable()
	client, cleanup := harness(t, table, 128)
	defer cleanup()

	if _, _, found, err := client.Lookup(context.Background(), "s1", "/latest/nope"); found || err != nil {
		t.Fatalf("Lookup found an undeclared path or errored: found=%t err=%v", found, err)
	}
}

// TestLookupPropagatesTableUnavailable covers the requirement that a worker
// RPC failure return 503/504 as classified: when the master's table itself
// fails (e.g. internal/proxyendpoints.ErrUnavailable), resolve() must respond with
// ErrorCode: ErrCodeUnavailable, and the client must surface it as a
// distinct error — never collapsed into an ordinary "not found" the caller
// could silently fall through on.
func TestLookupPropagatesTableUnavailable(t *testing.T) {
	table := newFakeTable()
	table.lookupErr = errors.New("table unavailable")
	client, cleanup := harness(t, table, 128)
	defer cleanup()

	name, _, found, err := client.Lookup(context.Background(), "s1", "/latest/a")
	if err == nil || found || name != "" {
		t.Fatalf("Lookup with an unavailable table = name=%q found=%t err=%v, want a non-nil error and not found", name, found, err)
	}
}

func TestConcurrentRequestsMultiplex(t *testing.T) {
	table := newFakeTable()
	for i := 0; i < 20; i++ {
		name := string(rune('a' + i))
		table.byPath["s1\x00/latest/"+name] = [2]string{name, "store"}
		table.store["s1\x00"+name] = []byte(name + "-value")
	}
	client, cleanup := harness(t, table, 128)
	defer cleanup()

	var wg sync.WaitGroup
	errs := make(chan string, 20)
	for i := 0; i < 20; i++ {
		name := string(rune('a' + i))
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			gotName, _, found, err := client.Lookup(context.Background(), "s1", "/latest/"+name)
			if err != nil || !found || gotName != name {
				errs <- "lookup mismatch for " + name
				return
			}
			value, _, _, present, err := client.ServeStore(context.Background(), "s1", name)
			if err != nil || !present || string(value) != name+"-value" {
				errs <- "serve mismatch for " + name
			}
		}(name)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
}

func TestDuplicateRequestIDRejectedServerSide(t *testing.T) {
	table := newFakeTable()
	table.byPath["s1\x00/latest/a"] = [2]string{"a", "store"}
	table.store["s1\x00a"] = []byte("v")

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	srv := NewServer(table, 128, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Serve(ctx, serverConn)

	// Send the same request_id twice directly on the wire (bypassing
	// Client, which always mints fresh IDs) to exercise the server's
	// duplicate-ID guard.
	if err := writeFrame(clientConn, &wireMsg{Type: typeRequest, RequestID: 1, Request: &EndpointRequest{SandboxID: "s1", Path: "/latest/a"}}); err != nil {
		t.Fatal(err)
	}
	first, err := readFrame(clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if first.Type != typeResponse || first.RequestID != 1 || !first.Response.Found {
		t.Fatalf("first response = %+v", first)
	}

	// request_id 1 is no longer inflight (already replied+removed), so
	// reusing it is legal again — prove the duplicate guard actually fires
	// while the FIRST is still outstanding instead.
	table.mu.Lock()
	table.block = make(chan struct{})
	table.mu.Unlock()
	if err := writeFrame(clientConn, &wireMsg{Type: typeRequest, RequestID: 2, Request: &EndpointRequest{SandboxID: "s1", Path: "/latest/a"}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // let request_id 2 register as inflight
	if err := writeFrame(clientConn, &wireMsg{Type: typeRequest, RequestID: 2, Request: &EndpointRequest{SandboxID: "s1", Path: "/latest/a"}}); err != nil {
		t.Fatal(err)
	}
	close(table.block)

	resp, err := readFrame(clientConn)
	if err != nil {
		t.Fatal(err)
	}
	if resp.RequestID != 2 {
		t.Fatalf("got response for request_id %d, want exactly one reply for id 2 (the duplicate must be silently dropped)", resp.RequestID)
	}
}

func TestInflightLimitRejectsOverCap(t *testing.T) {
	table := newFakeTable()
	table.byPath["s1\x00/latest/a"] = [2]string{"a", "store"}
	table.block = make(chan struct{})

	client, cleanup := harness(t, table, 1)
	defer cleanup()

	done := make(chan struct{})
	go func() {
		client.Lookup(context.Background(), "s1", "/latest/a") // occupies the one inflight slot
		close(done)
	}()
	time.Sleep(50 * time.Millisecond)

	// Over the cap must be a distinct failure (a worker RPC
	// close/timeout/backpressure must return 503/504 as classified), never
	// silently collapsed into an ordinary "not found" — the caller must not
	// fall through to the built-in envd response for this sandbox/path.
	name, _, found, err := client.Lookup(context.Background(), "s1", "/latest/a")
	if err == nil || found || name != "" {
		t.Fatalf("Lookup over the inflight cap = name=%q found=%t err=%v, want a non-nil error and not found", name, found, err)
	}

	table.mu.Lock()
	close(table.block)
	table.mu.Unlock()
	<-done
}

func TestCancellationFreesServerResources(t *testing.T) {
	table := newFakeTable()
	table.byPath["s1\x00/latest/a"] = [2]string{"a", "store"}
	table.block = make(chan struct{})
	defer func() {
		table.mu.Lock()
		if table.block != nil {
			close(table.block)
		}
		table.mu.Unlock()
	}()

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	srv := NewServer(table, 1, nil, nil) // cap 1: a second call only succeeds if the first was truly cancelled server-side
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.Serve(ctx, serverConn)
	client := NewClient(clientConn)

	callCtx, callCancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer callCancel()
	if _, _, found, err := client.Lookup(callCtx, "s1", "/latest/a"); found || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Lookup should have been cancelled by its own context timeout: found=%t err=%v", found, err)
	}

	// Give the server a moment to process the Cancel frame the client sent.
	time.Sleep(50 * time.Millisecond)

	table.mu.Lock()
	table.block = nil // let subsequent calls return immediately
	table.mu.Unlock()
	if _, _, found, err := client.Lookup(context.Background(), "s1", "/latest/a"); !found || err != nil {
		t.Fatalf("Lookup after cancellation freed the inflight slot should have succeeded: found=%t err=%v", found, err)
	}
}

func TestClientCloseUnblocksPendingCalls(t *testing.T) {
	table := newFakeTable()
	table.byPath["s1\x00/latest/a"] = [2]string{"a", "store"}
	table.block = make(chan struct{})

	clientConn, serverConn := net.Pipe()
	srv := NewServer(table, 128, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Serve(ctx, serverConn)
	client := NewClient(clientConn)

	done := make(chan struct{})
	go func() {
		client.Lookup(context.Background(), "s1", "/latest/a")
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)

	cancel()
	_ = clientConn.Close()
	_ = serverConn.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Lookup did not unblock after the connection closed")
	}
	close(table.block)
}
