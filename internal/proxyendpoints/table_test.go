package proxyendpoints

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/mmdsrelay"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestLookupUnavailableBeforeFirstBookmark(t *testing.T) {
	// A non-nil relay so ServeRelay's own relay==nil early-503 doesn't mask
	// the availability check this test targets.
	tb := New(&fakeRelayFetcher{}, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	// Before the first bookmark, the table is unavailable — this must
	// surface as ErrUnavailable, never a plain found=false the caller could
	// confuse with "no such endpoint" and silently fall through on.
	if _, _, found, err := tb.Lookup(context.Background(), "s1", "/latest/a"); found || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Lookup before bookmark = found=%t err=%v, want found=false err=ErrUnavailable", found, err)
	}
	if _, _, _, present, err := tb.ServeStore(context.Background(), "s1", "a"); present || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ServeStore before bookmark = present=%t err=%v, want present=false err=ErrUnavailable", present, err)
	}
	if _, _, _, ok, err := tb.ServeRelay(context.Background(), "s1", "a"); ok || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("ServeRelay before bookmark = ok=%t err=%v, want ok=false err=ErrUnavailable", ok, err)
	}
}

func TestBeginBookmarkStagesThenSwapsAtomically(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.MmdsBookmark("gen-1")

	name, backend, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a")
	if !found || name != "a" || backend != "store" {
		t.Fatalf("Lookup after bookmark = name=%q backend=%q found=%t", name, backend, found)
	}
}

func TestBookmarkWithMismatchedGenerationIsDropped(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.MmdsBookmark("gen-stale") // does not match "gen-1"

	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a"); found {
		t.Fatal("Lookup found an entry staged under a mismatched bookmark generation")
	}
}

func TestBookmarkSweepsEntriesNotSeenThisGeneration(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "old", Path: "/latest/old", BackendType: "store"})
	tb.MmdsBookmark("gen-1")

	// A second full generation that no longer includes "old" (e.g. the
	// sandbox was deleted while disconnected) sweeps it away at bookmark.
	tb.BeginMmdsSync("gen-2")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "new", Path: "/latest/new", BackendType: "store"})
	tb.MmdsBookmark("gen-2")

	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/old"); found {
		t.Fatal("Lookup found an entry not re-declared in the latest full generation")
	}
	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/new"); !found {
		t.Fatal("Lookup did not find the latest generation's entry")
	}
}

func TestEqualRevisionMismatchForcesResync(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store", Revision: 1, ContentType: "text/plain"})
	tb.MmdsBookmark("gen-1")
	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a"); !found {
		t.Fatal("setup: entry should be visible after the first bookmark")
	}

	// Same revision, different payload (a protocol error).
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store", Revision: 1, ContentType: "application/json"})

	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a"); found {
		t.Fatal("Lookup still found an entry after an equal-revision mismatch forced a resync")
	}
}

func TestLowerRevisionIgnored(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store", Revision: 5, ValuePresent: true, SecretPlaintext: "new"})
	tb.MmdsBookmark("gen-1")

	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store", Revision: 3, ValuePresent: true, SecretPlaintext: "stale"})

	value, _, revision, present, _ := tb.ServeStore(context.Background(), "s1", "a")
	if !present || revision != 5 || string(value) != "new" {
		t.Fatalf("ServeStore after a stale lower-revision upsert = value=%q revision=%d present=%t", value, revision, present)
	}
}

func TestApplyMmdsDeleteRemovesEntry(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.MmdsBookmark("gen-1")

	tb.ApplyMmdsDelete(routesync.MmdsEndpointKey{SandboxID: "s1", Name: "a"})
	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a"); found {
		t.Fatal("Lookup found an entry after ApplyMmdsDelete")
	}
}

func TestDisconnectedClearsLiveAndMarksUnavailable(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store", Revision: 1, ValuePresent: true, SecretPlaintext: "hello"})
	tb.MmdsBookmark("gen-1")
	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a"); !found {
		t.Fatal("setup: entry should be visible after the first bookmark")
	}

	tb.Disconnected()

	if _, _, found, _ := tb.Lookup(context.Background(), "s1", "/latest/a"); found {
		t.Fatal("Lookup still found an entry after Disconnected")
	}
	if _, _, _, present, _ := tb.ServeStore(context.Background(), "s1", "a"); present {
		t.Fatal("ServeStore still returned present=true after Disconnected")
	}
}

func TestDisconnectedWakesParkedWaitEarly(t *testing.T) {
	tb := New(nil, 0, 5*time.Second, nil) // long timeout: the test fails if it actually waits it out
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.MmdsBookmark("gen-1")

	done := make(chan struct{})
	var present bool
	go func() {
		_, _, _, present, _ = tb.ServeStore(context.Background(), "s1", "a")
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let ServeStore park before disconnecting
	start := time.Now()
	tb.Disconnected()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStore did not wake up after Disconnected")
	}
	if present {
		t.Fatal("ServeStore returned present=true for a request parked across a disconnect")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("ServeStore took %v to wake after Disconnected, want near-immediate", elapsed)
	}
}

func TestCapacityBoundRejectsOverLimit(t *testing.T) {
	tb := New(nil, 2, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "b", Path: "/latest/b", BackendType: "store"})
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "c", Path: "/latest/c", BackendType: "store"}) // over the cap
	tb.MmdsBookmark("gen-1")

	count := 0
	for _, path := range []string{"/latest/a", "/latest/b", "/latest/c"} {
		if _, _, found, _ := tb.Lookup(context.Background(), "s1", path); found {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("got %d entries after exceeding the capacity bound, want 2", count)
	}
}

func TestServeStoreNeverConfiguredWaitsThenNotPresent(t *testing.T) {
	tb := New(nil, 0, 50*time.Millisecond, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.MmdsBookmark("gen-1")

	start := time.Now()
	_, _, _, present, _ := tb.ServeStore(context.Background(), "s1", "a")
	if present {
		t.Fatal("ServeStore returned present=true for a never-configured endpoint")
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("ServeStore returned too fast (%v), want it to have waited out the timeout", elapsed)
	}
}

func TestServeStoreWakesOnLiveUpsert(t *testing.T) {
	tb := New(nil, 0, 5*time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store"})
	tb.MmdsBookmark("gen-1")

	done := make(chan struct{})
	var value []byte
	var present bool
	go func() {
		value, _, _, present, _ = tb.ServeStore(context.Background(), "s1", "a")
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "store", Revision: 1, ValuePresent: true, SecretPlaintext: "hello"})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeStore did not wake up after a live upsert")
	}
	if !present || string(value) != "hello" {
		t.Fatalf("ServeStore after wake = present=%t value=%q", present, value)
	}
}

type fakeRelayFetcher struct {
	mu     sync.Mutex
	calls  int
	block  chan struct{}
	result mmdsrelay.Result
}

func (f *fakeRelayFetcher) Fetch(ctx context.Context, key, rawURL, headerName, headerValue string) mmdsrelay.Result {
	f.mu.Lock()
	f.calls++
	block := f.block
	f.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return mmdsrelay.Result{Status: 499}
		}
	}
	return f.result
}

func TestServeRelayNilClientReturns503(t *testing.T) {
	tb := New(nil, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{SandboxID: "s1", Name: "a", Path: "/latest/a", BackendType: "relay"})
	tb.MmdsBookmark("gen-1")

	status, _, _, ok, _ := tb.ServeRelay(context.Background(), "s1", "a")
	if !ok || status != http.StatusServiceUnavailable {
		t.Fatalf("ServeRelay with nil relay = status=%d ok=%t, want 503/true", status, ok)
	}
}

func TestServeRelayDispatchesConfiguredEndpoint(t *testing.T) {
	fake := &fakeRelayFetcher{result: mmdsrelay.Result{Status: 200, ContentType: "application/json", Body: []byte(`{}`)}}
	tb := New(fake, 0, time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{
		SandboxID: "s1", Name: "creds", Path: "/latest/creds", BackendType: "relay",
		PublicConfigJSON: `{"url":"https://example.com/creds","auth_header_name":"X-Auth"}`,
		Revision:         1, ValuePresent: true, SecretPlaintext: "secret",
	})
	tb.MmdsBookmark("gen-1")

	status, contentType, body, ok, _ := tb.ServeRelay(context.Background(), "s1", "creds")
	if !ok || status != 200 || contentType != "application/json" || string(body) != "{}" {
		t.Fatalf("ServeRelay = status=%d contentType=%q body=%q ok=%t", status, contentType, body, ok)
	}
	if fake.calls != 1 {
		t.Fatalf("got %d relay Fetch calls, want 1", fake.calls)
	}
}

func TestServeRelayCancelsOnLiveUpsert(t *testing.T) {
	fake := &fakeRelayFetcher{block: make(chan struct{})}
	tb := New(fake, 0, 5*time.Second, nil)
	tb.BeginMmdsSync("gen-1")
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{
		SandboxID: "s1", Name: "creds", Path: "/latest/creds", BackendType: "relay",
		PublicConfigJSON: `{"url":"https://example.com/creds","auth_header_name":"X-Auth"}`,
		Revision:         1, ValuePresent: true, SecretPlaintext: "old",
	})
	tb.MmdsBookmark("gen-1")

	done := make(chan mmdsrelay.Result, 1)
	go func() {
		status, _, _, _, _ := tb.ServeRelay(context.Background(), "s1", "creds")
		done <- mmdsrelay.Result{Status: status}
	}()

	time.Sleep(50 * time.Millisecond)
	tb.ApplyMmdsUpsert(routesync.MmdsEndpointEntry{
		SandboxID: "s1", Name: "creds", Path: "/latest/creds", BackendType: "relay",
		PublicConfigJSON: `{"url":"https://example.com/creds","auth_header_name":"X-Auth"}`,
		Revision:         2, ValuePresent: true, SecretPlaintext: "new",
	})

	select {
	case res := <-done:
		if res.Status != 499 {
			t.Fatalf("ServeRelay result status = %d, want 499 (cancellation sentinel)", res.Status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeRelay did not cancel its in-flight Fetch after a live upsert")
	}
}
